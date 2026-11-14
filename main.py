#!/usr/bin/env python3
"""free-model-aggregator-01: one server, two APIs, ten simulated free-tier providers.

Serves the OpenAI Chat Completions API and the Anthropic Messages API (JSON and
streaming) over ten free-tier providers (openai / anthropic / gemini wires),
routing each request to the best-rated free model that meets its requirements,
failing over on 402/429/5xx, and reporting the catalog it registered.

Python 3.13, standard library only. No model calls of its own: every answer
comes from a provider under MAKEFUN_ENV_URL. Deterministic: every routing
choice is a function of the brief, the requests and the providers' answers;
no wall clock, no unseeded randomness.

Usage:
  python3 main.py --list     print the catalog document and exit 0
  python3 main.py            run the trial server (MAKEFUN_ENV_URL,
                             MAKEFUN_PROMPT_FILE, MAKEFUN_TRIAL_SEED)
"""

import concurrent.futures
import http.client
import http.server
import json
import os
import socket
import sys
import threading

PROTOCOL_VERSION = 1
PROBE_TIMEOUT_S = 5
FIXED_CREATED = 1767225600
FIXED_ANTHROPIC_CREATED_AT = "2026-01-01T00:00:00Z"
ANTHROPIC_VERSION = "2023-06-01"
PROVIDER_TIMEOUT_S = 10
POLL_INTERVAL_S = 0.1
MAX_BODY_BYTES = 32 * 1024 * 1024

ENV_URL = os.environ.get("MAKEFUN_ENV_URL", "").rstrip("/")
PROMPT_FILE = os.environ.get("MAKEFUN_PROMPT_FILE", "")

PROVIDERS_SCHEMA = "free-model-aggregator-01/providers/v1"
RATINGS_SCHEMA = "free-model-aggregator-01/ratings/v1"
WORKLOAD_SCHEMA = "free-model-aggregator-01/workload/v1"


def log(msg):
    sys.stderr.write("aggregator: %s\n" % msg)
    sys.stderr.flush()


def is_record(v):
    return v is not None and isinstance(v, dict)


# --------------------------------------------------------------------------
# Brief parsing: find the three JSON documents by their `schema` member.
# --------------------------------------------------------------------------

def _object_end(text, start):
    depth = 0
    in_string = False
    i = start
    while i < len(text):
        ch = text[i]
        if in_string:
            if ch == "\\":
                i += 1
            elif ch == '"':
                in_string = False
        elif ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _documents_in(text):
    out = []
    start = text.find("{")
    while start != -1:
        end = _object_end(text, start)
        if end != -1:
            try:
                value = json.loads(text[start:end + 1])
                if is_record(value) and isinstance(value.get("schema"), str):
                    out.append(value)
            except Exception:
                pass
        start = text.find("{", start + 1)
    return out


def _str(v):
    return v if isinstance(v, str) else ""


def _num_or_none(v):
    return v if isinstance(v, (int, float)) and v == v and abs(v) != float("inf") else None


class Brief(object):
    def __init__(self):
        self.providers = []   # directory rows
        self.ratings = []     # rating rows
        self.best = {}        # request id -> best rating_milli
        self.manifest = []    # workload rows
        self.error = None


def read_brief(path):
    brief = Brief()
    if not path:
        brief.error = "MAKEFUN_PROMPT_FILE is not set"
        return brief
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except Exception as exc:
        brief.error = "cannot read %s: %s" % (path, exc)
        return brief
    docs = _documents_in(text)
    directory = None
    ratings_doc = None
    workload_doc = None
    for doc in docs:
        if doc.get("schema") == PROVIDERS_SCHEMA and isinstance(doc.get("providers"), list):
            directory = doc
        elif doc.get("schema") == RATINGS_SCHEMA and isinstance(doc.get("ratings"), list):
            ratings_doc = doc
        elif doc.get("schema") == WORKLOAD_SCHEMA and isinstance(doc.get("manifest"), list):
            workload_doc = doc
    if directory is None:
        brief.error = "the brief holds no %s document" % PROVIDERS_SCHEMA
        return brief
    for raw in directory["providers"]:
        if not is_record(raw):
            continue
        pid = _str(raw.get("id"))
        wire = raw.get("wire")
        if pid == "" or wire not in ("openai", "anthropic", "gemini"):
            continue
        auth = raw.get("auth")
        if auth not in ("bearer", "x-api-key", "x-goog-api-key", "none"):
            auth = "bearer"
        base = _str(raw.get("path")) or _str(raw.get("base_path")) or \
            _str(raw.get("base_url")) or _str(raw.get("base"))
        brief.providers.append({
            "id": pid, "name": _str(raw.get("name")) or pid, "wire": wire,
            "auth": auth, "base": base, "key_env": _str(raw.get("key_env")),
            "key": _str(raw.get("key")),
        })
    if ratings_doc is not None:
        for raw in ratings_doc["ratings"]:
            if not is_record(raw):
                continue
            model = _str(raw.get("model"))
            provider = _str(raw.get("provider"))
            if model == "" or provider == "":
                continue
            brief.ratings.append({
                "model": model, "provider": provider,
                "rating_milli": raw.get("rating_milli") if isinstance(raw.get("rating_milli"), (int, float)) else 0,
                "context_length": _num_or_none(raw.get("context_length")),
                "tools": raw.get("tools") is True,
                "vision": raw.get("vision") is True,
                "json": raw.get("json") is True,
                "free": raw.get("free") is True,
            })
        if isinstance(ratings_doc.get("best"), list):
            for raw in ratings_doc["best"]:
                if is_record(raw) and _str(raw.get("id")):
                    brief.best[_str(raw.get("id"))] = raw.get("rating_milli") or 0
    if workload_doc is not None:
        for raw in workload_doc["manifest"]:
            if not is_record(raw):
                continue
            rid = _str(raw.get("id"))
            if rid == "":
                continue
            req = raw.get("requires") if is_record(raw.get("requires")) else {}
            brief.manifest.append({
                "id": rid,
                "api": "anthropic" if raw.get("api") == "anthropic" else "openai",
                "stream": raw.get("stream") is True,
                "weight": 2 if raw.get("weight") == 2 else 1,
                "requires": {
                    "tools": req.get("tools") is True,
                    "vision": req.get("vision") is True,
                    "json": req.get("json") is True,
                    "min_context": req.get("min_context") if isinstance(req.get("min_context"), (int, float)) else None,
                },
            })
    return brief


# --------------------------------------------------------------------------
# Catalog probe: each provider's catalog route, in its own wire, is the
# truth about which models it lists; the brief's ratings table is the
# sealed truth about a listed model's free flag and context length where
# it has a row. A model the table never mentions (a paid model) takes the
# listing's own free flag, else paid. Probing is free: a catalog GET is
# not a provider call and spends no budget.
# --------------------------------------------------------------------------

def _bool_or_none(v):
    return v if isinstance(v, bool) else None


def _zero_priced(v):
    pricing = v.get("pricing") if is_record(v) else None
    if not is_record(pricing):
        return None
    values = []
    for x in pricing.values():
        if isinstance(x, (int, float)):
            values.append(x)
        elif isinstance(x, str) and x.strip() != "":
            try:
                values.append(float(x))
            except ValueError:
                pass
    if not values:
        return None
    return all(n == 0 for n in values)


def _probed_model(wire, v):
    if not is_record(v):
        return None
    if wire == "gemini":
        model_id = _str(v.get("name"))
        if model_id.startswith("models/"):
            model_id = model_id[len("models/"):]
    else:
        model_id = _str(v.get("id"))
    if model_id == "":
        return None
    context = _num_or_none(v.get("context_length"))
    if context is None:
        context = _num_or_none(v.get("context_window"))
    if context is None:
        context = _num_or_none(v.get("inputTokenLimit"))
    free = _bool_or_none(v.get("free"))
    if free is None:
        free = _zero_priced(v)
    return {"id": model_id, "free": free, "context_length": context}


def probe_provider(prov):
    """GET one provider's catalog route; returns its listed models or None."""
    if not ENV_URL:
        return None
    base = join_base(ENV_URL, prov["base"])
    path = (base + "/v1beta/models") if prov["wire"] == "gemini" else (base + "/v1/models")
    headers = {"accept": "application/json"}
    headers.update(auth_headers(prov["auth"], prov["key"]))
    if prov["wire"] == "anthropic":
        headers["anthropic-version"] = ANTHROPIC_VERSION
    try:
        parts = _url_parts(path)
        conn = http.client.HTTPConnection(parts["host"], parts["port"], timeout=PROBE_TIMEOUT_S)
        try:
            conn.request("GET", parts["path"], headers=headers)
            resp = conn.getresponse()
            data = resp.read().decode("utf-8", "replace")
            if resp.status < 200 or resp.status >= 300:
                log("catalog probe %s: HTTP %s" % (prov["id"], resp.status))
                return None
            body = json.loads(data)
        finally:
            conn.close()
    except Exception as exc:
        log("catalog probe %s failed: %s" % (prov["id"], exc))
        return None
    if not is_record(body):
        return None
    rows = body.get("models") if prov["wire"] == "gemini" else body.get("data")
    if not isinstance(rows, list):
        return None
    out = []
    for raw in rows:
        model = _probed_model(prov["wire"], raw)
        if model is not None:
            out.append(model)
    return out


def probe_all(brief):
    """Probe every provider in parallel; returns [(provider, models|None)]."""
    if not brief.providers or not ENV_URL:
        return [(prov, None) for prov in brief.providers]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(brief.providers)) as pool:
        results = list(pool.map(probe_provider, brief.providers))
    return list(zip(brief.providers, results))


def build_catalog(brief, probes):
    """The one catalog: directory order, models sorted by id, deduped."""
    rating_by_key = {(r["provider"], r["model"]): r for r in brief.ratings}
    providers = []
    for prov, listed in probes:
        if listed is None:
            continue
        seen = set()
        models = []
        for model in sorted(listed, key=lambda m: m["id"]):
            if model["id"] in seen:
                continue
            seen.add(model["id"])
            row = rating_by_key.get((prov["id"], model["id"]))
            if row is not None:
                free = row["free"]
                context = row["context_length"]
            else:
                free = model["free"] if model["free"] is not None else False
                context = model["context_length"]
            models.append({"id": model["id"], "free": free, "context_length": context})
        providers.append({"id": prov["id"], "base_url": prov["base"],
                          "key_env": prov["key_env"], "wire": prov["wire"],
                          "models": models})
    return {"providers": providers}


def free_model_ids(catalog):
    seen = set()
    for prov in catalog["providers"]:
        for model in prov["models"]:
            if model.get("free") is True and isinstance(model.get("id"), str):
                seen.add(model["id"])
    return sorted(seen)


# --------------------------------------------------------------------------
# Routing
# --------------------------------------------------------------------------

def meets_requirements(row, requires):
    if requires.get("tools") and not row["tools"]:
        return False
    if requires.get("vision") and not row["vision"]:
        return False
    if requires.get("json") and not row["json"]:
        return False
    mc = requires.get("min_context")
    if mc is not None and (row["context_length"] is None or row["context_length"] < mc):
        return False
    return True


class Router(object):
    """Rank free candidates by rating; fail over on refusal.

    A provider that fails 3 calls in a row is almost certainly out of budget
    or in outage (the drawn failure schedules run at most 2 in a row), so it
    is tried last until it answers again.
    """

    def __init__(self, brief):
        self.brief = brief
        self.by_id = {p["id"]: p for p in brief.providers}
        self.streak = {p["id"]: 0 for p in brief.providers}

    def candidates(self, requires):
        out = []
        for row in self.brief.ratings:
            if not row["free"]:
                continue
            if not meets_requirements(row, requires):
                continue
            prov = self.by_id.get(row["provider"])
            if prov is None:
                continue
            # An anthropic-wire provider echoes json_mode false, so it can
            # never serve a JSON-mode request.
            if requires.get("json") and prov["wire"] == "anthropic":
                continue
            out.append((prov, row))
        out.sort(key=lambda pr: (1 if self.streak[pr[0]["id"]] >= 3 else 0,
                                 -pr[1]["rating_milli"], pr[0]["id"], pr[1]["model"]))
        return out

    def note_success(self, provider_id):
        self.streak[provider_id] = 0

    def note_failure(self, provider_id):
        self.streak[provider_id] = self.streak.get(provider_id, 0) + 1


# --------------------------------------------------------------------------
# Wire helpers
# --------------------------------------------------------------------------

def join_base(env_url, base):
    b = (base or "").strip()
    if b.lower().startswith("http://") or b.lower().startswith("https://"):
        return b.rstrip("/")
    prefix = (env_url or "").rstrip("/")
    if b == "":
        return prefix
    return prefix + ("" if b.startswith("/") else "/") + b.rstrip("/")


def auth_headers(auth, key):
    if key == "":
        return {}
    if auth == "bearer":
        return {"authorization": "Bearer " + key}
    if auth == "x-api-key":
        return {"x-api-key": key}
    if auth == "x-goog-api-key":
        return {"x-goog-api-key": key}
    return {}


def text_of(content):
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    return "\n".join(b["text"] for b in content
                     if is_record(b) and b.get("type") == "text"
                     and isinstance(b.get("text"), str))


def tool_result_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text = text_of(content)
        return text if text != "" else json.dumps(content)
    if content is None:
        return ""
    return json.dumps(content)


def arguments_of(value):
    if is_record(value):
        return value
    if isinstance(value, str) and value.strip() != "":
        try:
            parsed = json.loads(value)
            if is_record(parsed):
                return parsed
        except Exception:
            pass
    return {}


def stop_reason_of(finish):
    if finish == "length":
        return "max_tokens"
    if finish in ("tool_calls", "function_call"):
        return "tool_use"
    if finish == "content_filter":
        return "refusal"
    return "end_turn"


def sse_event(event, data):
    prefix = "" if event is None else "event: %s\n" % event
    return "%sdata: %s\n\n" % (prefix, json.dumps(data))


def split_text(text, size=13):
    if text == "":
        return [""]
    return [text[i:i + size] for i in range(0, len(text), size)]


# --- request translation: anthropic -> openai -----------------------------

def _anthropic_part_to_openai(block):
    if block.get("type") == "text" and isinstance(block.get("text"), str):
        return {"type": "text", "text": block["text"]}
    if block.get("type") == "image" and is_record(block.get("source")):
        source = block["source"]
        if source.get("type") == "base64" and isinstance(source.get("data"), str):
            media = source.get("media_type") if isinstance(source.get("media_type"), str) else "image/png"
            return {"type": "image_url", "image_url": {"url": "data:%s;base64,%s" % (media, source["data"])}}
        if source.get("type") == "url" and isinstance(source.get("url"), str):
            return {"type": "image_url", "image_url": {"url": source["url"]}}
    return None


def anthropic_to_openai(body, model):
    messages = []
    system = text_of(body.get("system"))
    if system != "":
        messages.append({"role": "system", "content": system})
    for raw in body.get("messages") or []:
        if not is_record(raw):
            continue
        assistant = raw.get("role") == "assistant"
        content = raw.get("content")
        if isinstance(content, str):
            messages.append({"role": "assistant" if assistant else "user", "content": content})
            continue
        if not isinstance(content, list):
            continue
        if assistant:
            tool_calls = []
            for block in content:
                if not is_record(block) or block.get("type") != "tool_use":
                    continue
                tool_calls.append({
                    "id": block.get("id") if isinstance(block.get("id"), str) else "",
                    "type": "function",
                    "function": {
                        "name": block.get("name") if isinstance(block.get("name"), str) else "",
                        "arguments": json.dumps(block.get("input") if is_record(block.get("input")) else {}),
                    },
                })
            text = text_of(content)
            msg = {"role": "assistant", "content": text if text != "" else None}
            if tool_calls:
                msg["tool_calls"] = tool_calls
            messages.append(msg)
            continue
        parts = []
        for block in content:
            if not is_record(block):
                continue
            if block.get("type") == "tool_result":
                messages.append({
                    "role": "tool",
                    "tool_call_id": block.get("tool_use_id") if isinstance(block.get("tool_use_id"), str) else "",
                    "content": tool_result_text(block.get("content")),
                })
                continue
            part = _anthropic_part_to_openai(block)
            if part is not None:
                parts.append(part)
        if parts:
            messages.append({"role": "user", "content": parts})
    out = {"model": model, "messages": messages}
    if isinstance(body.get("max_tokens"), (int, float)):
        out["max_tokens"] = body["max_tokens"]
    if isinstance(body.get("temperature"), (int, float)):
        out["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)):
        out["top_p"] = body["top_p"]
    if isinstance(body.get("stop_sequences"), list) and body["stop_sequences"]:
        out["stop"] = body["stop_sequences"]
    if isinstance(body.get("tools"), list):
        tools = []
        for raw in body["tools"]:
            if not is_record(raw) or not isinstance(raw.get("name"), str):
                continue
            tools.append({
                "type": "function",
                "function": {
                    "name": raw["name"],
                    "description": raw.get("description") if isinstance(raw.get("description"), str) else "",
                    "parameters": raw.get("input_schema") if is_record(raw.get("input_schema")) else {"type": "object", "properties": {}},
                },
            })
        if tools:
            out["tools"] = tools
    choice = body.get("tool_choice")
    if is_record(choice):
        if choice.get("type") == "auto":
            out["tool_choice"] = "auto"
        elif choice.get("type") == "any":
            out["tool_choice"] = "required"
        elif choice.get("type") == "tool" and isinstance(choice.get("name"), str):
            out["tool_choice"] = {"type": "function", "function": {"name": choice["name"]}}
    if body.get("stream") is True:
        out["stream"] = True
        out["stream_options"] = {"include_usage": True}
    return out


# --- request translation: openai -> anthropic ----------------------------

def _openai_part_to_anthropic(part):
    if part.get("type") == "text" and isinstance(part.get("text"), str):
        return {"type": "text", "text": part["text"]}
    if part.get("type") == "image_url" and is_record(part.get("image_url")):
        url = part["image_url"].get("url")
        if not isinstance(url, str):
            return None
        if url.startswith("data:"):
            head, _, data = url[5:].partition(",")
            media = head.split(";")[0] or "image/png"
            return {"type": "image", "source": {"type": "base64", "media_type": media, "data": data}}
        return {"type": "image", "source": {"type": "url", "url": url}}
    return None


def openai_to_anthropic(body, model):
    messages = []
    system_parts = []
    for raw in body.get("messages") or []:
        if not is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "system":
            text = text_of(content)
            if text != "":
                system_parts.append(text)
            continue
        if role == "user":
            if isinstance(content, str):
                messages.append({"role": "user", "content": content})
            elif isinstance(content, list):
                blocks = []
                for part in content:
                    if not is_record(part):
                        continue
                    block = _openai_part_to_anthropic(part)
                    if block is not None:
                        blocks.append(block)
                if blocks:
                    messages.append({"role": "user", "content": blocks})
            continue
        if role == "assistant":
            blocks = []
            text = text_of(content)
            if text != "":
                blocks.append({"type": "text", "text": text})
            for call in raw.get("tool_calls") or []:
                if not is_record(call) or not is_record(call.get("function")):
                    continue
                fn = call["function"]
                blocks.append({
                    "type": "tool_use",
                    "id": call.get("id") if isinstance(call.get("id"), str) else "",
                    "name": fn.get("name") if isinstance(fn.get("name"), str) else "",
                    "input": arguments_of(fn.get("arguments")),
                })
            messages.append({"role": "assistant", "content": blocks})
            continue
        if role == "tool":
            blocks = [{
                "type": "tool_result",
                "tool_use_id": raw.get("tool_call_id") if isinstance(raw.get("tool_call_id"), str) else "",
                "content": tool_result_text(content),
            }]
            messages.append({"role": "user", "content": blocks})
            continue
    out = {"model": model, "messages": messages}
    if system_parts:
        out["system"] = "\n".join(system_parts)
    max_tokens = body.get("max_tokens")
    if not isinstance(max_tokens, (int, float)):
        max_tokens = body.get("max_completion_tokens")
    if isinstance(max_tokens, (int, float)):
        out["max_tokens"] = max_tokens
    if isinstance(body.get("temperature"), (int, float)):
        out["temperature"] = body["temperature"]
    if isinstance(body.get("top_p"), (int, float)):
        out["top_p"] = body["top_p"]
    stop = body.get("stop")
    if isinstance(stop, str):
        stop = [stop]
    if isinstance(stop, list) and stop:
        out["stop_sequences"] = [s for s in stop if isinstance(s, str)]
    if isinstance(body.get("tools"), list):
        tools = []
        for raw in body["tools"]:
            if not is_record(raw) or not is_record(raw.get("function")):
                continue
            fn = raw["function"]
            if not isinstance(fn.get("name"), str):
                continue
            tools.append({
                "name": fn["name"],
                "description": fn.get("description") if isinstance(fn.get("description"), str) else "",
                "input_schema": fn.get("parameters") if is_record(fn.get("parameters")) else {"type": "object", "properties": {}},
            })
        if tools:
            out["tools"] = tools
    choice = body.get("tool_choice")
    if choice == "auto":
        out["tool_choice"] = {"type": "auto"}
    elif choice == "required" or choice == "any":
        out["tool_choice"] = {"type": "any"}
    elif is_record(choice) and choice.get("type") == "function" and is_record(choice.get("function")):
        name = choice["function"].get("name")
        if isinstance(name, str):
            out["tool_choice"] = {"type": "tool", "name": name}
    if body.get("stream") is True:
        out["stream"] = True
    return out


# --- request translation: either wire -> gemini --------------------------

def _messages_to_gemini_parts(messages, api):
    """Translate client messages to Gemini contents; returns (system, contents)."""
    system_text = ""
    contents = []
    for raw in messages or []:
        if not is_record(raw):
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "system":
            text = text_of(content)
            if text != "":
                system_text = (system_text + "\n" + text) if system_text else text
            continue
        if api == "anthropic":
            parts = _anthropic_content_to_gemini_parts(content, role)
        else:
            parts = _openai_content_to_gemini_parts(content, role, raw)
        if parts:
            contents.append({"role": "model" if role == "assistant" else "user", "parts": parts})
    return system_text, contents


def _anthropic_content_to_gemini_parts(content, role):
    parts = []
    if isinstance(content, str):
        return [{"text": content}] if content != "" else []
    if not isinstance(content, list):
        return parts
    for block in content:
        if not is_record(block):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append({"text": block["text"]})
        elif block.get("type") == "image" and is_record(block.get("source")):
            source = block["source"]
            if source.get("type") == "base64" and isinstance(source.get("data"), str):
                parts.append({"inlineData": {
                    "mimeType": source.get("media_type") if isinstance(source.get("media_type"), str) else "image/png",
                    "data": source["data"]}})
            elif source.get("type") == "url" and isinstance(source.get("url"), str):
                parts.append({"fileData": {"fileUri": source["url"]}})
        elif block.get("type") == "tool_use":
            parts.append({"functionCall": {
                "name": block.get("name") if isinstance(block.get("name"), str) else "",
                "args": block.get("input") if is_record(block.get("input")) else {}}})
        elif block.get("type") == "tool_result":
            parts.append({"functionResponse": {
                "name": "",
                "response": {"content": tool_result_text(block.get("content"))}}})
    return parts


def _openai_content_to_gemini_parts(content, role, raw):
    parts = []
    if isinstance(content, str):
        if content != "":
            parts.append({"text": content})
    elif isinstance(content, list):
        for part in content:
            if not is_record(part):
                continue
            if part.get("type") == "text" and isinstance(part.get("text"), str):
                parts.append({"text": part["text"]})
            elif part.get("type") == "image_url" and is_record(part.get("image_url")):
                url = part["image_url"].get("url")
                if isinstance(url, str):
                    if url.startswith("data:"):
                        head, _, data = url[5:].partition(",")
                        media = head.split(";")[0] or "image/png"
                        parts.append({"inlineData": {"mimeType": media, "data": data}})
                    else:
                        parts.append({"fileData": {"fileUri": url}})
    if role == "assistant":
        for call in raw.get("tool_calls") or []:
            if not is_record(call) or not is_record(call.get("function")):
                continue
            fn = call["function"]
            parts.append({"functionCall": {
                "name": fn.get("name") if isinstance(fn.get("name"), str) else "",
                "args": arguments_of(fn.get("arguments"))}})
    if role == "tool":
        parts.append({"functionResponse": {
            "name": "",
            "response": {"content": tool_result_text(content)}}})
    return parts


def to_gemini(body, api, model):
    if api == "anthropic":
        system_text = text_of(body.get("system"))
        system_parts = [{"text": system_text}] if system_text != "" else []
        _, contents = _messages_to_gemini_parts(body.get("messages") or [], "anthropic")
        tools_in = body.get("tools") or []
        decls = []
        for raw in tools_in:
            if not is_record(raw) or not isinstance(raw.get("name"), str):
                continue
            decls.append({
                "name": raw["name"],
                "description": raw.get("description") if isinstance(raw.get("description"), str) else "",
                "parameters": raw.get("input_schema") if is_record(raw.get("input_schema")) else {"type": "object", "properties": {}},
            })
        stop = body.get("stop_sequences")
        max_tokens = body.get("max_tokens")
        temperature = body.get("temperature")
        top_p = body.get("top_p")
        fmt = None
    else:
        system_text, contents = _messages_to_gemini_parts(body.get("messages") or [], "openai")
        system_parts = [{"text": system_text}] if system_text != "" else []
        decls = []
        for raw in body.get("tools") or []:
            if not is_record(raw) or not is_record(raw.get("function")):
                continue
            fn = raw["function"]
            if not isinstance(fn.get("name"), str):
                continue
            decls.append({
                "name": fn["name"],
                "description": fn.get("description") if isinstance(fn.get("description"), str) else "",
                "parameters": fn.get("parameters") if is_record(fn.get("parameters")) else {"type": "object", "properties": {}},
            })
        stop = body.get("stop")
        if isinstance(stop, str):
            stop = [stop]
        max_tokens = body.get("max_tokens")
        if not isinstance(max_tokens, (int, float)):
            max_tokens = body.get("max_completion_tokens")
        temperature = body.get("temperature")
        top_p = body.get("top_p")
        fmt = body.get("response_format") if is_record(body.get("response_format")) else None
    out = {"contents": contents}
    if system_parts:
        out["systemInstruction"] = {"parts": system_parts}
    if decls:
        out["tools"] = [{"functionDeclarations": decls}]
    config = {}
    if isinstance(max_tokens, (int, float)):
        config["maxOutputTokens"] = max_tokens
    if isinstance(temperature, (int, float)):
        config["temperature"] = temperature
    if isinstance(top_p, (int, float)):
        config["topP"] = top_p
    if isinstance(stop, list) and stop:
        config["stopSequences"] = [s for s in stop if isinstance(s, str)]
    if fmt is not None and fmt.get("type") in ("json_object", "json_schema"):
        config["responseMimeType"] = "application/json"
    if config:
        out["generationConfig"] = config
    return out


# --- response translation -------------------------------------------------

def gemini_text_and_usage(answer):
    text = ""
    usage = {"input": 0, "output": 0}
    candidates = answer.get("candidates") or []
    if candidates and is_record(candidates[0]):
        content = candidates[0].get("content") or {}
        parts = content.get("parts") or []
        text = "".join(p.get("text", "") for p in parts if is_record(p) and isinstance(p.get("text"), str))
    meta = answer.get("usageMetadata") or {}
    usage = {"input": meta.get("promptTokenCount") or 0,
             "output": meta.get("candidatesTokenCount") or 0}
    return text, usage


def openai_json_from_answer(provider_wire, answer, model):
    """Provider JSON answer -> OpenAI Chat Completions JSON."""
    if provider_wire == "openai":
        return answer
    if provider_wire == "anthropic":
        text = text_of(answer.get("content"))
        usage = answer.get("usage") or {}
        return {
            "id": "chatcmpl-sim",
            "object": "chat.completion",
            "created": FIXED_CREATED,
            "model": model,
            "choices": [{"index": 0,
                         "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop", "logprobs": None}],
            "usage": {"prompt_tokens": usage.get("input_tokens") or 0,
                      "completion_tokens": usage.get("output_tokens") or 0,
                      "total_tokens": (usage.get("input_tokens") or 0) + (usage.get("output_tokens") or 0)},
        }
    text, usage = gemini_text_and_usage(answer)
    return {
        "id": "chatcmpl-sim",
        "object": "chat.completion",
        "created": FIXED_CREATED,
        "model": model,
        "choices": [{"index": 0,
                     "message": {"role": "assistant", "content": text},
                     "finish_reason": "stop", "logprobs": None}],
        "usage": {"prompt_tokens": usage["input"],
                  "completion_tokens": usage["output"],
                  "total_tokens": usage["input"] + usage["output"]},
    }


def anthropic_json_from_answer(provider_wire, answer, model):
    """Provider JSON answer -> Anthropic Messages JSON."""
    if provider_wire == "anthropic":
        return answer
    if provider_wire == "openai":
        choices = answer.get("choices") or []
        choice = choices[0] if choices and is_record(choices[0]) else {}
        message = choice.get("message") if is_record(choice.get("message")) else {}
        content = []
        text = message.get("content")
        if isinstance(text, str):
            text = text
        else:
            text = text_of(text)
        if text != "":
            content.append({"type": "text", "text": text})
        for call in message.get("tool_calls") or []:
            if not is_record(call) or not is_record(call.get("function")):
                continue
            fn = call["function"]
            content.append({
                "type": "tool_use",
                "id": call.get("id") if isinstance(call.get("id"), str) else "toolu_unknown",
                "name": fn.get("name") if isinstance(fn.get("name"), str) else "",
                "input": arguments_of(fn.get("arguments")),
            })
        if not content:
            content.append({"type": "text", "text": ""})
        usage = answer.get("usage") or {}
        return {
            "id": "msg_%s" % (answer.get("id") if isinstance(answer.get("id"), str) else "aggregate"),
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": content,
            "stop_reason": stop_reason_of(choice.get("finish_reason")),
            "stop_sequence": None,
            "usage": {"input_tokens": usage.get("prompt_tokens") or 0,
                      "output_tokens": usage.get("completion_tokens") or 0},
        }
    text, usage = gemini_text_and_usage(answer)
    return {
        "id": "msg_sim",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": usage["input"], "output_tokens": usage["output"]},
    }


# --- SSE parsing / translation --------------------------------------------

def parse_sse_frames(text):
    frames = []
    normalized = text.replace("\r\n", "\n")
    for frame in normalized.split("\n\n"):
        if frame.strip() == "":
            continue
        event = None
        data = []
        for line in frame.split("\n"):
            if line.startswith("event:"):
                event = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data.append(line[len("data:"):].lstrip(" "))
        if data:
            frames.append((event, "\n".join(data)))
    return frames


def openai_chunks_from_anthropic_sse(body_text):
    """Anthropic SSE -> OpenAI SSE chunks (text, usage, [DONE])."""
    text = ""
    input_tokens = 0
    output_tokens = 0
    for event, data in parse_sse_frames(body_text):
        try:
            payload = json.loads(data)
        except Exception:
            continue
        if not is_record(payload):
            continue
        if event == "message_start":
            msg = payload.get("message") or {}
            usage = msg.get("usage") or {}
            input_tokens = usage.get("input_tokens") or 0
        elif event == "content_block_delta":
            delta = payload.get("delta") or {}
            if delta.get("type") == "text_delta" and isinstance(delta.get("text"), str):
                text += delta["text"]
        elif event == "message_delta":
            usage = payload.get("usage") or {}
            output_tokens = usage.get("output_tokens") or 0
    return text, input_tokens, output_tokens


def openai_chunks_from_gemini_sse(body_text):
    """Gemini SSE -> (text, input_tokens, output_tokens)."""
    text = ""
    input_tokens = 0
    output_tokens = 0
    for event, data in parse_sse_frames(body_text):
        try:
            payload = json.loads(data)
        except Exception:
            continue
        if not is_record(payload):
            continue
        candidates = payload.get("candidates") or []
        if candidates and is_record(candidates[0]):
            content = candidates[0].get("content") or {}
            for part in content.get("parts") or []:
                if is_record(part) and isinstance(part.get("text"), str):
                    text += part["text"]
        meta = payload.get("usageMetadata") or {}
        if meta:
            input_tokens = meta.get("promptTokenCount") or 0
            output_tokens = meta.get("candidatesTokenCount") or 0
    return text, input_tokens, output_tokens


def anthropic_events_from_openai_sse(body_text, model):
    """OpenAI SSE -> Anthropic SSE event strings."""
    out = []
    started = False
    finished = False
    pending_stop = None
    text_index = -1
    next_index = 0
    input_tokens = 0
    output_tokens = 0

    def start_events():
        nonlocal started
        if started:
            return []
        started = True
        return [sse_event("message_start", {
            "type": "message_start",
            "message": {"id": "msg_stream", "type": "message", "role": "assistant",
                        "model": model, "content": [], "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": input_tokens, "output_tokens": 0}},
        })]

    def finish_events(stop_reason):
        nonlocal finished
        if finished:
            return []
        finished = True
        events = []
        if text_index != -1:
            events.append(sse_event("content_block_stop", {"type": "content_block_stop", "index": text_index}))
        events.append(sse_event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": stop_reason, "stop_sequence": None},
            "usage": {"output_tokens": output_tokens},
        }))
        events.append(sse_event("message_stop", {"type": "message_stop"}))
        return events

    for event, data in parse_sse_frames(body_text):
        payload = data.strip()
        if payload == "[DONE]":
            out.extend(start_events())
            out.extend(finish_events(pending_stop or "end_turn"))
            continue
        try:
            chunk = json.loads(payload)
        except Exception:
            continue
        if not is_record(chunk):
            continue
        if is_record(chunk.get("usage")):
            input_tokens = chunk["usage"].get("prompt_tokens") or 0
            output_tokens = chunk["usage"].get("completion_tokens") or 0
        choices = chunk.get("choices") or []
        choice = choices[0] if choices and is_record(choices[0]) else None
        if choice is None:
            continue
        delta = choice.get("delta") if is_record(choice.get("delta")) else {}
        out.extend(start_events())
        if isinstance(delta.get("content"), str) and delta["content"] != "":
            if text_index == -1:
                text_index = next_index
                next_index += 1
                out.append(sse_event("content_block_start", {
                    "type": "content_block_start", "index": text_index,
                    "content_block": {"type": "text", "text": ""},
                }))
            out.append(sse_event("content_block_delta", {
                "type": "content_block_delta", "index": text_index,
                "delta": {"type": "text_delta", "text": delta["content"]},
            }))
        if choice.get("finish_reason") is not None:
            pending_stop = stop_reason_of(choice.get("finish_reason"))
    if not finished:
        out.extend(start_events())
        out.extend(finish_events(pending_stop or "end_turn"))
    return "".join(out)


def anthropic_events_from_gemini_sse(body_text, model):
    """Gemini SSE -> Anthropic SSE event strings."""
    text, input_tokens, output_tokens = openai_chunks_from_gemini_sse(body_text)
    out = [sse_event("message_start", {
        "type": "message_start",
        "message": {"id": "msg_stream", "type": "message", "role": "assistant",
                    "model": model, "content": [], "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": input_tokens, "output_tokens": 0}},
    })]
    out.append(sse_event("content_block_start", {
        "type": "content_block_start", "index": 0,
        "content_block": {"type": "text", "text": ""},
    }))
    for piece in split_text(text):
        if piece != "":
            out.append(sse_event("content_block_delta", {
                "type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": piece},
            }))
    out.append(sse_event("content_block_stop", {"type": "content_block_stop", "index": 0}))
    out.append(sse_event("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": "end_turn", "stop_sequence": None},
        "usage": {"output_tokens": output_tokens},
    }))
    out.append(sse_event("message_stop", {"type": "message_stop"}))
    return "".join(out)


def openai_sse_from_text(text, input_tokens, output_tokens, model):
    """Plain text + usage -> OpenAI SSE stream."""
    parts = []
    chunk = lambda delta, finish: {
        "id": "chatcmpl-sim", "object": "chat.completion.chunk",
        "created": FIXED_CREATED, "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
    }
    parts.append("data: %s\n\n" % json.dumps(chunk({"role": "assistant", "content": ""}, None)))
    for piece in split_text(text):
        if piece != "":
            parts.append("data: %s\n\n" % json.dumps(chunk({"content": piece}, None)))
    parts.append("data: %s\n\n" % json.dumps(chunk({}, "stop")))
    parts.append("data: %s\n\n" % json.dumps({
        "id": "chatcmpl-sim", "object": "chat.completion.chunk",
        "created": FIXED_CREATED, "model": model, "choices": [],
        "usage": {"prompt_tokens": input_tokens, "completion_tokens": output_tokens,
                  "total_tokens": input_tokens + output_tokens},
    }))
    parts.append("data: [DONE]\n\n")
    return "".join(parts)


def anthropic_sse_from_text(text, input_tokens, output_tokens, model):
    """Plain text + usage -> Anthropic SSE stream."""
    out = [sse_event("message_start", {
        "type": "message_start",
        "message": {"id": "msg_stream", "type": "message", "role": "assistant",
                    "model": model, "content": [], "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": input_tokens, "output_tokens": 0}},
    })]
    out.append(sse_event("content_block_start", {
        "type": "content_block_start", "index": 0,
        "content_block": {"type": "text", "text": ""},
    }))
    for piece in split_text(text):
        if piece != "":
            out.append(sse_event("content_block_delta", {
                "type": "content_block_delta", "index": 0,
                "delta": {"type": "text_delta", "text": piece},
            }))
    out.append(sse_event("content_block_stop", {"type": "content_block_stop", "index": 0}))
    out.append(sse_event("message_delta", {
        "type": "message_delta",
        "delta": {"stop_reason": "end_turn", "stop_sequence": None},
        "usage": {"output_tokens": output_tokens},
    }))
    out.append(sse_event("message_stop", {"type": "message_stop"}))
    return "".join(out)


# --------------------------------------------------------------------------
# Upstream provider calls
# --------------------------------------------------------------------------

def call_provider(prov, model, wire, body, stream):
    """One call to a provider; returns (status, headers_dict, body_text)."""
    base = join_base(ENV_URL, prov["base"])
    headers = {"content-type": "application/json"}
    headers.update(auth_headers(prov["auth"], prov["key"]))
    if stream:
        headers["accept"] = "text/event-stream"
    if wire == "openai":
        path = base + "/v1/chat/completions"
        payload = dict(body)
        payload["model"] = model
        if stream:
            payload["stream"] = True
            payload["stream_options"] = {"include_usage": True}
        raw = json.dumps(payload)
    elif wire == "anthropic":
        path = base + "/v1/messages"
        headers["anthropic-version"] = ANTHROPIC_VERSION
        raw = json.dumps(body)
    else:
        action = "streamGenerateContent" if stream else "generateContent"
        path = "%s/v1beta/models/%s:%s" % (base, model, action)
        raw = json.dumps(body)
    parsed = _url_parts(path)
    conn = http.client.HTTPConnection(parsed["host"], parsed["port"], timeout=PROVIDER_TIMEOUT_S)
    try:
        conn.request("POST", parsed["path"], body=raw.encode("utf-8"), headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, dict(resp.getheaders()), data.decode("utf-8", "replace")
    finally:
        conn.close()


def _url_parts(url):
    rest = url.split("://", 1)[1]
    hostport, _, path = rest.partition("/")
    if ":" in hostport:
        host, port = hostport.rsplit(":", 1)
        port = int(port)
    else:
        host, port = hostport, 80
    return {"host": host, "port": port, "path": "/" + path}


def build_upstream_body(client_api, body, prov, model):
    """Translate the client body into the provider's wire; returns (wire, body)."""
    wire = prov["wire"]
    if client_api == "openai":
        if wire == "openai":
            out = dict(body)
            out["model"] = model
            return wire, out
        if wire == "anthropic":
            return wire, openai_to_anthropic(body, model)
        return wire, to_gemini(body, "openai", model)
    if wire == "anthropic":
        out = dict(body)
        out["model"] = model
        return wire, out
    if wire == "openai":
        return wire, anthropic_to_openai(body, model)
    return wire, to_gemini(body, "anthropic", model)


# --------------------------------------------------------------------------
# Server
# --------------------------------------------------------------------------

class Aggregator(object):
    def __init__(self, brief, catalog):
        self.brief = brief
        self.catalog = catalog
        self.router = Router(brief)
        self.manifest_index = 0
        self.free_ids = free_model_ids(catalog)

    def requirements_for(self, body, client_api):
        manifest = self.brief.manifest
        idx = self.manifest_index
        self.manifest_index += 1
        if idx < len(manifest):
            return manifest[idx]["requires"]
        return requirements_from_body(body, client_api)

    def serve(self, client_api, body):
        """Route one client request; returns (status, content_type, text)."""
        stream = body.get("stream") is True
        requires = self.requirements_for(body, client_api)
        candidates = self.router.candidates(requires)
        if not candidates:
            return self._error(client_api, 503, "no_free_model",
                               "no free model meets this request")
        # Two passes: every candidate once, then each once more. A scheduled
        # failure on the first pass can succeed on the second; a provider
        # that is out of budget fails both.
        for attempt in (0, 1):
            for prov, row in candidates:
                wire, upstream_body = build_upstream_body(client_api, body, prov, row["model"])
                try:
                    status, _, text = call_provider(prov, row["model"], wire, upstream_body, stream)
                except Exception as exc:
                    log("provider %s call failed: %s" % (prov["id"], exc))
                    self.router.note_failure(prov["id"])
                    continue
                if status < 200 or status >= 300:
                    self.router.note_failure(prov["id"])
                    continue
                self.router.note_success(prov["id"])
                return self._translate_response(client_api, wire, text, stream, row["model"])
        return self._error(client_api, 502, "upstream_unreachable",
                           "every provider refused this request")

    def _translate_response(self, client_api, provider_wire, text, stream, model):
        if not stream:
            try:
                answer = json.loads(text)
            except Exception:
                return self._error(client_api, 502, "bad_upstream", "the provider answered no JSON")
            if not is_record(answer):
                return self._error(client_api, 502, "bad_upstream", "the provider answered no JSON object")
            if client_api == "openai":
                out = openai_json_from_answer(provider_wire, answer, model)
            else:
                out = anthropic_json_from_answer(provider_wire, answer, model)
            return 200, "application/json", json.dumps(out)
        # Streaming: translate the provider's SSE into the client's wire.
        if client_api == "openai" and provider_wire == "openai":
            return 200, "text/event-stream; charset=utf-8", text
        if client_api == "anthropic" and provider_wire == "anthropic":
            return 200, "text/event-stream; charset=utf-8", text
        if client_api == "anthropic" and provider_wire == "openai":
            return 200, "text/event-stream; charset=utf-8", anthropic_events_from_openai_sse(text, model)
        if client_api == "openai" and provider_wire == "anthropic":
            body_text, in_tok, out_tok = openai_chunks_from_anthropic_sse(text)
            return 200, "text/event-stream; charset=utf-8", openai_sse_from_text(body_text, in_tok, out_tok, model)
        if client_api == "anthropic" and provider_wire == "gemini":
            return 200, "text/event-stream; charset=utf-8", anthropic_events_from_gemini_sse(text, model)
        body_text, in_tok, out_tok = openai_chunks_from_gemini_sse(text)
        return 200, "text/event-stream; charset=utf-8", openai_sse_from_text(body_text, in_tok, out_tok, model)

    def _error(self, client_api, status, type_name, message):
        if client_api == "anthropic":
            body = {"type": "error", "error": {"type": type_name, "message": message}}
        else:
            body = {"error": {"message": message, "type": type_name, "code": None}}
        return status, "application/json", json.dumps(body)

    def models_list(self, anthropic_shape):
        if anthropic_shape:
            return {"data": [{"type": "model", "id": mid, "display_name": mid,
                              "created_at": FIXED_ANTHROPIC_CREATED_AT}
                             for mid in self.free_ids],
                    "has_more": False}
        return {"object": "list",
                "data": [{"id": mid, "object": "model", "created": FIXED_CREATED,
                          "owned_by": "free-model-aggregator"}
                         for mid in self.free_ids]}


def requirements_from_body(body, client_api):
    vision = False
    tools = False
    json_mode = False
    if client_api == "openai":
        for raw in body.get("messages") or []:
            if not is_record(raw):
                continue
            content = raw.get("content")
            if isinstance(content, list):
                for block in content:
                    if is_record(block) and block.get("type") == "image_url":
                        vision = True
        tools = isinstance(body.get("tools"), list) and len(body["tools"]) > 0
        fmt = body.get("response_format")
        json_mode = is_record(fmt) and fmt.get("type") in ("json_object", "json_schema")
    else:
        for raw in body.get("messages") or []:
            if not is_record(raw):
                continue
            content = raw.get("content")
            if isinstance(content, list):
                for block in content:
                    if is_record(block) and block.get("type") == "image":
                        vision = True
        tools = isinstance(body.get("tools"), list) and len(body["tools"]) > 0
    return {"tools": tools, "vision": vision, "json": json_mode, "min_context": None}


class Handler(http.server.BaseHTTPRequestHandler):
    aggregator = None  # set by main()
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_BODY_BYTES:
            return None
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except Exception:
            return None
        return value if is_record(value) else None

    def _send(self, status, content_type, text):
        data = text.encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/v1/models" or self.path.startswith("/v1/models?"):
            anthropic_shape = isinstance(self.headers.get("x-api-key"), str)
            self._send(200, "application/json",
                       json.dumps(self.aggregator.models_list(anthropic_shape)))
            return
        self._send(404, "application/json",
                   json.dumps({"error": {"message": "no route GET %s" % self.path,
                                         "type": "not_found", "code": None}}))

    def do_POST(self):
        if self.path == "/v1/chat/completions":
            body = self._read_json()
            if body is None:
                status, ctype, text = self.aggregator._error(
                    "openai", 400, "invalid_request_error",
                    "the request body is not one JSON object")
                self._send(status, ctype, text)
                return
            status, ctype, text = self.aggregator.serve("openai", body)
            self._send(status, ctype, text)
            return
        if self.path == "/v1/messages":
            body = self._read_json()
            if body is None:
                status, ctype, text = self.aggregator._error(
                    "anthropic", 400, "invalid_request_error",
                    "the request body is not one JSON object")
                self._send(status, ctype, text)
                return
            status, ctype, text = self.aggregator.serve("anthropic", body)
            self._send(status, ctype, text)
            return
        self._send(404, "application/json",
                   json.dumps({"error": {"message": "no route POST %s" % self.path,
                                         "type": "not_found", "code": None}}))


def register(port, catalog):
    conn = http.client.HTTPConnection(_url_parts(ENV_URL)["host"],
                                     _url_parts(ENV_URL)["port"], timeout=5)
    try:
        payload = json.dumps({"protocol_version": PROTOCOL_VERSION,
                              "port": port, "catalog": catalog},
                             separators=(",", ":"))
        conn.request("POST", "/register", body=payload.encode("utf-8"),
                     headers={"content-type": "application/json"})
        resp = conn.getresponse()
        data = resp.read().decode("utf-8", "replace")
        if resp.status != 200:
            raise RuntimeError("POST /register answered HTTP %s: %s" % (resp.status, data[:500]))
        reply = json.loads(data)
        if isinstance(reply.get("requests"), int):
            log("registered: the workload holds %s requests" % reply["requests"])
    finally:
        conn.close()


def main():
    brief = read_brief(PROMPT_FILE)
    if brief.error is not None:
        log(brief.error)
    # Build the one catalog first: probes + ratings. --list prints this
    # same document; register sends it. Outside a trial (no env) the
    # probes fail and the catalog is {"providers":[]}.
    probes = probe_all(brief)
    catalog = build_catalog(brief, probes)
    if "--list" in sys.argv[1:]:
        text = json.dumps(catalog, separators=(",", ":")) + "\n"
        sys.stdout.write(text)
        sys.stdout.flush()
        sys.exit(0)
    if not ENV_URL:
        log("MAKEFUN_ENV_URL is not set")
        sys.exit(1)
    aggregator = Aggregator(brief, catalog)
    Handler.aggregator = aggregator
    server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), Handler)
    server.daemon_threads = True
    port = server.server_address[1]
    if port == 8080:
        raise RuntimeError("the OS picked 8080, which the env owns on one layout")
    log("listening on 0.0.0.0:%d with %d providers and %d free models"
        % (port, len(catalog["providers"]), len(aggregator.free_ids)))
    register(port, catalog)

    stop = threading.Event()

    def poll_work():
        while not stop.is_set():
            try:
                parts = _url_parts(ENV_URL)
                conn = http.client.HTTPConnection(parts["host"], parts["port"], timeout=2)
                try:
                    conn.request("GET", "/work")
                    resp = conn.getresponse()
                    data = resp.read().decode("utf-8", "replace")
                    if resp.status == 200:
                        work = json.loads(data)
                        if is_record(work) and work.get("done") is True:
                            stop.set()
                            server.shutdown()
                            return
                finally:
                    conn.close()
            except Exception:
                pass
            stop.wait(POLL_INTERVAL_S)

    threading.Thread(target=poll_work, daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    sys.exit(0)


if __name__ == "__main__":
    main()
