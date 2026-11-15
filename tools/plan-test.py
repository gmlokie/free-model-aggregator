"""Planner test: seed-derived plan = one contiguous safe run per provider
(longest safe run), min-cost max-flow assignment of requests to providers
maximising weighted rating. Checks quality >= 99 with exactly 20 attributed
calls on each practice trial."""
import json
import importlib.util
_spec = importlib.util.spec_from_file_location(
    'derive_draw', '/home/hatch/workspace/bounty-tools/makefun/derive-draw.py')
_dd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_dd)
draw_instance, PRACTICE_SEEDS = _dd.draw_instance, _dd.PRACTICE_SEEDS

KIT = '/home/hatch/workspace/bounty-tools/makefun/kit-free-model-aggregator-01/free-model-aggregator-01-kit'


def safe_runs(draw):
    """Maximal contiguous safe call-number runs per provider.
    Safe = k<=budget, k not in fail_calls, k < outage_from (if any)."""
    runs = {}
    for pid, p in draw['providers'].items():
        bad = set(p['fail_calls'])
        if p['outage_from'] is not None:
            bad |= set(range(p['outage_from'], 10 ** 6))
        bad |= set(range(p['budget'] + 1, 10 ** 6))
        cur, out = [], []
        for k in range(1, p['budget'] + 1):
            if k in bad:
                if cur:
                    out.append(tuple(cur))
                    cur = []
            else:
                cur.append(k)
        if cur:
            out.append(tuple(cur))
        runs[pid] = out
    return runs


def best_run(runs):
    """Longest run; tie-break earliest start (fewest sacrifices)."""
    return max(runs, key=lambda r: (len(r), -r[0]))


_spec2 = importlib.util.spec_from_file_location(
    'v1main', '/home/hatch/workspace/bounty-tools/makefun/entries/free-model-aggregator/main.py')
_v1 = importlib.util.module_from_spec(_spec2)
_spec2.loader.exec_module(_v1)


def load_brief():
    brief = _v1.read_brief(
        '%s/practice/trial-0/input/prompt.txt' % KIT)
    assert brief.error is None, brief.error
    return brief.providers, brief.ratings, brief.manifest, brief.best


def mcmf(requests, providers_caps, edges):
    """Min-cost max-flow: source->requests (1), requests->providers (cost),
    providers->sink (cap). edges: dict (ri, pi) -> cost. Returns assignment."""
    n_req = len(requests)
    pids = list(providers_caps)
    pidx = {p: i for i, p in enumerate(pids)}
    N = 2 + n_req + len(pids)
    SRC, SNK = 0, N - 1
    # adjacency: list of [to, cap, cost, rev_index]
    g = [[] for _ in range(N)]

    def add(u, v, cap, cost):
        g[u].append([v, cap, cost, len(g[v])])
        g[v].append([u, 0, -cost, len(g[u]) - 1])

    for i in range(n_req):
        add(SRC, 1 + i, 1, 0)
    for (ri, pi), cost in sorted(edges.items()):
        add(1 + ri, 1 + n_req + pidx[pi], 1, cost)
    for p, cap in providers_caps.items():
        add(1 + n_req + pidx[p], SNK, cap, 0)
    flow = cost = 0
    assign = {}
    while True:
        # Bellman-Ford (small graph, negative costs OK), deterministic order
        dist = [10 ** 18] * N
        prev = [None] * N
        dist[SRC] = 0
        for _ in range(N):
            changed = False
            for u in range(N):
                if dist[u] == 10 ** 18:
                    continue
                for ei, e in enumerate(g[u]):
                    if e[1] > 0 and dist[u] + e[2] < dist[e[0]]:
                        dist[e[0]] = dist[u] + e[2]
                        prev[e[0]] = (u, ei)
                        changed = True
            if not changed:
                break
        if dist[SNK] == 10 ** 18:
            break
        # augment 1 unit
        v = SNK
        while v != SRC:
            u, ei = prev[v]
            e = g[u][ei]
            e[1] -= 1
            g[v][e[3]][1] += 1
            if u != SRC and v != SNK and u >= 1 and u < 1 + n_req:
                assign[u - 1] = pids[v - 1 - n_req]
            v = u
        flow += 1
        cost += dist[SNK]
    return flow, cost, assign


def plan_and_grade(t):
    providers_doc, ratings, manifest, best = load_brief()
    draw = draw_instance(PRACTICE_SEEDS[t])
    runs = safe_runs(draw)
    caps = {}
    plan_runs = {}
    for pid, rs in runs.items():
        r = best_run(rs)
        caps[pid] = len(r)
        plan_runs[pid] = r
    # best (provider, model) per request
    best_model = {}  # (ri, pid) -> (rating, model)
    for ri, req in enumerate(manifest):
        for row in ratings:
            if not row['free']:
                continue
            rq = req['requires']
            if rq['tools'] and not row['tools']:
                continue
            if rq['vision'] and not row['vision']:
                continue
            if rq['json'] and not row['json']:
                continue
            if rq['min_context'] is not None and (
                    row['context_length'] is None or
                    row['context_length'] < rq['min_context']):
                continue
            key = (ri, row['provider'])
            if key not in best_model or row['rating_milli'] > best_model[key][0]:
                best_model[key] = (row['rating_milli'], row['model'])
    edges = {}
    for (ri, pid), (rating, model) in best_model.items():
        edges[(ri, pid)] = -manifest[ri]['weight'] * rating
    flow, cost, assign = mcmf(manifest, caps, edges)
    bound = sum(manifest[i]['weight'] * best[manifest[i]['id']] for i in range(len(manifest)))
    earned = -cost
    quality = (100 * earned) // bound
    total_sac = sum(r[0] - 1 for r in plan_runs.values() if r and any(
        assign.get(i) == pid for i in range(len(manifest))))
    print(f"trial {t}: flow={flow}/20 quality={quality} "
          f"earned={earned}/{bound} sacrifices={total_sac}")
    for i, req in enumerate(manifest):
        pid = assign.get(i)
        rating, model = best_model.get((i, pid), (0, '?'))
        print(f"  {req['id']} w{req['weight']} -> {pid} {model} r{rating} "
              f"(best {best[req['id']]})")
    return quality


if __name__ == '__main__':
    for t in range(3):
        q = plan_and_grade(t)
        print(f"  => trial {t} quality {q} {'PASS (>=99)' if q >= 99 else 'FAIL'}")
        print()
