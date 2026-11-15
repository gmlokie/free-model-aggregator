"""Reimplementation of the kit generator's Stream PRNG + drawInstance.

Source: program/generator/draw.mjs (kit, byte-for-byte as the harness seals it).
Stream: HMAC-SHA256(key=seed bytes, msg="<label>/<block>"), bytes consumed
sequentially; uint32 big-endian; int(lo,hi) by rejection over 32-bit words.
drawInstance: budgets, fail_calls (1-2 draws from [2, budget-1]), one
designated provider forced to include 2, one outage provider.
"""
import hashlib
import hmac
import json

PROVIDERS = [
    ('groq', 9, 12), ('cerebras', 5, 6), ('gemini', 6, 8), ('zai', 4, 5),
    ('openrouter', 10, 12), ('sambanova', 4, 5), ('pollinations', 10, 14),
    ('cloudflare', 7, 8), ('ovh', 5, 6), ('kilol', 12, 16),
]
IDS = [p[0] for p in PROVIDERS]


class Stream:
    def __init__(self, seed_hex, label):
        if len(seed_hex) != 64 or any(c not in '0123456789abcdef' for c in seed_hex):
            raise ValueError('seed must be 64 lowercase hex chars')
        self.key = bytes.fromhex(seed_hex)
        self.label = label
        self.block = 0
        self.buf = b''
        self.pos = 0

    def byte(self):
        if self.pos == len(self.buf):
            self.buf = hmac.new(
                self.key, ('%s/%d' % (self.label, self.block)).encode(),
                hashlib.sha256).digest()
            self.block += 1
            self.pos = 0
        b = self.buf[self.pos]
        self.pos += 1
        return b

    def uint32(self):
        return int.from_bytes(bytes([self.byte() for _ in range(4)]), 'big')

    def int(self, lo, hi):
        n = hi - lo + 1
        limit = (2 ** 32 // n) * n
        while True:
            w = self.uint32()
            if w < limit:
                return lo + (w % n)

    def pick(self, xs):
        return xs[self.int(0, len(xs) - 1)]


def draw_fail_calls(st, lo, hi, count):
    picked = set()
    while len(picked) < count:
        picked.add(st.int(lo, hi))
    return sorted(picked)


def draw_instance(seed_hex):
    second_call = Stream(seed_hex, 'failover/second-call').pick(IDS)
    outage = Stream(seed_hex, 'outage').pick(IDS)
    providers = {}
    for pid, cmin, cmax in PROVIDERS:
        budget = Stream(seed_hex, 'provider/%s/budget' % pid).int(cmin, cmax)
        count = Stream(seed_hex, 'provider/%s/fails/count' % pid).int(1, 2)
        fails = draw_fail_calls(
            Stream(seed_hex, 'provider/%s/fails' % pid), 2, budget - 1, count)
        providers[pid] = {'budget': budget, 'fail_calls': fails,
                          'outage_from': None}
    designated = providers[second_call]
    if 2 not in designated['fail_calls']:
        designated['fail_calls'] = sorted([2] + designated['fail_calls'][1:])
    down = providers[outage]
    down['outage_from'] = Stream(
        seed_hex, 'provider/%s/outage' % outage).int(2, down['budget'] - 1)
    return {'second_call': second_call, 'outage': outage, 'providers': providers}


PRACTICE_SEEDS = {
    0: '4f751e962e1eb1d5b461859662944e82c2ce329459e7bc6842a5ab35170c3f81',
    1: '01fc82338daa99de8447d0258d87cb2fc1f5eb386cc8b6237c3c86259b73e448',
    2: '21086a502f48ccc50a24c68de145b40cbb8cb4877b2c8c7d3cb2159c13248d55',
}


if __name__ == '__main__':
    for t, seed in PRACTICE_SEEDS.items():
        got = draw_instance(seed)
        env = json.load(open(
            '/home/hatch/workspace/bounty-tools/makefun/kit-free-model-aggregator-01/'
            'free-model-aggregator-01-kit/practice/trial-%d/input/env/providers.json' % t))
        ok = True
        for p in env['providers']:
            g = got['providers'][p['id']]
            match = (g['budget'] == p['budget'] and
                     g['fail_calls'] == sorted(p['fail_calls']) and
                     g['outage_from'] == p.get('outage_from'))
            if not match:
                ok = False
                print(f"trial {t} {p['id']}: MISMATCH got={g} want budget={p['budget']} "
                      f"fails={p['fail_calls']} outage={p.get('outage_from')}")
        print(f"trial {t}: {'EXACT MATCH' if ok else 'MISMATCH'} "
              f"(designated={got['second_call']}, outage={got['outage']})")
