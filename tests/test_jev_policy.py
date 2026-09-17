import asyncio
from types import SimpleNamespace

from trade_jev.encode import Context, Position
from trade_jev.policies import JevPolicy, JsonlCache, RateLimiter
from tests.test_harness import S, make_day


class FakeClient:
    def __init__(self):
        self.calls = 0

    async def system_one(self, state, questions, model=None):
        self.calls += 1
        assert set(questions) == {"action"} and state["position"]["side"] == "flat"
        ans = SimpleNamespace(choice="BUY", probabilities={"BUY": 0.7, "SELL": 0.1, "HOLD": 0.2})
        return SimpleNamespace(choices={"action": ans}, usage=SimpleNamespace(input_tokens=500),
                               model="jev-1.13.0")


def test_jev_calls_once_then_serves_from_cache(tmp_path):
    day = make_day([400] * 80)
    ctx = Context(day, 70 * S, 70, {15: 55, 60: 10}, Position())
    client = FakeClient()

    async def go():
        p = JevPolicy(client, JsonlCache(tmp_path / "c.jsonl"), RateLimiter(100))
        d1 = await p(ctx)
        p2 = JevPolicy(client, JsonlCache(tmp_path / "c.jsonl"), RateLimiter(100))  # reload from disk
        d2 = await p2(ctx)
        return d1, d2

    d1, d2 = asyncio.run(go())
    assert client.calls == 1
    assert (d1.action, d1.cached, d1.tokens) == ("BUY", False, 500)
    assert (d2.action, d2.cached, d2.probs["BUY"]) == ("BUY", True, 0.7)


class Seq:
    name = "seq"

    def __init__(self, answers):
        self.answers = iter(answers)

    async def __call__(self, ctx):
        from trade_jev.policies import Decision
        a, p = next(self.answers)
        return Decision(a, {a: p})


def test_gated_needs_two_confident_agreeing_answers():
    from trade_jev.policies import Gated
    day = make_day([400] * 80)
    g = Gated(Seq([("BUY", 0.9), ("BUY", 0.7), ("BUY", 0.85), ("BUY", 0.95), ("SELL", 0.99)]), 0.8, 2)
    out = [asyncio.run(g(Context(day, i * S, i, {15: 0, 60: 0}, Position()))).action for i in range(5)]
    assert out == ["HOLD", "HOLD", "HOLD", "BUY", "HOLD"]
