from __future__ import annotations

from collections import Counter

import numpy as np

from trade_jev.harness import DayResult

JEV_USD_PER_TOKEN = 0.042 / 1e6


def summarize(results: list[DayResult]) -> dict:
    results = sorted(results, key=lambda r: r.day)
    trades = [t for r in results for t in r.trades]
    pnls = np.array([t.pnl for t in trades])
    daily = np.array([r.pnl for r in results])
    cum = np.cumsum(pnls) if len(pnls) else np.zeros(1)
    wins, losses = pnls[pnls > 0], pnls[pnls <= 0]
    decisions = [d for r in results for d in r.decisions]
    tokens = sum(d["tokens"] for d in decisions if not d["cached"])
    return {
        "days": len(results),
        "net_pnl": round(float(pnls.sum()), 2),
        "daily_pnl": {r.day: round(r.pnl, 2) for r in results},
        "trades": len(trades),
        "trades_per_day": round(len(trades) / max(len(results), 1), 1),
        "win_rate": round(float(len(wins) / len(pnls)), 3) if len(pnls) else None,
        "avg_win": round(float(wins.mean()), 2) if len(wins) else None,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else None,
        "avg_points": round(float(np.mean([t.points for t in trades])), 3) if trades else None,
        "sharpe_daily_ann": (round(float(daily.mean() / daily.std(ddof=1) * np.sqrt(252)), 2)
                             if len(daily) > 1 and daily.std(ddof=1) > 0 else None),
        "max_drawdown": round(float((np.maximum.accumulate(np.r_[0, cum]) - np.r_[0, cum]).max()), 2),
        "exit_reasons": dict(Counter(t.reason for t in trades)),
        "actions": dict(Counter(d["action"] for d in decisions)),
        "decisions": len(decisions),
        "api_calls": sum(1 for d in decisions if d["tokens"] and not d["cached"]),
        "avg_tokens": round(float(np.mean([d["tokens"] for d in decisions if d["tokens"]])), 1)
        if any(d["tokens"] for d in decisions) else None,
        "api_cost_usd": round(tokens * JEV_USD_PER_TOKEN, 4),
    }
