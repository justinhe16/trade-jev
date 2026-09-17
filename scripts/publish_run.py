"""Publish a run's Jev answers and results to results/<run_id>/ (committed to git).

  uv run python scripts/publish_run.py runs/<id> [runs/<id> ...]

Market data is stripped so the output can be shared: no order books (the `state` Jev saw),
no mid prices, and trades keep only times, side, exit reason, points and P&L.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

from trade_jev.data import ns_to_et

ROOT = Path(__file__).resolve().parents[1]
ANSWER_FIELDS = ("day", "t_ns", "time_et", "raw_action", "probs", "action", "position", "tokens")
TRADE_FIELDS = ("day", "side", "entry_et", "exit_et", "held_s", "reason", "points", "pnl")


def publish(run_dir: Path) -> Path:
    jev_dir = next(run_dir.glob("jev*/"), None)
    if jev_dir is None:
        raise SystemExit(f"{run_dir} has no Jev policy — nothing to publish")
    out = ROOT / "results" / run_dir.name
    out.mkdir(parents=True, exist_ok=True)

    cfg = json.loads((run_dir / "config.json").read_text())
    res = json.loads((run_dir / "results.json").read_text())
    summary = json.loads((run_dir / "summary.json").read_text())
    (out / "run.json").write_text(json.dumps({
        "run_id": run_dir.name, "policy": jev_dir.name, "model": cfg.get("model"),
        "encoder": cfg.get("encoder"), "days": cfg["days"], "config": cfg["config"],
        "gate": cfg.get("gate"), "jev_api_calls": res.get("jev_api_calls"),
        "jev_cost_usd": res.get("jev_cost_usd"), "wall_time_s": res.get("wall_time_s"),
        "results": res.get("policies"),
        "metrics": summary,
    }, indent=2))

    n = 0
    with (jev_dir / "decisions.jsonl").open() as src, (out / "answers.jsonl").open("w") as dst:
        for line in src:
            d = json.loads(line)
            if d.get("raw_action") is None:  # runs from before gating stored the raw answer as `action`
                d["raw_action"] = d["action"]
            dst.write(json.dumps({k: d.get(k) for k in ANSWER_FIELDS}) + "\n")
            n += 1

    with (jev_dir / "trades.csv").open() as src, (out / "trades.csv").open("w", newline="") as dst:
        w = csv.DictWriter(dst, fieldnames=TRADE_FIELDS)
        w.writeheader()
        for t in csv.DictReader(src):
            entry, exit_ = int(t["entry_ns"]), int(t["exit_ns"])
            w.writerow({"day": t["day"], "side": "long" if int(t["side"]) > 0 else "short",
                        "entry_et": ns_to_et(entry), "exit_et": ns_to_et(exit_),
                        "held_s": round((exit_ - entry) / 1e9, 1), "reason": t["reason"],
                        "points": t["points"], "pnl": t["pnl"]})
    print(f"{run_dir.name}: {n:,} answers → {out}")
    return out


if __name__ == "__main__":
    for p in sys.argv[1:]:
        publish(Path(p))
