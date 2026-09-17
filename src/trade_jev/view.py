"""View: a local web app to replay any mix of runs, days and settings.

  uv run python -m trade_jev.view            # → http://localhost:8765

Serves viz/index.html plus a small JSON API:
  GET /api/runs                  runs from runs/index.jsonl that have stored Jev answers
  GET /api/day?run=<id>&day=<d>  one day's price track + stored answers (built once, cached)
"""

from __future__ import annotations

import argparse
import gzip
import json
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import numpy as np

from trade_jev.data import TICK, POINT_VALUE, secs
from trade_jev.replay import RunDay, load_market, load_runs

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / "runs"
VIZ = ROOT / "viz"
CACHE = ROOT / "cache" / "view"
_build_lock = threading.Lock()


# ---------------------------------------------------------------- data

def list_runs() -> list[dict]:
    """Viewable runs (newest first): indexed runs whose Jev answers are on disk."""
    out = []
    index = RUNS / "index.jsonl"
    if not index.exists():
        return out
    for line in index.read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        rd = RUNS / rec["run_id"]
        if not rd.is_dir() or not any(rd.glob("jev*/decisions.jsonl")):
            continue
        meta = json.loads((rd / "config.json").read_text())
        jev = next((v for k, v in rec["policies"].items() if k.startswith("jev")), {})
        out.append({
            "run_id": rec["run_id"],
            "finished_at": rec.get("finished_at"),
            "days": rec["days"],
            "config": meta["config"],
            "gate": meta.get("gate") or {"min_conf": 0, "agree": 1, "min_hold_s": 0},
            "model": rec.get("model") or meta.get("model"),
            "jev_calls": rec.get("jev_api_calls", 0) + rec.get("jev_cache_hits", 0),
            "jev_pnl": jev.get("net_pnl"),
            "baselines": {k: v["net_pnl"] for k, v in rec["policies"].items() if not k.startswith("jev")},
        })
    return out[::-1]


def second_track(day, t0: int, t1: int) -> dict:
    """Per-second last/min/max of best bid & ask (ticks) over [t0, t1)."""
    n = int((t1 - t0) // secs(1))
    edges = t0 + np.arange(n + 1, dtype=np.int64) * secs(1)
    starts = np.searchsorted(day.ts, edges, side="left")
    lo_i, hi_i = starts[:-1], starts[1:]
    empty = lo_i == hi_i
    last = np.maximum(hi_i - 1, 0)  # last row at or before each second's end

    def ext(arr, fn):
        # reduceat reduces over [idx[k], idx[k+1]); empty seconds keep the prevailing touch
        idx = np.minimum(np.r_[lo_i, hi_i[-1]], len(arr) - 1)
        return np.where(empty, arr[last], fn.reduceat(arr, idx)[:-1])

    bid, ask = day.bid, day.ask
    return {
        "bid": bid[last].tolist(), "ask": ask[last].tolist(),
        "bid_lo": ext(bid, np.minimum).tolist(), "bid_hi": ext(bid, np.maximum).tolist(),
        "ask_lo": ext(ask, np.minimum).tolist(), "ask_hi": ext(ask, np.maximum).tolist(),
    }


def build_day(run_id: str, rd: RunDay) -> dict:
    """Everything the page needs to replay one day of one run."""
    cfg = rd.config
    decs = sorted(rd.answers.values(), key=lambda d: d["t_ns"])
    day, grid = load_market(rd)
    t0, t1 = int(grid[0]), int(grid[-1]) + secs(cfg.cadence_s)
    fills = day.row_at(np.array([d["t_ns"] for d in decs]) + secs(cfg.latency_ms / 1000))  # as the harness
    return {
        "run_id": run_id, "day": rd.day, "symbol": day.symbol, "t0_ns": t0,
        "tick": TICK, "point_value": POINT_VALUE, "commission": cfg.commission,
        "track": second_track(day, t0, t1),
        "decisions": [{
            "t": round((d["t_ns"] - t0) / 1e9, 3),
            "et": d["time_et"],
            "raw": d.get("raw_action") or d["action"],
            "p": d["probs"],
            "fb": int(day.bid[f]), "fa": int(day.ask[f]),
            "ft": round((int(day.ts[f]) - t0) / 1e9, 3),
            "state": d.get("state"),
        } for d, f in zip(decs, fills)],
    }


def day_json_gz(run_id: str, day: str) -> bytes:
    """Gzipped day payload, cached on disk and rebuilt if the run's answers change."""
    rd_dir = RUNS / run_id
    dec_file = next(rd_dir.glob("jev*/decisions.jsonl"))
    stamp = int(dec_file.stat().st_mtime)
    path = CACHE / f"{run_id}__{day}__{stamp}.json.gz"
    with _build_lock:
        if not path.exists():
            rd = load_runs([rd_dir])[day]
            CACHE.mkdir(parents=True, exist_ok=True)
            data = json.dumps(build_day(run_id, rd), separators=(",", ":")).encode()
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(gzip.compress(data, 5))
            tmp.rename(path)
    return path.read_bytes()


# ---------------------------------------------------------------- http

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quiet, but show API errors
        if args and str(args[1]).startswith(("4", "5")):
            super().log_message(fmt, *args)

    def _send(self, body: bytes, ctype: str, gz: bool = False, status=HTTPStatus.OK):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-store")
        if gz:
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, msg: str):
        self._send(json.dumps({"error": msg}).encode(), "application/json", status=status)

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        try:
            if url.path in ("/", "/index.html"):
                self._send((VIZ / "index.html").read_bytes(), "text/html; charset=utf-8")
            elif url.path == "/replay.js":
                self._send((VIZ / "replay.js").read_bytes(), "text/javascript; charset=utf-8")
            elif url.path == "/api/runs":
                self._send(json.dumps(list_runs()).encode(), "application/json")
            elif url.path == "/api/day":
                run_id, day = q.get("run", ""), q.get("day", "")
                if "/" in run_id or ".." in run_id or not (RUNS / run_id).is_dir():
                    return self._error(HTTPStatus.NOT_FOUND, f"unknown run {run_id!r}")
                self._send(day_json_gz(run_id, day), "application/json", gz=True)
            else:
                self._error(HTTPStatus.NOT_FOUND, "not found")
        except KeyError as e:
            self._error(HTTPStatus.NOT_FOUND, f"no stored answers for {e}")
        except BrokenPipeError:
            pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay any mix of runs, days and settings in the browser.")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--no-open", action="store_true", help="don't open a browser tab")
    args = ap.parse_args()
    srv = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://localhost:{args.port}"
    print(f"viewer → {url}  ({len(list_runs())} runs)  ctrl-c to stop", flush=True)
    if not args.no_open:
        webbrowser.open(url)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
