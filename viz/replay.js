// Replay (browser/Node): a run's stored Jev answers + settings → trades, equity, stats.
// Mirrors trade_jev.policies.Gated and trade_jev.harness at 1-second price resolution:
// fills use the exact bid/ask at t + latency; stops/targets are checked per second using
// intra-second extremes; stops fill at the stop price (no gap slippage), so replays with tight stops
// read somewhat better than the Python harness. With stops off, results match it exactly.

function replay(day, params, consts) {
  const { minConf, agree, minHold, stop, target, commission } = params;
  const { tick, pointValue } = consts;
  const tr = day.track;
  const n = tr.bid.length;

  const trades = [];
  const gated = [];            // per decision: {action, pos (before)}
  const hist = [];
  let pos = null;              // {side, entry (ticks), entryT (s)}
  let watch = 0;               // first second not yet checked for stop/target
  let changes = 0;

  const close = (px, t, reason) => {
    const pts = (px - pos.entry) * pos.side * tick;
    trades.push({
      side: pos.side, entryT: pos.entryT, entryPx: pos.entry * tick,
      exitT: t, exitPx: px * tick, reason, points: pts,
      pnl: pts * pointValue - 2 * commission,
    });
    pos = null;
  };

  const checkExits = (upto) => {           // seconds [watch, upto]
    const lo = watch;
    watch = Math.max(watch, upto + 1);
    if (!pos || (!stop && !target)) return;
    for (let s = lo; s <= upto && s < n; s++) {
      if (pos.side > 0) {
        if (stop && tr.bid_lo[s] <= pos.entry - stop) return close(pos.entry - stop, s + 0.5, "stop");
        if (target && tr.bid_hi[s] >= pos.entry + target) return close(pos.entry + target, s + 0.5, "target");
      } else {
        if (stop && tr.ask_hi[s] >= pos.entry + stop) return close(pos.entry + stop, s + 0.5, "stop");
        if (target && tr.ask_lo[s] <= pos.entry - target) return close(pos.entry - target, s + 0.5, "target");
      }
    }
  };

  for (const d of day.decisions) {
    checkExits(Math.ceil(d.t) - 1);
    const before = pos ? pos.side : 0;
    const p = d.p[d.raw] ?? 0;
    hist.push(p >= minConf ? d.raw : null);
    const last = hist.slice(-agree);
    let action = "HOLD";
    if (last.length === agree && last.every((x) => x === last[0]) && (last[0] === "BUY" || last[0] === "SELL")) {
      action = last[0];
    }
    if (action !== "HOLD" && pos && d.t - pos.entryT < minHold) action = "HOLD";
    gated.push({ action, pos: before });

    const side = action === "BUY" ? 1 : action === "SELL" ? -1 : 0;
    if (side && side !== before) {
      const px = side > 0 ? d.fa : d.fb;
      if (pos) close(px, d.ft, "jev");
      pos = { side, entry: px, entryT: d.ft };
      watch = Math.floor(d.ft) + 1;
      changes++;
    }
  }
  checkExits(n - 1);
  if (pos) close(pos.side > 0 ? tr.bid[n - 1] : tr.ask[n - 1], n, "eod");

  // per-second equity: realized (net) + unrealized at mid
  const mid = new Float64Array(n);
  for (let s = 0; s < n; s++) mid[s] = (tr.bid[s] + tr.ask[s]) * tick / 2;
  const unreal = new Float64Array(n);
  const realizedStep = new Float64Array(n + 1);
  const position = new Int8Array(n);
  for (const t of trades) {
    const a = Math.min(n, Math.ceil(t.entryT));
    const b = Math.min(n, Math.ceil(t.exitT));
    for (let s = a; s < b; s++) {
      unreal[s] += (mid[s] - t.entryPx) * t.side * pointValue;
      position[s] = t.side;
    }
    realizedStep[b] += t.pnl;
  }
  const equity = new Float64Array(n);
  let realized = 0, peak = 0, maxDD = 0;
  for (let s = 0; s < n; s++) {
    realized += realizedStep[s];
    equity[s] = realized + unreal[s];
    peak = Math.max(peak, equity[s]);
    maxDD = Math.max(maxDD, peak - equity[s]);
  }

  const net = trades.reduce((a, t) => a + t.pnl, 0);
  const wins = trades.filter((t) => t.pnl > 0).length;
  const reasons = {};
  for (const t of trades) reasons[t.reason] = (reasons[t.reason] || 0) + 1;
  return {
    trades, gated, mid, equity, position,
    stats: {
      net, trades: trades.length, wins, winRate: trades.length ? wins / trades.length : null,
      changes, maxDD, reasons,
      avgHold: trades.length ? trades.reduce((a, t) => a + (t.exitT - t.entryT), 0) / trades.length : null,
    },
  };
}

if (typeof module !== "undefined") module.exports = { replay };
