// Paper tab: daily live paper sessions from the paper DB (GET /api/paper/sessions, /api/paper/session?id=).
const $ = (id) => document.getElementById(id);
const S = { list: null, sel: null, detail: null, tpol: null, dfilter: "all", timer: null };
try { S.dfilter = localStorage.getItem("jev-paper-dfilter") || "all"; } catch (e) {}
const fmtPx = (v) => v == null ? "–" : v.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmtUsd = (v) => v == null || isNaN(v) ? "–" : (v > 0 ? "+" : v < 0 ? "−" : "") + "$" + Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: 0 });
const cls = (v) => v > 0 ? "pos" : v < 0 ? "neg" : "";
const esc = (s) => String(s ?? "").replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const etTime = (ns) => ns == null ? "–" : new Date(ns / 1e6).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour12: false });
const isJev = (n) => n.startsWith("jev");
const col = (n) => isJev(n) ? "var(--p1)" : { random: "var(--neutral)", imbalance: "var(--p4)", hold: "var(--axis)" }[n] || "var(--p2)";
function ago(iso) {
  if (!iso) return "–";
  const s = Math.max(0, Math.round((Date.now() - Date.parse(iso)) / 1000));
  return s < 90 ? `${s}s ago` : s < 5400 ? `${Math.round(s / 60)}m ago` : s < 129600 ? `${Math.round(s / 3600)}h ago` : `${Math.round(s / 86400)}d ago`;
}
const noSession = (s) => s.status === "holiday" || s.status === "skipped";  // the runner chose not to trade
function statusOf(s) {
  if (s.status === "skipped") return ["skipped", "skipped", s.error || "skipped"];
  if (s.status === "running" && s.stale) return ["dead?", "dead", "running, but no heartbeat since " + ago(s.heartbeat_at)];
  return [s.status, s.status, s.status === "running" ? "heartbeat " + ago(s.heartbeat_at) : ""];
}
const jevOf = (s) => Object.keys(s.policies || {}).find(isJev);

// ------------------------------------------------------------------ list
function renderList() {
  const L = S.list; if (!L) return;
  $("dbpath").textContent = L.db;
  const ss = L.sessions;
  $("idle").hidden = ss.length > 0; $("app").hidden = !ss.length;
  const running = ss.filter((s) => s.status === "running" && !s.stale).length;
  const dead = ss.filter((s) => s.status === "running" && s.stale).length;
  const el = $("status");
  el.textContent = dead ? `${dead} dead?` : running ? "LIVE" : "idle";
  el.className = "pill " + (dead ? "warn" : running ? "ok" : "");
  $("meta").textContent = L.db;
  if (!ss.length) return;
  renderTiles(ss); renderSessions(ss);
}
function renderTiles(ss) {
  const traded = ss.filter((s) => !noSession(s) && Object.keys(s.policies).length);
  const names = [...new Set(traded.flatMap((s) => Object.keys(s.policies)))].sort((a, b) => isJev(b) - isJev(a));
  const total = (n) => traded.reduce((a, s) => a + (s.policies[n]?.pnl || 0), 0);
  const jev = names.find(isJev);
  const jevDays = traded.filter((s) => jev && s.policies[jev]);
  const won = jevDays.filter((s) => s.policies[jev].pnl > 0).length;
  const cost = traded.reduce((a, s) => a + (s.policies[jev]?.jev_cost_usd || 0), 0);
  const items = [["Days traded", traded.length, `${ss.filter(noSession).length} skipped (holiday / weekend / feed off or credentials missing)`]];
  if (jev) items.push(["Jev P&L", `<span class="${cls(total(jev))}">${fmtUsd(total(jev))}</span>`, `${won} of ${jevDays.length} days positive`]);
  for (const n of names.filter((n) => !isJev(n))) items.push([n, `<span class="${cls(total(n))}">${fmtUsd(total(n))}</span>`, "baseline"]);
  if (jev) items.push(["Jev cost", "$" + cost.toFixed(3), "API spend, finished days"]);
  $("tiles").innerHTML = items.map(([k, v, s]) => `<div><div class="k">${esc(k)}</div><div class="v">${v}</div><div class="s">${esc(s)}</div></div>`).join("");
}
function renderSessions(ss) {
  const base = [...new Set(ss.flatMap((s) => Object.keys(s.policies)).filter((n) => !isJev(n)))].sort();
  $("sessions").innerHTML = `<thead><tr><th>Day</th><th>Status</th><th>Source</th><th>Jev P&amp;L</th>${base.map((n) => `<th>${esc(n)}</th>`).join("")}
    <th>Jev trades</th><th>Wins</th><th>Max DD</th><th>Jev cost</th><th>Session</th></tr></thead><tbody>` +
    ss.map((s) => {
      const [label, c, title] = statusOf(s), j = jevOf(s), jp = j ? s.policies[j] : null;
      const cell = (p) => p ? `<td class="${cls(p.pnl)}">${fmtUsd(p.pnl)}${s.final ? "" : "*"}</td>` : "<td>–</td>";
      return `<tr class="click ${S.sel === s.session_id ? "sel" : ""}" data-id="${esc(s.session_id)}"><td>${esc(s.day)}</td>
        <td><span class="st ${c}" title="${esc(title)}">${esc(label)}</span>${s.error && !noSession(s) ? " ⚠" : ""}</td>
        <td class="muted">${esc(s.source || "–")}</td>
        ${cell(jp)}${base.map((n) => cell(s.policies[n])).join("")}
        <td>${jp ? jp.trades : "–"}</td><td>${jp ? jp.wins : "–"}</td><td>${jp ? fmtUsd(-jp.max_drawdown) : "–"}</td>
        <td>${jp?.jev_cost_usd != null ? "$" + jp.jev_cost_usd.toFixed(3) : "–"}</td>
        <td style="text-align:left" class="muted">${esc(noSession(s) ? s.error : s.session_id)}</td></tr>`;
    }).join("") + `</tbody><tfoot><tr><td colspan="${9 + base.length}" class="note" style="text-align:left">* still running: totals from the trades closed so far.</td></tr></tfoot>`;
}
$("sessions").addEventListener("click", (e) => {
  const tr = e.target.closest("tr[data-id]"); if (!tr) return;
  select(tr.dataset.id);
});

// ------------------------------------------------------------------ detail
async function select(id) {
  if (S.sel !== id) S.tpol = null;
  S.sel = id;
  const u = new URL(location); u.searchParams.set("id", id); history.replaceState(null, "", u);
  renderList();
  await loadDetail();
}
async function loadDetail() {
  if (!S.sel) return;
  const r = await fetch("/api/paper/session?id=" + encodeURIComponent(S.sel));
  if (!r.ok) { $("detail").hidden = true; return; }
  S.detail = await r.json();
  renderDetail();
}
function renderDetail() {
  const d = S.detail; if (!d) return;
  $("detail").hidden = false;
  const [label, , title] = statusOf(d);
  $("dtitle").innerHTML = `${esc(d.day)} <span class="sub">· ${esc(d.symbol || "")} · ${esc(label)}${title ? " (" + esc(title) + ")" : ""}</span>`;
  $("dmeta").textContent = [d.session_id, d.source && "source " + d.source, d.run_dir, d.jev_model && "model " + d.jev_model,
    d.started_at && "started " + new Date(d.started_at).toLocaleString(), d.ended_at && "ended " + new Date(d.ended_at).toLocaleString()].filter(Boolean).join(" · ");
  $("derror").hidden = !d.error || noSession(d); $("derror").textContent = d.error || "";
  const names = Object.keys(d.policies).sort((a, b) => isJev(b) - isJev(a) || a.localeCompare(b));
  $("dpolicies").innerHTML = names.length ? `<thead><tr><th>Policy</th><th>P&amp;L</th><th>Trades</th><th>Wins</th><th>Win %</th><th>Max DD</th><th>Jev calls</th><th>Jev cost</th></tr></thead><tbody>` +
    names.map((n) => {
      const p = d.policies[n];
      return `<tr><td><span class="sw" style="background:${col(n)}"></span>${esc(n)}</td><td class="${cls(p.pnl)}"><b>${fmtUsd(p.pnl)}</b></td><td>${p.trades}</td><td>${p.wins}</td>
        <td>${p.trades ? Math.round(100 * p.wins / p.trades) + "%" : "–"}</td><td>${fmtUsd(-p.max_drawdown)}</td>
        <td>${isJev(n) && p.jev_calls != null ? p.jev_calls.toLocaleString() : "–"}</td><td>${isJev(n) && p.jev_cost_usd != null ? "$" + p.jev_cost_usd.toFixed(3) : "–"}</td></tr>`;
    }).join("") + "</tbody>" + (d.final ? "" : `<tfoot><tr><td colspan="8" class="note" style="text-align:left">Running: totals from trades closed so far; Jev calls and cost are written at the session end.</td></tr></tfoot>`)
    : `<tbody><tr><td class="empty">${noSession(d) ? (d.status === "skipped" ? "Skipped: " : "No session: ") + esc(d.error) : "No trades yet."}</td></tr></tbody>`;
  const st = d.settings;
  $("dsettings").textContent = st ? `Settings: confidence ≥ ${st.min_conf}, ${st.agree} agreeing, ${st.min_hold_s}s min hold, stop ${st.stop_ticks || "off"} / target ${st.target_ticks || "off"} ticks, every ${st.cadence_s}s from ${st.start_et} to ${st.end_et} ET, ${st.latency_ms} ms latency, $${st.commission}/side.` : "";
  renderTrades(d, names); renderDecs(d);
}
function renderTrades(d, names) {
  const pols = [...new Set([...names, ...d.trades.map((t) => t.policy)])];
  if (!S.tpol || !pols.includes(S.tpol)) S.tpol = pols.find(isJev) || pols[0] || null;
  $("tpols").innerHTML = pols.map((n) => `<button class="btn" data-p="${esc(n)}" aria-pressed="${n === S.tpol}">${esc(n)}</button>`).join("");
  const tr = d.trades.filter((t) => t.policy === S.tpol).reverse();
  $("dtrades").innerHTML = tr.length ? `<thead><tr><th>Side</th><th>Entry ET</th><th>Entry</th><th>Exit ET</th><th>Exit</th><th>Why</th><th>Points</th><th>P&amp;L</th></tr></thead><tbody>` +
    tr.map((t) => `<tr><td>${esc(t.side)}</td><td>${etTime(t.entry_ns)}</td><td>${fmtPx(t.entry_px)}</td><td>${etTime(t.exit_ns)}</td><td>${fmtPx(t.exit_px)}</td><td>${esc(t.reason)}</td><td>${t.points.toFixed(2)}</td><td class="${cls(t.pnl)}">${fmtUsd(t.pnl)}</td></tr>`).join("") + "</tbody>"
    : `<tbody><tr><td class="empty">No closed trades${S.tpol ? " for " + esc(S.tpol) : ""}.</td></tr></tbody>`;
}
$("tpols").addEventListener("click", (e) => {
  const b = e.target.closest("button[data-p]"); if (!b) return;
  S.tpol = b.dataset.p; renderDetail();
});
function renderDecs(d) {
  const all = d.decisions;
  const pick = { all: () => true, acted: (x) => x.action !== "HOLD", errors: (x) => !!x.error }[S.dfilter] || (() => true);
  const rows = all.filter(pick).reverse();
  $("dcount").textContent = `· ${all.length.toLocaleString()} answers${all.length ? `, ${all.filter((x) => x.action !== "HOLD").length} acted on, ${all.filter((x) => x.error).length} errors${all.some((x) => x.raw_action === "WARMUP") ? `, ${all.filter((x) => x.raw_action === "WARMUP").length} warm-up` : ""}` : ""}`;
  $("dfilter").innerHTML = [["all", "All"], ["acted", "Acted on"], ["errors", "Errors"]]
    .map(([k, l]) => `<button class="btn" data-f="${k}" aria-pressed="${S.dfilter === k}">${l}</button>`).join("");
  $("ddecs").innerHTML = rows.length ? `<thead><tr><th>Time ET</th><th>Mid</th><th>Answer</th><th>p</th><th>Acted</th><th>Position</th><th>ms</th><th>Error</th></tr></thead><tbody>` +
    rows.map((x) => {
      const raw = x.raw_action || x.action, p = x.probs?.[raw];
      const warm = raw === "WARMUP";  // Tradovate late start: HOLD without a Jev call, not an error
      return `<tr><td>${esc(x.time_et)}</td><td>${fmtPx(x.mid)}</td><td${warm ? ' class="muted" title="late-start warm-up: no Jev call"' : ""}>${warm ? "warmup" : esc(raw)}</td><td>${p != null ? (p * 100).toFixed(0) + "%" : "–"}</td>
        <td>${x.action !== "HOLD" ? "<b>" + esc(x.action) + "</b>" : "–"}</td><td>${x.position > 0 ? "long" : x.position < 0 ? "short" : "flat"}</td>
        <td>${x.jev_ms != null ? Math.round(x.jev_ms) : "–"}</td><td style="text-align:left" class="muted">${esc(x.error || "")}</td></tr>`;
    }).join("") + "</tbody>"
    : `<tbody><tr><td class="empty">${all.length ? "No decisions match." : "No Jev decisions recorded."}</td></tr></tbody>`;
}
$("dfilter").addEventListener("click", (e) => {
  const b = e.target.closest("button[data-f]"); if (!b) return;
  S.dfilter = b.dataset.f; try { localStorage.setItem("jev-paper-dfilter", S.dfilter); } catch (e) {}
  renderDecs(S.detail);
});

// ------------------------------------------------------------------ data
async function refresh() {
  try {
    S.list = await (await fetch("/api/paper/sessions")).json();
    renderList();
    const sel = S.list.sessions.find((s) => s.session_id === S.sel);
    if (sel && (sel.status === "running" || !S.detail || S.detail.status !== sel.status)) await loadDetail();
  } catch (e) {
    $("status").textContent = "DISCONNECTED"; $("status").className = "pill warn";
  }
  const live = S.list?.sessions.some((s) => s.status === "running" && !s.stale);
  clearTimeout(S.timer);
  S.timer = setTimeout(refresh, live ? 5000 : 60000);  // running sessions show live
}
function paintTheme() {
  const dark = document.documentElement.dataset.theme === "dark";
  $("theme").textContent = dark ? "☀ Light" : "☾ Dark";
}
$("theme").onclick = () => {
  const next = document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = next;
  try { localStorage.setItem("jev-viewer-theme", next); } catch (e) {}
  paintTheme();
};
paintTheme();
S.sel = new URL(location).searchParams.get("id");
refresh().then(() => { if (!S.sel && S.list?.sessions.length) select(S.list.sessions[0].session_id); });
