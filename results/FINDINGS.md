# Findings

**Question:** can Jev (TypeSafe's contextual classifier) trade NQ futures from the raw order book, with no model training?

**Setup:** every 15s, Jev sees the raw L10 order book, recent prices, order flow and our position, and answers BUY, SELL or HOLD. A simple harness trades 1 contract with realistic fills and costs. The test covers 15 trading days (Jun 8–26 2026) and 22,401 Jev calls, and cost $0.91.

## 1. Acting on every answer: noise

Acted on directly, Jev's answers were mostly noise. It flips between BUY and SELL from one snapshot to the next, which is consistent with its documented weakness on raw numbers ("not a calculator"). Trading every answer loses $15–35k a day to spreads and commissions.

## 2. Filtering the answers: a signal emerges

We added simple settings on top of Jev's answers, with no extra model calls:
- **Confidence cutoff:** only act when Jev's probability is ≥ 0.7.
- **Agreeing answers:** only act after 4 same-side answers in a row (one minute).
- **Stop and target:** close at −200 or +100 ticks.

Replaying the stored answers under these settings produced a steady result:

| Setting | P&L (15 days) | Winning days | Worst day | Trades |
|---|---|---|---|---|
| **Jev + filters** (0.7 cutoff, 4 agreeing, 200 stop / 100 target) | **+$20,795** | **12 / 15** | **−$1,065** | 178 |
| Jev, every answer | −$128,590 | 3 / 15 | −$37,950 | 6,941 |
| Order-book imbalance rule | −$21,820 | 4 / 15 | −$10,390 | — |
| Random trades | −$11,230 | 6 / 15 | −$15,760 | — |

These filter settings were chosen from 1,920 tried on these same 15 days, and the table replays stored answers rather than a live run. Treat it as an interesting lead to test on new data, not a proven edge.
