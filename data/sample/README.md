# Sample (synthetic)

10 L10 order-book snapshots with **made-up numbers**, 15s apart from 09:30 ET on 2026-06-23. They have the same columns, types and file naming as the real Databento day files described in [`../DATA.md`](../DATA.md). `sample.jsonl` has the same rows in readable form. Regenerate them with `uv run python scripts/make_sample.py`.

```bash
TRADE_JEV_DATA=data/sample uv run python -m trade_jev.run --days 2026-06-23 --policies hold,random,imbalance
```
