"""trade-jev: backtest Jev (TypeSafe) as a BUY/SELL/HOLD trader on NQ L10 data."""

from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")  # TYPESAFE_API_KEY, TRADE_JEV_DATA — for every entry point
