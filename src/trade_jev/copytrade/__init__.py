"""Copy trading: each account in accounts.yaml mirrors Jev's paper position changes (see README "Copy trading").

Paper trading stays the signal source and never places an order; this package is a separate daemon
(`python -m trade_jev.copytrade run`, launchd com.tradejev.copy) that the paper session posts its intents to.
"""
