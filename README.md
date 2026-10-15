# Nifty-willy-quant-bot
Nifty 50 Options Trading Bot

An automated intraday trading system for Nifty 50 index options, built in Python on the DhanHQ API. It streams live index ticks, evaluates a multi-factor signal on 5-minute candles, picks the ATM option contract, and places a bracket (super) order with stop-loss, target and trailing jump.

Disclaimer: Educational personal project. Not financial advice. Trading derivatives involves substantial risk of loss.

Strategy

Signals are evaluated on 5-minute candles built from live ticks.

Condition	CALL (buy ATM CE)	PUT (buy ATM PE)
Trend (200 EMA)	Price above EMA	Price below EMA
Breakout	Price above previous candle high	Price below previous candle low
Momentum (Williams %R, 14)	Above -20	Below -80
Trend strength (ADX, 14)	Above 16	Above 16

Risk management

Stop-loss: 12% of option premium (minimum 8 points)
Target: 24% of premium (1:2 risk-reward)
Trailing jump: half of stop-loss distance
No new entries after 3:15 PM IST
Features
Async architecture (asyncio) with separate WebSocket streams for market data and order updates
Automatic nearest-expiry and ATM strike resolution, with offline security-master fallback
Bracket orders via DhanHQ super order API
Position state reset on order-update events (target/SL hit, rejection)
Structured logging
Tech stack

Python, pandas, pandas-ta, asyncio, WebSockets, DhanHQ API
