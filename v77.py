import os
import time
import socket
import asyncio
import logging
import warnings
import pandas as pd
import pandas_ta as ta
from datetime import datetime, timedelta, time as dt_time
from dotenv import load_dotenv

# Suppress CSV DtypeWarning when parsing exchange master files
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# Structured Institutional Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NSEQuantEngine")

# DhanHQ v2.1.0 Architecture Imports
from dhanhq import dhanhq, DhanContext
from dhanhq import MarketFeed, OrderUpdate

# IPv4 Socket Patch for deterministic WebSocket connectivity
original_getaddrinfo = socket.getaddrinfo

def ipv4_only_getaddrinfo(*args, **kwargs):
    responses = original_getaddrinfo(*args, **kwargs)
    return [res for res in responses if res[0] == socket.AF_INET]

socket.getaddrinfo = ipv4_only_getaddrinfo

# ==========================================
# 1. CREDENTIALS & CONTEXT INITIALIZATION
# ==========================================
load_dotenv()

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")

if not CLIENT_ID or not ACCESS_TOKEN:
    raise ValueError("Missing API credentials. Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN in .env")

dhan_context = DhanContext(CLIENT_ID, ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# ==========================================
# 2. ENGINE CONFIGURATION & PARAMETERS
# ==========================================
NIFTY_SECURITY_ID = 13
CANDLE_INTERVAL_SECONDS = 300  # 5-Minute OHLC Aggregation
NUM_LOTS = 1
DEFAULT_LOT_SIZE = 65          # NSE Nifty 50 standard lot size
QUANTITY = NUM_LOTS * DEFAULT_LOT_SIZE

# Quantitative Regime & Oscillator Parameters
EMA_PERIOD = 200
WILLR_PERIOD = 14
ADX_PERIOD = 14
ADX_THRESHOLD = 16.0

# Corrected Williams %R Thresholds (-20 = Top 20% Thrust, -80 = Bottom 20% Thrust)
WILLR_UPPER_BAND = -20.0
WILLR_LOWER_BAND = -80.0

# Dynamic Option Risk-Bracket Parameters (Percentage of Option Premium)
SL_PCT = 0.12                  # 12% Stop-Loss
TARGET_PCT = 0.24              # 24% Target (1:2 Risk-to-Reward)
MIN_SL_POINTS = 8.0            # Minimum point floor for low-premium expiry days

# NSE Session Boundaries
MARKET_OPEN_TIME = dt_time(9, 15)
NO_NEW_ENTRY_TIME = dt_time(15, 15)
MARKET_CLOSE_TIME = dt_time(15, 30)

# Runtime State
live_market_data = {}
feed_instance = None
cached_nearest_expiry = None
active_order_id = None
active_sec_id = None

logger.info("Downloading NSE Security Master for offline contract resolution...")
try:
    security_df = dhan.fetch_security_list(mode='compact')
    logger.info(f"Loaded {len(security_df)} exchange instruments into memory.")
except Exception as e:
    logger.warning(f"Could not fetch offline security master: {e}")
    security_df = None


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    return round(round(price / tick_size) * tick_size, 2)


# ==========================================
# 3. ASYNCHRONOUS WEBSOCKET STREAMS
# ==========================================
async def market_data_stream():
    global feed_instance
    logger.info("Initializing Market Data WebSocket Stream (v2.1.0)...")

    raw_instruments = [(dhanhq.INDEX, str(NIFTY_SECURITY_ID), MarketFeed.Ticker)]
    feed_instance = MarketFeed(dhan_context, raw_instruments, version="v2")
    await feed_instance.connect()

    if hasattr(feed_instance, 'authorize'):
        await feed_instance.authorize()

    await feed_instance.subscribe_instruments()
    logger.info("Market WebSocket connected. Streaming Nifty 50 index ticks.")

    try:
        while True:
            data = await feed_instance.get_instrument_data()
            if data and isinstance(data, dict):
                sec_id = str(data.get('security_id', data.get('securityId', '')))
                ltp = data.get('last_price', data.get('LTP', data.get('lastPrice', 0)))

                if sec_id and ltp and float(ltp) > 0:
                    live_market_data[sec_id] = float(ltp)
            await asyncio.sleep(0.001)
    except Exception as e:
        logger.error(f"Market WebSocket error: {e}")
    finally:
        if feed_instance:
            await feed_instance.disconnect()


def on_order_update(order_data: dict):
    global active_order_id, active_sec_id
    try:
        data = order_data.get("Data", order_data) if isinstance(order_data, dict) else {}
        if not data:
            return

        order_id = str(data.get('orderId', data.get('order_id', '')))
        order_status = str(data.get('orderStatus', data.get('order_status', data.get('status', '')))).upper().strip()
        transaction_type = str(data.get('transactionType', data.get('transaction_type', ''))).upper().strip()
        sec_id = str(data.get('securityId', data.get('security_id', '')))

        if not order_id:
            return

        if active_sec_id and sec_id == str(active_sec_id):
            if transaction_type == "SELL" and order_status in ["TRADED", "EXECUTED"]:
                logger.info(f"Target/SL bracket leg executed via WebSocket for security {sec_id}. Position flat.")
                active_order_id = None
                active_sec_id = None
            elif order_status in ["REJECTED", "CANCELLED"] and order_id == str(active_order_id):
                logger.warning(f"Primary entry order {order_id} was {order_status}. Resetting execution state.")
                active_order_id = None
                active_sec_id = None

    except Exception as e:
        logger.error(f"Order update parse exception: {e}")


async def order_update_stream():
    order_client = OrderUpdate(dhan_context)
    order_client.on_update = on_order_update
    while True:
        try:
            await order_client.connect_order_update()
        except Exception as e:
            logger.warning(f"Order WebSocket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)


# ==========================================
# 4. DERIVATIVE CONTRACT & ORDER ENGINE
# ==========================================
def _filter_nifty_options_df(df: pd.DataFrame) -> pd.DataFrame:
    """Isolates active NSE Nifty 50 Index Options and excludes sectoral indices."""
    df = df.copy()
    df.columns = [str(c).upper().strip() for c in df.columns]

    df = df[df['SEM_TRADING_SYMBOL'].astype(str).str.startswith('NIFTY')]
    df = df[~df['SEM_TRADING_SYMBOL'].astype(str).str.contains(
        'BANK|IT|AUTO|FMCG|FIN|MID|NXT|NEXT', na=False
    )]

    df['PARSED_EXPIRY'] = pd.to_datetime(df['SEM_EXPIRY_DATE'], errors='coerce').dt.date
    today_date = datetime.now().date()
    df = df[df['PARSED_EXPIRY'] >= today_date]

    return df.sort_values(by='PARSED_EXPIRY', ascending=True)


def get_nearest_expiry() -> str:
    global cached_nearest_expiry
    if cached_nearest_expiry:
        return cached_nearest_expiry

    # Tier 1: Live Exchange Expiry API
    for segment in ["IDX_I", "NSE_FNO"]:
        try:
            expiry_resp = dhan.expiry_list(
                under_security_id=NIFTY_SECURITY_ID,
                under_exchange_segment=segment
            )
            if isinstance(expiry_resp, dict) and expiry_resp.get('status', '').lower() == 'success':
                data = expiry_resp.get('data')
                if isinstance(data, list) and len(data) > 0:
                    cached_nearest_expiry = str(data[0])
                    return cached_nearest_expiry
                elif isinstance(data, dict) and data:
                    first_val = list(data.values())[0]
                    cached_nearest_expiry = str(first_val[0]) if isinstance(first_val, list) and first_val else str(first_val)
                    return cached_nearest_expiry
        except Exception:
            pass

    # Tier 2: Offline Security Master CSV (Handles NSE holiday shifts)
    if security_df is not None and not security_df.empty:
        try:
            nifty_df = _filter_nifty_options_df(security_df)
            if not nifty_df.empty:
                nearest_date = nifty_df['PARSED_EXPIRY'].iloc[0]
                cached_nearest_expiry = nearest_date.strftime("%Y-%m-%d")
                logger.info(f"Resolved holiday-adjusted expiry from Security Master: {cached_nearest_expiry}")
                return cached_nearest_expiry
        except Exception as e:
            logger.warning(f"Security Master expiry lookup failed: {e}")

    # Tier 3: Mathematical Tuesday Fallback (NSE Nifty weekly expiry standard)
    logger.warning("Using mathematical Tuesday expiry fallback.")
    today = datetime.now()
    days_ahead = (1 - today.weekday()) % 7
    cached_nearest_expiry = (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
    return cached_nearest_expiry


def get_atm_option_security_id(option_type: str, current_ltp: float):
    global QUANTITY
    atm_strike = round(float(current_ltp) / 50) * 50
    expiry = get_nearest_expiry()

    # Tier 1: Live Option Chain API
    if expiry:
        for segment in ["IDX_I", "NSE_FNO"]:
            try:
                resp = dhan.option_chain(
                    under_security_id=NIFTY_SECURITY_ID,
                    under_exchange_segment=segment,
                    expiry=expiry
                )
                if isinstance(resp, dict):
                    resp_data = resp.get('data', {})
                    if 'oc' in resp_data and bool(resp_data['oc']):
                        oc_data = resp_data['oc']
                        closest_key = min(oc_data.keys(), key=lambda k: abs(float(k) - atm_strike))
                        target_opt = oc_data[closest_key].get('ce') if option_type == "CALL" else oc_data[closest_key].get('pe')
                        if target_opt:
                            return target_opt.get('security_id', target_opt.get('securityId'))
            except Exception:
                pass

    # Tier 2: Offline Master DataFrame Fallback
    if security_df is not None and not security_df.empty:
        try:
            df = _filter_nifty_options_df(security_df)
            opt_code = 'CE' if option_type == 'CALL' else 'PE'

            df['SEM_STRIKE_PRICE'] = pd.to_numeric(df['SEM_STRIKE_PRICE'], errors='coerce')
            df = df[(df['SEM_STRIKE_PRICE'] == float(atm_strike)) & (df['SEM_OPTION_TYPE'] == opt_code)]

            if expiry:
                target_date = pd.to_datetime(expiry, errors='coerce').date()
                df_exp = df[df['PARSED_EXPIRY'] == target_date]
                df = df_exp if not df_exp.empty else df

            if not df.empty:
                matched_row = df.iloc[0]
                if 'SEM_LOT_UNITS' in matched_row and pd.notna(matched_row['SEM_LOT_UNITS']):
                    lot_units = int(float(matched_row['SEM_LOT_UNITS']))
                    if lot_units > 0:
                        QUANTITY = NUM_LOTS * lot_units
                return str(matched_row['SEM_SMST_SECURITY_ID'])
        except Exception as e:
            logger.error(f"Offline option strike mapping failed: {e}")
    return None


def has_open_positions(check_sec_id=None) -> bool:
    try:
        positions_resp = dhan.get_positions()
        if isinstance(positions_resp, dict) and positions_resp.get('status', '').lower() == 'success':
            data = positions_resp.get('data', [])
            for pos in data:
                pos_sec_id = str(pos.get('securityId', pos.get('security_id', '')))
                if check_sec_id and pos_sec_id != str(check_sec_id):
                    continue

                try:
                    buy_qty = int(float(pos.get('buyQty', 0)))
                except (ValueError, TypeError):
                    buy_qty = 0

                try:
                    sell_qty = int(float(pos.get('sellQty', 0)))
                except (ValueError, TypeError):
                    sell_qty = 0

                try:
                    net_qty = int(float(pos.get('netQty', pos.get('net_qty', buy_qty - sell_qty))))
                except (ValueError, TypeError):
                    net_qty = buy_qty - sell_qty

                pos_type = str(pos.get('positionType', pos.get('position_type', ''))).upper()

                if net_qty != 0 and pos_type != 'CLOSED':
                    return True
            return False
        return True
    except Exception as e:
        logger.error(f"REST position verification failed: {e}")
        return True


def execute_super_order(security_id: str, transaction_type: str, entry_ltp: float):
    try:
        normalized_entry = round_to_tick(float(entry_ltp))

        # Dynamic percentage-based risk brackets with minimum point floor
        sl_points = max(MIN_SL_POINTS, normalized_entry * SL_PCT)
        target_points = max(MIN_SL_POINTS * 2.0, normalized_entry * TARGET_PCT)
        trail_jump = round_to_tick(max(2.0, sl_points * 0.5))

        target_price = round_to_tick(normalized_entry + target_points)
        stop_loss_price = round_to_tick(max(0.05, normalized_entry - sl_points))

        logger.info(
            f"Submitting Bracket Order | SecID: {security_id} | Entry: {normalized_entry:.2f} | "
            f"Target: {target_price:.2f} | SL: {stop_loss_price:.2f} | TrailJump: {trail_jump:.2f}"
        )

        order_response = dhan.place_super_order(
            security_id=str(security_id),
            exchange_segment="NSE_FNO",
            transaction_type=transaction_type,
            quantity=int(QUANTITY),
            order_type="LIMIT",
            product_type="INTRADAY",
            price=normalized_entry,
            targetPrice=target_price,
            stopLossPrice=stop_loss_price,
            trailingJump=trail_jump,
            tag="QUANT_EXEC_V2"
        )
        if isinstance(order_response, dict) and order_response.get('status', '').lower() == 'success':
            return order_response.get('data', {}).get('orderId', order_response.get('data', {}).get('order_id'))
    except Exception as e:
        logger.error(f"Bracket Order execution failed: {e}")
    return None


# ==========================================
# 5. CORE QUANTITATIVE EVENT LOOP
# ==========================================
async def trading_bot_loop():
    global feed_instance, active_order_id, active_sec_id
    logger.info(
        f"Quantitative Engine Initialized | Interval: {CANDLE_INTERVAL_SECONDS}s | "
        f"Indicators: EMA({EMA_PERIOD}), Will%R({WILLR_PERIOD}), ADX({ADX_PERIOD})"
    )

    last_candle_time = None
    signal_triggered_this_candle = False
    last_rest_sync_time = time.time()

    # Historical Warmup Fetch (7-day lookback of 5-minute bars)
    from_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    response = dhan.intraday_minute_data(
        str(NIFTY_SECURITY_ID), "IDX_I", "INDEX",
        from_date, datetime.now().strftime('%Y-%m-%d'), interval=5
    )

    if not (isinstance(response, dict) and response.get('status', '').lower() == 'success'):
        logger.error("Failed to fetch historical warmup bars from REST API.")
        return

    data_dict = response.get('data', {})
    df = pd.DataFrame({
        'high': data_dict.get('high', []),
        'low': data_dict.get('low', []),
        'close': data_dict.get('close', [])
    })

    if len(df) <= EMA_PERIOD:
        logger.error(f"Insufficient historical bars for EMA-{EMA_PERIOD} warmup (got {len(df)} bars).")
        return

    df['EMA_200'] = ta.ema(df['close'], length=EMA_PERIOD)
    base_ema_200 = float(df['EMA_200'].iloc[-1])

    base_adx = 0.0
    adx_df = ta.adx(df['high'], df['low'], df['close'], length=ADX_PERIOD)
    if adx_df is not None and not adx_df.empty:
        adx_val = adx_df[f'ADX_{ADX_PERIOD}'].iloc[-1]
        if pd.notna(adx_val):
            base_adx = float(adx_val)

    last_candle_high = float(df['high'].iloc[-1])
    last_candle_low = float(df['low'].iloc[-1])

    while feed_instance is None:
        await asyncio.sleep(0.5)

    live_high = None
    live_low = None

    while True:
        now = datetime.now()
        now_ts = time.time()
        current_time = now.time()

        # Periodic REST State Reconciliation (15s heartbeat when an order is active)
        if active_order_id and (now_ts - last_rest_sync_time > 15.0):
            last_rest_sync_time = now_ts
            try:
                order_resp = await asyncio.to_thread(dhan.get_order_by_id, active_order_id)
                order_status = (
                    str(order_resp.get('data', {}).get('orderStatus', 'UNKNOWN')).upper().strip()
                    if isinstance(order_resp, dict) and order_resp.get('status', '').lower() == 'success'
                    else "UNKNOWN"
                )

                if order_status in ["REJECTED", "CANCELLED"]:
                    logger.warning(f"REST Sync: Order {active_order_id} is {order_status}. Resetting state.")
                    active_order_id, active_sec_id = None, None
                elif order_status not in ["PENDING", "OPEN", "TRANSIT"]:
                    in_position = await asyncio.to_thread(has_open_positions, active_sec_id)
                    if not in_position:
                        logger.info(f"REST Sync: Position closed for {active_sec_id}. Resetting state.")
                        if order_status == "UNKNOWN":
                            try:
                                await asyncio.to_thread(dhan.cancel_super_order, active_order_id, "ENTRY_LEG")
                            except Exception:
                                pass
                        active_order_id, active_sec_id = None, None
            except Exception:
                pass

        # Strict NSE Normal Trading Session Check (09:15 to 15:30 IST, Mon-Fri)
        is_market_open = (MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME) and (now.weekday() < 5)
        can_enter_new_trade = (MARKET_OPEN_TIME <= current_time <= NO_NEW_ENTRY_TIME) and (now.weekday() < 5)

        if is_market_open:
            current_ltp = live_market_data.get(str(NIFTY_SECURITY_ID))

            if current_ltp:
                if live_high is None or current_ltp > live_high:
                    live_high = current_ltp
                if live_low is None or current_ltp < live_low:
                    live_low = current_ltp

                interval_minutes = CANDLE_INTERVAL_SECONDS // 60
                aligned_minute = (now.minute // interval_minutes) * interval_minutes
                current_candle_window = now.replace(minute=aligned_minute, second=0, microsecond=0)
                if last_candle_time is None:
                    last_candle_time = current_candle_window

                # 5-Minute Bar Rollover & Vectorized Indicator Update
                if now >= last_candle_time + timedelta(seconds=CANDLE_INTERVAL_SECONDS):
                    new_row = pd.DataFrame({'high': [live_high], 'low': [live_low], 'close': [current_ltp]})
                    df = pd.concat([df, new_row], ignore_index=True).tail(300).reset_index(drop=True)

                    df['EMA_200'] = ta.ema(df['close'], length=EMA_PERIOD)
                    base_ema_200 = float(df['EMA_200'].iloc[-1])

                    adx_df = ta.adx(df['high'], df['low'], df['close'], length=ADX_PERIOD)
                    if adx_df is not None and not adx_df.empty:
                        adx_val = adx_df[f'ADX_{ADX_PERIOD}'].iloc[-1]
                        if pd.notna(adx_val):
                            base_adx = float(adx_val)

                    last_candle_high = live_high
                    last_candle_low = live_low
                    last_candle_time = current_candle_window
                    signal_triggered_this_candle = False

                    logger.info(
                        f"Bar Closed | Close: {current_ltp:.2f} | EMA200: {base_ema_200:.2f} | "
                        f"ADX: {base_adx:.2f} | Prior High/Low: {last_candle_high:.2f}/{last_candle_low:.2f}"
                    )

                    live_high, live_low = current_ltp, current_ltp

                # Real-Time Signal Evaluation
                if can_enter_new_trade and not active_order_id and not signal_triggered_this_candle:
                    past_highs = df['high'].iloc[-(WILLR_PERIOD - 1):]
                    past_lows = df['low'].iloc[-(WILLR_PERIOD - 1):]

                    live_hh = max(float(past_highs.max()), live_high)
                    live_ll = min(float(past_lows.min()), live_low)

                    live_willr_100 = (
                        ((live_hh - current_ltp) / (live_hh - live_ll)) * -100.0
                        if live_hh != live_ll else -50.0
                    )

                    alpha_200 = 2.0 / (EMA_PERIOD + 1.0)
                    live_ema_200 = (current_ltp * alpha_200) + (base_ema_200 * (1.0 - alpha_200))

                    signal = None
                    # Symmetric CALL Condition: Uptrend + Prior High Breakout + Upper 20% Momentum Thrust + Active ADX
                    if (
                        current_ltp > live_ema_200
                        and current_ltp > last_candle_high
                        and live_willr_100 > WILLR_UPPER_BAND
                        and base_adx > ADX_THRESHOLD
                    ):
                        signal = "CALL"

                    # Symmetric PUT Condition: Downtrend + Prior Low Breakdown + Lower 20% Momentum Thrust + Active ADX
                    elif (
                        current_ltp < live_ema_200
                        and current_ltp < last_candle_low
                        and live_willr_100 < WILLR_LOWER_BAND
                        and base_adx > ADX_THRESHOLD
                    ):
                        signal = "PUT"

                    if signal:
                        logger.info(
                            f"Signal Triggered: {signal} | Spot: {current_ltp:.2f} | "
                            f"Will%R: {live_willr_100:.2f} | ADX: {base_adx:.2f}"
                        )
                        signal_triggered_this_candle = True
                        sec_id = get_atm_option_security_id(signal, current_ltp)

                        if sec_id:
                            fno_segment_int = getattr(MarketFeed, 'NSE_FNO', 2)
                            try:
                                feed_instance.subscribe_symbols([(fno_segment_int, str(sec_id), MarketFeed.Ticker)])
                            except Exception as e:
                                logger.debug(f"Dynamic symbol subscription notice: {e}")

                            entry_ltp = None
                            try:
                                int_sec_id = int(sec_id)

                                def extract_ltp(resp_data, target):
                                    if not isinstance(resp_data, dict):
                                        return None
                                    def _search(d):
                                        if isinstance(d, dict):
                                            for k, v in d.items():
                                                if str(k) == str(target) and isinstance(v, dict):
                                                    val = v.get('last_price', v.get('lastPrice', v.get('LTP')))
                                                    if val:
                                                        return val
                                                if isinstance(v, (dict, list)):
                                                    res = _search(v)
                                                    if res:
                                                        return res
                                        elif isinstance(d, list):
                                            for item in d:
                                                if isinstance(item, dict):
                                                    if str(item.get('securityId', item.get('security_id', ''))) == str(target):
                                                        val = item.get('last_price', item.get('lastPrice', item.get('LTP')))
                                                        if val:
                                                            return val
                                                    res = _search(item)
                                                    if res:
                                                        return res
                                        return None
                                    return _search(resp_data)

                                quote = await asyncio.to_thread(dhan.ticker_data, {"NSE_FNO": [int_sec_id]})
                                entry_ltp = extract_ltp(quote, sec_id)
                                if not entry_ltp:
                                    quote_full = await asyncio.to_thread(dhan.quote_data, {"NSE_FNO": [int_sec_id]})
                                    entry_ltp = extract_ltp(quote_full, sec_id)
                            except Exception:
                                pass

                            if not entry_ltp or float(entry_ltp) <= 0:
                                for _ in range(20):
                                    await asyncio.sleep(0.05)
                                    ws_ltp = live_market_data.get(str(sec_id))
                                    if ws_ltp and float(ws_ltp) > 0:
                                        entry_ltp = ws_ltp
                                        break

                            if entry_ltp and float(entry_ltp) > 0:
                                active_order_id = await asyncio.to_thread(
                                    execute_super_order, sec_id, "BUY", float(entry_ltp)
                                )
                                if active_order_id:
                                    active_sec_id = sec_id
                                    logger.info(f"Super Order Active | OrderID: {active_order_id} | SecID: {sec_id}")
                            else:
                                logger.warning(f"Aborting trade: Unable to verify valid LTP for contract {sec_id}.")

        await asyncio.sleep(0.01)


async def main():
    await asyncio.gather(market_data_stream(), order_update_stream(), trading_bot_loop())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Engine shutdown requested by user.")
