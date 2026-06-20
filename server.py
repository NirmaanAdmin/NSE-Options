"""
server.py
─────────
Options Bot — Kite Connect edition (NRML, multi-stock, server-owned exits).

ARCHITECTURE
────────────
1. Pine sends ONLY entry signals (fun2sh-style). Server owns the entire
   trade lifecycle once filled: trailing SL ladder, partial books, the exit.
2. Pine sends `action=buy` → server buys ITM CE. `action=sell` → ITM PE.
3. If Pine sends an OPPOSITE-direction entry on a symbol the server is
   already in: close current option + reopen as the opposite type (FLIP).
4. Same-symbol same-direction entry: SKIP (don't stack).
5. Background `ladder_worker` polls every EXIT_POLL_SEC:
     - Per-position trailing SL on UNDERLYING SPOT price (mirroring fun2sh):
         k=0 → SL at spot_entry ∓ SL_PCT
         k≥1 → SL one rung behind highest crossed (BE at first rung)
     - Books BOOK_PCT of original lots at each new rung, up to MAX_BOOKS
     - After MAX_BOOKS books, pure trail (no more books, SL keeps ratcheting)
     - Per-position PREMIUM SL backstop: if option premium drops PREMIUM_SL_PCT
       below entry premium → close that position regardless of spot
6. Safety stack (all carry over from the CoinDCX server unchanged in spirit):
     - PROFIT lock (combined premium PnL ≥ N% → close ALL)
     - LOSS lock (combined premium PnL ≤ -N% → close ALL)
     - Daily cap (cumulative locked profit % across the day)
     - Streak pause (N consecutive profit locks → pause for an hour)
     - SL lockout (per-symbol cooldown after SL hits)
     - Position cap (MAX_POSITIONS hard ceiling on concurrent symbols)
     - Cooldowns after each lock fires
     - Expiry safety sweep (close any open position within N days of expiry)
7. Entry-only Pine + server-owned exits = no exit webhook race. Pine cannot
   "double-fire" books / SLs / closes that the server then re-acts on.

WEBHOOK CONTRACT (POST /webhook)
─────────────────────────────────
  Headers:  X-Auth-Secret: <WEBHOOK_SECRET>   (preferred)
  Body JSON:
    {
      "secret":   "<WEBHOOK_SECRET>",     # alternative to header
      "symbol":   "TATASTEEL",            # underlying name; matches Kite name
      "action":   "buy" | "sell",
      "type":     "entry",                # ONLY entry is processed
      "price":    217.91                  # spot at signal (for logging + entry baseline)
    }

  Any non-`entry` type is rejected (Pine is no longer the brain).
"""
import os
import json
import math
import time
import logging
import threading
import hmac as _hmac_cmp
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Dict

import pytz
from flask import Flask, request, jsonify

from kite_client import KiteClient
from instruments import load as load_instruments, list_underlyings
from option_resolver import pick_option
from position_sizer import size_position, get_available_equity
from tradable_symbols import (
    TRADABLE_UNDERLYINGS, INDEX_UNDERLYINGS,
    is_tradable, is_index, spot_ltp_key,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("bot")

IST = pytz.timezone("Asia/Kolkata")

# ═══════════════════════════════════════════════════════════════════════
#  ENV CONFIG
# ═══════════════════════════════════════════════════════════════════════
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "")
PRODUCT          = os.environ.get("KITE_PRODUCT", "NRML")  # NRML = overnight carry

# Ladder (server-owned exits) — all percentages on SPOT
EXIT_POLL_SEC     = float(os.environ.get("EXIT_POLL_SEC", "10"))
SL_PCT            = float(os.environ.get("SL_PCT", "3.0"))        # initial SL %
TP_STEP_PCT       = float(os.environ.get("TP_STEP_PCT", "4.0"))   # rung spacing %
BOOK_PCT          = float(os.environ.get("BOOK_PCT", "33.0"))     # % of original lots/book
MAX_BOOKS         = int(os.environ.get("MAX_BOOKS", "2"))         # books before pure trail
LADDER_VERBOSE    = os.environ.get("LADDER_VERBOSE", "false").lower() == "true"

# Per-position premium SL backstop (premium-side hard floor independent of spot)
PREMIUM_SL_PCT    = float(os.environ.get("PREMIUM_SL_PCT", "50.0"))

# Combined PnL locks (computed on PREMIUM PnL across all open option positions)
PROFIT_LOCK_ENABLED   = os.environ.get("PROFIT_LOCK_ENABLED", "true").lower() == "true"
PROFIT_LOCK_PCT       = float(os.environ.get("PROFIT_LOCK_PCT", "30.0"))
PROFIT_LOCK_CD        = int(os.environ.get("PROFIT_LOCK_COOLDOWN_SEC", "1800"))
LOSS_LOCK_ENABLED     = os.environ.get("LOSS_LOCK_ENABLED", "true").lower() == "true"
LOSS_LOCK_PCT         = float(os.environ.get("LOSS_LOCK_PCT", "25.0"))
LOSS_LOCK_CD          = int(os.environ.get("LOSS_LOCK_COOLDOWN_SEC", "1800"))

# Daily cap (cumulative profit-locked %, resets at IST midnight)
DAILY_CAP_ENABLED     = os.environ.get("DAILY_CAP_ENABLED", "true").lower() == "true"
DAILY_CAP_PCT         = float(os.environ.get("DAILY_CAP_PCT", "50.0"))

# Streak pause (N consecutive profit locks → pause for STREAK_PAUSE_SEC)
STREAK_PAUSE_ENABLED  = os.environ.get("STREAK_PAUSE_ENABLED", "true").lower() == "true"
STREAK_PAUSE_COUNT    = int(os.environ.get("STREAK_PAUSE_COUNT", "3"))
STREAK_PAUSE_SEC      = int(os.environ.get("STREAK_PAUSE_SEC", "3600"))

# Per-symbol SL lockout (after a ladder SL fires, block re-entry on that symbol)
SL_LOCKOUT_ENABLED    = os.environ.get("SL_LOCKOUT_ENABLED", "true").lower() == "true"
SL_LOCKOUT_SEC        = int(os.environ.get("SL_LOCKOUT_SEC", "600"))  # 10 min default

# Position cap — hard ceiling on distinct symbols held at once
MAX_POSITIONS         = int(os.environ.get("MAX_POSITIONS", "5"))  # 0 = unlimited

# Expiry safety
EXPIRY_SAFETY_DAYS    = int(os.environ.get("EXPIRY_SAFETY_DAYS", "1"))

# Persistence files (mount /app/data as a Railway volume for restart durability)
DATA_DIR              = os.environ.get("DATA_DIR", "/app/data")
ACTIVE_TRADES_FILE    = os.environ.get("ACTIVE_TRADES_FILE", f"{DATA_DIR}/active_trades.json")
STREAK_FILE           = os.environ.get("STREAK_FILE",        f"{DATA_DIR}/streak_state.json")

# Trading hours guard
MKT_OPEN_HM  = (9, 15)
MKT_CLOSE_HM = (15, 30)


# ═══════════════════════════════════════════════════════════════════════
#  AUTH (fail-CLOSED, constant-time)
# ═══════════════════════════════════════════════════════════════════════
if not WEBHOOK_SECRET:
    log.warning(
        "⚠️ WEBHOOK_SECRET is empty — ALL authenticated endpoints will reject. "
        "Set WEBHOOK_SECRET in the environment before going live."
    )


def _secret_ok(provided) -> bool:
    if not WEBHOOK_SECRET:
        return False
    if not provided:
        return False
    return _hmac_cmp.compare_digest(str(provided), WEBHOOK_SECRET)


def _request_secret() -> Optional[str]:
    """X-Auth-Secret header preferred (out of access logs) — falls back to query."""
    return request.headers.get("X-Auth-Secret") or request.args.get("secret")


def _redact(d):
    """Copy of payload with the secret masked for logging."""
    if not isinstance(d, dict):
        return d
    out = dict(d)
    if "secret" in out:
        out["secret"] = "***"
    return out


# ═══════════════════════════════════════════════════════════════════════
#  STATE
# ═══════════════════════════════════════════════════════════════════════
app  = Flask(__name__)
kite = KiteClient()

# active_trades[underlying] = {
#   "tradingsymbol":      str,    e.g. "TATASTEEL25JUN200CE"
#   "token":              int,    Kite instrument_token (for ltp/orders)
#   "lot_size":           int,
#   "expiry":             str,    ISO date string (JSON-serializable)
#   "option_type":        "CE" | "PE",
#   "side":               "buy" | "sell",  # the Pine SIGNAL side
#   "lots_total":         int,    original lots at entry
#   "lots_remaining":     int,
#   "qty":                int,    lots_remaining × lot_size
#   "premium_at_entry":   float,  avg fill premium
#   "spot_at_entry":      float,  spot LTP at entry time
#   "entry_time":         float,  unix epoch
#   "order_id":           str,
#   "ladder_books":       int,    books done so far
#   "ladder_sl_spot":     float,  current trailing SL on SPOT price
# }
active_trades: Dict[str, dict] = {}

# RLock serializes order-placement + state mutation across the Flask request
# thread and all background workers. Order-of-events guarantees come from
# holding this lock around `check → mutate → place_order → save` sequences.
_trade_lock = threading.RLock()

# Lock-state scalars (read/write atomic under GIL)
_profit_lock_until: float = 0.0
_loss_lock_until:   float = 0.0
_daily_locked_pct_sum: float = 0.0
_daily_lock_count:     int = 0
_daily_paused:         bool = False
_daily_counter_date:   Optional[date] = None

# Streak pause state (persisted to disk)
_streak_profit_lock_count: int = 0
_streak_pause_until:       float = 0.0
_streak_last_lock_at:      Optional[str] = None
_streak_history:           list = []

# SL lockout: {underlying: unlock_epoch}
_sl_lockout: Dict[str, float] = {}

# Boot status
_boot_done = False

# Trade event log (in-memory ring buffer for /stats)
trade_log: list = []
MAX_LOG_ENTRIES = 500

# Ladder worker introspection
_ladder_last_check_at: Optional[str] = None
_ladder_last_error:    Optional[str] = None
_ladder_closes:        int = 0


# ═══════════════════════════════════════════════════════════════════════
#  PERSISTENCE — atomic JSON writes
# ═══════════════════════════════════════════════════════════════════════
def _atomic_write_json(path: str, obj) -> None:
    """Write JSON atomically: dump to .tmp, fsync, os.replace.
    Survives worker timeout / crash mid-write."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w") as f:
            json.dump(obj, f, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception as e:
        log.warning(f"⚠️ Failed to persist {path}: {e}")


def _save_active_trades():
    _atomic_write_json(ACTIVE_TRADES_FILE, active_trades)


def _load_active_trades():
    try:
        with open(ACTIVE_TRADES_FILE) as f:
            data = json.load(f)
        if isinstance(data, dict):
            active_trades.update(data)
        if data:
            log.info(f"📂 Restored {len(data)} active trade(s): {list(data.keys())}")
        else:
            log.info("📂 active_trades file empty — starting fresh")
    except FileNotFoundError:
        log.info(f"📂 No active_trades file at {ACTIVE_TRADES_FILE} — starting fresh")
    except Exception as e:
        log.warning(f"⚠️ Failed to load active_trades ({e}) — starting fresh")


def _save_streak():
    _atomic_write_json(STREAK_FILE, {
        "count":         _streak_profit_lock_count,
        "pause_until":   _streak_pause_until,
        "last_lock_at":  _streak_last_lock_at,
        "history":       _streak_history[-50:],
    })


def _load_streak():
    global _streak_profit_lock_count, _streak_pause_until
    global _streak_last_lock_at, _streak_history
    try:
        with open(STREAK_FILE) as f:
            state = json.load(f)
        _streak_profit_lock_count = int(state.get("count", 0))
        _streak_pause_until       = float(state.get("pause_until", 0.0))
        _streak_last_lock_at      = state.get("last_lock_at")
        _streak_history           = state.get("history", [])
        log.info(
            f"📊 Streak loaded: count={_streak_profit_lock_count}, "
            f"paused={_streak_pause_until > time.time()}"
        )
    except FileNotFoundError:
        log.info(f"📊 No streak file at {STREAK_FILE} — counter starts at 0")
    except Exception as e:
        log.warning(f"⚠️ Streak file corrupt ({e}) — starting fresh")


# ═══════════════════════════════════════════════════════════════════════
#  HELPERS — time / symbol / market
# ═══════════════════════════════════════════════════════════════════════
def now_ist() -> datetime:
    return datetime.now(IST)


def market_open() -> bool:
    n = now_ist()
    if n.weekday() >= 5:
        return False
    op = n.replace(hour=MKT_OPEN_HM[0],  minute=MKT_OPEN_HM[1],  second=0, microsecond=0)
    cl = n.replace(hour=MKT_CLOSE_HM[0], minute=MKT_CLOSE_HM[1], second=0, microsecond=0)
    return op <= n <= cl


def normalize_symbol(raw: str) -> str:
    """Pine may send 'NSE:TATASTEEL', 'TATASTEEL', or 'TATASTEEL-EQ'.
    All map to the underlying name used by Kite's instruments.name."""
    s = (raw or "").strip()
    if ":" in s:
        s = s.split(":", 1)[1]
    s = s.replace("-EQ", "").strip().upper()
    return s


def _expiry_to_date(expiry_val) -> Optional[date]:
    """active_trades stores expiry as string after JSON roundtrip. Coerce back."""
    if isinstance(expiry_val, date):
        return expiry_val
    if isinstance(expiry_val, str):
        try:
            return datetime.fromisoformat(expiry_val).date()
        except Exception:
            pass
    return None


def log_trade_event(symbol, action, alert_type, result, reason=""):
    trade_log.append({
        "time":   now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "action": action,
        "type":   alert_type,
        "result": result,
        "reason": reason,
    })
    if len(trade_log) > MAX_LOG_ENTRIES:
        trade_log.pop(0)


# ═══════════════════════════════════════════════════════════════════════
#  DAILY CAP — cumulative profit-locked %, resets at IST midnight
# ═══════════════════════════════════════════════════════════════════════
def _check_and_reset_daily_counter():
    global _daily_locked_pct_sum, _daily_lock_count, _daily_paused, _daily_counter_date
    today = now_ist().date()
    if _daily_counter_date != today:
        if _daily_counter_date is not None:
            log.info(
                f"🌅 IST midnight — daily reset "
                f"(prev: {_daily_lock_count} locks, {_daily_locked_pct_sum:.2f}%)"
            )
        _daily_locked_pct_sum = 0.0
        _daily_lock_count = 0
        _daily_paused = False
        _daily_counter_date = today


def _bump_daily_counter(lock_pct: float):
    global _daily_locked_pct_sum, _daily_lock_count, _daily_paused
    if not DAILY_CAP_ENABLED:
        return
    _check_and_reset_daily_counter()
    _daily_locked_pct_sum += abs(lock_pct)
    _daily_lock_count += 1
    log.info(
        f"📊 Daily: lock #{_daily_lock_count} +{abs(lock_pct):.2f}% "
        f"→ cumulative {_daily_locked_pct_sum:.2f}% / cap {DAILY_CAP_PCT}%"
    )
    if _daily_locked_pct_sum >= DAILY_CAP_PCT:
        _daily_paused = True
        log.warning(
            f"🛑 DAILY CAP REACHED — entries paused until IST midnight"
        )


def daily_cap_active() -> bool:
    if not DAILY_CAP_ENABLED:
        return False
    _check_and_reset_daily_counter()
    return _daily_paused


# ═══════════════════════════════════════════════════════════════════════
#  STREAK PAUSE
# ═══════════════════════════════════════════════════════════════════════
def in_streak_pause() -> bool:
    global _streak_profit_lock_count, _streak_pause_until
    if _streak_pause_until <= 0:
        return False
    if time.time() < _streak_pause_until:
        return True
    # Expired — clear and reset for next streak
    log.info(
        f"✅ Streak pause cleared — counter reset (was {_streak_profit_lock_count})"
    )
    _streak_profit_lock_count = 0
    _streak_pause_until = 0.0
    _streak_history.append({
        "ts":    datetime.now(timezone.utc).isoformat(),
        "event": "pause_expired",
        "count": 0,
    })
    _save_streak()
    return False


def streak_pause_remaining_sec() -> int:
    if not in_streak_pause():
        return 0
    return max(0, int(_streak_pause_until - time.time()))


def _register_profit_lock(trigger_pct=None):
    global _streak_profit_lock_count, _streak_pause_until, _streak_last_lock_at
    _streak_profit_lock_count += 1
    _streak_last_lock_at = datetime.now(timezone.utc).isoformat()
    log.info(
        f"📈 Profit-lock streak: {_streak_profit_lock_count}/{STREAK_PAUSE_COUNT}"
    )
    event = {
        "ts":          _streak_last_lock_at,
        "event":       "profit_lock",
        "count":       _streak_profit_lock_count,
        "trigger_pct": trigger_pct,
    }
    if STREAK_PAUSE_ENABLED and _streak_profit_lock_count >= STREAK_PAUSE_COUNT:
        _streak_pause_until = time.time() + STREAK_PAUSE_SEC
        log.warning(
            f"⏸️ STREAK PAUSE: {_streak_profit_lock_count} consecutive profit locks "
            f"— pausing entries for {STREAK_PAUSE_SEC // 60}m"
        )
        event["paused"] = True
        event["pause_until"] = _streak_pause_until
    _streak_history.append(event)
    _save_streak()


def _register_loss_lock():
    global _streak_profit_lock_count, _streak_pause_until
    if _streak_profit_lock_count == 0 and _streak_pause_until <= 0:
        return
    log.info(
        f"🔄 Streak reset by loss lock — counter was {_streak_profit_lock_count}"
    )
    _streak_history.append({
        "ts":              datetime.now(timezone.utc).isoformat(),
        "event":           "loss_lock_reset",
        "previous_count":  _streak_profit_lock_count,
    })
    _streak_profit_lock_count = 0
    _streak_pause_until = 0.0
    _save_streak()


# ═══════════════════════════════════════════════════════════════════════
#  SL LOCKOUT — per-symbol cooldown after a ladder SL fires
# ═══════════════════════════════════════════════════════════════════════
def mark_sl_lockout(underlying: str, reason: str = ""):
    if not SL_LOCKOUT_ENABLED:
        return
    until = time.time() + SL_LOCKOUT_SEC
    _sl_lockout[underlying] = until
    log.info(
        f"🚫 SL LOCKOUT armed: {underlying} blocked for {SL_LOCKOUT_SEC // 60}m ({reason})"
    )


def in_sl_lockout(underlying: str):
    """Returns (locked: bool, remaining_sec: int)."""
    if not SL_LOCKOUT_ENABLED:
        return False, 0
    until = _sl_lockout.get(underlying, 0)
    if until == 0:
        return False, 0
    remaining = int(until - time.time())
    if remaining <= 0:
        _sl_lockout.pop(underlying, None)
        return False, 0
    return True, remaining


# ═══════════════════════════════════════════════════════════════════════
#  PRICE FETCHING — bulk LTPs for both spots and options
# ═══════════════════════════════════════════════════════════════════════
def _fetch_spot_and_option_ltps(snapshot):
    """For each (underlying, trade) in snapshot, fetch spot LTP + option LTP.
    Returns (spots: dict, opts: dict) keyed by underlying.
    Spots dict: {underlying: spot_price}.
    Opts dict:  {underlying: option_premium}.
    Missing entries → not in dict (caller handles)."""
    if not snapshot:
        return {}, {}
    # Spots via string keys (NSE:NAME or index special) — one bulk call
    spot_keys  = [spot_ltp_key(u) for u, _ in snapshot]
    spot_keys_by_under = {u: spot_ltp_key(u) for u, _ in snapshot}
    # Option premiums via int tokens — one bulk call
    opt_tokens = [int(t["token"]) for _, t in snapshot]
    opt_tokens_by_under = {u: int(t["token"]) for u, t in snapshot}

    try:
        spot_raw = kite.bulk_ltp(spot_keys) if spot_keys else {}
    except Exception as e:
        log.error(f"spot LTP fetch failed: {e}")
        spot_raw = {}
    try:
        opt_raw = kite.bulk_ltp(opt_tokens) if opt_tokens else {}
    except Exception as e:
        log.error(f"option LTP fetch failed: {e}")
        opt_raw = {}

    spots, opts = {}, {}
    for u, _ in snapshot:
        sk = spot_keys_by_under[u]
        if sk in spot_raw:
            spots[u] = spot_raw[sk]
        tk = str(opt_tokens_by_under[u])
        if tk in opt_raw:
            opts[u] = opt_raw[tk]
    return spots, opts


# ═══════════════════════════════════════════════════════════════════════
#  COMBINED PNL — premium-based net% across all active option positions
# ═══════════════════════════════════════════════════════════════════════
def compute_combined_pnl():
    """Returns (net_pnl_inr, total_cost_inr, net_pct).
    All on PREMIUM basis (we're long the option, cost = premium paid).
    Returns (None, None, None) if any LTP missing — caller skips decisions."""
    with _trade_lock:
        if not active_trades:
            return 0.0, 0.0, 0.0
        snapshot = [(u, dict(t)) for u, t in active_trades.items()]

    _, opts = _fetch_spot_and_option_ltps(snapshot)

    total_pnl  = 0.0
    total_cost = 0.0
    for u, t in snapshot:
        if u not in opts:
            log.warning(f"compute_combined_pnl: missing option LTP for {u}")
            return None, None, None
        ltp   = opts[u]
        entry = float(t["premium_at_entry"])
        qty   = int(t["qty"])
        total_pnl  += (ltp - entry) * qty
        total_cost += entry * qty

    if total_cost <= 0:
        return total_pnl, 0.0, 0.0
    return total_pnl, total_cost, (total_pnl / total_cost) * 100.0


# ═══════════════════════════════════════════════════════════════════════
#  LADDER — server-owned exits (per-position trailing SL + partial books)
# ═══════════════════════════════════════════════════════════════════════
def _spot_sl_price(side: str, spot_entry: float) -> Optional[float]:
    """Initial SL price on spot. CE (buy) → entry × (1 - SL%). PE (sell) → entry × (1 + SL%)."""
    if not spot_entry or spot_entry <= 0:
        return None
    if side == "buy":
        return spot_entry * (1.0 - SL_PCT / 100.0)
    return spot_entry * (1.0 + SL_PCT / 100.0)


def _spot_sl_breached(side: str, spot_now: float, sl: float) -> bool:
    """CE breaches at/below SL (spot dropped); PE breaches at/above (spot rose)."""
    if not sl or sl <= 0 or not spot_now or spot_now <= 0:
        return False
    return spot_now <= sl if side == "buy" else spot_now >= sl


def _rungs_crossed(side: str, spot_now: float, spot_entry: float) -> int:
    """How many TP_STEP_PCT rungs INTO PROFIT on spot. 0 if not yet in profit."""
    if spot_entry <= 0 or spot_now <= 0:
        return 0
    prof = ((spot_now - spot_entry) / spot_entry) if side == "buy" \
           else ((spot_entry - spot_now) / spot_entry)
    if prof <= 0:
        return 0
    return int(math.floor(prof / (TP_STEP_PCT / 100.0)))


def _ladder_sl_target(side: str, spot_entry: float, k: int) -> Optional[float]:
    """SL price implied by k rungs crossed:
      k <= 0 → initial stop (entry ∓ SL_PCT)
      k == 1 → breakeven (entry)
      k >= 2 → entry ± TP_STEP_PCT × (k - 1)
    Long adds, short (PE) subtracts."""
    if k <= 0:
        return _spot_sl_price(side, spot_entry)
    step = TP_STEP_PCT / 100.0
    if side == "buy":
        return spot_entry * (1.0 + step * (k - 1))
    return spot_entry * (1.0 - step * (k - 1))


def _ratchet_sl(side: str, cur_sl: Optional[float], target: Optional[float]) -> Optional[float]:
    """SL only ratchets toward profit — never loosens.
    CE: max(cur, target).  PE: min(cur, target)."""
    if cur_sl is None:
        return target
    if target is None:
        return cur_sl
    return max(cur_sl, target) if side == "buy" else min(cur_sl, target)


def _sell_lots(underlying: str, lots: int, reason: str = "") -> bool:
    """SELL `lots` lots of the option currently open on `underlying`.
    Updates active_trades. Returns True on order success.

    Caller already holds _trade_lock OR is in the ladder worker which
    re-acquires it on its own (RLock is reentrant; no deadlock risk)."""
    with _trade_lock:
        t = active_trades.get(underlying)
        if not t:
            log.warning(f"_sell_lots: no active trade for {underlying}")
            return False
        if lots <= 0:
            log.warning(f"_sell_lots: lots={lots} ≤ 0")
            return False
        if lots > t["lots_remaining"]:
            log.warning(
                f"_sell_lots: clamping {lots} → {t['lots_remaining']} (remaining)"
            )
            lots = t["lots_remaining"]
        sym = t["tradingsymbol"]
        qty = lots * t["lot_size"]

    try:
        oid = kite.place_market_order(
            tradingsymbol=sym,
            transaction_type="SELL",
            quantity=qty,
            product=PRODUCT,
        )
        log.info(f"✅ SELL {qty} {sym} (reason={reason}, order={oid})")
    except Exception as e:
        log.error(f"❌ SELL {sym} failed: {e}", exc_info=True)
        return False

    with _trade_lock:
        t = active_trades.get(underlying)
        if not t:
            return True
        t["lots_remaining"] -= lots
        t["qty"]            -= qty
        t["ladder_books"]   = int(t.get("ladder_books", 0)) + 1
        if t["lots_remaining"] <= 0:
            log.info(f"🏁 Closed all of {underlying} ({sym})")
            active_trades.pop(underlying, None)
        _save_active_trades()
    return True


def close_position(underlying: str, reason: str = "") -> bool:
    """Close ALL remaining lots on this symbol."""
    with _trade_lock:
        t = active_trades.get(underlying)
        if not t:
            return False
        lots = int(t["lots_remaining"])
    if lots <= 0:
        return False
    return _sell_lots(underlying, lots, reason=reason)


def _ladder_step(underlying: str, spot_now: float, option_now: float):
    """One ladder evaluation for one symbol. Holds _trade_lock around the
    read/mutate cycle. The order matters:
      1. Premium-SL backstop (catastrophic premium drop closes the position
         even if spot didn't trip).
      2. Spot-SL breach (committed trailing stop on spot).
      3. Advance — book lots at new rungs, then ratchet trailing SL.
    """
    global _ladder_closes
    with _trade_lock:
        t = active_trades.get(underlying)
        if not t:
            return
        side       = t["side"]
        spot_entry = float(t.get("spot_at_entry", 0) or 0)
        prem_entry = float(t.get("premium_at_entry", 0) or 0)
        lots_left  = int(t.get("lots_remaining", 0))
        if spot_entry <= 0 or spot_now <= 0 or lots_left <= 0:
            return

        cur_sl = t.get("ladder_sl_spot")
        if cur_sl is None:
            # First touch — arm the initial stop. Should normally be set at
            # entry, but defend against persisted-state oddities.
            cur_sl = _spot_sl_price(side, spot_entry)
            t["ladder_sl_spot"] = cur_sl
            _save_active_trades()
            log.info(
                f"🪜 LADDER armed: {underlying} {side.upper()} spot_entry={spot_entry} "
                f"SL0={cur_sl:.4f} (±{SL_PCT}%)"
            )

        books = int(t.get("ladder_books", 0))
        orig_lots = int(t.get("lots_total", lots_left))

        # ── 1. Premium-SL backstop ───────────────────────────
        if prem_entry > 0 and PREMIUM_SL_PCT > 0:
            prem_drop_pct = (1.0 - (option_now / prem_entry)) * 100.0
            if prem_drop_pct >= PREMIUM_SL_PCT:
                log.warning(
                    f"🚨 PREMIUM SL: {underlying} option {option_now} "
                    f"(−{prem_drop_pct:.1f}% vs entry {prem_entry}) — closing"
                )
                ok = close_position(underlying, reason="premium_sl")
                if ok:
                    mark_sl_lockout(underlying, reason=f"premium SL @ {option_now}")
                    log_trade_event(underlying, "sell", "premium_sl", "FILLED",
                                    f"prem={option_now} drop={prem_drop_pct:.1f}%")
                    _ladder_closes += 1
                return

        # ── 2. Spot-SL breach (committed trailing) ───────────
        if _spot_sl_breached(side, spot_now, cur_sl):
            reason = "spot_sl" if books == 0 else "trail_sl"
            log.warning(
                f"🛡️ LADDER STOP: {underlying} {side.upper()} spot={spot_now} "
                f"hit SL={cur_sl:.4f} (books={books}) — flattening"
            )
            ok = close_position(underlying, reason=reason)
            if ok:
                mark_sl_lockout(underlying, reason=f"{reason} @ spot {spot_now}")
                log_trade_event(underlying, "sell", reason, "FILLED",
                                f"spot={spot_now} sl={cur_sl:.4f} books={books}")
                _ladder_closes += 1
            return

        # ── 3. Advance — books at new rungs, then ratchet trail ──
        k = _rungs_crossed(side, spot_now, spot_entry)
        if LADDER_VERBOSE:
            prof = ((spot_now - spot_entry) / spot_entry if side == "buy"
                    else (spot_entry - spot_now) / spot_entry) * 100.0
            log.info(
                f"🪜 ladder[{underlying}] {side} spot={spot_now} entry={spot_entry} "
                f"prof={prof:+.2f}% k={k} books={books}/{MAX_BOOKS} "
                f"sl={cur_sl:.4f} opt_ltp={option_now} (entry {prem_entry})"
            )

        target_books = min(MAX_BOOKS, k)
        booked_any = False
        while books < target_books and t["lots_remaining"] > 0:
            book_lots = max(1, math.floor(orig_lots * BOOK_PCT / 100.0))
            book_lots = min(book_lots, t["lots_remaining"])
            if book_lots <= 0:
                break
            log.info(
                f"🪜 LADDER BOOK #{books+1}/{MAX_BOOKS}: {underlying} "
                f"selling {book_lots} lots of {t['lots_remaining']} (rung {books+1}, spot={spot_now})"
            )
            # Release the lock for the order call; _sell_lots re-acquires it
            # (RLock so safe). Re-read trade afterwards in case it changed.
            pass  # we're already inside lock; _sell_lots is reentrant
            ok = _sell_lots(underlying, book_lots, reason=f"server_book_{books+1}")
            if not ok:
                break  # retry next poll, don't advance counter
            log_trade_event(underlying, "sell", "server_book", "FILLED",
                            f"book #{books+1} lots={book_lots}")
            t = active_trades.get(underlying)
            if not t:
                # entire position closed (book reduced to 0) — nothing more
                return
            books = int(t.get("ladder_books", books + 1))
            booked_any = True

        # Ratchet trailing SL — only update if it moved
        target_sl = _ladder_sl_target(side, spot_entry, k)
        new_sl    = _ratchet_sl(side, cur_sl, target_sl)
        if new_sl is not None and new_sl != cur_sl:
            log.info(
                f"🪜 LADDER trail: {underlying} SL {cur_sl:.4f} → {new_sl:.4f} "
                f"(rungs={k}, books={books})"
            )
            t["ladder_sl_spot"] = new_sl
            _save_active_trades()
        elif booked_any:
            _save_active_trades()


def ladder_worker():
    """Server-owned exits — runs forever, evaluates every active trade
    every EXIT_POLL_SEC. Self-gates on _boot_done + market hours."""
    global _ladder_last_check_at, _ladder_last_error
    log.info(
        f"🪜 Ladder worker started — poll={EXIT_POLL_SEC}s SL={SL_PCT}% "
        f"TP_STEP={TP_STEP_PCT}% BOOK={BOOK_PCT}%×{MAX_BOOKS} "
        f"PREMIUM_SL={PREMIUM_SL_PCT}%"
    )
    while True:
        try:
            time.sleep(EXIT_POLL_SEC)
            if not _boot_done:
                continue
            # Expiry sweep runs even outside market hours so morning-of-expiry
            # squareoff is consistent.
            _expiry_safety_sweep()
            if not market_open():
                continue
            with _trade_lock:
                snapshot = [(u, dict(t)) for u, t in active_trades.items()]
            if not snapshot:
                continue

            spots, opts = _fetch_spot_and_option_ltps(snapshot)
            _ladder_last_check_at = datetime.now(timezone.utc).isoformat()
            if not spots and not opts:
                _ladder_last_error = "all LTP fetches failed"
                continue
            _ladder_last_error = None

            for u, _ in snapshot:
                if u not in spots:
                    log.debug(f"ladder: no spot LTP for {u}, skipping cycle")
                    continue
                if u not in opts:
                    log.debug(f"ladder: no option LTP for {u}, skipping cycle")
                    continue
                try:
                    _ladder_step(u, spots[u], opts[u])
                except Exception as e:
                    log.error(f"ladder step error {u}: {e}", exc_info=True)
        except Exception as e:
            _ladder_last_error = str(e)
            log.error(f"ladder_worker error: {e}", exc_info=True)


def _expiry_safety_sweep():
    """Close any positions whose expiry is within EXPIRY_SAFETY_DAYS.
    Runs every cycle (including off-hours) so morning-of-expiry is consistent."""
    today = now_ist().date()
    with _trade_lock:
        near = []
        for u, t in active_trades.items():
            exp = _expiry_to_date(t.get("expiry"))
            if exp is None:
                continue
            if (exp - today).days <= EXPIRY_SAFETY_DAYS:
                near.append((u, exp))
    for u, exp in near:
        log.warning(
            f"⏰ EXPIRY guard: {u} expires {exp} "
            f"(within {EXPIRY_SAFETY_DAYS}d) — closing"
        )
        close_position(u, reason="expiry_safety")


# ═══════════════════════════════════════════════════════════════════════
#  CLOSE-ALL (profit/loss lock) — closes every position, arms cooldown
# ═══════════════════════════════════════════════════════════════════════
def close_all_positions(trigger_reason="profit lock",
                        trigger_pct: Optional[float] = None,
                        lock_type: str = "profit") -> int:
    """Close every position via market SELL, then arm the matching cooldown.
    Cooldown is armed BEFORE the close loop (mirrors CoinDCX pattern) so the
    webhook gate starts rejecting new entries the instant the lock fires."""
    global _profit_lock_until, _loss_lock_until

    # Arm cooldown FIRST — before any orders go out — so a webhook landing
    # mid-close is rejected by the gate.
    if lock_type == "loss":
        _loss_lock_until = time.time() + LOSS_LOCK_CD
    else:
        _profit_lock_until = time.time() + PROFIT_LOCK_CD

    header_emoji = "🛑" if lock_type == "loss" else "🔒"
    header_label = "LOSS LOCK" if lock_type == "loss" else "PROFIT LOCK"

    with _trade_lock:
        snapshot = list(active_trades.keys())
    log.info(
        f"{header_emoji} {header_label} triggered ({trigger_reason}) "
        f"— closing {len(snapshot)} positions"
    )

    closed = 0
    for u in snapshot:
        try:
            if close_position(u, reason=f"{lock_type}_lock"):
                closed += 1
                log_trade_event(u, "sell",
                                "loss_lock" if lock_type == "loss" else "profit_lock",
                                "FILLED", trigger_reason)
        except Exception as e:
            log.error(f"❌ lock close exception {u}: {e}", exc_info=True)

    if lock_type == "loss":
        cooldown_end = datetime.fromtimestamp(_loss_lock_until).strftime("%H:%M:%S")
        log.info(f"✅ LOSS LOCK complete — closed {closed}, cooldown until {cooldown_end}")
        _register_loss_lock()
    else:
        cooldown_end = datetime.fromtimestamp(_profit_lock_until).strftime("%H:%M:%S")
        log.info(f"✅ PROFIT LOCK complete — closed {closed}, cooldown until {cooldown_end}")
        if trigger_pct is not None:
            _bump_daily_counter(trigger_pct)
        _register_profit_lock(trigger_pct=trigger_pct)

    return closed


# ═══════════════════════════════════════════════════════════════════════
#  PROFIT / LOSS LOCK WORKERS
# ═══════════════════════════════════════════════════════════════════════
def profit_lock_worker():
    log.info(
        f"🎯 Profit-lock monitor started — threshold={PROFIT_LOCK_PCT}%, "
        f"poll={EXIT_POLL_SEC}s, cooldown={PROFIT_LOCK_CD}s, "
        f"enabled={PROFIT_LOCK_ENABLED}"
    )
    while True:
        try:
            time.sleep(EXIT_POLL_SEC)
            if not _boot_done or not PROFIT_LOCK_ENABLED:
                continue
            if time.time() < _profit_lock_until:
                continue  # in cooldown
            if not market_open() or not active_trades:
                continue
            pnl, cost, pct = compute_combined_pnl()
            if pct is None or cost <= 0:
                continue
            log.info(
                f"🎯 profit-lock check: pnl=₹{pnl:.0f} cost=₹{cost:.0f} "
                f"net={pct:.2f}% (threshold={PROFIT_LOCK_PCT}%) "
                f"positions={len(active_trades)}"
            )
            if pct >= PROFIT_LOCK_PCT:
                close_all_positions(
                    trigger_reason=f"net premium PnL {pct:.2f}% ≥ {PROFIT_LOCK_PCT}%",
                    trigger_pct=pct,
                    lock_type="profit",
                )
        except Exception as e:
            log.error(f"profit-lock worker error: {e}", exc_info=True)


def loss_lock_worker():
    log.info(
        f"🛑 Loss-lock monitor started — threshold=-{LOSS_LOCK_PCT}%, "
        f"poll={EXIT_POLL_SEC}s, cooldown={LOSS_LOCK_CD}s, "
        f"enabled={LOSS_LOCK_ENABLED}"
    )
    while True:
        try:
            time.sleep(EXIT_POLL_SEC)
            if not _boot_done or not LOSS_LOCK_ENABLED:
                continue
            if time.time() < _loss_lock_until:
                continue
            if time.time() < _profit_lock_until:
                continue  # don't double-fire if profit lock just hit
            if not market_open() or not active_trades:
                continue
            pnl, cost, pct = compute_combined_pnl()
            if pct is None or cost <= 0:
                continue
            log.info(
                f"🛑 loss-lock check: pnl=₹{pnl:.0f} cost=₹{cost:.0f} "
                f"net={pct:.2f}% (threshold=-{LOSS_LOCK_PCT}%) "
                f"positions={len(active_trades)}"
            )
            if pct <= -LOSS_LOCK_PCT:
                close_all_positions(
                    trigger_reason=f"net premium PnL {pct:.2f}% ≤ -{LOSS_LOCK_PCT}%",
                    trigger_pct=None,
                    lock_type="loss",
                )
        except Exception as e:
            log.error(f"loss-lock worker error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════
#  TOKEN REFRESH — daily at 07:00 IST + boot-time self-heal
# ═══════════════════════════════════════════════════════════════════════
def token_refresh_loop():
    from token_refresh import refresh
    while True:
        try:
            n = now_ist()
            target = n.replace(hour=7, minute=0, second=0, microsecond=0)
            if n >= target:
                target = target + timedelta(days=1)
            sleep_sec = (target - n).total_seconds()
            log.info(
                f"⏰ Next token refresh at {target.isoformat()} "
                f"(sleeping {sleep_sec:.0f}s)"
            )
            time.sleep(sleep_sec)
            log.info("⏰ Running scheduled token refresh...")
            refresh()
            kite.reload_token()
            log.info("⏰ Scheduled token refresh OK")
        except Exception as e:
            log.error(f"Scheduled token refresh failed: {e}", exc_info=True)
            time.sleep(300)


def ensure_valid_token_at_boot():
    if kite.is_token_valid():
        log.info("✅ Existing token is valid")
        return
    log.warning("Cached token missing/expired — running boot-time refresh")
    try:
        from token_refresh import refresh
        refresh()
        kite.reload_token()
        if not kite.is_token_valid():
            raise RuntimeError("Token still invalid after refresh")
        log.info("✅ Boot-time token refresh OK")
    except Exception as e:
        log.error(f"❌ Boot-time token refresh failed: {e}", exc_info=True)
        log.error("Server will start but trading will fail until token is fixed")


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY EXECUTION (used by both fresh entry + flip-reopen paths)
# ═══════════════════════════════════════════════════════════════════════
def _do_entry(underlying: str, action: str, spot: float):
    """Open a fresh position. Caller has already cleared any same-symbol
    existing position (flip path) and acquired _trade_lock if needed.
    Resolves option → sizes 20% equity → places BUY market → records state."""
    # 1. Resolve contract
    try:
        contract = pick_option(kite, underlying, spot, action)
    except Exception as e:
        log.error(f"Resolver failed for {underlying}: {e}")
        log_trade_event(underlying, action, "entry", "REJECT", f"resolver: {e}")
        return jsonify({"status": "rejected", "reason": f"resolver: {e}"}), 200

    token = int(contract["instrument_token"])
    lot   = int(contract["lot_size"])

    # 2. Fetch live option premium for sizing (and verify token is real)
    try:
        ltp_data = kite.ltp([token])
        option_ltp = float(ltp_data[str(token)]["last_price"])
    except Exception as e:
        log.error(f"LTP fetch for sizing failed: {e}")
        return jsonify({"status": "rejected", "reason": f"ltp: {e}"}), 200
    if option_ltp <= 0:
        return jsonify({"status": "rejected", "reason": f"bad ltp {option_ltp}"}), 200

    # 3. Fetch fresh spot LTP (Pine's price is a hint; we anchor the ladder
    # to live spot at order time for accuracy)
    try:
        sk = spot_ltp_key(underlying)
        spot_data = kite.ltp([sk])
        live_spot = float(spot_data[sk]["last_price"])
    except Exception as e:
        log.warning(f"spot LTP fetch failed ({e}), falling back to Pine price {spot}")
        live_spot = spot

    # 4. Size
    lots = size_position(kite, option_ltp, lot)
    if lots <= 0:
        log_trade_event(underlying, action, "entry", "REJECT", "insufficient equity")
        return jsonify({
            "status": "rejected",
            "reason": "insufficient equity (calc < 1 lot)",
        }), 200

    qty = lots * lot

    # 5. Place market BUY
    try:
        oid = kite.place_market_order(
            tradingsymbol=contract["tradingsymbol"],
            transaction_type="BUY",
            quantity=qty,
            product=PRODUCT,
        )
    except Exception as e:
        log.error(f"Order placement failed: {e}", exc_info=True)
        log_trade_event(underlying, action, "entry", "REJECT", f"order: {e}")
        return jsonify({"status": "rejected", "reason": f"order: {e}"}), 200

    # 6. Confirm fill; fall back to LTP if no fill price in 10s
    fill_price = kite.get_order_avg_price(oid, timeout=10.0)
    if not fill_price or fill_price <= 0:
        log.warning(
            f"Order {oid} no fill price after 10s → using LTP {option_ltp} as proxy"
        )
        fill_price = option_ltp

    # 7. Record state — ladder will pick it up on its next cycle
    with _trade_lock:
        active_trades[underlying] = {
            "tradingsymbol":      contract["tradingsymbol"],
            "token":              token,
            "lot_size":           lot,
            "expiry":             str(contract["expiry"]),
            "option_type":        "CE" if action == "buy" else "PE",
            "side":               action,
            "lots_total":         lots,
            "lots_remaining":     lots,
            "qty":                qty,
            "premium_at_entry":   float(fill_price),
            "spot_at_entry":      float(live_spot),
            "entry_time":         time.time(),
            "order_id":           oid,
            "ladder_books":       0,
            "ladder_sl_spot":     _spot_sl_price(action, live_spot),
        }
        _save_active_trades()

    log.info(
        f"🚀 ENTRY {underlying} {action.upper()} → {contract['tradingsymbol']} "
        f"({lots} lots × {lot} = {qty}) @ premium {fill_price} spot {live_spot}"
    )
    log_trade_event(underlying, action, "entry", "FILLED",
                    f"{contract['tradingsymbol']} lots={lots} prem={fill_price}")
    return jsonify({
        "status":        "filled",
        "tradingsymbol": contract["tradingsymbol"],
        "lots":          lots,
        "qty":           qty,
        "premium":       fill_price,
        "spot":          live_spot,
        "expiry":        str(contract["expiry"]),
    }), 200


# ═══════════════════════════════════════════════════════════════════════
#  WEBHOOK — ENTRY ONLY (server is the brain)
# ═══════════════════════════════════════════════════════════════════════
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True, silent=True)
        if data is None:
            try:
                data = json.loads((request.data or b"{}").decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return jsonify({"status": "rejected", "reason": "invalid JSON body"}), 200
        log.info(f"📩 Webhook: {json.dumps(_redact(data))}")

        if not _secret_ok(data.get("secret")):
            return jsonify({"error": "unauthorized"}), 401

        action     = (data.get("action") or "").lower()
        symbol_raw = data.get("symbol") or ""
        alert_type = (data.get("type") or "entry").lower()
        spot       = float(data.get("price") or 0)

        if action not in ("buy", "sell"):
            return jsonify({"status": "rejected",
                            "reason": f"bad action {action!r}"}), 200
        if not symbol_raw:
            return jsonify({"status": "rejected", "reason": "missing symbol"}), 200

        # Entry-only — explicitly reject anything else. Pine is no longer
        # the brain; books/closes/reverses are server-owned.
        if alert_type != "entry":
            log.info(f"⛔ Non-entry alert {alert_type!r} rejected — server owns exits")
            return jsonify({
                "status": "rejected",
                "reason": f"alert type {alert_type!r} not accepted "
                          f"(server owns exits — only 'entry' is processed)"
            }), 200

        underlying = normalize_symbol(symbol_raw)
        log.info(f"⤷ ENTRY {action.upper()} {underlying} @ spot {spot}")

        # ─── Whitelist check ─────────────────────────────────
        if not is_tradable(underlying):
            log.info(f"🚫 Symbol {underlying} not in tradable whitelist")
            log_trade_event(underlying, action, "entry", "REJECT",
                            "symbol not in whitelist")
            return jsonify({
                "status": "rejected",
                "reason": f"symbol {underlying} not in tradable whitelist "
                          f"(see tradable_symbols.py)"
            }), 200

        # ─── Market-hours gate ───────────────────────────────
        if not market_open():
            return jsonify({"status": "rejected", "reason": "market closed"}), 200

        # ─── Daily cap ───────────────────────────────────────
        if daily_cap_active():
            log.info(
                f"🛑 DAILY CAP: rejecting entry for {underlying} "
                f"(cumulative {_daily_locked_pct_sum:.2f}% / cap {DAILY_CAP_PCT}%)"
            )
            log_trade_event(underlying, action, "entry", "DAILY_CAP",
                            f"sum={_daily_locked_pct_sum:.2f}%")
            return jsonify({
                "status": "rejected",
                "reason": f"daily cap ({_daily_locked_pct_sum:.2f}% ≥ {DAILY_CAP_PCT}%)"
            }), 200

        # ─── Cooldowns (profit, loss) ────────────────────────
        if time.time() < _profit_lock_until:
            remaining = int(_profit_lock_until - time.time())
            return jsonify({"status": "rejected",
                            "reason": f"profit-lock cooldown ({remaining}s)"}), 200
        if time.time() < _loss_lock_until:
            remaining = int(_loss_lock_until - time.time())
            return jsonify({"status": "rejected",
                            "reason": f"loss-lock cooldown ({remaining}s)"}), 200

        # ─── Streak pause ────────────────────────────────────
        if in_streak_pause():
            return jsonify({
                "status": "rejected",
                "reason": f"streak pause "
                          f"({_streak_profit_lock_count} consecutive profit locks, "
                          f"{streak_pause_remaining_sec()}s left)"
            }), 200

        # ─── SL lockout (per symbol) ─────────────────────────
        locked, remaining = in_sl_lockout(underlying)
        if locked:
            log.info(f"🚫 SL LOCKOUT: rejecting entry for {underlying} — {remaining}s left")
            log_trade_event(underlying, action, "entry", "SL_LOCKOUT",
                            f"{remaining}s remaining")
            return jsonify({"status": "rejected",
                            "reason": f"SL lockout ({remaining}s)"}), 200

        # ─── Main dispatch under _trade_lock ─────────────────
        with _trade_lock:
            # Already in a position on this symbol?
            existing = active_trades.get(underlying)
            if existing:
                if existing["side"] == action:
                    # Same direction → SKIP (don't stack)
                    mins = int((time.time() - existing["entry_time"]) // 60)
                    log.info(
                        f"🚫 SKIP entry: {underlying} already active ({mins}m, "
                        f"{existing['side']}, same direction)"
                    )
                    log_trade_event(underlying, action, "entry", "SKIP",
                                    f"already active ({mins}m, same dir)")
                    return jsonify({"status": "skipped",
                                    "reason": "already active (same direction)"}), 200

                # OPPOSITE direction → FLIP: close current option, open opposite type
                prev_side = existing["side"]
                prev_sym  = existing["tradingsymbol"]
                log.info(
                    f"🔄 FLIP: {underlying} {prev_side.upper()}→{action.upper()} "
                    f"(closing {prev_sym} then reopening as {action})"
                )

                ok = close_position(underlying, reason=f"flip {prev_side}→{action}")
                if not ok:
                    log.error(f"❌ FLIP close failed for {underlying} — aborting reopen")
                    log_trade_event(underlying, action, "flip_entry", "ABORT",
                                    "close failed")
                    return jsonify({
                        "status": "aborted",
                        "reason": "flip close failed; staying flat",
                    }), 200

                # Small settle wait — give Kite a moment to register the close
                # before we reopen on the opposite side. Don't hold the lock
                # over the sleep — release and re-acquire.
                pass  # we still hold _trade_lock; release-then-reacquire pattern below

            # NEW or post-flip: cap check before opening
            if MAX_POSITIONS > 0 and underlying not in active_trades \
                    and len(active_trades) >= MAX_POSITIONS:
                log.info(
                    f"🧮 POSITION CAP: rejecting {underlying} — "
                    f"{len(active_trades)}/{MAX_POSITIONS} open "
                    f"[{', '.join(active_trades.keys())}]"
                )
                log_trade_event(underlying, action, "entry", "POS_CAP",
                                f"{len(active_trades)}/{MAX_POSITIONS}")
                return jsonify({
                    "status": "rejected",
                    "reason": f"position cap ({len(active_trades)}/{MAX_POSITIONS})",
                }), 200

            # Place the entry. _do_entry releases lock internally for the
            # Kite API calls; safe under RLock.
            return _do_entry(underlying, action, spot)

    except Exception as e:
        log.error(f"Webhook handler error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════
#  ADMIN / STATUS ENDPOINTS (all require WEBHOOK_SECRET)
# ═══════════════════════════════════════════════════════════════════════
@app.route("/health", methods=["GET"])
def health():
    """Public — no secret. Returns counts only, no symbol list (per CHANGES.md)."""
    return jsonify({
        "ok":            True,
        "ts":            time.time(),
        "boot_done":     _boot_done,
        "active_count":  len(active_trades),
        "market_open":   market_open(),
    })


@app.route("/status", methods=["GET"])
def status():
    """Full status — requires secret."""
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403

    _check_and_reset_daily_counter()
    net_pnl, cost, net_pct = compute_combined_pnl()
    with _trade_lock:
        trades_view = {
            u: {
                "tradingsymbol":     t.get("tradingsymbol"),
                "option_type":       t.get("option_type"),
                "side":              t.get("side"),
                "lots_total":        t.get("lots_total"),
                "lots_remaining":    t.get("lots_remaining"),
                "qty":               t.get("qty"),
                "premium_at_entry":  t.get("premium_at_entry"),
                "spot_at_entry":     t.get("spot_at_entry"),
                "ladder_sl_spot":    t.get("ladder_sl_spot"),
                "ladder_books":      t.get("ladder_books", 0),
                "expiry":            t.get("expiry"),
                "age_min":           int((time.time() - t.get("entry_time", 0)) / 60),
            }
            for u, t in active_trades.items()
        }
    return jsonify({
        "active_trades":       trades_view,
        "active_count":        len(trades_view),
        "combined_pnl_inr":    net_pnl,
        "combined_cost_inr":   cost,
        "combined_pct":        net_pct,
        "profit_cooldown_sec": max(0, int(_profit_lock_until - time.time())),
        "loss_cooldown_sec":   max(0, int(_loss_lock_until - time.time())),
        "daily_locked_pct":    _daily_locked_pct_sum,
        "daily_lock_count":    _daily_lock_count,
        "daily_paused":        _daily_paused,
        "streak_count":        _streak_profit_lock_count,
        "streak_pause_sec":    streak_pause_remaining_sec(),
        "sl_lockouts": {
            u: max(0, int(t - time.time()))
            for u, t in _sl_lockout.items() if t > time.time()
        },
        "market_open":         market_open(),
        "ladder_last_check":   _ladder_last_check_at,
        "ladder_last_error":   _ladder_last_error,
        "ladder_closes":       _ladder_closes,
        "now_ist":             now_ist().isoformat(),
        "max_positions":       MAX_POSITIONS,
    })


@app.route("/equity", methods=["GET"])
def equity_endpoint():
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    return jsonify({"available_inr": get_available_equity(kite)})


@app.route("/close-all", methods=["POST"])
def close_all_endpoint():
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    closed = 0
    with _trade_lock:
        for u in list(active_trades.keys()):
            if close_position(u, reason="manual_close_all"):
                closed += 1
    return jsonify({"closed": closed})


@app.route("/close-one", methods=["POST"])
def close_one_endpoint():
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    payload = request.get_json(force=True, silent=True) or {}
    u = normalize_symbol(payload.get("symbol") or request.args.get("symbol") or "")
    if not u:
        return jsonify({"error": "missing symbol"}), 400
    ok = close_position(u, reason="manual")
    return jsonify({"closed": ok, "underlying": u})


@app.route("/locks/clear", methods=["POST"])
def clear_locks():
    global _profit_lock_until, _loss_lock_until
    global _daily_paused, _daily_locked_pct_sum, _daily_lock_count
    global _streak_profit_lock_count, _streak_pause_until
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    _profit_lock_until = 0.0
    _loss_lock_until   = 0.0
    _daily_paused      = False
    _daily_locked_pct_sum = 0.0
    _daily_lock_count = 0
    _streak_profit_lock_count = 0
    _streak_pause_until = 0.0
    _sl_lockout.clear()
    _save_streak()
    return jsonify({"ok": True})


@app.route("/refresh-instruments", methods=["POST"])
def refresh_instruments_endpoint():
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    rows = load_instruments(kite, force=True)
    return jsonify({"loaded": len(rows)})


@app.route("/underlyings", methods=["GET"])
def underlyings_endpoint():
    """Returns the symbols Kite currently has option chains for. Also flags
    which ones are in the tradable whitelist."""
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    kite_underlyings = list_underlyings(kite)
    return jsonify({
        "kite_underlyings":     kite_underlyings,
        "kite_count":           len(kite_underlyings),
        "tradable_whitelist":   sorted(TRADABLE_UNDERLYINGS),
        "whitelist_count":      len(TRADABLE_UNDERLYINGS),
        "in_kite_not_whitelist": sorted(set(kite_underlyings) - TRADABLE_UNDERLYINGS),
        "in_whitelist_not_kite": sorted(TRADABLE_UNDERLYINGS - set(kite_underlyings)),
    })


@app.route("/stats", methods=["GET"])
def stats():
    """Recent trade events (in-memory ring buffer)."""
    if not _secret_ok(_request_secret()):
        return jsonify({"error": "unauthorized"}), 403
    return jsonify({
        "log_count": len(trade_log),
        "events":    trade_log[-100:],
    })


# ═══════════════════════════════════════════════════════════════════════
#  BOOT
# ═══════════════════════════════════════════════════════════════════════
def boot():
    global _boot_done, _daily_counter_date

    log.info("=" * 70)
    log.info(" OPTIONS BOT — Kite Connect | Server-Owned Exits | Entry-Only Pine ")
    log.info("=" * 70)
    log.info(
        f"Ladder: SL={SL_PCT}% TP_STEP={TP_STEP_PCT}% BOOK={BOOK_PCT}%×{MAX_BOOKS} "
        f"PREMIUM_SL={PREMIUM_SL_PCT}% poll={EXIT_POLL_SEC}s"
    )
    log.info(
        f"Locks: PROFIT={PROFIT_LOCK_PCT}%/{PROFIT_LOCK_CD}s "
        f"LOSS={LOSS_LOCK_PCT}%/{LOSS_LOCK_CD}s "
        f"DAILY_CAP={DAILY_CAP_PCT}% STREAK={STREAK_PAUSE_COUNT}×{STREAK_PAUSE_SEC // 60}m"
    )
    log.info(
        f"Caps: MAX_POSITIONS={MAX_POSITIONS} EQUITY_PCT="
        f"{os.environ.get('EQUITY_PCT_PER_TRADE','20')}% EXPIRY_SAFETY={EXPIRY_SAFETY_DAYS}d"
    )
    log.info(f"Whitelist: {len(TRADABLE_UNDERLYINGS)} symbols ({len(INDEX_UNDERLYINGS)} indices)")

    _daily_counter_date = now_ist().date()

    ensure_valid_token_at_boot()
    _load_active_trades()
    _load_streak()
    try:
        load_instruments(kite, force=True)
    except Exception as e:
        log.error(f"Failed to load instruments at boot: {e}", exc_info=True)

    # Start background workers
    threading.Thread(target=ladder_worker,        daemon=True, name="ladder").start()
    threading.Thread(target=profit_lock_worker,   daemon=True, name="profit_lock").start()
    threading.Thread(target=loss_lock_worker,     daemon=True, name="loss_lock").start()

    if os.environ.get("KITE_TOTP_SECRET"):
        threading.Thread(target=token_refresh_loop, daemon=True, name="token_refresh").start()
    else:
        log.warning(
            "KITE_TOTP_SECRET not set — daily token refresh disabled. "
            "Manually update the token file each morning."
        )

    _boot_done = True
    log.info("✅ Boot complete")


# Run boot at import time so gunicorn workers and direct execution both work.
boot()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    log.info(f"Starting dev server on :{port}")
    app.run(host="0.0.0.0", port=port)
