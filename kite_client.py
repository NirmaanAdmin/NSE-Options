"""
kite_client.py
──────────────
Thin wrapper around KiteConnect SDK with:
  - Lazy access_token reload from disk
  - Auto-retry on TokenException (re-reads token, retries once)
  - Convenience methods for the order shapes we actually use
  - Order fill confirmation by polling orderbook
  - Bulk LTP fetch for both option tokens (int) AND spot strings ("NSE:RELIANCE")

On TokenException we re-read the file once (in case a concurrent refresh
happened). If still bad, surface the exception so the worker can pause.
"""
import os
import time
import logging
from typing import Optional, Union, List, Dict
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException

log = logging.getLogger("kite.client")

API_KEY    = os.environ.get("KITE_API_KEY", "")
TOKEN_PATH = os.environ.get("ACCESS_TOKEN_PATH", "/app/data/kite_access_token.txt")


class KiteClient:
    def __init__(self):
        if not API_KEY:
            raise RuntimeError("KITE_API_KEY env var is required")
        self.kite = KiteConnect(api_key=API_KEY)
        self._load_token(required=False)

    # ─── Token management ────────────────────────────────────────────
    def _load_token(self, required: bool = True) -> bool:
        try:
            with open(TOKEN_PATH) as f:
                tok = f.read().strip()
            if not tok:
                raise RuntimeError("Token file is empty")
            self.kite.set_access_token(tok)
            log.info(f"Loaded access_token from {TOKEN_PATH}")
            return True
        except FileNotFoundError:
            msg = f"{TOKEN_PATH} not found — run token_refresh.py"
            if required:
                log.error(msg)
                raise
            log.warning(msg)
            return False
        except Exception as e:
            log.error(f"Failed to load token: {e}")
            if required:
                raise
            return False

    def reload_token(self) -> bool:
        return self._load_token(required=False)

    def _call(self, fn, *args, **kwargs):
        """Call a KiteConnect method with one-shot retry on TokenException."""
        try:
            return fn(*args, **kwargs)
        except TokenException as e:
            log.warning(f"TokenException ({e}) — reloading token and retrying once")
            if not self._load_token(required=False):
                raise
            return fn(*args, **kwargs)

    def is_token_valid(self) -> bool:
        try:
            self.kite.profile()
            return True
        except TokenException:
            return False
        except Exception as e:
            log.error(f"is_token_valid: unexpected error {e}")
            return False

    # ─── Orders ──────────────────────────────────────────────────────
    def place_market_order(
        self,
        tradingsymbol: str,
        transaction_type: str,
        quantity: int,
        product: str = "NRML",
        exchange: str = "NFO",
    ) -> str:
        log.info(
            f"📤 ORDER {transaction_type} MKT {quantity} {tradingsymbol} "
            f"({product} / {exchange})"
        )
        oid = self._call(
            self.kite.place_order,
            variety=self.kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transaction_type,
            quantity=int(quantity),
            product=product,
            order_type=self.kite.ORDER_TYPE_MARKET,
            validity=self.kite.VALIDITY_DAY,
        )
        log.info(f"   order_id = {oid}")
        return oid

    def orders(self) -> list:
        return self._call(self.kite.orders)

    def get_order_avg_price(self, order_id: str, timeout: float = 10.0) -> Optional[float]:
        """Poll orderbook for fill confirmation. Returns avg fill price or None."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                for o in self.orders():
                    if str(o.get("order_id")) != str(order_id):
                        continue
                    status = o.get("status", "")
                    if status == "COMPLETE":
                        ap = float(o.get("average_price", 0) or 0)
                        log.info(f"   order {order_id} filled @ {ap}")
                        return ap
                    if status in ("REJECTED", "CANCELLED"):
                        log.error(
                            f"   order {order_id} ended {status}: "
                            f"{o.get('status_message', '?')}"
                        )
                        return None
            except Exception as e:
                log.error(f"orders() poll error: {e}")
            time.sleep(0.5)
        log.warning(f"   order {order_id} fill timeout after {timeout}s")
        return None

    # ─── Positions / margins ─────────────────────────────────────────
    def positions(self) -> dict:
        return self._call(self.kite.positions)

    def margins(self, segment: str = "equity") -> dict:
        return self._call(self.kite.margins, segment=segment)

    # ─── Market data ─────────────────────────────────────────────────
    def ltp(self, instruments: Union[List[int], List[str]]) -> dict:
        """Returns dict keyed by str(int) when ints are passed, or by
        'EXCHANGE:TRADINGSYMBOL' when strings are passed.

        Per Kite API, you can mix ints and strings in one call BUT the
        return dict will have inconsistent keys. So we recommend separating
        calls by key-type if you need to look results up by either.
        """
        return self._call(self.kite.ltp, instruments)

    def bulk_ltp(self, items: list) -> Dict[str, float]:
        """Convenience: returns {key_string: last_price} for whatever you
        passed. Handles both int tokens and 'EXCHANGE:TRADINGSYMBOL' strings.
        Strips out missing/bad entries. Returns {} on outright failure."""
        if not items:
            return {}
        try:
            raw = self.ltp(items)
        except Exception as e:
            log.error(f"bulk_ltp failed: {e}")
            return {}
        out = {}
        for k, v in (raw or {}).items():
            try:
                px = float((v or {}).get("last_price", 0))
                if px > 0:
                    out[str(k)] = px
            except (TypeError, ValueError):
                continue
        return out

    def quote(self, instruments: Union[List[int], List[str]]) -> dict:
        return self._call(self.kite.quote, instruments)

    # ─── Instruments dump ────────────────────────────────────────────
    def instruments(self, exchange: str = "NFO") -> list:
        return self._call(self.kite.instruments, exchange)
