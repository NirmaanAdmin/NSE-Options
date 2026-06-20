# Options Bot — Kite Connect (Server-Owned Exits)

Automated NSE options trading via Zerodha Kite Connect. Pine sends entry
signals; **the server owns the entire trade lifecycle after fill** —
trailing stop, partial books, the exit.

Ported from the CoinDCX crypto bot (`server2.py` reference) and adapted
for Indian equity options. Multi-stock + indices, NRML overnight carry,
20% equity per trade, ITM strikes.

---

## What changed from the previous ZLHMA build

| | Old ZLHMA bot | This build |
|---|---|---|
| Brain | Pine | **Server** |
| Pine emits | entry / book / close / reverse | **entry only** |
| Exits | Pine-driven via webhook | **Server-owned ladder** (poll-driven) |
| TP/SL anchor | Spot price signals from Pine | **Spot LTP polled live by server** |
| Re-entry on same symbol | Pine drove the reverse webhook | Opposite-side entry from Pine = **FLIP** (server closes + reopens) |
| Position cap | None | **`MAX_POSITIONS=5`** default |
| Per-position premium SL | None | **`PREMIUM_SL_PCT=50%`** backstop |
| Webhook auth | secret in JSON only | **`X-Auth-Secret` header** preferred (out of logs) |
| Gunicorn | default | **`--workers 1 --threads 8 --preload-app false`** (required) |

The previous ZLHMA package at `/mnt/user-data/outputs/zlhma-options-bot/`
is superseded by this one.

---

## Architecture

### Webhook contract (entry only)

`POST /webhook`

```json
{
  "secret":  "<WEBHOOK_SECRET>",
  "symbol":  "TATASTEEL",
  "action":  "buy",
  "type":    "entry",
  "price":   217.91
}
```

- `action=buy`  → server buys an **ITM CE** (call) — bets on spot up
- `action=sell` → server buys an **ITM PE** (put) — bets on spot down
- `type` must be `"entry"`; anything else is rejected (Pine is no longer the brain)
- `secret` can also be sent as `X-Auth-Secret` header (preferred — keeps it out of access logs)

### Per-symbol behaviour

| Pine signal | Server state on that symbol | Outcome |
|---|---|---|
| `action=buy`  | flat                | open CE position |
| `action=sell` | flat                | open PE position |
| `action=buy`  | already in CE (buy) | **SKIP** (same direction; no stacking) |
| `action=sell` | already in PE (sell)| **SKIP** |
| `action=buy`  | in PE (sell)        | **FLIP**: close PE, open CE |
| `action=sell` | in CE (buy)         | **FLIP**: close CE, open PE |

### Server-owned ladder (every `EXIT_POLL_SEC` seconds)

For each active position, the server polls the **underlying spot LTP**
and the **option premium LTP**, then runs three checks in order:

1. **Premium-SL backstop** — option premium fell `PREMIUM_SL_PCT`% below entry
   → close that position. (Catches IV crush / gap-down where spot didn't trip.)
2. **Spot-SL breach** — current spot crossed the trailing stop → close all
   remaining lots on this symbol, arm SL lockout.
3. **Advance** — count rungs crossed in profit (`TP_STEP_PCT`% steps on spot):
   - At each new rung up to `MAX_BOOKS`: SELL `BOOK_PCT`% of original lots
   - Ratchet trailing SL to one rung behind the highest rung crossed:
     - `k=0` → initial stop (entry ± `SL_PCT`%)
     - `k=1` → breakeven (entry)
     - `k=2` → entry ± `TP_STEP_PCT`% × 1
     - `k=3` → entry ± `TP_STEP_PCT`% × 2 …
   - SL **never loosens** — only moves toward profit (`max` for CE, `min` for PE)

After `MAX_BOOKS` books are done, no more books — pure trail until SL fires.

**Why spot-based rungs, not premium-based?** Option premium can swing
4% intraday without a real spot move (volatility noise). A spot-based
ladder mirrors what was actually backtested in the Pine strategy. The
premium-SL backstop covers the catastrophic-drop case.

### Combined PnL locks (premium basis)

`profit_lock_worker` and `loss_lock_worker` independently poll every
`EXIT_POLL_SEC`. They compute combined premium PnL across all open
positions: `Σ(option_ltp - premium_at_entry) × qty / Σ(premium_at_entry × qty)`.

- ≥ `PROFIT_LOCK_PCT`% → close ALL, arm profit cooldown, bump daily counter
- ≤ −`LOSS_LOCK_PCT`% → close ALL, arm loss cooldown, reset streak counter

### Safety stack (all toggleable)

- **Daily cap** — cumulative locked profit % resets at IST midnight; pauses entries when hit
- **Streak pause** — `N` consecutive profit locks → pause entries for `STREAK_PAUSE_SEC`
- **SL lockout** — per-symbol cooldown after ladder SL fires
- **Position cap** — `MAX_POSITIONS=5` ceiling on concurrent symbols (default)
- **Cooldowns** — independent profit/loss lock windows
- **Expiry safety sweep** — closes any position within `EXPIRY_SAFETY_DAYS` of expiry (runs off-hours too, so morning-of-expiry is consistent)
- **Market-hours gate** — entries rejected outside 09:15–15:30 IST Mon–Fri
- **Whitelist** — webhook symbol must be in `tradable_symbols.py` (214 underlyings)

### Token refresh

- At boot: if cached token is invalid, run full TOTP login flow
- Daily at 07:00 IST: scheduled refresh thread
- On any `TokenException` mid-call: reload token from disk and retry once

---

## Deployment to Railway

### 1. Create the Kite app

1. Go to https://developers.kite.trade → **Create new app**
2. App type: **Connect**
3. Redirect URL: `http://127.0.0.1` (headless setup; we don't run a callback server)
4. Save the `api_key` and `api_secret`

### 2. Get your TOTP secret

The `KITE_TOTP_SECRET` is the **base32 secret** from your Kite 2FA QR code,
NOT the 6-digit code. To extract:

1. In Kite web → Profile → Settings → Account → Security → External TOTP
2. When setting up TOTP, the QR contains a URL like
   `otpauth://totp/Kite:XX1234?secret=ABCDEFGHIJKLMNOP&...`
3. Copy the value after `secret=`. That's `KITE_TOTP_SECRET`.

If you already have TOTP set up and don't have the secret stored anywhere:
- Disable & re-enable TOTP in Kite to get a fresh QR
- Or use an authenticator app that lets you export secrets

### 3. Push to Railway

```bash
git init && git add . && git commit -m "options bot — server-owned exits"
# create a new Railway project, connect this repo
```

### 4. Set env vars in Railway

Copy from `.env.example`. **Required:**

```
KITE_API_KEY        = <from step 1>
KITE_API_SECRET     = <from step 1>
KITE_USER_ID        = ZX1234  (your Kite user id)
KITE_PASSWORD       = <your Kite password>
KITE_TOTP_SECRET    = <from step 2, base32 string>
WEBHOOK_SECRET      = <any strong random string>
```

Optional tuning vars all have sensible defaults in server.py — see
`.env.example` for the full list.

### 5. Mount a volume

In Railway: **Settings → Volumes → Attach Volume**. Mount at `/app/data`.
Without this, `active_trades.json` / `streak_state.json` / `kite_access_token.txt`
all get wiped on every redeploy.

### 6. Verify boot

After deploy, hit `https://<your-app>.up.railway.app/health` — should return:

```json
{"ok": true, "boot_done": true, "active_count": 0, "market_open": false}
```

Then with the secret: `GET /status?secret=<WEBHOOK_SECRET>` — should
return the full state including `max_positions: 5`.

### 7. Set up Pine alerts

1. In TradingView, open any NSE underlying chart (e.g. `NSE:TATASTEEL`)
2. Paste `pine/fun2sh_options.pine` into the Pine editor and Add to chart
3. Set inputs:
   - **Webhook Secret**: same as `WEBHOOK_SECRET`
   - **Start Direction**: Long or Short (initial bias)
   - **Underlying Override**: leave blank — auto-derives from `syminfo.ticker`
4. Right-click any signal → **Add alert**
   - Condition: the script's plot (use **Any alert() function call**)
   - Webhook URL: `https://<your-app>.up.railway.app/webhook`
   - Message: leave empty (the script builds the JSON)
5. Repeat for each symbol you want to trade (one alert per chart per symbol)

### 8. Test on a single symbol first

Recommended: start with **NIFTY** or **BANKNIFTY** (weekly cash-settled,
tight spreads). Reduce `EQUITY_PCT_PER_TRADE` to 5–10% for the first few
days, watch the logs, then scale up.

---

## Endpoints reference

All require `?secret=<WEBHOOK_SECRET>` or `X-Auth-Secret: <WEBHOOK_SECRET>`
EXCEPT `/health` which is public (counts only, no symbol leak).

| Endpoint | Method | What it does |
|---|---|---|
| `/health` | GET | Public uptime check; counts + boot/market flags |
| `/webhook` | POST | Pine entry signal handler |
| `/status` | GET | Full state: active trades, locks, streak, daily cap, SL lockouts, ladder health |
| `/equity` | GET | Currently available INR equity from Kite margins |
| `/close-all` | POST | Manual emergency close of every position |
| `/close-one` | POST | Close one symbol; body or `?symbol=NAME` |
| `/locks/clear` | POST | Clears all cooldowns, daily cap, streak, SL lockouts (manual override) |
| `/refresh-instruments` | POST | Force-refresh NFO instruments dump |
| `/underlyings` | GET | Diff between Kite's live NFO names and the whitelist |
| `/stats` | GET | Last 100 trade events from the in-memory log |

---

## Key tunables (start here)

Most of these are fine at defaults. Tune deliberately, one at a time:

| Var | Default | Effect |
|---|---|---|
| `MAX_POSITIONS` | 5 | Cap on concurrent symbols. Lower = more conservative, higher = more diversification but smaller per-position size due to sliding 20% equity pool |
| `EQUITY_PCT_PER_TRADE` | 20 | % of available equity per trade. Pair with MAX_POSITIONS — at 20%×5 you can theoretically hold 5 full-sized positions before sizing rejects |
| `SL_PCT` | 3.0 | Initial spot SL distance. Wider = more breathing room, more loss/trade |
| `TP_STEP_PCT` | 4.0 | Rung spacing on spot. Tighter = books faster, trails closer |
| `BOOK_PCT` × `MAX_BOOKS` | 33% × 2 | Two partial books of 1/3 each → 1/3 left for the trail. Set MAX_BOOKS=0 for pure trail / no books |
| `PREMIUM_SL_PCT` | 50.0 | Per-position premium-drop backstop. Lower = exits faster on IV crush, but risks shaking out on volatility |
| `PROFIT_LOCK_PCT` | 30.0 | Combined premium-PnL profit lock |
| `LOSS_LOCK_PCT` | 25.0 | Combined premium-PnL loss lock |
| `DAILY_CAP_PCT` | 50.0 | Cumulative locked profit % per IST day |
| `STRIKE_OFFSET` | 0 | 0 = ITM-by-one. +1 = ATM. -1 = deeper ITM. Options closer to ATM = higher gamma but higher decay risk |
| `EXPIRY_SAFETY_DAYS` | 1 | Days from expiry to refuse new entries / force-close. **Non-negotiable** for stock options (physical settlement risk) |

---

## What "tweaks later" means

Per your note, the Pine script + this server are the v1 contract. Likely
follow-up adjustments (in rough order):

1. Watch `LADDER_VERBOSE=true` logs in real markets — does spot-based
   trailing actually capture trends, or is `TP_STEP_PCT=4` too tight/loose
   for high-IV symbols?
2. If `PREMIUM_SL_PCT=50` triggers too often → loosen to 60–70%, or just
   trust the spot SL alone (set premium SL very wide).
3. The 20% × 5-positions math assumes equal sizing. Reality: first trade
   sizes off full equity, fifth trade sizes off ~20% remaining equity, so
   you'll naturally see size taper across concurrent positions. If this
   becomes annoying, switch to fixed-INR-per-trade sizing.
4. Index vs stock: stock options have physical settlement and chunkier
   lot sizes. Indices have weekly expiries. Mix per your preference.

---

## Files

```
options-bot/
├── server.py              # main Flask app, all workers, ladder, webhook handler
├── kite_client.py         # KiteConnect SDK wrapper; token-reload retry; bulk LTP
├── token_refresh.py       # daily TOTP login flow, writes to disk
├── instruments.py         # NFO instruments cache (6h TTL)
├── option_resolver.py     # ITM strike picker with STRIKE_OFFSET
├── position_sizer.py      # 20% available equity sizing
├── tradable_symbols.py    # whitelist: 214 NSE F&O symbols (5 indices + 209 stocks)
├── pine/
│   └── fun2sh_options.pine  # entry-only Pine for any NSE chart
├── Procfile               # gunicorn --workers 1 --threads 8 --preload-app false
├── runtime.txt            # python-3.11.10
├── requirements.txt       # Flask, gunicorn, kiteconnect, pyotp, requests, pytz
├── .env.example           # all env vars documented
└── README.md              # this file
```
