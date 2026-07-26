"""Overnight (20:00-04:00 ET) 1-min prices via TradingView — Blue Ocean ATS data.

The premise is that TradingView carries the Blue Ocean overnight session on its
main US-stock symbols, so a logged-in extended-session pull returns real
overnight trades. This module fills ONLY the 20:00->04:00 gap on the 24-h
price+density chart; Finviz stays authoritative for the 04:00-20:00 session.

STATUS 2026-07-26 — that premise does not hold as deployed, measured, not
assumed: an anonymous extended-session pull returns bars ONLY for 04:00-20:00
ET (bars-per-UTC-hour is zero across 00:00-07:59, i.e. the whole overnight
window) for TSLA and NVDA, both of which print essentially every post-market
minute. Neither the `backend` nor the `chart-service` Railway service sets
TV_USERNAME / TV_PASSWORD, so _client() is on the anonymous fallback. Until
those are set AND a pull is re-measured, overnight_bars() correctly returns []
for every ticker and the chart draws the dashed prev-close carry across the
gap. That empty gap is the honest answer, not a bug to route around.

Data path is the unofficial `tvDatafeed` package (TradingView has no official
data API). Treat as best-effort and fragile by design:
  * lazy import — the service runs fine if the package isn't installed
  * any failure returns [] and the chart falls back to the dashed
    prev-close carry
  * TV_USERNAME / TV_PASSWORD come from chart-service/.env (gitignored,
    never logged) — overnight coverage needs the owner's paid TV account
  * thin tape: overnight minutes without trades simply have no bar

Timezone note (this was wrong once — read before touching _to_et_naive):
tvDatafeed builds its index with `datetime.datetime.fromtimestamp(ts)` (see
tvDatafeed/main.py), which is naive and in the HOST PROCESS's local timezone —
NOT the exchange's. The deploy container sets no TZ, so it is UTC. The original
code assumed the naive index was already ET and passed it through untouched,
which shifted every bar +4h (EDT) and silently republished the PRIOR session's
16:00-20:00 post-market as "overnight" — data Finviz already covers — while the
real 20:00-04:00 window was never selected at all. Naive values are therefore
interpreted in the host's local zone (the inverse of what fromtimestamp did)
and converted to ET; they are never passed through.

That shift was invisible for months because a calm ticker's 16:00-20:00 tape
looks plausible plotted at 20:00-24:00. `verify_alignment` is the guard: pass
the session's known Finviz closes to overnight_bars() and any future re-shift
is caught against real overlap bars instead of shipping wrong prices.
"""
import os
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

EDT = ZoneInfo("America/New_York")

_lock = threading.Lock()
_tv = None                       # singleton TvDatafeed (one login per process)
_exchange_cache: dict = {}       # ticker -> resolved exchange
_cache: dict = {}                # (ticker, date) -> {"ts": epoch, "bars": [...]}
_TTL = 300

_EXCHANGES = ("NASDAQ", "NYSE", "AMEX")   # tried in order per ticker

# Alignment guard (see the timezone note above). Bars that fall inside the
# Finviz-covered session are compared against Finviz's own closes for the same
# HH:MM; a real misalignment moves prices far more than intraday venue noise.
_ALIGN_TOL = 0.005          # 0.5% median relative error
_ALIGN_MIN_SAMPLES = 20     # below this the overlap is too thin to judge


def available() -> bool:
    """True if the tvDatafeed package is importable (installed)."""
    try:
        import tvDatafeed  # noqa: F401
        return True
    except Exception:
        return False


def _client():
    """One logged-in TvDatafeed per process. Anonymous fallback works but
    almost certainly lacks the overnight session."""
    global _tv
    with _lock:
        if _tv is not None:
            return _tv
        from tvDatafeed import TvDatafeed
        user = os.environ.get("TV_USERNAME")
        pw = os.environ.get("TV_PASSWORD")
        _tv = TvDatafeed(user, pw) if user and pw else TvDatafeed()
        return _tv


def _to_et_naive(idx_val):
    """Normalize a bar timestamp to naive ET wall-clock.

    A naive value is whatever `datetime.fromtimestamp()` produced inside this
    process, i.e. host-local time — UTC in the container, ET on a dev Mac. The
    bare `.astimezone()` attaches exactly that zone (with the right DST offset
    for the date), which is the correct inverse regardless of how the host is
    configured; interpreting naive as ET is what caused the +4h bug.
    """
    dt = idx_val.to_pydatetime() if hasattr(idx_val, "to_pydatetime") else idx_val
    if dt.tzinfo is None:
        dt = dt.astimezone()          # attach the host zone fromtimestamp used
    return dt.astimezone(EDT).replace(tzinfo=None)


def _fetch_df(ticker: str):
    """1-min extended-session bars, resolving the exchange on first use."""
    from tvDatafeed import Interval
    tv = _client()
    tried = ([_exchange_cache[ticker]] if ticker in _exchange_cache
             else list(_EXCHANGES))
    for exch in tried:
        try:
            df = tv.get_hist(symbol=ticker, exchange=exch,
                             interval=Interval.in_1_minute,
                             n_bars=3000, extended_session=True)
        except Exception:
            df = None
        if df is not None and len(df):
            _exchange_cache[ticker] = exch
            return df
    return None


def verify_alignment(overlap: list, session_closes: dict):
    """Compare TV bars that land inside the Finviz-covered session against
    Finviz's own close for the same HH:MM. Returns (ok, detail).

    This is the regression guard for the +4h bug: a whole-hours timezone shift
    lands each bar on a minute it never traded at, and on any ticker that
    actually moves that shows up immediately as a large median error. It is a
    tripwire, not a proof — a flat tape can survive a shift within tolerance —
    so it is deliberately paired with the deterministic fix in _to_et_naive
    rather than relied on alone. ok=True with a "unverified" detail means the
    overlap was too thin to judge, never that alignment was confirmed.
    """
    if not session_closes:
        return True, "unverified: no Finviz session closes supplied"
    errs = []
    for b in overlap:
        ref = session_closes.get(b["ts"].strftime("%H:%M"))
        if ref:
            errs.append(abs(b["close"] - ref) / ref)
    if len(errs) < _ALIGN_MIN_SAMPLES:
        return True, f"unverified: only {len(errs)} overlap bars"
    errs.sort()
    med = errs[len(errs) // 2]
    detail = f"{len(errs)} overlap bars, median error {med * 100:.3f}%"
    return med <= _ALIGN_TOL, detail


def overnight_bars(ticker: str, date_str: str, session_closes: dict = None) -> list:
    """Real overnight bars for the 24-h window of date_str: naive-ET closes in
    [date-1 20:00, date 04:00). Returns [{"ts": dt, "close": f, "volume": f}]
    sorted by time; [] on any failure (caller falls back to prev-close carry).

    session_closes maps "HH:MM" -> Finviz close for date_str's 04:00-20:00
    session. When supplied, bars landing in that window are checked against it
    and a failed check discards the whole pull — publishing no overnight data
    beats publishing misaligned prices that look real."""
    key = (ticker, date_str)
    hit = _cache.get(key)
    if hit and time.time() - hit["ts"] < _TTL:
        return hit["bars"]

    try:
        d0 = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return []
    start = d0 - timedelta(hours=4)              # date-1 20:00
    end = d0 + timedelta(hours=4)                # date   04:00
    sess_start = d0 + timedelta(hours=4)         # date   04:00
    sess_end = d0 + timedelta(hours=20)          # date   20:00

    try:
        df = _fetch_df(ticker)
    except Exception:
        df = None
    if df is None or not len(df):
        return []

    # One pass, two windows: the overnight bars we want, and the bars that fall
    # inside Finviz's session — those are only ever used to verify alignment,
    # never published (Finviz stays authoritative for 04:00-20:00).
    overlap = []
    bars = []
    for idx, row in df.iterrows():
        dt = _to_et_naive(idx)
        if sess_start <= dt < sess_end:
            try:
                overlap.append({"ts": dt, "close": float(row["close"])})
            except Exception:
                pass
            continue
        if start <= dt < end:
            try:
                bars.append({"ts": dt, "close": float(row["close"]),
                             "volume": float(row.get("volume") or 0)})
            except Exception:
                continue
    bars.sort(key=lambda b: b["ts"])

    ok, detail = verify_alignment(overlap, session_closes)
    if not ok:
        print(f"  [tv_overnight] ALIGNMENT CHECK FAILED {ticker}|{date_str}: "
              f"{detail} — discarding {len(bars)} overnight bars. TV timestamps "
              f"are likely shifted; see the timezone note in tv_overnight.py.")
        bars = []
    elif bars:
        print(f"  [tv_overnight] {ticker}|{date_str}: {len(bars)} overnight bars ({detail})")

    _cache[key] = {"ts": time.time(), "bars": bars}
    return bars
