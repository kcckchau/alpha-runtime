"""
Historical data loader — fetches 1-minute bars from IBKR and converts
them into Candle domain objects with historical=True.

Responsibilities:
- Call ib_insync reqHistoricalDataAsync with correct duration / useRTH flags.
- Classify each bar's session (premarket / RTH / afterhours).
- Compute per-session, per-day VWAP using bar.average (WAP) when available.
- Assign deterministic IDs so repeated loads are safe upserts.

VWAP accuracy:
  IBKR historical bars include `average` (WAP = weighted avg price for the
  period).  We use that directly instead of the (H+L+C)/3 approximation.
  VWAP resets at every RTH open AND at every calendar-day boundary so
  multi-day backtracks never bleed across days.

Date iteration:
  We iterate calendar days backwards and *count trading days* (Mon–Fri,
  skipping weekends).  A proper NYSE holiday calendar is not used here —
  if the market was closed the IBKR request simply returns zero bars, which
  we log and skip.
"""

from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone, timedelta
from typing import Optional, TYPE_CHECKING

import pytz
import structlog

from packages.core.models import (
    Candle,
    SessionType,
    Timeframe,
    candle_stable_id,
)
from runtime.market_data.ibkr.session_classifier import classify_session

if TYPE_CHECKING:
    from ib_insync import IB, Contract

logger = structlog.get_logger(__name__)

# Pause between historical data requests to respect IBKR pacing rules
# (IBKR allows ~60 req per 10 min; 0.25 s is conservative)
_REQUEST_PAUSE_S = 0.25
_ET = pytz.timezone("America/New_York")


def _bar_to_utc(bar_date: object) -> datetime:
    """Normalise ib_insync BarData.date to a UTC-aware datetime."""
    if isinstance(bar_date, datetime):
        if bar_date.tzinfo is None:
            return bar_date.replace(tzinfo=timezone.utc)
        return bar_date.astimezone(timezone.utc)
    # String form when formatDate=1: "YYYYMMDD  HH:MM:SS" (ET)
    raw = str(bar_date).strip().replace("  ", " ")
    try:
        dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S")
    except ValueError:
        dt = datetime.strptime(raw, "%Y%m%d")
    return _ET.localize(dt).astimezone(timezone.utc)


def _bar_wap(bar: object, high: float, low: float, close: float) -> float:
    """
    Return the best available weighted average price for a bar.

    IBKR historical bars expose `average` (= Σ(price×qty)/Σqty for that
    period) which is more accurate than the (H+L+C)/3 typical-price proxy.
    Falls back to typical price when `average` is missing or zero.
    """
    wap = getattr(bar, "average", None)
    if wap and float(wap) > 0:
        return float(wap)
    return (high + low + close) / 3.0


def _bars_to_candles(
    raw_bars: list,
    symbol: str,
    timeframe: Timeframe = Timeframe.MIN_1,
) -> list[Candle]:
    """
    Convert a list of ib_insync BarData objects to Candle domain objects.

    VWAP resets:
    - At a PREMARKET→RTH session transition.
    - At a calendar date boundary (so multi-day fetches don't bleed across days).
    - At the start of a new PREMARKET session (new day's premarket).
    """
    candles: list[Candle] = []
    cum_volume = 0.0
    cum_turnover = 0.0
    prev_session: Optional[SessionType] = None
    prev_date_et: Optional[str] = None  # "YYYY-MM-DD" in ET

    for bar in raw_bars:
        ts_utc = _bar_to_utc(bar.date)
        session = classify_session(ts_utc)
        date_et = ts_utc.astimezone(_ET).strftime("%Y-%m-%d")

        # Reset accumulators at day boundary OR session-start boundary
        new_day = prev_date_et is not None and date_et != prev_date_et
        new_session = (
            session != prev_session
            and session in (SessionType.PREMARKET, SessionType.RTH)
        )
        if new_day or new_session:
            cum_volume = 0.0
            cum_turnover = 0.0

        prev_session = session
        prev_date_et = date_et

        volume = float(getattr(bar, "volume", 0) or 0)
        open_ = float(bar.open)
        high = float(bar.high)
        low = float(bar.low)
        close = float(bar.close)

        wap = _bar_wap(bar, high, low, close)
        cum_volume += volume
        cum_turnover += wap * volume

        vwap = cum_turnover / cum_volume if cum_volume > 0 else close

        candle = Candle(
            id=candle_stable_id(symbol, timeframe, ts_utc),
            symbol=symbol,
            timeframe=timeframe,
            timestamp=ts_utc,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            vwap=vwap,
            cumulative_volume=cum_volume,
            cumulative_turnover=cum_turnover,
            session=session,
            is_partial=False,
            historical=True,
        )
        candles.append(candle)

    return candles


_AGGREGATE_PERIOD_SECONDS: dict[Timeframe, int] = {
    Timeframe.MIN_5:  300,
    Timeframe.HOUR_1: 3600,
}


def aggregate_candles(
    candles_1m: list[Candle],
    timeframe: Timeframe,
) -> list[Candle]:
    """
    Aggregate a sorted list of 1m Candle objects into a higher timeframe.

    Bucketing is on UTC period boundaries (floor(epoch / period_seconds)).
    Each resulting bar:
      - open / high / low / close  — from the constituent 1m bars
      - volume                     — sum of constituent volumes
      - vwap                       — session-cumulative VWAP at the close of the
                                     period (last constituent bar's vwap), which
                                     is the standard institutional VWAP definition
      - cumulative_volume/turnover — last constituent bar's running totals
      - session                    — first constituent bar's session (period open)
      - historical                 — always True
    """
    if not candles_1m:
        return []

    period_sec = _AGGREGATE_PERIOD_SECONDS.get(timeframe)
    if period_sec is None:
        raise ValueError(f"aggregate_candles: unsupported timeframe {timeframe!r}")

    buckets: dict[datetime, list[Candle]] = {}
    for c in candles_1m:
        bucket_epoch = math.floor(c.timestamp.timestamp() / period_sec) * period_sec
        bucket_ts = datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)
        buckets.setdefault(bucket_ts, []).append(c)

    result: list[Candle] = []
    for bucket_ts in sorted(buckets):
        bars = buckets[bucket_ts]
        first, last = bars[0], bars[-1]
        result.append(Candle(
            id=candle_stable_id(first.symbol, timeframe, bucket_ts),
            symbol=first.symbol,
            timeframe=timeframe,
            timestamp=bucket_ts,
            open=first.open,
            high=max(b.high for b in bars),
            low=min(b.low  for b in bars),
            close=last.close,
            volume=sum(b.volume for b in bars),
            vwap=last.vwap,
            cumulative_volume=last.cumulative_volume,
            cumulative_turnover=last.cumulative_turnover,
            session=first.session,
            is_partial=False,
            historical=True,
        ))

    return result


def _today_fetch_duration(now_utc: datetime, premarket_start_hour: int = 4) -> str:
    """
    Return an IBKR durationStr (in seconds) that exactly covers the period from
    today's premarket open (premarket_start_hour ET) to *now_utc*.

    Using explicit seconds instead of "1 D" avoids IBKR's ambiguous "trading day"
    boundary which can leave the first 30-60 minutes of premarket un-fetched.

    Clamped to:
      • minimum 3 600 S  (1 hour  — safety floor)
      • maximum 57 600 S (16 hours — full 04:00-20:00 ET extended session)
    """
    now_et = now_utc.astimezone(_ET)
    pm_start = now_et.replace(
        hour=premarket_start_hour, minute=0, second=0, microsecond=0
    )
    # If we're before today's premarket start (e.g. running at 03:00 ET),
    # roll back to yesterday's premarket anchor.
    if now_et < pm_start:
        pm_start -= timedelta(days=1)

    elapsed = int((now_et - pm_start).total_seconds()) + 120   # +2 min buffer
    elapsed = max(3_600, min(57_600, elapsed))
    return f"{elapsed} S"


def _last_n_trading_days(n: int, reference: datetime) -> list[datetime]:
    """
    Return the ET midnight datetimes for the last *n* weekday (Mon–Fri)
    calendar days before *reference* (not including reference itself).

    Note: does not filter NYSE holidays — zero-bar IBKR responses from
    holiday requests are handled by the caller.
    """
    results: list[datetime] = []
    ref_et = reference.astimezone(_ET)
    offset = 1
    while len(results) < n:
        candidate = ref_et - timedelta(days=offset)
        offset += 1
        if candidate.weekday() < 5:  # Mon=0 … Fri=4
            results.append(candidate)
    return results


async def fetch_symbol_history(
    ib: "IB",
    contract: "Contract",
    symbol: str,
    lookback_days: int = 3,
    bar_size: str = "1 min",
    what_to_show: str = "TRADES",
    premarket_start_hour: int = 4,
) -> list[Candle]:
    """
    Fetch historical 1-minute bars for *symbol*:
      - Today: full session including premarket from 04:00 ET (useRTH=False)
      - Prior N *trading* days: RTH only (useRTH=True), skipping weekends.

    Returns all candles sorted by timestamp ascending, deduplicated by
    (symbol, timeframe, timestamp).
    """
    all_candles: list[Candle] = []
    now_utc = datetime.now(tz=timezone.utc)

    # ------------------------------------------------------------------ #
    # 1. Today's session: premarket (04:00 ET) through now                 #
    # ------------------------------------------------------------------ #
    # Use exact-seconds duration so IBKR returns bars from the premarket
    # open (04:00 ET) without being cut off by its "trading day" boundary.
    today_duration = _today_fetch_duration(now_utc, premarket_start_hour)
    logger.debug("bootstrap.today_duration", symbol=symbol, duration=today_duration)
    try:
        today_bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",          # empty = now
            durationStr=today_duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=False,            # include premarket
            formatDate=2,            # returns UTC datetime objects
            keepUpToDate=False,
        )
        today_candles = _bars_to_candles(today_bars, symbol)
        logger.info(
            "bootstrap.today_loaded",
            symbol=symbol,
            bar_count=len(today_candles),
            first_bar=today_candles[0].timestamp.isoformat() if today_candles else None,
            last_bar=today_candles[-1].timestamp.isoformat() if today_candles else None,
        )
        all_candles.extend(today_candles)
    except Exception:
        logger.exception("bootstrap.today_fetch_error", symbol=symbol)

    await asyncio.sleep(_REQUEST_PAUSE_S)

    # ------------------------------------------------------------------ #
    # 2. Prior N *trading* days: RTH only                                  #
    # ------------------------------------------------------------------ #
    prior_days = _last_n_trading_days(lookback_days, now_utc)

    for prior_day_et in prior_days:
        # Build end-of-RTH datetime: strip tzinfo, set 16:00, re-localize so
        # pytz recalculates the correct DST offset (plain .replace() keeps the
        # old UTC offset which is wrong across DST transitions).
        naive_16 = prior_day_et.replace(tzinfo=None).replace(
            hour=16, minute=0, second=0, microsecond=0
        )
        end_et = _ET.localize(naive_16, is_dst=False)
        # IBKR expects ET local time: "YYYYMMDD HH:MM:SS"
        end_str = end_et.strftime("%Y%m%d %H:%M:%S")

        try:
            prior_bars = await ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_str,
                durationStr="1 D",
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=True,          # RTH only for prior days
                formatDate=2,
                keepUpToDate=False,
            )
            if not prior_bars:
                logger.info(
                    "bootstrap.prior_day_no_bars",
                    symbol=symbol,
                    date=prior_day_et.strftime("%Y-%m-%d"),
                    note="possible holiday or no data",
                )
                await asyncio.sleep(_REQUEST_PAUSE_S)
                continue

            prior_candles = _bars_to_candles(prior_bars, symbol)
            logger.info(
                "bootstrap.prior_day_loaded",
                symbol=symbol,
                date=prior_day_et.strftime("%Y-%m-%d"),
                bar_count=len(prior_candles),
                first_bar=prior_candles[0].timestamp.isoformat() if prior_candles else None,
            )
            all_candles.extend(prior_candles)
        except Exception:
            logger.exception(
                "bootstrap.prior_day_fetch_error",
                symbol=symbol,
                date=prior_day_et.strftime("%Y-%m-%d"),
            )

        await asyncio.sleep(_REQUEST_PAUSE_S)

    # Sort chronologically; deduplicate by stable ID
    seen: set[str] = set()
    unique: list[Candle] = []
    for c in sorted(all_candles, key=lambda c: c.timestamp):
        if c.id not in seen:
            seen.add(c.id)
            unique.append(c)

    logger.info(
        "bootstrap.fetch_complete",
        symbol=symbol,
        total_bars=len(unique),
    )
    return unique
