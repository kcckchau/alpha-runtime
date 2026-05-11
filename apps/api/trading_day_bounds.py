"""
ET trading-day window for querying stored candles.

US equities extended session for one ET *calendar* day:
  [04:00 ET, 20:00 ET)  — half-open in UTC, aligned with session_classifier
  (premarket through after-hours; CLOSED outside this band on weekdays).
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Tuple

import pytz

_ET = pytz.timezone("America/New_York")


def today_trade_date_et() -> date:
    """Current calendar date in America/New_York."""
    return datetime.now(tz=_ET).date()


def parse_trade_date(s: str) -> date:
    """Parse YYYY-MM-DD; raises ValueError if invalid."""
    s = s.strip()
    try:
        y, m, d = (int(p) for p in s.split("-", 2))
        return date(y, m, d)
    except (ValueError, TypeError) as e:
        raise ValueError(f"invalid trade date {s!r}, expected YYYY-MM-DD") from e


def extended_session_utc_bounds(
    trade_date: date,
    *,
    premarket_hour: int = 4,
    premarket_minute: int = 0,
    session_end_hour: int = 20,
    session_end_minute: int = 0,
) -> Tuple[datetime, datetime]:
    """
    Return UTC bounds [start, end) for all 1m bars that fall on trade_date
    from premarket open through the end of after-hours (exclusive at 20:00 ET).
    """
    start_et = _ET.localize(
        datetime.combine(trade_date, time(premarket_hour, premarket_minute, 0))
    )
    end_et = _ET.localize(
        datetime.combine(trade_date, time(session_end_hour, session_end_minute, 0))
    )
    return (
        start_et.astimezone(timezone.utc),
        end_et.astimezone(timezone.utc),
    )
