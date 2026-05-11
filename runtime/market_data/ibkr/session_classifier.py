"""
Classify a UTC timestamp into RTH / premarket / afterhours / closed
for US equities (Eastern time, with DST handling).
"""

from __future__ import annotations

from datetime import datetime

import pytz

from packages.core.models import SessionType

_ET = pytz.timezone("America/New_York")

# RTH: 09:30–16:00 ET
_RTH_START = (9, 30)
_RTH_END = (16, 0)

# Premarket: 04:00–09:30 ET
_PREMARKET_START = (4, 0)

# Afterhours: 16:00–20:00 ET
_AFTERHOURS_END = (20, 0)


def classify_session(ts: datetime) -> SessionType:
    """Return the trading session for a UTC-aware datetime."""
    et = ts.astimezone(_ET)
    # Weekend
    if et.weekday() >= 5:
        return SessionType.CLOSED

    hm = (et.hour, et.minute)

    if _RTH_START <= hm < _RTH_END:
        return SessionType.RTH
    if _PREMARKET_START <= hm < _RTH_START:
        return SessionType.PREMARKET
    if _RTH_END <= hm < _AFTERHOURS_END:
        return SessionType.AFTERHOURS
    return SessionType.CLOSED
