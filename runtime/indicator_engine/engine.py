"""
Indicator engine.

Computes and maintains rolling indicator state per symbol.
Subscribes to CANDLE_FINAL events and emits INDICATORS_UPDATED.

Indicators:
- VWAP (read from candle.vwap — already computed by candle engine)
- Opening range high/low (first 30 min of RTH, 09:30–10:00 ET)
- Rolling volume average (20-period)
- Relative volume (current period / rolling avg)
- Prior day high/low (carried over from previous RTH close)
- Premarket high/low (accumulated from premarket session)
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import Optional

import pytz
import structlog

from packages.core.models import (
    Candle,
    IndicatorSnapshot,
    SessionType,
    Timeframe,
)
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)

_ET = pytz.timezone("America/New_York")

# Opening range: first 30 minutes of RTH
_OR_END_HOUR, _OR_END_MINUTE = 10, 0
_VOLUME_LOOKBACK = 20  # periods for rolling avg


class _SymbolState:
    """Mutable per-symbol indicator state updated on each 1m candle."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

        # Opening range (reset each RTH open)
        self.or_high: Optional[float] = None
        self.or_low: Optional[float] = None
        self.or_locked: bool = False   # True once the 30-min window is done

        # Prior-day levels (carried across sessions)
        self.prior_day_high: Optional[float] = None
        self.prior_day_low: Optional[float] = None
        self._rth_high: Optional[float] = None
        self._rth_low: Optional[float] = None

        # Premarket levels (reset each day)
        self.premarket_high: Optional[float] = None
        self.premarket_low: Optional[float] = None

        # Volume rolling window
        self._volume_window: deque[float] = deque(maxlen=_VOLUME_LOOKBACK)

        # Session tracking for resets
        self._last_session: Optional[SessionType] = None
        self._last_date: Optional[str] = None  # YYYY-MM-DD in ET

    def update(self, candle: Candle) -> IndicatorSnapshot:
        et_ts = candle.timestamp.astimezone(_ET)
        date_str = et_ts.strftime("%Y-%m-%d")

        # --- Date rollover → save prior-day levels, reset intraday state ---
        if date_str != self._last_date and self._last_date is not None:
            if self._rth_high is not None:
                self.prior_day_high = self._rth_high
                self.prior_day_low = self._rth_low
            self._rth_high = None
            self._rth_low = None
            self.or_high = None
            self.or_low = None
            self.or_locked = False
            self.premarket_high = None
            self.premarket_low = None
            logger.debug("indicator_engine.day_rollover", symbol=self.symbol, date=date_str)

        self._last_date = date_str

        # --- Premarket levels ---
        if candle.session == SessionType.PREMARKET:
            if self.premarket_high is None or candle.high > self.premarket_high:
                self.premarket_high = candle.high
            if self.premarket_low is None or candle.low < self.premarket_low:
                self.premarket_low = candle.low

        # --- RTH tracking ---
        if candle.session == SessionType.RTH:
            self._rth_high = (
                candle.high if self._rth_high is None else max(self._rth_high, candle.high)
            )
            self._rth_low = (
                candle.low if self._rth_low is None else min(self._rth_low, candle.low)
            )

            # Opening range: accumulate until 10:00 ET
            if not self.or_locked:
                or_end = (et_ts.hour, et_ts.minute) >= (_OR_END_HOUR, _OR_END_MINUTE)
                if or_end:
                    self.or_locked = True
                    logger.debug(
                        "indicator_engine.or_locked",
                        symbol=self.symbol,
                        or_high=self.or_high,
                        or_low=self.or_low,
                    )
                else:
                    self.or_high = (
                        candle.high
                        if self.or_high is None
                        else max(self.or_high, candle.high)
                    )
                    self.or_low = (
                        candle.low
                        if self.or_low is None
                        else min(self.or_low, candle.low)
                    )

        # --- Volume rolling average ---
        self._volume_window.append(candle.volume)
        rolling_avg = (
            sum(self._volume_window) / len(self._volume_window)
            if self._volume_window
            else None
        )
        rel_vol = (candle.volume / rolling_avg) if rolling_avg and rolling_avg > 0 else None

        # --- VWAP distance ---
        vwap = candle.vwap if candle.vwap > 0 else None
        vwap_dist = (
            (candle.close - vwap) / vwap * 100.0 if vwap and vwap > 0 else None
        )

        or_mid = (
            (self.or_high + self.or_low) / 2.0
            if self.or_high is not None and self.or_low is not None
            else None
        )

        return IndicatorSnapshot(
            symbol=self.symbol,
            timestamp=candle.timestamp,
            timeframe=candle.timeframe,
            vwap=vwap,
            vwap_distance_pct=vwap_dist,
            opening_range_high=self.or_high,
            opening_range_low=self.or_low,
            opening_range_mid=or_mid,
            rolling_volume_avg=rolling_avg,
            relative_volume=rel_vol,
            prior_day_high=self.prior_day_high,
            prior_day_low=self.prior_day_low,
            premarket_high=self.premarket_high,
            premarket_low=self.premarket_low,
        )


class IndicatorEngine:
    """
    Subscribes to CANDLE_FINAL on the 1-minute timeframe.
    Maintains rolling indicator state per symbol.
    Publishes IndicatorSnapshot via INDICATORS_UPDATED.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._states: dict[str, _SymbolState] = {}

        bus.subscribe(EventType.CANDLE_FINAL, self._on_candle_event)

    def register_symbol(self, symbol: str) -> None:
        if symbol not in self._states:
            self._states[symbol] = _SymbolState(symbol)
            logger.info("indicator_engine.registered", symbol=symbol)

    async def _on_candle_event(self, event: Event) -> None:
        candle: Candle = event.payload
        if candle.timeframe != Timeframe.MIN_1:
            return
        if candle.symbol not in self._states:
            return

        state = self._states[candle.symbol]
        snapshot = state.update(candle)

        await self._bus.publish(
            Event(
                type=EventType.INDICATORS_UPDATED,
                payload=snapshot,
                source="indicator_engine",
            )
        )
        logger.debug(
            "indicator_engine.updated",
            symbol=snapshot.symbol,
            vwap=snapshot.vwap,
            rvol=snapshot.relative_volume,
        )
