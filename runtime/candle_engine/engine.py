"""
Candle aggregation engine.

Consumes 5-second Tick events (from the IBKR adapter's reqRealTimeBars output)
and aggregates them into 1-minute and 5-minute candles.

Design:
- A CandleBuilder per (symbol, timeframe) manages one open bar at a time.
- VWAP is calculated incrementally as a session-anchored cumulative VWAP.
- Session resets (premarket → RTH, etc.) reset the cumulative VWAP state.
- Both PARTIAL (in-progress) and FINAL candles are published to the event bus.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import structlog

from packages.core.models import Candle, SessionType, Tick, Timeframe
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)

# Timeframes we build from 5-second ticks
TARGET_TIMEFRAMES = [Timeframe.MIN_1, Timeframe.MIN_5]

_TIMEFRAME_SECONDS: dict[Timeframe, int] = {
    Timeframe.MIN_1: 60,
    Timeframe.MIN_5: 300,
}


def _floor_timestamp(ts: datetime, period_seconds: int) -> datetime:
    """Truncate *ts* to the nearest *period_seconds* boundary (UTC)."""
    epoch = ts.timestamp()
    floored = math.floor(epoch / period_seconds) * period_seconds
    return datetime.fromtimestamp(floored, tz=timezone.utc)


class _CandleBuilder:
    """
    Accumulates ticks for a single (symbol, timeframe) into open/close bars.

    VWAP = cumulative(price * volume) / cumulative(volume) anchored to session open.
    """

    def __init__(self, symbol: str, timeframe: Timeframe) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self._period = _TIMEFRAME_SECONDS[timeframe]

        # Current open bar state
        self._bar_open: Optional[datetime] = None
        self._open = self._high = self._low = self._close = 0.0
        self._volume = 0.0

        # Track the session at bar-open time so final candle has correct label
        self._bar_session: Optional[SessionType] = None

        # Session-anchored VWAP accumulators
        self._cum_volume = 0.0
        self._cum_turnover = 0.0  # sum(typical_price * volume)
        self._current_session: Optional[SessionType] = None

    def update(self, tick: Tick) -> tuple[Optional[Candle], Optional[Candle]]:
        """
        Process one tick.

        Returns (partial_candle, final_candle):
        - partial_candle is always the current in-progress bar.
        - final_candle is set only when the previous bar is completed.
        """
        bar_open = _floor_timestamp(tick.timestamp, self._period)

        final_candle: Optional[Candle] = None

        # New bar period → close the previous one BEFORE updating session
        if self._bar_open is not None and bar_open != self._bar_open:
            final_candle = self._build_candle(is_partial=False)
            self._reset_bar()

        # Detect session reset → reset VWAP accumulators (after closing old bar)
        if tick.session != self._current_session:
            if tick.session in (SessionType.RTH, SessionType.PREMARKET):
                self._cum_volume = 0.0
                self._cum_turnover = 0.0
                logger.debug(
                    "candle_engine.session_reset",
                    symbol=self.symbol,
                    timeframe=self.timeframe,
                    new_session=tick.session,
                )
            self._current_session = tick.session

        # Start a new bar if needed
        if self._bar_open is None:
            self._bar_open = bar_open
            self._bar_session = tick.session
            self._open = tick.open
            self._high = tick.high
            self._low = tick.low
        else:
            self._high = max(self._high, tick.high)
            self._low = min(self._low, tick.low)

        self._close = tick.close
        self._volume += tick.volume

        # Typical price for VWAP: (H + L + C) / 3
        typical = (tick.high + tick.low + tick.close) / 3.0
        self._cum_volume += tick.volume
        self._cum_turnover += typical * tick.volume

        partial_candle = self._build_candle(is_partial=True)
        return partial_candle, final_candle

    def _build_candle(self, *, is_partial: bool) -> Candle:
        vwap = (
            self._cum_turnover / self._cum_volume
            if self._cum_volume > 0
            else self._close
        )
        return Candle(
            symbol=self.symbol,
            timeframe=self.timeframe,
            timestamp=self._bar_open,  # type: ignore[arg-type]
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            vwap=vwap,
            cumulative_volume=self._cum_volume,
            cumulative_turnover=self._cum_turnover,
            # Use the session captured at bar-open time, not current tick session
            session=self._bar_session or SessionType.RTH,
            is_partial=is_partial,
        )

    def _reset_bar(self) -> None:
        self._bar_open = None
        self._bar_session = None
        self._open = self._high = self._low = self._close = 0.0
        self._volume = 0.0


class CandleEngine:
    """
    Orchestrates _CandleBuilder instances for all (symbol, timeframe) pairs.

    Usage:
        engine = CandleEngine(bus)
        engine.register_symbol("QQQ")
        # Call engine.on_tick(tick) whenever a tick arrives.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        # builders[(symbol, timeframe)] = _CandleBuilder
        self._builders: dict[tuple[str, Timeframe], _CandleBuilder] = {}
        self._symbols: set[str] = set()

    def register_symbol(self, symbol: str) -> None:
        if symbol in self._symbols:
            return
        self._symbols.add(symbol)
        for tf in TARGET_TIMEFRAMES:
            self._builders[(symbol, tf)] = _CandleBuilder(symbol, tf)
        logger.info("candle_engine.registered", symbol=symbol)

    async def on_tick(self, tick: Tick) -> None:
        """Entry point: process a normalized tick and emit candle events."""
        if tick.symbol not in self._symbols:
            logger.warning("candle_engine.unregistered_symbol", symbol=tick.symbol)
            return

        for tf in TARGET_TIMEFRAMES:
            builder = self._builders[(tick.symbol, tf)]
            partial, final = builder.update(tick)

            if final is not None:
                await self._bus.publish(
                    Event(
                        type=EventType.CANDLE_FINAL,
                        payload=final,
                        source="candle_engine",
                    )
                )
                logger.debug(
                    "candle_engine.final",
                    symbol=final.symbol,
                    timeframe=final.timeframe,
                    ts=final.timestamp.isoformat(),
                    close=final.close,
                    vwap=final.vwap,
                )

            if partial is not None:
                await self._bus.publish(
                    Event(
                        type=EventType.CANDLE_PARTIAL,
                        payload=partial,
                        source="candle_engine",
                    )
                )
