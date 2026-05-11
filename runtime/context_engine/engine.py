"""
Context engine.

Subscribes to INDICATORS_UPDATED and the last N final 1m candles.
Derives observable intraday market state (MarketContext) without regime guessing.

What we track:
- Above / below VWAP (and the moment of crossing)
- Distance from VWAP as a percentage
- ORB breakout up/down — did price close above OR high / below OR low?
- ORB hold — is price still holding above OR high after the breakout?
- Higher highs / lower lows over the last N candles (structural observation only)
- Opening drive — strong directional move in the first 30 min of RTH

We deliberately do NOT classify "trend day" / "chop day" — that requires
hindsight and invites overfitting.
"""

from __future__ import annotations

from collections import deque
from typing import Optional

import structlog

from packages.core.models import (
    Candle,
    IndicatorSnapshot,
    MarketContext,
    SessionType,
    Timeframe,
)
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)

_STRUCTURE_LOOKBACK = 5   # bars for HH/LL detection
_OPENING_DRIVE_BARS = 6   # first ~6 1m bars = 30 min (conservative)
_OPENING_DRIVE_MIN_PCT = 0.20  # ≥ 0.20% move in either direction = drive


class _SymbolContext:
    """Mutable context state for one symbol."""

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self._candle_window: deque[Candle] = deque(maxlen=_STRUCTURE_LOOKBACK + 1)
        self._above_vwap: Optional[bool] = None  # previous value for cross detection
        self._orb_broken_up: bool = False
        self._orb_broken_down: bool = False
        self._rth_bar_count: int = 0
        self._opening_drive_up: bool = False
        self._opening_drive_down: bool = False
        self._last_session: Optional[SessionType] = None

    def update(
        self, candle: Candle, indicators: IndicatorSnapshot
    ) -> MarketContext:
        if candle.session != self._last_session:
            if candle.session == SessionType.RTH:
                self._rth_bar_count = 0
                self._orb_broken_up = False
                self._orb_broken_down = False
                self._opening_drive_up = False
                self._opening_drive_down = False
            self._last_session = candle.session

        if candle.session == SessionType.RTH:
            self._rth_bar_count += 1

        self._candle_window.append(candle)

        # --- VWAP relationship ---
        vwap = indicators.vwap
        above_vwap: Optional[bool] = None
        vwap_cross_up = False
        vwap_cross_down = False

        if vwap and vwap > 0:
            above_vwap = candle.close > vwap
            if self._above_vwap is not None:
                vwap_cross_up = (not self._above_vwap) and above_vwap
                vwap_cross_down = self._above_vwap and (not above_vwap)
            self._above_vwap = above_vwap

        vwap_dist = indicators.vwap_distance_pct

        # --- ORB breakout ---
        orb_high = indicators.opening_range_high
        orb_low = indicators.opening_range_low
        orb_breakout_up = False
        orb_breakout_down = False
        orb_hold: Optional[bool] = None

        if orb_high and not self._orb_broken_up and candle.close > orb_high:
            self._orb_broken_up = True
            orb_breakout_up = True
            logger.info("context_engine.orb_breakout_up", symbol=self.symbol, price=candle.close)

        if orb_low and not self._orb_broken_down and candle.close < orb_low:
            self._orb_broken_down = True
            orb_breakout_down = True
            logger.info("context_engine.orb_breakout_down", symbol=self.symbol, price=candle.close)

        if self._orb_broken_up and orb_high:
            orb_hold = candle.close > orb_high

        # --- Price structure (HH / LL) ---
        higher_highs, lower_lows = self._detect_structure()

        # --- Opening drive ---
        if candle.session == SessionType.RTH and self._rth_bar_count <= _OPENING_DRIVE_BARS:
            candles = list(self._candle_window)
            if len(candles) >= 2:
                first_open = candles[0].open
                last_close = candles[-1].close
                move_pct = abs(last_close - first_open) / first_open * 100.0
                if move_pct >= _OPENING_DRIVE_MIN_PCT:
                    self._opening_drive_up = last_close > first_open
                    self._opening_drive_down = last_close < first_open

        return MarketContext(
            symbol=self.symbol,
            timestamp=candle.timestamp,
            above_vwap=above_vwap,
            vwap_cross_up=vwap_cross_up,
            vwap_cross_down=vwap_cross_down,
            vwap_distance_pct=vwap_dist,
            orb_breakout_up=orb_breakout_up,
            orb_breakout_down=orb_breakout_down,
            orb_hold=orb_hold,
            higher_highs=higher_highs,
            lower_lows=lower_lows,
            opening_drive_up=self._opening_drive_up,
            opening_drive_down=self._opening_drive_down,
            session=candle.session,
            indicators=indicators,
            historical=candle.historical,
        )

    def _detect_structure(self) -> tuple[bool, bool]:
        """
        Detect HH and LL over the last N bars.

        HH: every high is strictly higher than the previous high.
        LL: every low is strictly lower than the previous low.
        """
        bars = list(self._candle_window)
        if len(bars) < 3:
            return False, False

        highs = [b.high for b in bars]
        lows = [b.low for b in bars]

        higher_highs = all(highs[i] > highs[i - 1] for i in range(1, len(highs)))
        lower_lows = all(lows[i] < lows[i - 1] for i in range(1, len(lows)))
        return higher_highs, lower_lows


class ContextEngine:
    """
    Subscribes to CANDLE_FINAL (1m) and INDICATORS_UPDATED.
    Pairs the latest candle and indicator snapshot per symbol.
    Emits MarketContext via CONTEXT_UPDATED.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._states: dict[str, _SymbolContext] = {}
        # Buffer the latest indicator snapshot per symbol so we can pair with candles
        self._latest_indicators: dict[str, IndicatorSnapshot] = {}

        bus.subscribe(EventType.CANDLE_FINAL, self._on_candle)
        bus.subscribe(EventType.INDICATORS_UPDATED, self._on_indicators)

    def register_symbol(self, symbol: str) -> None:
        if symbol not in self._states:
            self._states[symbol] = _SymbolContext(symbol)
            logger.info("context_engine.registered", symbol=symbol)

    async def _on_indicators(self, event: Event) -> None:
        snap: IndicatorSnapshot = event.payload
        self._latest_indicators[snap.symbol] = snap

    async def _on_candle(self, event: Event) -> None:
        candle: Candle = event.payload
        if candle.timeframe != Timeframe.MIN_1:
            return
        if candle.symbol not in self._states:
            return

        indicators = self._latest_indicators.get(candle.symbol)
        if indicators is None:
            return

        ctx = self._states[candle.symbol].update(candle, indicators)

        await self._bus.publish(
            Event(
                type=EventType.CONTEXT_UPDATED,
                payload=ctx,
                source="context_engine",
            )
        )
        logger.debug(
            "context_engine.updated",
            symbol=ctx.symbol,
            above_vwap=ctx.above_vwap,
            vwap_cross_up=ctx.vwap_cross_up,
            orb_breakout_up=ctx.orb_breakout_up,
        )
