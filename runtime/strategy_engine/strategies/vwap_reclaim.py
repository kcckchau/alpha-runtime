"""
QQQ VWAP Reclaim Long strategy.

Setup logic (all conditions must be true on the same bar):
1. Price was below VWAP at the prior bar close.
2. Current bar closes above VWAP (reclaim).
3. Strong close: close is in the upper half of the bar (close > midpoint).
4. Volume confirmation: relative volume >= threshold.
5. Not overextended: distance from VWAP is within a reasonable range.
6. RTH session only.
7. Opening range must be established (OR window complete).

Risk parameters:
- Stop: candle low (with a configurable buffer in ticks).
- Target: entry + (entry - stop) * min_rr_ratio.

This strategy is intentionally conservative — it does not fire on every
VWAP reclaim.  The goal is quality setups, not frequency.
"""

from __future__ import annotations

from typing import Optional

import structlog

from packages.core.models import (
    Candle,
    MarketContext,
    SessionType,
    SetupSignal,
    SignalDirection,
    Timeframe,
)
from runtime.strategy_engine.base import Strategy

logger = structlog.get_logger(__name__)


class VWAPReclaimLong(Strategy):
    """
    Long entry on a clean VWAP reclaim with volume and price structure confirmation.

    Designed primarily for QQQ (1-minute timeframe) but symbol-agnostic.
    """

    STRATEGY_ID = "vwap_reclaim_long_v1"

    def __init__(
        self,
        symbol: str,
        min_rvol: float = 1.2,
        max_vwap_distance_pct: float = 0.5,
        stop_buffer_pct: float = 0.05,
        min_rr: float = 2.0,
        timeframe: Timeframe = Timeframe.MIN_1,
    ) -> None:
        self._symbol = symbol
        self._min_rvol = min_rvol
        self._max_vwap_distance_pct = max_vwap_distance_pct
        self._stop_buffer_pct = stop_buffer_pct
        self._min_rr = min_rr
        self._timeframe = timeframe

        # Rolling state
        self._last_above_vwap: Optional[bool] = None
        self._pending_context: Optional[MarketContext] = None
        self._last_candle: Optional[Candle] = None

    @property
    def strategy_id(self) -> str:
        return self.STRATEGY_ID

    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]:
        if candle.symbol != self._symbol or candle.timeframe != self._timeframe:
            return None
        self._last_candle = candle
        return None  # Signal generation is triggered by context update

    async def on_context_update(self, context: MarketContext) -> Optional[SetupSignal]:
        if context.symbol != self._symbol:
            return None

        self._pending_context = context

        # We need a paired candle
        if self._last_candle is None:
            return None

        return self._evaluate(self._last_candle, context)

    def _evaluate(
        self, candle: Candle, ctx: MarketContext
    ) -> Optional[SetupSignal]:
        """Apply all entry conditions and return a signal or None."""

        # --- Gate conditions (hard filters) ---

        if candle.session != SessionType.RTH:
            return None

        if ctx.indicators is None:
            return None

        ind = ctx.indicators

        # Opening range must be locked (30 min window complete)
        if ind.opening_range_high is None or ind.opening_range_low is None:
            return None

        # Must be a VWAP reclaim: was below, now above
        if not ctx.vwap_cross_up:
            return None

        vwap = ind.vwap
        if vwap is None or vwap <= 0:
            return None

        # Strong close: close must be in the upper half of the candle
        bar_range = candle.high - candle.low
        if bar_range <= 0:
            return None
        close_position = (candle.close - candle.low) / bar_range
        if close_position < 0.5:
            return None

        # Volume confirmation
        rvol = ind.relative_volume
        if rvol is None or rvol < self._min_rvol:
            return None

        # Not overextended from VWAP
        vwap_dist = ind.vwap_distance_pct
        if vwap_dist is None or vwap_dist > self._max_vwap_distance_pct:
            return None

        # --- Risk levels ---
        stop = candle.low * (1.0 - self._stop_buffer_pct / 100.0)
        risk = candle.close - stop
        if risk <= 0:
            return None

        target = candle.close + risk * self._min_rr
        rr = (target - candle.close) / risk

        # Confidence: simple heuristic based on rvol and close quality
        confidence = min(1.0, (rvol / 2.0) * close_position)

        signal = SetupSignal(
            strategy_id=self.STRATEGY_ID,
            symbol=self._symbol,
            direction=SignalDirection.LONG,
            timestamp=candle.timestamp,
            candle_ref=candle.id,
            entry_price=candle.close,
            stop_price=stop,
            target_price=target,
            risk_reward=round(rr, 2),
            confidence=round(confidence, 3),
            notes=(
                f"VWAP reclaim @ {candle.close:.2f} | VWAP={vwap:.2f} | "
                f"rvol={rvol:.2f}x | close_qual={close_position:.0%}"
            ),
            context_snapshot=ctx,
        )

        logger.info(
            "vwap_reclaim.signal",
            symbol=self._symbol,
            entry=signal.entry_price,
            stop=signal.stop_price,
            target=signal.target_price,
            rr=signal.risk_reward,
            confidence=signal.confidence,
        )
        return signal
