"""
QQQ VWAP Reclaim Long strategy.

Primary trigger (candle + context layer — unchanged):
1. Price was below VWAP at the prior bar close.
2. Current bar closes above VWAP (reclaim).
3. Strong close: close is in the upper half of the bar (close > midpoint).
4. Volume confirmation: relative volume >= threshold.
5. Not overextended: distance from VWAP is within a reasonable range.
6. RTH session only.
7. Opening range must be established (OR window complete).

Microstructure confirmation layer (optional, applied when available):
A signal that passes all primary checks is additionally vetted against
the latest MicrostructureSnapshot for the same symbol.  When microstructure
data is absent the confirmation step is skipped (fail-open, not fail-closed).

Rejection conditions (microstructure):
  - spread_reject:     spread_pct > max_spread_pct (liquidity too thin)
  - pressure_reject:   quote_pressure < min_quote_pressure (sellers dominant)
  - pull_reject:       liquidity_pull=True (fast bid cancel detected)

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
    MicrostructureSnapshot,
    SessionType,
    SetupSignal,
    SignalDirection,
    Timeframe,
)
from runtime.strategy_engine.base import Strategy

logger = structlog.get_logger(__name__)


class VWAPReclaimLong(Strategy):
    """
    Long entry on a clean VWAP reclaim with volume and price structure
    confirmation.  Microstructure is used as an optional veto layer.

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
        # ── Microstructure thresholds ─────────────────────────────────
        max_spread_pct: float = 0.05,        # reject if spread > 5 bp
        min_quote_pressure: float = -0.1,    # reject if smoothed pressure < -0.1
        use_microstructure: bool = True,     # set False to disable entirely
    ) -> None:
        self._symbol = symbol
        self._min_rvol = min_rvol
        self._max_vwap_distance_pct = max_vwap_distance_pct
        self._stop_buffer_pct = stop_buffer_pct
        self._min_rr = min_rr
        self._timeframe = timeframe
        self._max_spread_pct = max_spread_pct
        self._min_quote_pressure = min_quote_pressure
        self._use_microstructure = use_microstructure

        # Rolling state
        self._last_candle: Optional[Candle] = None
        self._pending_context: Optional[MarketContext] = None

    @property
    def strategy_id(self) -> str:
        return self.STRATEGY_ID

    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]:
        if candle.symbol != self._symbol or candle.timeframe != self._timeframe:
            return None
        self._last_candle = candle
        return None  # Signal generation is triggered by context update

    async def on_context_update(
        self,
        context: MarketContext,
        microstructure: Optional[MicrostructureSnapshot] = None,
    ) -> Optional[SetupSignal]:
        if context.symbol != self._symbol:
            return None

        self._pending_context = context

        if self._last_candle is None:
            return None

        return self._evaluate(self._last_candle, context, microstructure)

    # ------------------------------------------------------------------
    # Internal evaluation
    # ------------------------------------------------------------------

    def _evaluate(
        self,
        candle: Candle,
        ctx: MarketContext,
        micro: Optional[MicrostructureSnapshot],
    ) -> Optional[SetupSignal]:
        """Apply all entry conditions and return a signal or None."""

        # ── Primary gate conditions (hard filters) ────────────────────

        if candle.session != SessionType.RTH:
            return None

        if ctx.indicators is None:
            return None

        ind = ctx.indicators

        if ind.opening_range_high is None or ind.opening_range_low is None:
            return None

        if not ctx.vwap_cross_up:
            return None

        vwap = ind.vwap
        if vwap is None or vwap <= 0:
            return None

        bar_range = candle.high - candle.low
        if bar_range <= 0:
            return None
        close_position = (candle.close - candle.low) / bar_range
        if close_position < 0.5:
            return None

        rvol = ind.relative_volume
        if rvol is None or rvol < self._min_rvol:
            return None

        vwap_dist = ind.vwap_distance_pct
        if vwap_dist is None or vwap_dist > self._max_vwap_distance_pct:
            return None

        # ── Microstructure confirmation (optional veto) ───────────────

        micro_veto, veto_reason = self._microstructure_veto(micro)
        if micro_veto:
            logger.debug(
                "vwap_reclaim.micro_veto",
                symbol=self._symbol,
                reason=veto_reason,
                spread_pct=micro.spread_pct if micro else None,
                quote_pressure=micro.quote_pressure if micro else None,
                liquidity_pull=micro.liquidity_pull if micro else None,
            )
            return None

        # ── Risk levels ───────────────────────────────────────────────

        stop = candle.low * (1.0 - self._stop_buffer_pct / 100.0)
        risk = candle.close - stop
        if risk <= 0:
            return None

        target = candle.close + risk * self._min_rr
        rr = (target - candle.close) / risk

        confidence = min(1.0, (rvol / 2.0) * close_position)

        # Boost confidence slightly when microstructure is positive
        if micro is not None and self._use_microstructure:
            if micro.quote_pressure is not None and micro.quote_pressure > 0.1:
                confidence = min(1.0, confidence * 1.1)

        micro_note = ""
        if micro is not None:
            micro_note = (
                f" | spread={micro.spread_pct:.3f}% "
                f"| pressure={micro.quote_pressure:.3f}"
                f"| pull={micro.liquidity_pull}"
            ) if (
                micro.spread_pct is not None and micro.quote_pressure is not None
            ) else ""

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
                f"{micro_note}"
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

    def _microstructure_veto(
        self, micro: Optional[MicrostructureSnapshot]
    ) -> tuple[bool, str]:
        """
        Return (should_veto, reason).

        Fail-open: if microstructure is disabled or unavailable, never veto.
        """
        if not self._use_microstructure or micro is None:
            return False, ""

        # Spread too wide — liquidity thin, slippage risk
        if (
            micro.spread_pct is not None
            and micro.spread_pct > self._max_spread_pct
        ):
            return True, f"spread_wide={micro.spread_pct:.4f}%>{self._max_spread_pct}%"

        # Quote pressure negative — sellers dominating the L1 order flow
        if (
            micro.quote_pressure is not None
            and micro.quote_pressure < self._min_quote_pressure
        ):
            return True, f"pressure_negative={micro.quote_pressure:.4f}<{self._min_quote_pressure}"

        # Liquidity pull — best bid just got yanked, fragile support
        if micro.liquidity_pull:
            return True, "liquidity_pull=True"

        return False, ""
