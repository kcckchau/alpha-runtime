"""
Strategy plugin system.

All strategies are isolated plugins that implement the Strategy interface.
They MUST NOT:
- Read from brokers directly.
- Modify shared state outside their own return values.
- Depend on execution or risk engines.

They receive domain events and return optional SetupSignal objects.

Microstructure is delivered as an optional parameter to on_context_update().
Strategies that want to use it should declare the parameter; those that
don't can ignore it — the signature is backward-compatible via a default.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import structlog

from packages.core.models import Candle, MarketContext, MicrostructureSnapshot, SetupSignal
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)


class Strategy(ABC):
    """Base class for all strategy plugins."""

    @property
    @abstractmethod
    def strategy_id(self) -> str:
        """Unique identifier for this strategy (e.g. 'qqq_vwap_reclaim_long')."""

    @abstractmethod
    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]:
        """Called on every final candle. Return a signal or None."""

    @abstractmethod
    async def on_context_update(
        self,
        context: MarketContext,
        microstructure: Optional[MicrostructureSnapshot] = None,
    ) -> Optional[SetupSignal]:
        """
        Called on every context update.

        *microstructure* is the latest MicrostructureSnapshot for the same
        symbol if the MicrostructureEngine is running, otherwise None.
        Strategies must treat None as "data unavailable" and should NOT
        reject signals solely because microstructure is missing.
        """

    async def on_microstructure_update(
        self, snap: MicrostructureSnapshot
    ) -> Optional[SetupSignal]:
        """
        Optional hook called on every MICROSTRUCTURE_UPDATED event.

        Default implementation returns None so strategies that only use
        microstructure as a confirmation filter (via on_context_update) do
        not need to override this.
        """
        return None


class StrategyEngine:
    """
    Orchestrates registered strategy plugins.

    Subscribes to:
      CANDLE_FINAL           → calls strategy.on_candle()
      CONTEXT_UPDATED        → calls strategy.on_context_update() with latest
                               MicrostructureSnapshot injected automatically
      MICROSTRUCTURE_UPDATED → stores latest snapshot per symbol AND calls
                               strategy.on_microstructure_update()

    Historical/bootstrap events (candle.historical=True) are passed to
    strategies for state warmup but signals are suppressed by default.
    Set suppress_historical=False to allow signals from replayed sessions.
    """

    def __init__(self, bus: EventBus, suppress_historical: bool = True) -> None:
        self._bus = bus
        self._strategies: list[Strategy] = []
        self._suppress_historical = suppress_historical
        # Latest microstructure per symbol — injected into on_context_update
        self._latest_micro: dict[str, MicrostructureSnapshot] = {}

        bus.subscribe(EventType.CANDLE_FINAL, self._on_candle_event)
        bus.subscribe(EventType.CONTEXT_UPDATED, self._on_context_event)
        bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, self._on_microstructure_event)

    def register(self, strategy: Strategy) -> None:
        self._strategies.append(strategy)
        logger.info("strategy_engine.registered", strategy_id=strategy.strategy_id)

    # ------------------------------------------------------------------
    # Bus handlers
    # ------------------------------------------------------------------

    async def _on_candle_event(self, event: Event) -> None:
        candle: Candle = event.payload
        is_historical = candle.historical
        for strategy in self._strategies:
            try:
                signal = await strategy.on_candle(candle)
                if signal and not (is_historical and self._suppress_historical):
                    await self._emit(signal)
            except Exception:
                logger.exception(
                    "strategy_engine.candle_error",
                    strategy_id=strategy.strategy_id,
                )

    async def _on_context_event(self, event: Event) -> None:
        ctx: MarketContext = event.payload
        is_historical = ctx.historical
        micro = self._latest_micro.get(ctx.symbol)
        for strategy in self._strategies:
            try:
                signal = await strategy.on_context_update(ctx, micro)
                if signal and not (is_historical and self._suppress_historical):
                    await self._emit(signal)
            except Exception:
                logger.exception(
                    "strategy_engine.context_error",
                    strategy_id=strategy.strategy_id,
                )

    async def _on_microstructure_event(self, event: Event) -> None:
        snap: MicrostructureSnapshot = event.payload
        self._latest_micro[snap.symbol] = snap
        for strategy in self._strategies:
            try:
                signal = await strategy.on_microstructure_update(snap)
                if signal:
                    await self._emit(signal)
            except Exception:
                logger.exception(
                    "strategy_engine.microstructure_error",
                    strategy_id=strategy.strategy_id,
                )

    async def _emit(self, signal: SetupSignal) -> None:
        await self._bus.publish(
            Event(
                type=EventType.SIGNAL_DETECTED,
                payload=signal,
                source=signal.strategy_id,
            )
        )
        logger.info(
            "strategy_engine.signal_detected",
            strategy_id=signal.strategy_id,
            symbol=signal.symbol,
            direction=signal.direction,
            entry=signal.entry_price,
            rr=signal.risk_reward,
        )
