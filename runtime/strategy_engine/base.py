"""
Strategy plugin system.

All strategies are isolated plugins that implement the Strategy interface.
They MUST NOT:
- Read from brokers directly.
- Modify shared state outside their own return values.
- Depend on execution or risk engines.

They receive domain events and return optional SetupSignal objects.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import structlog

from packages.core.models import Candle, MarketContext, SetupSignal
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
    async def on_context_update(self, context: MarketContext) -> Optional[SetupSignal]:
        """Called on every context update. Return a signal or None."""


class StrategyEngine:
    """
    Orchestrates registered strategy plugins.

    Subscribes to CANDLE_FINAL and CONTEXT_UPDATED.
    For each event, runs all registered strategies in order.
    Any returned SetupSignal is published as SIGNAL_DETECTED.

    Historical/bootstrap events (candle.historical=True) are passed to
    strategies for state warmup but signals are suppressed by default.
    Set suppress_historical=False to allow signals from replayed sessions.
    """

    def __init__(self, bus: EventBus, suppress_historical: bool = True) -> None:
        self._bus = bus
        self._strategies: list[Strategy] = []
        self._suppress_historical = suppress_historical

        bus.subscribe(EventType.CANDLE_FINAL, self._on_candle_event)
        bus.subscribe(EventType.CONTEXT_UPDATED, self._on_context_event)

    def register(self, strategy: Strategy) -> None:
        self._strategies.append(strategy)
        logger.info("strategy_engine.registered", strategy_id=strategy.strategy_id)

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
        for strategy in self._strategies:
            try:
                signal = await strategy.on_context_update(ctx)
                if signal and not (is_historical and self._suppress_historical):
                    await self._emit(signal)
            except Exception:
                logger.exception(
                    "strategy_engine.context_error",
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
