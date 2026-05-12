"""
Internal async event bus.

Design goals:
- In-process pub/sub for the modular monolith.
- Clean interface so it can be backed by Redis Streams later without changing callers.
- Typed events with a well-known EventType registry.
- No hidden globals — callers inject or use get_bus() for the process-level singleton.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Callable, Coroutine

import structlog

logger = structlog.get_logger(__name__)


class EventType(StrEnum):
    # Market data pipeline
    TICK_RECEIVED = "tick.received"
    CANDLE_PARTIAL = "candle.partial"
    CANDLE_FINAL = "candle.final"

    # Microstructure pipeline (L1 quotes, L2 depth, computed metrics)
    QUOTE_UPDATED = "quote.updated"
    ORDERBOOK_UPDATED = "orderbook.updated"
    MICROSTRUCTURE_UPDATED = "microstructure.updated"

    # Indicator / context
    INDICATORS_UPDATED = "indicators.updated"
    CONTEXT_UPDATED = "context.updated"

    # Strategy
    SIGNAL_DETECTED = "signal.detected"
    SIGNAL_APPROVED = "signal.approved"
    SIGNAL_REJECTED = "signal.rejected"
    SIGNAL_EXPIRED = "signal.expired"

    # Execution
    ORDER_REQUESTED = "order.requested"
    ORDER_SUBMITTED = "order.submitted"
    ORDER_FILLED = "order.filled"
    ORDER_RESULT_UPDATED = "order.result_updated"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"

    # Position management
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    EXIT_TRIGGERED = "position.exit_triggered"

    # Signal approval gate
    SIGNAL_PENDING = "signal.pending"

    # System
    SESSION_OPEN = "session.open"
    SESSION_CLOSE = "session.close"
    ERROR = "system.error"

    # Bootstrap / backfill lifecycle
    BOOTSTRAP_STARTED = "bootstrap.started"
    BOOTSTRAP_COMPLETE = "bootstrap.complete"


# ---------------------------------------------------------------------------
# Typed channel helpers
# ---------------------------------------------------------------------------
# Events are typed by EventType; these helpers produce the namespaced
# "channel" strings used in log context and future Redis pub/sub migration.

def quote_channel(symbol: str) -> str:
    """alpha:quotes:{symbol}"""
    return f"alpha:quotes:{symbol}"


def orderbook_channel(symbol: str) -> str:
    """alpha:orderbook:{symbol}"""
    return f"alpha:orderbook:{symbol}"


def microstructure_channel(symbol: str) -> str:
    """alpha:microstructure:{symbol}"""
    return f"alpha:microstructure:{symbol}"


HandlerFn = Callable[["Event"], Coroutine[Any, Any, None]]


class Event:
    """Lightweight envelope wrapping any domain object."""

    __slots__ = ("type", "payload", "timestamp", "source")

    def __init__(
        self,
        type: EventType,
        payload: Any,
        source: str = "unknown",
        timestamp: datetime | None = None,
    ) -> None:
        self.type = type
        self.payload = payload
        self.source = source
        self.timestamp = timestamp or datetime.now(tz=timezone.utc)

    def __repr__(self) -> str:
        return f"Event(type={self.type}, source={self.source}, ts={self.timestamp.isoformat()})"


class EventBus:
    """
    Asyncio-native in-process event bus.

    Subscribers register coroutine handlers per EventType.
    publish() dispatches concurrently to all subscribers and awaits them.

    Errors in individual subscribers are logged but do not propagate,
    preventing one bad handler from blocking the pipeline.
    """

    def __init__(self) -> None:
        self._handlers: dict[EventType, list[HandlerFn]] = defaultdict(list)
        self._wildcard_handlers: list[HandlerFn] = []

    def subscribe(self, event_type: EventType, handler: HandlerFn) -> None:
        self._handlers[event_type].append(handler)
        logger.debug("event_bus.subscribed", event_type=event_type, handler=handler.__qualname__)

    def subscribe_all(self, handler: HandlerFn) -> None:
        """Subscribe to every event type — useful for audit loggers, replay recorders."""
        self._wildcard_handlers.append(handler)

    def unsubscribe(self, event_type: EventType, handler: HandlerFn) -> None:
        try:
            self._handlers[event_type].remove(handler)
        except ValueError:
            pass

    async def publish(self, event: Event) -> None:
        handlers = list(self._handlers[event.type]) + self._wildcard_handlers
        if not handlers:
            return
        tasks = [asyncio.create_task(self._call(h, event)) for h in handlers]
        await asyncio.gather(*tasks)

    async def _call(self, handler: HandlerFn, event: Event) -> None:
        try:
            await handler(event)
        except Exception:
            logger.exception(
                "event_bus.handler_error",
                event_type=event.type,
                handler=handler.__qualname__,
            )


# ---------------------------------------------------------------------------
# Process-level singleton
# ---------------------------------------------------------------------------

_bus: EventBus | None = None


def get_bus() -> EventBus:
    global _bus
    if _bus is None:
        _bus = EventBus()
    return _bus
