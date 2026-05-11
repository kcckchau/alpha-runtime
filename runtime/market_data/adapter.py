"""
Broker-agnostic market data adapter interface.

Any broker implementation must satisfy this protocol.  The rest of the
pipeline (candle engine, etc.) only depends on this interface — never on
ib_insync or any other broker library directly.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Coroutine, Any

from packages.core.models import Tick

TickCallback = Callable[[Tick], Coroutine[Any, Any, None]]


class MarketDataAdapter(ABC):
    """Abstract base class for broker market data connections."""

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the broker."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully close the broker connection."""

    @abstractmethod
    async def subscribe(self, symbol: str) -> None:
        """Subscribe to real-time data for *symbol*."""

    @abstractmethod
    async def unsubscribe(self, symbol: str) -> None:
        """Stop receiving data for *symbol*."""

    @abstractmethod
    def on_tick(self, callback: TickCallback) -> None:
        """
        Register an async callback invoked on every incoming tick.

        The adapter MUST normalize broker-specific bar/tick data into
        the canonical :class:`~packages.core.models.Tick` before calling
        the callback.
        """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return True when the adapter has an active broker connection."""
