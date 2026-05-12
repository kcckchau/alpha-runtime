"""
Broker-agnostic market data adapter interface.

Any broker implementation must satisfy this protocol.  The rest of the
pipeline (candle engine, quote engine, etc.) only depends on this interface —
never on ib_insync or any other broker library directly.

Callback types:
    TickCallback        — 5-second real-time bars → CandleEngine
    QuoteCallback       — L1 bid/ask snapshots    → QuoteEngine / MicrostructureEngine
    DepthCallback       — L2 order book snapshots → OrderBookEngine / MicrostructureEngine

Both QuoteCallback and DepthCallback are optional.  When a broker does not
support them the adapter simply never calls them; the rest of the system
degrades gracefully (MicrostructureEngine runs L1-only mode).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Coroutine, Any

from packages.core.models import OrderBookSnapshot, QuoteSnapshot, Tick

TickCallback = Callable[[Tick], Coroutine[Any, Any, None]]
QuoteCallback = Callable[[QuoteSnapshot], Coroutine[Any, Any, None]]
DepthCallback = Callable[[OrderBookSnapshot], Coroutine[Any, Any, None]]


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
        Register an async callback invoked on every incoming 5-s bar tick.

        The adapter MUST normalize broker-specific bar/tick data into
        the canonical :class:`~packages.core.models.Tick` before calling
        the callback.
        """

    def on_quote(self, callback: QuoteCallback) -> None:
        """
        Register an async callback invoked on every L1 best-bid/ask update.

        Default implementation is a no-op so adapters that do not support
        L1 quotes do not need to override this method.
        """

    def on_depth_update(self, callback: DepthCallback) -> None:
        """
        Register an async callback invoked on every L2 depth snapshot.

        Default implementation is a no-op so adapters that do not support
        Level II do not need to override this method.
        """

    @property
    @abstractmethod
    def is_connected(self) -> bool:
        """Return True when the adapter has an active broker connection."""
