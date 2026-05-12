"""
OrderBook Engine — maintains the current best-N order book for each symbol
and publishes snapshots on the bus (ORDERBOOK_UPDATED).

When Level II data is unavailable (settings.stream_depth=False) this engine
receives no updates and stays silent; the MicrostructureEngine then
operates in L1-only mode automatically.

Bus channel: alpha:orderbook:{symbol}   (EventType.ORDERBOOK_UPDATED payload)
"""

from __future__ import annotations

from typing import Optional

import structlog

from packages.core.models import OrderBookSnapshot
from packages.messaging.bus import Event, EventBus, EventType, orderbook_channel

logger = structlog.get_logger(__name__)


class OrderBookEngine:
    """
    Receives OrderBookSnapshot objects from the adapter callback and
    re-publishes them as ORDERBOOK_UPDATED events.

    Also exposes the latest snapshot per-symbol via latest() so the
    MicrostructureEngine can read it synchronously without subscribing.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._symbols: set[str] = set()
        self._latest: dict[str, OrderBookSnapshot] = {}

    def register_symbol(self, symbol: str) -> None:
        self._symbols.add(symbol.upper())
        logger.info("orderbook_engine.symbol_registered", symbol=symbol)

    def latest(self, symbol: str) -> Optional[OrderBookSnapshot]:
        """Return the most recent snapshot, or None if not yet received."""
        return self._latest.get(symbol.upper())

    async def on_depth_update(self, snapshot: OrderBookSnapshot) -> None:
        """Adapter callback — called on every L2 depth update."""
        if snapshot.symbol not in self._symbols:
            return

        self._latest[snapshot.symbol] = snapshot

        top_bid = snapshot.bids[0].price if snapshot.bids else None
        top_ask = snapshot.asks[0].price if snapshot.asks else None
        logger.debug(
            "orderbook_engine.updated",
            symbol=snapshot.symbol,
            bid_levels=len(snapshot.bids),
            ask_levels=len(snapshot.asks),
            top_bid=top_bid,
            top_ask=top_ask,
            channel=orderbook_channel(snapshot.symbol),
        )

        await self._bus.publish(
            Event(
                type=EventType.ORDERBOOK_UPDATED,
                payload=snapshot,
                source=f"orderbook_engine:{snapshot.symbol}",
            )
        )
