"""
Quote Engine — consumes L1 bid/ask snapshots from the market-data adapter
and publishes them on the internal bus (QUOTE_UPDATED).

This engine is intentionally thin: its only job is to bridge the adapter
callback into the async event bus so any subscriber (MicrostructureEngine,
monitoring, recording) receives a typed QuoteSnapshot without depending on
the adapter directly.

Bus channel: alpha:quotes:{symbol}   (EventType.QUOTE_UPDATED payload)
"""

from __future__ import annotations

import structlog

from packages.core.models import QuoteSnapshot
from packages.messaging.bus import Event, EventBus, EventType, quote_channel

logger = structlog.get_logger(__name__)


class QuoteEngine:
    """
    Receives QuoteSnapshot objects from the adapter callback and re-publishes
    them as QUOTE_UPDATED events on the bus.

    Registered symbols filter out quotes from unsubscribed instruments so
    a single global adapter instance can feed multiple QuoteEngine users.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._symbols: set[str] = set()

    def register_symbol(self, symbol: str) -> None:
        self._symbols.add(symbol.upper())
        logger.info("quote_engine.symbol_registered", symbol=symbol)

    async def on_quote(self, quote: QuoteSnapshot) -> None:
        """Adapter callback — called on every L1 bid/ask update."""
        if quote.symbol not in self._symbols:
            return

        logger.debug(
            "quote_engine.received",
            symbol=quote.symbol,
            bid=quote.bid,
            ask=quote.ask,
            spread=quote.spread,
            channel=quote_channel(quote.symbol),
        )

        await self._bus.publish(
            Event(
                type=EventType.QUOTE_UPDATED,
                payload=quote,
                source=f"quote_engine:{quote.symbol}",
            )
        )
