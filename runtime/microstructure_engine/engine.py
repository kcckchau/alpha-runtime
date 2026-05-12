"""
Microstructure Engine.

Subscribes to:
  QUOTE_UPDATED    — L1 bid/ask snapshots  (always available)
  ORDERBOOK_UPDATED — L2 book snapshots   (optional, L2 mode only)

Computes per-symbol MicrostructureSnapshot on every quote update and
publishes MICROSTRUCTURE_UPDATED on the bus.

Metrics computed
────────────────
  spread              ask - bid (price units)
  spread_pct          spread / mid * 100
  mid                 (bid + ask) / 2
  bid_ask_imbalance   (bid_sz - ask_sz) / (bid_sz + ask_sz) ∈ [-1, +1]
                        > 0  →  more bid support
  depth_imbalance     same but summed over all visible L2 levels (None if no L2)
  quote_pressure      EMA(α=0.2) of bid_ask_imbalance — smoothed directional
                      buying/selling pressure signal
  liquidity_pull      True when best bid OR ask size drops by > 50 % in one tick
                      (fast-cancel / spoofing indicator)

All metrics are Optional — a snapshot with None fields is still published
so subscribers can react to partial information.

Bus channel: alpha:microstructure:{symbol}  (EventType.MICROSTRUCTURE_UPDATED)
"""

from __future__ import annotations

from typing import Optional

import structlog

from packages.core.models import (
    MicrostructureSnapshot,
    OrderBookSnapshot,
    QuoteSnapshot,
    SessionType,
)
from packages.messaging.bus import Event, EventBus, EventType, microstructure_channel

logger = structlog.get_logger(__name__)

_QUOTE_PRESSURE_ALPHA = 0.2   # EMA smoothing factor for quote_pressure
_LIQUIDITY_PULL_THRESHOLD = 0.5  # 50 % drop triggers liquidity pull flag


class _SymbolState:
    """Mutable per-symbol microstructure state, not exposed outside engine."""

    __slots__ = (
        "last_quote",
        "last_book",
        "quote_pressure",
        "prev_bid_size",
        "prev_ask_size",
    )

    def __init__(self) -> None:
        self.last_quote: Optional[QuoteSnapshot] = None
        self.last_book: Optional[OrderBookSnapshot] = None
        self.quote_pressure: Optional[float] = None
        self.prev_bid_size: Optional[float] = None
        self.prev_ask_size: Optional[float] = None


class MicrostructureEngine:
    """
    Computes microstructure metrics from L1 quotes and optional L2 depth.

    Call register_symbol() for each symbol before subscribing the adapter.

    When stream_depth=False the engine runs in L1-only mode and depth_imbalance
    is always None — all other metrics still work.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._state: dict[str, _SymbolState] = {}

        bus.subscribe(EventType.QUOTE_UPDATED, self._on_quote_event)
        bus.subscribe(EventType.ORDERBOOK_UPDATED, self._on_book_event)

    def register_symbol(self, symbol: str) -> None:
        sym = symbol.upper()
        if sym not in self._state:
            self._state[sym] = _SymbolState()
        logger.info("microstructure_engine.symbol_registered", symbol=sym)

    def latest(self, symbol: str) -> Optional[MicrostructureSnapshot]:
        """Return the most-recently computed snapshot (synchronous read)."""
        state = self._state.get(symbol.upper())
        if state is None or state.last_quote is None:
            return None
        return self._compute(symbol.upper(), state)

    # ------------------------------------------------------------------
    # Bus subscribers
    # ------------------------------------------------------------------

    async def _on_quote_event(self, event: Event) -> None:
        quote: QuoteSnapshot = event.payload
        state = self._state.get(quote.symbol)
        if state is None:
            return

        state.last_quote = quote
        snap = self._compute(quote.symbol, state)

        # Update prev sizes for next liquidity-pull comparison
        state.prev_bid_size = quote.bid_size
        state.prev_ask_size = quote.ask_size

        await self._publish(snap)

    async def _on_book_event(self, event: Event) -> None:
        book: OrderBookSnapshot = event.payload
        state = self._state.get(book.symbol)
        if state is None:
            return
        state.last_book = book
        # No separate publish — next QUOTE_UPDATED will pick up the new book.

    # ------------------------------------------------------------------
    # Core computation
    # ------------------------------------------------------------------

    def _compute(self, symbol: str, state: _SymbolState) -> MicrostructureSnapshot:
        quote = state.last_quote
        book = state.last_book

        bid = quote.bid if quote else None
        ask = quote.ask if quote else None
        bid_sz = quote.bid_size if quote else None
        ask_sz = quote.ask_size if quote else None
        session = quote.session if quote else SessionType.RTH
        ts = quote.timestamp if quote else None

        # ── Spread / mid ─────────────────────────────────────────────
        spread: Optional[float] = None
        spread_pct: Optional[float] = None
        mid: Optional[float] = None
        if bid is not None and ask is not None and ask >= bid:
            spread = round(ask - bid, 6)
            mid = (bid + ask) / 2.0
            if mid and mid > 0:
                spread_pct = round(spread / mid * 100, 6)

        # ── L1 bid/ask size imbalance ─────────────────────────────────
        bid_ask_imbalance: Optional[float] = None
        if bid_sz is not None and ask_sz is not None:
            total = bid_sz + ask_sz
            if total > 0:
                bid_ask_imbalance = round((bid_sz - ask_sz) / total, 4)

        # ── Smoothed quote pressure (EMA of bid_ask_imbalance) ────────
        if bid_ask_imbalance is not None:
            if state.quote_pressure is None:
                state.quote_pressure = bid_ask_imbalance
            else:
                state.quote_pressure = round(
                    _QUOTE_PRESSURE_ALPHA * bid_ask_imbalance
                    + (1.0 - _QUOTE_PRESSURE_ALPHA) * state.quote_pressure,
                    4,
                )

        # ── L2 depth imbalance ────────────────────────────────────────
        depth_imbalance: Optional[float] = None
        if book is not None and (book.bids or book.asks):
            total_bid = sum(lv.size for lv in book.bids)
            total_ask = sum(lv.size for lv in book.asks)
            book_total = total_bid + total_ask
            if book_total > 0:
                depth_imbalance = round((total_bid - total_ask) / book_total, 4)

        # ── Liquidity pull detection ──────────────────────────────────
        liquidity_pull = False
        if (
            bid_sz is not None
            and ask_sz is not None
            and state.prev_bid_size is not None
            and state.prev_ask_size is not None
        ):
            if state.prev_bid_size > 0:
                bid_drop = (state.prev_bid_size - bid_sz) / state.prev_bid_size
                if bid_drop > _LIQUIDITY_PULL_THRESHOLD:
                    liquidity_pull = True
            if state.prev_ask_size > 0:
                ask_drop = (state.prev_ask_size - ask_sz) / state.prev_ask_size
                if ask_drop > _LIQUIDITY_PULL_THRESHOLD:
                    liquidity_pull = True

        from datetime import datetime, timezone
        return MicrostructureSnapshot(
            symbol=symbol,
            timestamp=ts or datetime.now(tz=timezone.utc),
            session=session,
            spread=spread,
            spread_pct=spread_pct,
            mid=mid,
            bid_ask_imbalance=bid_ask_imbalance,
            depth_imbalance=depth_imbalance,
            quote_pressure=state.quote_pressure,
            liquidity_pull=liquidity_pull,
        )

    async def _publish(self, snap: MicrostructureSnapshot) -> None:
        logger.debug(
            "microstructure_engine.computed",
            symbol=snap.symbol,
            spread=snap.spread,
            bid_ask_imbalance=snap.bid_ask_imbalance,
            quote_pressure=snap.quote_pressure,
            liquidity_pull=snap.liquidity_pull,
            channel=microstructure_channel(snap.symbol),
        )
        await self._bus.publish(
            Event(
                type=EventType.MICROSTRUCTURE_UPDATED,
                payload=snap,
                source=f"microstructure_engine:{snap.symbol}",
            )
        )
