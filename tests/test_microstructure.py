"""
Unit tests for the microstructure layer.

Covers:
- QuoteSnapshot properties (spread, mid)
- MicrostructureEngine metric computation from L1
- MicrostructureEngine depth_imbalance from L2
- Liquidity pull detection
- VWAPReclaimLong microstructure veto logic
- StrategyEngine injects latest MicrostructureSnapshot into on_context_update
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from packages.core.models import (
    BookLevel,
    Candle,
    IndicatorSnapshot,
    MarketContext,
    MicrostructureSnapshot,
    OrderBookSnapshot,
    QuoteSnapshot,
    SessionType,
    SignalDirection,
    Timeframe,
)
from packages.messaging.bus import Event, EventBus, EventType
from runtime.microstructure_engine.engine import MicrostructureEngine
from runtime.quote_engine.engine import QuoteEngine
from runtime.orderbook_engine.engine import OrderBookEngine
from runtime.strategy_engine.base import StrategyEngine
from runtime.strategy_engine.strategies.vwap_reclaim import VWAPReclaimLong


_NOW = datetime(2024, 1, 15, 15, 0, 0, tzinfo=timezone.utc)  # RTH


# ── helpers ───────────────────────────────────────────────────────────────────


def make_quote(
    symbol: str = "QQQ",
    bid: float = 399.90,
    ask: float = 400.10,
    bid_size: float = 300.0,
    ask_size: float = 200.0,
) -> QuoteSnapshot:
    return QuoteSnapshot(
        symbol=symbol,
        timestamp=_NOW,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        session=SessionType.RTH,
    )


def make_book(
    symbol: str = "QQQ",
    bid_sizes: list[float] | None = None,
    ask_sizes: list[float] | None = None,
) -> OrderBookSnapshot:
    bid_sizes = bid_sizes or [300.0, 200.0, 100.0]
    ask_sizes = ask_sizes or [100.0, 150.0, 200.0]
    bids = [BookLevel(price=400.0 - i * 0.01, size=s) for i, s in enumerate(bid_sizes)]
    asks = [BookLevel(price=400.01 + i * 0.01, size=s) for i, s in enumerate(ask_sizes)]
    return OrderBookSnapshot(
        symbol=symbol, timestamp=_NOW, bids=bids, asks=asks, session=SessionType.RTH
    )


def make_candle(
    close: float = 400.5,
    low: float = 399.0,
    high: float = 401.0,
    open_: float = 399.5,
    rvol: float = 1.5,
    vwap_dist: float = 0.1,
) -> tuple[Candle, MarketContext]:
    ind = IndicatorSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        timeframe=Timeframe.MIN_1,
        vwap=400.0,
        vwap_distance_pct=vwap_dist,
        opening_range_high=401.0,
        opening_range_low=398.0,
        rolling_volume_avg=1_000_000.0,
        relative_volume=rvol,
        ema_9=399.8,
        ema_21=399.0,
    )
    candle = Candle(
        symbol="QQQ",
        timeframe=Timeframe.MIN_1,
        timestamp=_NOW,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1_500_000.0,
        vwap=400.0,
        session=SessionType.RTH,
    )
    ctx = MarketContext(
        symbol="QQQ",
        timestamp=_NOW,
        above_vwap=True,
        vwap_cross_up=True,
        vwap_distance_pct=vwap_dist,
        orb_breakout_up=False,
        higher_highs=True,
        session=SessionType.RTH,
        indicators=ind,
    )
    return candle, ctx


# ── QuoteSnapshot properties ──────────────────────────────────────────────────


def test_quote_snapshot_spread_and_mid() -> None:
    q = make_quote(bid=399.90, ask=400.10)
    assert abs(q.spread - 0.20) < 1e-9
    assert abs(q.mid - 400.00) < 1e-9


def test_quote_snapshot_spread_pct() -> None:
    q = make_quote(bid=399.90, ask=400.10)
    # spread_pct is computed in MicrostructureEngine, not QuoteSnapshot
    assert q.spread is not None
    assert q.mid is not None


def test_quote_snapshot_none_when_missing() -> None:
    q = QuoteSnapshot(symbol="QQQ", timestamp=_NOW, bid=None, ask=None)
    assert q.spread is None
    assert q.mid is None


# ── MicrostructureEngine — L1 metrics ────────────────────────────────────────


@pytest.mark.asyncio
async def test_microstructure_l1_spread_and_mid() -> None:
    bus = EventBus()
    engine = MicrostructureEngine(bus)
    engine.register_symbol("QQQ")

    received: list[MicrostructureSnapshot] = []

    async def capture(evt: Event) -> None:
        received.append(evt.payload)

    bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, capture)

    quote = make_quote(bid=399.90, ask=400.10, bid_size=300, ask_size=200)
    await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, quote, "test"))

    assert len(received) == 1
    snap = received[0]
    assert abs(snap.spread - 0.20) < 1e-6
    assert abs(snap.mid - 400.00) < 1e-6
    assert snap.spread_pct is not None and snap.spread_pct > 0


@pytest.mark.asyncio
async def test_microstructure_bid_ask_imbalance_positive() -> None:
    bus = EventBus()
    engine = MicrostructureEngine(bus)
    engine.register_symbol("QQQ")

    received: list[MicrostructureSnapshot] = []
    bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, lambda e: received.append(e.payload))

    # More bid size than ask → positive imbalance
    quote = make_quote(bid_size=300, ask_size=100)
    await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, quote, "test"))

    snap = received[0]
    assert snap.bid_ask_imbalance is not None
    assert snap.bid_ask_imbalance > 0


@pytest.mark.asyncio
async def test_microstructure_quote_pressure_smoothing() -> None:
    bus = EventBus()
    engine = MicrostructureEngine(bus)
    engine.register_symbol("QQQ")

    snaps: list[MicrostructureSnapshot] = []
    bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, lambda e: snaps.append(e.payload))

    # Fire 5 quotes with positive imbalance
    for _ in range(5):
        q = make_quote(bid_size=400, ask_size=100)
        await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, q, "test"))

    # quote_pressure should be positive and converging
    assert snaps[-1].quote_pressure is not None
    assert snaps[-1].quote_pressure > 0


@pytest.mark.asyncio
async def test_microstructure_liquidity_pull_detected() -> None:
    bus = EventBus()
    engine = MicrostructureEngine(bus)
    engine.register_symbol("QQQ")

    snaps: list[MicrostructureSnapshot] = []
    bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, lambda e: snaps.append(e.payload))

    # First quote: large bid size
    q1 = make_quote(bid_size=1000, ask_size=200)
    await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, q1, "test"))
    assert not snaps[-1].liquidity_pull

    # Second quote: bid size drops > 50 %
    q2 = make_quote(bid_size=400, ask_size=200)
    await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, q2, "test"))
    assert snaps[-1].liquidity_pull


# ── MicrostructureEngine — L2 depth imbalance ────────────────────────────────


@pytest.mark.asyncio
async def test_microstructure_depth_imbalance_bid_heavy() -> None:
    bus = EventBus()
    engine = MicrostructureEngine(bus)
    engine.register_symbol("QQQ")

    snaps: list[MicrostructureSnapshot] = []
    bus.subscribe(EventType.MICROSTRUCTURE_UPDATED, lambda e: snaps.append(e.payload))

    # Inject book with much more bid volume
    book = make_book(bid_sizes=[500, 400, 300], ask_sizes=[100, 100, 100])
    await engine._on_book_event(Event(EventType.ORDERBOOK_UPDATED, book, "test"))

    # Now fire a quote to trigger publish with the stored book
    q = make_quote()
    await engine._on_quote_event(Event(EventType.QUOTE_UPDATED, q, "test"))

    snap = snaps[-1]
    assert snap.depth_imbalance is not None
    assert snap.depth_imbalance > 0  # bid heavy


# ── VWAPReclaimLong microstructure veto ──────────────────────────────────────


def test_vwap_reclaim_passes_without_microstructure() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle, ctx = make_candle()
    strat._last_candle = candle

    signal = strat._evaluate(candle, ctx, None)
    assert signal is not None
    assert signal.direction == SignalDirection.LONG


def test_vwap_reclaim_vetoed_by_wide_spread() -> None:
    strat = VWAPReclaimLong(symbol="QQQ", max_spread_pct=0.05)
    candle, ctx = make_candle()
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=0.10,     # wider than threshold
        quote_pressure=0.2,
        liquidity_pull=False,
    )
    signal = strat._evaluate(candle, ctx, micro)
    assert signal is None


def test_vwap_reclaim_vetoed_by_negative_pressure() -> None:
    strat = VWAPReclaimLong(symbol="QQQ", min_quote_pressure=-0.1)
    candle, ctx = make_candle()
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=0.02,
        quote_pressure=-0.3,   # below threshold
        liquidity_pull=False,
    )
    signal = strat._evaluate(candle, ctx, micro)
    assert signal is None


def test_vwap_reclaim_vetoed_by_liquidity_pull() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle, ctx = make_candle()
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=0.02,
        quote_pressure=0.3,
        liquidity_pull=True,  # bid just got yanked
    )
    signal = strat._evaluate(candle, ctx, micro)
    assert signal is None


def test_vwap_reclaim_passes_with_good_microstructure() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle, ctx = make_candle()
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=0.02,
        quote_pressure=0.4,
        liquidity_pull=False,
    )
    signal = strat._evaluate(candle, ctx, micro)
    assert signal is not None


def test_vwap_reclaim_micro_disabled_ignores_bad_data() -> None:
    strat = VWAPReclaimLong(symbol="QQQ", use_microstructure=False)
    candle, ctx = make_candle()
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=1.0,         # terrible spread
        quote_pressure=-0.9,    # extremely negative
        liquidity_pull=True,
    )
    # With use_microstructure=False veto is disabled → signal still fires
    signal = strat._evaluate(candle, ctx, micro)
    assert signal is not None


# ── StrategyEngine injects microstructure into on_context_update ──────────────


@pytest.mark.asyncio
async def test_strategy_engine_injects_microstructure() -> None:
    bus = EventBus()
    engine = StrategyEngine(bus)

    injected_micro: list[Optional[MicrostructureSnapshot]] = []

    class _Spy(VWAPReclaimLong):
        async def on_context_update(
            self, context: MarketContext, microstructure=None
        ) -> None:
            injected_micro.append(microstructure)
            return None

    engine.register(_Spy(symbol="QQQ"))

    # Publish a microstructure event first so engine stores it
    micro = MicrostructureSnapshot(
        symbol="QQQ",
        timestamp=_NOW,
        spread_pct=0.02,
        quote_pressure=0.3,
        liquidity_pull=False,
    )
    await bus.publish(Event(EventType.MICROSTRUCTURE_UPDATED, micro, "test"))

    # Now publish a context event
    _, ctx = make_candle()
    await bus.publish(Event(EventType.CONTEXT_UPDATED, ctx, "test"))

    assert len(injected_micro) == 1
    assert injected_micro[0] is micro
