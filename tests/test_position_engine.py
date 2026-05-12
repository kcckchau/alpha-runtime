"""
Tests for PositionEngine stop/target hit logic.

Follows the pattern established in test_risk_engine.py:
- Fresh EventBus per test
- Publish events to drive the engine
- Assert published events and position state
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from packages.core.models import (
    ExitReason,
    ExitTrigger,
    Fill,
    OrderRequest,
    OrderSide,
    Position,
    PositionStatus,
    SetupSignal,
    SignalDirection,
    SignalStatus,
    Tick,
)
from packages.messaging.bus import Event, EventBus, EventType
from runtime.position_engine.engine import PositionEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2024, 1, 15, 14, 35, tzinfo=timezone.utc)


def _make_signal(
    symbol: str = "QQQ",
    strategy_id: str = "strat_a",
    entry: float = 400.0,
    stop: float = 398.0,
    target: float = 406.0,
) -> SetupSignal:
    rr = (target - entry) / (entry - stop)
    return SetupSignal(
        strategy_id=strategy_id,
        symbol=symbol,
        direction=SignalDirection.LONG,
        status=SignalStatus.APPROVED,
        timestamp=_TS,
        entry_price=entry,
        stop_price=stop,
        target_price=target,
        risk_reward=rr,
        confidence=0.8,
    )


def _make_order(signal_id: str, symbol: str = "QQQ", side: OrderSide = OrderSide.BUY) -> OrderRequest:
    return OrderRequest(
        signal_id=signal_id,
        symbol=symbol,
        side=side,
        quantity=5.0,
        limit_price=400.0,
        stop_price=398.0,
        timestamp=_TS,
        paper=True,
    )


def _make_fill(
    order_id: str,
    symbol: str = "QQQ",
    price: float = 400.0,
    side: OrderSide = OrderSide.BUY,
) -> Fill:
    return Fill(
        order_id=order_id,
        symbol=symbol,
        side=side,
        quantity=5.0,
        price=price,
        timestamp=_TS,
        paper=True,
    )


def _make_tick(
    symbol: str = "QQQ",
    open_: float = 400.0,
    high: float = 401.0,
    low: float = 399.0,
    close: float = 400.5,
) -> Tick:
    return Tick(
        symbol=symbol,
        timestamp=_TS,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=1000.0,
    )


async def _setup_position(
    bus: EventBus,
    symbol: str = "QQQ",
    entry: float = 400.0,
    stop: float = 398.0,
    target: float = 406.0,
) -> tuple[SetupSignal, OrderRequest, Fill]:
    """Publish SIGNAL_APPROVED → ORDER_REQUESTED → ORDER_FILLED to create a position."""
    signal = _make_signal(symbol=symbol, entry=entry, stop=stop, target=target)
    await bus.publish(Event(type=EventType.SIGNAL_APPROVED, payload=signal, source="test"))

    order = _make_order(signal_id=signal.id, symbol=symbol)
    await bus.publish(Event(type=EventType.ORDER_REQUESTED, payload=order, source="test"))

    fill = _make_fill(order_id=order.id, symbol=symbol, price=entry)
    await bus.publish(Event(type=EventType.ORDER_FILLED, payload=fill, source="test"))

    return signal, order, fill


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_position_opened_on_fill() -> None:
    """ORDER_FILLED (BUY) should create a position and publish POSITION_OPENED."""
    bus = EventBus()
    engine = PositionEngine(bus)

    opened: list[Position] = []

    async def on_opened(e: Event) -> None:
        opened.append(e.payload)

    bus.subscribe(EventType.POSITION_OPENED, on_opened)

    await _setup_position(bus)

    assert len(opened) == 1
    pos = opened[0]
    assert pos.symbol == "QQQ"
    assert pos.entry_price == 400.0
    assert pos.stop_price == 398.0
    assert pos.target_price == 406.0
    assert pos.status == PositionStatus.OPEN
    assert pos.side == OrderSide.BUY
    assert pos.quantity == 5.0


@pytest.mark.asyncio
async def test_stop_hit_publishes_exit_triggered() -> None:
    """A tick whose low <= stop_price should publish EXIT_TRIGGERED with STOP_HIT."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list[ExitTrigger] = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)

    await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    # Tick with low exactly at stop
    tick = _make_tick(symbol="QQQ", open_=400.0, high=400.5, low=398.0, close=399.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    assert len(exits) == 1
    assert exits[0].reason == ExitReason.STOP_HIT
    assert exits[0].fill_price == 398.0
    assert exits[0].symbol == "QQQ"
    assert exits[0].quantity == 5.0


@pytest.mark.asyncio
async def test_stop_hit_below_stop_price() -> None:
    """A tick whose low < stop_price (gapped through) should still trigger STOP_HIT."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list[ExitTrigger] = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)

    await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    tick = _make_tick(symbol="QQQ", open_=399.0, high=399.5, low=397.0, close=397.5)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    assert len(exits) == 1
    assert exits[0].reason == ExitReason.STOP_HIT
    assert exits[0].fill_price == 398.0  # fills at stop_price, not below


@pytest.mark.asyncio
async def test_target_hit_publishes_exit_triggered() -> None:
    """A tick whose high >= target_price should publish EXIT_TRIGGERED with TARGET_HIT."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list[ExitTrigger] = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)

    await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    tick = _make_tick(symbol="QQQ", open_=402.0, high=406.0, low=401.5, close=405.5)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    assert len(exits) == 1
    assert exits[0].reason == ExitReason.TARGET_HIT
    assert exits[0].fill_price == 406.0  # fills at target_price


@pytest.mark.asyncio
async def test_no_exit_when_price_between_stop_and_target() -> None:
    """A tick with price between stop and target should NOT trigger an exit."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list[ExitTrigger] = []
    updates: list[Position] = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    async def on_update(e: Event) -> None:
        updates.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)
    bus.subscribe(EventType.POSITION_UPDATED, on_update)

    await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    tick = _make_tick(symbol="QQQ", open_=400.0, high=402.0, low=399.0, close=401.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    assert len(exits) == 0
    assert len(updates) == 1
    pos = updates[0]
    assert pos.status == PositionStatus.OPEN
    assert pos.current_price == 401.0
    assert pos.unrealized_pnl == pytest.approx((401.0 - 400.0) * 5.0)


@pytest.mark.asyncio
async def test_no_double_exit_after_stop_hit() -> None:
    """Once a stop is hit, subsequent ticks below stop should not fire EXIT_TRIGGERED again."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list[ExitTrigger] = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)

    await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    tick1 = _make_tick(symbol="QQQ", low=397.5, high=399.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick1, source="test"))

    tick2 = _make_tick(symbol="QQQ", low=396.0, high=398.5)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick2, source="test"))

    assert len(exits) == 1, "Should only fire EXIT_TRIGGERED once"


@pytest.mark.asyncio
async def test_exit_triggers_closing_order() -> None:
    """EXIT_TRIGGERED should result in a SELL OrderRequest being published."""
    bus = EventBus()
    engine = PositionEngine(bus)

    orders: list[OrderRequest] = []

    async def on_order(e: Event) -> None:
        if isinstance(e.payload, OrderRequest):
            orders.append(e.payload)

    bus.subscribe(EventType.ORDER_REQUESTED, on_order)

    signal, first_order, _ = await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    # Consume the opening order from cache
    opening_orders = [o for o in orders if o.side == OrderSide.BUY]
    assert len(opening_orders) == 1

    tick = _make_tick(symbol="QQQ", low=397.5, high=399.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    closing_orders = [o for o in orders if o.side == OrderSide.SELL]
    assert len(closing_orders) == 1
    co = closing_orders[0]
    assert co.symbol == "QQQ"
    assert co.limit_price == 398.0  # stop_price
    assert co.quantity == 5.0
    assert co.signal_id == signal.id


@pytest.mark.asyncio
async def test_tick_for_different_symbol_ignored() -> None:
    """Ticks for symbols without open positions should be ignored."""
    bus = EventBus()
    engine = PositionEngine(bus)

    exits: list = []

    async def on_exit(e: Event) -> None:
        exits.append(e.payload)

    bus.subscribe(EventType.EXIT_TRIGGERED, on_exit)

    await _setup_position(bus, symbol="QQQ", entry=400.0, stop=398.0, target=406.0)

    # SPY tick that would trigger stop/target logic if wrongly applied to QQQ
    tick = _make_tick(symbol="SPY", open_=200.0, high=210.0, low=190.0, close=200.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    assert len(exits) == 0


@pytest.mark.asyncio
async def test_position_closed_on_closing_fill() -> None:
    """When the closing SELL fill arrives, position status should transition to CLOSED."""
    bus = EventBus()
    engine = PositionEngine(bus)

    orders: list[OrderRequest] = []

    async def on_order(e: Event) -> None:
        if isinstance(e.payload, OrderRequest):
            orders.append(e.payload)

    bus.subscribe(EventType.ORDER_REQUESTED, on_order)

    signal, _, _ = await _setup_position(bus, entry=400.0, stop=398.0, target=406.0)

    # Hit the stop
    tick = _make_tick(symbol="QQQ", low=397.5, high=399.0)
    await bus.publish(Event(type=EventType.TICK_RECEIVED, payload=tick, source="test"))

    # Simulate closing fill
    closing_order = next(o for o in orders if o.side == OrderSide.SELL)
    closing_fill = _make_fill(
        order_id=closing_order.id,
        symbol="QQQ",
        price=398.0,
        side=OrderSide.SELL,
    )
    await bus.publish(Event(type=EventType.ORDER_FILLED, payload=closing_fill, source="test"))

    open_positions = engine.get_open_positions()
    assert len(open_positions) == 0

    assert engine.get_open_position_for_symbol("QQQ") is None
