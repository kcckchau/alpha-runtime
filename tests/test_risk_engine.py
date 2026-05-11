"""
Tests for RiskEngine.

Covers:
- _on_fill removes all strategy_id:symbol keys (not just symbol)
- Duplicate prevention: second signal rejected while order is open
- After fill, same strategy+symbol can be approved again
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from packages.core.models import Fill, OrderSide, SessionType, SetupSignal, SignalDirection, SignalStatus
from packages.messaging.bus import Event, EventBus, EventType
from runtime.risk_engine.config import RiskSettings
from runtime.risk_engine.engine import RiskEngine


def _make_signal(
    symbol: str = "QQQ",
    strategy_id: str = "strat_a",
    entry: float = 400.0,
    stop: float = 398.0,
    target: float = 404.0,
) -> SetupSignal:
    rr = (target - entry) / (entry - stop)
    return SetupSignal(
        strategy_id=strategy_id,
        symbol=symbol,
        direction=SignalDirection.LONG,
        timestamp=datetime(2024, 1, 15, 14, 35, tzinfo=timezone.utc),
        entry_price=entry,
        stop_price=stop,
        target_price=target,
        risk_reward=rr,
        confidence=0.8,
    )


def _make_fill(symbol: str = "QQQ") -> Fill:
    return Fill(
        order_id="ord-1",
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=5.0,
        price=400.0,
        timestamp=datetime(2024, 1, 15, 14, 36, tzinfo=timezone.utc),
    )


def _low_rr_settings() -> RiskSettings:
    return RiskSettings(rth_only=False, min_rr=0.0, max_daily_loss=10_000.0, risk_per_trade=100.0, max_shares=500.0)


@pytest.mark.asyncio
async def test_on_fill_clears_open_order_by_strategy_symbol_key() -> None:
    """After a fill, the strategy+symbol slot should be freed so the next signal is accepted."""
    bus = EventBus()
    engine = RiskEngine(bus, settings=_low_rr_settings())

    approved: list = []
    rejected: list = []

    async def on_approved(e):
        approved.append(e.payload)

    async def on_rejected(e):
        rejected.append(e.payload)

    bus.subscribe(EventType.SIGNAL_APPROVED, on_approved)
    bus.subscribe(EventType.SIGNAL_REJECTED, on_rejected)

    # First signal → approved, key "strat_a:QQQ" added
    sig1 = _make_signal()
    await bus.publish(Event(type=EventType.SIGNAL_DETECTED, payload=sig1, source="test"))
    assert len(approved) == 1
    assert len(rejected) == 0

    # Second signal before fill → duplicate → rejected
    sig2 = _make_signal()
    await bus.publish(Event(type=EventType.SIGNAL_DETECTED, payload=sig2, source="test"))
    assert len(rejected) == 1
    assert rejected[0].status == SignalStatus.REJECTED

    # Fill arrives → key removed
    fill = _make_fill("QQQ")
    await bus.publish(Event(type=EventType.ORDER_FILLED, payload=fill, source="test"))

    # Third signal after fill → approved again
    sig3 = _make_signal()
    await bus.publish(Event(type=EventType.SIGNAL_DETECTED, payload=sig3, source="test"))
    assert len(approved) == 2, "Should be re-approved after fill clears the open-order key"


@pytest.mark.asyncio
async def test_on_fill_does_not_clear_different_symbol() -> None:
    """A fill for SPY must not free QQQ's open-order slot."""
    bus = EventBus()
    engine = RiskEngine(bus, settings=_low_rr_settings())

    rejected: list = []

    async def on_rejected(e):
        rejected.append(e.payload)

    bus.subscribe(EventType.SIGNAL_REJECTED, on_rejected)

    sig = _make_signal(symbol="QQQ")
    await bus.publish(Event(type=EventType.SIGNAL_DETECTED, payload=sig, source="test"))

    # Fill for a different symbol
    fill_spy = _make_fill("SPY")
    await bus.publish(Event(type=EventType.ORDER_FILLED, payload=fill_spy, source="test"))

    # QQQ slot is still open → rejected
    sig2 = _make_signal(symbol="QQQ")
    await bus.publish(Event(type=EventType.SIGNAL_DETECTED, payload=sig2, source="test"))
    assert len(rejected) == 1, "QQQ slot should still be blocked after SPY fill"
