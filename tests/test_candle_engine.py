"""
Unit tests for the candle aggregation engine.

Tests cover:
- 5s tick → 1m candle aggregation
- VWAP computation
- Partial vs final candle emission
- Session reset clearing VWAP accumulators
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from packages.core.models import SessionType, Tick, Timeframe
from packages.messaging.bus import EventBus, EventType
from runtime.candle_engine.engine import CandleEngine, _floor_timestamp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_tick(
    symbol: str = "QQQ",
    ts: str = "2024-01-15T14:30:00+00:00",
    open: float = 400.0,
    high: float = 401.0,
    low: float = 399.0,
    close: float = 400.5,
    volume: float = 1000.0,
    session: SessionType = SessionType.RTH,
) -> Tick:
    return Tick(
        symbol=symbol,
        timestamp=datetime.fromisoformat(ts),
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        session=session,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_floor_timestamp_1m() -> None:
    ts = datetime(2024, 1, 15, 14, 32, 47, tzinfo=timezone.utc)
    floored = _floor_timestamp(ts, 60)
    assert floored == datetime(2024, 1, 15, 14, 32, 0, tzinfo=timezone.utc)


def test_floor_timestamp_5m() -> None:
    ts = datetime(2024, 1, 15, 14, 33, 47, tzinfo=timezone.utc)
    floored = _floor_timestamp(ts, 300)
    assert floored == datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_partial_candle_emitted_on_each_tick() -> None:
    bus = EventBus()
    engine = CandleEngine(bus)
    engine.register_symbol("QQQ")

    partial_events: list = []

    async def capture(event):
        partial_events.append(event.payload)

    bus.subscribe(EventType.CANDLE_PARTIAL, capture)

    tick = make_tick(ts="2024-01-15T14:30:05+00:00")
    await engine.on_tick(tick)

    # 3 timeframes (1m, 5m, 1h) × 1 tick = 3 partial events
    assert len(partial_events) == 3


@pytest.mark.asyncio
async def test_final_candle_emitted_when_period_closes() -> None:
    bus = EventBus()
    engine = CandleEngine(bus)
    engine.register_symbol("QQQ")

    finals: list = []

    async def capture_final(event):
        finals.append(event.payload)

    bus.subscribe(EventType.CANDLE_FINAL, capture_final)

    # Tick in bar 1 (14:30)
    await engine.on_tick(make_tick(ts="2024-01-15T14:30:05+00:00"))
    assert len(finals) == 0  # no final yet

    # Tick in bar 2 (14:31) — should close the 14:30 bar
    await engine.on_tick(make_tick(ts="2024-01-15T14:31:05+00:00"))
    # One final for 1m timeframe (14:30 bar), none yet for 5m
    one_min_finals = [f for f in finals if f.timeframe == Timeframe.MIN_1]
    assert len(one_min_finals) == 1
    assert one_min_finals[0].timestamp == datetime(2024, 1, 15, 14, 30, 0, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_vwap_incremental() -> None:
    """VWAP = cumulative(typical * vol) / cumulative(vol)."""
    bus = EventBus()
    engine = CandleEngine(bus)
    engine.register_symbol("QQQ")

    finals: list = []

    async def capture(event):
        if event.payload.timeframe == Timeframe.MIN_1:
            finals.append(event.payload)

    bus.subscribe(EventType.CANDLE_FINAL, capture)

    # Three ticks in bar 1
    for i in range(3):
        await engine.on_tick(make_tick(
            ts=f"2024-01-15T14:30:0{i+1}+00:00",
            high=401.0,
            low=399.0,
            close=400.0,
            volume=100.0,
        ))

    # Tick in bar 2 to close bar 1
    await engine.on_tick(make_tick(ts="2024-01-15T14:31:01+00:00"))
    assert len(finals) == 1

    candle = finals[0]
    # Typical price = (401 + 399 + 400) / 3 = 400.0; same for all 3 ticks
    assert abs(candle.vwap - 400.0) < 0.01


@pytest.mark.asyncio
async def test_session_reset_clears_vwap() -> None:
    """Session change from premarket to RTH should reset cumulative VWAP."""
    bus = EventBus()
    engine = CandleEngine(bus)
    engine.register_symbol("QQQ")

    finals: list = []

    async def capture(event):
        if event.payload.timeframe == Timeframe.MIN_1:
            finals.append(event.payload)

    bus.subscribe(EventType.CANDLE_FINAL, capture)

    # Premarket tick (high price to inflate VWAP)
    await engine.on_tick(make_tick(
        ts="2024-01-15T13:29:05+00:00",  # 09:29 ET = premarket
        high=500.0, low=498.0, close=499.0,
        volume=500.0,
        session=SessionType.PREMARKET,
    ))

    # RTH tick — should start fresh VWAP
    await engine.on_tick(make_tick(
        ts="2024-01-15T14:30:05+00:00",
        high=401.0, low=399.0, close=400.0,
        volume=100.0,
        session=SessionType.RTH,
    ))

    # Close the RTH bar
    await engine.on_tick(make_tick(ts="2024-01-15T14:31:05+00:00", session=SessionType.RTH))

    rth_finals = [f for f in finals if f.session == SessionType.RTH]
    assert len(rth_finals) == 1
    # VWAP should be near 400, not skewed by the 499 premarket tick
    assert abs(rth_finals[0].vwap - 400.0) < 1.0
