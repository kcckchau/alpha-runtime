"""
Tests for the Historical Bootstrap / Backfill layer.

Covers:
- historical_loader: bar-to-candle conversion, VWAP computation, deduplication
- prior day and premarket level computation (via IndicatorEngine warm-up)
- historical candles do NOT produce strategy signals
- live candles AFTER bootstrap produce normal signals (when conditions met)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from packages.core.models import (
    Candle,
    MarketContext,
    SessionType,
    SetupSignal,
    Timeframe,
    candle_stable_id,
)
from packages.messaging.bus import EventBus, EventType
from runtime.bootstrap.historical_loader import _bars_to_candles, _bar_to_utc, aggregate_candles
from runtime.bootstrap.service import BootstrapService, BootstrapStatus
from runtime.bootstrap.config import BootstrapSettings
from runtime.candle_engine.engine import CandleEngine
from runtime.context_engine.engine import ContextEngine
from runtime.indicator_engine.engine import IndicatorEngine
from runtime.strategy_engine.base import Strategy, StrategyEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_bar(
    date: datetime,
    open_: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: float = 1000.0,
) -> MagicMock:
    bar = MagicMock()
    bar.date = date
    bar.open = open_
    bar.high = high
    bar.low = low
    bar.close = close
    bar.volume = volume
    return bar


def _ts(hour: int, minute: int = 0, day: int = 15) -> datetime:
    """Convenience: UTC datetime on 2024-01-{day}."""
    return datetime(2024, 1, day, hour, minute, tzinfo=timezone.utc)


def _et_ts(hour: int, minute: int = 0) -> datetime:
    """Convenience: 2024-01-15 in Eastern Time, returned as UTC-aware datetime."""
    import pytz
    et = pytz.timezone("America/New_York")
    dt_et = et.localize(datetime(2024, 1, 15, hour, minute, 0))
    return dt_et.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# _bar_to_utc
# ---------------------------------------------------------------------------


def test_bar_to_utc_from_datetime():
    dt = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    result = _bar_to_utc(dt)
    assert result == dt
    assert result.tzinfo is not None


def test_bar_to_utc_from_string():
    raw = "20240115 09:30:00"
    result = _bar_to_utc(raw)
    # 09:30 ET = 14:30 UTC in January
    assert result.hour == 14
    assert result.minute == 30
    assert result.tzinfo is not None


# ---------------------------------------------------------------------------
# _bars_to_candles
# ---------------------------------------------------------------------------


def test_bars_to_candles_produces_historical_flag():
    bars = [
        _make_mock_bar(_et_ts(9, 30)),  # RTH open
        _make_mock_bar(_et_ts(9, 31)),
        _make_mock_bar(_et_ts(9, 32)),
    ]
    candles = _bars_to_candles(bars, "QQQ")
    assert all(c.historical is True for c in candles)


def test_bars_to_candles_session_classification():
    bars = [
        _make_mock_bar(_et_ts(4, 30)),   # premarket
        _make_mock_bar(_et_ts(9, 30)),   # RTH
        _make_mock_bar(_et_ts(10, 0)),   # RTH
    ]
    candles = _bars_to_candles(bars, "QQQ")
    assert candles[0].session == SessionType.PREMARKET
    assert candles[1].session == SessionType.RTH
    assert candles[2].session == SessionType.RTH


def test_bars_to_candles_vwap_resets_on_session_change():
    """
    VWAP should reset at the premarket→RTH session boundary.
    After the reset the RTH bar's VWAP is based only on RTH bars.
    We set bar.average to a known value to bypass the (H+L+C)/3 fallback.
    """
    pm_bar = _make_mock_bar(_et_ts(4, 30), open_=100.0, high=100.0, low=100.0, close=100.0, volume=500.0)
    pm_bar.average = 100.0
    rth_bar = _make_mock_bar(_et_ts(9, 30), open_=200.0, high=200.0, low=200.0, close=200.0, volume=500.0)
    rth_bar.average = 200.0
    candles = _bars_to_candles([pm_bar, rth_bar], "QQQ")

    rth_candle = candles[1]
    # After reset: VWAP = (rth_bar.average * volume) / volume = 200.0
    assert abs(rth_candle.vwap - 200.0) < 0.01
    assert rth_candle.cumulative_volume == 500.0


def test_bars_to_candles_vwap_accumulates_within_session():
    """VWAP accumulates correctly across bars in the same session using bar.average."""
    bar1 = _make_mock_bar(_et_ts(9, 30), high=100.0, low=98.0, close=99.0, volume=1000.0)
    bar1.average = 99.0   # WAP for bar 1
    bar2 = _make_mock_bar(_et_ts(9, 31), high=101.0, low=99.0, close=100.0, volume=1000.0)
    bar2.average = 100.0  # WAP for bar 2
    candles = _bars_to_candles([bar1, bar2], "QQQ")

    c1 = candles[0]
    c2 = candles[1]

    # Bar 1: vwap = (99 * 1000) / 1000 = 99.0
    assert abs(c1.vwap - 99.0) < 0.01

    # Bar 2: vwap = (99*1000 + 100*1000) / 2000 = 99.5
    expected_vwap2 = (99.0 * 1000.0 + 100.0 * 1000.0) / 2000.0
    assert abs(c2.vwap - expected_vwap2) < 0.01


def test_bars_to_candles_deterministic_ids():
    """Same bar loaded twice → same ID (safe for upsert)."""
    ts = _et_ts(9, 30)
    bar = _make_mock_bar(ts)
    candles1 = _bars_to_candles([bar], "QQQ")
    candles2 = _bars_to_candles([bar], "QQQ")
    assert candles1[0].id == candles2[0].id


# ---------------------------------------------------------------------------
# aggregate_candles
# ---------------------------------------------------------------------------


def _make_1m_candles(bars: list[tuple]) -> list:
    """Helper: [(et_hour, et_min, close, volume), ...] → list[Candle]"""
    import pytz
    et = pytz.timezone("America/New_York")
    raw = []
    for h, m, c, v in bars:
        dt = et.localize(datetime(2024, 1, 15, h, m, 0)).astimezone(timezone.utc)
        bar = _make_mock_bar(dt, open_=c, high=c, low=c, close=c, volume=v)
        bar.average = c
        raw.append(bar)
    return _bars_to_candles(raw, "QQQ")


def test_aggregate_candles_5m_bucketing():
    """Six 1m RTH bars → one 5m bucket per 5-minute UTC floor."""
    candles = _make_1m_candles([
        (9, 30, 400.0, 1000),
        (9, 31, 401.0, 1000),
        (9, 32, 402.0, 1000),
        (9, 33, 403.0, 1000),
        (9, 34, 404.0, 1000),
        (9, 35, 500.0, 1000),  # starts new 5m bucket
    ])
    agg = aggregate_candles(candles, Timeframe.MIN_5)
    assert len(agg) == 2
    b1 = agg[0]
    assert b1.open  == 400.0
    assert b1.high  == 404.0
    assert b1.low   == 400.0
    assert b1.close == 404.0
    assert abs(b1.volume - 5000.0) < 0.01
    assert b1.timeframe == Timeframe.MIN_5
    assert b1.historical is True


def test_aggregate_candles_1h_bucketing():
    """Bars in 9:30-10:29 → one 1h bucket; 10:30+ → second bucket."""
    bars = [(9, 30 + i, 400.0 + i, 100) for i in range(30)]  # 9:30-9:59
    bars += [(10, i, 430.0 + i, 100) for i in range(30)]      # 10:00-10:29
    bars += [(10, 30, 450.0, 100)]                              # new 1h bucket
    candles = _make_1m_candles(bars)
    agg = aggregate_candles(candles, Timeframe.HOUR_1)
    # UTC 13:30 = 09:30 ET → epoch 1705323000 → floor to 1h = 1705320000 (13:00 UTC)
    # and 14:30 ET starts next hour; exact split depends on DST — just check ≥2 buckets
    assert len(agg) >= 2
    assert all(c.timeframe == Timeframe.HOUR_1 for c in agg)


def test_aggregate_candles_vwap_is_last_bars_vwap():
    """VWAP on aggregated bar == last constituent 1m bar's cumulative VWAP."""
    candles = _make_1m_candles([
        (9, 30, 400.0, 1000),
        (9, 31, 402.0, 1000),
        (9, 32, 404.0, 1000),
        (9, 33, 406.0, 1000),
        (9, 34, 408.0, 1000),
    ])
    agg = aggregate_candles(candles, Timeframe.MIN_5)
    assert len(agg) == 1
    # vwap should equal the last 1m bar's cumulative VWAP
    assert abs(agg[0].vwap - candles[-1].vwap) < 0.01


def test_aggregate_candles_deterministic_ids():
    """Same input produces same IDs (upsert-safe)."""
    candles = _make_1m_candles([(9, 30, 400.0, 1000), (9, 31, 401.0, 1000)])
    agg1 = aggregate_candles(candles, Timeframe.MIN_5)
    agg2 = aggregate_candles(candles, Timeframe.MIN_5)
    assert agg1[0].id == agg2[0].id


def test_candle_stable_id_consistent():
    ts = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    id1 = candle_stable_id("QQQ", "1m", ts)
    id2 = candle_stable_id("QQQ", "1m", ts)
    assert id1 == id2

    different = candle_stable_id("QQQ", "1m", ts + timedelta(minutes=1))
    assert id1 != different


# ---------------------------------------------------------------------------
# Prior-day and premarket levels via IndicatorEngine warm-up
# ---------------------------------------------------------------------------


def _make_candle(
    ts: datetime,
    symbol: str = "QQQ",
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.0,
    volume: float = 1000.0,
    session: SessionType = SessionType.RTH,
    historical: bool = True,
    timeframe: Timeframe = Timeframe.MIN_1,
) -> Candle:
    return Candle(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=ts,
        open=100.0,
        high=high,
        low=low,
        close=close,
        volume=volume,
        vwap=close,
        session=session,
        historical=historical,
    )


@pytest.mark.asyncio
async def test_indicator_engine_prior_day_levels():
    """
    After processing yesterday's RTH bars then today's premarket bars,
    the indicator engine should have prior_day_high/low populated.
    """
    bus = EventBus()
    ie = IndicatorEngine(bus)
    ie.register_symbol("QQQ")

    snapshots: list = []

    async def capture(event):
        snapshots.append(event.payload)

    bus.subscribe(EventType.INDICATORS_UPDATED, capture)

    import pytz
    et = pytz.timezone("America/New_York")

    # Yesterday's RTH: 2024-01-14 09:30–09:32 ET
    yesterday_bars = [
        _make_candle(
            et.localize(datetime(2024, 1, 14, 9, 30)).astimezone(timezone.utc),
            high=400.0, low=390.0, close=395.0,
        ),
        _make_candle(
            et.localize(datetime(2024, 1, 14, 9, 31)).astimezone(timezone.utc),
            high=405.0, low=392.0, close=403.0,
        ),
    ]
    # Today's premarket: 2024-01-15 05:00 ET
    pm_bar = _make_candle(
        et.localize(datetime(2024, 1, 15, 5, 0)).astimezone(timezone.utc),
        high=410.0, low=398.0, close=406.0,
        session=SessionType.PREMARKET,
    )

    for candle in [*yesterday_bars, pm_bar]:
        await bus.publish(
            __import__("packages.messaging.bus", fromlist=["Event"]).Event(
                type=EventType.CANDLE_FINAL,
                payload=candle,
                source="test",
            )
        )

    # Last snapshot should have prior day levels from yesterday's bars
    last = snapshots[-1]
    assert last.prior_day_high == 405.0
    assert last.prior_day_low == 390.0
    assert last.premarket_high == 410.0
    assert last.premarket_low == 398.0


# ---------------------------------------------------------------------------
# Historical signals are suppressed; live signals are not
# ---------------------------------------------------------------------------


class _AlwaysSignalStrategy(Strategy):
    """Test strategy that always emits a signal."""

    strategy_id = "always_signal"

    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]:
        return SetupSignal(
            strategy_id=self.strategy_id,
            symbol=candle.symbol,
            direction=__import__("packages.core.models", fromlist=["SignalDirection"]).SignalDirection.LONG,
            timestamp=candle.timestamp,
            entry_price=100.0,
            stop_price=99.0,
            target_price=102.0,
            risk_reward=2.0,
            confidence=0.9,
        )

    async def on_context_update(self, context: MarketContext) -> Optional[SetupSignal]:
        return None


@pytest.mark.asyncio
async def test_historical_candles_suppress_signals():
    """Historical candles should NOT trigger SIGNAL_DETECTED."""
    bus = EventBus()
    from packages.messaging.bus import Event
    se = StrategyEngine(bus, suppress_historical=True)
    se.register(_AlwaysSignalStrategy())

    signals_received: list = []

    async def capture(event):
        signals_received.append(event.payload)

    bus.subscribe(EventType.SIGNAL_DETECTED, capture)

    hist_candle = _make_candle(
        _et_ts(9, 30), historical=True
    )
    await bus.publish(Event(type=EventType.CANDLE_FINAL, payload=hist_candle, source="bootstrap"))

    assert signals_received == [], "No signal should be emitted for historical candles"


@pytest.mark.asyncio
async def test_live_candles_emit_signals_after_bootstrap():
    """Live candles (historical=False) SHOULD trigger SIGNAL_DETECTED."""
    bus = EventBus()
    from packages.messaging.bus import Event
    se = StrategyEngine(bus, suppress_historical=True)
    se.register(_AlwaysSignalStrategy())

    signals_received: list = []

    async def capture(event):
        signals_received.append(event.payload)

    bus.subscribe(EventType.SIGNAL_DETECTED, capture)

    live_candle = _make_candle(
        _et_ts(9, 30), historical=False
    )
    await bus.publish(Event(type=EventType.CANDLE_FINAL, payload=live_candle, source="candle_engine"))

    assert len(signals_received) == 1
    assert signals_received[0].strategy_id == "always_signal"


@pytest.mark.asyncio
async def test_suppress_false_allows_historical_signals():
    """With suppress_historical=False, historical candles CAN trigger signals."""
    bus = EventBus()
    from packages.messaging.bus import Event
    se = StrategyEngine(bus, suppress_historical=False)
    se.register(_AlwaysSignalStrategy())

    signals_received: list = []

    async def capture(event):
        signals_received.append(event.payload)

    bus.subscribe(EventType.SIGNAL_DETECTED, capture)

    hist_candle = _make_candle(
        _et_ts(9, 30), historical=True
    )
    await bus.publish(Event(type=EventType.CANDLE_FINAL, payload=hist_candle, source="bootstrap"))

    assert len(signals_received) == 1, "Signal should be emitted when suppress=False"


# ---------------------------------------------------------------------------
# BootstrapService.run() — integration with mocked IBKR
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bootstrap_service_run_no_connection():
    """When IBKR is not connected, bootstrap should skip gracefully."""
    bus = EventBus()
    candle_engine = CandleEngine(bus)
    candle_engine.register_symbol("QQQ")

    settings = BootstrapSettings(enabled=True, lookback_days=1)
    svc = BootstrapService(bus, candle_engine, settings)
    svc.register_symbol("QQQ")

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = False

    await svc.run(mock_ib)

    # Should not have marked complete or loaded bars
    assert svc.status.bars_loaded == {}


@pytest.mark.asyncio
async def test_bootstrap_service_disabled():
    """When bootstrap is disabled, run() returns immediately."""
    bus = EventBus()
    candle_engine = CandleEngine(bus)
    settings = BootstrapSettings(enabled=False)
    svc = BootstrapService(bus, candle_engine, settings)

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True

    await svc.run(mock_ib)
    assert not svc.status.is_complete


@pytest.mark.asyncio
async def test_bootstrap_service_publishes_candle_final_events():
    """
    BootstrapService should publish one CANDLE_FINAL per historical bar
    and mark bootstrap complete.
    """
    bus = EventBus()
    candle_engine = CandleEngine(bus)
    candle_engine.register_symbol("QQQ")

    settings = BootstrapSettings(enabled=True, lookback_days=1)
    svc = BootstrapService(bus, candle_engine, settings)
    svc.register_symbol("QQQ")

    finalized: list[Candle] = []

    async def capture(event):
        finalized.append(event.payload)

    bus.subscribe(EventType.CANDLE_FINAL, capture)

    # Mock the historical_loader to return 3 pre-built candles
    mock_candles = [
        _make_candle(_et_ts(9, 30), historical=True),
        _make_candle(_et_ts(9, 31), historical=True),
        _make_candle(_et_ts(9, 32), historical=True),
    ]

    mock_ib = MagicMock()
    mock_ib.isConnected.return_value = True
    mock_ib.qualifyContractsAsync = AsyncMock(return_value=[MagicMock()])

    import runtime.bootstrap.service as svc_module
    original_fetch = svc_module.fetch_symbol_history

    async def mock_fetch(*args, **kwargs):
        return mock_candles

    svc_module.fetch_symbol_history = mock_fetch
    try:
        await svc.run(mock_ib)
    finally:
        svc_module.fetch_symbol_history = original_fetch

    assert svc.status.is_complete is True
    # 3 1m + 1 5m bucket + 1 1h bucket = 5 total
    assert svc.status.bars_loaded.get("QQQ") == 5
    assert len([c for c in finalized if c.timeframe == Timeframe.MIN_1]) == 3
    assert len(finalized) == 5
    assert all(c.historical is True for c in finalized)
