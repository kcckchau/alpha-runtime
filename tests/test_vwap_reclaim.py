"""
Unit tests for the QQQ VWAP Reclaim Long strategy.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from packages.core.models import (
    Candle,
    IndicatorSnapshot,
    MarketContext,
    SessionType,
    Timeframe,
)
from runtime.strategy_engine.strategies.vwap_reclaim import VWAPReclaimLong


def make_context(
    symbol: str = "QQQ",
    vwap_cross_up: bool = True,
    above_vwap: bool = True,
    session: SessionType = SessionType.RTH,
    or_high: float = 399.0,
    or_low: float = 395.0,
    vwap: float = 399.0,
    rvol: float = 1.5,
    vwap_dist: float = 0.25,
) -> MarketContext:
    indicators = IndicatorSnapshot(
        symbol=symbol,
        timestamp=datetime.now(tz=timezone.utc),
        timeframe=Timeframe.MIN_1,
        vwap=vwap,
        vwap_distance_pct=vwap_dist,
        opening_range_high=or_high,
        opening_range_low=or_low,
        opening_range_mid=(or_high + or_low) / 2,
        rolling_volume_avg=1_000_000.0,
        relative_volume=rvol,
    )
    return MarketContext(
        symbol=symbol,
        timestamp=datetime.now(tz=timezone.utc),
        above_vwap=above_vwap,
        vwap_cross_up=vwap_cross_up,
        vwap_distance_pct=vwap_dist,
        session=session,
        indicators=indicators,
    )


def make_candle(
    symbol: str = "QQQ",
    open: float = 398.0,
    high: float = 400.5,
    low: float = 397.5,
    close: float = 400.0,
    volume: float = 1_500_000.0,
    session: SessionType = SessionType.RTH,
) -> Candle:
    return Candle(
        symbol=symbol,
        timeframe=Timeframe.MIN_1,
        timestamp=datetime.now(tz=timezone.utc),
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
        vwap=399.0,
        session=session,
    )


@pytest.mark.asyncio
async def test_signal_fires_on_valid_vwap_reclaim() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle = make_candle()
    ctx = make_context()

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is not None
    assert signal.direction.value == "long"
    assert signal.entry_price == candle.close
    assert signal.stop_price < candle.close
    assert signal.risk_reward >= 2.0


@pytest.mark.asyncio
async def test_no_signal_without_vwap_cross() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle = make_candle()
    ctx = make_context(vwap_cross_up=False)

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None


@pytest.mark.asyncio
async def test_no_signal_outside_rth() -> None:
    strat = VWAPReclaimLong(symbol="QQQ")
    candle = make_candle(session=SessionType.PREMARKET)
    ctx = make_context(session=SessionType.PREMARKET)

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None


@pytest.mark.asyncio
async def test_no_signal_weak_close() -> None:
    """Close in the lower half of the bar should not trigger."""
    strat = VWAPReclaimLong(symbol="QQQ")
    # Open=400, High=402, Low=397, Close=398 → close_position = (398-397)/(402-397) = 0.2 < 0.5
    candle = make_candle(open=400.0, high=402.0, low=397.0, close=398.0)
    ctx = make_context()

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None


@pytest.mark.asyncio
async def test_no_signal_low_rvol() -> None:
    strat = VWAPReclaimLong(symbol="QQQ", min_rvol=1.5)
    candle = make_candle()
    ctx = make_context(rvol=0.8)

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None


@pytest.mark.asyncio
async def test_no_signal_overextended_from_vwap() -> None:
    strat = VWAPReclaimLong(symbol="QQQ", max_vwap_distance_pct=0.5)
    candle = make_candle()
    ctx = make_context(vwap_dist=1.2)  # > 0.5% threshold

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None


@pytest.mark.asyncio
async def test_no_signal_without_opening_range() -> None:
    """OR not yet established → no signal."""
    strat = VWAPReclaimLong(symbol="QQQ")
    candle = make_candle()

    # Context with no OR levels
    indicators = IndicatorSnapshot(
        symbol="QQQ",
        timestamp=datetime.now(tz=timezone.utc),
        timeframe=Timeframe.MIN_1,
        vwap=399.0,
        vwap_distance_pct=0.25,
        opening_range_high=None,
        opening_range_low=None,
        rolling_volume_avg=1_000_000.0,
        relative_volume=1.5,
    )
    ctx = MarketContext(
        symbol="QQQ",
        timestamp=datetime.now(tz=timezone.utc),
        vwap_cross_up=True,
        session=SessionType.RTH,
        indicators=indicators,
    )

    await strat.on_candle(candle)
    signal = await strat.on_context_update(ctx)

    assert signal is None
