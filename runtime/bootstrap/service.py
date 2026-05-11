"""
Bootstrap service — historical backfill orchestrator.

On startup, before live streaming begins, this service:
  1. Fetches historical 1-minute bars from IBKR for each configured symbol.
  2. Publishes each bar as a CANDLE_FINAL event (historical=True) so the
     indicator and context engines warm up their state naturally.
  3. Seeds the CandleEngine's per-session VWAP accumulators so live
     streaming continues from the correct cumulative state.
  4. Persists all historical candles via the EventRecorder (same bus path).
  5. Emits BOOTSTRAP_COMPLETE when done.

Strategy signals from historical events are suppressed in the strategy
engine when suppress_historical=True (the default).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import structlog

from packages.core.models import Candle, SessionType
from packages.messaging.bus import Event, EventBus, EventType
from packages.core.models import Timeframe
from runtime.bootstrap.config import BootstrapSettings
from runtime.bootstrap.historical_loader import aggregate_candles, fetch_symbol_history

if TYPE_CHECKING:
    from ib_insync import IB
    from runtime.candle_engine import CandleEngine

logger = structlog.get_logger(__name__)


@dataclass
class BootstrapStatus:
    is_running: bool = False
    is_complete: bool = False
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    symbols: list[str] = field(default_factory=list)
    bars_loaded: dict[str, int] = field(default_factory=dict)  # symbol → count
    error: Optional[str] = None


class BootstrapService:
    """
    Orchestrates historical backfill for all registered symbols.

    After run() completes the candle / indicator / context engines have
    warmed-up state and the CandleEngine VWAP accumulators are seeded.
    """

    def __init__(
        self,
        bus: EventBus,
        candle_engine: "CandleEngine",
        settings: Optional[BootstrapSettings] = None,
    ) -> None:
        self._bus = bus
        self._candle_engine = candle_engine
        self._settings = settings or BootstrapSettings()
        self.status = BootstrapStatus()

    def register_symbol(self, symbol: str) -> None:
        if symbol not in self.status.symbols:
            self.status.symbols.append(symbol)

    async def run(self, ib: "IB") -> None:
        """
        Execute backfill for all registered symbols.

        Designed to be awaited once from main.py startup before live
        streaming is activated.  Safe to call even if IBKR is disconnected —
        it will log a warning and return immediately so the runtime still
        starts, just without historical state.
        """
        if not self._settings.enabled:
            logger.info("bootstrap.disabled")
            return

        if not ib.isConnected():
            logger.warning(
                "bootstrap.ibkr_not_connected",
                note="Skipping historical backfill; live data will start from zero.",
            )
            return

        self.status.is_running = True
        self.status.started_at = datetime.now(tz=timezone.utc)
        self.status.is_complete = False
        self.status.error = None

        await self._bus.publish(
            Event(
                type=EventType.BOOTSTRAP_STARTED,
                payload={"symbols": self.status.symbols},
                source="bootstrap",
            )
        )

        logger.info(
            "bootstrap.started",
            symbols=self.status.symbols,
            lookback_days=self._settings.lookback_days,
        )

        try:
            for symbol in self.status.symbols:
                await self._backfill_symbol(ib, symbol)
        except Exception as exc:
            self.status.error = str(exc)
            logger.exception("bootstrap.error")
        finally:
            self.status.is_running = False
            self.status.is_complete = True
            self.status.completed_at = datetime.now(tz=timezone.utc)

        total = sum(self.status.bars_loaded.values())
        logger.info(
            "bootstrap.complete",
            total_bars=total,
            bars_by_symbol=self.status.bars_loaded,
        )

        await self._bus.publish(
            Event(
                type=EventType.BOOTSTRAP_COMPLETE,
                payload={
                    "bars_loaded": dict(self.status.bars_loaded),
                    "completed_at": self.status.completed_at.isoformat(),
                },
                source="bootstrap",
            )
        )

    async def _backfill_symbol(self, ib: "IB", symbol: str) -> None:
        from ib_insync import Stock

        contract = Stock(symbol, "SMART", "USD")
        await ib.qualifyContractsAsync(contract)

        candles = await fetch_symbol_history(
            ib=ib,
            contract=contract,
            symbol=symbol,
            lookback_days=self._settings.lookback_days,
            bar_size=self._settings.bar_size,
            what_to_show=self._settings.what_to_show,
            premarket_start_hour=self._settings.premarket_start_hour,
        )

        if not candles:
            logger.warning("bootstrap.no_bars", symbol=symbol)
            self.status.bars_loaded[symbol] = 0
            return

        # Build higher-timeframe candles from the 1m set so the DB is fully
        # populated from the first boot (no waiting for live aggregation).
        candles_5m  = aggregate_candles(candles, Timeframe.MIN_5)
        candles_1h  = aggregate_candles(candles, Timeframe.HOUR_1)

        total = len(candles) + len(candles_5m) + len(candles_1h)
        self.status.bars_loaded[symbol] = total

        # Publish 1m candles first so indicator/context engines warm up in order.
        for candle in candles:
            await self._bus.publish(
                Event(type=EventType.CANDLE_FINAL, payload=candle, source="bootstrap")
            )

        # Publish 5m and 1h candles; indicator engine ignores non-1m bars.
        for candle in candles_5m:
            await self._bus.publish(
                Event(type=EventType.CANDLE_FINAL, payload=candle, source="bootstrap")
            )
        for candle in candles_1h:
            await self._bus.publish(
                Event(type=EventType.CANDLE_FINAL, payload=candle, source="bootstrap")
            )

        # Seed CandleEngine VWAP accumulators from the last today-session bar
        # so live streaming picks up cumulative VWAP without a reset.
        self._seed_vwap(symbol, candles)  # 1m candles carry the session state

        logger.info(
            "bootstrap.symbol_complete",
            symbol=symbol,
            bars_1m=len(candles),
            bars_5m=len(candles_5m),
            bars_1h=len(candles_1h),
        )

    def _seed_vwap(self, symbol: str, candles: list[Candle]) -> None:
        """
        Find the most recent premarket or RTH bar and seed the CandleEngine
        VWAP accumulators so the live stream continues the session VWAP.
        """
        # Walk backwards to find the last bar with a meaningful session
        for candle in reversed(candles):
            if candle.session in (SessionType.PREMARKET, SessionType.RTH):
                self._candle_engine.seed_vwap_state(
                    symbol=symbol,
                    cum_volume=candle.cumulative_volume,
                    cum_turnover=candle.cumulative_turnover,
                    session=candle.session,
                )
                logger.info(
                    "bootstrap.vwap_seeded",
                    symbol=symbol,
                    session=candle.session,
                    cum_volume=candle.cumulative_volume,
                    vwap=candle.vwap,
                    seed_bar_ts=candle.timestamp.isoformat(),
                )
                return

        logger.warning("bootstrap.no_seedable_bar", symbol=symbol)
