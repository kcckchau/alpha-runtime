"""
alpha-runtime API entry point.

Wires the full pipeline on startup:
  market_data → candle_engine → indicator_engine → context_engine
  → strategy_engine → risk_engine → execution_engine

Serves REST endpoints and WebSocket feeds.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apps.api.config import get_api_settings
from apps.api.logging import configure_logging
from apps.api.routes import candles, chart, context, health, signals
from apps.api.routes import bootstrap as bootstrap_route
from apps.api.websockets.feeds import router as ws_router
from apps.api.state import AppState
from packages.core.models import Tick as TickModel
from packages.db.session import init_db
from packages.messaging.bus import Event, EventType, get_bus
from runtime.bootstrap import BootstrapService
from runtime.bootstrap.config import BootstrapSettings
from runtime.candle_engine import CandleEngine
from runtime.context_engine import ContextEngine
from runtime.execution_engine import ExecutionEngine
from runtime.indicator_engine import IndicatorEngine
from runtime.market_data.ibkr import IBKRAdapter
from runtime.replay_engine import EventRecorder
from runtime.risk_engine import RiskEngine
from runtime.strategy_engine import StrategyEngine
from runtime.strategy_engine.strategies import VWAPReclaimLong

configure_logging()
logger = structlog.get_logger(__name__)
settings = get_api_settings()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # ---- startup ----
    logger.info("api.startup", symbols=settings.symbols)

    await init_db()

    bus = get_bus()
    state = AppState.get()

    bootstrap_settings = BootstrapSettings()

    candle_engine = CandleEngine(bus)
    indicator_engine = IndicatorEngine(bus)
    context_engine = ContextEngine(bus)
    strategy_engine = StrategyEngine(
        bus,
        suppress_historical=bootstrap_settings.suppress_historical_signals,
    )
    risk_engine = RiskEngine(bus)
    execution_engine = ExecutionEngine(bus, paper=True)
    _recorder = EventRecorder(bus)  # noqa: F841 — subscribes itself

    bootstrap_svc = BootstrapService(bus, candle_engine, bootstrap_settings)

    state.candle_engine = candle_engine
    state.indicator_engine = indicator_engine
    state.context_engine = context_engine
    state.strategy_engine = strategy_engine
    state.risk_engine = risk_engine
    state.execution_engine = execution_engine
    state.bus = bus
    state.bootstrap_status = bootstrap_svc.status

    for symbol in settings.symbols:
        candle_engine.register_symbol(symbol)
        indicator_engine.register_symbol(symbol)
        context_engine.register_symbol(symbol)
        strategy_engine.register(VWAPReclaimLong(symbol=symbol))
        bootstrap_svc.register_symbol(symbol)

    adapter = IBKRAdapter()

    async def on_tick(tick: TickModel) -> None:
        await bus.publish(
            Event(type=EventType.TICK_RECEIVED, payload=tick, source="market_data")
        )
        await candle_engine.on_tick(tick)

    adapter.on_tick(on_tick)
    state.adapter = adapter

    # connect() is non-blocking: tries once, schedules background retries on failure
    await adapter.connect()

    if adapter.is_connected:
        # ---- historical bootstrap (before live streaming) ----
        logger.info("api.bootstrap_starting")
        await bootstrap_svc.run(adapter._ib)  # type: ignore[attr-defined]
        logger.info("api.bootstrap_done", bars=state.bootstrap_status.bars_loaded)

        # ---- subscribe to live real-time bars ----
        for symbol in settings.symbols:
            await adapter.subscribe(symbol)
    else:
        logger.warning(
            "api.ibkr_unavailable",
            note="Live data disabled. Start TWS/Gateway to enable. Replay and REST still work.",
        )

    logger.info("api.ready")

    yield  # ---- running ----

    # ---- shutdown ----
    if state.adapter:
        await state.adapter.disconnect()
    logger.info("api.shutdown")


app = FastAPI(
    title="alpha-runtime",
    description="Real-time trading infrastructure platform",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(candles.router)
app.include_router(signals.router)
app.include_router(context.router)
app.include_router(chart.router)
app.include_router(bootstrap_route.router)
app.include_router(ws_router)


if __name__ == "__main__":
    uvicorn.run(
        "apps.api.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        reload=False,
    )
