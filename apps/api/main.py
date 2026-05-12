"""
alpha-runtime API entry point.

Wires the full pipeline on startup:
  market_data → candle_engine → indicator_engine → context_engine
              → quote_engine → orderbook_engine → microstructure_engine
  → strategy_engine → risk_engine → execution_engine → position_engine

Serves REST endpoints and WebSocket feeds.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

import structlog
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from apps.api.config import get_api_settings
from apps.api.logging import configure_logging
from apps.api.routes import candles, chart, context, health, signals
from apps.api.routes import bootstrap as bootstrap_route
from apps.api.routes import positions as positions_route
from apps.api.websockets.feeds import router as ws_router
from apps.api.state import AppState
from packages.core.models import OrderBookSnapshot, QuoteSnapshot, SignalStatus, Tick as TickModel
from packages.db.orm_models import SignalORM
from packages.db.session import get_session, init_db
from packages.messaging.bus import Event, EventType, get_bus
from runtime.bootstrap import BootstrapService
from runtime.bootstrap.config import BootstrapSettings
from runtime.candle_engine import CandleEngine
from runtime.context_engine import ContextEngine
from runtime.execution_engine import ExecutionEngine
from runtime.indicator_engine import IndicatorEngine
from runtime.market_data.ibkr import IBKRAdapter
from runtime.microstructure_engine import MicrostructureEngine
from runtime.orderbook_engine import OrderBookEngine
from runtime.position_engine import PositionEngine
from runtime.quote_engine import QuoteEngine
from runtime.replay_engine import EventRecorder
from runtime.risk_engine import RiskEngine
from runtime.risk_engine.config import get_risk_settings
from runtime.strategy_engine import StrategyEngine
from runtime.strategy_engine.strategies import VWAPReclaimLong

configure_logging()
logger = structlog.get_logger(__name__)
settings = get_api_settings()


async def _signal_expiry_loop(bus, risk_settings) -> None:
    """Background task: expire PENDING_APPROVAL signals older than signal_expiry_minutes."""
    from sqlalchemy import select as sa_select, update as sa_update
    while True:
        await asyncio.sleep(60)
        try:
            threshold = datetime.now(tz=timezone.utc) - timedelta(
                minutes=risk_settings.signal_expiry_minutes
            )
            async with get_session() as db:
                stmt = sa_select(SignalORM).where(
                    SignalORM.status == SignalStatus.PENDING_APPROVAL,
                    SignalORM.timestamp < threshold,
                )
                result = await db.execute(stmt)
                rows = result.scalars().all()
                for row in rows:
                    row.status = SignalStatus.EXPIRED
                    await bus.publish(
                        Event(
                            type=EventType.SIGNAL_EXPIRED,
                            payload=row.id,
                            source="api",
                        )
                    )
                    logger.info("signal_expiry.expired", signal_id=row.id)
                if rows:
                    logger.info("signal_expiry.batch", count=len(rows))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("signal_expiry.error")


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
    position_engine = PositionEngine(bus)
    _recorder = EventRecorder(bus)  # noqa: F841 — subscribes itself

    # ── Microstructure pipeline ──────────────────────────────────────
    # QuoteEngine and OrderBookEngine are thin bridge layers that put
    # adapter callbacks onto the bus.  MicrostructureEngine subscribes
    # to those bus events and computes derived metrics.
    # All three degrade gracefully when L1/L2 data is unavailable.
    quote_engine = QuoteEngine(bus)
    orderbook_engine = OrderBookEngine(bus)
    microstructure_engine = MicrostructureEngine(bus)

    bootstrap_svc = BootstrapService(bus, candle_engine, bootstrap_settings)

    state.candle_engine = candle_engine
    state.indicator_engine = indicator_engine
    state.context_engine = context_engine
    state.strategy_engine = strategy_engine
    state.risk_engine = risk_engine
    state.execution_engine = execution_engine
    state.position_engine = position_engine
    state.bus = bus
    state.bootstrap_status = bootstrap_svc.status

    for symbol in settings.symbols:
        candle_engine.register_symbol(symbol)
        indicator_engine.register_symbol(symbol)
        context_engine.register_symbol(symbol)
        quote_engine.register_symbol(symbol)
        orderbook_engine.register_symbol(symbol)
        microstructure_engine.register_symbol(symbol)
        position_engine.register_symbol(symbol)
        strategy_engine.register(VWAPReclaimLong(symbol=symbol))
        bootstrap_svc.register_symbol(symbol)

    adapter = IBKRAdapter()

    async def on_tick(tick: TickModel) -> None:
        await bus.publish(
            Event(type=EventType.TICK_RECEIVED, payload=tick, source="market_data")
        )
        await candle_engine.on_tick(tick)

    async def on_quote(quote: QuoteSnapshot) -> None:
        await quote_engine.on_quote(quote)

    async def on_depth(snap: OrderBookSnapshot) -> None:
        await orderbook_engine.on_depth_update(snap)

    adapter.on_tick(on_tick)
    adapter.on_quote(on_quote)
    adapter.on_depth_update(on_depth)
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

    # ---- background: expire stale pending-approval signals ----
    _expiry_task = asyncio.create_task(_signal_expiry_loop(bus, get_risk_settings()))

    yield  # ---- running ----

    # ---- shutdown ----
    _expiry_task.cancel()
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
app.include_router(positions_route.router)
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
