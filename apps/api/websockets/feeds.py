"""
WebSocket live feeds.

/ws/candles  — streams CANDLE_FINAL events (all symbols)
/ws/signals  — streams SIGNAL_DETECTED events
/ws/fills    — streams ORDER_FILLED events

Each feed maintains a set of connected clients. The event bus handler
broadcasts to all connected clients; disconnected clients are pruned.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from packages.messaging.bus import Event, EventType, get_bus

router = APIRouter(prefix="/ws", tags=["websocket"])
logger = structlog.get_logger(__name__)


class _Broadcaster:
    """Fan-out messages to all connected WebSocket clients."""

    def __init__(self) -> None:
        self._clients: set[WebSocket] = set()

    def add(self, ws: WebSocket) -> None:
        self._clients.add(ws)

    def remove(self, ws: WebSocket) -> None:
        self._clients.discard(ws)

    async def broadcast(self, payload: dict[str, Any]) -> None:
        text = json.dumps(payload, default=str)
        dead: list[WebSocket] = []
        for client in list(self._clients):
            try:
                await client.send_text(text)
            except Exception:
                dead.append(client)
        for ws in dead:
            self._clients.discard(ws)


_candle_bcast = _Broadcaster()
_signal_bcast = _Broadcaster()
_fill_bcast = _Broadcaster()


async def _setup_bus_handlers() -> None:
    bus = get_bus()

    async def on_candle(event: Event) -> None:
        c = event.payload
        await _candle_bcast.broadcast(
            {
                "type": "candle",
                "symbol": c.symbol,
                "timeframe": c.timeframe,
                "timestamp": c.timestamp.isoformat(),
                "open": c.open,
                "high": c.high,
                "low": c.low,
                "close": c.close,
                "volume": c.volume,
                "vwap": c.vwap,
                "session": c.session,
                "is_partial": c.is_partial,
            }
        )

    async def on_signal(event: Event) -> None:
        s = event.payload
        await _signal_bcast.broadcast(
            {
                "type": "signal",
                "id": s.id,
                "strategy_id": s.strategy_id,
                "symbol": s.symbol,
                "direction": s.direction,
                "status": s.status,
                "timestamp": s.timestamp.isoformat(),
                "entry_price": s.entry_price,
                "stop_price": s.stop_price,
                "target_price": s.target_price,
                "risk_reward": s.risk_reward,
                "confidence": s.confidence,
                "notes": s.notes,
            }
        )

    async def on_fill(event: Event) -> None:
        from packages.core.models import Fill
        payload = event.payload
        if not isinstance(payload, Fill):
            return
        await _fill_bcast.broadcast(
            {
                "type": "fill",
                "id": payload.id,
                "order_id": payload.order_id,
                "symbol": payload.symbol,
                "side": payload.side,
                "quantity": payload.quantity,
                "price": payload.price,
                "timestamp": payload.timestamp.isoformat(),
                "paper": payload.paper,
            }
        )

    bus.subscribe(EventType.CANDLE_FINAL, on_candle)
    bus.subscribe(EventType.CANDLE_PARTIAL, on_candle)
    bus.subscribe(EventType.SIGNAL_DETECTED, on_signal)
    bus.subscribe(EventType.ORDER_FILLED, on_fill)


# Register handlers once on first import via a startup hook
_handlers_registered = False


async def _ensure_handlers() -> None:
    global _handlers_registered
    if not _handlers_registered:
        await _setup_bus_handlers()
        _handlers_registered = True


@router.websocket("/candles")
async def ws_candles(ws: WebSocket) -> None:
    await _ensure_handlers()
    await ws.accept()
    _candle_bcast.add(ws)
    logger.info("ws.candle_connected")
    try:
        while True:
            await ws.receive_text()  # keep-alive; client may send pings
    except WebSocketDisconnect:
        pass
    finally:
        _candle_bcast.remove(ws)
        logger.info("ws.candle_disconnected")


@router.websocket("/signals")
async def ws_signals(ws: WebSocket) -> None:
    await _ensure_handlers()
    await ws.accept()
    _signal_bcast.add(ws)
    logger.info("ws.signal_connected")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _signal_bcast.remove(ws)
        logger.info("ws.signal_disconnected")


@router.websocket("/fills")
async def ws_fills(ws: WebSocket) -> None:
    await _ensure_handlers()
    await ws.accept()
    _fill_bcast.add(ws)
    logger.info("ws.fill_connected")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _fill_bcast.remove(ws)
        logger.info("ws.fill_disconnected")
