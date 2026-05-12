from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from apps.api.state import AppState
from packages.core.models import (
    OrderRequest,
    OrderSide,
    SetupSignal,
    SignalDirection,
    SignalStatus,
)
from packages.db.orm_models import SignalORM
from packages.db.session import get_session
from packages.messaging.bus import Event, EventType
from runtime.risk_engine.config import get_risk_settings

router = APIRouter(prefix="/signals", tags=["signals"])
logger = structlog.get_logger(__name__)


@router.get("")
async def get_signals(
    symbol: Optional[str] = Query(None),
    status: Optional[SignalStatus] = Query(None),
    limit: int = Query(50, ge=1, le=500),
) -> list[dict]:
    async with get_session() as db:
        stmt = (
            select(SignalORM)
            .order_by(SignalORM.timestamp.desc())
            .limit(limit)
        )
        if symbol:
            stmt = stmt.where(SignalORM.symbol == symbol.upper())
        if status:
            stmt = stmt.where(SignalORM.status == status)

        result = await db.execute(stmt)
        rows = result.scalars().all()

    return [_signal_dict(r) for r in rows]


@router.post("/{signal_id}/approve")
async def approve_signal(signal_id: str) -> dict:
    risk_settings = get_risk_settings()

    async with get_session() as db:
        result = await db.execute(select(SignalORM).where(SignalORM.id == signal_id))
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="Signal not found")

        if row.status != SignalStatus.PENDING_APPROVAL:
            raise HTTPException(
                status_code=400,
                detail=f"Signal is not pending approval (current status: {row.status})",
            )

        now = datetime.now(tz=timezone.utc)
        expiry_threshold = now - timedelta(minutes=risk_settings.signal_expiry_minutes)
        signal_ts = row.timestamp
        if signal_ts.tzinfo is None:
            signal_ts = signal_ts.replace(tzinfo=timezone.utc)
        if signal_ts < expiry_threshold:
            row.status = SignalStatus.EXPIRED
            raise HTTPException(status_code=422, detail="Signal has expired")

        row.status = SignalStatus.APPROVED
        entry_price: float = row.entry_price
        stop_price: float = row.stop_price
        symbol: str = row.symbol
        strategy_id: str = row.strategy_id
        direction: str = row.direction
        signal_data = _signal_dict(row)

    state = AppState.get()
    if state.risk_engine is None or state.bus is None:
        raise HTTPException(status_code=503, detail="Runtime not ready")

    quantity = state.risk_engine.size_position(entry_price, stop_price)
    state.risk_engine.mark_order_open(strategy_id, symbol)

    signal = _reconstruct_signal(signal_data)

    order = OrderRequest(
        signal_id=signal_id,
        symbol=symbol,
        side=OrderSide.BUY if direction.lower() == "long" else OrderSide.SELL,
        quantity=quantity,
        limit_price=entry_price,
        stop_price=stop_price,
        timestamp=now,
        paper=risk_settings.paper_mode,
    )

    await state.bus.publish(
        Event(type=EventType.SIGNAL_APPROVED, payload=signal, source="api")
    )
    await state.bus.publish(
        Event(type=EventType.ORDER_REQUESTED, payload=order, source="api")
    )

    logger.info(
        "signals.approved",
        signal_id=signal_id,
        symbol=symbol,
        quantity=quantity,
        entry=entry_price,
    )
    return signal_data


@router.post("/{signal_id}/reject")
async def reject_signal(signal_id: str) -> dict:
    async with get_session() as db:
        result = await db.execute(select(SignalORM).where(SignalORM.id == signal_id))
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="Signal not found")

        row.status = SignalStatus.REJECTED
        signal_data = _signal_dict(row)

    state = AppState.get()
    if state.bus is not None:
        signal = _reconstruct_signal(signal_data)
        await state.bus.publish(
            Event(type=EventType.SIGNAL_REJECTED, payload=signal, source="api")
        )

    logger.info("signals.rejected", signal_id=signal_id)
    return signal_data


def _signal_dict(row: SignalORM) -> dict:
    return {
        "id": row.id,
        "strategy_id": row.strategy_id,
        "symbol": row.symbol,
        "direction": row.direction,
        "status": row.status,
        "timestamp": row.timestamp.isoformat(),
        "entry_price": row.entry_price,
        "stop_price": row.stop_price,
        "target_price": row.target_price,
        "risk_reward": row.risk_reward,
        "confidence": row.confidence,
        "notes": row.notes,
    }


def _reconstruct_signal(data: dict) -> SetupSignal:
    """Reconstruct a SetupSignal domain object from serialised signal data."""
    ts = data["timestamp"]
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return SetupSignal(
        id=data["id"],
        strategy_id=data["strategy_id"],
        symbol=data["symbol"],
        direction=SignalDirection(data["direction"]),
        status=SignalStatus(data["status"]),
        timestamp=ts,
        entry_price=data["entry_price"],
        stop_price=data["stop_price"],
        target_price=data["target_price"],
        risk_reward=data["risk_reward"],
        confidence=data["confidence"],
        notes=data.get("notes", ""),
    )
