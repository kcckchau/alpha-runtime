from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from packages.core.models import SignalStatus
from packages.db.orm_models import SignalORM
from packages.db.session import get_session

router = APIRouter(prefix="/signals", tags=["signals"])


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
    return await _update_signal_status(signal_id, SignalStatus.APPROVED)


@router.post("/{signal_id}/reject")
async def reject_signal(signal_id: str) -> dict:
    return await _update_signal_status(signal_id, SignalStatus.REJECTED)


async def _update_signal_status(signal_id: str, new_status: SignalStatus) -> dict:
    async with get_session() as db:
        result = await db.execute(
            select(SignalORM).where(SignalORM.id == signal_id)
        )
        row = result.scalar_one_or_none()
        if row is None:
            raise HTTPException(status_code=404, detail="Signal not found")
        row.status = new_status
        return _signal_dict(row)


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
