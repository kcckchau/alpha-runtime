from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from packages.core.models import Candle, SessionType, Timeframe
from packages.db.orm_models import CandleORM
from packages.db.session import get_session

router = APIRouter(prefix="/candles", tags=["candles"])


@router.get("/{symbol}")
async def get_candles(
    symbol: str,
    timeframe: Timeframe = Query(Timeframe.MIN_1),
    date: Optional[str] = Query(None, description="YYYY-MM-DD, defaults to today"),
    session: Optional[SessionType] = Query(None),
    limit: int = Query(200, ge=1, le=2000),
) -> list[dict]:
    async with get_session() as db:
        stmt = (
            select(CandleORM)
            .where(CandleORM.symbol == symbol.upper())
            .where(CandleORM.timeframe == timeframe)
            .where(CandleORM.is_partial.is_(False))
            .order_by(CandleORM.timestamp.desc())
            .limit(limit)
        )
        if session:
            stmt = stmt.where(CandleORM.session == session)

        result = await db.execute(stmt)
        rows = result.scalars().all()

    if not rows:
        return []

    return [
        {
            "id": r.id,
            "symbol": r.symbol,
            "timeframe": r.timeframe,
            "timestamp": r.timestamp.isoformat(),
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
            "vwap": r.vwap,
            "session": r.session,
        }
        for r in reversed(rows)
    ]
