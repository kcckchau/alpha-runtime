from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import select

from apps.api.trading_day_bounds import (
    extended_session_utc_bounds,
    parse_trade_date,
    today_trade_date_et,
)
from packages.core.models import SessionType, Timeframe
from packages.db.orm_models import CandleORM
from packages.db.session import get_session

router = APIRouter(prefix="/candles", tags=["candles"])


@router.get("/{symbol}")
async def get_candles(
    symbol: str,
    timeframe: Timeframe = Query(Timeframe.MIN_1),
    date: Optional[str] = Query(
        None,
        description="ET calendar day YYYY-MM-DD; defaults to today (New York). "
        "Returns finalized bars from 04:00 ET through before 20:00 ET that day.",
    ),
    session: Optional[SessionType] = Query(None),
    limit: int = Query(2000, ge=1, le=5000),
) -> list[dict]:
    try:
        trade_d = parse_trade_date(date) if date else today_trade_date_et()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    start_utc, end_utc = extended_session_utc_bounds(trade_d)

    async with get_session() as db:
        stmt = (
            select(CandleORM)
            .where(CandleORM.symbol == symbol.upper())
            .where(CandleORM.timeframe == timeframe.value)
            .where(CandleORM.is_partial.is_(False))
            .where(CandleORM.timestamp >= start_utc)
            .where(CandleORM.timestamp < end_utc)
            .order_by(CandleORM.timestamp.asc())
            .limit(limit)
        )
        if session:
            stmt = stmt.where(CandleORM.session == session.value)

        result = await db.execute(stmt)
        rows = result.scalars().all()

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
        for r in rows
    ]
