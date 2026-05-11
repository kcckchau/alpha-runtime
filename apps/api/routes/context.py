from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from packages.db.orm_models import MarketContextORM
from packages.db.session import get_session

router = APIRouter(prefix="/context", tags=["context"])


@router.get("/{symbol}")
async def get_context(symbol: str) -> dict:
    """Return the most recent market context for a symbol."""
    async with get_session() as db:
        result = await db.execute(
            select(MarketContextORM)
            .where(MarketContextORM.symbol == symbol.upper())
            .order_by(MarketContextORM.timestamp.desc())
            .limit(1)
        )
        row = result.scalar_one_or_none()

    if row is None:
        raise HTTPException(status_code=404, detail="No context found for symbol")

    return {
        "id": row.id,
        "symbol": row.symbol,
        "timestamp": row.timestamp.isoformat(),
        "above_vwap": row.above_vwap,
        "vwap_cross_up": row.vwap_cross_up,
        "vwap_cross_down": row.vwap_cross_down,
        "vwap_distance_pct": row.vwap_distance_pct,
        "orb_breakout_up": row.orb_breakout_up,
        "orb_breakout_down": row.orb_breakout_down,
        "orb_hold": row.orb_hold,
        "higher_highs": row.higher_highs,
        "lower_lows": row.lower_lows,
        "opening_drive_up": row.opening_drive_up,
        "opening_drive_down": row.opening_drive_down,
        "session": row.session,
    }
