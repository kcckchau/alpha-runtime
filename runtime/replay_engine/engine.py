"""
Replay engine.

Loads a recorded session from the database and re-publishes its events
through the event bus in order, with optional speed multiplier.

This lets you:
- Re-run strategies against historical data.
- Audit exactly what the system saw and decided.
- Debug setups without connecting to a live broker.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

import structlog
from sqlalchemy import select

from packages.core.models import Candle, SessionType, Timeframe
from packages.db.orm_models import CandleORM
from packages.db.session import get_session
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)


class ReplayEngine:
    """
    Replays a session by loading candles from the DB and publishing them
    through the event bus as if they were live.

    Upstream engines (indicator, context, strategy) receive the same events
    they would receive in production.
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        self._running = False

    async def replay(
        self,
        symbol: str,
        date_str: str,
        timeframe: Timeframe = Timeframe.MIN_1,
        speed: float = 0.0,
        session_type: Optional[SessionType] = None,
    ) -> None:
        """
        Replay candles for *symbol* on *date_str* (YYYY-MM-DD).

        Args:
            speed: seconds to sleep between candles. 0 = as fast as possible.
            session_type: filter to a specific session. None = all sessions.
        """
        self._running = True
        candles = await self._load_candles(symbol, date_str, timeframe, session_type)

        logger.info(
            "replay_engine.start",
            symbol=symbol,
            date=date_str,
            count=len(candles),
            speed=speed,
        )

        for candle in candles:
            if not self._running:
                logger.info("replay_engine.stopped")
                break

            await self._bus.publish(
                Event(
                    type=EventType.CANDLE_FINAL,
                    payload=candle,
                    source="replay_engine",
                )
            )

            if speed > 0:
                await asyncio.sleep(speed)

        self._running = False
        logger.info("replay_engine.complete", symbol=symbol, date=date_str)

    def stop(self) -> None:
        self._running = False

    async def _load_candles(
        self,
        symbol: str,
        date_str: str,
        timeframe: Timeframe,
        session_type: Optional[SessionType],
    ) -> list[Candle]:
        async with get_session() as session:
            stmt = (
                select(CandleORM)
                .where(CandleORM.symbol == symbol)
                .where(CandleORM.timeframe == timeframe)
                .where(CandleORM.is_partial.is_(False))
                .order_by(CandleORM.timestamp)
            )
            if session_type:
                stmt = stmt.where(CandleORM.session == session_type)

            result = await session.execute(stmt)
            rows = result.scalars().all()

        # Filter by date (stored as UTC; compare date portion)
        candles: list[Candle] = []
        for row in rows:
            ts = row.timestamp
            if ts.strftime("%Y-%m-%d") != date_str:
                continue
            candles.append(
                Candle(
                    id=row.id,
                    symbol=row.symbol,
                    timeframe=Timeframe(row.timeframe),
                    timestamp=row.timestamp,
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    volume=row.volume,
                    vwap=row.vwap,
                    cumulative_volume=row.cumulative_volume,
                    cumulative_turnover=row.cumulative_turnover,
                    session=SessionType(row.session),
                    is_partial=row.is_partial,
                )
            )
        return candles
