"""
EventRecorder — subscribes to all bus events and persists domain objects
to the database for later replay.

This is a first-class citizen of the architecture: every candle, signal,
order, and fill is persisted automatically without coupling any other engine
to the database layer.
"""

from __future__ import annotations

import structlog

from packages.core.models import (
    Candle,
    Fill,
    IndicatorSnapshot,
    MarketContext,
    OrderResult,
    SetupSignal,
    candle_stable_id,
)
from packages.db.session import get_session
from packages.db.orm_models import (
    CandleORM,
    FillORM,
    IndicatorSnapshotORM,
    MarketContextORM,
    OrderORM,
    SignalORM,
)
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)


class EventRecorder:
    """
    Wildcard event subscriber that persists domain events to PostgreSQL.

    Uses async SQLAlchemy sessions; each persist is wrapped in its own
    short-lived session to avoid long-running transactions.
    """

    def __init__(self, bus: EventBus) -> None:
        bus.subscribe(EventType.CANDLE_FINAL, self._on_candle)
        bus.subscribe(EventType.INDICATORS_UPDATED, self._on_indicators)
        bus.subscribe(EventType.CONTEXT_UPDATED, self._on_context)
        bus.subscribe(EventType.SIGNAL_DETECTED, self._on_signal)
        bus.subscribe(EventType.ORDER_SUBMITTED, self._on_order)
        bus.subscribe(EventType.ORDER_FILLED, self._on_fill)
        bus.subscribe(EventType.ORDER_RESULT_UPDATED, self._on_order_result)
        bus.subscribe(EventType.ORDER_CANCELLED, self._on_order_result)

    async def _on_candle(self, event: Event) -> None:
        candle: Candle = event.payload
        # Use deterministic ID so re-loading the same bar is an upsert, not duplicate
        stable = candle_stable_id(candle.symbol, candle.timeframe, candle.timestamp)
        orm = CandleORM(
            id=stable,
            symbol=candle.symbol,
            timeframe=candle.timeframe.value,
            timestamp=candle.timestamp,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            vwap=candle.vwap,
            cumulative_volume=candle.cumulative_volume,
            cumulative_turnover=candle.cumulative_turnover,
            session=candle.session.value,
            is_partial=candle.is_partial,
        )
        await self._upsert(orm, "candle", stable)

    async def _on_indicators(self, event: Event) -> None:
        snap: IndicatorSnapshot = event.payload
        orm = IndicatorSnapshotORM(
            id=snap.id,
            symbol=snap.symbol,
            timeframe=snap.timeframe.value,
            timestamp=snap.timestamp,
            vwap=snap.vwap,
            vwap_distance_pct=snap.vwap_distance_pct,
            opening_range_high=snap.opening_range_high,
            opening_range_low=snap.opening_range_low,
            opening_range_mid=snap.opening_range_mid,
            rolling_volume_avg=snap.rolling_volume_avg,
            relative_volume=snap.relative_volume,
            prior_day_high=snap.prior_day_high,
            prior_day_low=snap.prior_day_low,
            premarket_high=snap.premarket_high,
            premarket_low=snap.premarket_low,
            ema_9=snap.ema_9,
            ema_21=snap.ema_21,
        )
        await self._upsert(orm, "indicator", snap.id)

    async def _on_context(self, event: Event) -> None:
        ctx: MarketContext = event.payload
        orm = MarketContextORM(
            id=ctx.id,
            symbol=ctx.symbol,
            timestamp=ctx.timestamp,
            above_vwap=ctx.above_vwap,
            vwap_cross_up=ctx.vwap_cross_up,
            vwap_cross_down=ctx.vwap_cross_down,
            vwap_distance_pct=ctx.vwap_distance_pct,
            orb_breakout_up=ctx.orb_breakout_up,
            orb_breakout_down=ctx.orb_breakout_down,
            orb_hold=ctx.orb_hold,
            higher_highs=ctx.higher_highs,
            lower_lows=ctx.lower_lows,
            opening_drive_up=ctx.opening_drive_up,
            opening_drive_down=ctx.opening_drive_down,
            session=ctx.session.value,
        )
        await self._upsert(orm, "context", ctx.id)

    async def _on_signal(self, event: Event) -> None:
        signal: SetupSignal = event.payload
        orm = SignalORM(
            id=signal.id,
            strategy_id=signal.strategy_id,
            symbol=signal.symbol,
            direction=signal.direction,
            status=signal.status,
            timestamp=signal.timestamp,
            candle_ref=signal.candle_ref,
            entry_price=signal.entry_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            risk_reward=signal.risk_reward,
            confidence=signal.confidence,
            notes=signal.notes,
        )
        await self._upsert(orm, "signal", signal.id)

    async def _on_order(self, event: Event) -> None:
        result: OrderResult = event.payload
        if not isinstance(result, OrderResult):
            return
        orm = OrderORM(
            id=result.id,
            order_id=result.order_id,
            signal_id=result.signal_id,
            symbol=result.symbol,
            side=result.side,
            requested_quantity=result.requested_quantity,
            filled_quantity=result.filled_quantity,
            avg_fill_price=result.avg_fill_price,
            status=result.status,
            submitted_at=result.submitted_at,
            updated_at=result.updated_at,
            paper=result.paper,
            error_message=result.error_message,
        )
        await self._upsert(orm, "order", result.id)

    async def _on_fill(self, event: Event) -> None:
        fill: Fill = event.payload
        orm = FillORM(
            id=fill.id,
            order_id=fill.order_id,
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            timestamp=fill.timestamp,
            paper=fill.paper,
        )
        await self._upsert(orm, "fill", fill.id)

    async def _on_order_result(self, event: Event) -> None:
        result: OrderResult = event.payload
        await self._on_order(event)

    async def _upsert(self, orm_obj: object, entity: str, entity_id: str) -> None:
        try:
            async with get_session() as session:
                await session.merge(orm_obj)
        except Exception:
            logger.exception(
                "recorder.persist_error", entity=entity, entity_id=entity_id
            )
