"""
Position engine.

Subscribes to ORDER_FILLED and TICK_RECEIVED to maintain an in-memory
position book.  Detects stop and target hits on each tick and publishes
EXIT_TRIGGERED, which in turn submits a closing OrderRequest.

Signal cache:
    The engine caches SetupSignal objects received on SIGNAL_APPROVED so it
    can look up stop/target prices when a fill arrives.

Order cache:
    The engine caches OrderRequest objects received on ORDER_REQUESTED so it
    can correlate a Fill (which only has order_id) back to the originating
    signal.  Closing orders (SELL side) are also cached and used to
    transition position status to CLOSED when their fill arrives.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import structlog

from packages.core.models import (
    ExitReason,
    ExitTrigger,
    Fill,
    OrderRequest,
    OrderSide,
    Position,
    PositionStatus,
    SetupSignal,
    Tick,
)
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)


class PositionEngine:
    """
    Maintains in-memory positions derived from order fills.

    Lifecycle:
      SIGNAL_APPROVED  → cache SetupSignal (stop/target prices)
      ORDER_REQUESTED  → cache OrderRequest (signal_id link)
      ORDER_FILLED     → open position (BUY) or close position (SELL)
      TICK_RECEIVED    → update current price, check stop/target
      EXIT_TRIGGERED   → submit closing OrderRequest
    """

    def __init__(self, bus: EventBus) -> None:
        self._bus = bus
        # position_id → Position
        self._positions: dict[str, Position] = {}
        # symbol → list[position_id] for quick tick fan-out
        self._open_by_symbol: dict[str, list[str]] = defaultdict(list)
        # signal_id → SetupSignal
        self._signal_cache: dict[str, SetupSignal] = {}
        # order_id → OrderRequest
        self._order_cache: dict[str, OrderRequest] = {}

        bus.subscribe(EventType.SIGNAL_APPROVED, self._on_signal_approved)
        bus.subscribe(EventType.ORDER_REQUESTED, self._on_order_requested)
        bus.subscribe(EventType.ORDER_FILLED, self._on_fill)
        bus.subscribe(EventType.TICK_RECEIVED, self._on_tick)
        bus.subscribe(EventType.EXIT_TRIGGERED, self._on_exit_triggered)

    def register_symbol(self, symbol: str) -> None:
        """Satisfy the standard engine interface; no per-symbol init needed."""

    # ------------------------------------------------------------------
    # Public query API (used by REST routes)
    # ------------------------------------------------------------------

    def get_open_positions(self) -> list[Position]:
        return [p for p in self._positions.values() if p.status == PositionStatus.OPEN]

    def get_open_position_for_symbol(self, symbol: str) -> Optional[Position]:
        for pid in self._open_by_symbol.get(symbol.upper(), []):
            pos = self._positions.get(pid)
            if pos and pos.status == PositionStatus.OPEN:
                return pos
        return None

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_signal_approved(self, event: Event) -> None:
        signal: SetupSignal = event.payload
        self._signal_cache[signal.id] = signal
        logger.debug(
            "position_engine.signal_cached",
            signal_id=signal.id,
            symbol=signal.symbol,
        )

    async def _on_order_requested(self, event: Event) -> None:
        order: OrderRequest = event.payload
        self._order_cache[order.id] = order

    async def _on_fill(self, event: Event) -> None:
        fill: Fill = event.payload
        if not isinstance(fill, Fill):
            return

        order = self._order_cache.get(fill.order_id)
        if not order:
            logger.warning(
                "position_engine.fill_unmatched",
                order_id=fill.order_id,
                symbol=fill.symbol,
            )
            return

        if order.side == OrderSide.SELL:
            await self._handle_closing_fill(fill, order)
        else:
            await self._handle_opening_fill(fill, order)

    async def _handle_opening_fill(self, fill: Fill, order: OrderRequest) -> None:
        signal = self._signal_cache.get(order.signal_id)
        if not signal:
            logger.warning(
                "position_engine.signal_not_cached",
                signal_id=order.signal_id,
                order_id=order.id,
            )
            return

        position = Position(
            position_id=str(uuid.uuid4()),
            symbol=fill.symbol,
            strategy_id=signal.strategy_id,
            signal_id=signal.id,
            entry_price=fill.price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            quantity=fill.quantity,
            side=fill.side,
            opened_at=fill.timestamp,
            status=PositionStatus.OPEN,
        )

        self._positions[position.position_id] = position
        self._open_by_symbol[fill.symbol].append(position.position_id)

        await self._bus.publish(
            Event(
                type=EventType.POSITION_OPENED,
                payload=position,
                source="position_engine",
            )
        )
        logger.info(
            "position_engine.position_opened",
            position_id=position.position_id,
            symbol=position.symbol,
            entry=position.entry_price,
            stop=position.stop_price,
            target=position.target_price,
            qty=position.quantity,
        )

    async def _handle_closing_fill(self, fill: Fill, order: OrderRequest) -> None:
        for pos in self._positions.values():
            if pos.signal_id == order.signal_id and pos.status in (
                PositionStatus.STOPPED,
                PositionStatus.TARGETED,
            ):
                pos.status = PositionStatus.CLOSED
                self._open_by_symbol[pos.symbol] = [
                    pid for pid in self._open_by_symbol[pos.symbol]
                    if pid != pos.position_id
                ]
                await self._bus.publish(
                    Event(
                        type=EventType.POSITION_UPDATED,
                        payload=pos,
                        source="position_engine",
                    )
                )
                logger.info(
                    "position_engine.position_closed",
                    position_id=pos.position_id,
                    symbol=pos.symbol,
                    close_price=fill.price,
                )
                break

    async def _on_tick(self, event: Event) -> None:
        tick: Tick = event.payload
        pids = list(self._open_by_symbol.get(tick.symbol, []))
        if not pids:
            return

        for pid in pids:
            pos = self._positions.get(pid)
            if not pos or pos.status != PositionStatus.OPEN:
                continue

            # Update live price and unrealised PnL
            pos.current_price = tick.close
            if pos.side == OrderSide.BUY:
                pos.unrealized_pnl = (tick.close - pos.entry_price) * pos.quantity
            else:
                pos.unrealized_pnl = (pos.entry_price - tick.close) * pos.quantity

            exit_trigger: Optional[ExitTrigger] = None

            if pos.side == OrderSide.BUY:
                if tick.low <= pos.stop_price:
                    pos.status = PositionStatus.STOPPED
                    exit_trigger = ExitTrigger(
                        position_id=pos.position_id,
                        symbol=pos.symbol,
                        strategy_id=pos.strategy_id,
                        signal_id=pos.signal_id,
                        side=pos.side,
                        reason=ExitReason.STOP_HIT,
                        fill_price=pos.stop_price,
                        quantity=pos.quantity,
                        timestamp=tick.timestamp,
                    )
                elif tick.high >= pos.target_price:
                    pos.status = PositionStatus.TARGETED
                    exit_trigger = ExitTrigger(
                        position_id=pos.position_id,
                        symbol=pos.symbol,
                        strategy_id=pos.strategy_id,
                        signal_id=pos.signal_id,
                        side=pos.side,
                        reason=ExitReason.TARGET_HIT,
                        fill_price=pos.target_price,
                        quantity=pos.quantity,
                        timestamp=tick.timestamp,
                    )
            else:  # SHORT
                if tick.high >= pos.stop_price:
                    pos.status = PositionStatus.STOPPED
                    exit_trigger = ExitTrigger(
                        position_id=pos.position_id,
                        symbol=pos.symbol,
                        strategy_id=pos.strategy_id,
                        signal_id=pos.signal_id,
                        side=pos.side,
                        reason=ExitReason.STOP_HIT,
                        fill_price=pos.stop_price,
                        quantity=pos.quantity,
                        timestamp=tick.timestamp,
                    )
                elif tick.low <= pos.target_price:
                    pos.status = PositionStatus.TARGETED
                    exit_trigger = ExitTrigger(
                        position_id=pos.position_id,
                        symbol=pos.symbol,
                        strategy_id=pos.strategy_id,
                        signal_id=pos.signal_id,
                        side=pos.side,
                        reason=ExitReason.TARGET_HIT,
                        fill_price=pos.target_price,
                        quantity=pos.quantity,
                        timestamp=tick.timestamp,
                    )

            if exit_trigger:
                await self._bus.publish(
                    Event(
                        type=EventType.EXIT_TRIGGERED,
                        payload=exit_trigger,
                        source="position_engine",
                    )
                )
                logger.info(
                    "position_engine.exit_triggered",
                    position_id=pos.position_id,
                    symbol=pos.symbol,
                    reason=exit_trigger.reason,
                    fill_price=exit_trigger.fill_price,
                )

            await self._bus.publish(
                Event(
                    type=EventType.POSITION_UPDATED,
                    payload=pos,
                    source="position_engine",
                )
            )

    async def _on_exit_triggered(self, event: Event) -> None:
        trigger: ExitTrigger = event.payload
        pos = self._positions.get(trigger.position_id)
        if not pos:
            return

        closing_side = OrderSide.SELL if trigger.side == OrderSide.BUY else OrderSide.BUY
        closing_order = OrderRequest(
            signal_id=trigger.signal_id,
            symbol=trigger.symbol,
            side=closing_side,
            quantity=trigger.quantity,
            limit_price=trigger.fill_price,
            stop_price=None,
            timestamp=trigger.timestamp,
            paper=True,
        )

        await self._bus.publish(
            Event(
                type=EventType.ORDER_REQUESTED,
                payload=closing_order,
                source="position_engine",
            )
        )
        logger.info(
            "position_engine.closing_order_submitted",
            position_id=trigger.position_id,
            symbol=trigger.symbol,
            side=closing_side,
            price=trigger.fill_price,
            qty=trigger.quantity,
        )
