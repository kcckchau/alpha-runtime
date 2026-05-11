"""
Execution engine.

Subscribes to ORDER_REQUESTED.
In paper mode: simulates fills conservatively (worst-case fill assumptions).
In live mode: routes to the broker adapter (not yet implemented).

Paper trading assumptions (conservative):
- Limit orders fill at the limit price if the next tick trades through it.
- Market orders fill at the last known price + a configurable slippage.
- Partial fills are not simulated — orders are all-or-nothing in paper mode.
- We track open paper positions in memory (no broker state needed).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import structlog

from packages.core.models import (
    Fill,
    OrderRequest,
    OrderResult,
    OrderSide,
    OrderStatus,
    Tick,
)
from packages.messaging.bus import Event, EventBus, EventType

logger = structlog.get_logger(__name__)

_MARKET_SLIPPAGE_PCT = 0.05  # 0.05% slippage on market orders


class PaperFillSimulator:
    """
    Minimal paper fill simulator.

    On each tick, checks pending limit orders and fills them if the
    market has traded through the limit price.
    """

    def __init__(self) -> None:
        # order_id → (OrderRequest, OrderResult)
        self._pending: dict[str, tuple[OrderRequest, OrderResult]] = {}

    def submit(self, order: OrderRequest) -> OrderResult:
        result = OrderResult(
            order_id=order.id,
            signal_id=order.signal_id,
            symbol=order.symbol,
            side=order.side,
            requested_quantity=order.quantity,
            status=OrderStatus.SUBMITTED,
            submitted_at=order.timestamp,
            paper=True,
        )
        self._pending[order.id] = (order, result)
        return result

    def on_tick(self, tick: Tick) -> list[tuple[OrderResult, Fill]]:
        """Check if any pending order can be filled against this tick."""
        fills: list[tuple[OrderResult, Fill]] = []
        to_remove: list[str] = []

        for order_id, (order, result) in self._pending.items():
            if order.symbol != tick.symbol:
                continue

            fill_price = self._compute_fill_price(order, tick)
            if fill_price is None:
                continue

            fill = Fill(
                order_id=order.id,
                symbol=order.symbol,
                side=order.side,
                quantity=order.quantity,
                price=fill_price,
                timestamp=tick.timestamp,
                paper=True,
            )
            result.fills.append(fill)
            result.filled_quantity = order.quantity
            result.avg_fill_price = fill_price
            result.status = OrderStatus.FILLED
            result.updated_at = tick.timestamp

            fills.append((result, fill))
            to_remove.append(order_id)

        for oid in to_remove:
            del self._pending[oid]

        return fills

    def cancel_all(self, symbol: Optional[str] = None) -> list[OrderResult]:
        cancelled: list[OrderResult] = []
        to_remove = [
            oid
            for oid, (order, _) in self._pending.items()
            if symbol is None or order.symbol == symbol
        ]
        for oid in to_remove:
            _, result = self._pending.pop(oid)
            result.status = OrderStatus.CANCELLED
            result.updated_at = datetime.now(tz=timezone.utc)
            cancelled.append(result)
        return cancelled

    @staticmethod
    def _compute_fill_price(order: OrderRequest, tick: Tick) -> Optional[float]:
        if order.limit_price is None:
            # Market order — fill at next tick close + slippage
            slip = tick.close * _MARKET_SLIPPAGE_PCT / 100.0
            return tick.close + (slip if order.side == OrderSide.BUY else -slip)

        # Limit buy: fill if tick.low <= limit (market traded through our price)
        if order.side == OrderSide.BUY and tick.low <= order.limit_price:
            return order.limit_price

        # Limit sell: fill if tick.high >= limit
        if order.side == OrderSide.SELL and tick.high >= order.limit_price:
            return order.limit_price

        return None


class ExecutionEngine:
    """
    Routes approved orders to paper or live execution.

    Paper mode only in v0.1.
    """

    def __init__(self, bus: EventBus, paper: bool = True) -> None:
        self._bus = bus
        self._paper = paper
        self._simulator = PaperFillSimulator()

        bus.subscribe(EventType.ORDER_REQUESTED, self._on_order_requested)
        bus.subscribe(EventType.TICK_RECEIVED, self._on_tick)

    async def _on_order_requested(self, event: Event) -> None:
        order: OrderRequest = event.payload
        if not order.paper and not self._paper:
            logger.error("execution_engine.live_not_implemented")
            return

        result = self._simulator.submit(order)

        await self._bus.publish(
            Event(
                type=EventType.ORDER_SUBMITTED,
                payload=result,
                source="execution_engine",
            )
        )
        logger.info(
            "execution_engine.order_submitted",
            order_id=order.id,
            symbol=order.symbol,
            side=order.side,
            qty=order.quantity,
            limit=order.limit_price,
            paper=True,
        )

    async def _on_tick(self, event: Event) -> None:
        tick: Tick = event.payload
        fills = self._simulator.on_tick(tick)

        for result, fill in fills:
            await self._bus.publish(
                Event(
                    type=EventType.ORDER_FILLED,
                    payload=fill,
                    source="execution_engine",
                )
            )
            await self._bus.publish(
                Event(
                    type=EventType.ORDER_FILLED,
                    payload=result,
                    source="execution_engine",
                )
            )
            logger.info(
                "execution_engine.fill",
                order_id=fill.order_id,
                symbol=fill.symbol,
                price=fill.price,
                qty=fill.quantity,
                paper=True,
            )
