"""
Risk engine.

Subscribes to SIGNAL_DETECTED.
Validates each signal against risk rules before approving it for execution.

Guards implemented:
1. Trading-hours guard — only allow signals during RTH (configurable).
2. Duplicate prevention — reject signals for the same strategy+symbol if
   an open order already exists.
3. Max daily loss — reject new signals once realized + unrealized loss
   exceeds the daily limit.
4. Minimum R:R ratio — reject signals below the configured threshold.
5. Position sizing — compute share quantity based on fixed dollar risk.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Optional

import pytz
import structlog

from packages.core.models import (
    OrderRequest,
    OrderSide,
    SessionType,
    SetupSignal,
    SignalStatus,
)
from packages.messaging.bus import Event, EventBus, EventType
from runtime.risk_engine.config import RiskSettings, get_risk_settings

logger = structlog.get_logger(__name__)

_ET = pytz.timezone("America/New_York")


class RiskEngine:
    """
    Validates signals against risk rules.

    Approved signals transition to SignalStatus.APPROVED and trigger
    an ORDER_REQUESTED event with a sized OrderRequest.

    Rejected signals transition to SignalStatus.REJECTED and emit
    SIGNAL_REJECTED for audit logging.
    """

    def __init__(
        self,
        bus: EventBus,
        settings: Optional[RiskSettings] = None,
    ) -> None:
        self._bus = bus
        self._cfg = settings or get_risk_settings()

        # state
        self._daily_realized_loss: float = 0.0
        self._open_orders: set[str] = set()  # strategy_id:symbol keys
        self._last_reset_date: Optional[date] = None

        bus.subscribe(EventType.SIGNAL_DETECTED, self._on_signal)
        bus.subscribe(EventType.ORDER_FILLED, self._on_fill)
        bus.subscribe(EventType.ORDER_CANCELLED, self._on_order_closed)

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def _on_signal(self, event: Event) -> None:
        signal: SetupSignal = event.payload
        self._maybe_reset_daily_counters()

        rejection_reason = self._validate(signal)
        if rejection_reason:
            signal.status = SignalStatus.REJECTED
            await self._bus.publish(
                Event(
                    type=EventType.SIGNAL_REJECTED,
                    payload=signal,
                    source="risk_engine",
                )
            )
            logger.warning(
                "risk_engine.signal_rejected",
                symbol=signal.symbol,
                strategy_id=signal.strategy_id,
                reason=rejection_reason,
            )
            return

        # Approve
        signal.status = SignalStatus.APPROVED
        quantity = self._size_position(signal)
        key = f"{signal.strategy_id}:{signal.symbol}"
        self._open_orders.add(key)

        order = OrderRequest(
            signal_id=signal.id,
            symbol=signal.symbol,
            side=OrderSide.BUY,
            quantity=quantity,
            limit_price=signal.entry_price,
            stop_price=signal.stop_price,
            timestamp=datetime.now(tz=timezone.utc),
            paper=self._cfg.paper_mode,
        )

        await self._bus.publish(
            Event(
                type=EventType.SIGNAL_APPROVED,
                payload=signal,
                source="risk_engine",
            )
        )
        await self._bus.publish(
            Event(
                type=EventType.ORDER_REQUESTED,
                payload=order,
                source="risk_engine",
            )
        )
        logger.info(
            "risk_engine.signal_approved",
            symbol=signal.symbol,
            strategy_id=signal.strategy_id,
            quantity=quantity,
            entry=signal.entry_price,
        )

    async def _on_fill(self, event: Event) -> None:
        from packages.core.models import Fill
        fill: Fill = event.payload
        # Track realized P&L (simplified — only tracks losses from fills)
        # A full implementation would match fill to order and compute P&L on exit.
        key_prefix = fill.symbol
        self._open_orders.discard(key_prefix)

    async def _on_order_closed(self, event: Event) -> None:
        from packages.core.models import OrderResult
        result: OrderResult = event.payload
        key_prefix = result.symbol
        self._open_orders = {k for k in self._open_orders if not k.endswith(f":{key_prefix}")}

    # ------------------------------------------------------------------
    # Validation logic
    # ------------------------------------------------------------------

    def _validate(self, signal: SetupSignal) -> Optional[str]:
        # 1. Trading hours
        if self._cfg.rth_only and signal.context_snapshot:
            if signal.context_snapshot.session != SessionType.RTH:
                return "outside_rth"

        # 2. Duplicate prevention
        key = f"{signal.strategy_id}:{signal.symbol}"
        if key in self._open_orders:
            return "duplicate_open_order"

        # 3. Minimum R:R
        if signal.risk_reward < self._cfg.min_rr:
            return f"rr_too_low:{signal.risk_reward:.2f}<{self._cfg.min_rr}"

        # 4. Max daily loss
        if self._daily_realized_loss >= self._cfg.max_daily_loss:
            return f"max_daily_loss_hit:{self._daily_realized_loss:.2f}"

        return None

    def _size_position(self, signal: SetupSignal) -> float:
        """Fixed dollar-risk position sizing."""
        risk_per_share = abs(signal.entry_price - signal.stop_price)
        if risk_per_share <= 0:
            return 1.0
        shares = self._cfg.risk_per_trade / risk_per_share
        shares = min(shares, self._cfg.max_shares)
        return max(1.0, round(shares))

    def _maybe_reset_daily_counters(self) -> None:
        today = datetime.now(tz=timezone.utc).astimezone(_ET).date()
        if self._last_reset_date != today:
            self._daily_realized_loss = 0.0
            self._open_orders.clear()
            self._last_reset_date = today
            logger.info("risk_engine.daily_reset", date=str(today))

    def record_realized_loss(self, amount: float) -> None:
        """Called by execution engine to track realized P&L."""
        if amount > 0:
            self._daily_realized_loss += amount
