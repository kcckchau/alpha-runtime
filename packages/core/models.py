"""
Core domain models for alpha-runtime.

All models are immutable Pydantic v2 dataclasses.  Infrastructure layers
(DB, serialization, messaging) must adapt *to* these, not the other way around.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Optional

# Stable namespace for deterministic candle UUIDs (upsert deduplication)
_CANDLE_NS = uuid.UUID("a3c4b5d6-e7f8-4a9b-8c0d-1e2f3a4b5c6d")


def candle_stable_id(symbol: str, timeframe: str, ts: datetime) -> str:
    """
    Return a deterministic UUID for a (symbol, timeframe, timestamp) triple.

    Using this as the candle primary key means loading the same bar twice
    via bootstrap or recorder will upsert rather than duplicate.
    """
    return str(uuid.uuid5(_CANDLE_NS, f"{symbol}:{timeframe}:{ts.isoformat()}"))

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class Timeframe(StrEnum):
    SEC_5 = "5s"
    MIN_1 = "1m"
    MIN_5 = "5m"
    MIN_15 = "15m"
    MIN_30 = "30m"
    HOUR_1 = "1h"
    DAY_1 = "1d"


class SessionType(StrEnum):
    PREMARKET = "premarket"
    RTH = "rth"
    AFTERHOURS = "afterhours"
    CLOSED = "closed"


class SignalDirection(StrEnum):
    LONG = "long"
    SHORT = "short"


class SignalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    FILLED = "filled"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


# ---------------------------------------------------------------------------
# Market data primitives
# ---------------------------------------------------------------------------


class Tick(BaseModel):
    """Normalized real-time price tick from any broker adapter."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    session: SessionType = SessionType.RTH


class Candle(BaseModel):
    """OHLCV candle with VWAP for a specific timeframe and symbol."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timeframe: Timeframe
    timestamp: datetime  # bar open time
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    cumulative_volume: float = 0.0
    cumulative_turnover: float = 0.0  # sum(price * volume) for intra-session VWAP
    session: SessionType = SessionType.RTH
    is_partial: bool = False  # True while the bar is still building
    historical: bool = False  # True for bootstrap/backfill candles


# ---------------------------------------------------------------------------
# Indicator layer
# ---------------------------------------------------------------------------


class IndicatorSnapshot(BaseModel):
    """Point-in-time computed indicator values for a symbol."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime
    timeframe: Timeframe

    # VWAP (session-anchored)
    vwap: Optional[float] = None
    vwap_distance_pct: Optional[float] = None  # (close - vwap) / vwap * 100

    # Opening range (first 30 min of RTH)
    opening_range_high: Optional[float] = None
    opening_range_low: Optional[float] = None
    opening_range_mid: Optional[float] = None

    # Volume
    rolling_volume_avg: Optional[float] = None  # 20-period average
    relative_volume: Optional[float] = None  # current / rolling_avg

    # Reference levels
    prior_day_high: Optional[float] = None
    prior_day_low: Optional[float] = None
    premarket_high: Optional[float] = None
    premarket_low: Optional[float] = None

    # Exponential moving averages (RTH-anchored, reset each day)
    ema_9: Optional[float] = None
    ema_21: Optional[float] = None


# ---------------------------------------------------------------------------
# Context engine output
# ---------------------------------------------------------------------------


class MarketContext(BaseModel):
    """
    Observable intraday market state — no regime guessing.

    We record *what is* (price relative to levels, recent structure)
    rather than pretending to classify trend/chop.
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime

    # VWAP relationship
    above_vwap: Optional[bool] = None
    vwap_cross_up: bool = False    # crossed above this bar
    vwap_cross_down: bool = False  # crossed below this bar
    vwap_distance_pct: Optional[float] = None

    # Opening range
    orb_breakout_up: bool = False   # broke above OR high
    orb_breakout_down: bool = False
    orb_hold: Optional[bool] = None  # holding above OR high after breakout

    # Price structure (last N candles)
    higher_highs: bool = False
    lower_lows: bool = False

    # Opening drive: strong directional move in first 30 min
    opening_drive_up: bool = False
    opening_drive_down: bool = False

    session: SessionType = SessionType.RTH
    indicators: Optional[IndicatorSnapshot] = None
    historical: bool = False  # True for bootstrap/backfill context


# ---------------------------------------------------------------------------
# Strategy output
# ---------------------------------------------------------------------------


class SetupSignal(BaseModel):
    """A detected trading setup emitted by a strategy plugin."""

    model_config = ConfigDict(frozen=False)  # mutable for status transitions

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy_id: str
    symbol: str
    direction: SignalDirection
    status: SignalStatus = SignalStatus.PENDING

    timestamp: datetime
    candle_ref: Optional[str] = None  # Candle.id that triggered this signal

    entry_price: float
    stop_price: float
    target_price: float

    risk_reward: float = Field(ge=0.0)
    confidence: float = Field(ge=0.0, le=1.0)

    notes: str = ""
    context_snapshot: Optional[MarketContext] = None


# ---------------------------------------------------------------------------
# Execution layer
# ---------------------------------------------------------------------------


class OrderRequest(BaseModel):
    """Request to place an order, created by the execution engine."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    signal_id: str
    symbol: str
    side: OrderSide
    quantity: float
    limit_price: Optional[float] = None  # None = market order
    stop_price: Optional[float] = None
    timestamp: datetime
    paper: bool = True  # always start in paper mode


class Fill(BaseModel):
    """A single execution fill."""

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    symbol: str
    side: OrderSide
    quantity: float
    price: float
    timestamp: datetime
    paper: bool = True


class OrderResult(BaseModel):
    """Final state of a submitted order."""

    model_config = ConfigDict(frozen=False)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    order_id: str
    signal_id: str
    symbol: str
    side: OrderSide
    requested_quantity: float
    filled_quantity: float = 0.0
    avg_fill_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    fills: list[Fill] = Field(default_factory=list)
    submitted_at: datetime
    updated_at: Optional[datetime] = None
    paper: bool = True
    error_message: Optional[str] = None
