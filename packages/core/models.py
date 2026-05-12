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
    PENDING_APPROVAL = "pending_approval"
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
# Microstructure primitives
# ---------------------------------------------------------------------------


class BookLevel(BaseModel):
    """A single price level in the order book (bid or ask side)."""

    model_config = ConfigDict(frozen=True)

    price: float
    size: float


class QuoteSnapshot(BaseModel):
    """
    L1 best bid/ask snapshot from a broker quote stream.

    Produced by the IBKR adapter on every quote tick and published on
    the alpha:quotes:{symbol} channel.  All downstream engines receive
    this instead of raw broker types.
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime
    bid: Optional[float] = None
    ask: Optional[float] = None
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    session: SessionType = SessionType.RTH

    @property
    def spread(self) -> Optional[float]:
        if self.bid is not None and self.ask is not None:
            return round(self.ask - self.bid, 6)
        return None

    @property
    def mid(self) -> Optional[float]:
        if self.bid is not None and self.ask is not None:
            return (self.bid + self.ask) / 2.0
        return None


class OrderBookSnapshot(BaseModel):
    """
    L2 order book snapshot (top-N levels) for a symbol.

    Produced by the IBKR adapter when depth streaming is enabled and
    published on alpha:orderbook:{symbol}.  Both sides are ordered from
    best price inward (bids descending, asks ascending).
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime
    bids: list[BookLevel] = Field(default_factory=list)
    asks: list[BookLevel] = Field(default_factory=list)
    session: SessionType = SessionType.RTH


class MicrostructureSnapshot(BaseModel):
    """
    Point-in-time microstructure metrics derived from L1/L2 data.

    Published on alpha:microstructure:{symbol} by the MicrostructureEngine.
    All fields are Optional so the snapshot degrades gracefully when
    Level II data is unavailable.

    Metrics:
        spread              — ask - bid in price units
        spread_pct          — spread / mid * 100
        mid                 — (bid + ask) / 2
        bid_ask_imbalance   — (bid_size - ask_size) / (bid_size + ask_size)
                              positive = more bid support
        depth_imbalance     — same ratio but summed over all visible book levels
        quote_pressure      — smoothed EMA of bid_ask_imbalance (α=0.2)
        liquidity_pull      — True when the best bid/ask size drops more than
                              50% within two consecutive snapshots (fast cancel)
    """

    model_config = ConfigDict(frozen=True)

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    timestamp: datetime
    session: SessionType = SessionType.RTH

    spread: Optional[float] = None
    spread_pct: Optional[float] = None
    mid: Optional[float] = None

    bid_ask_imbalance: Optional[float] = None   # [-1, +1]
    depth_imbalance: Optional[float] = None     # [-1, +1], None if no L2
    quote_pressure: Optional[float] = None      # smoothed, [-1, +1]
    liquidity_pull: bool = False                # True = fast cancel detected


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


# ---------------------------------------------------------------------------
# Position management
# ---------------------------------------------------------------------------


class PositionStatus(StrEnum):
    OPEN = "open"
    STOPPED = "stopped"
    TARGETED = "targeted"
    CLOSED = "closed"


class ExitReason(StrEnum):
    STOP_HIT = "stop_hit"
    TARGET_HIT = "target_hit"
    MANUAL = "manual"


class Position(BaseModel):
    """In-memory tracking of an open position derived from a filled order."""

    model_config = ConfigDict(frozen=False)

    position_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    strategy_id: str
    signal_id: str
    entry_price: float
    stop_price: float
    target_price: float
    quantity: float
    side: OrderSide
    opened_at: datetime
    status: PositionStatus = PositionStatus.OPEN
    current_price: Optional[float] = None
    unrealized_pnl: Optional[float] = None


class ExitTrigger(BaseModel):
    """Payload published when a position's stop or target is hit."""

    model_config = ConfigDict(frozen=True)

    position_id: str
    symbol: str
    strategy_id: str
    signal_id: str
    side: OrderSide
    reason: ExitReason
    fill_price: float
    quantity: float
    timestamp: datetime
