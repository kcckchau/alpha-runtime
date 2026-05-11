"""
SQLAlchemy ORM models mapped to TimescaleDB tables.

Hypertables are created via the init SQL script in infra/docker/timescaledb/init.sql.
This module only defines the ORM structure; TimescaleDB-specific DDL
(create_hypertable) is applied separately.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class CandleORM(Base):
    __tablename__ = "candles"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    vwap: Mapped[float] = mapped_column(Float, nullable=False)
    cumulative_volume: Mapped[float] = mapped_column(Float, default=0.0)
    cumulative_turnover: Mapped[float] = mapped_column(Float, default=0.0)
    session: Mapped[str] = mapped_column(String(20), nullable=False)
    is_partial: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        Index("ix_candles_symbol_timeframe_ts", "symbol", "timeframe", "timestamp"),
    )


class IndicatorSnapshotORM(Base):
    __tablename__ = "indicators"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timeframe: Mapped[str] = mapped_column(String(10), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    vwap: Mapped[float | None] = mapped_column(Float)
    vwap_distance_pct: Mapped[float | None] = mapped_column(Float)
    opening_range_high: Mapped[float | None] = mapped_column(Float)
    opening_range_low: Mapped[float | None] = mapped_column(Float)
    opening_range_mid: Mapped[float | None] = mapped_column(Float)
    rolling_volume_avg: Mapped[float | None] = mapped_column(Float)
    relative_volume: Mapped[float | None] = mapped_column(Float)
    prior_day_high: Mapped[float | None] = mapped_column(Float)
    prior_day_low: Mapped[float | None] = mapped_column(Float)
    premarket_high: Mapped[float | None] = mapped_column(Float)
    premarket_low: Mapped[float | None] = mapped_column(Float)

    __table_args__ = (
        Index("ix_indicators_symbol_ts", "symbol", "timestamp"),
    )


class MarketContextORM(Base):
    __tablename__ = "market_context"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    above_vwap: Mapped[bool | None] = mapped_column(Boolean)
    vwap_cross_up: Mapped[bool] = mapped_column(Boolean, default=False)
    vwap_cross_down: Mapped[bool] = mapped_column(Boolean, default=False)
    vwap_distance_pct: Mapped[float | None] = mapped_column(Float)
    orb_breakout_up: Mapped[bool] = mapped_column(Boolean, default=False)
    orb_breakout_down: Mapped[bool] = mapped_column(Boolean, default=False)
    orb_hold: Mapped[bool | None] = mapped_column(Boolean)
    higher_highs: Mapped[bool] = mapped_column(Boolean, default=False)
    lower_lows: Mapped[bool] = mapped_column(Boolean, default=False)
    opening_drive_up: Mapped[bool] = mapped_column(Boolean, default=False)
    opening_drive_down: Mapped[bool] = mapped_column(Boolean, default=False)
    session: Mapped[str] = mapped_column(String(20), nullable=False)

    __table_args__ = (
        Index("ix_market_context_symbol_ts", "symbol", "timestamp"),
    )


class SignalORM(Base):
    __tablename__ = "signals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    candle_ref: Mapped[str | None] = mapped_column(String(36))
    entry_price: Mapped[float] = mapped_column(Float, nullable=False)
    stop_price: Mapped[float] = mapped_column(Float, nullable=False)
    target_price: Mapped[float] = mapped_column(Float, nullable=False)
    risk_reward: Mapped[float] = mapped_column(Float, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    notes: Mapped[str] = mapped_column(Text, default="")

    orders: Mapped[list[OrderORM]] = relationship("OrderORM", back_populates="signal")

    __table_args__ = (
        Index("ix_signals_symbol_ts", "symbol", "timestamp"),
    )


class OrderORM(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True, index=True)
    signal_id: Mapped[str] = mapped_column(String(36), ForeignKey("signals.id"), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    requested_quantity: Mapped[float] = mapped_column(Float, nullable=False)
    filled_quantity: Mapped[float] = mapped_column(Float, default=0.0)
    avg_fill_price: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    paper: Mapped[bool] = mapped_column(Boolean, default=True)
    error_message: Mapped[str | None] = mapped_column(Text)

    signal: Mapped[SignalORM] = relationship("SignalORM", back_populates="orders")
    fills: Mapped[list[FillORM]] = relationship("FillORM", back_populates="order")

    __table_args__ = (
        Index("ix_orders_symbol_ts", "symbol", "submitted_at"),
    )


class FillORM(Base):
    __tablename__ = "fills"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    order_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("orders.order_id"), nullable=False, index=True
    )
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    paper: Mapped[bool] = mapped_column(Boolean, default=True)

    order: Mapped[OrderORM] = relationship("OrderORM", back_populates="fills")

    __table_args__ = (
        Index("ix_fills_order_ts", "order_id", "timestamp"),
    )


class ReplaySessionORM(Base):
    """Metadata for a recorded session that can be replayed."""

    __tablename__ = "replay_sessions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    date: Mapped[str] = mapped_column(String(10), nullable=False, index=True)  # YYYY-MM-DD
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    session_type: Mapped[str] = mapped_column(String(20), nullable=False)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    tick_count: Mapped[int] = mapped_column(Integer, default=0)
    candle_count: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[str] = mapped_column(Text, default="")
