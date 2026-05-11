"""
SQLAlchemy async engine + session factory.

Uses asyncpg driver.  The DATABASE_URL must point at a TimescaleDB instance.
Example: postgresql+asyncpg://alpha:alpha@localhost:5432/alpha_runtime
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase

from packages.db.config import get_db_settings


class Base(DeclarativeBase):
    pass


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        settings = get_db_settings()
        _engine = create_async_engine(
            settings.database_url,
            echo=settings.echo_sql,
            pool_size=settings.pool_size,
            max_overflow=settings.max_overflow,
            pool_pre_ping=True,
        )
    return _engine


def _factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            bind=engine(),
            expire_on_commit=False,
            class_=AsyncSession,
        )
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncIterator[AsyncSession]:
    async with _factory()() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db() -> None:
    """Create all tables (dev/test only — use Alembic in production)."""
    from packages.db.orm_models import Base as ORMBase  # local import avoids circular

    async with engine().begin() as conn:
        await conn.run_sync(ORMBase.metadata.create_all)
        # Lightweight, idempotent dev bootstrap for newly added columns.
        # Proper production migration flow should be Alembic.
        await conn.execute(
            text("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ema_9 DOUBLE PRECISION")
        )
        await conn.execute(
            text("ALTER TABLE indicators ADD COLUMN IF NOT EXISTS ema_21 DOUBLE PRECISION")
        )
