"""
Application state container — avoids global variables scattered across modules.

All mutable process-level state lives here and is accessed via AppState.get().
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from packages.messaging.bus import EventBus
    from runtime.candle_engine import CandleEngine
    from runtime.context_engine import ContextEngine
    from runtime.execution_engine import ExecutionEngine
    from runtime.indicator_engine import IndicatorEngine
    from runtime.market_data.adapter import MarketDataAdapter
    from runtime.risk_engine import RiskEngine
    from runtime.strategy_engine import StrategyEngine


class AppState:
    _instance: Optional["AppState"] = None

    def __init__(self) -> None:
        self.bus: Optional["EventBus"] = None
        self.adapter: Optional["MarketDataAdapter"] = None
        self.candle_engine: Optional["CandleEngine"] = None
        self.indicator_engine: Optional["IndicatorEngine"] = None
        self.context_engine: Optional["ContextEngine"] = None
        self.strategy_engine: Optional["StrategyEngine"] = None
        self.risk_engine: Optional["RiskEngine"] = None
        self.execution_engine: Optional["ExecutionEngine"] = None

    @classmethod
    def get(cls) -> "AppState":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
