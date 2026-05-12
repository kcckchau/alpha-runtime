"""
Position REST endpoints.

GET /positions            — all open positions from PositionEngine in-memory state
GET /positions/{symbol}   — current open position for a symbol, or 404
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from apps.api.state import AppState

router = APIRouter(prefix="/positions", tags=["positions"])


def _position_dict(pos) -> dict:
    return {
        "position_id": pos.position_id,
        "symbol": pos.symbol,
        "strategy_id": pos.strategy_id,
        "signal_id": pos.signal_id,
        "entry_price": pos.entry_price,
        "stop_price": pos.stop_price,
        "target_price": pos.target_price,
        "quantity": pos.quantity,
        "side": pos.side,
        "opened_at": pos.opened_at.isoformat(),
        "status": pos.status,
        "current_price": pos.current_price,
        "unrealized_pnl": pos.unrealized_pnl,
    }


@router.get("")
async def get_positions() -> list[dict]:
    state = AppState.get()
    if state.position_engine is None:
        return []
    return [_position_dict(p) for p in state.position_engine.get_open_positions()]


@router.get("/{symbol}")
async def get_position_for_symbol(symbol: str) -> dict:
    state = AppState.get()
    if state.position_engine is None:
        raise HTTPException(status_code=404, detail="No position found")
    pos = state.position_engine.get_open_position_for_symbol(symbol.upper())
    if pos is None:
        raise HTTPException(status_code=404, detail=f"No open position for {symbol.upper()}")
    return _position_dict(pos)
