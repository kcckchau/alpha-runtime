"""GET /bootstrap/status — exposes historical backfill progress."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from apps.api.state import AppState

router = APIRouter(prefix="/bootstrap", tags=["bootstrap"])


@router.get("/status")
async def bootstrap_status() -> JSONResponse:
    """Return the current state of the historical bootstrap / backfill."""
    state = AppState.get()
    bs = state.bootstrap_status

    if bs is None:
        return JSONResponse(
            {
                "status": "not_started",
                "is_running": False,
                "is_complete": False,
                "symbols": [],
                "bars_loaded": {},
                "started_at": None,
                "completed_at": None,
                "error": None,
            }
        )

    return JSONResponse(
        {
            "status": (
                "running"
                if bs.is_running
                else ("complete" if bs.is_complete else "pending")
            ),
            "is_running": bs.is_running,
            "is_complete": bs.is_complete,
            "symbols": bs.symbols,
            "bars_loaded": bs.bars_loaded,
            "started_at": bs.started_at.isoformat() if bs.started_at else None,
            "completed_at": bs.completed_at.isoformat() if bs.completed_at else None,
            "error": bs.error,
        }
    )
