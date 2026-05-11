from datetime import datetime, timezone

from fastapi import APIRouter

from apps.api.state import AppState

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    state = AppState.get()
    return {
        "status": "ok",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "ibkr_connected": (
            state.adapter.is_connected if state.adapter else False
        ),
    }
