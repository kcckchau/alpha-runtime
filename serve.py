#!/usr/bin/env python3
"""
Run the API with the repo root on sys.path.

Use this if you see: ModuleNotFoundError: No module named 'apps'
(often caused by starting uvicorn from a directory other than the repo root).

  python serve.py

Or from anywhere:

  python /path/to/alpha-runtime/serve.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import uvicorn

from apps.api.config import get_api_settings

if __name__ == "__main__":
    settings = get_api_settings()
    uvicorn.run(
        "apps.api.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level,
        reload=False,
    )
