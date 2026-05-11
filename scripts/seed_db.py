#!/usr/bin/env python3
"""
Seed the database schema and apply TimescaleDB hypertables.

Run once after starting docker-compose:
    python scripts/seed_db.py
"""

import asyncio
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from packages.db.session import init_db


async def main() -> None:
    print("Creating tables...")
    await init_db()
    print("Done. Tables created.")
    print()
    print("NOTE: Connect to the DB and run infra/docker/timescaledb/init.sql")
    print("      to convert tables to TimescaleDB hypertables.")
    print()
    print("      psql postgresql://alpha:alpha@localhost:5432/alpha_runtime")
    print("      \\i infra/docker/timescaledb/init.sql")


if __name__ == "__main__":
    asyncio.run(main())
