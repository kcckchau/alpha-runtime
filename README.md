# alpha-runtime

> Professional real-time trading infrastructure platform — modular monolith, event-driven, broker-agnostic, built for research and execution.

---

## Philosophy

alpha-runtime is built as a **modular monolith first**. Every engine runs in the same process under one asyncio event loop, communicating through an internal event bus. The interfaces are designed so individual engines can be extracted to separate services later if needed — without rewriting business logic.

**What this is:**
- A serious trading infrastructure platform with clean architecture
- Replayable: every candle, signal, order and fill is persisted
- Research-first: replay any session through the full strategy stack
- Paper-trading ready out of the box

**What this is NOT:**
- A toy trading bot
- A distributed microservices mess
- A regime-predicting black box

---

## Architecture

```
market_data (IBKR adapter)
    │  Tick events
    ▼
candle_engine          ← aggregates 5s bars → 1m/5m candles, VWAP
    │  CANDLE_FINAL / CANDLE_PARTIAL
    ▼
indicator_engine       ← VWAP, OR high/low, rvol, prior day H/L, premarket H/L
    │  INDICATORS_UPDATED
    ▼
context_engine         ← above/below VWAP, ORB, opening drive, HH/LL
    │  CONTEXT_UPDATED
    ▼
strategy_engine        ← plugin-based strategies (e.g. VWAP Reclaim Long)
    │  SIGNAL_DETECTED
    ▼
risk_engine            ← duplicate guard, max daily loss, sizing, R:R, hours
    │  SIGNAL_APPROVED + ORDER_REQUESTED
    ▼
execution_engine       ← paper fill simulation / live broker routing
    │  ORDER_FILLED
    ▼
replay_engine recorder ← persists everything to TimescaleDB
```

**Internal event bus** (`packages/messaging/bus.py`): asyncio pub/sub, easily swappable to Redis Streams for scale-out.

---

## Project Structure

```
alpha-runtime/
├── apps/
│   └── api/                 # FastAPI entry point, routes, WebSocket feeds
├── runtime/
│   ├── market_data/         # Broker adapter interface + IBKR implementation
│   ├── candle_engine/       # 5s→1m/5m aggregation, session VWAP
│   ├── indicator_engine/    # VWAP, OR, rvol, reference levels
│   ├── context_engine/      # Observable intraday state
│   ├── strategy_engine/     # Plugin system + strategies/
│   │   └── strategies/
│   │       └── vwap_reclaim.py
│   ├── risk_engine/         # Guards, position sizing
│   ├── execution_engine/    # Paper simulator / live execution
│   └── replay_engine/       # Event recorder + session replayer
├── packages/
│   ├── core/                # Domain models (Pydantic v2, immutable)
│   ├── messaging/           # Internal async event bus
│   └── db/                  # SQLAlchemy async ORM + session management
├── infra/
│   └── docker/
│       ├── Dockerfile
│       └── timescaledb/
│           └── init.sql     # TimescaleDB hypertable setup
├── scripts/
│   └── seed_db.py
├── tests/
│   ├── test_candle_engine.py
│   └── test_vwap_reclaim.py
├── docker-compose.yml
├── requirements.txt
├── pyproject.toml
└── .env.example
```

---

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d db redis
```

### 2. Set up environment

```bash
cp .env.example .env
# Edit .env — at minimum set IBKR_PORT to your TWS/Gateway port
```

### 3. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Initialize the database

```bash
python scripts/seed_db.py

# Then apply TimescaleDB hypertables:
psql postgresql://alpha:alpha@localhost:5432/alpha_runtime \
  -f infra/docker/timescaledb/init.sql
```

### 5. Run the API

```bash
python -m apps.api.main
```

Or with Docker:

```bash
docker compose up api
```

---

## REST API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Service health + IBKR connection status |
| `GET` | `/candles/{symbol}` | Historical candles (filterable by timeframe, session, date) |
| `GET` | `/signals` | All setup signals (filterable by symbol, status) |
| `GET` | `/context/{symbol}` | Latest market context for a symbol |
| `POST` | `/signals/{id}/approve` | Manually approve a pending signal |
| `POST` | `/signals/{id}/reject` | Manually reject a pending signal |

**WebSocket feeds** (real-time):
- `ws://localhost:8000/ws/candles` — live candle stream
- `ws://localhost:8000/ws/signals` — live signal detections
- `ws://localhost:8000/ws/fills` — paper fill confirmations

---

## Domain Models

All models are **immutable Pydantic v2** objects defined in `packages/core/models.py`.

| Model | Description |
|-------|-------------|
| `Tick` | Normalized 5-second bar from any broker |
| `Candle` | OHLCV + session VWAP for a timeframe |
| `IndicatorSnapshot` | Point-in-time computed indicators |
| `MarketContext` | Observable intraday state (no regime guessing) |
| `SetupSignal` | A detected trading setup with entry/stop/target |
| `OrderRequest` | Sized order created by the risk engine |
| `OrderResult` | Final state of an order with fill details |

---

## Strategies

Strategies are isolated plugins implementing the `Strategy` interface:

```python
class Strategy(ABC):
    @property
    @abstractmethod
    def strategy_id(self) -> str: ...

    @abstractmethod
    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]: ...

    @abstractmethod
    async def on_context_update(self, context: MarketContext) -> Optional[SetupSignal]: ...
```

### QQQ VWAP Reclaim Long (`vwap_reclaim_long_v1`)

Triggers when **all** of the following are true:

1. Price was below VWAP at prior bar, crosses above this bar
2. Strong close: candle close is in the **upper 50%** of the bar range
3. Volume confirmation: relative volume ≥ 1.2× rolling average
4. Not overextended: distance from VWAP ≤ 0.5%
5. Opening range is established (post 10:00 ET)
6. RTH session only

Risk levels:
- **Stop**: candle low − 0.05% buffer
- **Target**: entry + (entry − stop) × 2.0 (minimum 2:1 R:R)

---

## Risk Engine

| Guard | Default | Config key |
|-------|---------|------------|
| RTH only | `true` | `RISK_RTH_ONLY` |
| Min R:R | 2.0 | `RISK_MIN_RR` |
| Max daily loss | $500 | `RISK_MAX_DAILY_LOSS` |
| Risk per trade | $100 | `RISK_RISK_PER_TRADE` |
| Max shares | 500 | `RISK_MAX_SHARES` |
| Duplicate prevention | always on | — |

---

## Replay

Every session is automatically recorded. Replay any date:

```python
from packages.messaging.bus import get_bus
from runtime.replay_engine import ReplayEngine

bus = get_bus()
# ... wire your engines to the bus ...

replay = ReplayEngine(bus)
await replay.replay(symbol="QQQ", date_str="2024-01-15", speed=0.0)
# speed=0 = max speed, speed=1.0 = 1 second per candle
```

This re-runs the full strategy stack against historical data with no code changes.

---

## IBKR Connection

The adapter uses `ib_insync` with `reqRealTimeBars(5 sec)`.

| TWS Mode | Port |
|----------|------|
| Paper TWS | 7497 |
| Live TWS | 7496 |
| IB Gateway (paper) | 4002 |
| IB Gateway (live) | 4001 |

The adapter reconnects automatically with exponential back-off (2s → 4s → 8s → … → 60s).

If IBKR is unavailable on startup, the API continues running — replay and REST endpoints remain functional.

---

## Running Tests

```bash
pytest tests/ -v
```

---

## Configuration

All configuration uses `pydantic-settings` with environment variable prefixes:

| Prefix | Module |
|--------|--------|
| `DB_` | `packages/db/config.py` |
| `IBKR_` | `runtime/market_data/ibkr/config.py` |
| `RISK_` | `runtime/risk_engine/config.py` |
| `API_` | `apps/api/config.py` |

See `.env.example` for all available settings.

---

## Extending

### Adding a new strategy

```python
# runtime/strategy_engine/strategies/my_strategy.py
from runtime.strategy_engine.base import Strategy

class MyStrategy(Strategy):
    @property
    def strategy_id(self) -> str:
        return "my_strategy_v1"

    async def on_candle(self, candle: Candle) -> Optional[SetupSignal]:
        return None

    async def on_context_update(self, context: MarketContext) -> Optional[SetupSignal]:
        # Your logic here
        ...
```

Register it in `apps/api/main.py`:

```python
strategy_engine.register(MyStrategy(symbol="SPY"))
```

### Adding a new broker adapter

Implement `runtime/market_data/adapter.py:MarketDataAdapter` and wire it in `main.py`.

### Scaling to distributed

When ready:
1. Replace `packages/messaging/bus.py` internals with Redis Streams publishers/consumers
2. Each engine becomes a separate consumer group
3. No other code changes needed — all engines only see the `EventBus` interface
