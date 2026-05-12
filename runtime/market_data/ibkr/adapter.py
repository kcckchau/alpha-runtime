"""
IBKR market data adapter using ib_insync.

Key design constraints:
- ib_insync uses its own IB event loop (util.startLoop / asyncio integration).
- We bridge it into the main asyncio loop via run_coro_threadsafe where needed.
- Real-time bars (5-second) are the primary tick source → CandleEngine.
- reqMktData provides L1 bid/ask updates → QuoteEngine / MicrostructureEngine.
- reqMktDepth provides optional L2 depth → OrderBookEngine / MicrostructureEngine.
- Reconnect logic uses exponential back-off capped at 60 s.
- The adapter NEVER leaks ib_insync types to callers — all output is typed models.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog
from ib_insync import IB, Contract, RealTimeBar, Stock, Ticker
from ib_insync.decoder import Decoder

from packages.core.models import (
    BookLevel,
    OrderBookSnapshot,
    QuoteSnapshot,
    SessionType,
    Tick,
)
from runtime.market_data.adapter import (
    DepthCallback,
    MarketDataAdapter,
    QuoteCallback,
    TickCallback,
)
from runtime.market_data.ibkr.config import IBKRSettings, get_ibkr_settings
from runtime.market_data.ibkr.session_classifier import classify_session

logger = structlog.get_logger(__name__)

_RECONNECT_DELAYS = [2, 4, 8, 16, 32, 60]  # seconds
_DECODER_GUARD_INSTALLED = False


def _install_decoder_guard() -> None:
    """
    Guard ib_insync against unknown/new message IDs from newer TWS versions.

    ib_insync can raise KeyError in Decoder.interpret() when IBKR introduces
    protocol messages unknown to this ib_insync build. We ignore only those
    unknown IDs so known messages still decode normally.
    """
    global _DECODER_GUARD_INSTALLED
    if _DECODER_GUARD_INSTALLED:
        return

    original_interpret = Decoder.interpret

    def safe_interpret(self: Decoder, fields):  # type: ignore[no-untyped-def]
        try:
            return original_interpret(self, fields)
        except KeyError as exc:
            msg_id = fields[0] if fields else "unknown"
            logger.warning("ibkr.decoder_unknown_message", msg_id=msg_id, error=str(exc))
            return None

    Decoder.interpret = safe_interpret  # type: ignore[assignment]
    _DECODER_GUARD_INSTALLED = True


def _normalize_bar_time(value: object) -> datetime:
    """Normalize ib_insync RealTimeBar.time to a UTC-aware datetime."""
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(float(value), tz=timezone.utc)


def _safe_float(value: object) -> Optional[float]:
    """Return float if value is a valid finite number, else None."""
    try:
        f = float(value)  # type: ignore[arg-type]
        return f if f == f and f != float("inf") and f != float("-inf") else None
    except (TypeError, ValueError):
        return None


class IBKRAdapter(MarketDataAdapter):
    """
    Connects to TWS / IB Gateway and streams:
      - 5-second real-time bars  → TickCallback (CandleEngine)
      - L1 bid/ask quotes        → QuoteCallback (QuoteEngine / MicrostructureEngine)
      - L2 market depth          → DepthCallback (OrderBookEngine / MicrostructureEngine)
        (only when settings.stream_depth=True)
    """

    def __init__(self, settings: Optional[IBKRSettings] = None) -> None:
        self._settings = settings or get_ibkr_settings()
        _install_decoder_guard()
        self._ib: IB = IB()
        self._tick_callbacks: list[TickCallback] = []
        self._quote_callbacks: list[QuoteCallback] = []
        self._depth_callbacks: list[DepthCallback] = []
        self._subscriptions: dict[str, Contract] = {}
        # Per-symbol depth book accumulator (mutable, keyed by (op, side, row))
        self._depth_books: dict[str, dict[tuple[int, int], BookLevel]] = {}
        self._connected = False
        self._reconnect_task: Optional[asyncio.Task[None]] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        self._ib.disconnectedEvent += self._on_disconnected

    # ------------------------------------------------------------------
    # MarketDataAdapter interface
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        connected = await self._try_once()
        if not connected:
            logger.warning(
                "ibkr.connect_failed_scheduling_retry",
                host=self._settings.host,
                port=self._settings.port,
            )
            self._start_retry_loop()

    async def disconnect(self) -> None:
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        if self._ib.isConnected():
            self._ib.disconnect()
        self._connected = False
        logger.info("ibkr.disconnected")

    async def subscribe(self, symbol: str) -> None:
        if symbol in self._subscriptions:
            logger.debug("ibkr.already_subscribed", symbol=symbol)
            return

        contract = Stock(symbol, "SMART", "USD")
        await self._ib.qualifyContractsAsync(contract)
        self._subscriptions[symbol] = contract

        # ── 5-second real-time bars (Tick/CandleEngine) ──────────────
        bars = self._ib.reqRealTimeBars(
            contract,
            barSize=5,
            whatToShow="TRADES",
            useRTH=False,
        )
        bars.updateEvent += self._make_bar_handler(symbol)

        # ── L1 quote stream ──────────────────────────────────────────
        if self._settings.stream_quotes:
            ticker: Ticker = self._ib.reqMktData(
                contract,
                genericTickList="",
                snapshot=False,
                regulatorySnapshot=False,
            )
            ticker.updateEvent += self._make_quote_handler(symbol)
            logger.info("ibkr.quotes_subscribed", symbol=symbol)

        # ── L2 depth stream ──────────────────────────────────────────
        if self._settings.stream_depth:
            self._depth_books[symbol] = {}
            depth = self._ib.reqMktDepth(
                contract,
                numRows=self._settings.depth_rows,
                isSmartDepth=True,
            )
            depth.updateEvent += self._make_depth_handler(symbol)
            logger.info(
                "ibkr.depth_subscribed",
                symbol=symbol,
                rows=self._settings.depth_rows,
            )

        logger.info("ibkr.subscribed", symbol=symbol)

    async def unsubscribe(self, symbol: str) -> None:
        if symbol not in self._subscriptions:
            return
        self._subscriptions.pop(symbol)
        self._depth_books.pop(symbol, None)
        logger.info("ibkr.unsubscribed", symbol=symbol)

    def on_tick(self, callback: TickCallback) -> None:
        self._tick_callbacks.append(callback)

    def on_quote(self, callback: QuoteCallback) -> None:
        self._quote_callbacks.append(callback)

    def on_depth_update(self, callback: DepthCallback) -> None:
        self._depth_callbacks.append(callback)

    @property
    def is_connected(self) -> bool:
        return self._connected and self._ib.isConnected()

    # ------------------------------------------------------------------
    # Internal helpers — connection management
    # ------------------------------------------------------------------

    async def _try_once(self) -> bool:
        try:
            await self._ib.connectAsync(
                self._settings.host,
                self._settings.port,
                clientId=self._settings.client_id,
                timeout=self._settings.timeout,
            )
            self._connected = True
            logger.info(
                "ibkr.connected",
                host=self._settings.host,
                port=self._settings.port,
                client_id=self._settings.client_id,
            )
            if self._subscriptions:
                symbols = list(self._subscriptions.keys())
                self._subscriptions.clear()
                for sym in symbols:
                    await self.subscribe(sym)
            return True
        except Exception:
            self._connected = False
            return False

    async def _retry_loop(self) -> None:
        for attempt, delay in enumerate(_RECONNECT_DELAYS, start=1):
            logger.info("ibkr.reconnect_wait", delay_s=delay, attempt=attempt)
            await asyncio.sleep(delay)
            if await self._try_once():
                return
            logger.warning("ibkr.connect_failed", attempt=attempt)
        logger.error("ibkr.connect_exhausted", max_attempts=len(_RECONNECT_DELAYS))
        self._reconnect_task = None

    def _start_retry_loop(self) -> None:
        if self._loop is None or self._loop.is_closed():
            return
        if self._reconnect_task and not self._reconnect_task.done():
            return
        self._reconnect_task = self._loop.create_task(self._retry_loop())

    def _on_disconnected(self) -> None:
        self._connected = False
        logger.warning("ibkr.unexpected_disconnect")
        self._start_retry_loop()

    def _schedule(self, coro) -> None:  # type: ignore[no-untyped-def]
        """Thread-safely schedule a coroutine on the main asyncio loop."""
        if self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(coro, self._loop)

    # ------------------------------------------------------------------
    # Internal helpers — event handlers
    # ------------------------------------------------------------------

    def _make_bar_handler(self, symbol: str):
        """Return a closure that handles RealTimeBar events for *symbol*."""

        def handler(bars: list[RealTimeBar], has_new_bar: bool) -> None:
            if not has_new_bar or not bars:
                return
            bar = bars[-1]
            ts = _normalize_bar_time(bar.time)
            session = classify_session(ts)
            open_price = getattr(bar, "open", getattr(bar, "open_", None))
            if open_price is None:
                logger.warning("ibkr.bar_missing_open", symbol=symbol)
                return
            tick = Tick(
                symbol=symbol,
                timestamp=ts,
                open=float(open_price),
                high=float(bar.high),
                low=float(bar.low),
                close=float(bar.close),
                volume=float(bar.volume),
                session=session,
            )
            self._schedule(self._dispatch_tick(tick))

        return handler

    def _make_quote_handler(self, symbol: str):
        """Return a closure that handles Ticker (L1 quote) update events."""

        def handler(ticker: Ticker) -> None:
            ts = datetime.now(tz=timezone.utc)
            session = classify_session(ts)

            bid = _safe_float(ticker.bid)
            ask = _safe_float(ticker.ask)
            bid_size = _safe_float(ticker.bidSize)
            ask_size = _safe_float(ticker.askSize)

            # Skip completely empty quote updates (ib_insync fires eagerly)
            if bid is None and ask is None:
                return

            quote = QuoteSnapshot(
                symbol=symbol,
                timestamp=ts,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                session=session,
            )
            self._schedule(self._dispatch_quote(quote))

        return handler

    def _make_depth_handler(self, symbol: str):
        """
        Return a closure that handles market-depth update events.

        ib_insync fires depth updates as individual row changes
        (operation=0 insert, 1 update, 2 delete; side=0 ask, 1 bid).
        We maintain a running book dict and publish a full snapshot after
        every update.
        """

        def handler(ticker: Ticker, side: int, op: int, row: int,
                    price: float, size: float) -> None:
            book = self._depth_books.get(symbol)
            if book is None:
                return

            key = (side, row)
            if op == 2:  # delete
                book.pop(key, None)
            else:         # insert or update
                p = _safe_float(price)
                s = _safe_float(size)
                if p is not None and s is not None:
                    book[key] = BookLevel(price=p, size=s)

            # Rebuild sorted sides and publish snapshot
            bids = sorted(
                (v for (sd, _), v in book.items() if sd == 1),
                key=lambda lv: lv.price,
                reverse=True,  # best bid first
            )
            asks = sorted(
                (v for (sd, _), v in book.items() if sd == 0),
                key=lambda lv: lv.price,  # best ask first
            )
            ts = datetime.now(tz=timezone.utc)
            snapshot = OrderBookSnapshot(
                symbol=symbol,
                timestamp=ts,
                bids=bids,
                asks=asks,
                session=classify_session(ts),
            )
            self._schedule(self._dispatch_depth(snapshot))

        return handler

    # ------------------------------------------------------------------
    # Async dispatchers
    # ------------------------------------------------------------------

    async def _dispatch_tick(self, tick: Tick) -> None:
        for cb in self._tick_callbacks:
            try:
                await cb(tick)
            except Exception:
                logger.exception("ibkr.tick_callback_error", symbol=tick.symbol)

    async def _dispatch_quote(self, quote: QuoteSnapshot) -> None:
        for cb in self._quote_callbacks:
            try:
                await cb(quote)
            except Exception:
                logger.exception("ibkr.quote_callback_error", symbol=quote.symbol)

    async def _dispatch_depth(self, snap: OrderBookSnapshot) -> None:
        for cb in self._depth_callbacks:
            try:
                await cb(snap)
            except Exception:
                logger.exception("ibkr.depth_callback_error", symbol=snap.symbol)
