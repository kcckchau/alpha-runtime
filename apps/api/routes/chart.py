"""
apps/api/routes/chart.py
─────────────────────────
Live trading chart served as a self-contained HTML page.

Features:
- Timeframe selector: 1m / 5m / 1h  (selectable, persisted in URL hash)
- Session-anchored VWAP  (cyan)
- EMA 9  (amber)
- EMA 21 (violet)
- Live WebSocket candle updates
- Signal overlay (entry / stop / target lines on chart)
- Context panel + signal feed below chart

Backend:
- GET /candles/{symbol}?timeframe=1m&date=YYYY-MM-DD  → ET session day (04:00–20:00 ET)
- WS  /ws/candles                               → live updates
- WS  /ws/signals                               → signal alerts
- GET /context/{symbol}                         → context polling
"""

import json
from typing import Optional

import structlog
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import select

from apps.api.trading_day_bounds import (
    extended_session_utc_bounds,
    parse_trade_date,
    today_trade_date_et,
)
from packages.core.models import Timeframe
from packages.db.orm_models import CandleORM
from packages.db.session import get_session

router = APIRouter(tags=["chart"])
logger = structlog.get_logger(__name__)

# Map URL param → DB timeframe value
_TF_MAP = {
    "1m":  Timeframe.MIN_1.value,
    "5m":  Timeframe.MIN_5.value,
    "1h":  Timeframe.HOUR_1.value,
}
_DEFAULT_TF = "1m"
_CHART_LIMIT = 2000


@router.get("/chart/{symbol}", response_class=HTMLResponse)
async def live_chart(
    symbol: str,
    tf: str = _DEFAULT_TF,
    date: Optional[str] = None,
) -> HTMLResponse:
    symbol = symbol.upper()
    tf = tf if tf in _TF_MAP else _DEFAULT_TF
    db_tf = _TF_MAP[tf]

    try:
        trade_d = parse_trade_date(date) if date else today_trade_date_et()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    start_utc, end_utc = extended_session_utc_bounds(trade_d)
    live_candle_ws = trade_d == today_trade_date_et()

    initial: list[dict] = []
    try:
        async with get_session() as db:
            stmt = (
                select(CandleORM)
                .where(CandleORM.symbol == symbol)
                .where(CandleORM.timeframe == db_tf)
                .where(CandleORM.is_partial.is_(False))
                .where(CandleORM.timestamp >= start_utc)
                .where(CandleORM.timestamp < end_utc)
                .order_by(CandleORM.timestamp.asc())
                .limit(_CHART_LIMIT)
            )
            result = await db.execute(stmt)
            rows = result.scalars().all()
            initial = [
                {
                    "timestamp": r.timestamp.isoformat(),
                    "open":    r.open,
                    "high":    r.high,
                    "low":     r.low,
                    "close":   r.close,
                    "volume":  r.volume,
                    "vwap":    r.vwap,
                    "session": r.session,
                }
                for r in rows
            ]
    except Exception as exc:
        logger.warning("chart.load_failed", symbol=symbol, tf=tf, error=str(exc))
        initial = []

    candles_json = json.dumps(initial)

    html = _build_html(
        symbol,
        tf,
        candles_json,
        trade_d.isoformat(),
        live_candle_ws,
    )
    return HTMLResponse(content=html)


def _build_html(
    symbol: str,
    active_tf: str,
    candles_json: str,
    trade_date: str,
    live_candle_ws: bool,
) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{symbol} · alpha-runtime</title>
  <script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap');

    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

    :root {{
      --bg:        #080b10;
      --surface:   #0e1219;
      --border:    #1a2235;
      --border2:   #243044;
      --text:      #c8d6e8;
      --muted:     #4e6282;
      --muted2:    #6b84a3;
      --accent:    #00d4ff;
      --green:     #00e676;
      --red:       #ff4560;
      --amber:     #ffb300;
      --violet:    #b388ff;
      --font-mono: 'IBM Plex Mono', monospace;
      --font-sans: 'IBM Plex Sans', sans-serif;
    }}

    html, body {{ height: 100%; overflow: hidden; }}

    body {{
      background: var(--bg);
      color: var(--text);
      font-family: var(--font-sans);
      display: flex;
      flex-direction: column;
    }}

    /* ── Top bar ─────────────────────────────── */
    .topbar {{
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      gap: 16px;
      padding: 0 16px;
      height: 44px;
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      position: relative;
      z-index: 10;
    }}

    .symbol {{
      font-family: var(--font-mono);
      font-size: 15px;
      font-weight: 600;
      color: #fff;
      letter-spacing: 0.05em;
    }}

    .price-display {{
      font-family: var(--font-mono);
      font-size: 13px;
      display: flex;
      gap: 12px;
      align-items: center;
    }}
    .price-last  {{ color: #fff; font-weight: 600; font-size: 15px; }}
    .price-chg   {{ font-size: 12px; }}
    .price-chg.pos {{ color: var(--green); }}
    .price-chg.neg {{ color: var(--red); }}

    /* ── Timeframe tabs ──────────────────────── */
    .tf-group {{
      display: flex;
      gap: 2px;
      margin-left: auto;
    }}

    .tf-btn {{
      font-family: var(--font-mono);
      font-size: 11px;
      font-weight: 500;
      letter-spacing: 0.06em;
      padding: 4px 10px;
      border: 1px solid var(--border2);
      background: transparent;
      color: var(--muted2);
      cursor: pointer;
      border-radius: 3px;
      transition: all 0.12s;
    }}
    .tf-btn:hover   {{ background: var(--border); color: var(--text); }}
    .tf-btn.active  {{
      background: var(--accent);
      border-color: var(--accent);
      color: #000;
      font-weight: 600;
    }}

    /* ── Legend ─────────────────────────────── */
    .legend {{
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      gap: 18px;
      padding: 0 16px;
      height: 28px;
      background: var(--bg);
      border-bottom: 1px solid var(--border);
      font-family: var(--font-mono);
      font-size: 11px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 5px;
      color: var(--muted2);
    }}
    .legend-dot {{
      width: 20px;
      height: 2px;
      border-radius: 1px;
      flex-shrink: 0;
    }}
    .legend-item span {{ font-weight: 500; }}
    .legend-val {{ color: var(--text); }}

    /* ── Chart area ──────────────────────────── */
    .chart-wrap {{
      flex: 1 1 auto;
      min-height: 0;
      display: flex;
      flex-direction: column;
    }}

    #chart {{
      flex: 1 1 auto;
      min-height: 0;
      width: 100%;
    }}

    /* ── Bottom panels ───────────────────────── */
    .bottom {{
      flex: 0 0 auto;
      display: grid;
      grid-template-columns: 1fr 1fr 1fr 1fr;
      height: 148px;
      border-top: 1px solid var(--border);
    }}

    .panel {{
      padding: 10px 14px;
      border-right: 1px solid var(--border);
      overflow: hidden;
    }}
    .panel:last-child {{ border-right: none; }}

    .panel-hdr {{
      font-family: var(--font-mono);
      font-size: 9px;
      font-weight: 600;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      color: var(--muted);
      margin-bottom: 8px;
    }}

    /* Context grid */
    .ctx-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 3px 12px;
    }}
    .ctx-row {{
      font-family: var(--font-mono);
      font-size: 10px;
      display: flex;
      justify-content: space-between;
      gap: 4px;
    }}
    .ctx-key {{ color: var(--muted2); }}
    .ctx-val {{ color: var(--text); font-weight: 500; }}
    .ctx-val.true  {{ color: var(--green); }}
    .ctx-val.false {{ color: var(--muted); }}
    .ctx-val.pos   {{ color: var(--green); }}
    .ctx-val.neg   {{ color: var(--red); }}

    /* Indicator values */
    .ind-grid {{
      display: flex;
      flex-direction: column;
      gap: 4px;
    }}
    .ind-row {{
      font-family: var(--font-mono);
      font-size: 11px;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .ind-swatch {{ width: 16px; height: 2px; border-radius: 1px; flex-shrink: 0; }}
    .ind-label  {{ color: var(--muted2); font-size: 10px; width: 44px; }}
    .ind-val    {{ color: var(--text); font-weight: 500; }}

    /* Signals */
    .signal-feed {{
      display: flex;
      flex-direction: column;
      gap: 3px;
      overflow: hidden;
    }}
    .sig-row {{
      font-family: var(--font-mono);
      font-size: 10px;
      display: flex;
      align-items: center;
      gap: 6px;
      color: var(--muted2);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .sig-row.long  {{ color: var(--green); }}
    .sig-row.short {{ color: var(--red); }}
    .sig-time  {{ color: var(--muted); flex-shrink: 0; }}
    .sig-badge {{
      flex-shrink: 0;
      font-size: 9px;
      font-weight: 600;
      padding: 1px 5px;
      border-radius: 2px;
    }}
    .sig-badge.long  {{ background: rgba(0,230,118,.15); color: var(--green); }}
    .sig-badge.short {{ background: rgba(255,69,96,.15);  color: var(--red); }}

    /* Microstructure panel */
    .micro-grid {{
      display: flex;
      flex-direction: column;
      gap: 5px;
    }}
    .micro-row {{
      font-family: var(--font-mono);
      font-size: 10px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .micro-key {{ color: var(--muted2); }}
    .micro-val {{
      font-weight: 600;
      font-family: var(--font-mono);
      font-size: 11px;
    }}
    .micro-val.good    {{ color: var(--green); }}
    .micro-val.warn    {{ color: var(--amber); }}
    .micro-val.bad     {{ color: var(--red); }}
    .micro-val.neutral {{ color: var(--text); }}
    .micro-val.muted   {{ color: var(--muted); }}

    /* Flash animation: liquidity pull alert */
    @keyframes flashBorder {{
      0%   {{ border-color: var(--red); box-shadow: 0 0 0 1px var(--red); }}
      100% {{ border-color: var(--border); box-shadow: none; }}
    }}
    .panel.flash-alert {{
      animation: flashBorder 2s ease-out forwards;
    }}

    /* Sign-change highlight on pressure value */
    @keyframes pressureFlash {{
      0%   {{ background: rgba(255,255,255,.18); }}
      100% {{ background: transparent; }}
    }}
    .micro-val.pressure-flash {{
      animation: pressureFlash 0.6s ease-out forwards;
      border-radius: 2px;
    }}

    /* Status bar */
    #statusbar {{
      flex: 0 0 auto;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      height: 22px;
      background: var(--surface);
      border-top: 1px solid var(--border);
      font-family: var(--font-mono);
      font-size: 10px;
      color: var(--muted);
    }}
    .ws-dot {{
      display: inline-block;
      width: 6px; height: 6px;
      border-radius: 50%;
      background: var(--muted);
      margin-right: 5px;
      transition: background 0.3s;
    }}
    .ws-dot.live {{ background: var(--green); box-shadow: 0 0 4px var(--green); }}
  </style>
</head>
<body>

  <!-- Top bar -->
  <div class="topbar">
    <div class="symbol">{symbol}</div>
    <div class="price-display">
      <span class="price-last" id="priceDisplay">—</span>
      <span class="price-chg"  id="priceChg"></span>
    </div>
    <div class="tf-group">
      <button class="tf-btn" data-tf="1m">1m</button>
      <button class="tf-btn" data-tf="5m">5m</button>
      <button class="tf-btn" data-tf="1h">1h</button>
    </div>
  </div>

  <!-- Legend -->
  <div class="legend">
    <div class="legend-item">
      <div class="legend-dot" style="background:var(--accent)"></div>
      <span>VWAP</span> <span class="legend-val" id="legVwap">—</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:var(--amber)"></div>
      <span>EMA 9</span> <span class="legend-val" id="legEma9">—</span>
    </div>
    <div class="legend-item">
      <div class="legend-dot" style="background:var(--violet)"></div>
      <span>EMA 21</span> <span class="legend-val" id="legEma21">—</span>
    </div>
    <div class="legend-item" style="margin-left:auto">
      <span id="legOHLC" style="font-family:var(--font-mono);font-size:11px;color:var(--muted2)"></span>
    </div>
  </div>

  <!-- Chart -->
  <div class="chart-wrap">
    <div id="chart"></div>
  </div>

  <!-- Bottom panels -->
  <div class="bottom">
    <div class="panel">
      <div class="panel-hdr">Indicators</div>
      <div class="ind-grid" id="indPanel">
        <div class="ind-row">
          <div class="ind-swatch" style="background:var(--accent)"></div>
          <div class="ind-label">VWAP</div>
          <div class="ind-val" id="indVwap">—</div>
        </div>
        <div class="ind-row">
          <div class="ind-swatch" style="background:var(--amber)"></div>
          <div class="ind-label">EMA 9</div>
          <div class="ind-val" id="indEma9">—</div>
        </div>
        <div class="ind-row">
          <div class="ind-swatch" style="background:var(--violet)"></div>
          <div class="ind-label">EMA 21</div>
          <div class="ind-val" id="indEma21">—</div>
        </div>
        <div class="ind-row" style="margin-top:4px">
          <div class="ind-swatch" style="background:var(--muted)"></div>
          <div class="ind-label">ORB H</div>
          <div class="ind-val" id="indOrbH">—</div>
        </div>
        <div class="ind-row">
          <div class="ind-swatch" style="background:var(--muted)"></div>
          <div class="ind-label">ORB L</div>
          <div class="ind-val" id="indOrbL">—</div>
        </div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-hdr">Context</div>
      <div class="ctx-grid" id="ctxPanel">
        <div class="ctx-row"><span class="ctx-key">above vwap</span><span class="ctx-val" id="cAboveVwap">—</span></div>
        <div class="ctx-row"><span class="ctx-key">vwap dist%</span><span class="ctx-val" id="cVwapDist">—</span></div>
        <div class="ctx-row"><span class="ctx-key">cross ↑</span><span class="ctx-val" id="cCrossUp">—</span></div>
        <div class="ctx-row"><span class="ctx-key">cross ↓</span><span class="ctx-val" id="cCrossDn">—</span></div>
        <div class="ctx-row"><span class="ctx-key">orb hold</span><span class="ctx-val" id="cOrbHold">—</span></div>
        <div class="ctx-row"><span class="ctx-key">orb brk ↑</span><span class="ctx-val" id="cOrbUp">—</span></div>
        <div class="ctx-row"><span class="ctx-key">drive ↑</span><span class="ctx-val" id="cDriveUp">—</span></div>
        <div class="ctx-row"><span class="ctx-key">drive ↓</span><span class="ctx-val" id="cDriveDn">—</span></div>
        <div class="ctx-row"><span class="ctx-key">HH</span><span class="ctx-val" id="cHH">—</span></div>
        <div class="ctx-row"><span class="ctx-key">LL</span><span class="ctx-val" id="cLL">—</span></div>
      </div>
    </div>
    <div class="panel">
      <div class="panel-hdr">Signals</div>
      <div class="signal-feed" id="sigFeed"></div>
    </div>
    <div class="panel" id="microPanel">
      <div class="panel-hdr">Microstructure</div>
      <div class="micro-grid">
        <div class="micro-row">
          <span class="micro-key">spread%</span>
          <span class="micro-val neutral" id="mSpread">—</span>
        </div>
        <div class="micro-row">
          <span class="micro-key">pressure</span>
          <span class="micro-val neutral" id="mPressure">—</span>
        </div>
        <div class="micro-row">
          <span class="micro-key">pull</span>
          <span class="micro-val muted" id="mPull">—</span>
        </div>
        <div class="micro-row">
          <span class="micro-key">imbalance</span>
          <span class="micro-val neutral" id="mImbalance">—</span>
        </div>
      </div>
    </div>
  </div>

  <!-- Status bar -->
  <div id="statusbar">
    <div><span class="ws-dot" id="wsDot"></span><span id="wsStatus">connecting</span></div>
    <div id="barCount" style="color:var(--muted)"></div>
  </div>

<script>
const SYMBOL = "{symbol}";
const INITIAL_TF = "{active_tf}";
const TRADE_DATE = {json.dumps(trade_date)};
const LIVE_CANDLE_WS = {json.dumps(live_candle_ws)};
const INITIAL_CANDLES = {candles_json};

// ── EMA multipliers ──────────────────────────────────────────────────
const K9  = 2 / (9  + 1);
const K21 = 2 / (21 + 1);

// ── State ────────────────────────────────────────────────────────────
let activeTf    = INITIAL_TF;
let liveEma9    = null;
let liveEma21   = null;
let openRef     = null;   // first bar's open for day change %
let candleWs    = null;
let signalWs    = null;
let microWs     = null;
let _prevMicro  = {{ spread_pct: null, quote_pressure: null, liquidity_pull: null, bid_ask_imbalance: null }};
let ctxTimer    = null;
let activeLines = [];     // signal price lines on chart
let baseRows    = [];     // cached 1m rows — used to resample 5m/1h client-side
let liveAgg     = {{}};     // accumulates 1m ticks into the active higher-TF bucket

// ── DOM refs ─────────────────────────────────────────────────────────
const $  = id => document.getElementById(id);
const wsDot    = $("wsDot");
const wsStatus = $("wsStatus");
const barCount = $("barCount");
const priceDisplay = $("priceDisplay");
const priceChg     = $("priceChg");
const sigFeed      = $("sigFeed");

// ── Chart setup ──────────────────────────────────────────────────────
const chartEl = $("chart");

// Normalise the raw server timeframe string → "1m" / "5m" / "1h"
function normTf(raw) {{
  return String(raw)
    .replace("Timeframe.", "")
    .replace("MIN_1",  "1m")
    .replace("MIN_5",  "5m")
    .replace("HOUR_1", "1h");
}}

// All timestamps stored and transmitted as UTC Unix seconds.
// Format the time axis in America/New_York so the chart reads in ET.
const _ET_TZ = "America/New_York";

function _etTime(epoch) {{
  return new Date(epoch * 1000).toLocaleTimeString("en-US", {{
    timeZone: _ET_TZ, hour: "2-digit", minute: "2-digit", hour12: false,
  }});
}}
function _etDate(epoch) {{
  return new Date(epoch * 1000).toLocaleDateString("en-US", {{
    timeZone: _ET_TZ, month: "short", day: "numeric",
  }});
}}

const chart = LightweightCharts.createChart(chartEl, {{
  layout:   {{ background: {{ color: "#080b10" }}, textColor: "#c8d6e8" }},
  grid:     {{ vertLines: {{ color: "#111927" }}, horzLines: {{ color: "#111927" }} }},
  rightPriceScale: {{ borderColor: "#1a2235", scaleMargins: {{ top: 0.08, bottom: 0.12 }} }},
  timeScale: {{
    borderColor: "#1a2235",
    timeVisible: true,
    secondsVisible: false,
    fixLeftEdge: false,
    // tickMarkFormatter: show dates for day-level ticks, ET time for intraday
    tickMarkFormatter: (time, tickMarkType) => {{
      // TickMarkType: 0=Year 1=Month 2=DayOfMonth 3=Time 4=TimeWithSeconds
      if (tickMarkType <= 2) return _etDate(time);
      return _etTime(time);
    }},
  }},
  localization: {{
    // Crosshair tooltip time label
    timeFormatter: (time) => `${{_etDate(time)}} ${{_etTime(time)}} ET`,
  }},
  crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
  handleScroll: true,
  handleScale: true,
}});

// Series
function mkCandle() {{
  const opts = {{
    upColor:        "#00e676",
    downColor:      "#ff4560",
    borderUpColor:  "#00e676",
    borderDownColor:"#ff4560",
    wickUpColor:    "#00e676",
    wickDownColor:  "#ff4560",
  }};
  if (chart.addCandlestickSeries) return chart.addCandlestickSeries(opts);
  return chart.addSeries(LightweightCharts.CandlestickSeries, opts);
}}
function mkLine(color, width, dashed) {{
  const opts = {{ color, lineWidth: width, priceLineVisible: false, lastValueVisible: false,
                  lineStyle: dashed ? LightweightCharts.LineStyle.Dashed : LightweightCharts.LineStyle.Solid }};
  if (chart.addLineSeries) return chart.addLineSeries(opts);
  return chart.addSeries(LightweightCharts.LineSeries, opts);
}}

const candleSeries = mkCandle();
const vwapSeries   = mkLine("#00d4ff", 1.5, false);
const ema9Series   = mkLine("#ffb300", 1,   false);
const ema21Series  = mkLine("#b388ff", 1,   false);

// Crosshair legend
chart.subscribeCrosshairMove(param => {{
  if (!param.time || !param.seriesData) return;
  const c = param.seriesData.get(candleSeries);
  const v = param.seriesData.get(vwapSeries);
  const e9 = param.seriesData.get(ema9Series);
  const e21 = param.seriesData.get(ema21Series);
  if (c) $("legOHLC").textContent =
    `O ${{c.open?.toFixed(2)}}  H ${{c.high?.toFixed(2)}}  L ${{c.low?.toFixed(2)}}  C ${{c.close?.toFixed(2)}}`;
  if (v?.value)  {{ $("legVwap").textContent  = v.value.toFixed(2); }}
  if (e9?.value) {{ $("legEma9").textContent  = e9.value.toFixed(2); }}
  if (e21?.value){{ $("legEma21").textContent = e21.value.toFixed(2); }}
}});

// Resize
const ro = new ResizeObserver(() => {{
  chart.applyOptions({{ width: chartEl.clientWidth, height: chartEl.clientHeight }});
}});
ro.observe(chartEl);

// ── Helpers ──────────────────────────────────────────────────────────
function p(v, d=2) {{ return typeof v === "number" ? v.toFixed(d) : "—"; }}

function sanitize(o, h, l, c) {{
  const [on, hn, ln, cn] = [+o, +h, +l, +c];
  if (![on,hn,ln,cn].every(Number.isFinite)) return null;
  const hi = Math.max(hn, on, cn);
  const lo = Math.min(ln, on, cn);
  const eps = Math.max(0.01, Math.abs(cn) * 1e-5);
  return {{ open: on, high: hi > lo ? hi : cn+eps, low: hi > lo ? lo : cn-eps, close: cn }};
}}

function toEpoch(ts) {{ return Math.floor(new Date(ts).getTime() / 1000); }}

function buildRows(raw) {{
  if (!Array.isArray(raw) || !raw.length) return [];
  const rows = raw.flatMap(c => {{
    const t = toEpoch(c.timestamp);
    if (!t || t <= 0) return [];
    const ohlc = sanitize(c.open, c.high, c.low, c.close);
    if (!ohlc) return [];
    return [{{ time: t, ...ohlc,
               vwap:    +c.vwap    || 0,
               volume:  +c.volume  || 0,
               session: String(c.session || "").toLowerCase() }}];
  }});
  rows.sort((a,b) => a.time - b.time);
  // dedup: later row wins on same timestamp
  const out = [];
  let last = null;
  for (const r of rows) {{
    if (r.time === last) {{ out[out.length-1] = r; }}
    else {{ out.push(r); last = r.time; }}
  }}
  return out;
}}

/**
 * Aggregate 1m rows into a coarser timeframe by bucketing on UTC period boundaries.
 * VWAP is volume-weighted across component bars; session is taken from the period's
 * first (open) bar so markers show the dominant session label.
 */
function resampleRows(rows, periodSec) {{
  if (!rows.length) return [];
  const buckets = new Map();
  for (const r of rows) {{
    const t = Math.floor(r.time / periodSec) * periodSec;
    const b = buckets.get(t);
    const vol = r.volume || 1;
    if (!b) {{
      buckets.set(t, {{ time: t,
        open: r.open, high: r.high, low: r.low, close: r.close,
        vwap: r.vwap, volume: vol, _tvp: r.vwap * vol,
        session: r.session }});
    }} else {{
      b.high   = Math.max(b.high, r.high);
      b.low    = Math.min(b.low,  r.low);
      b.close  = r.close;
      b.volume += vol;
      b._tvp   += r.vwap * vol;
      b.vwap   = b._tvp / b.volume;
      // keep the first bar's session as bucket label
    }}
  }}
  return Array.from(buckets.values())
    .sort((a, b) => a.time - b.time)
    .map(({{ _tvp, ...r }}) => r);
}}

function computeEma(rows, k) {{
  let ema = null;
  return rows.map(r => {{
    ema = ema === null ? r.close : r.close * k + ema * (1 - k);
    return {{ time: r.time, value: ema }};
  }});
}}

// Per-session candle colour overrides (dimmed for pre/AH; RTH uses series defaults)
const SESSION_STYLE = {{
  premarket:  {{
    up: {{ color:"#0d2b1a", borderColor:"#1e5c36", wickColor:"#2a4a5e" }},
    dn: {{ color:"#2b0d1a", borderColor:"#5c1e36", wickColor:"#2a4a5e" }},
  }},
  afterhours: {{
    up: {{ color:"#0d1726", borderColor:"#1e3660", wickColor:"#2a3a6e" }},
    dn: {{ color:"#1a0d26", borderColor:"#361e60", wickColor:"#2a3a6e" }},
  }},
}};

const SESSION_MARKER = {{
  premarket:  {{ text:"PRE", color:"#4e6282", position:"belowBar", shape:"arrowUp" }},
  rth:        {{ text:"RTH", color:"#00d4ff", position:"belowBar", shape:"arrowUp" }},
  afterhours: {{ text:"AH",  color:"#b388ff", position:"belowBar", shape:"arrowUp" }},
}};

function applyData(rows) {{
  // Always clear — prevents stale TF data hanging around when new load is empty
  candleSeries.setData([]);
  vwapSeries.setData([]);
  ema9Series.setData([]);
  ema21Series.setData([]);
  if (typeof candleSeries.setMarkers === "function") candleSeries.setMarkers([]);

  if (!rows.length) {{
    barCount.textContent = `0 bars · ${{activeTf}}`;
    return;
  }}

  // ── Candles with per-session colour ─────────────────────────────────
  const candleData = rows.map(r => {{
    const st = SESSION_STYLE[r.session];
    const isUp = r.close >= r.open;
    const extra = st ? (isUp ? st.up : st.dn) : {{}};
    return {{ time:r.time, open:r.open, high:r.high, low:r.low, close:r.close, ...extra }};
  }});
  candleSeries.setData(candleData);

  // ── Session-transition markers ───────────────────────────────────────
  const markers = [];
  let lastSess = null;
  for (const r of rows) {{
    if (r.session && r.session !== lastSess) {{
      const m = SESSION_MARKER[r.session];
      if (m) markers.push({{ time: r.time, ...m, size: 0.6 }});
      lastSess = r.session;
    }}
  }}
  if (markers.length && typeof candleSeries.setMarkers === "function") {{
    candleSeries.setMarkers(markers);
  }}

  // ── Overlay lines ────────────────────────────────────────────────────
  vwapSeries.setData(rows.map(r => ({{ time: r.time, value: r.vwap }})));
  const e9  = computeEma(rows, K9);
  const e21 = computeEma(rows, K21);
  ema9Series.setData(e9);
  ema21Series.setData(e21);

  // Seed live EMA state and UI
  liveEma9  = e9.length  ? e9[e9.length-1].value   : null;
  liveEma21 = e21.length ? e21[e21.length-1].value : null;
  openRef = rows[0].open;

  const last = rows[rows.length-1];
  $("legVwap").textContent  = p(last.vwap);
  $("legEma9").textContent  = p(liveEma9);
  $("legEma21").textContent = p(liveEma21);
  $("indVwap").textContent  = p(last.vwap);
  $("indEma9").textContent  = p(liveEma9);
  $("indEma21").textContent = p(liveEma21);
  updatePriceDisplay(last.close);
  barCount.textContent = `${{rows.length}} bars · ${{activeTf}}`;
  chart.timeScale().fitContent();
}}

function updatePriceDisplay(close) {{
  priceDisplay.textContent = close.toFixed(2);
  if (openRef !== null) {{
    const chg = close - openRef;
    const pct = (chg / openRef * 100);
    const sign = chg >= 0 ? "+" : "";
    priceChg.textContent  = `${{sign}}${{chg.toFixed(2)}} (${{sign}}${{pct.toFixed(2)}}%)`;
    priceChg.className    = "price-chg " + (chg >= 0 ? "pos" : "neg");
  }}
}}

// ── Timeframe switch ──────────────────────────────────────────────────
document.querySelectorAll(".tf-btn").forEach(btn => {{
  if (btn.dataset.tf === activeTf) btn.classList.add("active");
  btn.addEventListener("click", async () => {{
    const tf = btn.dataset.tf;
    if (tf === activeTf) return;
    activeTf = tf;
    liveEma9 = liveEma21 = openRef = null;
    liveAgg = {{}};
    document.querySelectorAll(".tf-btn").forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    // clear signal lines
    activeLines.forEach(l => chart.removePriceLine(l));
    activeLines = [];
    barCount.textContent = "loading…";
    await loadTf(tf);
    // reconnect WS with new tf filter
    reconnectCandleWs();
  }});
}});

// ── Data loading ──────────────────────────────────────────────────────

/** Ensure baseRows (1m) is populated. Returns the rows. */
async function ensureBase() {{
  if (baseRows.length) return baseRows;
  try {{
    if (INITIAL_TF === "1m" && INITIAL_CANDLES.length) {{
      baseRows = buildRows(INITIAL_CANDLES);
    }} else {{
      const res = await fetch(`/candles/${{SYMBOL}}?timeframe=1m&date=${{encodeURIComponent(TRADE_DATE)}}&limit=2000`);
      if (res.ok) baseRows = buildRows(await res.json());
    }}
  }} catch(e) {{ console.error("ensureBase", e); }}
  return baseRows;
}}

async function loadTf(tf) {{
  let rows = [];
  try {{
    if (tf === "1m") {{
      // Use embedded data on first load, then fetch on TF-switch back
      if (INITIAL_TF === "1m" && INITIAL_CANDLES.length && !baseRows.length) {{
        rows = buildRows(INITIAL_CANDLES);
      }} else {{
        const res = await fetch(`/candles/${{SYMBOL}}?timeframe=1m&date=${{encodeURIComponent(TRADE_DATE)}}&limit=2000`);
        if (res.ok) rows = buildRows(await res.json());
      }}
      baseRows = rows;   // cache for higher-TF resampling

    }} else if (tf === "5m") {{
      // Prefer live DB bars; fall back to client-side resample if sparse
      const res = await fetch(`/candles/${{SYMBOL}}?timeframe=5m&date=${{encodeURIComponent(TRADE_DATE)}}&limit=2000`);
      if (res.ok) {{
        const j = await res.json();
        if (j.length >= 2) rows = buildRows(j);
      }}
      if (rows.length < 2) {{
        // Bootstrap only stores 1m; resample on the fly
        rows = resampleRows(await ensureBase(), 300);
      }}

    }} else if (tf === "1h") {{
      // 1h bars are never persisted — always build from 1m
      rows = resampleRows(await ensureBase(), 3600);

    }} else {{
      // Fallback for any future timeframe
      const res = await fetch(`/candles/${{SYMBOL}}?timeframe=${{tf}}&date=${{encodeURIComponent(TRADE_DATE)}}&limit=2000`);
      if (res.ok) rows = buildRows(await res.json());
    }}
  }} catch(e) {{ console.error("loadTf", e); }}
  applyData(rows);
}}

// ── Context ───────────────────────────────────────────────────────────
function setCtx(id, val) {{
  const el = $(id);
  if (!el) return;
  if (val === null || val === undefined) {{ el.textContent = "—"; el.className = "ctx-val"; return; }}
  if (typeof val === "boolean") {{
    el.textContent = val ? "yes" : "no";
    el.className = "ctx-val " + (val ? "true" : "false");
  }} else {{
    const n = +val;
    el.textContent = Number.isFinite(n) ? (n >= 0 ? "+" : "") + n.toFixed(2) + "%" : String(val);
    el.className = "ctx-val " + (n >= 0 ? "pos" : "neg");
  }}
}}

function renderContext(ctx) {{
  setCtx("cAboveVwap", ctx.above_vwap);
  setCtx("cVwapDist",  ctx.vwap_distance_pct);
  setCtx("cCrossUp",   ctx.vwap_cross_up);
  setCtx("cCrossDn",   ctx.vwap_cross_down);
  setCtx("cOrbHold",   ctx.orb_hold);
  setCtx("cOrbUp",     ctx.orb_breakout_up);
  setCtx("cDriveUp",   ctx.opening_drive_up);
  setCtx("cDriveDn",   ctx.opening_drive_down);
  setCtx("cHH",        ctx.higher_highs);
  setCtx("cLL",        ctx.lower_lows);
  // ORB levels from indicators if available
  const ind = ctx.indicators;
  if (ind) {{
    $("indOrbH").textContent = ind.opening_range_high ? p(ind.opening_range_high) : "—";
    $("indOrbL").textContent = ind.opening_range_low  ? p(ind.opening_range_low)  : "—";
  }}
}}

async function pollContext() {{
  try {{
    const res = await fetch(`/context/${{SYMBOL}}`);
    if (res.ok) renderContext(await res.json());
  }} catch(_) {{}}
  ctxTimer = setTimeout(pollContext, 5000);
}}

// ── Signal feed ───────────────────────────────────────────────────────
function addSignal(msg) {{
  const dir = msg.direction?.toLowerCase();
  const t   = new Date(msg.timestamp).toLocaleTimeString("en-US", {{ hour:"2-digit", minute:"2-digit" }});
  const row = document.createElement("div");
  row.className = `sig-row ${{dir}}`;
  row.innerHTML = `
    <span class="sig-time">${{t}}</span>
    <span class="sig-badge ${{dir}}">${{dir?.toUpperCase()}}</span>
    <span>e${{p(msg.entry_price)}} · s${{p(msg.stop_price)}} · t${{p(msg.target_price)}} · ${{p(msg.risk_reward,1)}}R</span>
  `;
  sigFeed.prepend(row);
  while (sigFeed.children.length > 8) sigFeed.removeChild(sigFeed.lastChild);

  // Add price lines to chart
  if (typeof candleSeries.createPriceLine === "function") {{
    const isLong = dir === "long";
    const mkLine = (price, color, title) => {{
      const l = candleSeries.createPriceLine({{
        price, color, lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true, title,
      }});
      activeLines.push(l);
    }};
    mkLine(msg.entry_price, "#00e676",  "ENT");
    mkLine(msg.stop_price,  "#ff4560",  "STP");
    if (msg.target_price) mkLine(msg.target_price, "#00d4ff", "TGT");
  }}
}}

// ── WebSocket: candles ────────────────────────────────────────────────
function setWsState(state) {{
  if (state === "live") {{
    wsDot.className = "ws-dot live";
    wsStatus.textContent = "live";
  }} else {{
    wsDot.className = "ws-dot";
    wsStatus.textContent = state;
  }}
}}

// Map tf → ws timeframe string from server
const TF_WS = {{ "1m": "1m", "5m": "5m", "1h": "1h" }};

function connectCandleWs() {{
  if (!LIVE_CANDLE_WS) {{
    setWsState("historical");
    return;
  }}
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${{proto}}://${{location.host}}/ws/candles`);
  candleWs = ws;

  ws.onopen  = () => setWsState("live");
  ws.onclose = () => {{
    setWsState("reconnecting");
    if (candleWs === ws) setTimeout(connectCandleWs, 1500);
  }};
  ws.onerror = () => ws.close();

  ws.onmessage = evt => {{
    const msg = JSON.parse(evt.data);
    if (msg.type !== "candle" || msg.symbol !== SYMBOL) return;

    const msgTf  = normTf(msg.timeframe);
    const is1m   = msgTf === "1m";
    const isExact = msgTf === activeTf;

    // Always accept 1m ticks (used for live aggregation into 5m/1h).
    // Also accept an exact-TF match if the server happens to emit one.
    if (!is1m && !isExact) return;

    const t = toEpoch(msg.timestamp);
    const ohlc = sanitize(msg.open, msg.high, msg.low, msg.close);
    if (!ohlc || !t) return;

    let barTime = t;
    let barOhlc = ohlc;
    let barVwap = +msg.vwap;

    if (is1m && activeTf !== "1m") {{
      // Fold the 1m tick into the current higher-TF bucket
      const periodSec = activeTf === "5m" ? 300 : activeTf === "1h" ? 3600 : 0;
      if (!periodSec) return;
      barTime = Math.floor(t / periodSec) * periodSec;
      if (!liveAgg[barTime]) {{
        liveAgg[barTime] = {{
          open: ohlc.open, high: ohlc.high, low: ohlc.low, close: ohlc.close,
          tvp: barVwap, vol: 1,
        }};
        // Keep at most 2 buckets so memory stays bounded
        const keys = Object.keys(liveAgg).map(Number).sort((a,b)=>a-b);
        if (keys.length > 2) delete liveAgg[keys[0]];
      }} else {{
        const b = liveAgg[barTime];
        b.high  = Math.max(b.high, ohlc.high);
        b.low   = Math.min(b.low,  ohlc.low);
        b.close = ohlc.close;
        b.tvp  += barVwap;
        b.vol  += 1;
      }}
      const b = liveAgg[barTime];
      barOhlc = {{ open: b.open, high: b.high, low: b.low, close: b.close }};
      barVwap = b.tvp / b.vol;
    }}

    try {{ candleSeries.update({{ time: barTime, ...barOhlc }}); }} catch(_) {{}}
    try {{ vwapSeries.update({{ time: barTime, value: barVwap }}); }} catch(_) {{}}

    const c = barOhlc.close;
    liveEma9  = liveEma9  === null ? c : c * K9  + liveEma9  * (1 - K9);
    liveEma21 = liveEma21 === null ? c : c * K21 + liveEma21 * (1 - K21);
    try {{ ema9Series.update({{ time: barTime, value: liveEma9 }}); }}  catch(_) {{}}
    try {{ ema21Series.update({{ time: barTime, value: liveEma21 }}); }} catch(_) {{}}

    // Update live indicator panel
    $("indVwap").textContent  = p(barVwap);
    $("indEma9").textContent  = p(liveEma9);
    $("indEma21").textContent = p(liveEma21);
    $("legVwap").textContent  = p(barVwap);
    $("legEma9").textContent  = p(liveEma9);
    $("legEma21").textContent = p(liveEma21);
    updatePriceDisplay(c);
  }};
}}

function reconnectCandleWs() {{
  if (!LIVE_CANDLE_WS) return;
  if (candleWs) {{ const old = candleWs; candleWs = null; old.close(); }}
  connectCandleWs();
}}

// ── WebSocket: microstructure ─────────────────────────────────────────
function updateMicro(data) {{
  const EPS = 0.001;

  // spread%
  const sp = data.spread_pct;
  if (_prevMicro.spread_pct === null || Math.abs((sp ?? 0) - (_prevMicro.spread_pct ?? 0)) > EPS) {{
    const el = $("mSpread");
    el.textContent = typeof sp === "number" ? sp.toFixed(3) + "%" : "—";
    el.className = "micro-val " + (sp === null || sp === undefined ? "neutral" : sp < 0.03 ? "good" : sp <= 0.05 ? "warn" : "bad");
    _prevMicro.spread_pct = sp;
  }}

  // quote pressure
  const qp = data.quote_pressure;
  if (_prevMicro.quote_pressure === null || Math.abs((qp ?? 0) - (_prevMicro.quote_pressure ?? 0)) > EPS) {{
    const el = $("mPressure");
    const prevSign = _prevMicro.quote_pressure !== null ? Math.sign(_prevMicro.quote_pressure) : 0;
    el.textContent = typeof qp === "number" ? (qp >= 0 ? "+" : "") + qp.toFixed(3) : "—";
    el.className = "micro-val " + (qp === null || qp === undefined ? "neutral" : qp > 0.1 ? "good" : qp < -0.1 ? "bad" : "neutral");
    if (_prevMicro.quote_pressure !== null && typeof qp === "number" && Math.sign(qp) !== prevSign) {{
      el.classList.add("pressure-flash");
      setTimeout(() => el.classList.remove("pressure-flash"), 600);
    }}
    _prevMicro.quote_pressure = qp;
  }}

  // liquidity pull
  const pull = data.liquidity_pull;
  if (pull !== _prevMicro.liquidity_pull) {{
    const el = $("mPull");
    el.textContent = pull ? "YES" : "no";
    el.className = "micro-val " + (pull ? "bad" : "muted");
    if (pull) {{
      const panel = $("microPanel");
      panel.classList.remove("flash-alert");
      void panel.offsetWidth;  // force reflow to restart animation
      panel.classList.add("flash-alert");
      setTimeout(() => panel.classList.remove("flash-alert"), 2000);
    }}
    _prevMicro.liquidity_pull = pull;
  }}

  // bid/ask imbalance
  const imb = data.bid_ask_imbalance;
  if (_prevMicro.bid_ask_imbalance === null || Math.abs((imb ?? 0) - (_prevMicro.bid_ask_imbalance ?? 0)) > EPS) {{
    const el = $("mImbalance");
    el.textContent = typeof imb === "number" ? (imb >= 0 ? "+" : "") + imb.toFixed(3) : "—";
    el.className = "micro-val " + (imb === null || imb === undefined ? "neutral" : imb > 0 ? "good" : imb < 0 ? "bad" : "neutral");
    _prevMicro.bid_ask_imbalance = imb;
  }}
}}

function connectMicroWs() {{
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${{proto}}://${{location.host}}/ws/microstructure`);
  microWs = ws;
  ws.onmessage = evt => {{
    const msg = JSON.parse(evt.data);
    if (msg.type === "microstructure" && msg.symbol === SYMBOL) updateMicro(msg);
  }};
  ws.onclose = () => {{ if (microWs === ws) setTimeout(connectMicroWs, 1500); }};
  ws.onerror = () => ws.close();
}}

// ── WebSocket: signals ────────────────────────────────────────────────
function connectSignalWs() {{
  const proto = location.protocol === "https:" ? "wss" : "ws";
  const ws = new WebSocket(`${{proto}}://${{location.host}}/ws/signals`);
  signalWs = ws;
  ws.onmessage = evt => {{
    const msg = JSON.parse(evt.data);
    if (msg.type === "signal" && msg.symbol === SYMBOL) addSignal(msg);
  }};
  ws.onclose = () => {{ if (signalWs === ws) setTimeout(connectSignalWs, 1500); }};
  ws.onerror = () => ws.close();
}}

// ── Boot ──────────────────────────────────────────────────────────────
(async () => {{
  await loadTf(INITIAL_TF);
  if (LIVE_CANDLE_WS) connectCandleWs();
  else setWsState("historical");
  connectSignalWs();
  connectMicroWs();
  pollContext();
}})();
</script>
</body>
</html>"""