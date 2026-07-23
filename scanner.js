"use strict";

/**
 * Signal Scan — Coil / Breakout Scanner (headless, Node.js) — MID TIER
 * ------------------------------------------------------------------
 * Ported from the browser version (index.html). Connects to Deriv's
 * public websocket feed, tracks Volatility indices for the Impulse ->
 * Coil -> Breakout pattern, and pushes alerts to a Telegram chat
 * instead of rendering cards in a browser.
 *
 * This build is hardcoded to the Mid volatility tier (see WHITELIST
 * below) rather than reading a TREND_BIAS-style env var — one tier per
 * deployment. Trend detection uses the same signed EMA/ADX scoring
 * model as the canonical TrendEngine (price vs EMA200, EMA20 vs EMA50,
 * EMA50 vs EMA200, EMA200 5-bar slope, and ADX(14)/DMI direction when
 * ADX > 25), not a plain binary AND-stack.
 *
 * Required environment variables (set these in Railway's Variables tab):
 *   TELEGRAM_BOT_TOKEN   - token from @BotFather
 *   TELEGRAM_CHAT_ID     - your chat id (message @userinfobot, or use a group/channel id)
 *
 * Optional environment variables (all have sane defaults):
 *   TIMEFRAME              - M5 | M15 | M30 | H1   (default M15)
 *   TREND_BIAS             - off | h4h1 | h4h1mixed | h4only | h1only  (default h4h1)
 *   PORT                   - health-check HTTP port (Railway sets this automatically)
 *   EMA20_LEN              - default 20
 *   EMA50_LEN              - default 50
 *   EMA200_LEN             - default 200
 *   ATR_PERIOD             - default 14
 *   COIL_LOOKBACK          - bars in the coil box window (default 8)
 *   COMPRESSION_ATR_MULT   - max box height as a multiple of ATR (default 2.0)
 *   HUG_RATIO              - EMA-near-edge threshold for anticipated direction (default 0.35)
 *   IMPULSE_LOOKBACK       - bars checked before the box for the prior impulse (default 12)
 *   IMPULSE_ATR_MULT       - min impulse move as a multiple of ATR (default 2.5)
 *   BREAKOUT_ATR_MULT      - buffer beyond the box edge as a multiple of ATR (default 0.15)
 *   BREAKOUT_EXPANSION_MULT- min breakout candle range vs coil avg range (default 1.6)
 *   MAX_COIL_BARS          - bars a matured coil can go stale before resetting (default 12)
 *   TP_ATR_MULT            - take-profit distance as a multiple of ATR (default 2.0)
 *   SL_ATR_MULT            - stop-loss distance as a multiple of ATR (default 1.0)
 *   MAX_BARS_TRACK         - bars to track an open position before timeout (default 20)
 */

const http = require("http");
const https = require("https");
const WebSocket = require("ws");

// ==========================================================================
// CONFIG
// ==========================================================================
// Small helper: parse an env var as a positive number, falling back to a
// default if it's unset, empty, or not a valid number.
function envNum(name, fallback) {
  const v = parseFloat(process.env[name]);
  return Number.isFinite(v) && v > 0 ? v : fallback;
}

const VALID_TREND_BIAS = ["off", "h4h1", "h4h1mixed", "h4only", "h1only"];

const DEFAULTS = {
  timeframe: process.env.TIMEFRAME || "M15",
  ema20: envNum("EMA20_LEN", 20),
  ema50: envNum("EMA50_LEN", 50),
  ema200: envNum("EMA200_LEN", 200),
  atrPeriod: envNum("ATR_PERIOD", 14),
  coilLookback: envNum("COIL_LOOKBACK", 8),
  compressionAtrMult: envNum("COMPRESSION_ATR_MULT", 2.0),
  hugRatio: envNum("HUG_RATIO", 0.35),
  impulseLookback: envNum("IMPULSE_LOOKBACK", 12),
  impulseAtrMult: envNum("IMPULSE_ATR_MULT", 2.5),
  breakoutAtrMult: envNum("BREAKOUT_ATR_MULT", 0.15),
  breakoutExpansionMult: envNum("BREAKOUT_EXPANSION_MULT", 1.6),
  maxCoilBars: envNum("MAX_COIL_BARS", 12),
  tpAtrMult: envNum("TP_ATR_MULT", 2.0),
  slAtrMult: envNum("SL_ATR_MULT", 1.0),
  maxBarsTrack: envNum("MAX_BARS_TRACK", 20),
  // H4 + H1 strict alignment ('h4h1') is now the default trend bias: both
  // H4 and H1 must be clean (non-mixed) and agree with each other and with
  // the breakout direction for a signal to be allowed.
  trendBias: VALID_TREND_BIAS.includes(process.env.TREND_BIAS) ? process.env.TREND_BIAS : "h4h1",
};
const CFG = { ...DEFAULTS };

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
  console.error(
    "[fatal] TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set as environment variables."
  );
  process.exit(1);
}

const GRAN = { M5: 300, M15: 900, M30: 1800, H1: 3600, H4: 14400 };
const HISTORY_COUNT = 300;

function primaryGranularity() {
  return GRAN[CFG.timeframe] || GRAN.M15;
}
function neededGranularities() {
  return [...new Set([primaryGranularity(), GRAN.H1, GRAN.H4])];
}

// Mid-tier scope: this headless build is hardcoded to the Mid volatility
// tier (Volatility 50/75/100, Volatility 50/75/90/100 (1s)) rather than
// reading a TREND_BIAS-style env var, since this process only ever runs
// one tier per deployment. These values are Mid-tier-only and diverge
// INTENTIONALLY from the browser twin (index.html), which still exposes
// the full symbol set — do not silently re-widen this list when porting
// changes between the two files.
const WHITELIST = {
  volatility: [50, 75, 100],
  volatilityOneSec: [50, 75, 90, 100],
};

// ==========================================================================
// TELEGRAM
// ==========================================================================
// Simple queue with a small delay between sends so bursts of signals across
// many symbols don't trip Telegram's rate limits.
let tgQueue = [];
let tgRunning = false;

function sendTelegram(text) {
  tgQueue.push(text);
  runTelegramQueue();
}

function runTelegramQueue() {
  if (tgRunning) return;
  tgRunning = true;
  const step = () => {
    if (tgQueue.length === 0) {
      tgRunning = false;
      return;
    }
    const text = tgQueue.shift();
    const payload = JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text,
      parse_mode: "HTML",
      disable_web_page_preview: true,
    });
    const req = https.request(
      {
        hostname: "api.telegram.org",
        path: `/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => {
          if (res.statusCode !== 200) {
            console.error("[telegram error]", res.statusCode, body);
          }
        });
      }
    );
    req.on("error", (err) => console.error("[telegram request error]", err.message));
    req.write(payload);
    req.end();
    setTimeout(step, 350);
  };
  step();
}

// ==========================================================================
// SYMBOL RESOLUTION
// ==========================================================================
function classifySymbol(entry) {
  const name = entry.underlying_symbol_name || entry.display_name || "";
  const market = entry.market || "";
  if (market !== "synthetic_index") return null;
  let m;
  if ((m = name.match(/^Volatility\s+(\d+(?:\.\d+)?)\s*\(1s\)\s*Index$/i))) {
    return { type: "volatility", oneSec: true, num: parseFloat(m[1]) };
  }
  if ((m = name.match(/^Volatility\s+(\d+(?:\.\d+)?)\s*Index$/i))) {
    return { type: "volatility", oneSec: false, num: parseFloat(m[1]) };
  }
  return null;
}

function buildSymbolList(activeSymbols) {
  const found = { volatility: [] };
  activeSymbols.forEach((entry) => {
    const c = classifySymbol(entry);
    if (!c || c.type !== "volatility") return;
    const item = { symbol: entry.underlying_symbol || entry.symbol, display_name: entry.underlying_symbol_name || entry.display_name, ...c };
    const whitelisted = c.oneSec
      ? WHITELIST.volatilityOneSec.includes(c.num)
      : WHITELIST.volatility.includes(c.num);
    if (whitelisted) found.volatility.push({ ...item, cat: "volatility" });
  });

  const out = [];
  const seen = new Set();
  function push(symbol, label, cat, type, num, oneSec) {
    if (seen.has(symbol)) return;
    seen.add(symbol);
    out.push({ symbol, label, cat, type, num, oneSec });
  }

  found.volatility
    .sort((a, b) => a.num - b.num || (a.oneSec === b.oneSec ? 0 : a.oneSec ? 1 : -1))
    .forEach((i) => push(i.symbol, i.display_name, "volatility", i.type, i.num, i.oneSec));

  return out;
}

function tvSymbolFor(item) {
  if (!item) return null;
  const n = item.num;
  const numStr = String(n);
  if (item.type === "volatility") {
    return item.oneSec ? `VOLATILITY_${numStr}_1S_INDEX` : `VOLATILITY_${numStr}_INDEX`;
  }
  return null;
}
function tvUrlFor(item) {
  const sym = tvSymbolFor(item);
  if (!sym) return null;
  return `https://www.tradingview.com/chart/?symbol=DERIV%3A${encodeURIComponent(sym)}`;
}

// ==========================================================================
// WEBSOCKET STATE
// ==========================================================================
let ws = null;
let SYMBOLS = [];
const DATA = {};
// The New Deriv API no longer guarantees `echo_req` on responses, so we tag
// every request with a req_id and keep a lookup here to know which
// symbol/timeframe a `candles` response belongs to.
let reqIdSeq = 1;
const pendingRequests = new Map();
let requestQueue = [];
let queueRunning = false;
let reconnectAttempts = 0;
let manualStop = false;
let pingHandle = null;
let openPositions = [];

function connect() {
  manualStop = false;
  console.log("[status] connecting...");
  // New Deriv API: public, read-only market data needs no app_id/auth —
  // just connect straight to the public WebSocket endpoint.
  ws = new WebSocket("wss://api.derivws.com/trading/v1/options/ws/public");

  ws.on("open", () => {
    const wasReconnect = reconnectAttempts > 0;
    reconnectAttempts = 0;
    console.log(wasReconnect ? "[status] reconnected" : "[status] live");
    if (SYMBOLS.length === 0) {
      wsSend({ active_symbols: "brief" });
    } else {
      SYMBOLS.forEach((s) => enqueueSymbolRequests(s.symbol));
      runQueue();
    }
    startPing();
  });

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }
    handleMessage(msg);
  });

  ws.on("error", (err) => {
    console.error("[ws error]", err.message);
  });

  ws.on("close", () => {
    stopPing();
    if (!manualStop) {
      console.log("[status] reconnecting...");
      scheduleReconnect();
    } else {
      console.log("[status] offline");
    }
  });
}

function scheduleReconnect() {
  reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.6, reconnectAttempts), 20000);
  setTimeout(() => {
    if (!manualStop) connect();
  }, delay);
}

function startPing() {
  stopPing();
  pingHandle = setInterval(() => wsSend({ ping: 1 }), 28000);
}
function stopPing() {
  if (pingHandle) {
    clearInterval(pingHandle);
    pingHandle = null;
  }
}

function wsSend(payload) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    if (payload.req_id === undefined) payload.req_id = reqIdSeq++;
    ws.send(JSON.stringify(payload));
    return payload.req_id;
  }
  return false;
}

function enqueueSymbolRequests(symbol) {
  neededGranularities().forEach((granularity) => {
    requestQueue.push({ symbol, granularity });
  });
}

function runQueue() {
  if (queueRunning) return;
  queueRunning = true;
  const step = () => {
    if (requestQueue.length === 0) {
      queueRunning = false;
      return;
    }
    const job = requestQueue.shift();
    const reqId = wsSend({
      ticks_history: job.symbol,
      style: "candles",
      granularity: job.granularity,
      count: HISTORY_COUNT,
      end: "latest",
      adjust_start_time: 1,
      subscribe: 1,
    });
    if (reqId) pendingRequests.set(reqId, { symbol: job.symbol, granularity: job.granularity });
    setTimeout(step, 220);
  };
  step();
}

// ==========================================================================
// MESSAGE HANDLING
// ==========================================================================
function handleMessage(msg) {
  // The New Deriv API no longer guarantees echo_req, so pull the pending
  // request (if any) by req_id before falling back to echo_req.
  const pending = (msg.req_id !== undefined) ? pendingRequests.get(msg.req_id) : undefined;

  if (msg.error) {
    if (pending) {
      console.error("[api error]", pending.symbol, msg.error.message);
      pendingRequests.delete(msg.req_id);
    } else {
      const req = msg.echo_req || {};
      if (req.ticks_history) {
        console.error("[api error]", req.ticks_history, msg.error.message);
      } else {
        console.error("[api error]", msg.error.message);
        manualStop = true;
        try {
          ws.close();
        } catch (e) {}
      }
    }
    return;
  }

  if (msg.msg_type === "active_symbols") {
    SYMBOLS = buildSymbolList(msg.active_symbols || []);
    SYMBOLS.forEach((s) => initSymbolData(s.symbol, s.label));
    console.log(`[status] connected — tracking ${SYMBOLS.length} instruments`);
    SYMBOLS.forEach((s) => enqueueSymbolRequests(s.symbol));
    runQueue();
    return;
  }

  if (msg.msg_type === "candles") {
    const req = msg.echo_req || {};
    const symbol = pending ? pending.symbol : req.ticks_history;
    const granularity = pending ? pending.granularity : req.granularity;
    if (msg.req_id !== undefined) pendingRequests.delete(msg.req_id);
    const sd = DATA[symbol];
    if (!sd || !msg.candles) return;
    sd.candles[granularity] = msg.candles.map((c) => ({
      epoch: c.epoch,
      open: +c.open,
      high: +c.high,
      low: +c.low,
      close: +c.close,
    }));
    recomputeTimeframe(symbol, granularity);
    return;
  }

  if (msg.msg_type === "ohlc") {
    const o = msg.ohlc;
    if (!o) return;
    const symbol = o.symbol || o.underlying_symbol;
    const granularity = +o.granularity;
    const sd = DATA[symbol];
    if (!sd) return;
    const arr = sd.candles[granularity];
    if (!arr || arr.length === 0) return;

    const last = arr[arr.length - 1];
    const candle = { epoch: +o.open_time, open: +o.open, high: +o.high, low: +o.low, close: +o.close };

    if (last.epoch === candle.epoch) {
      arr[arr.length - 1] = candle;
    } else if (candle.epoch > last.epoch) {
      arr.push(candle);
      if (arr.length > HISTORY_COUNT + 5) arr.shift();
    } else {
      return;
    }

    const justClosed = last.epoch !== candle.epoch;
    recomputeTimeframe(symbol, granularity, justClosed ? last : null);
  }
}

// ==========================================================================
// SYMBOL DATA MODEL
// ==========================================================================
function initSymbolData(symbol, label) {
  DATA[symbol] = {
    symbol,
    label,
    candles: {},
    ind: {
      h1: { ema20: null, ema50: null, ema200: null, score: 0, trend: "mixed" },
      h4: { ema20: null, ema50: null, ema200: null, score: 0, trend: "mixed" },
      primary: { ema50: null, atr: null },
    },
    price: null,
    coil: newCoilState(),
    lastSignal: null,
    openPosition: null,
  };
}

function newCoilState() {
  return {
    active: false,
    matured: false,
    boxHigh: null,
    boxLow: null,
    avgRange: null,
    staleBars: 0,
    anticipatedDirection: null, // 'buy' | 'sell' | null — where EMA50 sits inside the box
    biasSnapshot: null, // { h1, h4 } trend readings at the moment the coil matured / was gated
  };
}

// ==========================================================================
// INDICATORS
// ==========================================================================
function computeEMASeries(closes, period) {
  if (closes.length < period) return new Array(closes.length).fill(null);
  const k = 2 / (period + 1);
  const out = new Array(closes.length).fill(null);
  let seed = 0;
  for (let i = 0; i < period; i++) seed += closes[i];
  seed /= period;
  out[period - 1] = seed;
  let prev = seed;
  for (let i = period; i < closes.length; i++) {
    const val = closes[i] * k + prev * (1 - k);
    out[i] = val;
    prev = val;
  }
  return out;
}

function computeATRSeries(candles, period) {
  const n = candles.length;
  const out = new Array(n).fill(null);
  if (n < period + 1) return out;
  const tr = new Array(n).fill(null);
  for (let i = 1; i < n; i++) {
    const c = candles[i],
      p = candles[i - 1];
    tr[i] = Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close));
  }
  let seed = 0;
  for (let i = 1; i <= period; i++) seed += tr[i];
  seed /= period;
  out[period] = seed;
  let prev = seed;
  for (let i = period + 1; i < n; i++) {
    const val = (prev * (period - 1) + tr[i]) / period;
    out[i] = val;
    prev = val;
  }
  return out;
}

// Wilder ADX/DMI, ported from the canonical TrendEngine (no_wick.html):
// same seeding (simple sum over the first `period` bars), same Wilder
// smoothing recurrence thereafter, same divide-by-zero guard for flat
// candle runs (zero true range collapses +DI/-DI/DX to 0 instead of NaN).
const ADX_PERIOD = 14;

function computeADXSeries(candles, period) {
  const n = candles.length;
  const out = new Array(n).fill(null);
  if (n < period * 2 + 1) return out;

  const tr = new Array(n).fill(0);
  const plusDM = new Array(n).fill(0);
  const minusDM = new Array(n).fill(0);
  for (let i = 1; i < n; i++) {
    const c = candles[i], p = candles[i - 1];
    const upMove = c.high - p.high;
    const downMove = p.low - c.low;
    plusDM[i] = (upMove > downMove && upMove > 0) ? upMove : 0;
    minusDM[i] = (downMove > upMove && downMove > 0) ? downMove : 0;
    tr[i] = Math.max(c.high - c.low, Math.abs(c.high - p.close), Math.abs(c.low - p.close));
  }

  // Wilder seed: simple sum over the first `period` bars.
  let trSum = 0, plusDMSum = 0, minusDMSum = 0;
  for (let i = 1; i <= period; i++) {
    trSum += tr[i];
    plusDMSum += plusDM[i];
    minusDMSum += minusDM[i];
  }
  const smTR = new Array(n).fill(null);
  const smPlusDM = new Array(n).fill(null);
  const smMinusDM = new Array(n).fill(null);
  smTR[period] = trSum;
  smPlusDM[period] = plusDMSum;
  smMinusDM[period] = minusDMSum;

  for (let i = period + 1; i < n; i++) {
    smTR[i] = smTR[i - 1] - (smTR[i - 1] / period) + tr[i];
    smPlusDM[i] = smPlusDM[i - 1] - (smPlusDM[i - 1] / period) + plusDM[i];
    smMinusDM[i] = smMinusDM[i - 1] - (smMinusDM[i - 1] / period) + minusDM[i];
  }

  const plusDI = new Array(n).fill(null);
  const minusDI = new Array(n).fill(null);
  const dx = new Array(n).fill(null);
  for (let i = period; i < n; i++) {
    if (!smTR[i]) {
      // divide-by-zero guard: a run of flat/identical candles collapses
      // true range to zero, so treat DI/DX as neutral rather than NaN.
      plusDI[i] = 0; minusDI[i] = 0; dx[i] = 0;
      continue;
    }
    plusDI[i] = 100 * (smPlusDM[i] / smTR[i]);
    minusDI[i] = 100 * (smMinusDM[i] / smTR[i]);
    const diSum = plusDI[i] + minusDI[i];
    dx[i] = diSum === 0 ? 0 : 100 * Math.abs(plusDI[i] - minusDI[i]) / diSum;
  }

  // ADX seed: simple average of the first `period` DX values, then
  // Wilder-smoothed thereafter.
  const adxStart = period * 2;
  let adxPrev = null;
  let dxSum = 0;
  for (let i = period; i < adxStart; i++) dxSum += dx[i];
  adxPrev = dxSum / period;
  out[adxStart] = { adx: adxPrev, plusDI: plusDI[adxStart], minusDI: minusDI[adxStart] };
  for (let i = adxStart + 1; i < n; i++) {
    adxPrev = (adxPrev * (period - 1) + dx[i]) / period;
    out[i] = { adx: adxPrev, plusDI: plusDI[i], minusDI: minusDI[i] };
  }
  return out;
}

// Signed scoring trend model (matches the canonical TrendEngine):
//  +1/-1 price vs EMA200
//  +1/-1 EMA20 vs EMA50
//  +1/-1 EMA50 vs EMA200
//  +1/-1 EMA200 slope over 5 completed bars (epsilon = ema200 * 0.0001)
//  +1/-1 ADX(14)/DMI direction, only when ADX > 25
// trend label: 'bullish' if score >= 2, 'bearish' if score <= -2, else 'mixed'.
function trendFromEma(ema20, ema50, ema200, ema200Prev5, price, adxData) {
  if (ema20 === null || ema50 === null || ema200 === null) return { score: 0, trend: "mixed" };

  let score = 0;
  score += price > ema200 ? 1 : -1;
  score += ema20 > ema50 ? 1 : -1;
  score += ema50 > ema200 ? 1 : -1;

  if (ema200Prev5 !== null && ema200Prev5 !== undefined) {
    const epsilon = ema200 * 0.0001;
    const slope = ema200 - ema200Prev5;
    if (slope > epsilon) score += 1;
    else if (slope < -epsilon) score -= 1;
  }

  if (adxData && adxData.adx !== null && adxData.adx > 25) {
    if (adxData.plusDI > adxData.minusDI) score += 1;
    else if (adxData.plusDI < adxData.minusDI) score -= 1;
  }

  let trend;
  if (score >= 2) trend = "bullish";
  else if (score <= -2) trend = "bearish";
  else trend = "mixed";
  return { score, trend };
}

// Trend Bias is the single source of trend confirmation for trade entries.
// The selected mode (CFG.trendBias) decides how H4/H1 are read and whether
// a breakout in a given direction is permitted:
//   off        - no trend filter, every breakout is allowed.
//   h4h1       - both H4 and H1 must be clean (non-mixed) and agree with
//                each other and with the breakout direction.
//   h4h1mixed  - cascade: H4 is the primary bias; H1 only steps in if H4
//                is mixed. If both are mixed there's no bias to enforce.
//   h4only     - only H4 is checked; mixed H4 means no bias to enforce.
//   h1only     - only H1 is checked; mixed H1 means no bias to enforce.
// Returns { conflicts, bias } where `bias` identifies the resolved trend
// and `conflicts` says whether the given direction is blocked by it. This
// is evaluated once a coil matures, before the scanner is allowed to enter
// the Watching state — a setup that conflicts with the Trend Bias never
// gets a chance to breakout.
function trendGate(sd, direction) {
  const h4 = sd.ind.h4.trend, h1 = sd.ind.h1.trend;
  const mode = CFG.trendBias;
  const matches = (bias) =>
    bias !== "mixed" &&
    ((direction === "buy" && bias === "bullish") || (direction === "sell" && bias === "bearish"));

  if (mode === "off") {
    return { conflicts: false, bias: "mixed" };
  }
  if (mode === "h4h1") {
    if (h4 === "mixed" || h1 === "mixed" || h4 !== h1) {
      return { conflicts: true, bias: "mixed" };
    }
    return { conflicts: !matches(h4), bias: h4 };
  }
  if (mode === "h4only") {
    return { conflicts: h4 !== "mixed" && !matches(h4), bias: h4 };
  }
  if (mode === "h1only") {
    return { conflicts: h1 !== "mixed" && !matches(h1), bias: h1 };
  }
  // h4h1mixed (default): H4 primary, H1 fallback.
  const bias = h4 !== "mixed" ? h4 : h1;
  return { conflicts: bias !== "mixed" && !matches(bias), bias };
}

function recomputeTimeframe(symbol, granularity, justClosedCandle) {
  const sd = DATA[symbol];
  if (!sd) return;
  const candles = sd.candles[granularity];
  if (!candles || candles.length === 0) return;
  const closes = candles.map((c) => c.close);
  const lastClose = closes[closes.length - 1];

  const isPrimary = granularity === primaryGranularity();
  const isH1 = granularity === GRAN.H1;
  const isH4 = granularity === GRAN.H4;

  if (isPrimary) sd.price = lastClose;
  else if (sd.price == null) sd.price = lastClose;

  if (isH1 || isH4) {
    const e20 = computeEMASeries(closes, CFG.ema20);
    const e50b = computeEMASeries(closes, CFG.ema50);
    const e200 = computeEMASeries(closes, CFG.ema200);
    const adxSeries = computeADXSeries(candles, ADX_PERIOD);
    const i = closes.length - 1;
    const ema20 = e20[i],
      ema50 = e50b[i],
      ema200 = e200[i];
    const ema200Prev5 = i - 5 >= 0 ? e200[i - 5] : null;
    const adxData = adxSeries[i] || null;
    const key = isH1 ? "h1" : "h4";
    const { score, trend } = trendFromEma(ema20, ema50, ema200, ema200Prev5, lastClose, adxData);
    sd.ind[key] = { ema20, ema50, ema200, score, trend };
  }

  if (isPrimary) {
    const e50 = computeEMASeries(closes, CFG.ema50);
    const atrArr = computeATRSeries(candles, CFG.atrPeriod);
    const i = closes.length - 1;
    sd.ind.primary.ema50 = e50[i];
    sd.ind.primary.atr = atrArr[i];

    if (justClosedCandle) {
      const idxClosed = candles.length - 2;
      const emaAtClose = e50[idxClosed];
      const atrAtClose = atrArr[idxClosed];
      if (emaAtClose !== null && atrAtClose !== null && idxClosed >= 0) {
        evaluateCoil(symbol, candles, e50, atrArr, idxClosed);
      }
      updateOpenPositions(symbol, justClosedCandle);
    }
  }
}

// ==========================================================================
// SIGNAL STATE MACHINE — Coil / Compression Breakout
// ==========================================================================
function windowHighLow(candles, start, endInclusive) {
  let hi = -Infinity,
    lo = Infinity,
    sum = 0;
  for (let i = start; i <= endInclusive; i++) {
    const c = candles[i];
    if (c.high > hi) hi = c.high;
    if (c.low < lo) lo = c.low;
    sum += c.high - c.low;
  }
  return { hi, lo, avgRange: sum / (endInclusive - start + 1) };
}

// Returns the largest upward and downward deviation from the EMA seen in
// [start, endExclusive), tracked separately so callers can tell whether
// the impulse leading into a box was predominantly an UP move or a DOWN
// move — not just "how far did price get from the EMA".
function maxDeviationFromEma(candles, start, endExclusive, ema) {
  let up = 0,
    down = 0;
  for (let i = start; i < endExclusive; i++) {
    const c = candles[i];
    if (c.high - ema > up) up = c.high - ema;
    if (ema - c.low > down) down = ema - c.low;
  }
  return { up, down };
}

// Where EMA50 sits inside the coil box tells us which way the box is
// resting on it. EMA near the box FLOOR -> the box sits mostly ABOVE EMA,
// coiling on top of it with EMA acting as support underneath -> a BUY
// breakout (continuation) is anticipated. EMA near the box CEILING -> the
// box sits mostly BELOW EMA, coiling under it with EMA acting as
// resistance overhead -> a SELL breakout (continuation) is anticipated.
// EMA sitting roughly in the middle -> centered coil, no directional read.
function anticipatedDirectionForBox(boxLow, boxHigh, ema) {
  const height = boxHigh - boxLow;
  if (height <= 0) return null;
  const emaPos = (ema - boxLow) / height; // 0 = at box low, 1 = at box high
  if (emaPos <= CFG.hugRatio) return "buy";
  if (emaPos >= 1 - CFG.hugRatio) return "sell";
  return null;
}

// A coil's "anticipated" direction (from where EMA50 sits in the box) is
// purely geometric — it says nothing about the Trend Bias filter. This
// checks whether that anticipated direction would actually survive the
// same gate a real breakout has to pass, so the "Watching" preview only
// carries a directional call when it's one that wouldn't be filtered if
// the box broke out right now.
function evaluateCoil(symbol, candles, emaArr, atrArr, idxClosed) {
  const sd = DATA[symbol];
  const coil = sd.coil;
  const closedCandle = candles[idxClosed];
  const ema = emaArr[idxClosed];
  const atr = atrArr[idxClosed];
  if (ema === null || atr === null || atr <= 0) return;

  const boxStart = idxClosed - CFG.coilLookback + 1;
  const impulseStart = boxStart - CFG.impulseLookback;

  if (coil.matured) {
    const bufferHigh = coil.boxHigh + CFG.breakoutAtrMult * atr;
    const bufferLow = coil.boxLow - CFG.breakoutAtrMult * atr;
    const brokeUp = closedCandle.close > bufferHigh;
    const brokeDown = closedCandle.close < bufferLow;
    const candleRange = closedCandle.high - closedCandle.low;
    const expanded = coil.avgRange > 0 && candleRange >= CFG.breakoutExpansionMult * coil.avgRange;

    if ((brokeUp || brokeDown) && expanded) {
      const direction = brokeUp ? "buy" : "sell";

      if (coil.anticipatedDirection && direction !== coil.anticipatedDirection) {
        fireFiltered(symbol, direction, closedCandle, `against anticipated ${coil.anticipatedDirection.toUpperCase()} setup`);
        resetCoil(coil);
        return;
      }

      // Trend Bias was already validated before this coil was allowed to
      // enter Watching, so a breakout can never be rejected for it here.
      fireSignal(symbol, direction, closedCandle, atr, coil.biasSnapshot);
      resetCoil(coil);
      return;
    }

    coil.staleBars++;
    if (coil.staleBars > CFG.maxCoilBars) {
      resetCoil(coil);
      return;
    }
    if (boxStart >= 0) {
      const w = windowHighLow(candles, boxStart, idxClosed);
      if (w.hi - w.lo <= CFG.compressionAtrMult * atr && w.lo <= ema && ema <= w.hi) {
        const rawDir = anticipatedDirectionForBox(w.lo, w.hi, ema);
        // If the box still has a directional read but the live H1/H4 trend
        // has since drifted against it (e.g. H4 flipped to mixed while
        // this coil was Watching), discard the coil outright — same as at
        // maturation — instead of nulling anticipatedDirection. Nulling it
        // used to silently disable the breakout-direction filter below,
        // letting a breakout in EITHER direction fire unfiltered on a
        // stale bias. A centered box (rawDir === null) has no directional
        // read to conflict with, so it's left alone here.
        if (rawDir !== null && trendGate(sd, rawDir).conflicts) {
          resetCoil(coil);
          return;
        }
        coil.boxHigh = w.hi;
        coil.boxLow = w.lo;
        coil.avgRange = w.avgRange;
        coil.anticipatedDirection = rawDir;
        coil.biasSnapshot = { h1: sd.ind.h1.trend, h4: sd.ind.h4.trend };
      }
    }
    return;
  }

  if (boxStart < 0 || impulseStart < 0) return;
  const w = windowHighLow(candles, boxStart, idxClosed);
  const isTight = w.hi - w.lo <= CFG.compressionAtrMult * atr;
  const straddles = w.lo <= ema && ema <= w.hi;
  if (!isTight || !straddles) {
    coil.active = false;
    return;
  }

  const rawDir = anticipatedDirectionForBox(w.lo, w.hi, ema);

  // Rejection check: the impulse feeding into this box must have moved
  // AWAY from the EMA in the OPPOSITE direction of the box, then reversed
  // back through/toward the EMA to form the box on the other side — i.e.
  // a pullback into the EMA that gets rejected, not a move that pauses on
  // the same side it's already on. A sell setup (box resting under the
  // EMA, EMA as resistance overhead) requires the impulse to have been a
  // strong move UP away from the EMA that then reversed down into the
  // box. A buy setup (box resting above the EMA, EMA as support beneath)
  // requires the impulse to have been a strong move DOWN that then
  // reversed up into the box. A centered box (rawDir === null) has no
  // directional read, so either side of a qualifying impulse still
  // counts, same as before.
  const dev = maxDeviationFromEma(candles, impulseStart, boxStart, ema);
  let impulseOk;
  if (rawDir === "sell") {
    impulseOk = dev.up >= CFG.impulseAtrMult * atr;
  } else if (rawDir === "buy") {
    impulseOk = dev.down >= CFG.impulseAtrMult * atr;
  } else {
    impulseOk = Math.max(dev.up, dev.down) >= CFG.impulseAtrMult * atr;
  }
  if (!impulseOk) {
    coil.active = false;
    return;
  }

  // Trend Check happens here, before Watching is entered. If the setup has
  // a directional read and it conflicts with the selected Trend Bias, the
  // setup is discarded outright — no Watching, no notification, no further
  // monitoring of this coil. The scanner simply waits for the next valid
  // Impulse + Coil.
  if (rawDir !== null && trendGate(sd, rawDir).conflicts) {
    resetCoil(coil);
    return;
  }

  coil.active = true;
  coil.matured = true;
  coil.staleBars = 0;
  coil.boxHigh = w.hi;
  coil.boxLow = w.lo;
  coil.avgRange = w.avgRange;
  coil.anticipatedDirection = rawDir;
  coil.biasSnapshot = { h1: sd.ind.h1.trend, h4: sd.ind.h4.trend };
  firePreview(symbol, closedCandle, coil);
}

function resetCoil(coil) {
  coil.active = false;
  coil.matured = false;
  coil.boxHigh = null;
  coil.boxLow = null;
  coil.avgRange = null;
  coil.staleBars = 0;
  coil.anticipatedDirection = null;
  coil.biasSnapshot = null;
}

function priceDigits(price) {
  return price < 10 ? 5 : price < 1000 ? 3 : 2;
}

function symbolMeta(symbol) {
  return SYMBOLS.find((s) => s.symbol === symbol);
}

function chartLink(symbol) {
  const item = symbolMeta(symbol);
  const url = item ? tvUrlFor(item) : null;
  return url ? `\n<a href="${url}">Chart</a>` : "";
}

function firePreview(symbol, closedCandle, coil) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const price = closedCandle.close.toFixed(digits);
  const hug = coil.anticipatedDirection === "buy" ? "Coiling above EMA · BUY" : coil.anticipatedDirection === "sell" ? "Coiling below EMA · SELL" : null;
  console.log(`[watching] ${sd.label} @ ${price} box ${coil.boxLow.toFixed(digits)}–${coil.boxHigh.toFixed(digits)}${hug ? ` (${hug})` : ""}`);
  sendTelegram(
    `👀 <b>Watching</b> — ${sd.label}\n` +
      `Price: ${price}\n` +
      `Box: ${coil.boxLow.toFixed(digits)}–${coil.boxHigh.toFixed(digits)}` +
      (hug ? `\n${hug}` : "") +
      `\nH1/H4 bias: ${coil.biasSnapshot.h1}/${coil.biasSnapshot.h4}` +
      chartLink(symbol)
  );
}

function fireFiltered(symbol, direction, closedCandle, reason) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const price = closedCandle.close.toFixed(digits);
  console.log(`[filtered] ${sd.label} ${direction} @ ${price} — ${reason}`);
  sendTelegram(
    `🚫 <b>Filtered breakout</b> — ${sd.label}\n` +
      `${direction.toUpperCase()} — ${reason}\n` +
      `Price: ${price}` +
      chartLink(symbol)
  );
}

function fireSignal(symbol, direction, closedCandle, atr, biasSnapshot) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const entry = closedCandle.close;
  const price = entry.toFixed(digits);
  sd.lastSignal = { direction, price, time: Date.now() };
  const bias = biasSnapshot || { h1: sd.ind.h1.trend, h4: sd.ind.h4.trend };

  const tp = direction === "buy" ? entry + CFG.tpAtrMult * atr : entry - CFG.tpAtrMult * atr;
  const sl = direction === "buy" ? entry - CFG.slAtrMult * atr : entry + CFG.slAtrMult * atr;

  const emoji = direction === "buy" ? "🟢" : "🔴";
  console.log(`[signal] ${direction.toUpperCase()} ${sd.label} @ ${price} TP ${tp.toFixed(digits)} SL ${sl.toFixed(digits)}`);
  sendTelegram(
    `${emoji} <b>${direction.toUpperCase()} signal</b> — ${sd.label}\n` +
      `Entry: ${price}\n` +
      `TP: ${tp.toFixed(digits)}\n` +
      `SL: ${sl.toFixed(digits)}\n` +
      `H1/H4 bias: ${bias.h1}/${bias.h4}` +
      chartLink(symbol)
  );

  const pos = { symbol, direction, entry, tp, sl, mfe: 0, mae: 0, bars: 0 };
  openPositions.push(pos);
  sd.openPosition = pos;
}

function updateOpenPositions(symbol, closedCandle) {
  if (openPositions.length === 0) return;
  const sd = DATA[symbol];
  const { high, low } = closedCandle;
  const remaining = [];
  for (const pos of openPositions) {
    if (pos.symbol !== symbol) {
      remaining.push(pos);
      continue;
    }

    pos.bars++;
    const favorable = pos.direction === "buy" ? high - pos.entry : pos.entry - low;
    const adverse = pos.direction === "buy" ? pos.entry - low : high - pos.entry;
    if (favorable > pos.mfe) pos.mfe = favorable;
    if (adverse > pos.mae) pos.mae = adverse;

    const hitSl = pos.direction === "buy" ? low <= pos.sl : high >= pos.sl;
    const hitTp = pos.direction === "buy" ? high >= pos.tp : low <= pos.tp;

    let outcome = null;
    if (hitSl) outcome = "sl";
    else if (hitTp) outcome = "tp";
    else if (pos.bars >= CFG.maxBarsTrack) outcome = "timeout";

    if (outcome) {
      if (sd && sd.openPosition === pos) sd.openPosition = null;
      const label = sd ? sd.label : symbol;
      const digits = priceDigits(pos.entry);
      const tag = outcome === "tp" ? "✅ Hit TP" : outcome === "sl" ? "❌ Hit SL" : "⏱ Timed out";
      // Report the price actually reached — the TP/SL level for those
      // outcomes, or the closing price of the timeout bar — alongside the
      // entry, rather than just restating the entry price on its own
      // (which reads as if the exit happened at the entry price).
      const exitPrice = outcome === "tp" ? pos.tp : outcome === "sl" ? pos.sl : closedCandle.close;
      console.log(`[outcome] ${label} ${pos.direction} ${tag} entry ${pos.entry.toFixed(digits)} exit ${exitPrice.toFixed(digits)}`);
      sendTelegram(
        `${tag} — ${label}\n` +
          `${pos.direction.toUpperCase()}\n` +
          `Entry: ${pos.entry.toFixed(digits)}\n` +
          `Exit: ${exitPrice.toFixed(digits)}` +
          chartLink(symbol)
      );
    } else {
      remaining.push(pos);
    }
  }
  openPositions = remaining;
}

// ==========================================================================
// HEALTH-CHECK HTTP SERVER (Railway)
// ==========================================================================
const PORT = process.env.PORT || 3000;
http
  .createServer((req, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end(
      `Signal Scan running. Tracking ${SYMBOLS.length} instruments. Open positions: ${openPositions.length}.\n`
    );
  })
  .listen(PORT, () => console.log(`[health] listening on :${PORT}`));

// ==========================================================================
// START
// ==========================================================================
process.on("unhandledRejection", (err) => console.error("[unhandled]", err));
process.on("SIGTERM", () => {
  manualStop = true;
  if (ws) try { ws.close(); } catch (e) {}
  process.exit(0);
});

console.log(
  `[boot] Signal Scan starting — timeframe ${CFG.timeframe}, trend bias ${CFG.trendBias}, ` +
    `EMA ${CFG.ema20}/${CFG.ema50}/${CFG.ema200}, ATR ${CFG.atrPeriod}`
);
sendTelegram("🔔 COIL SYSTEM ONLINE\nDeriv Synthetic Indices Scanner is up and running on Railway");
connect();
