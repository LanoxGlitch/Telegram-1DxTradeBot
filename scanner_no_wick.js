/**
 * Deriv Synthetic Indices Scanner — Always-On Backend
 * ----------------------------------------------------
 * Ports the strategy implemented in the browser dashboard (index.html):
 *
 *   1. TREND ENGINE  — a signed Trend Score (-5..+5) built from:
 *        price vs EMA200, EMA20 vs EMA50 alignment, EMA50 vs EMA200
 *        alignment, EMA200 5-bar slope, and ADX(14) +DI/-DI direction
 *        (only counted when ADX > 25). The score maps to a bias bucket:
 *        Strong Bullish / Bullish / Mixed / Bearish / Strong Bearish.
 *
 *   2. ENTRY ENGINE  — "No-Wick" price-action + retest, gated by the
 *      trend bias (Bullish family -> BUY only, Bearish family -> SELL
 *      only, Mixed -> ignored):
 *        - BUY setup:  a bullish candle with (effectively) no lower wick.
 *                      Its low becomes the level to watch.
 *        - SELL setup: a bearish candle with (effectively) no upper wick.
 *                      Its high becomes the level to watch.
 *        A retest of that level within `countdownLength` candles triggers
 *        a signal with a 1:1 stop-loss/take-profit (risk = the pattern
 *        candle's own high-low range). The setup then resolves win/loss
 *        against that stop/target on every scan tick.
 *
 * Telegram wiring, WebSocket plumbing, reconnect logic, and the health
 * check HTTP server follow the same shape as the previous backend so
 * this drops into the same always-on host (Railway, Render, a VPS, etc).
 */

require('dotenv').config();
const WebSocket = require('ws');
const http = require('http');

// ============================================================================
// CONFIG
// ============================================================================
// New Deriv API: public, read-only market data needs no app_id/auth — just
// connect straight to the public WebSocket endpoint. DERIV_APP_ID is no
// longer used by this script (kept as a no-op env var for compatibility
// with any deploy config that still sets it).
const WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

// Timeframes streamed for every symbol (id -> granularity in seconds),
// matching Config.TIMEFRAMES in index.html.
const TIMEFRAMES = { '15m': 900, '30m': 1800, '1h': 3600 };
const HISTORY_COUNT = 300;

function intEnv(name, def) { const v = parseInt(process.env[name], 10); return isNaN(v) ? def : v; }
function floatEnv(name, def) { const v = parseFloat(process.env[name]); return isNaN(v) ? def : v; }

const CFG = {
  // Trend Engine
  ema20: intEnv('EMA20_LEN', 20),
  ema50: intEnv('EMA50_LEN', 50),
  ema200: intEnv('EMA200_LEN', 200),
  adxPeriod: intEnv('ADX_PERIOD', 14),
  adxThreshold: floatEnv('ADX_STRENGTH_THRESHOLD', 25),
  slopeLookback: intEnv('SLOPE_LOOKBACK_BARS', 5),
  slopeEpsilonMultiplier: floatEnv('SLOPE_EPSILON_MULTIPLIER', 0.0001),
  scoreStrongBullMin: intEnv('SCORE_STRONG_BULLISH_MIN', 4),
  scoreBullMin: intEnv('SCORE_BULLISH_MIN', 2),
  scoreMixedMin: intEnv('SCORE_MIXED_MIN', -1),
  scoreBearMin: intEnv('SCORE_BEARISH_MIN', -3),
  // EMA200 wants 200 closes + 5 bars of slope lookback + headroom for a
  // stable Wilder ADX(14) average — below this the trend can't be honestly scored.
  minCandles: intEnv('MIN_CANDLES', 210),

  // Entry Engine
  trendTimeframe: TIMEFRAMES[process.env.TREND_TIMEFRAME] ? process.env.TREND_TIMEFRAME : '1h',
  entryTimeframe: TIMEFRAMES[process.env.ENTRY_TIMEFRAME] ? process.env.ENTRY_TIMEFRAME : '15m',
  wickToleranceMode: process.env.WICK_TOLERANCE_MODE === 'body' ? 'body' : 'range',
  wickTolerancePercent: floatEnv('WICK_TOLERANCE_PERCENT', 3),
  wickToleranceBodyPercent: floatEnv('WICK_TOLERANCE_BODY_PERCENT', 15),
  countdownLength: intEnv('COUNTDOWN_LENGTH', 10),

  scanIntervalMs: intEnv('SCAN_INTERVAL_MS', 2000),
};

// Symbol catalogue + default watchlist, mirrors Config.SYMBOLS /
// Config.ALLOWED_WATCHLIST_SYMBOL_IDS in index.html.
const SYMBOL_CATALOG = [
  { id: 'R_10', name: 'Volatility 10 Index' },
  { id: 'R_25', name: 'Volatility 25 Index' },
  { id: 'R_50', name: 'Volatility 50 Index' },
  { id: 'R_75', name: 'Volatility 75 Index' },
  { id: 'R_100', name: 'Volatility 100 Index' },
  { id: '1HZ10V', name: 'Volatility 10 (1s) Index' },
  { id: '1HZ25V', name: 'Volatility 25 (1s) Index' },
  { id: '1HZ50V', name: 'Volatility 50 (1s) Index' },
  { id: '1HZ75V', name: 'Volatility 75 (1s) Index' },
  { id: '1HZ100V', name: 'Volatility 100 (1s) Index' },
  { id: 'BOOM300N', name: 'Boom 300 Index' },
  { id: 'BOOM500', name: 'Boom 500 Index' },
  { id: 'BOOM1000', name: 'Boom 1000 Index' },
  { id: 'CRASH300N', name: 'Crash 300 Index' },
  { id: 'CRASH500', name: 'Crash 500 Index' },
  { id: 'CRASH1000', name: 'Crash 1000 Index' },
  { id: 'JD10', name: 'Jump 10 Index' },
  { id: 'JD25', name: 'Jump 25 Index' },
  { id: 'JD50', name: 'Jump 50 Index' },
  { id: 'JD75', name: 'Jump 75 Index' },
  { id: 'JD100', name: 'Jump 100 Index' },
  { id: 'STPRNG', name: 'Step Index' },
];
const SYMBOL_MAP = new Map(SYMBOL_CATALOG.map((s) => [s.id, s]));

const DEFAULT_WATCHLIST = ['R_50', '1HZ50V', 'R_75', '1HZ75V', 'R_100', '1HZ100V'];
const WATCHLIST_IDS = (process.env.WATCHLIST_SYMBOLS
  ? process.env.WATCHLIST_SYMBOLS.split(',').map((s) => s.trim()).filter(Boolean)
  : DEFAULT_WATCHLIST
).filter((id) => SYMBOL_MAP.has(id));

const SYMBOLS = WATCHLIST_IDS.map((id) => ({ symbol: id, label: SYMBOL_MAP.get(id).name }));

// ============================================================================
// STATE
// ============================================================================
let ws = null;
const DATA = {}; // symbol -> { candles: {15m,30m,1h}, trendCache: Map }
const MACHINES = new Map(); // symbol -> entry-engine state machine
// The New Deriv API no longer guarantees `echo_req` on responses, so we tag
// every request with a req_id and keep a lookup here to know which
// symbol/timeframe a `candles` response belongs to.
let reqIdSeq = 1;
const pendingRequests = new Map();
let requestQueue = [];
let queueRunning = false;
let reconnectAttempts = 0;
let startupNotified = false;
const BOOT_TIME = new Date();

// ============================================================================
// TELEGRAM
// ============================================================================
async function sendTelegram(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log('[telegram disabled — set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID]', text);
    return;
  }
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      }),
    });
    if (!res.ok) {
      const body = await res.text();
      console.error('[telegram] send failed:', res.status, body);
    }
  } catch (err) {
    console.error('[telegram] error:', err.message);
  }
}

async function sendStartupNotification() {
  if (startupNotified) return;
  startupNotified = true;
  const text =
    `🔔 <b>SYSTEM ONLINE · No-Wick</b>\n` +
    `Deriv Synthetic Indices Scanner is up and running.\n\n` +
    `Instruments loaded: <b>${SYMBOLS.length}</b>\n` +
    `Strategy: <b>Trend Score + No-Wick Retest</b> (active)\n` +
    `Trend TF: <b>${CFG.trendTimeframe}</b> · Entry TF: <b>${CFG.entryTimeframe}</b>\n` +
    `Started: <code>${BOOT_TIME.toISOString()}</code>`;
  console.log('[telegram] sending startup notification');
  await sendTelegram(text);
}

// ============================================================================
// WEBSOCKET
// ============================================================================
function connect() {
  console.log('[ws] connecting…');
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[ws] connected');
    reconnectAttempts = 0;
    SYMBOLS.forEach((s) => {
      if (!DATA[s.symbol]) initSymbolData(s.symbol, s.label);
      enqueueSymbolRequests(s.symbol);
    });
    runQueue();
    if (!startupNotified) {
      // Give the first history batch a moment to land before announcing.
      setTimeout(sendStartupNotification, 3000);
    }
  });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }
    handleMessage(msg);
  });

  ws.on('error', (err) => console.error('[ws] error:', err.message));

  ws.on('close', () => {
    console.log('[ws] closed — reconnecting…');
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  reconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.6, reconnectAttempts), 20000);
  console.log(`[ws] reconnecting in ${Math.round(delay / 1000)}s`);
  setTimeout(connect, delay);
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
  Object.entries(TIMEFRAMES).forEach(([tfName, granularity]) => {
    requestQueue.push({ symbol, granularity, tfName });
  });
}

function runQueue() {
  if (queueRunning) return;
  queueRunning = true;
  const step = () => {
    if (requestQueue.length === 0) { queueRunning = false; return; }
    const job = requestQueue.shift();
    const reqId = wsSend({
      ticks_history: job.symbol,
      style: 'candles',
      granularity: job.granularity,
      count: HISTORY_COUNT,
      end: 'latest',
      adjust_start_time: 1,
      subscribe: 1,
    });
    if (reqId) pendingRequests.set(reqId, { symbol: job.symbol, granularity: job.granularity });
    setTimeout(step, 220);
  };
  step();
}

// ============================================================================
// MESSAGE HANDLING
// ============================================================================
function handleMessage(msg) {
  // The New Deriv API no longer guarantees `echo_req` on responses, so pull
  // the pending request (if any) by req_id before falling back to echo_req.
  const pending = (msg.req_id !== undefined) ? pendingRequests.get(msg.req_id) : undefined;

  if (msg.error) {
    if (pending) {
      console.error('[api error]', pending.symbol, msg.error.message);
      pendingRequests.delete(msg.req_id);
    } else {
      const req = msg.echo_req || {};
      if (req.ticks_history) console.error('[api error]', req.ticks_history, msg.error.message);
    }
    return;
  }

  if (msg.msg_type === 'candles') {
    const req = msg.echo_req || {};
    const symbol = pending ? pending.symbol : req.ticks_history;
    const granularity = pending ? pending.granularity : req.granularity;
    if (msg.req_id !== undefined) pendingRequests.delete(msg.req_id);
    const sd = DATA[symbol];
    if (!sd || !msg.candles) return;
    const tf = tfKeyFromGranularity(granularity);
    sd.candles[tf] = msg.candles.map((c) => ({ epoch: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close }));
    return;
  }

  if (msg.msg_type === 'ohlc') {
    const o = msg.ohlc;
    if (!o) return;
    const symbol = o.symbol || o.underlying_symbol;
    const granularity = +o.granularity;
    const sd = DATA[symbol];
    if (!sd) return;
    const tf = tfKeyFromGranularity(granularity);
    const arr = sd.candles[tf];
    if (!arr || arr.length === 0) return;

    const last = arr[arr.length - 1];
    const candle = { epoch: +o.open_time, open: +o.open, high: +o.high, low: +o.low, close: +o.close };

    if (last.epoch === candle.epoch) {
      arr[arr.length - 1] = candle;
    } else if (candle.epoch > last.epoch) {
      arr.push(candle);
      if (arr.length > HISTORY_COUNT + 5) arr.shift();
    }
  }
}

function tfKeyFromGranularity(g) {
  if (g === TIMEFRAMES['15m']) return '15m';
  if (g === TIMEFRAMES['30m']) return '30m';
  if (g === TIMEFRAMES['1h']) return '1h';
  return '15m';
}

// ============================================================================
// SYMBOL DATA MODEL
// ============================================================================
function initSymbolData(symbol, label) {
  DATA[symbol] = {
    symbol, label,
    candles: { '15m': [], '30m': [], '1h': [] },
    trendCache: null, // { signature, result } — memoizes getTrend() per tf per tick
  };
}

// ============================================================================
// INDICATORS (EMA / Wilder ADX+DMI) — same math as TrendEngine in index.html
// ============================================================================
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

/** Wilder ADX(period) with +DI/-DI. Mirrors TrendEngine._adx in index.html. */
function computeADX(candles, period) {
  const n = candles.length;
  if (n < period * 2) return { adx: null, diPlus: null, diMinus: null };

  const tr = new Array(n).fill(0);
  const plusDM = new Array(n).fill(0);
  const minusDM = new Array(n).fill(0);

  for (let i = 1; i < n; i++) {
    const cur = candles[i], prev = candles[i - 1];
    tr[i] = Math.max(
      cur.high - cur.low,
      Math.abs(cur.high - prev.close),
      Math.abs(cur.low - prev.close)
    );
    const upMove = cur.high - prev.high;
    const downMove = prev.low - cur.low;
    plusDM[i] = (upMove > downMove && upMove > 0) ? upMove : 0;
    minusDM[i] = (downMove > upMove && downMove > 0) ? downMove : 0;
  }

  let atr = 0, plusDM14 = 0, minusDM14 = 0;
  for (let i = 1; i <= period; i++) { atr += tr[i]; plusDM14 += plusDM[i]; minusDM14 += minusDM[i]; }

  // Guard against a dead-flat run of candles collapsing the summed true
  // range to exactly 0, which would otherwise poison every later smoothed
  // value through the recursive formula below.
  let diPlus = 100 * (plusDM14 / (atr || 1));
  let diMinus = 100 * (minusDM14 / (atr || 1));
  const dxValues = [];
  dxValues.push(100 * Math.abs(diPlus - diMinus) / (diPlus + diMinus || 1));

  for (let i = period + 1; i < n; i++) {
    atr = atr - atr / period + tr[i];
    plusDM14 = plusDM14 - plusDM14 / period + plusDM[i];
    minusDM14 = minusDM14 - minusDM14 / period + minusDM[i];
    diPlus = 100 * (plusDM14 / (atr || 1));
    diMinus = 100 * (minusDM14 / (atr || 1));
    dxValues.push(100 * Math.abs(diPlus - diMinus) / (diPlus + diMinus || 1));
  }

  if (dxValues.length < period) return { adx: null, diPlus, diMinus };

  let adx = 0;
  for (let i = 0; i < period; i++) adx += dxValues[i];
  adx = adx / period;
  for (let i = period; i < dxValues.length; i++) {
    adx = (adx * (period - 1) + dxValues[i]) / period;
  }

  return { adx, diPlus, diMinus };
}

// ============================================================================
// TREND ENGINE — signed Trend Score (-5..+5) -> bias bucket
// ============================================================================
function scoreToBias(score) {
  if (score >= CFG.scoreStrongBullMin) return 'Strong Bullish';
  if (score >= CFG.scoreBullMin) return 'Bullish';
  if (score >= CFG.scoreMixedMin) return 'Mixed';
  if (score >= CFG.scoreBearMin) return 'Bearish';
  return 'Strong Bearish';
}

function directionFromBias(bias) {
  if (bias === 'Strong Bullish' || bias === 'Bullish') return 'bullish';
  if (bias === 'Strong Bearish' || bias === 'Bearish') return 'bearish';
  return 'mixed';
}

function emptyTrendResult(symbol, timeframe, status) {
  return {
    symbol, timeframe, status,
    price: null, ema20: null, ema50: null, ema200: null,
    priceVsEma200: null, emaAlignment: null, ema200Slope: null,
    adx14: null, diPlus: null, diMinus: null,
    score: null, bias: null,
  };
}

function streamSignature(candles) {
  if (!candles || candles.length === 0) return null;
  const last = candles[candles.length - 1];
  return `${candles.length}:${candles[0].epoch}:${last.epoch}:${last.close}`;
}

/** Determines market bias for a symbol on a given timeframe. Memoized per tick via a cheap stream fingerprint, same idea as TrendEngine.getTrend in index.html. */
function getTrend(symbol, timeframe) {
  const sd = DATA[symbol];
  if (!sd) return emptyTrendResult(symbol, timeframe, 'no_data');
  const candles = sd.candles[timeframe] || [];

  const signature = streamSignature(candles);
  if (signature && sd.trendCache && sd.trendCache.timeframe === timeframe && sd.trendCache.signature === signature) {
    return sd.trendCache.result;
  }

  if (candles.length === 0) return emptyTrendResult(symbol, timeframe, 'no_data');
  if (candles.length < CFG.minCandles) return emptyTrendResult(symbol, timeframe, 'insufficient_data');

  const closes = candles.map((c) => c.close);
  const completed = candles.slice(0, -1);

  const ema20Series = computeEMASeries(closes, CFG.ema20);
  const ema50Series = computeEMASeries(closes, CFG.ema50);
  const ema200Series = computeEMASeries(closes, CFG.ema200);
  const ema200CompletedSeries = computeEMASeries(completed.map((c) => c.close), CFG.ema200);

  const lastIdx = closes.length - 1;
  const price = closes[lastIdx];
  const ema20 = ema20Series[lastIdx];
  const ema50 = ema50Series[lastIdx];
  const ema200 = ema200Series[lastIdx];

  if (ema20 == null || ema50 == null || ema200 == null) {
    return emptyTrendResult(symbol, timeframe, 'insufficient_data');
  }

  const priceVsEma200 = price > ema200 ? 'above' : price < ema200 ? 'below' : 'equal';

  const ema20Aligned = ema20 > ema50 ? 'bullish' : ema20 < ema50 ? 'bearish' : 'flat';
  const ema50Aligned = ema50 > ema200 ? 'bullish' : ema50 < ema200 ? 'bearish' : 'flat';
  let emaAlignment = 'mixed';
  if (ema20Aligned === 'bullish' && ema50Aligned === 'bullish') emaAlignment = 'bullish';
  else if (ema20Aligned === 'bearish' && ema50Aligned === 'bearish') emaAlignment = 'bearish';

  let ema200Slope = 'flat';
  const compLastIdx = ema200CompletedSeries.length - 1;
  const compPrevIdx = compLastIdx - CFG.slopeLookback;
  if (compPrevIdx >= 0 && ema200CompletedSeries[compLastIdx] != null && ema200CompletedSeries[compPrevIdx] != null) {
    const slopeValue = ema200CompletedSeries[compLastIdx] - ema200CompletedSeries[compPrevIdx];
    const epsilon = ema200CompletedSeries[compLastIdx] * CFG.slopeEpsilonMultiplier;
    ema200Slope = slopeValue > epsilon ? 'rising' : slopeValue < -epsilon ? 'falling' : 'flat';
  }

  const dmi = computeADX(candles, CFG.adxPeriod);
  const adx14 = dmi.adx, diPlus = dmi.diPlus, diMinus = dmi.diMinus;

  let score = 0;
  score += priceVsEma200 === 'above' ? 1 : priceVsEma200 === 'below' ? -1 : 0;
  score += ema20Aligned === 'bullish' ? 1 : ema20Aligned === 'bearish' ? -1 : 0;
  score += ema50Aligned === 'bullish' ? 1 : ema50Aligned === 'bearish' ? -1 : 0;
  score += ema200Slope === 'rising' ? 1 : ema200Slope === 'falling' ? -1 : 0;
  if (adx14 != null && adx14 > CFG.adxThreshold && diPlus != null && diMinus != null) {
    score += diPlus > diMinus ? 1 : diPlus < diMinus ? -1 : 0;
  }

  const bias = scoreToBias(score);

  const result = {
    symbol, timeframe, status: 'ok',
    price, ema20, ema50, ema200,
    priceVsEma200, emaAlignment, ema200Slope,
    adx14, diPlus, diMinus,
    score, bias,
  };

  sd.trendCache = { timeframe, signature, result };
  return result;
}

// ============================================================================
// ENTRY ENGINE — "No-Wick" pattern + retest state machine
// ============================================================================
const EntryState = { IDLE: 'Idle', WATCHING: 'Watching', TRIGGERED: 'Triggered', EXPIRED: 'Expired' };

function getOrCreateMachine(symbol) {
  let m = MACHINES.get(symbol);
  if (!m) {
    m = { symbol, state: EntryState.IDLE, setup: null, lastProcessedEpoch: null };
    MACHINES.set(symbol, m);
  }
  return m;
}

function priceDigits(price) {
  return price < 10 ? 5 : price < 1000 ? 3 : 2;
}

function isNoLowerWickBullish(candle) {
  if (candle.close <= candle.open) return false;
  return passesWickCheck(candle, Math.abs(candle.low - candle.open));
}
function isNoUpperWickBearish(candle) {
  if (candle.close >= candle.open) return false;
  return passesWickCheck(candle, Math.abs(candle.high - candle.open));
}
function passesWickCheck(candle, wickSize) {
  return CFG.wickToleranceMode === 'body'
    ? wickSize <= wickToleranceBody(candle)
    : wickSize <= wickTolerance(candle);
}
function wickTolerance(candle) {
  const range = candle.high - candle.low;
  return Math.max(range * (CFG.wickTolerancePercent / 100), candle.close * 0.00005);
}
function wickToleranceBody(candle) {
  const body = Math.abs(candle.close - candle.open);
  return Math.max(body * (CFG.wickToleranceBodyPercent / 100), candle.close * 0.00005);
}

/** Advances (or creates) the state machine for a symbol by one scan cycle. */
function evaluateEntry(symbol, entryTimeframe, trendTimeframe) {
  const machine = getOrCreateMachine(symbol);
  const trend = getTrend(symbol, trendTimeframe);
  if (trend.status !== 'ok') return { status: trend.status, machine };

  const candles = DATA[symbol].candles[entryTimeframe] || [];
  if (candles.length < 3) return { status: 'insufficient_data', machine };

  const latestCompleted = candles[candles.length - 2];
  const liveCandle = candles[candles.length - 1] || latestCompleted;
  const isNewCandle = machine.lastProcessedEpoch !== latestCompleted.epoch;
  const direction = directionFromBias(trend.bias);

  switch (machine.state) {
    case EntryState.IDLE:
    case EntryState.EXPIRED:
      if (isNewCandle) tryDetect(machine, latestCompleted, direction, trend, trendTimeframe);
      break;
    case EntryState.WATCHING:
      if (direction !== machine.setup.biasDirection) {
        expireSetup(machine, 'trend_reversed');
      } else {
        checkRetest(machine, liveCandle);
        if (machine.state === EntryState.WATCHING && isNewCandle) {
          advanceWatching(machine, trend);
        }
      }
      break;
    case EntryState.TRIGGERED:
      checkOutcome(machine, symbol, entryTimeframe);
      break;
  }

  if (isNewCandle) machine.lastProcessedEpoch = latestCompleted.epoch;
  return { status: 'ok', machine };
}

function tryDetect(machine, patternCandle, direction, trend, trendTimeframe) {
  if (direction === 'mixed') {
    if (machine.state !== EntryState.IDLE) resetToIdle(machine);
    return;
  }

  let directionLabel = null, level = null;
  if (direction === 'bullish' && isNoLowerWickBullish(patternCandle)) {
    directionLabel = 'BUY';
    level = patternCandle.low;
  } else if (direction === 'bearish' && isNoUpperWickBearish(patternCandle)) {
    directionLabel = 'SELL';
    level = patternCandle.high;
  }

  if (!directionLabel) {
    if (machine.state !== EntryState.IDLE) resetToIdle(machine);
    return;
  }

  const range = Math.abs(patternCandle.high - patternCandle.low);
  const risk = range > 0 ? range : patternCandle.close * 0.0005;
  const stopLoss = directionLabel === 'BUY' ? level - risk : level + risk;
  const takeProfit = directionLabel === 'BUY' ? level + risk : level - risk;

  machine.setup = {
    id: `${machine.symbol}:${patternCandle.epoch}`,
    symbol: machine.symbol,
    directionLabel,
    biasDirection: direction,
    trend: trend.bias,
    trendScore: trend.score,
    trendTimeframe,
    level, risk, stopLoss, takeProfit,
    pattern: patternCandle,
    retest: null,
    maxRetestCandles: CFG.countdownLength,
    candlesSinceDetection: 0,
    countdown: CFG.countdownLength,
    detectionTime: Date.now(),
  };
  machine.state = EntryState.WATCHING;

  notifyPreview(machine.setup);
}

function checkRetest(machine, candle) {
  const setup = machine.setup;
  const retested = setup.biasDirection === 'bullish' ? candle.low <= setup.level : candle.high >= setup.level;
  if (retested) triggerSetup(machine, candle);
}

function advanceWatching(machine, trend) {
  const setup = machine.setup;
  setup.trend = trend.bias;
  setup.trendScore = trend.score;
  setup.candlesSinceDetection += 1;

  if (setup.candlesSinceDetection > setup.maxRetestCandles) {
    expireSetup(machine, 'countdown_elapsed');
    return;
  }
  setup.countdown = setup.maxRetestCandles - setup.candlesSinceDetection;
  if (setup.candlesSinceDetection >= setup.maxRetestCandles) expireSetup(machine, 'countdown_elapsed');
}

function checkOutcome(machine, symbol, entryTimeframe) {
  const setup = machine.setup;
  if (!setup || setup.takeProfit == null || setup.stopLoss == null) return;
  const candles = DATA[symbol].candles[entryTimeframe] || [];
  const liveCandle = candles[candles.length - 1];
  if (!liveCandle) return;
  const { high, low } = liveCandle;

  let outcome = null;
  if (setup.directionLabel === 'BUY') {
    if (low <= setup.stopLoss) outcome = 'loss';
    else if (high >= setup.takeProfit) outcome = 'win';
  } else {
    if (high >= setup.stopLoss) outcome = 'loss';
    else if (low <= setup.takeProfit) outcome = 'win';
  }
  if (outcome) resolveOutcome(machine, outcome, liveCandle.close);
}

function resolveOutcome(machine, outcome, exitPrice) {
  const setup = machine.setup;
  setup.outcome = outcome;
  setup.exitPrice = exitPrice;
  notifyOutcome(setup, outcome, exitPrice);
  machine.state = EntryState.IDLE;
  machine.setup = null;
}

function triggerSetup(machine, retestCandle) {
  const setup = machine.setup;
  setup.retest = retestCandle;
  setup.triggeredAt = Date.now();
  machine.state = EntryState.TRIGGERED;
  notifySignal(setup);
}

function expireSetup(machine, reason) {
  console.log(`[expired] ${machine.symbol} ${machine.setup.directionLabel} — ${reason}`);
  machine.state = EntryState.EXPIRED;
  machine.setup = null;
}

function resetToIdle(machine) {
  machine.state = EntryState.IDLE;
  machine.setup = null;
}

// ============================================================================
// TELEGRAM NOTIFICATIONS
// ============================================================================
function notifyPreview(setup) {
  const sd = DATA[setup.symbol];
  const digits = priceDigits(setup.level);
  const text =
    `📉 <b>${setup.directionLabel === 'BUY' ? 'BUY' : 'SELL'} SETUP DETECTED · No-Wick</b>\n` +
    `<b>${sd.label}</b> (${setup.symbol})\n` +
    `No-Wick level: <code>${setup.level.toFixed(digits)}</code>\n` +
    `Trend: ${setup.trend} (score ${setup.trendScore}) on ${setup.trendTimeframe}\n` +
    `Awaiting retest (window: ${setup.maxRetestCandles} candles on ${CFG.entryTimeframe})…`;

  console.log(`[PREVIEW] ${setup.directionLabel} ${sd.label} level=${setup.level.toFixed(digits)}`);
  sendTelegram(text);
}

function notifySignal(setup) {
  const sd = DATA[setup.symbol];
  const digits = priceDigits(setup.level);
  const emoji = setup.directionLabel === 'BUY' ? '🟢' : '🔴';
  const text =
    `${emoji} <b>${setup.directionLabel} SIGNAL CONFIRMED · No-Wick</b>\n` +
    `<b>${sd.label}</b> (${setup.symbol})\n` +
    `Entry: <code>${setup.level.toFixed(digits)}</code>\n` +
    `Stop Loss: <code>${setup.stopLoss.toFixed(digits)}</code>\n` +
    `Take Profit: <code>${setup.takeProfit.toFixed(digits)}</code>\n` +
    `Trend: ${setup.trend} (score ${setup.trendScore}) on ${setup.trendTimeframe}\n` +
    `Time: ${new Date().toISOString()}`;

  console.log(`[SIGNAL] ${setup.directionLabel} ${sd.label} @ ${setup.level.toFixed(digits)}`);
  sendTelegram(text);
}

function notifyOutcome(setup, outcome, exitPrice) {
  const sd = DATA[setup.symbol];
  const digits = priceDigits(exitPrice);
  const emoji = outcome === 'win' ? '✅' : '❌';
  const text =
    `${emoji} <b>${setup.directionLabel} SETUP ${outcome.toUpperCase()} · No-Wick</b>\n` +
    `<b>${sd.label}</b> (${setup.symbol})\n` +
    `Entry: <code>${setup.level.toFixed(digits)}</code> → Exit: <code>${exitPrice.toFixed(digits)}</code>\n` +
    `Time: ${new Date().toISOString()}`;

  console.log(`[OUTCOME] ${outcome.toUpperCase()} ${setup.directionLabel} ${sd.label}`);
  sendTelegram(text);
}

// ============================================================================
// SCANNER TICK — runs every CFG.scanIntervalMs, mirrors ScannerManager._tick
// ============================================================================
let scanTimer = null;
let lastCycleAt = null;

function tick() {
  SYMBOLS.forEach((s) => {
    if (!DATA[s.symbol]) return;
    evaluateEntry(s.symbol, CFG.entryTimeframe, CFG.trendTimeframe);
  });
  lastCycleAt = Date.now();
}

function startScanner() {
  if (scanTimer) return;
  tick();
  scanTimer = setInterval(tick, CFG.scanIntervalMs);
  console.log(`[scanner] running every ${CFG.scanIntervalMs}ms`);
}

// ============================================================================
// HEALTH-CHECK HTTP SERVER
// ============================================================================
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];

  if (url === '/test') {
    sendTelegram(
      `🧪 <b>TEST MESSAGE · No-Wick</b>\n` +
      `Bot connection is working.\n` +
      `Time: <code>${new Date().toISOString()}</code>`
    )
      .then(() => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'test message sent', time: new Date().toISOString() }));
      })
      .catch((err) => {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'error', error: err.message }));
      });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'running',
    symbolsTracked: SYMBOLS.length,
    wsState: ws ? ws.readyState : -1,
    strategy: 'Trend Score + No-Wick Retest',
    trendTimeframe: CFG.trendTimeframe,
    entryTimeframe: CFG.entryTimeframe,
    lastCycleAt,
    time: new Date().toISOString(),
  }));
}).listen(PORT, () => console.log(`[http] health check listening on :${PORT} (routes: /, /test)`));

// ============================================================================
// BOOT
// ============================================================================
console.log('Deriv Synthetic Indices Scanner — backend starting…');
console.log(`[config] watchlist: ${SYMBOLS.map((s) => s.symbol).join(', ')}`);
console.log(`[config] trend TF: ${CFG.trendTimeframe} · entry TF: ${CFG.entryTimeframe} · scan interval: ${CFG.scanIntervalMs}ms`);
if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
  console.warn('⚠️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — signals will only be logged to console.');
}
connect();
startScanner();
