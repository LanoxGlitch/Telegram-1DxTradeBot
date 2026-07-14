/**
 * Deriv Synthetic Indices Scanner — Always-On Backend
 * ----------------------------------------------------
 * Same Impulse & EMA50 Reclaim logic as the browser scanner, but runs
 * as a standalone Node.js process so it keeps scanning whether or not
 * anyone is looking at it. Sends alerts to Telegram.
 *
 * Deploy this to any always-on host (Railway, Render, a small VPS,
 * a Raspberry Pi, etc). It does NOT run in a browser tab.
 */

require('dotenv').config();
const WebSocket = require('ws');
const http = require('http');

// ============================================================================
// CONFIG
// ============================================================================
const APP_ID = process.env.DERIV_APP_ID || '1089';
const WS_URL = `wss://ws.derivws.com/websockets/v3?app_id=${APP_ID}`;

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';

const GRAN = { M15: 900, H1: 3600, H4: 14400 };
const HISTORY_COUNT = 300;

const CFG = {
  ema20: intEnv('EMA20_LEN', 20),
  ema50: intEnv('EMA50_LEN', 50),
  ema200: intEnv('EMA200_LEN', 200),
  atrPeriod: intEnv('ATR_PERIOD', 14),
  atrMult: floatEnv('ATR_MULT', 1.0),
  minPullback: intEnv('MIN_PULLBACK', 3),
  allowMixedH1: boolEnv('ALLOW_MIXED_H1', false),
};

const WHITELIST = {
  volatility: [10, 25, 50, 75, 100],
  volatilityWide: [150, 200, 250],
  jump: [10, 25, 50, 75, 100],
  stepMax: 5,
};

function intEnv(name, def) { const v = parseInt(process.env[name], 10); return isNaN(v) ? def : v; }
function floatEnv(name, def) { const v = parseFloat(process.env[name]); return isNaN(v) ? def : v; }
function boolEnv(name, def) { const v = process.env[name]; return v === undefined ? def : v === 'true'; }

// ============================================================================
// STATE
// ============================================================================
let ws = null;
let SYMBOLS = [];
const DATA = {};
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
    `🔔 <b>SYSTEM ONLINE</b>\n` +
    `Deriv Synthetic Indices Scanner is up and running on Railway.\n\n` +
    `Instruments loaded: <b>${SYMBOLS.length}</b>\n` +
    `Strategy: <b>Impulse &amp; EMA50 Reclaim</b> (active)\n` +
    `Started: <code>${BOOT_TIME.toISOString()}</code>`;
  console.log('[telegram] sending startup notification');
  await sendTelegram(text);
}

// ============================================================================
// SYMBOL RESOLUTION (same dynamic active_symbols approach as the browser tool)
// ============================================================================
function classifySymbol(entry) {
  const name = entry.display_name || '';
  const market = entry.market || '';
  if (market !== 'synthetic_index') return null;

  let m;
  if ((m = name.match(/^Volatility\s+(\d+(?:\.\d+)?)\s*\(1s\)\s*Index$/i))) {
    return { type: 'volatility', oneSec: true, num: parseFloat(m[1]) };
  }
  if ((m = name.match(/^Volatility\s+(\d+(?:\.\d+)?)\s*Index$/i))) {
    return { type: 'volatility', oneSec: false, num: parseFloat(m[1]) };
  }
  if ((m = name.match(/^Jump\s+(\d+)\s*Index$/i))) {
    return { type: 'jump', oneSec: false, num: parseInt(m[1], 10) };
  }
  if ((m = name.match(/^Step\s+Index\s*(\d+)?$/i))) {
    return { type: 'step', oneSec: false, num: m[1] ? parseInt(m[1], 10) : 0 };
  }
  if ((m = name.match(/^Step\s+(\d+(?:\.\d+)?)\s*Index$/i))) {
    return { type: 'step', oneSec: false, num: parseFloat(m[1]) };
  }
  return null;
}

function buildSymbolList(activeSymbols) {
  const found = { volatility: [], volatilityWide: [], jump: [], step: [] };

  activeSymbols.forEach((entry) => {
    const c = classifySymbol(entry);
    if (!c) return;
    const item = { symbol: entry.symbol, display_name: entry.display_name, ...c };
    if (c.type === 'volatility') {
      if (WHITELIST.volatility.includes(c.num)) found.volatility.push(item);
      if (WHITELIST.volatilityWide.includes(c.num)) found.volatilityWide.push(item);
    } else if (c.type === 'jump' && WHITELIST.jump.includes(c.num)) {
      found.jump.push(item);
    } else if (c.type === 'step') {
      found.step.push(item);
    }
  });

  const out = [];
  const seen = new Set();
  function push(symbol, label) {
    if (seen.has(symbol)) return;
    seen.add(symbol);
    out.push({ symbol, label });
  }

  found.volatility
    .sort((a, b) => a.num - b.num || (a.oneSec === b.oneSec ? 0 : a.oneSec ? 1 : -1))
    .forEach((i) => push(i.symbol, i.display_name));

  found.volatilityWide.sort((a, b) => a.num - b.num).forEach((i) => push(i.symbol, i.display_name));
  found.jump.sort((a, b) => a.num - b.num).forEach((i) => push(i.symbol, i.display_name));

  found.step
    .sort((a, b) => a.num - b.num)
    .slice(0, WHITELIST.stepMax)
    .forEach((i, idx) => push(i.symbol, i.display_name || `Step ${(0.1 * (idx + 1)).toFixed(1)}`));

  return out;
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
    if (SYMBOLS.length === 0) {
      wsSend({ active_symbols: 'brief', product_type: 'basic' });
    } else {
      SYMBOLS.forEach((s) => enqueueSymbolRequests(s.symbol));
      runQueue();
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
    ws.send(JSON.stringify(payload));
    return true;
  }
  return false;
}

function enqueueSymbolRequests(symbol) {
  Object.entries(GRAN).forEach(([tfName, granularity]) => {
    requestQueue.push({ symbol, granularity, tfName });
  });
}

function runQueue() {
  if (queueRunning) return;
  queueRunning = true;
  const step = () => {
    if (requestQueue.length === 0) { queueRunning = false; return; }
    const job = requestQueue.shift();
    wsSend({
      ticks_history: job.symbol,
      style: 'candles',
      granularity: job.granularity,
      count: HISTORY_COUNT,
      end: 'latest',
      subscribe: 1,
    });
    setTimeout(step, 220);
  };
  step();
}

// ============================================================================
// MESSAGE HANDLING
// ============================================================================
function handleMessage(msg) {
  if (msg.error) {
    const req = msg.echo_req || {};
    if (req.ticks_history) console.error('[api error]', req.ticks_history, msg.error.message);
    return;
  }

  if (msg.msg_type === 'active_symbols') {
    SYMBOLS = buildSymbolList(msg.active_symbols || []);
    console.log(`[symbols] resolved ${SYMBOLS.length} instruments`);
    SYMBOLS.forEach((s) => initSymbolData(s.symbol, s.label));
    SYMBOLS.forEach((s) => enqueueSymbolRequests(s.symbol));
    runQueue();
    sendStartupNotification();
    return;
  }

  if (msg.msg_type === 'candles') {
    const req = msg.echo_req || {};
    const symbol = req.ticks_history;
    const granularity = req.granularity;
    const sd = DATA[symbol];
    if (!sd || !msg.candles) return;
    const tf = tfKeyFromGranularity(granularity);
    sd.candles[tf] = msg.candles.map((c) => ({ epoch: c.epoch, open: +c.open, high: +c.high, low: +c.low, close: +c.close }));
    recomputeTimeframe(symbol, tf);
    return;
  }

  if (msg.msg_type === 'ohlc') {
    const o = msg.ohlc;
    if (!o) return;
    const symbol = o.symbol;
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
    } else {
      return;
    }

    const justClosed = last.epoch !== candle.epoch;
    recomputeTimeframe(symbol, tf, justClosed ? last : null);
  }
}

function tfKeyFromGranularity(g) {
  if (g === GRAN.M15) return 'm15';
  if (g === GRAN.H1) return 'h1';
  if (g === GRAN.H4) return 'h4';
  return 'm15';
}

// ============================================================================
// SYMBOL DATA MODEL
// ============================================================================
function initSymbolData(symbol, label) {
  DATA[symbol] = {
    symbol, label,
    candles: { m15: [], h1: [], h4: [] },
    ind: {
      h1: { ema20: null, ema50: null, ema200: null, trend: 'mixed' },
      h4: { ema20: null, ema50: null, ema200: null, trend: 'mixed' },
      m15: { ema50: null, atr: null },
    },
    price: null,
    buy: newSideState(),
    sell: newSideState(),
  };
}

function newSideState() {
  return {
    trendSideEstablished: false,
    pullbackActive: false,
    pullbackCandles: 0,
    armed: false,
  };
}

// ============================================================================
// INDICATORS
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

function computeATRSeries(candles, period) {
  const n = candles.length;
  const out = new Array(n).fill(null);
  if (n < period + 1) return out;
  const tr = new Array(n).fill(null);
  for (let i = 1; i < n; i++) {
    const c = candles[i], p = candles[i - 1];
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

function trendFromEma(ema20, ema50, ema200, price) {
  if (ema20 === null || ema50 === null || ema200 === null) return 'mixed';
  if (ema20 > ema50 && ema50 > ema200 && price > ema200) return 'bullish';
  if (ema20 < ema50 && ema50 < ema200 && price < ema200) return 'bearish';
  return 'mixed';
}

function recomputeTimeframe(symbol, tf, justClosedCandle) {
  const sd = DATA[symbol];
  if (!sd) return;
  const candles = sd.candles[tf];
  if (!candles || candles.length === 0) return;
  const closes = candles.map((c) => c.close);
  const lastClose = closes[closes.length - 1];
  sd.price = lastClose;

  if (tf === 'h1' || tf === 'h4') {
    const e20 = computeEMASeries(closes, CFG.ema20);
    const e50 = computeEMASeries(closes, CFG.ema50);
    const e200 = computeEMASeries(closes, CFG.ema200);
    const i = closes.length - 1;
    const ema20 = e20[i], ema50 = e50[i], ema200 = e200[i];
    sd.ind[tf] = { ema20, ema50, ema200, trend: trendFromEma(ema20, ema50, ema200, lastClose) };
  }

  if (tf === 'm15') {
    const e50 = computeEMASeries(closes, CFG.ema50);
    const atrArr = computeATRSeries(candles, CFG.atrPeriod);
    const i = closes.length - 1;
    sd.ind.m15.ema50 = e50[i];
    sd.ind.m15.atr = atrArr[i];

    if (justClosedCandle) {
      const idxClosed = candles.length - 2;
      const emaAtClose = e50[idxClosed];
      const atrAtClose = atrArr[idxClosed];
      if (emaAtClose !== null && atrAtClose !== null) {
        evaluateSide(symbol, 'sell', justClosedCandle, emaAtClose, atrAtClose);
        evaluateSide(symbol, 'buy', justClosedCandle, emaAtClose, atrAtClose);
      }
    }
  }
}

// ============================================================================
// SIGNAL STATE MACHINE (identical rules to the browser scanner)
// ============================================================================
function trendValid(symbol, direction, forCancellationCheck) {
  const sd = DATA[symbol];
  const h1 = sd.ind.h1.trend, h4 = sd.ind.h4.trend;
  const wantH4 = direction === 'sell' ? 'bearish' : 'bullish';
  const wantH1 = direction === 'sell' ? 'bearish' : 'bullish';

  if (h4 !== wantH4) return false;
  if (forCancellationCheck) return true;

  if (!CFG.allowMixedH1) return h1 === wantH1;
  return h1 === wantH1 || h1 === 'mixed';
}

function evaluateSide(symbol, direction, closedCandle, ema50, atr) {
  const sd = DATA[symbol];
  const state = sd[direction];
  const isBull = closedCandle.close > closedCandle.open;
  const isBear = closedCandle.close < closedCandle.open;
  const priceOnTrendSide = direction === 'sell' ? closedCandle.close < ema50 : closedCandle.close > ema50;
  const pullbackCandle = direction === 'sell' ? isBull : isBear;
  const reclaimCandle = direction === 'sell' ? isBear : isBull;

  if (state.armed) {
    if (!trendValid(symbol, direction, true)) {
      resetSide(state);
      return;
    }
    const emaCrossBack = direction === 'sell' ? closedCandle.close < ema50 : closedCandle.close > ema50;
    if (reclaimCandle && emaCrossBack) {
      fireSignal(symbol, direction, closedCandle);
      resetSide(state);
    }
    return;
  }

  if (!trendValid(symbol, direction, false)) return;

  if (priceOnTrendSide && !state.pullbackActive) {
    state.trendSideEstablished = true;
    state.pullbackCandles = 0;
  }

  if (state.trendSideEstablished && pullbackCandle) {
    state.pullbackActive = true;
    state.pullbackCandles++;
    const distance = Math.abs(closedCandle.close - ema50);
    const enoughCandles = state.pullbackCandles >= CFG.minPullback;
    const crossedBack = direction === 'sell' ? closedCandle.close > ema50 : closedCandle.close < ema50;
    const enoughDistance = atr > 0 && distance >= CFG.atrMult * atr;
    if (enoughCandles && crossedBack && enoughDistance) {
      state.armed = true;
      firePreview(symbol, direction, closedCandle);
    }
  } else if (state.pullbackActive && !pullbackCandle) {
    state.pullbackActive = false;
    state.pullbackCandles = 0;
  }
}

function resetSide(state) {
  state.trendSideEstablished = false;
  state.pullbackActive = false;
  state.pullbackCandles = 0;
  state.armed = false;
}

function priceDigits(price) {
  return price < 10 ? 5 : price < 1000 ? 3 : 2;
}

function firePreview(symbol, direction, closedCandle) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const price = closedCandle.close.toFixed(digits);
  const label = direction === 'buy' ? 'BUY SETUP DETECTED' : 'SELL SETUP DETECTED';
  const text =
    `📉 <b>${label}</b>\n` +
    `<b>${sd.label}</b> (${symbol})\n` +
    `Price: <code>${price}</code>\n` +
    `H1: ${sd.ind.h1.trend} · H4: ${sd.ind.h4.trend}\n` +
    `Awaiting confirmation…`;

  console.log(`[PREVIEW] ${direction.toUpperCase()} ${sd.label} @ ${price}`);
  sendTelegram(text);
}

function fireSignal(symbol, direction, closedCandle) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const price = closedCandle.close.toFixed(digits);
  const emoji = direction === 'buy' ? '🟢' : '🔴';
  const text =
    `${emoji} <b>${direction.toUpperCase()} SIGNAL CONFIRMED</b>\n` +
    `<b>${sd.label}</b> (${symbol})\n` +
    `Entry: <code>${price}</code>\n` +
    `H1: ${sd.ind.h1.trend} · H4: ${sd.ind.h4.trend}\n` +
    `Time: ${new Date().toISOString()}`;

  console.log(`[SIGNAL] ${direction.toUpperCase()} ${sd.label} @ ${price}`);
  sendTelegram(text);
}

// ============================================================================
// HEALTH-CHECK HTTP SERVER
// Many always-on hosts (Render, Railway) expect a web service to bind a
// port. This tiny server exists purely so the platform sees it as "alive".
// ============================================================================
const PORT = process.env.PORT || 3000;
http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];

  if (url === '/test') {
    sendTelegram(
      `🧪 <b>TEST MESSAGE</b>\n` +
      `Bot + Railway connection is working.\n` +
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
    strategy: 'Impulse & EMA50 Reclaim',
    time: new Date().toISOString(),
  }));
}).listen(PORT, () => console.log(`[http] health check listening on :${PORT} (routes: /, /test)`));

// ============================================================================
// BOOT
// ============================================================================
console.log('Deriv Synthetic Indices Scanner — backend starting…');
if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
  console.warn('⚠️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set — signals will only be logged to console.');
}
connect();
