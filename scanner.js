"use strict";

/**
 * Signal Scan — Coil / Breakout Scanner (headless, Node.js)
 * ------------------------------------------------------------------
 * Ported from the browser version (index.html). Connects to Deriv's
 * public websocket feed, tracks Volatility indices for the Impulse ->
 * Coil -> Breakout pattern, and pushes alerts to a Telegram chat
 * instead of rendering cards in a browser.
 *
 * Required environment variables (set these in Railway's Variables tab):
 *   TELEGRAM_BOT_TOKEN   - token from @BotFather
 *   TELEGRAM_CHAT_ID     - your chat id (message @userinfobot, or use a group/channel id)
 *
 * Optional environment variables (all have sane defaults):
 *   DERIV_APP_ID         - default '1089' (Deriv's public demo app id)
 *   TIMEFRAME            - M5 | M15 | M30 | H1   (default M15)
 *   TREND_FILTER         - 'true' | 'false'       (default true)
 *   PORT                 - health-check HTTP port (Railway sets this automatically)
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

const DEFAULTS = {
  appId: process.env.DERIV_APP_ID || "1089",
  timeframe: process.env.TIMEFRAME || "M15",
  ema20: envNum("EMA20_LEN", 20),
  ema50: envNum("EMA50_LEN", 50),
  ema200: envNum("EMA200_LEN", 200),
  atrPeriod: envNum("ATR_PERIOD", 14),
  coilLookback: 8, compressionAtrMult: 2.0,
  impulseLookback: 12, impulseAtrMult: 2.5,
  breakoutAtrMult: 0.15, breakoutExpansionMult: 1.6, maxCoilBars: 12,
  tpAtrMult: 2.0, slAtrMult: 1.0, maxBarsTrack: 20,
  trendFilterEnabled: (process.env.TREND_FILTER || "true").toLowerCase() !== "false",
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

const WHITELIST = {
  volatility: [10, 25, 50, 75, 100],
  volatilityOneSec: [10, 15, 25, 30, 50, 75, 90, 100, 150, 250],
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
  const name = entry.display_name || "";
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
    const item = { symbol: entry.symbol, display_name: entry.display_name, ...c };
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
let requestQueue = [];
let queueRunning = false;
let reconnectAttempts = 0;
let manualStop = false;
let pingHandle = null;
let openPositions = [];

function connect() {
  manualStop = false;
  console.log("[status] connecting...");
  const appId = (CFG.appId || "1089").trim() || "1089";
  ws = new WebSocket(`wss://ws.derivws.com/websockets/v3?app_id=${encodeURIComponent(appId)}`);

  ws.on("open", () => {
    const wasReconnect = reconnectAttempts > 0;
    reconnectAttempts = 0;
    console.log(wasReconnect ? "[status] reconnected" : "[status] live");
    if (SYMBOLS.length === 0) {
      wsSend({ active_symbols: "brief", product_type: "basic" });
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
    ws.send(JSON.stringify(payload));
    return true;
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
    wsSend({
      ticks_history: job.symbol,
      style: "candles",
      granularity: job.granularity,
      count: HISTORY_COUNT,
      end: "latest",
      subscribe: 1,
    });
    setTimeout(step, 220);
  };
  step();
}

// ==========================================================================
// MESSAGE HANDLING
// ==========================================================================
function handleMessage(msg) {
  if (msg.error) {
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
    const symbol = req.ticks_history;
    const granularity = req.granularity;
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
    const symbol = o.symbol;
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
      h1: { ema20: null, ema50: null, ema200: null, trend: "mixed" },
      h4: { ema20: null, ema50: null, ema200: null, trend: "mixed" },
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

function trendFromEma(ema20, ema50, ema200, price) {
  if (ema20 === null || ema50 === null || ema200 === null) return "mixed";
  if (ema20 > ema50 && ema50 > ema200 && price > ema200) return "bullish";
  if (ema20 < ema50 && ema50 < ema200 && price < ema200) return "bearish";
  return "mixed";
}

function trendBias(sd) {
  if (sd.ind.h4.trend !== "mixed") return sd.ind.h4.trend;
  return sd.ind.h1.trend;
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
    const i = closes.length - 1;
    const ema20 = e20[i],
      ema50 = e50b[i],
      ema200 = e200[i];
    const key = isH1 ? "h1" : "h4";
    sd.ind[key] = { ema20, ema50, ema200, trend: trendFromEma(ema20, ema50, ema200, lastClose) };
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

function maxDeviationFromEma(candles, start, endExclusive, ema) {
  let m = 0;
  for (let i = start; i < endExclusive; i++) {
    const c = candles[i];
    m = Math.max(m, Math.abs(c.high - ema), Math.abs(c.low - ema));
  }
  return m;
}

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
      const bias = trendBias(sd);
      const conflicts =
        CFG.trendFilterEnabled &&
        bias !== "mixed" &&
        ((direction === "buy" && bias !== "bullish") || (direction === "sell" && bias !== "bearish"));
      if (conflicts) {
        fireFiltered(symbol, direction, closedCandle, bias);
      } else {
        fireSignal(symbol, direction, closedCandle, atr);
      }
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
        coil.boxHigh = w.hi;
        coil.boxLow = w.lo;
        coil.avgRange = w.avgRange;
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

  const impulseOk = maxDeviationFromEma(candles, impulseStart, boxStart, ema) >= CFG.impulseAtrMult * atr;
  if (!impulseOk) {
    coil.active = false;
    return;
  }

  coil.active = true;
  coil.matured = true;
  coil.staleBars = 0;
  coil.boxHigh = w.hi;
  coil.boxLow = w.lo;
  coil.avgRange = w.avgRange;
  firePreview(symbol, closedCandle, coil);
}

function resetCoil(coil) {
  coil.active = false;
  coil.matured = false;
  coil.boxHigh = null;
  coil.boxLow = null;
  coil.avgRange = null;
  coil.staleBars = 0;
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
  console.log(`[watching] ${sd.label} @ ${price} box ${coil.boxLow.toFixed(digits)}–${coil.boxHigh.toFixed(digits)}`);
  sendTelegram(
    `👀 <b>Watching</b> — ${sd.label}\n` +
      `Price: ${price}\n` +
      `Box: ${coil.boxLow.toFixed(digits)}–${coil.boxHigh.toFixed(digits)}\n` +
      `H1/H4 bias: ${sd.ind.h1.trend}/${sd.ind.h4.trend}` +
      chartLink(symbol)
  );
}

function fireFiltered(symbol, direction, closedCandle, bias) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const price = closedCandle.close.toFixed(digits);
  console.log(`[filtered] ${sd.label} ${direction} @ ${price} vs ${bias} bias`);
  sendTelegram(
    `🚫 <b>Filtered breakout</b> — ${sd.label}\n` +
      `${direction.toUpperCase()} vs ${bias} H4/H1 bias\n` +
      `Price: ${price}` +
      chartLink(symbol)
  );
}

function fireSignal(symbol, direction, closedCandle, atr) {
  const sd = DATA[symbol];
  const digits = priceDigits(closedCandle.close);
  const entry = closedCandle.close;
  const price = entry.toFixed(digits);
  sd.lastSignal = { direction, price, time: Date.now() };

  const tp = direction === "buy" ? entry + CFG.tpAtrMult * atr : entry - CFG.tpAtrMult * atr;
  const sl = direction === "buy" ? entry - CFG.slAtrMult * atr : entry + CFG.slAtrMult * atr;

  const emoji = direction === "buy" ? "🟢" : "🔴";
  console.log(`[signal] ${direction.toUpperCase()} ${sd.label} @ ${price} TP ${tp.toFixed(digits)} SL ${sl.toFixed(digits)}`);
  sendTelegram(
    `${emoji} <b>${direction.toUpperCase()} signal</b> — ${sd.label}\n` +
      `Entry: ${price}\n` +
      `TP: ${tp.toFixed(digits)}\n` +
      `SL: ${sl.toFixed(digits)}\n` +
      `H1/H4 bias: ${sd.ind.h1.trend}/${sd.ind.h4.trend}` +
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
      console.log(`[outcome] ${label} ${pos.direction} ${tag}`);
      sendTelegram(
        `${tag} — ${label}\n` + `${pos.direction.toUpperCase()} @ ${pos.entry.toFixed(digits)}` + chartLink(symbol)
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
  `[boot] Signal Scan starting — timeframe ${CFG.timeframe}, trend filter ${CFG.trendFilterEnabled}, ` +
    `EMA ${CFG.ema20}/${CFG.ema50}/${CFG.ema200}, ATR ${CFG.atrPeriod}`
);
sendTelegram("🔔 COIL SYSTEM ONLINE\nDeriv Synthetic Indices Scanner is up and running on Railway");
connect();
