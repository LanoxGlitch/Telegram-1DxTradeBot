# Signal Scan — Telegram Edition

Headless port of the browser-based Coil/Breakout scanner. Runs 24/7 on Railway,
connects to Deriv's public feed, and sends alerts straight to a Telegram chat
instead of a browser tab (so no browser needs to stay open).

Same detection logic as the original page: watches Volatility indices for an
impulsive swing into EMA50, a tight consolidation ("coil") straddling it, and
a decisive breakout candle — then tracks that entry's TP/SL until it resolves.

## 1. Create a Telegram bot

1. Open Telegram, message **@BotFather**, send `/newbot`, follow the prompts.
2. BotFather gives you a token like `123456789:AAExample...` — save it.
3. Message your new bot anything (e.g. "hi") so it can message you back.
4. Get your chat id: message **@userinfobot** and it will reply with your
   numeric id. (For a group, add the bot to the group and use the group's id
   instead — group ids are negative numbers.)

## 2. Deploy to Railway

1. Push these files (`scanner.js`, `package.json`) to a new GitHub repo, or
   use Railway's "Deploy from local folder" / CLI option.
2. In Railway: **New Project → Deploy from GitHub repo** (or upload).
3. Go to the service's **Variables** tab and add:
   - `TELEGRAM_BOT_TOKEN` — from step 1
   - `TELEGRAM_CHAT_ID` — from step 1
   - (optional) `TIMEFRAME` — `M5`, `M15`, `M30`, or `H1` (default `M15`)
   - (optional) `DERIV_APP_ID` — default `1089` (Deriv's public demo id, fine to leave as-is)
   - (optional) `TREND_FILTER` — `true`/`false` (default `true`)
4. Railway auto-detects Node from `package.json` and runs `npm start`.
5. Check the **Deployments → Logs** tab — you should see:
   ```
   [boot] Signal Scan starting — timeframe M15, trend filter true
   [status] connecting...
   [status] live
   [status] connected — tracking 15 instruments
   ```
   and a "✅ Scanner connected" message should land in your Telegram chat.

Railway sets `PORT` automatically; the app opens a tiny HTTP endpoint on it
purely so Railway's health checks see something respond (visiting the URL
shows a one-line status message). No browser or dashboard is served —
everything else happens via Telegram.

## 3. What you'll receive in Telegram

- 👀 **Watching** — a coil has matured and is waiting for a breakout
- 🚫 **Filtered breakout** — a breakout fired but conflicted with the H4/H1
  trend filter (only sent if `TREND_FILTER=true`)
- 🟢/🔴 **BUY/SELL signal** — confirmed breakout, with entry/TP/SL
- ✅/❌/⏱ **Hit TP / Hit SL / Timed out** — resolution of a previously
  confirmed signal

Each message includes a TradingView chart link for that instrument.

## Notes / differences from the browser version

- No sound, no UI, no localStorage — settings are environment variables
  instead of a settings modal. To change strategy parameters (coil width,
  ATR multiples, TP/SL, etc.) beyond what's exposed as env vars, edit the
  `CFG` object at the top of `scanner.js` directly and redeploy.
- Position tracking (TP/SL/timeout) is in-memory only, same as the original
  — if the service restarts, any signals that were mid-flight are dropped
  rather than resumed.
- Only Volatility indices are scanned (matching the original's whitelist);
  no other Deriv markets are included.

## Running locally (optional, for testing before deploying)

```bash
npm install
TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=xxx node scanner.js
```
