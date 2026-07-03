# Deriv Synthetic Indices Scanner — Always-On Bot

Same Impulse & EMA50 Reclaim logic as the browser scanner, running as a
Node.js process so it keeps working when your phone is locked or your
laptop is closed. Sends alerts straight to Telegram.

## 1. Create a Telegram bot (2 minutes)

1. In Telegram, message **@BotFather** → `/newbot` → follow the prompts.
2. BotFather gives you a token like `123456789:AAExampleTokenAbcDefGhi`.
   That's your `TELEGRAM_BOT_TOKEN`.
3. Start a chat with your new bot (search its username, tap Start).
4. Get your chat id: message **@userinfobot** (or open
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` after sending your
   bot any message) and copy the numeric `chat.id`. That's your
   `TELEGRAM_CHAT_ID`.

## 2. Configure

```
cp .env.example .env
```

Fill in `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Everything else has
sane defaults matching the browser scanner.

## 3. Run locally (to test)

```
npm install
npm start
```

You should see `[symbols] resolved N instruments` in the console, then
signals will print to console and hit Telegram as they fire. Leave it
running for at least a few M15 candle closes to see it work end-to-end.

## 4. Deploy somewhere always-on

This is a small always-on Node process — pick whichever is easiest for you:

**Railway / Render (easiest, free tier available)**
1. Push this folder to a GitHub repo.
2. Railway: New Project → Deploy from GitHub → set the env vars from
   `.env.example` in the dashboard → deploy.
   Render: New → Web Service → same idea, set **Start Command** to
   `npm start`.
3. Both platforms expect a web service to bind a port — that's what the
   tiny built-in health-check server (`GET /`) is for. Nothing to configure.

**A VPS you already have / a spare always-on machine**
```
npm install
npm install -g pm2
pm2 start scanner.js --name deriv-scanner
pm2 save
pm2 startup   # follow the printed instructions so it survives reboots
```

**Note on Jamaica-based hosting/connectivity:** none of the above require
a local server — Railway/Render run in the cloud, so your own internet
connection or power doesn't need to stay up. If you'd rather self-host on
a Raspberry Pi or similar for cost reasons, `pm2` is the way to keep it
alive across reboots.

## What it does differently from the browser version

- No UI — it's a headless process. Check signals via Telegram or the
  console logs.
- `GET /` on whatever `PORT` is set returns a small JSON health check
  (symbol count, WS state) — purely so hosting platforms see it as "alive".
- Same symbol resolution (dynamic via `active_symbols`, not hardcoded
  codes), same EMA/ATR math, same pullback/reclaim state machine, same
  "Important Rule" (armed setups only cancel on an H4 flip, not H1).

## Tuning

All strategy settings are environment variables — edit `.env` (or the
host's dashboard) and restart the process. No code changes needed:

| Variable | Default | Meaning |
|---|---|---|
| `EMA20_LEN` / `EMA50_LEN` / `EMA200_LEN` | 20/50/200 | HTF trend EMA lengths |
| `ATR_PERIOD` | 14 | M15 ATR period |
| `ATR_MULT` | 1.0 | Minimum pullback distance, in ATR multiples |
| `MIN_PULLBACK` | 3 | Minimum consecutive pullback candles |
| `ALLOW_MIXED_H1` | false | Allow H1 Mixed when H4 is aligned |
