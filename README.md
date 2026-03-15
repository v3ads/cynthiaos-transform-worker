# cynthiaos-transform-worker

CynthiaOS Silver transformation worker. Runs on a cron schedule (`*/5 * * * *`), reads unprocessed Bronze events, normalises them into Silver entity records, and writes dead-letter entries for any events that fail transformation.

> **Status:** Scaffold only (TASK-011). Business logic is not yet implemented.

## Quick Start

```bash
cp .env.example .env
npm install
npm run dev
```

Health check: `GET http://localhost:3002/health`

## Scripts

| Command | Purpose |
|---------|---------|
| `npm run dev` | Run with hot-reload (ts-node-dev) |
| `npm run build` | Compile TypeScript to `dist/` |
| `npm start` | Run compiled output |
| `npm run typecheck` | Type-check without emitting |

## Docker

```bash
docker build -t cynthiaos-transform-worker .
docker run -p 3002:3002 --env-file .env cynthiaos-transform-worker
```

## Railway Deployment

- **Start command:** `node dist/index.js`
- **Build command:** `npm ci && npm run build`
- **Port:** Set `PORT` environment variable in Railway dashboard
- **Cron trigger:** Configured in Railway as `*/5 * * * *`

## Environment Variables

See `.env.example` for all required variables.
