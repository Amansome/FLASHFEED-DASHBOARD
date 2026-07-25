# Project

FlashFeed aggregates news, social sentiment, screener data, and momentum signals using Docker, MongoDB, Redis, Kafka, and a React frontend.

**Live:** https://backend-production-da72.up.railway.app — the Railway `backend` service serves both the dashboard and the API from one origin (see `Infrastructure/server/Dockerfile`). Commits on `main` deploy automatically.

---

## Quick Start (Local Development with Docker)

> ⚠️ This is the standard local setup that runs on your own machine. It uses Docker Compose with all services including Kafka.

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Node.js](https://nodejs.org/) v18+ installed

### Step 1: Clone and enter the project
```bash
git clone https://github.com/Rybread15325/FlashFeedCapstone2026.git
cd FlashFeedCapstone2026
git checkout OtisMur_CurrentCode
```

### Step 2: Create your environment file
```bash
cp .env.example .env
```

No changes needed — the defaults work out of the box.

### Step 3: Start all Docker services
```bash
docker compose up -d mongo redis zookeeper kafka kafka-init kafka-consumer backend
```

This starts:
| Service | Purpose | Port |
|---------|---------|------|
| **MongoDB** | Database for articles, social posts, screeners | 27017 |
| **Redis** | Caching layer for fast data access | 6379 |
| **Zookeeper** | Kafka coordination | 2181 |
| **Kafka** | Event streaming for real-time updates | 9092 |
| **Kafka Consumer** | Processes streamed events | — |
| **Backend** | REST API server | 3001 |

### Step 4: Start the frontend
```bash
cd app
npm install
npm run dev
```

### Step 5: Open the dashboard
Visit **http://localhost:5173** in your browser.

---

## Architecture

```
Frontend (Vite+React)  →  Backend (Express)  →  MongoDB
                              ↕                      
                            Redis (Cache)             
                              ↕                      
                            Kafka (Events)            
```

### Data Pipeline
1. **RSS Workers** fetch news from PR Newswire, GlobeNewswire, BusinessWire
2. **Screener Workers** pull data from Finviz Elite, TradingView
3. **Social Workers** collect posts from Reddit, StockTwits, Bluesky
4. **Sentiment Engine** classifies articles and social posts
5. **All data** flows through Kafka → processed → stored in MongoDB → cached in Redis

### Dashboard Pages
| Page | Description |
|------|-------------|
| **Overview** | Market status, recent articles, sentiment stats |
| **News** | Financial news with sentiment badges |
| **Screener** | Stock screener with fundamental & technical filters |
| **Social Feed** | Live social media posts with ticker matching |
| **Charts** | Candlestick charts with Bollinger Bands, RSI, MACD |
| **Momentum** | Top momentum movers with trade watch signals |
| **Correlation** | News-to-price correlation analysis |
| **Settings** | Manage keywords, sources, API connections |

---

## Configuration

### Environment Variables (`.env`)

All variables are optional with sensible defaults. The dashboard works without setting any API keys.

| Variable | Default | Purpose |
|----------|---------|---------|
| `CORS_ORIGIN` | `*` | Allowed origins for browser API calls |
| `DEFAULT_FETCH_MODE` | `fast` | `fast` or `full` data refresh mode |
| `MONGO_SERVER_SELECTION_TIMEOUT_MS` | `3000` | MongoDB connection timeout |

### API Keys (optional)

Only needed for premium/authenticated data sources:

| Variable | Source |
|----------|--------|
| `FINVIZ_AUTH_TOKEN` | Finviz Elite screener |
| `BENZINGA_API_KEY` | Benzinga news |
| `X_BEARER_TOKEN` | X/Twitter API |
| `REDDIT_CLIENT_ID` | Reddit API |
| `SCHWAB_ACCESS_TOKEN` | Charles Schwab |

---

## Team Access

To share your running dashboard with someone on the same network:

1. **Find your local IP:**
   ```bash
   ipconfig getifaddr en0   # macOS
   ipconfig                  # Windows
   ```

2. **They open** `http://YOUR_IP:5173` in their browser — no setup needed on their end.

---

## Troubleshooting

### `docker compose` command not found
Use `docker-compose` (with hyphen) on older Docker versions.

### Port already in use
Stop existing services on ports 3001, 5173, 27017, 6379, or 9092.

### Blank page or API errors
- Check running containers: `docker compose ps`
- View backend logs: `docker logs feedflash-backend`
- The Vite dev server proxies `/api` requests to `http://localhost:3001`

### CORS errors
Default `CORS_ORIGIN=*` allows all origins. To restrict:
```bash
CORS_ORIGIN=http://your-domain.com,http://localhost:5173
```

---

## Data Fetching

Click the **fetch button** in the top bar for a fast data refresh. For a full refresh:

```bash
curl -X POST "http://localhost:3001/api/fetch?mode=full"
```

Run the social collector for specific tickers:
```bash
docker exec feedflash-backend python3 1_News/pipeline/fetch_social_to_mongo.py
```

---


| Service | Check Command |
|---------|---------------|
| Redis | `docker exec feedflash-redis redis-cli ping` |
| Kafka | `docker exec feedflash-kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| MongoDB | `docker exec feedflash-mongo mongosh --eval "db.runCommand({ ping: 1 })" --quiet` |
| Backend | `curl http://localhost:3001/api/health` |
