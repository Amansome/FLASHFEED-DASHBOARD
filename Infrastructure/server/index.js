import 'dotenv/config'
import express from 'express'
import mongoose from 'mongoose'
import cors    from 'cors'
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { connectDB } from './db.js'
import Redis from 'ioredis'
import * as diskdb from './diskdb.js'   // hard-disk (on-disk SQLite) news store + retention sweeper

import articlesRouter    from './routes/articles.js'
import screenerRouter    from './routes/screener.js'
import entryScreenerRouter from './routes/entryScreener.js'
import exitScreenerRouter from './routes/exitScreener.js'
import v11ScreenerRouter from './routes/v11Screener.js'
import socialRouter      from './routes/social.js'
import correlationRouter from './routes/correlation.js'
import settingsRouter    from './routes/settings.js'
import decisionMapRouter from './routes/decisionMap.js'
import { approvedNewsSourceMongoFilter } from './sourceFilter.js'
import { dedupeWatcherSeries, loadWatcherFeatureMap, persistWatcherSnapshot } from './lib/watcherSnapshots.js'
import { normalizeRollingWindowMinutes, recordIsInsideRollingWindow, sliceCandlesToRollingWindow } from './lib/rollingWindow.js'
import * as predictionThresholdPolicy from './lib/predictionThresholdPolicy.js'

const app  = express()
const PORT = process.env.PORT || 3001
const SERVER_DIR = path.dirname(fileURLToPath(import.meta.url))
const PROJECT_ROOT = path.resolve(SERVER_DIR, '../..')
function readConfigJson(filename) {
  const candidates = [
    path.join(process.cwd(), 'config', filename),
    path.join(SERVER_DIR, 'config', filename),
    path.join(PROJECT_ROOT, 'config', filename),
  ]
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return JSON.parse(fs.readFileSync(candidate, 'utf8'))
    }
  }
  throw new Error(`Config file not found: ${filename}`)
}
const MARKET_WINDOW_TIME_ZONE = process.env.MARKET_WINDOW_TIMEZONE || 'America/New_York'
const MARKET_WINDOW_CLOSE_HOUR = Number(process.env.MARKET_WINDOW_CLOSE_HOUR_ET || 17)
const TRACKED_TICKER_FILE_CANDIDATES = [
  path.join(process.cwd(), 'config', 'social_tickers_100.txt'),
  path.join(PROJECT_ROOT, 'config', 'social_tickers_100.txt'),
  path.join(SERVER_DIR, 'config', 'social_tickers_100.txt'),
]
const TRACKED_TICKER_LIMIT = Math.max(1, Number(process.env.TRACKED_TICKER_LIMIT || process.env.SOCIAL_MAX_TICKERS || 250))
const NON_STOCK_TICKERS = new Set([
  "BTC", "ETH", "LTC", "DOGE", "SOL", "ADA", "XRP", "BNB", "DOT", "AVAX",
  "MATIC", "SHIB", "TRX", "BCH", "LINK", "ATOM", "UNI", "ETC", "FIL",
  "USD", "USDT", "USDC", "SPOT",
])
const US_EXCHANGES = new Set(["NASDAQ", "NYSE", "AMEX"])
const TRACKED_MARKET_INDICES = [
  { symbol: "DJI", name: "Dow Jones Industrial Average", category: "index" },
  { symbol: "SPX", name: "S&P 500", category: "index" },
  { symbol: "IXIC", name: "Nasdaq Composite", category: "index" },
  { symbol: "RUT", name: "Russell 2000 Index", category: "index" },
  { symbol: "NYA", name: "NYSE Composite", category: "index" },
]
const TRACKED_MARKETS = [
  ...Array.from(US_EXCHANGES).map(symbol => ({ symbol, name: `${symbol} listed equities`, category: "exchange" })),
  ...TRACKED_MARKET_INDICES,
]
const MAX_SIGNAL_CHANGE_PCT = Math.max(10, Number(process.env.MAX_SIGNAL_CHANGE_PCT || 300))
const PRIVATE_TRACKED_TICKERS = new Set(['SPACEX'])
const MIN_LIVE_MODEL_CONFIDENCE = Math.max(0, Math.min(1, Number(process.env.MIN_LIVE_MODEL_CONFIDENCE || 0.05)))
const WATCHER_SNAPSHOT_ENABLED = !['0', 'false', 'no'].includes(String(process.env.WATCHER_SNAPSHOT_ENABLED || 'true').toLowerCase())
const WATCHER_SNAPSHOT_INTERVAL_MS = Math.max(5 * 60_000, Number(process.env.WATCHER_SNAPSHOT_INTERVAL_MS || 10 * 60_000))
const WATCHER_SNAPSHOT_MIN_TICKER_INTERVAL_SEC = Math.max(5 * 60, Number(process.env.WATCHER_SNAPSHOT_MIN_TICKER_INTERVAL_SEC || 20 * 60))
const WATCHER_SNAPSHOT_BATCH_SIZE = Math.max(1, Math.min(75, Number(process.env.WATCHER_SNAPSHOT_BATCH_SIZE || 25)))
const WATCHER_SNAPSHOT_REQUEST_DELAY_MS = Math.max(0, Number(process.env.WATCHER_SNAPSHOT_REQUEST_DELAY_MS || 650))
const WATCHER_SNAPSHOT_TOP_MOVER_POOL = Math.max(WATCHER_SNAPSHOT_BATCH_SIZE, Math.min(250, Number(process.env.WATCHER_SNAPSHOT_TOP_MOVER_POOL || 100)))
const WATCHER_SNAPSHOT_MAX_AGE_SECONDS = Math.max(3600, Number(process.env.WATCHER_SNAPSHOT_MAX_AGE_SECONDS || 72 * 60 * 60))
const WATCHER_GROWTH_WINDOW_SECONDS = Math.max(3600, Number(process.env.WATCHER_GROWTH_WINDOW_SECONDS || 24 * 60 * 60))
const watcherSnapshotStatus = {
  enabled: WATCHER_SNAPSHOT_ENABLED,
  running: false,
  lastStartedAt: null,
  lastFinishedAt: null,
  lastError: null,
  lastTickers: [],
  lastCaptured: 0,
  lastSkippedRecent: 0,
}

// ── Middleware ────────────────────────────────────────────
const CORS_ORIGINS = (process.env.CORS_ORIGINS || 'http://localhost:5173,http://127.0.0.1:5173,http://localhost:5174,http://127.0.0.1:5174')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)
app.use(cors({ origin: CORS_ORIGINS }))
app.use(express.json({ limit: '2mb' }))

// ── RAM speed layer: Redis hot cache + Kafka→Redis feed reads ─────────────────
// Redis holds (a) a short-TTL cache of the expensive Mongo aggregations so the
// dashboard reads from RAM, and (b) the per-ticker hot window the Kafka consumer
// streams in (feed:{TICKER} ZSet → event:{id} hashes). If Redis is unavailable,
// every path transparently falls back to MongoDB — the app never breaks.
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
let redis = null
try {
  redis = new Redis(REDIS_URL, {
    lazyConnect: true,
    maxRetriesPerRequest: 2,
    enableOfflineQueue: false,
    enableAutoPipelining: true,
    keepAlive: 30000,
    retryStrategy: (n) => (n > 10 ? null : Math.min(n * 200, 2000)),
  })
  redis.on('error', () => {})                       // quiet; usage is guarded by status check
  redis.on('ready', () => console.log('  Redis   →  connected (RAM cache + hot feed active)'))
  redis.connect().catch(() => console.warn('  Redis   →  not reachable; serving from MongoDB only'))
} catch (e) {
  console.warn('  Redis   →  disabled:', e.message)
  redis = null
}
const redisReady = () => !!redis && redis.status === 'ready'

// Expose Redis to route handlers via app.locals so they can read from RAM cache
app.locals.redis = redis
app.locals.redisReady = redisReady
app.locals.redisUrl = REDIS_URL

// Transparent response cache for the heaviest GETs — identical JSON shape, served
// from RAM within the TTL window. Only successful (200) responses are cached.
const CACHE_RULES = [
  { match: (p) => p === '/api/screener',        ttl: Number(process.env.CACHE_TTL_SCREENER || 20) },
  { match: (p) => p === '/api/social/rolling',  ttl: Number(process.env.CACHE_TTL_SOCIAL || 15) },
  { match: (p) => p.startsWith('/api/charts/'), ttl: Number(process.env.CACHE_TTL_CHARTS || 20) },
  { match: (p) => p === '/api/momentum',        ttl: Number(process.env.CACHE_TTL_MOMENTUM || 15) },
  { match: (p) => p === '/api/correlation',     ttl: Number(process.env.CACHE_TTL_CORRELATION || 30) },
  { match: (p) => p === '/api/articles',        ttl: Number(process.env.CACHE_TTL_ARTICLES || 15) },
  { match: (p) => p.startsWith('/api/ai/'),     ttl: Number(process.env.CACHE_TTL_AI || 60) },
  { match: (p) => p === '/api/prediction/features', ttl: Number(process.env.CACHE_TTL_PREDICTION_FEATURES || 20) },
  { match: (p) => p === '/api/prediction/signals',  ttl: Number(process.env.CACHE_TTL_PREDICTION_SIGNALS || 20) },
  { match: (p) => p === '/api/prediction/active-context', ttl: Number(process.env.CACHE_TTL_PREDICTION_ACTIVE_CONTEXT || 20) },
  { match: (p) => p === '/api/prediction/audit',    ttl: Number(process.env.CACHE_TTL_PREDICTION_AUDIT || 45) },
  { match: (p) => p === '/api/prediction/model',    ttl: Number(process.env.CACHE_TTL_PREDICTION_MODEL || 120) },
  { match: (p) => p === '/api/prediction/replay',   ttl: Number(process.env.CACHE_TTL_PREDICTION_REPLAY || 120) },
]
const cacheTtlFor = (p) => { const r = CACHE_RULES.find((rule) => rule.match(p)); return r ? r.ttl : 0 }
const responseCacheNamespace = (path) => path.startsWith('/api/prediction/')
  ? predictionThresholdPolicy.predictionPolicyCacheNamespace()
  : 'default'
app.use(async (req, res, next) => {
  if (req.method !== 'GET' || !redisReady()) return next()
  if (req.query?.fresh === '1') return next()
  const ttl = cacheTtlFor(req.path)
  if (!ttl) return next()
  const key = `cache:${responseCacheNamespace(req.path)}:${req.originalUrl}`
  try {
    const hit = await redis.get(key)
    if (hit) {
      res.set('X-Cache', 'HIT')
      res.set('X-Cache-Namespace', responseCacheNamespace(req.path))
      if (req.path === '/api/momentum') {
        try {
          const parsed = JSON.parse(hit)
          return res.json({
            ...parsed,
            cacheMode: 'redis',
            cacheHit: true,
            cacheStore: 'redis-response-cache',
          })
        } catch (_) {
          return res.type('application/json').send(hit)
        }
      }
      return res.type('application/json').send(hit)
    }
  } catch (_) { /* fall through to compute from Mongo */ }
  const sendJson = res.json.bind(res)
  res.json = (body) => {
    if (res.statusCode === 200) {
      res.set('X-Cache', 'MISS')
      res.set('X-Cache-Namespace', responseCacheNamespace(req.path))
      try { redis.set(key, JSON.stringify(body), 'EX', ttl).catch(() => {}) } catch (_) {}
    }
    return sendJson(body)
  }
  next()
})

const ENDPOINT_TIMING_LOG_MS = Math.max(50, Number(process.env.ENDPOINT_TIMING_LOG_MS || 750))
app.use((req, res, next) => {
  const start = process.hrtime.bigint()
  res.on('finish', () => {
    if (!req.path.startsWith('/api/')) return
    const durationMs = Number(process.hrtime.bigint() - start) / 1e6
    if (durationMs < ENDPOINT_TIMING_LOG_MS && res.statusCode < 500) return
    console.log(JSON.stringify({
      type: 'endpoint_timing',
      method: req.method,
      path: req.path,
      status: res.statusCode,
      duration_ms: Number(durationMs.toFixed(1)),
      cache: res.getHeader('X-Cache') || null,
      cache_namespace: res.getHeader('X-Cache-Namespace') || null,
      policy_version: req.path.startsWith('/api/prediction/') ? PREDICTION_THRESHOLD_POLICY_VERSION : null,
    }))
  })
  next()
})

// GET /api/feed/:ticker — RAM-speed read of the per-ticker hot window that the
// Kafka consumer streams into Redis (feed:{TICKER} ZSet → event:{id} hashes).
app.get('/api/feed/:ticker', async (req, res) => {
  const ticker = String(req.params.ticker || '').toUpperCase().trim()
  const limit = Math.min(Number(process.env.ARTICLES_MAX_LIMIT || 5000), Math.max(1, Number(req.query.limit) || 5000))
  if (!ticker) return res.status(400).json({ error: 'ticker required' })
  if (!redisReady()) {
    return res.status(503).json({ ticker, source: 'none', count: 0, events: [],
      note: 'Redis not connected — start Redis and the Kafka consumer to use the hot feed.' })
  }
  try {
    const ids = await redis.zrevrange(`feed:${ticker}`, 0, limit - 1)
    if (!ids || !ids.length) return res.json({ ticker, source: 'redis', count: 0, events: [] })
    const pipe = redis.pipeline()
    ids.forEach((id) => pipe.hgetall(`event:${id}`))
    const rows = await pipe.exec()
    const events = rows
      .map(([err, h]) => (err || !h || Object.keys(h).length === 0) ? null : h)
      .filter(Boolean)
      .map((h) => {
        let s = Number(h.sentiment_score)
        if (Number.isNaN(s) && h.payload) { try { s = Number(JSON.parse(h.payload).sentiment_score) } catch (_) {} }
        return { ...h, sentiment_score: Number.isNaN(s) ? null : s }
      })
    const svals = events.map((e) => e.sentiment_score).filter((n) => typeof n === 'number')
    const avg = svals.length ? +(svals.reduce((a, b) => a + b, 0) / svals.length).toFixed(3) : null
    res.json({ ticker, source: 'redis', count: events.length, avg_sentiment: avg, events })
  } catch (e) {
    res.status(500).json({ ticker, error: e.message, events: [] })
  }
})

// ── AI analysis: directional scores + market overview from recent news ────────
// The "AI score" aggregates the per-article sentiment (already produced by the
// FinBERT + LLM sentiment stage) over the last few days into a directional
// -100..+100 score per ticker. Results are cached in Redis (RAM) for speed.
const AI_ARTICLES_COLLECTION = process.env.ARTICLES_COLLECTION || 'articles'
function aiTimestampMs(value) {
  if (!value) return 0
  if (value instanceof Date) return value.getTime()
  const n = Number(value)
  if (Number.isFinite(n) && n > 0) return n > 1_000_000_000_000 ? n : n * 1000
  const ms = Date.parse(String(value))
  return Number.isFinite(ms) ? ms : 0
}
function aiArticleTimeMs(a) {
  return aiTimestampMs(a.publish_date) || aiTimestampMs(a.published_at) || aiTimestampMs(a.fetched_date) ||
    aiTimestampMs(a.detected_at) || aiTimestampMs(a.createdAt) || aiTimestampMs(a.updatedAt)
}

function aiTickerTaggedArticleMatch() {
  return {
    $or: [
      { ticker: { $exists: true, $nin: ["", null] } },
      { tickers: { $exists: true, $nin: [[], "", null] } },
      { symbol: { $exists: true, $nin: ["", null] } },
      { symbols: { $exists: true, $nin: [[], "", null] } },
    ],
  }
}

async function aiRecentArticles(db, days) {
  const cutoffMs = Date.now() - Math.max(1, days) * 86_400_000
  const projection = {
    ticker: 1, tickers: 1, symbol: 1, symbols: 1,
    sentiment: 1, sentiment_score: 1, finbert_score: 1, vader_score: 1, gemini_sentiment: 1,
    ml_confidence: 1, title: 1, source: 1,
    publish_date: 1, published_at: 1, fetched_date: 1, detected_at: 1, createdAt: 1, updatedAt: 1,
  }
  const tickerTagged = aiTickerTaggedArticleMatch()
  const recentDocs = await db.collection(AI_ARTICLES_COLLECTION)
    .find({ $and: [recentArticleMatch(days), tickerTagged] }, { projection })
    .sort({ _id: -1 })
    .limit(8000)
    .toArray()

  const usable = recentDocs.filter(a => aiTickers(a).length && aiSentiment(a) !== null)
  const recent = usable.filter(a => {
    const ts = aiArticleTimeMs(a)
    return ts > 0 && ts >= cutoffMs
  })
  if (recent.length) return recent.slice(0, 8000)

  // If the stored timestamps are older/malformed, still show the latest real
  // ticker-tagged rows instead of a dead 0-count AI panel.
  const fallbackDocs = await db.collection(AI_ARTICLES_COLLECTION)
    .find(tickerTagged, { projection })
    .sort({ _id: -1 })
    .limit(5000)
    .toArray()
  return fallbackDocs.filter(a => aiTickers(a).length && aiSentiment(a) !== null).slice(0, 5000)
}
function aiSentiment(a) {
  let v = a.sentiment_score ?? a.finbert_score ?? a.vader_score ?? a.gemini_sentiment
  if (v == null) v = a.sentiment                       // string label fallback ("bullish"/"bearish"/"neutral")
  if (typeof v === 'string') {
    const s = v.toLowerCase()
    if (s.includes('bull') || s.includes('positive')) return 0.6
    if (s.includes('bear') || s.includes('negative')) return -0.6
    if (s.includes('neutral')) return 0
    const n = parseFloat(v); return Number.isFinite(n) ? n : null
  }
  return Number.isFinite(v) ? v : null
}
function aiTickers(a) {
  // articles store `ticker` as a comma-separated string, e.g. "AAPL,MSFT"
  const out = []
  const push = (val) => String(val || '').split(/[,\s]+/).forEach((t) => {
    const k = t.trim().toUpperCase()
    if (k && k.length <= 6 && /^[A-Z][A-Z0-9.\-]*$/.test(k)) out.push(k)
  })
  if (Array.isArray(a.tickers)) a.tickers.forEach(push)
  else if (a.tickers) push(a.tickers)
  if (a.ticker) push(a.ticker)
  if (a.symbol) push(a.symbol)
  if (Array.isArray(a.symbols)) a.symbols.forEach(push)
  else if (a.symbols) push(a.symbols)
  return Array.from(new Set(out)).filter(t => !NON_STOCK_TICKERS.has(t))
}
function aiScoreTickers(arts) {
  const m = new Map()
  for (const a of arts) {
    const s = aiSentiment(a); if (s === null) continue
    for (const t of aiTickers(a)) {
      const k = String(t).toUpperCase().trim(); if (!k || k.length > 8) continue
      const e = m.get(k) || { sum: 0, n: 0, pos: 0, neg: 0 }
      e.sum += s; e.n += 1; if (s > 0.15) e.pos += 1; else if (s < -0.15) e.neg += 1
      m.set(k, e)
    }
  }
  return m
}

app.get('/api/ai/scores', async (req, res) => {
  const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
  const limit = Math.min(Number(process.env.ARTICLES_MAX_LIMIT || 5000), Math.max(1, Number(req.query.limit) || 5000))
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ error: 'MongoDB not connected', scores: [] })
    const arts = await aiRecentArticles(db, days)
    const scored = [...aiScoreTickers(arts).entries()]
      .map(([ticker, e]) => {
        const avg = e.sum / e.n
        const score = Math.round(Math.max(-100, Math.min(100, avg * 100)))
        return {
          ticker, score,
          direction: score > 8 ? 'up' : score < -8 ? 'down' : 'flat',
          confidence: +Math.min(1, e.n / 20).toFixed(2),
          article_count: e.n, bullish: e.pos, bearish: e.neg,
        }
      })
      .filter((x) => x.article_count >= 1)
      .sort((a, b) => Math.abs(b.score) - Math.abs(a.score))
      .slice(0, limit)
    res.json({ days, generated_at: Date.now(), model: 'news-sentiment-aggregate', scores: scored })
  } catch (e) {
    res.status(500).json({ error: e.message, scores: [] })
  }
})

app.get('/api/ai/overview', async (req, res) => {
  const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ error: 'MongoDB not connected' })
    const arts = await aiRecentArticles(db, days)
    const sents = arts.map(aiSentiment).filter((n) => n !== null)
    const avg = sents.length ? sents.reduce((a, b) => a + b, 0) / sents.length : 0
    const ranked = [...aiScoreTickers(arts).entries()]
      .filter(([, e]) => e.n >= 1)
      .map(([ticker, e]) => ({ ticker, avg: e.sum / e.n, n: e.n }))
    const bull = [...ranked].sort((a, b) => b.avg - a.avg).slice(0, 5)
    const bear = [...ranked].sort((a, b) => a.avg - b.avg).slice(0, 5)
    const mood = avg > 0.1 ? 'risk-on' : avg < -0.1 ? 'risk-off' : 'mixed'
    const summary =
      `Across ${arts.length} ticker-tagged articles in the last ${days} day(s), overall news sentiment is ` +
      `${mood} (avg ${avg.toFixed(2)}). ` +
      (bull.length ? `Strongest positive coverage: ${bull.map((b) => b.ticker).join(', ')}. ` : '') +
      (bear.length ? `Most negative: ${bear.map((b) => b.ticker).join(', ')}.` : '')
    res.json({
      days, generated_at: Date.now(), model: 'news-sentiment-aggregate',
      article_count: arts.length, avg_sentiment: +avg.toFixed(3), mood, summary,
      top_bullish: bull.map((b) => ({ ticker: b.ticker, score: Math.round(b.avg * 100), article_count: b.n })),
      top_bearish: bear.map((b) => ({ ticker: b.ticker, score: Math.round(b.avg * 100), article_count: b.n })),
    })
  } catch (e) {
    res.status(500).json({ error: e.message })
  }
})

app.get('/api/ai/rankings', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: 'MongoDB not connected' })
    const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
    const limit = Math.min(100, Math.max(1, Number(req.query.limit) || 50))
    const socialWindow = Math.min(4320, Math.max(5, Number(req.query.window_minutes) || 1440))
    const minScore = Math.max(0, Math.min(100, Number(req.query.min_score) || 0))
    const [arts, tradeRows, model, signalRows] = await Promise.all([
      aiRecentArticles(db, days),
      loadEnrichedTradeWatchRows(db, { limit: Math.max(limit, 30), days, socialWindow }),
      loadLatestPredictionModel(db),
      db.collection('prediction_signals').find({}, {
        projection: { ticker: 1, signal_sec: 1, decision: 1, baseline_signal: 1, threshold_rule_signal: 1, model_signal: 1, label_status: 1, labels: 1 },
      }).sort({ signal_sec: -1 }).limit(500).toArray().catch(() => []),
    ])
    const validationMap = await loadPredictionValidationForTickers(db, tradeRows.map(row => row.ticker))
    const newsMap = aiScoreTickers(arts)
    const latestSignalByTicker = new Map()
    for (const row of signalRows) {
      const ticker = String(row.ticker || '').toUpperCase()
      if (ticker && !latestSignalByTicker.has(ticker)) latestSignalByTicker.set(ticker, row)
    }
    const rows = tradeRows.map((row, index) => {
      const ticker = String(row.ticker || '').toUpperCase()
      const validationRow = validationMap.get(ticker)
      const scoredRow = validationRow
        ? {
            ...row,
            correlation_score: validationRow.correlation_score ?? row.correlation_score,
            prediction_validation_correlation: validationRow.prediction_validation_correlation ?? row.prediction_validation_correlation,
            prediction_validation_accuracy_5m: validationRow.prediction_validation_accuracy_5m ?? row.prediction_validation_accuracy_5m,
            prediction_validation_samples: validationRow.prediction_validation_samples ?? row.prediction_validation_samples,
            prediction_validation_correct: validationRow.prediction_validation_correct ?? row.prediction_validation_correct,
            prediction_validation_avg_return_5m: validationRow.prediction_validation_avg_return_5m ?? row.prediction_validation_avg_return_5m,
            prediction_validation_latest_signal_sec: validationRow.prediction_validation_latest_signal_sec ?? row.prediction_validation_latest_signal_sec,
          }
        : row
      const news = newsMap.get(ticker) || { sum: 0, n: 0, pos: 0, neg: 0 }
      const newsAvg = news.n ? news.sum / news.n : Number(scoredRow.article_sentiment || 0)
      const newsScore = clamp((newsAvg + 1) / 2)
      const tradeScore = Number(scoredRow.trade_watch?.trade_watch_score || 0)
      const socialCount = Number(scoredRow.message_count || 0)
      const articleCount = Number(scoredRow.article_count || 0)
      const newsArticleCount = Math.max(articleCount, Number(news.n || 0))
      const evidenceScore = Number(scoredRow.trade_watch?.evidence_score || 0)
      const socialDensity = clamp(Math.log1p(socialCount) / Math.log1p(80))
      const features = predictionFeaturesFromMover(scoredRow, socialWindow)
      const thresholdEntry = evaluatePredictionEntryThreshold(scoredRow, features)
      const thresholdRuleSignal = thresholdRulePredictionFromEntry(scoredRow, thresholdEntry)
      const modelSignal = applyPredictionModel(features, model)
      const baselineSignal = baselinePredictionFromMover(scoredRow, thresholdEntry)
      const storedSignal = latestSignalByTicker.get(ticker)
      const usableModelSignal = isLiveModelSignalEligible(modelSignal, model) ? modelSignal : null
      const usableThresholdRuleSignal = thresholdRuleSignal?.entry_ready ? thresholdRuleSignal : null
      const usableStoredThresholdRuleSignal = storedSignal?.threshold_rule_signal?.entry_ready ? storedSignal.threshold_rule_signal : null
      const usableStoredModelSignal = isLiveModelSignalEligible(storedSignal?.model_signal, model) ? storedSignal.model_signal : null
      const activeSignal = usableThresholdRuleSignal || usableStoredThresholdRuleSignal || usableModelSignal || usableStoredModelSignal || storedSignal?.baseline_signal || baselineSignal
      const probabilityUp = Number(activeSignal?.probability_up)
      const modelDirectionBoost = activeSignal?.direction === 'up' ? 0.08 : activeSignal?.direction === 'down' ? -0.08 : 0
      const predictionScore = Number.isFinite(probabilityUp) ? clamp(probabilityUp) : activeSignal?.direction === 'up' ? 0.62 : activeSignal?.direction === 'down' ? 0.38 : 0.5
      const quoteFreshness = Number(scoredRow.trade_watch?.quote_freshness ?? 0.5)
      const densityCorrelationRaw = nullableNumber(features.price_density_correlation)
      const densityCorrelation = densityCorrelationRaw == null ? null : clamp(densityCorrelationRaw, -1, 1)
      const densityCorrelationComponent = densityCorrelation == null ? 0.5 : (densityCorrelation + 1) / 2
      const validationCorrelation = clamp(Number(features.prediction_validation_correlation || features.correlation_score || 0), -1, 1)
      const validationAccuracy = nullableNumber(features.prediction_validation_accuracy_5m)
      const validationReturn = nullableNumber(features.prediction_validation_avg_return_5m)
      const validationSamples = nullableNumber(features.prediction_validation_samples)
      const validationEdge = clamp(
        (Number.isFinite(validationAccuracy) ? Math.max(0, validationAccuracy - 0.5) * 1.4 : 0) +
        Math.max(0, validationCorrelation) * 0.25 +
        (Number.isFinite(validationReturn) ? Math.max(0, validationReturn) / 3 : 0),
        0,
        1,
      )
      const densitySetupScore = clamp(Number(features.threshold_setup_score ?? thresholdEntry?.setupScore ?? 0) / 100)
      const densityStatus = String(features.threshold_setup_status || thresholdEntry?.setupStatus || thresholdEntry?.status || '')
      const densitySetupActive = ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(densityStatus)
      const positiveCatalyst = Boolean(
        (newsArticleCount > 0 && newsAvg > 0.05) ||
        (socialCount > 0 && Number(scoredRow.social_sentiment || 0) > 0.08) ||
        densitySetupActive ||
        (densityCorrelation != null && densityCorrelation > 0.34) ||
        validationEdge > 0.10 ||
        Number(features.is_news_catalyst || 0) === 1
      )
      const technicalConfirmation = positiveCatalyst ? clamp(
        (Number(features.rsi || 50) >= 38 && Number(features.rsi || 50) <= 68 ? 0.35 : 0) +
        (Number(features.rsi_oversold || 0) * 0.20) +
        (Number(scoredRow.change_pct || 0) >= -4 && Number(scoredRow.change_pct || 0) <= 12 ? 0.20 : 0) +
        (Number(scoredRow.rel_volume || 0) >= 1.25 ? 0.25 : 0),
        0,
        1,
      ) : 0
      const blended = clamp(
        tradeScore * 0.20 + newsScore * 0.16 + evidenceScore * 0.13 + socialDensity * 0.08 +
        predictionScore * 0.15 + quoteFreshness * 0.05 + densityCorrelationComponent * 0.08 +
        densitySetupScore * 0.06 + validationEdge * 0.05 + technicalConfirmation * 0.04 + modelDirectionBoost
      )
      const aiRankScore = Number((blended * 100).toFixed(1))
      const bullishEvidence = positiveCatalyst && Number(scoredRow.change_pct || 0) >= 0 && (aiRankScore >= 58 || tradeScore >= 0.65)
      const direction = bullishEvidence ? 'bullish' : aiRankScore <= 38 || activeSignal?.direction === 'down' ? 'bearish' : 'watch'
      return {
        rank_seed: index + 1,
        ticker,
        company: scoredRow.company || '',
        price: scoredRow.price ?? null,
        change_pct: Number(scoredRow.change_pct || 0),
        rel_volume: Number(scoredRow.rel_volume || 0),
        volume: Number(scoredRow.volume || 0),
        ai_rank_score: aiRankScore,
        direction,
        confidence: Number(Math.abs(blended - 0.5).toFixed(3)),
        trade_watch_score: Number(tradeScore.toFixed(3)),
        prediction_signal: {
          direction: activeSignal?.direction || (predictionScore >= 0.55 ? 'up' : predictionScore <= 0.45 ? 'down' : 'watch'),
          probability_up: Number.isFinite(Number(activeSignal?.probability_up)) && Number(activeSignal?.probability_up) > 0
            ? Number(activeSignal.probability_up)
            : Number(predictionScore.toFixed(3)),
          confidence: activeSignal?.confidence ?? Number(Math.abs(predictionScore - 0.5).toFixed(3)),
          predicted_return_5m: activeSignal?.predicted_return_5m ?? null,
          predicted_return_intraday_trade: activeSignal?.predicted_return_intraday_trade ?? null,
          model: activeSignal?.model || 'baseline_trade_watch_v1',
          horizon: activeSignal?.horizon || null,
          entry_ready: Boolean(activeSignal?.entry_ready || thresholdEntry?.passed),
          threshold_status: activeSignal?.threshold_status || thresholdEntry?.status || null,
          threshold_policy_version: activeSignal?.threshold_policy_version || thresholdEntry?.policyVersion || null,
          backtest_trades: activeSignal?.backtest_trades ?? null,
          backtest_profit_factor: activeSignal?.backtest_profit_factor ?? null,
          backtest_validation_mean_return_pct: activeSignal?.backtest_validation_mean_return_pct ?? null,
          backtest_test_mean_return_pct: activeSignal?.backtest_test_mean_return_pct ?? null,
        },
        model_ready: Boolean(modelSignal),
        evidence: {
          news_score: Number((newsAvg * 100).toFixed(1)),
          news_articles: newsArticleCount,
          scored_news_articles: Number(news.n || 0),
          bullish_news: Number(news.pos || 0),
          bearish_news: Number(news.neg || 0),
          social_posts: socialCount,
          social_sentiment: Number(Number(scoredRow.social_sentiment || 0).toFixed(3)),
          evidence_score: Number(evidenceScore.toFixed(3)),
          agreement: Number(scoredRow.trade_watch?.agreement || 0),
          quote_age_minutes: scoredRow.trade_watch?.quote_age_minutes ?? null,
          latest_signal_status: storedSignal?.label_status || null,
          price_density_correlation: densityCorrelation == null ? null : Number(densityCorrelation.toFixed(3)),
          density_setup_score: Number((densitySetupScore * 100).toFixed(1)),
          density_setup_status: densityStatus || null,
          validation_accuracy_5m: Number.isFinite(validationAccuracy) ? Number(validationAccuracy.toFixed(3)) : null,
          validation_samples: validationSamples == null ? null : validationSamples,
          validation_avg_return_5m: Number.isFinite(validationReturn) ? Number(validationReturn.toFixed(3)) : null,
        },
        reasons: scoredRow.trade_watch?.reasons || [],
        risks: scoredRow.trade_watch?.risks || [],
      }
    })
      .filter(row => row.ticker && row.ai_rank_score >= minScore)
      .sort((a, b) => b.ai_rank_score - a.ai_rank_score || b.evidence.news_articles - a.evidence.news_articles)
      .slice(0, limit)
      .map((row, index) => ({ ...row, rank: index + 1 }))
    const modelValidation = modelValidationState(model)
    res.json({
      ok: true,
      generated_at: new Date().toISOString(),
      model: {
        name: model?.model_id || PREDICTION_MODEL_ID,
        status: model?.status || 'baseline',
        samples: Number(model?.samples || 0),
        min_samples: model?.min_samples || Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20),
        metrics: model?.metrics || null,
        validation_status: modelValidation.status,
        validation_edge: modelValidation.edge,
        live_classifier_enabled: modelValidation.allow_live_classifier,
        live_classifier_reason: modelValidation.reason,
        threshold_rule_live_enabled: true,
        threshold_rule_live_reason: 'validated_v11_threshold_rule_live_when_entry_ready',
        fallback: 'baseline_trade_watch_v1',
      },
      methodology: {
        ranking: 'blended Trade Watch, rolling news sentiment, social density, quote freshness, correlation, gated technical confirmation, and validated prediction signal',
        scaling: 'server-side capped and cached; no browser-side scan of Mongo collections',
        provider_dependency: 'none on read path; uses stored validated model when it beats baseline, otherwise shadows it and falls back to baseline/transparent evidence',
      },
      summary: {
        rows: rows.length,
        article_window_days: days,
        scored_articles: arts.length,
        social_window_minutes: socialWindow,
        bullish: rows.filter(r => r.direction === 'bullish').length,
        bearish: rows.filter(r => r.direction === 'bearish').length,
        watch: rows.filter(r => r.direction === 'watch').length,
        model_status: model?.status || 'baseline',
        model_samples: Number(model?.samples || 0),
      },
      rows,
    })
  } catch (err) {
    console.error('GET /api/ai/rankings failed:', err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get('/api/ai/ticker/:ticker', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    const ticker = String(req.params.ticker || '').toUpperCase().replace(/[^A-Z0-9.-]/g, '')
    if (!ticker) return res.status(400).json({ ok: false, error: 'Ticker is required' })
    const days = Math.min(14, Math.max(1, Number(req.query.days) || 3))
    const socialWindow = Math.min(4320, Math.max(5, Number(req.query.window_minutes) || 1440))
    const sinceSocialSec = Math.floor(Date.now() / 1000) - socialWindow * 60
    const tickerRegex = `(^|,\\s*)${escapeRegExp(ticker)}(\\s*,|$)`
    const articleMatch = {
      ...recentArticleMatch(days),
      ...approvedNewsSourceMongoFilter('source'),
      $or: [
        { ticker: { $regex: tickerRegex, $options: 'i' } },
        { tickers: ticker },
        { tickers_mentioned: ticker },
      ],
    }
    const [movers, arts, model, articles, articleCount, socialRows, predictionRows] = await Promise.all([
      loadPositiveFinvizMoverRows(db, 200),
      aiRecentArticles(db, days),
      loadLatestPredictionModel(db),
      db.collection('articles').find(articleMatch, {
        projection: { title: 1, source: 1, sentiment: 1, sentiment_score: 1, event_type: 1, sentiment_reason: 1, publish_date: 1, fetched_date: 1, detected_at: 1, url: 1 },
      }).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(20).toArray(),
      db.collection('articles').countDocuments(articleMatch),
      db.collection('socials').aggregate([
        ...socialTimeStages(),
        { $match: { _event_sec: { $gte: sinceSocialSec }, _ticker_candidates: ticker } },
        { $sort: { _event_sec: -1 } },
        { $limit: 20 },
        { $project: { _id: 0, platform: '$_norm_platform', author: 1, text: { $ifNull: ['$text', { $ifNull: ['$content', '$title'] }] }, sentiment: 1, sentiment_score: 1, url: 1, event_sec: '$_event_sec' } },
      ]).toArray(),
      db.collection('prediction_signals').find({ ticker }, {
        projection: { _id: 0, signal_id: 1, signal_sec: 1, decision: 1, baseline_signal: 1, threshold_rule_signal: 1, model_signal: 1, label_status: 1, labels: 1, rank: 1 },
      }).sort({ signal_sec: -1 }).limit(20).toArray(),
    ])
    const mover = movers.find(row => String(row.ticker || '').toUpperCase() === ticker)
    const [articleMap, socialMap, validationMap] = await Promise.all([
      loadArticleStatsForTickers(db, [ticker], days),
      loadSocialStatsForTickers(db, [ticker], socialWindow),
      loadPredictionValidationForTickers(db, [ticker]),
    ])
    const validationRow = validationMap.get(ticker)
    const enrichedBase = mover ? mergeMoverContext(mover, articleMap.get(ticker), socialMap.get(ticker)) : null
    const enriched = enrichedBase ? addTradeWatchFields(validationRow ? {
      ...enrichedBase,
      correlation_score: validationRow.correlation_score ?? enrichedBase.correlation_score,
      prediction_validation_correlation: validationRow.prediction_validation_correlation ?? enrichedBase.prediction_validation_correlation,
      prediction_validation_accuracy_5m: validationRow.prediction_validation_accuracy_5m ?? enrichedBase.prediction_validation_accuracy_5m,
      prediction_validation_samples: validationRow.prediction_validation_samples ?? enrichedBase.prediction_validation_samples,
      prediction_validation_correct: validationRow.prediction_validation_correct ?? enrichedBase.prediction_validation_correct,
      prediction_validation_avg_return_5m: validationRow.prediction_validation_avg_return_5m ?? enrichedBase.prediction_validation_avg_return_5m,
      prediction_validation_latest_signal_sec: validationRow.prediction_validation_latest_signal_sec ?? enrichedBase.prediction_validation_latest_signal_sec,
    } : enrichedBase) : null
    const news = aiScoreTickers(arts).get(ticker) || { sum: 0, n: 0, pos: 0, neg: 0 }
    const newsAvg = news.n ? news.sum / news.n : Number(enriched?.article_sentiment || 0)
    const features = enriched ? predictionFeaturesFromMover(enriched, socialWindow) : {}
    const thresholdEntry = enriched ? evaluatePredictionEntryThreshold(enriched, features) : null
    const thresholdRuleSignal = enriched ? thresholdRulePredictionFromEntry(enriched, thresholdEntry) : null
    const modelSignal = enriched ? applyPredictionModel(features, model) : null
    const baselineSignal = enriched ? baselinePredictionFromMover(enriched, thresholdEntry) : null
    const activeSignal = (thresholdRuleSignal?.entry_ready ? thresholdRuleSignal : null) ||
      (predictionRows[0]?.threshold_rule_signal?.entry_ready ? predictionRows[0].threshold_rule_signal : null) ||
      (isLiveModelSignalEligible(modelSignal, model) ? modelSignal : null) ||
      (isLiveModelSignalEligible(predictionRows[0]?.model_signal, model) ? predictionRows[0].model_signal : null) ||
      predictionRows[0]?.baseline_signal ||
      baselineSignal
    const socialCount = Number(enriched?.message_count || 0)
    const articleTotal = Number(enriched?.article_count || 0)
    const newsArticleTotal = Math.max(articleTotal, articleCount, Number(news.n || 0))
    const evidenceScore = Number(enriched?.trade_watch?.evidence_score || 0)
    const tradeScore = Number(enriched?.trade_watch?.trade_watch_score || 0)
    const predictionScore = Number.isFinite(Number(activeSignal?.probability_up)) ? clamp(Number(activeSignal.probability_up)) : activeSignal?.direction === 'up' ? 0.62 : activeSignal?.direction === 'down' ? 0.38 : 0.5
    const socialDensity = clamp(Math.log1p(socialCount) / Math.log1p(80))
    const quoteFreshness = Number(enriched?.trade_watch?.quote_freshness ?? 0.5)
    const densityCorrelationRaw = nullableNumber(features.price_density_correlation)
    const densityCorrelation = densityCorrelationRaw == null ? null : clamp(densityCorrelationRaw, -1, 1)
    const densityCorrelationComponent = densityCorrelation == null ? 0.5 : (densityCorrelation + 1) / 2
    const validationCorrelation = clamp(Number(features.prediction_validation_correlation || features.correlation_score || 0), -1, 1)
    const validationAccuracy = nullableNumber(features.prediction_validation_accuracy_5m)
    const validationReturn = nullableNumber(features.prediction_validation_avg_return_5m)
    const validationSamples = nullableNumber(features.prediction_validation_samples)
    const validationEdge = clamp(
      (Number.isFinite(validationAccuracy) ? Math.max(0, validationAccuracy - 0.5) * 1.4 : 0) +
      Math.max(0, validationCorrelation) * 0.25 +
      (Number.isFinite(validationReturn) ? Math.max(0, validationReturn) / 3 : 0),
      0,
      1,
    )
    const densitySetupScore = clamp(Number(features.threshold_setup_score ?? thresholdEntry?.setupScore ?? 0) / 100)
    const densityStatus = String(features.threshold_setup_status || thresholdEntry?.setupStatus || thresholdEntry?.status || '')
    const densitySetupActive = ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(densityStatus)
    const technicalConfirmation = (newsArticleTotal > 0 || socialCount > 0 || densitySetupActive || validationEdge > 0.1) ? clamp(
      (Number(features.rsi || 50) >= 38 && Number(features.rsi || 50) <= 68 ? 0.35 : 0) +
      (Number(features.rsi_oversold || 0) * 0.20) +
      (Number(enriched?.change_pct || 0) >= -4 && Number(enriched?.change_pct || 0) <= 12 ? 0.20 : 0) +
      (Number(enriched?.rel_volume || 0) >= 1.25 ? 0.25 : 0),
      0,
      1,
    ) : 0
    const blended = enriched ? clamp(
      tradeScore * 0.20 + clamp((newsAvg + 1) / 2) * 0.16 + evidenceScore * 0.13 + socialDensity * 0.08 +
      predictionScore * 0.15 + quoteFreshness * 0.05 + densityCorrelationComponent * 0.08 +
      densitySetupScore * 0.06 + validationEdge * 0.05 + technicalConfirmation * 0.04 +
      (activeSignal?.direction === 'up' ? 0.08 : activeSignal?.direction === 'down' ? -0.08 : 0)
    ) : 0
    const aiRankScore = Number((blended * 100).toFixed(1))
    const bullishEvidence = newsArticleTotal > 0 && newsAvg > 0.05 && Number(enriched?.change_pct || 0) >= 0 && (aiRankScore >= 58 || tradeScore >= 0.65)
    const correct5 = predictionRows.map(r => r.labels?.return_5m?.direction_correct).filter(v => v === true || v === false)
    const accuracy5m = correct5.length ? Number((correct5.filter(Boolean).length / correct5.length).toFixed(3)) : null
    const modelValidation = modelValidationState(model)
    const checks = [
      { label: 'Ticker in Finviz mover universe', status: mover ? 'pass' : 'warn', detail: mover ? 'Ticker is present in the latest Finviz positive mover set.' : 'Ticker is not in the current Finviz positive mover set.' },
      { label: 'News evidence window', status: articleCount > 0 ? 'pass' : 'warn', detail: `${articleCount} approved articles found in the last ${days} day(s).` },
      { label: 'Social evidence window', status: socialCount > 0 ? 'pass' : 'warn', detail: `${socialCount} social posts found in the selected ${socialWindow} minute window.` },
      { label: 'Prediction validation', status: correct5.length >= 20 ? 'pass' : correct5.length ? 'warn' : 'info', detail: correct5.length ? `${correct5.length} labeled 5m outcomes; accuracy ${Math.round((accuracy5m || 0) * 100)}%.` : 'No completed 5m labels yet; ranking uses current model/baseline signal.' },
      { label: 'Density setup', status: thresholdEntry?.passed ? 'pass' : densitySetupActive ? 'warn' : 'info', detail: `${densityStatus || 'unknown'}; corr ${densityCorrelation == null ? '--' : densityCorrelation.toFixed(3)}, setup score ${Math.round(densitySetupScore * 100)}.` },
      { label: 'Model validation set', status: modelValidation.allow_live_classifier ? 'pass' : Number(model?.metrics?.baseline_actionable_samples || 0) > 0 ? 'warn' : 'info', detail: `Live classifier: ${modelValidation.allow_live_classifier ? 'enabled' : `shadowed (${modelValidation.reason})`}.` },
    ]
    res.json({
      ok: true,
      ticker,
      days,
      social_window_minutes: socialWindow,
      score: {
        ai_rank_score: aiRankScore,
        direction: bullishEvidence ? 'bullish' : aiRankScore <= 38 ? 'bearish' : 'watch',
        trade_watch_score: Number(tradeScore.toFixed(3)),
        news_score: Number((newsAvg * 100).toFixed(1)),
        evidence_score: Number(evidenceScore.toFixed(3)),
        social_density_score: Number(socialDensity.toFixed(3)),
        prediction_score: Number(predictionScore.toFixed(3)),
        quote_freshness: Number(quoteFreshness.toFixed(3)),
        price_density_correlation: densityCorrelation == null ? null : Number(densityCorrelation.toFixed(3)),
        density_setup_score: Number((densitySetupScore * 100).toFixed(1)),
        density_setup_status: densityStatus || null,
        validation_edge: Number(validationEdge.toFixed(3)),
        validation_accuracy_5m: Number.isFinite(validationAccuracy) ? Number(validationAccuracy.toFixed(3)) : null,
        validation_samples: validationSamples == null ? null : validationSamples,
        validation_avg_return_5m: Number.isFinite(validationReturn) ? Number(validationReturn.toFixed(3)) : null,
      },
      mover: enriched ? {
        ticker,
        company: enriched.company || '',
        price: enriched.price ?? null,
        change_pct: Number(enriched.change_pct || 0),
        rel_volume: Number(enriched.rel_volume || 0),
        quote_age_minutes: enriched.trade_watch?.quote_age_minutes ?? null,
        reasons: enriched.trade_watch?.reasons || [],
        risks: enriched.trade_watch?.risks || [],
      } : null,
      evidence: {
        article_count: newsArticleTotal,
        approved_article_count: articleCount,
        scored_news_articles: Number(news.n || 0),
        bullish_news: Number(news.pos || 0),
        bearish_news: Number(news.neg || 0),
        social_posts: socialCount,
        social_sentiment: Number(Number(enriched?.social_sentiment || 0).toFixed(3)),
      },
      prediction: {
        active_signal: activeSignal || null,
        model_signal: modelSignal,
        baseline_signal: baselineSignal,
        model: model ? { status: model.status, samples: Number(model.samples || 0), metrics: model.metrics || null, updated_at: model.updated_at || null } : null,
        signals: predictionRows.map(r => ({
          signal_id: r.signal_id,
          signal_sec: r.signal_sec,
          time: timeLabel(r.signal_sec),
          decision: r.decision,
          rank: r.rank,
          label_status: r.label_status || 'pending',
          model_signal: r.model_signal || null,
          baseline_signal: r.baseline_signal || null,
          labels: r.labels || {},
        })),
        summary: { total: predictionRows.length, labeled: predictionRows.filter(r => r.label_status && r.label_status !== 'pending').length, complete: predictionRows.filter(r => r.label_status === 'complete').length, accuracy_5m: accuracy5m },
      },
      articles: articles.map(a => ({
        title: a.title || '',
        source: a.source || '',
        sentiment: a.sentiment || 'neutral',
        sentiment_score: Number(articleSentimentValue(a).toFixed(3)),
        event_type: a.event_type || 'general_news',
        reason: a.sentiment_reason || '',
        url: a.url || '',
        time: timeLabel(a.publish_date || a.fetched_date || a.detected_at),
      })),
      social_posts: socialRows.map(p => ({
        platform: p.platform || 'social',
        author: p.author || '',
        text: p.text || '',
        sentiment: typeof p.sentiment_score === 'number' ? Number(p.sentiment_score.toFixed(3)) : sentimentDirectionValue(p.sentiment),
        url: p.url || '',
        time: timeLabel(p.event_sec),
      })),
      checks,
    })
  } catch (err) {
    console.error('GET /api/ai/ticker/:ticker failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

const SUPPORTED_TRANSLATION_LANGUAGES = new Set(["en", "es", "fr", "de", "pt", "ja"])
const UNSUPPORTED_TRANSLATION_SCRIPT_RE = /[\u3400-\u9FFF\uF900-\uFAFF\u3040-\u30FF\uAC00-\uD7AF]/

const FINANCE_GLOSSARY = {
  en: [
    ["informa que", "reports that"],
    ["sus activos totales", "its total assets"],
    ["ascienden a", "amount to"],
    ["millones de dolares", "million dollars"],
    ["millones de dólares", "million dollars"],
    ["acciones", "stocks"],
    ["accion", "stock"],
    ["acción", "stock"],
    ["mercado bursatil", "stock market"],
    ["mercado bursátil", "stock market"],
    ["mercado de acoes", "stock market"],
    ["mercado de ações", "stock market"],
    ["ingresos", "revenue"],
    ["receita", "revenue"],
    ["chiffre d'affaires", "revenue"],
    ["umsatz", "revenue"],
    ["resultados", "earnings"],
    ["resultats", "earnings"],
    ["résultats", "earnings"],
    ["gewinne", "earnings"],
    ["ganancia", "profit"],
    ["lucro", "profit"],
    ["benefice", "profit"],
    ["bénéfice", "profit"],
    ["gewinn", "profit"],
    ["perdida", "loss"],
    ["pérdida", "loss"],
    ["perte", "loss"],
    ["verlust", "loss"],
    ["fusion", "merger"],
    ["fusión", "merger"],
    ["fusao", "merger"],
    ["fusão", "merger"],
    ["gappei", "merger"],
    ["adquisicion", "acquisition"],
    ["adquisición", "acquisition"],
    ["aquisicao", "acquisition"],
    ["aquisição", "acquisition"],
    ["ubernahme", "acquisition"],
    ["übernahme", "acquisition"],
    ["prevision", "guidance"],
    ["previsión", "guidance"],
    ["previsions", "guidance"],
    ["prévisions", "guidance"],
    ["projecao", "guidance"],
    ["projeção", "guidance"],
    ["ausblick", "guidance"],
    ["dividendo", "dividend"],
    ["dividende", "dividend"],
    ["inflacion", "inflation"],
    ["inflación", "inflation"],
    ["inflacao", "inflation"],
    ["inflação", "inflation"],
    ["mercado", "market"],
    ["marche", "market"],
    ["marché", "market"],
    ["markt", "market"],
    ["preco", "price"],
    ["preço", "price"],
    ["precio", "price"],
    ["prix", "price"],
    ["preis", "price"],
    ["sube", "rises"],
    ["sobe", "rises"],
    ["steigt", "rises"],
    ["cae", "falls"],
    ["cai", "falls"],
    ["baisse", "falls"],
    ["fallt", "falls"],
    ["fällt", "falls"],
    ["supera", "beats"],
    ["depasse", "beats"],
    ["dépasse", "beats"],
    ["ubertrifft", "beats"],
    ["übertrifft", "beats"],
  ],
  es: [
    ["stock market", "mercado bursatil"],
    ["stocks", "acciones"],
    ["stock", "accion"],
    ["shares", "acciones"],
    ["earnings", "resultados"],
    ["revenue", "ingresos"],
    ["profit", "ganancia"],
    ["loss", "perdida"],
    ["merger", "fusion"],
    ["acquisition", "adquisicion"],
    ["upgrade", "mejora"],
    ["downgrade", "rebaja"],
    ["guidance", "prevision"],
    ["dividend", "dividendo"],
    ["inflation", "inflacion"],
    ["market", "mercado"],
    ["price", "precio"],
    ["rally", "repunte"],
    ["falls", "cae"],
    ["rises", "sube"],
    ["beats", "supera"],
    ["misses", "no alcanza"],
  ],
  fr: [
    ["stock market", "marche boursier"],
    ["stocks", "actions"],
    ["stock", "action"],
    ["shares", "actions"],
    ["earnings", "resultats"],
    ["revenue", "chiffre d'affaires"],
    ["profit", "benefice"],
    ["loss", "perte"],
    ["merger", "fusion"],
    ["acquisition", "acquisition"],
    ["upgrade", "relevement"],
    ["downgrade", "abaissement"],
    ["guidance", "previsions"],
    ["dividend", "dividende"],
    ["inflation", "inflation"],
    ["market", "marche"],
    ["price", "prix"],
    ["rally", "rebond"],
    ["falls", "baisse"],
    ["rises", "monte"],
    ["beats", "depasse"],
    ["misses", "rate"],
  ],
  de: [
    ["stock market", "aktienmarkt"],
    ["stocks", "aktien"],
    ["stock", "aktie"],
    ["shares", "anteile"],
    ["earnings", "gewinne"],
    ["revenue", "umsatz"],
    ["profit", "gewinn"],
    ["loss", "verlust"],
    ["merger", "fusion"],
    ["acquisition", "ubernahme"],
    ["upgrade", "heraufstufung"],
    ["downgrade", "herabstufung"],
    ["guidance", "ausblick"],
    ["dividend", "dividende"],
    ["inflation", "inflation"],
    ["market", "markt"],
    ["price", "preis"],
    ["rally", "rallye"],
    ["falls", "fallt"],
    ["rises", "steigt"],
    ["beats", "ubertrifft"],
    ["misses", "verfehlt"],
  ],
  pt: [
    ["stock market", "mercado de acoes"],
    ["stocks", "acoes"],
    ["stock", "acao"],
    ["shares", "acoes"],
    ["earnings", "resultados"],
    ["revenue", "receita"],
    ["profit", "lucro"],
    ["loss", "perda"],
    ["merger", "fusao"],
    ["acquisition", "aquisicao"],
    ["upgrade", "elevacao"],
    ["downgrade", "rebaixamento"],
    ["guidance", "projecao"],
    ["dividend", "dividendo"],
    ["inflation", "inflacao"],
    ["market", "mercado"],
    ["price", "preco"],
    ["rally", "alta"],
    ["falls", "cai"],
    ["rises", "sobe"],
    ["beats", "supera"],
    ["misses", "fica abaixo"],
  ],
  ja: [
    ["stock market", "kabushiki shijo"],
    ["stocks", "kabushiki"],
    ["stock", "kabushiki"],
    ["shares", "kabushiki"],
    ["earnings", "gyoseki"],
    ["revenue", "uriage"],
    ["profit", "rieki"],
    ["loss", "sonshitsu"],
    ["merger", "gappei"],
    ["acquisition", "baishu"],
    ["upgrade", "kakuzuke hikiage"],
    ["downgrade", "kakuzuke hikisage"],
    ["guidance", "gyoseki yosou"],
    ["dividend", "haito"],
    ["inflation", "infure"],
    ["market", "shijo"],
    ["price", "kakaku"],
    ["rally", "joraku"],
    ["falls", "geraku"],
    ["rises", "josho"],
    ["beats", "uwamawaru"],
    ["misses", "shitamawaru"],
  ],
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

function glossaryTranslate(text, targetLanguage) {
  const glossary = FINANCE_GLOSSARY[targetLanguage] || []
  let translated = String(text || "")

  if (targetLanguage === "en") {
    translated = englishFallbackTranslate(translated)
  }

  for (const [source, target] of glossary) {
    translated = translated.replace(new RegExp(`\\b${escapeRegExp(source)}\\b`, "gi"), target)
  }

  return translated
}

const ENGLISH_DIRECT_TRANSLATIONS = [
  [
    /AIFA\s+就涉及\s+HyalRoute Communication Group Limited\s+的股份收購交易正式作出澄清及反駁/i,
    "AIFA issues formal clarification and rebuttal regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /AIFA、ヒアルルート・コミュニケーション・グループに関連する株式取得取引について、正式な説明および反論を発表/i,
    "AIFA issues formal explanation and rebuttal regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /Penjelasan Rasmi dan Penafian oleh AIFA Berhubung Transaksi Pemerolehan Saham yang melibatkan HyalRoute Communication Group Limited/i,
    "Official clarification and denial by AIFA regarding the share acquisition transaction involving HyalRoute Communication Group Limited",
  ],
  [
    /Huasun belegt den 12\. Platz der TIME-Liste der weltweit führenden GreenTech-Unternehmen 2026 und verbessert sich dank seines Engagements für die HJT-Technologie um 22 Plätze/i,
    "Huasun ranks 12th on TIME's 2026 list of the world's leading GreenTech companies and rises 22 places thanks to its commitment to HJT technology",
  ],
]

const ENGLISH_PHRASE_FALLBACKS = [
  ["belegt den", "ranks"],
  ["Platz der", "place on the"],
  ["TIME-Liste", "TIME list"],
  ["weltweit führenden", "world's leading"],
  ["GreenTech-Unternehmen", "GreenTech companies"],
  ["verbessert sich", "rises"],
  ["dank seines Engagements", "thanks to its commitment"],
  ["für die", "to the"],
  ["Technologie", "technology"],
  ["Plätze", "places"],
  ["Juni", "June"],
  ["Unternehmen", "company"],
  ["weltweit", "worldwide"],
  ["führenden", "leading"],
  ["Umsatz", "revenue"],
  ["Gewinn", "profit"],
  ["Verlust", "loss"],
  ["Aktien", "shares"],
  ["Markt", "market"],
  ["Prix", "price"],
  ["marché", "market"],
  ["résultats", "earnings"],
  ["acciones", "stocks"],
  ["mercado", "market"],
  ["ingresos", "revenue"],
  ["receita", "revenue"],
  ["ações", "stocks"],
  ["就涉及", "regarding"],
  ["的股份收購交易", "the share acquisition transaction"],
  ["股份收購交易", "share acquisition transaction"],
  ["正式作出", "formally issues"],
  ["澄清及反駁", "clarification and rebuttal"],
  ["澄清", "clarification"],
  ["反駁", "rebuttal"],
  ["ヒアルルート・コミュニケーション・グループ", "HyalRoute Communication Group"],
  ["に関連する", "regarding"],
  ["株式取得取引", "share acquisition transaction"],
  ["について", "regarding"],
  ["正式な説明", "formal explanation"],
  ["および反論", "and rebuttal"],
  ["を発表", "announces"],
  ["Penjelasan Rasmi", "Official clarification"],
  ["Penafian", "denial"],
  ["Berhubung", "regarding"],
  ["Transaksi Pemerolehan Saham", "share acquisition transaction"],
  ["yang melibatkan", "involving"],
]

function likelyNeedsEnglishFallback(text) {
  return /[\u3400-\u9FFF\uF900-\uFAFF\u3040-\u30FF\uAC00-\uD7AFäöüßéèêàáíóúñçãõ]|(?:\b(?:der|die|das|und|für|mit|von|belegt|verbessert|weltweit|führenden|acciones|mercado|ingresos|résultats|marché|receita|ações|penjelasan|rasmi|penafian|berhubung|transaksi|pemerolehan|saham|melibatkan)\b)/i.test(text)
}

function englishFallbackTranslate(text) {
  const original = String(text || "")

  for (const [pattern, translation] of ENGLISH_DIRECT_TRANSLATIONS) {
    if (pattern.test(original)) return translation
  }

  if (!likelyNeedsEnglishFallback(original)) return original

  let translated = original
  for (const [source, target] of ENGLISH_PHRASE_FALLBACKS) {
    translated = translated.replace(new RegExp(escapeRegExp(source), "gi"), target)
  }

  translated = translated
    .replace(/\bden\b/gi, "the")
    .replace(/\bder\b/gi, "of the")
    .replace(/\bdie\b/gi, "the")
    .replace(/\bdas\b/gi, "the")
    .replace(/\bund\b/gi, "and")
    .replace(/\bum\b/gi, "by")
    .replace(/\bauf\b/gi, "on")
    .replace(/\bin\b/gi, "in")
    .replace(/\bmit\b/gi, "with")

  return translated === original ? `English translation pending: ${original}` : translated
}

async function translateWithProvider(text, targetLanguage) {
  const url = process.env.TRANSLATION_API_URL
  if (!url || typeof fetch !== "function") return null

  const body = {
    q: text,
    text,
    source: "auto",
    target: targetLanguage,
    target_language: targetLanguage,
    format: "text",
  }

  if (process.env.TRANSLATION_API_KEY) {
    body.api_key = process.env.TRANSLATION_API_KEY
    body.apiKey = process.env.TRANSLATION_API_KEY
  }

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    throw new Error(`translation provider returned HTTP ${response.status}`)
  }

  const data = await response.json()
  return data.translatedText || data.translated_text || data.translation || data.text || null
}

function easternParts(date) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: MARKET_WINDOW_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date)

  return Object.fromEntries(
    parts
      .filter(part => part.type !== 'literal')
      .map(part => [part.type, Number(part.value)])
  )
}

function easternLocalToUtc(year, month, day, hour, minute = 0, second = 0) {
  const target = Date.UTC(year, month - 1, day, hour, minute, second)
  let guess = target

  for (let i = 0; i < 4; i += 1) {
    const parts = easternParts(new Date(guess))
    const actual = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second)
    const diff = target - actual
    if (diff === 0) break
    guess += diff
  }

  return new Date(guess)
}

function shiftLocalDate(year, month, day, deltaDays) {
  const shifted = new Date(Date.UTC(year, month - 1, day + deltaDays))
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
  }
}

function localWeekday(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day)).getUTCDay()
}

function latestMarketCloseCutoff(now = new Date()) {
  let { year, month, day, hour } = easternParts(now)
  let weekday = localWeekday(year, month, day)

  if (weekday === 0) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -2))
  } else if (weekday === 6) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
  } else if (hour < MARKET_WINDOW_CLOSE_HOUR) {
    ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
    while ([0, 6].includes(localWeekday(year, month, day))) {
      ;({ year, month, day } = shiftLocalDate(year, month, day, -1))
    }
  }

  return easternLocalToUtc(year, month, day, MARKET_WINDOW_CLOSE_HOUR)
}

function articleWindowMatch(cutoffMs) {
  const cutoffSec = Math.floor(cutoffMs / 1000)
  const cutoffDate = new Date(cutoffMs)
  const missingPublishDate = {
    $or: [
      { publish_date: { $exists: false } },
      { publish_date: null },
      { publish_date: "" },
    ],
  }

  return {
    $or: [
      { publish_date: { $type: "date", $gte: cutoffDate } },
      { publish_date: { $type: "int", $gte: cutoffSec } },
      { publish_date: { $type: "long", $gte: cutoffSec } },
      { publish_date: { $type: "double", $gte: cutoffSec } },
      {
        $and: [
          missingPublishDate,
          {
            $or: [
              { fetched_date: { $type: "date", $gte: cutoffDate } },
              { fetched_date: { $type: "int", $gte: cutoffSec } },
              { fetched_date: { $type: "long", $gte: cutoffSec } },
              { fetched_date: { $type: "double", $gte: cutoffSec } },
              { detected_at: { $type: "date", $gte: cutoffDate } },
              { detected_at: { $type: "int", $gte: cutoffSec } },
              { detected_at: { $type: "long", $gte: cutoffSec } },
              { detected_at: { $type: "double", $gte: cutoffSec } },
              { createdAt: { $gte: cutoffDate } },
            ],
          },
        ],
      },
    ],
  }
}

function recentArticleMatch(days = 0) {
  const n = Number(days || 0)
  const cutoffMs = Number.isFinite(n) && n > 0
    ? Date.now() - n * 86_400_000
    : latestMarketCloseCutoff().getTime()

  return articleWindowMatch(cutoffMs)
}

function articleMatchStage(match) {
  return Object.keys(match).length ? [{ $match: match }] : []
}

function tickerArticlePipeline({ days = 2, limit = 150, ticker = "", tickers = [] } = {}) {
  const wantedTickers = normalizeTickerList(
    ticker ? [ticker] : tickers,
    500,
    { ensurePrivate: false },
  )
  const match = {
    ...recentArticleMatch(days),
    ticker: { $exists: true, $nin: ["", null] },
  }

  const pipeline = [
    { $match: match },
    {
      $addFields: {
        _ticker_parts: {
          $map: {
            input: { $split: [{ $toUpper: { $toString: "$ticker" } }, ","] },
            as: "ticker_part",
            in: { $trim: { input: "$$ticker_part" } }
          }
        }
      }
    },
    { $unwind: "$_ticker_parts" },
    { $match: { _ticker_parts: { $ne: "", $nin: Array.from(NON_STOCK_TICKERS) } } },
  ]

  if (wantedTickers.length) pipeline.push({ $match: { _ticker_parts: { $in: wantedTickers } } })

  pipeline.push(
    {
      $addFields: {
        _article_kind: {
          $cond: [
            {
              $or: [
                { $in: ["$category", ["unstructured_public_title", "public_news", "public_market_news"]] },
                { $eq: ["$collector", "unstructured_news_title_only_v1"] },
                {
                  $regexMatch: {
                    input: { $toLower: { $toString: { $ifNull: ["$source", ""] } } },
                    regex: "unstructured"
                  }
                },
              ],
            },
            "unstructured",
            "structured",
          ],
        },
        _sentiment_direction: {
          $switch: {
            branches: [
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $addFields: {
        _sentiment_numeric: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"] ] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
            ],
            default: "$_sentiment_direction",
          },
        },
      },
    },
    {
      $addFields: {
        _source_weight: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    {
                      $or: [
                        { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$source", ""] } } }, regex: "sec|edgar" } },
                        { $eq: [{ $toLower: { $toString: { $ifNull: ["$event_type", ""] } } }, "sec_filing"] },
                      ],
                    },
                    { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] },
                  ],
                },
                then: 0.15,
              },
              {
                case: {
                  $in: [
                    { $toLower: { $toString: { $ifNull: ["$event_type", ""] } } },
                    ["earnings_beat", "earnings_miss", "guidance_raise", "guidance_cut", "fda_approval", "fda_rejection", "clinical_positive", "clinical_negative", "public_offering", "bankruptcy_default"],
                  ],
                },
                then: 1.35,
              },
            ],
            default: 1,
          },
        },
      },
    },
    {
      $group: {
        _id: "$_ticker_parts",
        count: { $sum: 1 },
        structured_count: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, 1, 0] } },
        unstructured_count: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, 1, 0] } },
        structured_weight_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, "$_source_weight", 0] } },
        unstructured_weight_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, "$_source_weight", 0] } },
        bullish: { $sum: { $cond: [{ $gt: ["$_sentiment_numeric", 0.08] }, 1, 0] } },
        bearish: { $sum: { $cond: [{ $lt: ["$_sentiment_numeric", -0.08] }, 1, 0] } },
        neutral: { $sum: { $cond: [{ $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }, 1, 0] } },
        score_sum: { $sum: "$_sentiment_numeric" },
        structured_bullish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $gt: ["$_sentiment_numeric", 0.08] }] }, 1, 0] } },
        structured_bearish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $lt: ["$_sentiment_numeric", -0.08] }] }, 1, 0] } },
        structured_neutral: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "structured"] }, { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }] }, 1, 0] } },
        structured_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, "$_sentiment_numeric", 0] } },
        structured_weighted_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "structured"] }, { $multiply: ["$_sentiment_numeric", "$_source_weight"] }, 0] } },
        unstructured_bullish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $gt: ["$_sentiment_numeric", 0.08] }] }, 1, 0] } },
        unstructured_bearish: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $lt: ["$_sentiment_numeric", -0.08] }] }, 1, 0] } },
        unstructured_neutral: { $sum: { $cond: [{ $and: [{ $eq: ["$_article_kind", "unstructured"] }, { $lte: [{ $abs: "$_sentiment_numeric" }, 0.08] }] }, 1, 0] } },
        unstructured_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, "$_sentiment_numeric", 0] } },
        unstructured_weighted_score_sum: { $sum: { $cond: [{ $eq: ["$_article_kind", "unstructured"] }, { $multiply: ["$_sentiment_numeric", "$_source_weight"] }, 0] } },
        sources: { $addToSet: "$source" },
        latest_publish: { $max: "$publish_date" },
        latest_fetch: { $max: "$fetched_date" }
      }
    },
    { $sort: { count: -1, latest_publish: -1 } },
    { $limit: Math.max(1, Math.min(300, Number(limit || 150))) },
    {
      $project: {
        _id: 0,
        ticker: "$_id",
        count: 1,
        structured_count: 1,
        unstructured_count: 1,
        structured_weight_sum: 1,
        unstructured_weight_sum: 1,
        bullish: 1,
        bearish: 1,
        neutral: 1,
        score_sum: 1,
        structured_bullish: 1,
        structured_bearish: 1,
        structured_neutral: 1,
        structured_score_sum: 1,
        structured_weighted_score_sum: 1,
        unstructured_bullish: 1,
        unstructured_bearish: 1,
        unstructured_neutral: 1,
        unstructured_score_sum: 1,
        unstructured_weighted_score_sum: 1,
        sources: 1,
        latest_publish: 1,
        latest_fetch: 1
      }
    }
  )

  return pipeline
}

function sentimentScore(row) {
  const hasArticleKinds = row.structured_count != null || row.unstructured_count != null
  if (hasArticleKinds) {
    const structuredWeight = 2
    const unstructuredWeight = 1
    const structuredCount = Number(row.structured_count || 0)
    const unstructuredCount = Number(row.unstructured_count || 0)
    if (row.structured_weighted_score_sum != null || row.unstructured_weighted_score_sum != null) {
      const structuredDenominator = row.structured_weight_sum != null ? Number(row.structured_weight_sum || 0) : structuredCount
      const unstructuredDenominator = row.unstructured_weight_sum != null ? Number(row.unstructured_weight_sum || 0) : unstructuredCount
      const numerator =
        structuredWeight * Number(row.structured_weighted_score_sum || 0) +
        unstructuredWeight * Number(row.unstructured_weighted_score_sum || 0)
      const denominator = structuredWeight * structuredDenominator + unstructuredWeight * unstructuredDenominator
      return denominator ? Number((numerator / (denominator + 1.5)).toFixed(3)) : 0
    }
    if (row.structured_score_sum != null || row.unstructured_score_sum != null) {
      const numerator =
        structuredWeight * Number(row.structured_score_sum || 0) +
        unstructuredWeight * Number(row.unstructured_score_sum || 0)
      const denominator = structuredWeight * structuredCount + unstructuredWeight * unstructuredCount
      return denominator ? Number((numerator / (denominator + 2)).toFixed(3)) : 0
    }
    const numerator =
      structuredWeight * (Number(row.structured_bullish || 0) - Number(row.structured_bearish || 0)) +
      unstructuredWeight * (Number(row.unstructured_bullish || 0) - Number(row.unstructured_bearish || 0))
    const denominator = structuredWeight * structuredCount + unstructuredWeight * unstructuredCount
    return denominator ? Number((numerator / (denominator + 2)).toFixed(3)) : 0
  }

  const total = Math.max(1, Number(row.count || 0))
  const priorNeutralWeight = 4
  return Number((((row.bullish || 0) - (row.bearish || 0)) / (total + priorNeutralWeight)).toFixed(3))
}

function kindSentimentScore(row, kind) {
  const prefix = kind === "unstructured" ? "unstructured" : "structured"
  const count = Number(row?.[`${prefix}_count`] || 0)
  if (!count) return 0
  if (row?.[`${prefix}_weighted_score_sum`] != null) {
    const denominator = Number(row?.[`${prefix}_weight_sum`] || count)
    return denominator ? Number((Number(row[`${prefix}_weighted_score_sum`] || 0) / (denominator + 0.75)).toFixed(3)) : 0
  }
  if (row?.[`${prefix}_score_sum`] != null) {
    return Number((Number(row[`${prefix}_score_sum`] || 0) / (count + 1)).toFixed(3))
  }
  return Number(((Number(row?.[`${prefix}_bullish`] || 0) - Number(row?.[`${prefix}_bearish`] || 0)) / (count + 2)).toFixed(3))
}

function sentimentDirectionValue(value) {
  const text = String(value || "").toLowerCase()
  if (/bull|positive/.test(text)) return 1
  if (/bear|negative/.test(text)) return -1
  return 0
}

function articleSentimentValue(row) {
  if (!row) return 0
  const direct = Number(row.sentiment_score)
  if (Number.isFinite(direct) && direct !== 0) return clamp(direct, -1, 1)
  const direction = sentimentDirectionValue(row.sentiment)
  const confidence = Number(row.ml_confidence)
  if (Number.isFinite(confidence) && confidence > 0) return clamp(direction * confidence, -1, 1)
  return direction
}

function stableHash(value) {
  let hash = 0
  const text = String(value || "")
  for (let i = 0; i < text.length; i += 1) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0
  return Math.abs(hash)
}

function derivedNumber(ticker, min, max, decimals = 2, salt = "") {
  const span = max - min
  const pct = (stableHash(`${ticker}:${salt}`) % 10000) / 10000
  return Number((min + span * pct).toFixed(decimals))
}

function nullableNumber(value) {
  if (value == null || value === "") return null
  if (typeof value === "number") return Number.isFinite(value) ? value : null

  let raw = String(value).trim()
  if (!raw || raw === "-" || raw === "--" || raw.toLowerCase() === "nan" || raw.toUpperCase() === "N/A") return null

  raw = raw.replace(/,/g, "").replace(/\$/g, "")
  if (raw.endsWith("%")) raw = raw.slice(0, -1)

  const suffix = raw.match(/^([-+]?\d*\.?\d+)\s*([KMBT])$/i)
  if (suffix) {
    const base = Number(suffix[1])
    const mult = { K: 1e3, M: 1e6, B: 1e9, T: 1e12 }[suffix[2].toUpperCase()] || 1
    return Number.isFinite(base) ? base * mult : null
  }

  const n = Number(raw)
  return Number.isFinite(n) ? n : null
}

function nullableFixed(value, decimals = 2) {
  const n = nullableNumber(value)
  return n == null ? null : Number(n.toFixed(decimals))
}

function clamp(value, min = 0, max = 1) {
  const n = Number(value)
  if (!Number.isFinite(n)) return min
  return Math.max(min, Math.min(max, n))
}

function marketCapBucket(marketCap) {
  const cap = Number(marketCap || 0)
  if (cap >= 200e9) return "Mega"
  if (cap >= 10e9) return "Large"
  if (cap >= 2e9) return "Mid"
  if (cap >= 300e6) return "Small"
  if (cap > 0) return "Micro"
  return "Unknown"
}

function normalizeExchange(value) {
  const raw = String(value || "").trim().toUpperCase()
  if (raw === "NYSEAMERICAN" || raw === "NYSE AMERICAN") return "AMEX"
  if (raw === "NAS") return "NASDAQ"
  return raw
}

function normalizeScreenerDoc(doc = {}) {
  const ticker = String(doc.ticker || "").toUpperCase()
  const hasStoredPrice = doc.price != null
  const price = nullableFixed(doc.price, 2)
  const change = doc.change_pct ?? doc.change_percent
  const changePct = nullableFixed(change, 2)
  const volume = nullableNumber(doc.volume)
  const avgVolume = nullableNumber(doc.avg_volume)
  const storedRelVolume = nullableNumber(doc.rel_volume ?? doc.relative_volume ?? doc.relVol)
  const relVolume = storedRelVolume != null
    ? Number(storedRelVolume.toFixed(2))
    : (volume != null && avgVolume ? Number((volume / Math.max(1, avgVolume)).toFixed(2)) : null)
  const marketCap = nullableNumber(doc.market_cap)
  const avgSentiment = Number(doc.avg_sentiment ?? doc.news_sentiment ?? doc.structured_sentiment ?? 0)

  return {
    ticker,
    company: doc.company || "",
    price,
    change_pct: changePct,
    volume,
    avg_volume: avgVolume,
    rel_volume: relVolume,
    market_cap: marketCap,
    market_cap_bucket: marketCapBucket(marketCap),
    sector: doc.sector || "Unclassified",
    industry: doc.industry || "Unclassified",
    country: doc.country || "",
    exchange: normalizeExchange(doc.exchange),
    index: doc.index || "",
    avg_sentiment: avgSentiment,
    social_sentiment: Number(doc.social_sentiment ?? 0),
    structured_sentiment: Number(doc.structured_sentiment ?? doc.news_sentiment ?? avgSentiment),
    sentiment: avgSentiment,
    message_count: Number(doc.message_count ?? 0),
    news_article_count: Number(doc.news_article_count ?? 0),
    bullish_count: Number(doc.bullish_count ?? 0),
    bearish_count: Number(doc.bearish_count ?? 0),
    neutral_count: Number(doc.neutral_count ?? 0),
    sources: doc.sources || [],
    pe_ratio: nullableNumber(doc.pe_ratio ?? doc.pe),
    forward_pe: nullableNumber(doc.forward_pe),
    peg: nullableNumber(doc.peg),
    ps_ratio: nullableNumber(doc.ps_ratio),
    pb_ratio: nullableNumber(doc.pb_ratio),
    dividend_yield: nullableNumber(doc.dividend_yield),
    eps_growth_this_y: nullableNumber(doc.eps_growth_this_y),
    eps_growth_next_y: nullableNumber(doc.eps_growth_next_y),
    sales_growth: nullableNumber(doc.sales_growth),
    sales_growth_past_5y: nullableNumber(doc.sales_growth_past_5y),
    gross_margin: nullableNumber(doc.gross_margin),
    operating_margin: nullableNumber(doc.operating_margin),
    profit_margin: nullableNumber(doc.profit_margin),
    roa: nullableNumber(doc.roa),
    roe: nullableNumber(doc.roe),
    roi: nullableNumber(doc.roi),
    current_ratio: nullableNumber(doc.current_ratio),
    quick_ratio: nullableNumber(doc.quick_ratio),
    lt_debt_equity: nullableNumber(doc.lt_debt_equity),
    debt_equity: nullableNumber(doc.debt_equity),
    beta: nullableNumber(doc.beta),
    rsi: nullableNumber(doc.rsi),
    sma20: nullableNumber(doc.sma20),
    sma50: nullableNumber(doc.sma50),
    sma200: nullableNumber(doc.sma200),
    perf_week: nullableNumber(doc.perf_week),
    perf_month: nullableNumber(doc.perf_month),
    perf_quarter: nullableNumber(doc.perf_quarter),
    perf_half: nullableNumber(doc.perf_half),
    perf_year: nullableNumber(doc.perf_year),
    perf_ytd: nullableNumber(doc.perf_ytd),
    atr: nullableNumber(doc.atr),
    gap: nullableNumber(doc.gap),
    analyst: doc.analyst || null,
    target_price: nullableFixed(doc.target_price, 2),
    inst_own: nullableNumber(doc.inst_own),
    insider_own: nullableNumber(doc.insider_own),
    insider_transactions: nullableNumber(doc.insider_transactions),
    inst_transactions: nullableNumber(doc.inst_transactions),
    float_short: nullableNumber(doc.float_short),
    shares_outstanding: nullableNumber(doc.shares_outstanding),
    shares_float: nullableNumber(doc.shares_float),
    short_ratio: nullableNumber(doc.short_ratio),
    short_interest: nullableNumber(doc.short_interest),
    float_percent: nullableNumber(doc.float_percent),
    payout_ratio: nullableNumber(doc.payout_ratio),
    eps_ttm: nullableNumber(doc.eps_ttm),
    eps_growth_qoq: nullableNumber(doc.eps_growth_qoq),
    eps_growth_past_5y: nullableNumber(doc.eps_growth_past_5y),
    eps_growth_next_5y: nullableNumber(doc.eps_growth_next_5y),
    price_to_cash: nullableNumber(doc.price_to_cash),
    price_to_free_cash_flow: nullableNumber(doc.price_to_free_cash_flow),
    book_per_share: nullableNumber(doc.book_per_share),
    cash_per_share: nullableNumber(doc.cash_per_share),
    dividend: nullableNumber(doc.dividend),
    employees: nullableNumber(doc.employees),
    income: nullableNumber(doc.income),
    sales: nullableNumber(doc.sales),
    after_hours_close: nullableFixed(doc.after_hours_close, 4),
    after_hours_change: nullableNumber(doc.after_hours_change),
    change_from_open: nullableNumber(doc.change_from_open),
    volatility_week: nullableNumber(doc.volatility_week),
    volatility_month: nullableNumber(doc.volatility_month),
    open: nullableFixed(doc.open, 4),
    high: nullableFixed(doc.high, 4),
    low: nullableFixed(doc.low, 4),
    trades: nullableNumber(doc.trades),
    optionable: doc.optionable || null,
    shortable: doc.shortable || null,
    ipo_date: doc.ipo_date || null,
    earnings_date: doc.earnings_date || null,
    price_density_correlation: nullableNumber(doc.price_density_correlation),
    previous_price_density_correlation: nullableNumber(doc.previous_price_density_correlation),
    threshold_pre_return_60m_pct: nullableNumber(doc.threshold_pre_return_60m_pct),
    threshold_trailing_60m_messages: nullableNumber(doc.threshold_trailing_60m_messages),
    threshold_feature_window_minutes: nullableNumber(doc.threshold_feature_window_minutes),
    threshold_feature_status: doc.threshold_feature_status || null,
    threshold_setup_status: doc.threshold_setup_status || null,
    threshold_setup_score: nullableNumber(doc.threshold_setup_score),
    threshold_setup_distance_to_entry: nullableNumber(doc.threshold_setup_distance_to_entry),
    threshold_feature_updated_at: doc.threshold_feature_updated_at || null,
    correlation_score: nullableNumber(doc.correlation_score),
    prediction_validation_correlation: nullableNumber(doc.prediction_validation_correlation),
    prediction_validation_accuracy_5m: nullableNumber(doc.prediction_validation_accuracy_5m),
    prediction_validation_samples: nullableNumber(doc.prediction_validation_samples),
    prediction_validation_correct: nullableNumber(doc.prediction_validation_correct),
    prediction_validation_avg_return_5m: nullableNumber(doc.prediction_validation_avg_return_5m),
    prediction_validation_latest_signal_sec: nullableNumber(doc.prediction_validation_latest_signal_sec),
    prediction_validation_updated_at: doc.prediction_validation_updated_at || null,
    prediction_validation_status: doc.prediction_validation_status || null,
    prediction_validation: doc.prediction_validation || null,
    previous_close: nullableFixed(doc.previous_close, 2),
    change: nullableFixed(doc.change, 2),
    quote_source: doc.quote_source || null,
    quote_time: doc.quote_time || null,
    quote_updated_at: doc.quote_updated_at || null,
    quote_status: doc.quote_status || (hasStoredPrice ? "priced" : "missing"),
  }
}

function isFinvizScreenerRow(row = {}) {
  const sourceText = `${row.quote_source || ""} ${row.source || ""} ${row.screener_source || ""} ${row.finviz_filter || ""}`.toLowerCase()
  return sourceText.includes("finviz")
}

function isCleanListedUsScreenerRow(row) {
  const ticker = String(row?.ticker || "").toUpperCase()
  const exchange = normalizeExchange(row?.exchange)
  const fromFinviz = isFinvizScreenerRow(row)

  return Boolean(
    ticker &&
    !ticker.includes(".") &&
    !ticker.includes("-") &&
    !NON_STOCK_TICKERS.has(ticker) &&
    (fromFinviz || US_EXCHANGES.has(exchange)) &&
    row.price != null &&
    Number(row.price) > 0 &&
    row.change_pct != null &&
    Number.isFinite(Number(row.change_pct)) &&
    Number(row.change_pct) > 0 &&
    Math.abs(Number(row.change_pct)) <= MAX_SIGNAL_CHANGE_PCT &&
    row.quote_status !== "missing"
  )
}

function isCleanListedUsThresholdEntryRow(row) {
  const ticker = String(row?.ticker || "").toUpperCase()
  const exchange = normalizeExchange(row?.exchange)

  return Boolean(
    ticker &&
    !ticker.includes(".") &&
    !ticker.includes("-") &&
    !NON_STOCK_TICKERS.has(ticker) &&
    US_EXCHANGES.has(exchange) &&
    row.price != null &&
    Number(row.price) > 0 &&
    row.change_pct != null &&
    Number.isFinite(Number(row.change_pct)) &&
    Math.abs(Number(row.change_pct)) <= MAX_SIGNAL_CHANGE_PCT &&
    row.quote_status !== "missing" &&
    row.threshold_feature_status === "entry_passed"
  )
}

function tickerStatsToScreenerRow(row, quoteRow = {}) {
  const score = sentimentScore(row)
  const quote = normalizeScreenerDoc({ ...quoteRow, ticker: quoteRow.ticker || row.ticker })
  const price = quote.price
  const volume = quote.quote_status === "priced" ? quote.volume : null
  return normalizeScreenerDoc({
    ...quote,
    ticker: row.ticker,
    company: quote.company || "",
    price,
    change_pct: quote.change_pct,
    volume,
    avg_volume: quote.quote_status === "priced" ? quote.avg_volume : null,
    market_cap: quote.market_cap,
    sector: quote.quote_status === "priced" ? quote.sector : "News matched",
    industry: quote.quote_status === "priced" ? quote.industry : "Ticker mentions",
    avg_sentiment: score,
    social_sentiment: quote.social_sentiment || 0,
    structured_sentiment: score,
    message_count: row.count || 0,
    news_article_count: row.count || 0,
    bullish_count: row.bullish || 0,
    bearish_count: row.bearish || 0,
    neutral_count: row.neutral || 0,
    sources: (row.sources || []).filter(Boolean).slice(0, 6),
    latest_publish: row.latest_publish,
    latest_fetch: row.latest_fetch,
  })
}

function tickerStatsToMomentumRow(row, quoteRow = {}) {
  const score = sentimentScore(row)
  const base = tickerStatsToScreenerRow(row, quoteRow)
  const volume = base.volume
  const articleCount = row.count || 0
  return {
    ...base,
    ticker: row.ticker,
    volume,
    avg_volume: base.avg_volume,
    rel_volume: base.rel_volume,
    sentiment: score,
    article_count: articleCount,
    message_count: articleCount,
    bullish_count: row.bullish || 0,
    bearish_count: row.bearish || 0,
    neutral_count: row.neutral || 0,
    sources: (row.sources || []).filter(Boolean).slice(0, 6),
    latest_publish: row.latest_publish,
    latest_fetch: row.latest_fetch,
    momentum_score: Number(Math.abs(base.change_pct || 0).toFixed(2)),
  }
}

function timeLabel(value) {
  const raw = Number(value || 0)
  const ms = raw > 1000000000000 ? raw : raw > 1000000000 ? raw * 1000 : Date.parse(value)
  if (!Number.isFinite(ms) || ms <= 0) return ""
  const diff = Math.max(0, Date.now() - ms)
  if (diff < 60_000) return "now"
  if (diff < 3_600_000) return `${Math.floor(diff / 60_000)}m`
  if (diff < 86_400_000) return `${Math.floor(diff / 3_600_000)}h`
  return new Date(ms).toLocaleDateString("en-US", { month: "short", day: "numeric" })
}

function normalizeTickerList(values = [], limit = TRACKED_TICKER_LIMIT, { ensurePrivate = true } = {}) {
  const max = Math.max(1, Number(limit || TRACKED_TICKER_LIMIT))
  const tickers = []
  const seen = new Set()

  const addTicker = (raw) => {
    const ticker = String(raw || "").trim().toUpperCase()
    if (!ticker || seen.has(ticker)) return
    if (!PRIVATE_TRACKED_TICKERS.has(ticker) && !/^[A-Z][A-Z0-9.-]{0,5}$/.test(ticker)) return
    tickers.push(ticker)
    seen.add(ticker)
  }

  for (const ticker of values) addTicker(ticker)
  if (ensurePrivate) {
    for (const ticker of PRIVATE_TRACKED_TICKERS) {
      if (!seen.has(ticker)) tickers.unshift(ticker)
    }
  }

  return tickers.slice(0, max)
}

function loadTrackedTickers(limit = TRACKED_TICKER_LIMIT) {
  const configured = process.env.TRACKED_TICKERS || ""
  if (configured.trim()) {
    return normalizeTickerList(configured.split(","), limit)
  }

  const configuredFile = process.env.TRACKED_TICKER_FILE || process.env.SOCIAL_TICKER_FILE || ""
  const candidates = configuredFile
    ? [path.isAbsolute(configuredFile) ? configuredFile : path.resolve(process.cwd(), configuredFile)]
    : TRACKED_TICKER_FILE_CANDIDATES

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/)
      const tickers = normalizeTickerList(lines, limit)
      if (tickers.length > 1) return tickers
    } catch {
      // Try the next known runtime layout.
    }
  }

  console.warn("Could not read tracked ticker file from known paths:", candidates.join(", "))
  return normalizeTickerList(["SPACEX"], limit)
}

async function loadArticleStats(db, days = 0) {
  const articles = db.collection("articles")
  const match = recentArticleMatch(days)
  const trackedTickers = loadTrackedTickers()
  const trackedMarketTickers = await loadTrackedMarketTickerSymbols(db, Number(process.env.TRACKED_MARKET_TICKER_LIMIT || 5000))

  const [sources, categories, sentimentRows, tickerRows, total, totalAll] = await Promise.all([
    articles.aggregate([
      ...articleMatchStage(match),
      { $group: { _id: "$source", count: { $sum: 1 } } },
      { $project: { _id: 0, source: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]).toArray(),
    articles.aggregate([
      ...articleMatchStage(match),
      { $group: { _id: "$category", count: { $sum: 1 } } },
      { $project: { _id: 0, category: "$_id", count: 1 } },
      { $sort: { count: -1 } }
    ]).toArray(),
    articles.aggregate([
      ...articleMatchStage(match),
      {
        $group: {
          _id: { $toLower: { $ifNull: ["$sentiment", "neutral"] } },
          count: { $sum: 1 }
        }
      }
    ]).toArray(),
    articles.aggregate(tickerArticlePipeline({ days, limit: 500 })).toArray(),
    articles.countDocuments(match),
    articles.countDocuments({})
  ])

  const sentiment = { bullish: 0, bearish: 0, neutral: 0, unknown: 0 }
  for (const row of sentimentRows) {
    const key = sentiment[row._id] == null ? "unknown" : row._id
    sentiment[key] = (sentiment[key] || 0) + row.count
  }

  return {
    total,
    total_recent: total,
    total_all: totalAll,
    sources,
    categories,
    sentiment,
    ticker_mentions: tickerRows,
    tracked_market_count: TRACKED_MARKETS.length,
    tracked_markets: TRACKED_MARKETS,
    tracked_exchanges: Array.from(US_EXCHANGES),
    tracked_indices: TRACKED_MARKET_INDICES,
    market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
    tracked_ticker_count: trackedTickers.length,
    tracked_tickers: trackedTickers,
    tracked_market_ticker_count: trackedMarketTickers.length,
    tracked_market_tickers: trackedMarketTickers.slice(0, 500),
  }
}

async function loadScreenerQuoteMap(db, tickers = []) {
  const unique = Array.from(new Set(tickers.map(t => String(t || "").toUpperCase()).filter(Boolean)))
  if (!unique.length) return new Map()

  const docs = await db.collection("screeners").find({ ticker: { $in: unique } }).toArray()
  return new Map(docs.map(doc => [String(doc.ticker || "").toUpperCase(), normalizeScreenerDoc(doc)]))
}

async function loadAllScreenerRows(db) {
  const docs = await db.collection("screeners").find({}).toArray()
  return docs.map(normalizeScreenerDoc).filter(row => row.ticker)
}

async function loadPositiveFinvizMoverRows(db, limit = 100) {
  const requestedLimit = Math.max(1, Math.min(300, Number(limit || 100)))

  const docs = await db.collection("screeners").find({
    ticker: { $exists: true, $nin: ["", null] },
    finviz_status: { $ne: "dropped" },
    $or: [
      { quote_source: "finviz_elite_screener" },
      { source: /finviz/i },
      { screener_source: /finviz/i },
      { finviz_filter: { $exists: true } },
      { finviz_seen_at: { $exists: true } },
    ],
  }).toArray()

  return docs
    .map(normalizeScreenerDoc)
    .filter(row => isFinvizScreenerRow(row) && isCleanListedUsScreenerRow(row))
    .sort((a, b) => {
      const changeDiff = Number(b.change_pct || 0) - Number(a.change_pct || 0)
      if (changeDiff !== 0) return changeDiff

      const relDiff = Number(b.rel_volume || 0) - Number(a.rel_volume || 0)
      if (relDiff !== 0) return relDiff

      return Number(b.volume || 0) - Number(a.volume || 0)
    })
    .slice(0, requestedLimit)
    .map((row, index) => ({
      ...row,
      finviz_rank: index + 1,
      discovery_source: "finviz_top_mover",
      positive_mover: true,
      sentiment: row.avg_sentiment || 0,
      article_count: row.news_article_count || 0,
      momentum_score: Number((row.change_pct || 0).toFixed(2)),
    }))
}

async function loadThresholdEntryRows(db, limit = 50) {
  const requestedLimit = Math.max(1, Math.min(200, Number(limit || 50)))
  const docs = await db.collection("screeners").find({
    ticker: { $exists: true, $nin: ["", null], $not: /\./ },
    exchange: { $in: Array.from(US_EXCHANGES) },
    price: { $gt: 0 },
    threshold_feature_policy_version: PREDICTION_THRESHOLD_POLICY_VERSION,
    threshold_feature_status: "entry_passed",
  }).sort({ threshold_setup_score: -1, threshold_feature_updated_at: -1, rel_volume: -1 }).limit(requestedLimit).toArray()

  return docs
    .map(normalizeScreenerDoc)
    .filter(row => isCleanListedUsThresholdEntryRow(row))
    .map((row, index) => ({
      ...row,
      rank: row.rank || index + 1,
      threshold_entry_candidate: true,
      discovery_source: "threshold_entry_passed",
      sentiment: row.avg_sentiment || 0,
      article_count: row.news_article_count || 0,
      momentum_score: Number((row.change_pct || 0).toFixed(2)),
    }))
}

async function loadTrackedMarketTickerSymbols(db, limit = 5000) {
  const requestedLimit = Math.max(1, Math.min(10000, Number(limit || 5000)))
  const docs = await db.collection("screeners").find(
    {
      ticker: { $exists: true, $nin: ["", null], $not: /\./ },
      exchange: { $in: Array.from(US_EXCHANGES) },
      quote_status: { $ne: "missing" },
    },
    { projection: { ticker: 1, volume: 1, market_cap: 1, quote_source: 1 } }
  ).sort({ volume: -1, market_cap: -1 }).limit(requestedLimit).toArray()
  return normalizeTickerList(docs.map(row => row.ticker), requestedLimit, { ensurePrivate: false })
}

async function loadArticleStatsForTickers(db, tickers = [], days = 2) {
  const wanted = new Set(tickers.map(t => String(t || "").toUpperCase()).filter(Boolean))
  if (!wanted.size) return new Map()

  const rows = await db.collection("articles")
    .aggregate(tickerArticlePipeline({ days, tickers: Array.from(wanted), limit: Math.max(wanted.size * 4, 150) }))
    .toArray()

  return new Map(
    rows
      .filter(row => wanted.has(String(row.ticker || "").toUpperCase()))
      .map(row => [String(row.ticker || "").toUpperCase(), row])
  )
}

async function loadSocialStatsForTickers(db, tickers = [], windowMinutes = 1440) {
  const wanted = normalizeTickerList(tickers, 300, { ensurePrivate: false })
  if (!wanted.length) return new Map()

  const sinceSec = Math.floor(Date.now() / 1000) - Math.max(1, Number(windowMinutes || 1440)) * 60
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    { $match: { _event_sec: { $gte: sinceSec } } },
    { $match: { _ticker_candidates: { $in: wanted } } },
    { $unwind: "$_ticker_candidates" },
    { $match: { _ticker_candidates: { $in: wanted } } },
    {
      $group: {
        _id: "$_ticker_candidates",
        count: { $sum: 1 },
        bullish: {
          $sum: {
            $cond: [
              { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } },
              1,
              0,
            ],
          },
        },
        bearish: {
          $sum: {
            $cond: [
              { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } },
              1,
              0,
            ],
          },
        },
        avg_sentiment_score: {
          $avg: {
            $switch: {
              branches: [
                {
                  case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] },
                  then: { $toDouble: "$sentiment_score" },
                },
                {
                  case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"] ] },
                  then: { $toDouble: "$sentiment" },
                },
                {
                  case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } },
                  then: 1,
                },
                {
                  case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } },
                  then: -1,
                },
              ],
              default: 0,
            },
          },
        },
        platforms: { $addToSet: "$_norm_platform" },
        latest_post: { $max: "$_event_sec" },
      },
    },
  ]).toArray()

  return new Map(rows.map(row => [String(row._id || "").toUpperCase(), row]))
}

async function loadPredictionValidationForTickers(db, tickers = []) {
  const wanted = normalizeTickerList(tickers, 300, { ensurePrivate: false })
  if (!wanted.length) return new Map()

  const rows = await db.collection("correlations").find({
    ticker: { $in: wanted },
    prediction_validation_samples: { $gte: 5 },
  }, {
    projection: {
      _id: 0,
      ticker: 1,
      correlation_score: 1,
      prediction_validation_correlation: 1,
      prediction_validation_accuracy_5m: 1,
      prediction_validation_samples: 1,
      prediction_validation_correct: 1,
      prediction_validation_avg_return_5m: 1,
      prediction_validation_latest_signal_sec: 1,
      prediction_validation_updated_at: 1,
      correlation_source: 1,
    },
  }).toArray().catch(() => [])

  return new Map(rows.map(row => [String(row.ticker || "").toUpperCase(), row]))
}

function bracketOrderResearchSignal(row, { sentiment, totalArticleCount, socialCount }) {
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const price = Number(row.price || 0)
  const supportCount = Number(totalArticleCount || 0) + Number(socialCount || 0)
  const capBucket = String(row.market_cap_bucket || "").toLowerCase()

  const moveScore = clamp(changePct / 20)
  const volumeScore = clamp(relVolume / 5)
  const sentimentScoreNorm = clamp((Number(sentiment || 0) + 1) / 2)
  const catalystScore = clamp(Math.log1p(supportCount) / Math.log1p(50))
  const confidence = clamp(
    moveScore * 0.35 +
    volumeScore * 0.25 +
    sentimentScoreNorm * 0.25 +
    catalystScore * 0.15
  )

  const volatile = capBucket === "micro" || capBucket === "small" || price < 5 || Math.abs(changePct) >= 25
  const stopLossPct = volatile ? 6 : 3
  const takeProfitPct = volatile ? 12 : 6
  const candidate = confidence >= 0.7 && changePct > 0 && sentiment > 0.05 && supportCount >= 2

  return {
    candidate,
    confidence: Number(confidence.toFixed(3)),
    direction: candidate ? "long_watch" : "monitor",
    entry_reference: Number.isFinite(price) && price > 0 ? Number(price.toFixed(4)) : null,
    stop_loss_pct: stopLossPct,
    take_profit_pct: takeProfitPct,
    support_count: supportCount,
    rationale: [
      changePct > 0 ? "positive price move" : "",
      relVolume >= 2 ? "elevated relative volume" : "",
      sentiment > 0.05 ? "positive weighted sentiment" : "",
      supportCount ? "matched news/social support" : "",
    ].filter(Boolean),
    status: "research_only_not_connected_to_broker",
  }
}

function mergeMoverContext(row, articleRow, socialRow) {
  const newsSentiment = articleRow ? sentimentScore(articleRow) : 0
  const structuredArticleCount = Number(articleRow?.structured_count || 0)
  const unstructuredArticleCount = Number(articleRow?.unstructured_count || 0)
  const totalArticleCount = Number(articleRow?.count || row.news_article_count || 0)
  const structuredSentiment = articleRow ? kindSentimentScore(articleRow, "structured") : Number(row.structured_sentiment || 0)
  const unstructuredSentiment = articleRow ? kindSentimentScore(articleRow, "unstructured") : 0
  const socialCount = Number(socialRow?.count || 0)
  const socialSentiment = socialCount
    ? Number((Number.isFinite(Number(socialRow.avg_sentiment_score))
      ? Number(socialRow.avg_sentiment_score)
      : ((socialRow.bullish || 0) - (socialRow.bearish || 0)) / Math.max(1, socialCount)).toFixed(3))
    : 0
  const structuredWeight = 2
  const unstructuredWeight = 1
  const socialWeight = 0.75
  const sentimentDenominator =
    structuredArticleCount * structuredWeight +
    unstructuredArticleCount * unstructuredWeight +
    socialCount * socialWeight
  const sentiment = sentimentDenominator
    ? Number(((
      structuredSentiment * structuredArticleCount * structuredWeight +
      unstructuredSentiment * unstructuredArticleCount * unstructuredWeight +
      socialSentiment * socialCount * socialWeight
    ) / sentimentDenominator).toFixed(3))
    : Number(row.avg_sentiment || 0)
  const bracketOrder = bracketOrderResearchSignal(row, { sentiment, totalArticleCount, socialCount })

  return {
    ...row,
    sentiment,
    article_sentiment: newsSentiment,
    social_sentiment: socialSentiment,
    structured_sentiment: structuredSentiment,
    unstructured_sentiment: unstructuredSentiment,
    article_count: totalArticleCount,
    structured_article_count: structuredArticleCount,
    unstructured_article_count: unstructuredArticleCount,
    news_article_count: totalArticleCount,
    message_count: socialCount,
    bullish_count: Number(articleRow?.bullish || 0) + Number(socialRow?.bullish || 0),
    bearish_count: Number(articleRow?.bearish || 0) + Number(socialRow?.bearish || 0),
    neutral_count: Number(articleRow?.neutral || 0),
    sources: [
      "Positive Movers",
      ...(articleRow?.sources || []),
      ...(socialRow?.platforms || []),
    ].filter(Boolean).slice(0, 8),
    latest_social: socialRow?.latest_post || null,
    momentum_score: Number((row.change_pct || 0).toFixed(2)),
    ai_numeric_rank: bracketOrder.confidence,
    bracket_order: bracketOrder,
  }
}

async function loadLatestMoverWatcherSnapshots(db, tickers = [], growthWindowMinutes = 1440) {
  if (!db || !Array.isArray(tickers) || !tickers.length) return new Map()
  return loadWatcherFeatureMap(db, tickers, {
    maxAgeSeconds: WATCHER_SNAPSHOT_MAX_AGE_SECONDS,
    growthWindowSeconds: normalizeRollingWindowMinutes(growthWindowMinutes) * 60,
  })
}

function isoFromSec(sec) {
  const value = Number(sec || 0)
  return Number.isFinite(value) && value > 0 ? new Date(value * 1000).toISOString() : null
}

function momentumTradingDate(sec = Math.floor(Date.now() / 1000)) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: MARKET_WINDOW_TIME_ZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(Number(sec || 0) * 1000))
}

function momentumQuoteAgeSeconds(row = {}) {
  const sec = timestampSeconds(row.quote_updated_at || row.quote_time || row.finviz_seen_at)
  return sec ? Math.max(0, Math.floor(Date.now() / 1000) - sec) : null
}

function momentumAlertScore(row = {}) {
  const change = Math.abs(Number(row.change_pct || 0))
  const relVol = Number(row.rel_volume || 0)
  const social = Number(row.message_count || 0)
  const articles = Number(row.article_count || 0)
  return change * 1.6 + Math.log1p(relVol) * 14 + Math.log1p(social + articles) * 6
}

function buildMomentumAlerts(rows = [], { limit = 8, windowMinutes = 1440 } = {}) {
  const alerts = []
  const requestedLimit = Math.max(1, Math.min(30, Number(limit || 8)))
  const minutes = Math.max(1, Number(windowMinutes || 1440))
  const addAlert = (row, type, severity, title, detail, scoreBoost = 0) => {
    if (!row?.ticker) return
    alerts.push({
      id: `${row.ticker}:${type}`,
      scope: "momentum",
      ticker: row.ticker,
      type,
      severity,
      title,
      detail,
      score: Number((momentumAlertScore(row) + scoreBoost).toFixed(2)),
      createdAt: new Date().toISOString(),
      source: "real_momentum_conditions",
      metrics: {
        change_pct: Number(row.change_pct || 0),
        rel_volume: Number(row.rel_volume || 0),
        price: row.price == null ? null : Number(row.price),
        article_count: Number(row.article_count || 0),
        message_count: Number(row.message_count || 0),
        message_density_per_hour: Number(((Number(row.message_count || 0) / minutes) * 60).toFixed(3)),
        sentiment: Number(row.sentiment || 0),
        quote_age_seconds: momentumQuoteAgeSeconds(row),
      },
    })
  }

  for (const row of rows) {
    const change = Number(row.change_pct || 0)
    const absChange = Math.abs(change)
    const relVol = Number(row.rel_volume || 0)
    const socialCount = Number(row.message_count || 0)
    const articleCount = Number(row.article_count || 0)
    const densityPerHour = (socialCount / minutes) * 60
    const sentiment = Number(row.sentiment || 0)
    const quoteAge = momentumQuoteAgeSeconds(row)
    const fresh = quoteAge == null ? false : quoteAge <= 30 * 60
    const evidence = articleCount + socialCount
    const price = Number(row.price || 0)
    const floatShort = Number(row.float_short || 0)

    if (fresh && relVol >= 2 && absChange >= 8) {
      addAlert(row, "fresh_mover", "watch", "Fresh mover", `${change >= 0 ? "+" : ""}${change.toFixed(2)}% move with ${relVol.toFixed(2)}x relative volume.`, 12)
    }
    if (relVol >= 10) {
      addAlert(row, "high_relative_volume", relVol >= 25 ? "warning" : "watch", "High relative volume", `${relVol.toFixed(2)}x relative volume versus stored screener baseline.`, 10)
    }
    if (absChange >= 20) {
      addAlert(row, "strong_price_move", absChange >= 50 ? "critical" : "warning", "Strong price move", `${change >= 0 ? "+" : ""}${change.toFixed(2)}% price move in the active screener row.`, 9)
    }
    if (densityPerHour >= 10) {
      addAlert(row, "message_density_spike", "watch", "Message density spike", `${densityPerHour.toFixed(1)} social messages/hour in the selected ${minutes}m window.`, 8)
    }
    if (evidence > 0 && Math.abs(sentiment) >= 0.25) {
      addAlert(row, sentiment > 0 ? "positive_sentiment" : "negative_sentiment", sentiment > 0 ? "watch" : "warning", sentiment > 0 ? "Positive sentiment" : "Negative sentiment", `${sentiment > 0 ? "+" : ""}${sentiment.toFixed(2)} weighted sentiment across ${evidence} evidence item${evidence === 1 ? "" : "s"}.`, 7)
    }
    if (articleCount > 0) {
      addAlert(row, "catalyst_found", "watch", "Catalyst/news found", `${articleCount} real news item${articleCount === 1 ? "" : "s"} attached to this mover.`, 6)
    }
    if (Number(row.ai_numeric_rank || row.bracket_order?.confidence || 0) >= 0.7) {
      addAlert(row, "high_watch_score", "watch", "High watch score", `Research score ${(Number(row.ai_numeric_rank || row.bracket_order?.confidence || 0) * 100).toFixed(0)}/100 from price, volume, sentiment, and evidence.`, 7)
    }
    if ((floatShort >= 15 || relVol >= 20) && (price > 0 && price < 10) && absChange >= 10) {
      addAlert(row, "squeeze_volatility", "warning", "Squeeze/volatility warning", `${relVol.toFixed(2)}x relative volume${floatShort ? ` and ${floatShort.toFixed(1)}% float short` : ""} on a sub-$10 mover.`, 8)
    }
  }

  const seen = new Set()
  return alerts
    .sort((a, b) => b.score - a.score)
    .filter(alert => {
      const key = `${alert.ticker}:${alert.type}`
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
    .slice(0, requestedLimit)
}

async function momentumMonitorMetadata(db, rows = [], { totalRows = rows.length, socialWindow = 1440, cacheMode = "mongo" } = {}) {
  const [finvizCount, finvizLatest] = await Promise.all([
    db.collection("screeners").countDocuments({
      quote_source: "finviz_elite_screener",
      finviz_status: { $ne: "dropped" },
    }),
    db.collection("screeners").findOne(
      { quote_source: "finviz_elite_screener", finviz_status: { $ne: "dropped" } },
      { sort: { finviz_seen_at: -1, quote_updated_at: -1 }, projection: { finviz_seen_at: 1, quote_updated_at: 1, ticker: 1 } }
    ),
  ])
  const latestSec = timestampSeconds(finvizLatest?.finviz_seen_at || finvizLatest?.quote_updated_at)
  const ageSeconds = latestSec ? Math.max(0, Math.floor(Date.now() / 1000) - latestSec) : null
  const sourceNames = new Set()
  if (finvizCount > 0) sourceNames.add("Finviz Elite Screener")
  for (const row of rows) {
    for (const source of row.sources || []) {
      if (source && source !== "Positive Movers") sourceNames.add(String(source))
    }
    if (row.quote_source) sourceNames.add(String(row.quote_source))
  }
  const stale = ageSeconds != null && ageSeconds > 30 * 60
  const status = !finvizCount
    ? "missing"
    : !rows.length
      ? "partial"
      : stale
        ? "stale"
        : "healthy"
  const label = status === "missing"
    ? "No FinViz metadata yet"
    : status === "partial"
      ? "FinViz rows exist; current filters hide all movers"
      : status === "stale"
        ? "Screener cache is stale"
        : "Healthy"
  return {
    status,
    label,
    finvizRows: finvizCount,
    visibleTickerCount: rows.length,
    filteredTickerCount: Number(totalRows || rows.length),
    quoteAgeSeconds: ageSeconds,
    screenerAgeSeconds: ageSeconds,
    lastFetchAt: isoFromSec(latestSec),
    latestTicker: finvizLatest?.ticker || null,
    liveSourceCount: sourceNames.size,
    liveSources: Array.from(sourceNames).slice(0, 12),
    cacheMode,
    cacheHit: false,
    dataFreshness: status,
    socialWindowMinutes: Number(socialWindow || 1440),
  }
}

async function saveMomentumSnapshot(db, rows = [], metadata = {}, { source = "Finviz Elite top movers", cacheMode = "mongo" } = {}) {
  if (!rows.length) return null
  const nowSec = Math.floor(Date.now() / 1000)
  const snapshotSec = Math.floor(nowSec / 60) * 60
  const tickers = rows.map(row => row.ticker).filter(Boolean)
  const doc = {
    _id: `momentum:${snapshotSec}`,
    createdAt: new Date(snapshotSec * 1000),
    created_at: new Date(snapshotSec * 1000),
    snapshot_sec: snapshotSec,
    tradingDate: momentumTradingDate(snapshotSec),
    trading_date: momentumTradingDate(snapshotSec),
    source,
    rowCount: rows.length,
    row_count: rows.length,
    tickers,
    top_tickers: tickers.slice(0, 20),
    topMovers: rows.slice(0, 20).map((row, index) => ({
      rank: index + 1,
      ticker: row.ticker,
      price: row.price == null ? null : Number(row.price),
      change_pct: Number(row.change_pct || 0),
      rel_volume: Number(row.rel_volume || 0),
      volume: Number(row.volume || 0),
      sentiment: Number(row.sentiment || 0),
      article_count: Number(row.article_count || 0),
      message_count: Number(row.message_count || 0),
    })),
    metadata,
    cacheMode,
    cache_mode: cacheMode,
  }
  await db.collection("momentum_snapshots").updateOne(
    { _id: doc._id },
    { $set: doc },
    { upsert: true },
  )
  await db.collection("momentum_snapshots").createIndex({ snapshot_sec: -1 }).catch(() => {})
  return doc
}

async function loadMomentumSnapshots(db, limit = 6) {
  const docs = await db.collection("momentum_snapshots")
    .find({})
    .sort({ snapshot_sec: -1, createdAt: -1 })
    .limit(Math.max(1, Math.min(50, Number(limit || 6))))
    .toArray()
  return docs.map(doc => ({
    createdAt: doc.createdAt || doc.created_at || null,
    created_at: doc.created_at || doc.createdAt || null,
    snapshot_sec: Number(doc.snapshot_sec || timestampSeconds(doc.createdAt || doc.created_at) || 0),
    tradingDate: doc.tradingDate || doc.trading_date || null,
    trading_date: doc.trading_date || doc.tradingDate || null,
    source: doc.source || "Finviz Elite top movers",
    rowCount: Number(doc.rowCount ?? doc.row_count ?? doc.tickers?.length ?? 0),
    row_count: Number(doc.row_count ?? doc.rowCount ?? doc.tickers?.length ?? 0),
    tickers: doc.tickers || doc.top_tickers || [],
    top_tickers: doc.top_tickers || doc.tickers || [],
    topMovers: doc.topMovers || doc.top_movers || [],
    metadata: doc.metadata || {},
    cacheMode: doc.cacheMode || doc.cache_mode || "mongo",
    cache_mode: doc.cache_mode || doc.cacheMode || "mongo",
  }))
}

function tradeWatchDecision(row) {
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const price = Number(row.price || 0)
  const sentiment = Number(row.sentiment || 0)
  const articleSentiment = Number(row.article_sentiment || 0)
  const socialSentiment = Number(row.social_sentiment || 0)
  const articleCount = Number(row.article_count || 0)
  const structuredCount = Number(row.structured_article_count || 0)
  const publicCount = Number(row.unstructured_article_count || 0)
  const socialCount = Number(row.message_count || 0)
  const supportCount = articleCount + socialCount
  const latestQuoteSec = timestampSeconds(row.quote_updated_at || row.quote_time)
  const quoteAgeMinutes = latestQuoteSec ? Math.max(0, (Date.now() / 1000 - latestQuoteSec) / 60) : null
  const quoteFreshness = quoteAgeMinutes == null ? 0.45 : clamp(1 - quoteAgeMinutes / 360, 0.2, 1)
  const capBucket = String(row.market_cap_bucket || "").toLowerCase()
  const microOrPenny = capBucket === "micro" || (Number.isFinite(price) && price > 0 && price < 1)

  const priceScore = clamp(changePct / 25)
  const volumeScore = clamp(relVolume / 8)
  const structuredScore = clamp(Math.log1p(structuredCount) / Math.log1p(8))
  const publicNewsScore = clamp(Math.log1p(publicCount) / Math.log1p(12))
  const socialDensityScore = clamp(Math.log1p(socialCount) / Math.log1p(60))
  const sentimentMagnitude = clamp((sentiment + 1) / 2)
  const socialMagnitude = clamp((socialSentiment + 1) / 2)
  const articleMagnitude = clamp((articleSentiment + 1) / 2)
  const evidenceScore = clamp(
    structuredScore * 0.3 +
    publicNewsScore * 0.2 +
    socialDensityScore * 0.25 +
    sentimentMagnitude * 0.15 +
    socialMagnitude * 0.1
  )
  const agreement = clamp(
    (changePct > 0 ? 0.25 : 0) +
    (sentiment > 0.05 ? 0.25 : sentiment < -0.05 ? -0.15 : 0.05) +
    (socialCount > 0 && socialSentiment > 0.05 ? 0.2 : socialCount > 0 && socialSentiment < -0.05 ? -0.1 : 0) +
    (articleCount > 0 && articleSentiment > 0.05 ? 0.2 : articleCount > 0 && articleSentiment < -0.05 ? -0.1 : 0) +
    (relVolume >= 2 ? 0.1 : 0),
    0,
    1
  )
  const thinSpikePenalty = supportCount === 0 ? 0.18 : supportCount === 1 ? 0.08 : 0
  const microPenalty = microOrPenny && supportCount < 3 ? 0.08 : 0
  const rawScore =
    priceScore * 0.25 +
    volumeScore * 0.2 +
    evidenceScore * 0.25 +
    agreement * 0.2 +
    quoteFreshness * 0.1 -
    thinSpikePenalty -
    microPenalty
  const score = clamp(rawScore)

  let decision = "Monitor"
  if (score >= 0.74 && agreement >= 0.65 && supportCount >= 2) decision = "High Watch"
  else if (score >= 0.58 && supportCount >= 1) decision = "Watch"
  else if (changePct >= 15 && supportCount === 0) decision = "Unsupported Spike"
  else if (sentiment < -0.15 || socialSentiment < -0.2) decision = "Divergent"

  const reasons = [
    changePct > 0 ? `price +${changePct.toFixed(2)}%` : "",
    relVolume >= 2 ? `${relVolume.toFixed(1)}x rel vol` : "",
    structuredCount ? `${structuredCount} structured news` : "",
    publicCount ? `${publicCount} public news` : "",
    socialCount ? `${socialCount} social posts` : "",
    sentiment > 0.05 ? `weighted sent +${sentiment.toFixed(2)}` : sentiment < -0.05 ? `weighted sent ${sentiment.toFixed(2)}` : "",
  ].filter(Boolean).slice(0, 5)
  const risks = [
    supportCount === 0 ? "no matched news/social evidence yet" : "",
    quoteAgeMinutes != null && quoteAgeMinutes > 180 ? `quote ${Math.round(quoteAgeMinutes)}m old` : "",
    microOrPenny ? "microcap/penny volatility" : "",
    socialCount > 0 && socialSentiment < -0.05 ? "negative social tone" : "",
    articleCount > 0 && articleSentiment < -0.05 ? "negative news tone" : "",
  ].filter(Boolean).slice(0, 4)

  return {
    trade_watch_score: Number(score.toFixed(3)),
    decision,
    confidence: Number((score * 100).toFixed(1)),
    agreement: Number(agreement.toFixed(3)),
    evidence_score: Number(evidenceScore.toFixed(3)),
    quote_freshness: Number(quoteFreshness.toFixed(3)),
    quote_age_minutes: quoteAgeMinutes == null ? null : Number(quoteAgeMinutes.toFixed(1)),
    support_count: supportCount,
    reasons,
    risks,
  }
}

function addTradeWatchFields(row) {
  return {
    ...row,
    trade_watch: tradeWatchDecision(row),
  }
}

const PREDICTION_HORIZONS_MINUTES = [5, 15, 60]
const PREDICTION_MODEL_ID = "trade_watch_linear_v1"
const PREDICTION_FEATURE_KEYS = [
  "change_pct",
  "rel_volume",
  "article_count",
  "article_sentiment",
  "structured_sentiment",
  "social_count",
  "social_density_per_minute",
  "social_sentiment",
  "weighted_sentiment",
  "evidence_score",
  "trade_watch_score",
  "agreement",
]

const PREDICTION_THRESHOLD_POLICY_VERSION = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY_VERSION
const PREDICTION_THRESHOLD_POLICY = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY

function predictionMarketCapTier(row = {}) {
  return predictionThresholdPolicy.predictionMarketCapTier(row)
}

function clonePlain(value) {
  return JSON.parse(JSON.stringify(value))
}

function normalizedSharesFloat(row = {}) {
  const value = nullableNumber(row.shares_float ?? row.float ?? row.float_shares)
  return value != null && value > 0 ? value : null
}

function predictionFloatBucket(row = {}) {
  return predictionThresholdPolicy.predictionFloatBucket(row)
}

function floatBucketLabel(bucket = "unknown") {
  if (bucket === "ultra_low") return "ultra-low float"
  if (bucket === "low") return "low float"
  if (bucket === "mid") return "mid float"
  if (bucket === "high") return "high float"
  if (bucket === "very_high") return "very high float"
  return "unknown float"
}

function socialBullBearDelta(row = {}) {
  return Number(row.bullish_count || 0) - Number(row.bearish_count || 0)
}

function catalystCount(row = {}) {
  return Number(row.article_count ?? row.news_article_count ?? 0)
}

function positiveSocialSupport(row = {}, minDelta = 1) {
  return Number(row.message_count || 0) > 0 &&
    (socialBullBearDelta(row) >= minDelta || Number(row.social_sentiment || row.sentiment || 0) >= 0.12)
}

function thresholdGuardEvaluation(row = {}, profile = {}, baseMinTrailing60Messages = 0) {
  return predictionThresholdPolicy.thresholdGuardEvaluation(row, profile, baseMinTrailing60Messages)
}

function predictionThresholdProfile(row = {}, profileOverride = null) {
  return predictionThresholdPolicy.predictionThresholdProfile(row, profileOverride)
}

function evaluatePredictionEntryThreshold(row = {}, features = {}, profileOverride = null) {
  return predictionThresholdPolicy.evaluatePredictionEntryThreshold(row, features, profileOverride)
}

function predictionFeaturesFromMover(row, socialWindowMinutes = 60) {
  const socialCount = Number(row.message_count || 0)
  const articleCount = Number(row.article_count || 0)
  const relVolume = Number(row.rel_volume || 0)
  const changePct = Number(row.change_pct || 0)
  const sentiment = Number(row.sentiment || 0)
  const validation = row.prediction_validation || {}
  const validationCorrelation = nullableNumber(row.prediction_validation_correlation ?? row.correlation_score)
  const validationAccuracy = nullableNumber(row.prediction_validation_accuracy_5m ?? validation.accuracy_5m ?? validation.accuracy)
  const validationSamples = nullableNumber(row.prediction_validation_samples ?? validation.samples)
  const validationAvgReturn = nullableNumber(row.prediction_validation_avg_return_5m ?? validation.avg_return_5m)
  const priceDensityCorrelation = nullableNumber(row.price_density_correlation)
  const previousPriceDensityCorrelation = nullableNumber(row.previous_price_density_correlation)
  const thresholdSetupScore = nullableNumber(row.threshold_setup_score)
  const thresholdSetupDistance = nullableNumber(row.threshold_setup_distance_to_entry)
  const evidenceScore =
    Number(row.article_sentiment || 0) * Math.min(1, articleCount / 5) +
    Number(row.social_sentiment || 0) * Math.min(1, socialCount / 20)

  return {
    price: Number(row.price || 0),
    change_pct: changePct,
    volume: Number(row.volume || 0),
    rel_volume: Number(relVolume.toFixed(3)),
    market_cap: Number(row.market_cap || 0),
    market_cap_bucket: row.market_cap_bucket || "Unknown",
    market_cap_tier: predictionMarketCapTier(row),
    shares_float: row.shares_float ?? null,
    float_bucket: predictionFloatBucket(row),
    float_short: row.float_short ?? null,
    short_interest: row.short_interest ?? null,
    price_density_correlation: priceDensityCorrelation,
    previous_price_density_correlation: previousPriceDensityCorrelation,
    correlation_score: validationCorrelation,
    prediction_validation_correlation: validationCorrelation,
    prediction_validation_accuracy_5m: validationAccuracy,
    prediction_validation_samples: validationSamples,
    prediction_validation_avg_return_5m: validationAvgReturn,
    prediction_validation_latest_signal_sec: row.prediction_validation_latest_signal_sec ?? null,
    threshold_pre_return_60m_pct: row.threshold_pre_return_60m_pct ?? null,
    threshold_trailing_60m_messages: row.threshold_trailing_60m_messages ?? null,
    threshold_setup_status: row.threshold_setup_status ?? null,
    threshold_setup_score: thresholdSetupScore,
    threshold_setup_distance_to_entry: thresholdSetupDistance,
    threshold_feature_policy_version: row.threshold_feature_policy_version ?? PREDICTION_THRESHOLD_POLICY_VERSION,
    threshold_feature_policy_name: row.threshold_feature_policy_name ?? predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule.name,
    threshold_feature_source: row.threshold_feature_source ?? predictionThresholdPolicy.PREDICTION_THRESHOLD_FEATURE_SOURCE,
    threshold_feature_window_minutes: row.threshold_feature_window_minutes ?? null,
    threshold_feature_updated_at: row.threshold_feature_updated_at ?? null,
    threshold_feature_provenance: row.threshold_feature_provenance ?? null,
    rsi: row.rsi ?? null,
    gap: row.gap ?? null,
    perf_week: row.perf_week ?? null,
    perf_month: row.perf_month ?? null,
    article_count: articleCount,
    structured_article_count: Number(row.structured_article_count || 0),
    unstructured_article_count: Number(row.unstructured_article_count || 0),
    article_sentiment: Number(Number(row.article_sentiment || 0).toFixed(3)),
    structured_sentiment: Number(Number(row.structured_sentiment || 0).toFixed(3)),
    unstructured_sentiment: Number(Number(row.unstructured_sentiment || 0).toFixed(3)),
    social_count: socialCount,
    social_density_per_minute: Number((socialCount / Math.max(1, socialWindowMinutes)).toFixed(3)),
    social_sentiment: Number(Number(row.social_sentiment || 0).toFixed(3)),
    weighted_sentiment: Number(sentiment.toFixed(3)),
    evidence_score: Number(evidenceScore.toFixed(3)),
    trade_watch_score: Number(row.trade_watch?.trade_watch_score || 0),
    agreement: Number(row.trade_watch?.agreement || 0),
    is_news_catalyst: Boolean(
      validation.recognizedNewsCatalyst ||
      validation.materialDirectCatalyst ||
      validation.recognizedSqueezeCatalyst ||
      row.validated_prediction ||
      row.prediction_validation_status === 'validated'
    ) ? 1 : 0,
  }
}

function baselinePredictionFromMover(row, thresholdEntry = null) {
  const features = predictionFeaturesFromMover(row)
  const tradeScore = Number(row.trade_watch?.trade_watch_score || 0)
  const evidence = Number(features.evidence_score || 0)
  const changePct = Number(row.change_pct || 0)
  const relVolume = Number(row.rel_volume || 0)
  const rawDirection = evidence >= 0.12 && changePct > 0
    ? "up"
    : evidence <= -0.12 && changePct < 0
      ? "down"
      : "watch"
  const entryReady = Boolean(thresholdEntry?.passed)
  const direction = entryReady ? rawDirection : "watch"
  const confidence = clamp(
    tradeScore * 0.45 +
    Math.min(1, Math.abs(evidence)) * 0.25 +
    Math.min(1, relVolume / 6) * 0.15 +
    Math.min(1, Math.abs(changePct) / 25) * 0.15
  )
  return {
    direction,
    raw_direction: rawDirection,
    confidence: Number(confidence.toFixed(3)),
    model: "baseline_trade_watch_v1",
    model_ready: Boolean(row.price && (row.article_count || row.message_count) && Number.isFinite(changePct) && entryReady),
    entry_ready: entryReady,
    threshold_status: thresholdEntry?.status || "not_evaluated",
    threshold_policy_version: thresholdEntry?.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
  }
}

function thresholdRulePredictionFromEntry(row, thresholdEntry = null) {
  const entryReady = Boolean(thresholdEntry?.passed)
  if (!entryReady) return null
  const backtest = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule?.backtestSummary || {}
  const expectedReturn = Number(backtest.meanNetReturnPct)
  const winRate = Number(backtest.winRate)
  if (!Number.isFinite(expectedReturn)) return null
  return {
    direction: expectedReturn > 0 ? "up" : expectedReturn < 0 ? "down" : "watch",
    raw_direction: expectedReturn > 0 ? "up" : expectedReturn < 0 ? "down" : "watch",
    predicted_return_intraday_trade: Number(expectedReturn.toFixed(3)),
    probability_up: Number.isFinite(winRate) ? Number(winRate.toFixed(3)) : null,
    confidence: Number.isFinite(winRate) ? Number(Math.abs(winRate - 0.5).toFixed(3)) : null,
    model: "threshold_rule_backtest_expectancy_v3",
    model_ready: true,
    entry_ready: true,
    threshold_status: thresholdEntry?.status || "entry_passed",
    threshold_policy_version: thresholdEntry?.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
    horizon: "intraday_trade_until_stop_or_eod",
    expected_return_source: "backtest_mean_net_return",
    backtest_trades: Number(backtest.trades || 0),
    backtest_profit_factor: Number.isFinite(Number(backtest.profitFactor)) ? Number(Number(backtest.profitFactor).toFixed(3)) : null,
    backtest_validation_mean_return_pct: Number.isFinite(Number(backtest.validationMeanNetReturnPct)) ? Number(Number(backtest.validationMeanNetReturnPct).toFixed(3)) : null,
    backtest_test_mean_return_pct: Number.isFinite(Number(backtest.testMeanNetReturnPct)) ? Number(Number(backtest.testMeanNetReturnPct).toFixed(3)) : null,
    note: "Rules-based prediction from the promoted message-density threshold backtest; not a trained ML next-day forecast.",
  }
}

function sigmoid(value) {
  return 1 / (1 + Math.exp(-value))
}

function applyPredictionModel(features = {}, model = null) {
  if (!model?.weights || !model?.feature_stats) return null
  let score = Number(model.intercept || 0)
  for (const key of model.feature_keys || PREDICTION_FEATURE_KEYS) {
    const stat = model.feature_stats[key] || { mean: 0, std: 1 }
    const std = Number(stat.std || 1) || 1
    const raw = Number(features[key])
    const normalized = Number.isFinite(raw) ? (raw - Number(stat.mean || 0)) / std : 0
    score += Number(model.weights[key] || 0) * normalized
  }
  const predictedReturn = Number(score.toFixed(3))
  const probabilityUp = Number(sigmoid(score / Math.max(0.25, Number(model.target_std || 1))).toFixed(3))
  return {
    model: model.model_id || PREDICTION_MODEL_ID,
    model_version: model.version || 1,
    direction: predictedReturn > 0.05 ? "up" : predictedReturn < -0.05 ? "down" : "watch",
    predicted_return_5m: predictedReturn,
    probability_up: probabilityUp,
    confidence: Number(Math.abs(probabilityUp - 0.5).toFixed(3)),
    trained_samples: Number(model.samples || 0),
  }
}

async function loadLatestPredictionModel(db) {
  return db.collection("prediction_models").findOne({ _id: PREDICTION_MODEL_ID })
}

function modelValidationState(model = null) {
  if (model?.status !== "trained") {
    return { status: model?.status || "missing", allow_live_classifier: false, edge: null, reason: "model_not_trained" }
  }
  if (!model.metrics) {
    return { status: "training_evaluation", allow_live_classifier: true, edge: null, reason: "temporary_model_without_persisted_metrics" }
  }
  const metrics = model.metrics || {}
  const actionable = Number(metrics.actionable_samples || 0)
  const accuracy = Number(metrics.directional_accuracy_5m)
  const baselineAccuracy = Number(metrics.baseline_directional_accuracy_5m)
  const minSamples = Number(process.env.MIN_LIVE_MODEL_VALIDATION_SAMPLES || 50)
  const minEdge = Number(process.env.MIN_LIVE_MODEL_EDGE || 0)
  const minAccuracy = Number(process.env.MIN_LIVE_MODEL_ACCURACY || 0.60)
  const edge = Number.isFinite(accuracy) && Number.isFinite(baselineAccuracy) ? Number((accuracy - baselineAccuracy).toFixed(3)) : null
  if (model?.direction_classifier?.type === "knn_centroid_direction_v1" && process.env.ALLOW_KNN_LIVE_MODEL !== "1") {
    return { status: "shadow_knn_disabled", allow_live_classifier: false, edge, reason: "knn_live_use_disabled" }
  }
  if (actionable < minSamples) {
    return { status: "shadow_insufficient_validation", allow_live_classifier: false, edge, reason: `needs_at_least_${minSamples}_actionable_holdout_samples` }
  }
  if (!Number.isFinite(accuracy)) {
    return { status: "shadow_no_validation_accuracy", allow_live_classifier: false, edge, reason: "missing_validation_accuracy" }
  }
  if (accuracy < minAccuracy) {
    return { status: "shadow_below_required_accuracy", allow_live_classifier: false, edge, reason: `model_accuracy_${accuracy.toFixed(3)}_below_required_${minAccuracy.toFixed(3)}` }
  }
  if (Number.isFinite(baselineAccuracy) && accuracy < baselineAccuracy + minEdge) {
    return { status: "shadow_under_baseline", allow_live_classifier: false, edge, reason: `model_accuracy_${accuracy.toFixed(3)}_below_required_baseline_${(baselineAccuracy + minEdge).toFixed(3)}` }
  }
  return { status: "live_validated_edge", allow_live_classifier: true, edge, reason: "model_beats_recent_baseline" }
}

function isLiveModelSignalEligible(signal = null, model = null) {
  if (!signal || model?.status !== "trained") return false
  if (Number(signal.confidence || 0) < MIN_LIVE_MODEL_CONFIDENCE) return false
  return modelValidationState(model).allow_live_classifier
}

async function loadEnrichedTradeWatchRows(db, { limit = 10, days = 2, socialWindow = 60 } = {}) {
  const requestedLimit = Math.max(1, Math.min(50, Number(limit || 10)))
  const [movers, thresholdEntries] = await Promise.all([
    loadPositiveFinvizMoverRows(db, Math.max(requestedLimit * 6, 100)),
    loadThresholdEntryRows(db, Math.max(requestedLimit, 50)),
  ])
  const byTicker = new Map()
  for (const row of movers) byTicker.set(row.ticker, row)
  for (const row of thresholdEntries) byTicker.set(row.ticker, { ...(byTicker.get(row.ticker) || {}), ...row, threshold_entry_candidate: true })
  const universe = [...byTicker.values()]
  const articleMap = await loadArticleStatsForTickers(db, universe.map(row => row.ticker), days)
  const socialMap = await loadSocialStatsForTickers(db, universe.map(row => row.ticker), socialWindow)
  return universe
    .map(row => addTradeWatchFields(mergeMoverContext(row, articleMap.get(row.ticker), socialMap.get(row.ticker))))
    .sort((a, b) => {
      const entryDiff = Number(Boolean(b.threshold_entry_candidate)) - Number(Boolean(a.threshold_entry_candidate))
      if (entryDiff !== 0) return entryDiff
      const scoreDiff = Number(b.trade_watch?.trade_watch_score || 0) - Number(a.trade_watch?.trade_watch_score || 0)
      if (scoreDiff !== 0) return scoreDiff
      const evidenceDiff = Number(b.trade_watch?.evidence_score || 0) - Number(a.trade_watch?.evidence_score || 0)
      if (evidenceDiff !== 0) return evidenceDiff
      return Number(b.change_pct || 0) - Number(a.change_pct || 0)
    })
    .slice(0, requestedLimit)
}

async function captureTradeWatchPredictionSignals(db, { limit = 10, days = 2, socialWindow = 60 } = {}) {
  const nowSec = Math.floor(Date.now() / 1000)
  const minuteBucket = Math.floor(nowSec / 60) * 60
  const [rows, model] = await Promise.all([
    loadEnrichedTradeWatchRows(db, { limit, days, socialWindow }),
    loadLatestPredictionModel(db),
  ])
  const docs = rows
    .filter(row => Number(row.price || 0) > 0)
    .map((row, index) => {
      const signalId = `${row.ticker}:${minuteBucket}`
      const features = predictionFeaturesFromMover(row, socialWindow)
      const thresholdEntry = evaluatePredictionEntryThreshold(row, features)
      const baseline = baselinePredictionFromMover(row, thresholdEntry)
      const thresholdRuleSignal = thresholdRulePredictionFromEntry(row, thresholdEntry)
      const rawModelSignal = applyPredictionModel(features, model)
      const modelSignal = rawModelSignal ? {
        ...rawModelSignal,
        raw_direction: rawModelSignal.direction,
        direction: thresholdEntry.passed ? rawModelSignal.direction : "watch",
        entry_ready: Boolean(thresholdEntry.passed),
        threshold_status: thresholdEntry.status,
        threshold_policy_version: thresholdEntry.policyVersion,
      } : null
      return {
        _id: signalId,
        signal_id: signalId,
        ticker: row.ticker,
        company: row.company || "",
        exchange: row.exchange || "",
        sector: row.sector || "",
        source: "trade_watch",
        discovery_source: row.discovery_source || "trade_watch",
        threshold_entry_candidate: Boolean(row.threshold_entry_candidate),
        signal_sec: minuteBucket,
        signal_at: new Date(minuteBucket * 1000),
        entry_price: Number(row.price || 0),
        entry_quote_source: row.quote_source || null,
        entry_quote_updated_at: row.quote_updated_at || null,
        rank: index + 1,
        decision: row.trade_watch?.decision || "Monitor",
        trade_watch: row.trade_watch || {},
        features,
        threshold_policy: thresholdEntry,
        entry_signal: {
          policy_version: thresholdEntry.policyVersion,
          tier: thresholdEntry.tier,
          status: thresholdEntry.status,
          passed: thresholdEntry.passed,
          entry_ready: Boolean(thresholdEntry.passed),
          execution: thresholdEntry.mechanics?.entry_execution,
          exit_rule: thresholdEntry.mechanics?.exit_rule,
          reason: thresholdEntry.reason,
          float_bucket: thresholdEntry.floatBucket,
          max_signal_abs_change_pct: thresholdEntry.maxSignalAbsChangePct,
          active_signal_abs_change_pct: thresholdEntry.activeSignalAbsChangePct,
          rejection_reasons: thresholdEntry.rejectionReasons || [],
        },
        baseline_signal: baseline,
        threshold_rule_signal: thresholdRuleSignal,
        model_signal: modelSignal,
        labels: {},
        label_status: "pending",
        horizons_minutes: PREDICTION_HORIZONS_MINUTES,
        created_at: new Date(),
        updated_at: new Date(),
      }
    })

  if (!docs.length) return { saved: 0, rows: [] }
  await db.collection("active_ticker_context").bulkWrite(
    docs.map(doc => ({
      updateOne: {
        filter: { ticker: doc.ticker },
        update: {
          $set: {
            ticker: doc.ticker,
            company: doc.company,
            exchange: doc.exchange,
            sector: doc.sector,
            source: doc.source,
            discovery_source: doc.discovery_source,
            signal_id: doc.signal_id,
            signal_sec: doc.signal_sec,
            signal_at: doc.signal_at,
            entry_price: doc.entry_price,
            rank: doc.rank,
            decision: doc.decision,
            trade_watch: doc.trade_watch,
            features: doc.features,
            threshold_policy: doc.threshold_policy,
            entry_signal: doc.entry_signal,
            baseline_signal: doc.baseline_signal,
            model_signal: doc.model_signal,
            threshold_policy_version: doc.entry_signal?.policy_version || PREDICTION_THRESHOLD_POLICY_VERSION,
            feature_policy_version: doc.features?.threshold_feature_policy_version || PREDICTION_THRESHOLD_POLICY_VERSION,
            feature_window_minutes: doc.features?.threshold_feature_window_minutes || null,
            context_version: "active_ticker_context_v1",
            context_provenance: {
              source: "trade_watch_prediction_snapshot",
              policy_version: doc.entry_signal?.policy_version || PREDICTION_THRESHOLD_POLICY_VERSION,
              threshold_feature_source: doc.features?.threshold_feature_source || null,
              signal_sec: doc.signal_sec,
              materialized_at: new Date(),
            },
            updated_at: new Date(),
          },
          $setOnInsert: { created_at: new Date() },
        },
        upsert: true,
      },
    })),
    { ordered: false },
  ).catch(err => console.warn("active_ticker_context materialization failed:", err.message || err))
  const result = await db.collection("prediction_signals").bulkWrite(
    docs.map(doc => {
      const { _id, labels, label_status, created_at, ...refreshFields } = doc
      return {
        updateOne: {
          filter: { _id: doc._id },
          update: {
            $setOnInsert: { _id, labels, label_status, created_at },
            $set: { ...refreshFields, last_seen_at: new Date(), last_rank: doc.rank },
          },
          upsert: true,
        },
      }
    }),
    { ordered: false }
  )
  return { saved: Number(result.upsertedCount || 0), rows: docs }
}

async function loadOutcomeOhlcBars(db, docs = [], maxHorizonMinutes = 60) {
  const byTicker = new Map()
  for (const doc of docs) {
    const ticker = String(doc.ticker || "").toUpperCase().trim()
    const signalSec = Number(doc.signal_sec || 0)
    if (!ticker || !Number.isFinite(signalSec) || signalSec <= 0) continue
    const row = byTicker.get(ticker) || { minSec: signalSec, maxSec: signalSec, docs: [] }
    row.minSec = Math.min(row.minSec, signalSec)
    row.maxSec = Math.max(row.maxSec, signalSec)
    row.docs.push(doc)
    byTicker.set(ticker, row)
  }

  const out = new Map()
  await Promise.all(Array.from(byTicker.entries()).map(async ([ticker, row]) => {
    const startSec = Math.max(0, Math.floor(row.minSec) - 10 * 60)
    const endSec = Math.ceil(row.maxSec + maxHorizonMinutes * 60 + 30 * 60)
    const docs = await db.collection("ohlcv_bars").find({
      ticker,
      $or: [
        { minute: { $gte: startSec, $lte: endSec } },
        { timestamp: { $gte: startSec, $lte: endSec } },
      ],
    }, {
      projection: { _id: 0, ticker: 1, minute: 1, timestamp: 1, close: 1, source: 1, providerInterval: 1, interval: 1, volume: 1 },
    }).sort({ minute: 1, timestamp: 1 }).toArray().catch(() => [])

    const bars = docs
      .map(bar => ({
        ...bar,
        _sec: timestampSeconds(bar.minute || bar.timestamp),
        _close: Number(bar.close || 0),
      }))
      .filter(bar => bar._sec > 0 && bar._close > 0)
      .sort((a, b) => a._sec - b._sec)
    out.set(ticker, bars)
  }))
  return out
}

function nearestOutcomeBarAtOrAfter(bars = [], targetSec = 0, maxDelaySeconds = 30 * 60) {
  if (!Array.isArray(bars) || !bars.length || !Number.isFinite(targetSec)) return null
  for (const bar of bars) {
    if (Number(bar._sec || 0) >= targetSec) {
      const delay = Number(bar._sec || 0) - targetSec
      return delay <= maxDelaySeconds ? bar : null
    }
  }
  return null
}

async function labelMaturePredictionSignals(db, { limit = 500, relabelLegacy = true } = {}) {
  const nowSec = Math.floor(Date.now() / 1000)
  const oldestDueSec = nowSec - Math.min(...PREDICTION_HORIZONS_MINUTES) * 60
  const requestedLimit = Math.max(1, Math.min(2000, Number(limit || 500)))
  const candidateLimit = Math.max(requestedLimit, Math.min(10000, requestedLimit * 5))
  const needsOhlcLabel = {
    $or: PREDICTION_HORIZONS_MINUTES.map(horizon => ({
      [`labels.return_${horizon}m.label_source`]: { $ne: "mongo_ohlcv_bars" },
    })),
  }
  const docs = await db.collection("prediction_signals").find({
    signal_sec: { $lte: oldestDueSec },
    entry_price: { $gt: 0 },
    ticker: { $exists: true, $nin: ["", null] },
    ...needsOhlcLabel,
  }).sort({ signal_sec: -1 }).limit(candidateLimit).toArray()
  if (!docs.length) return { checked: 0, labeled: 0, source: "mongo_ohlcv_bars", missing_ohlc: 0, relabeled_legacy: 0 }

  const ohlcMap = await loadOutcomeOhlcBars(db, docs, Math.max(...PREDICTION_HORIZONS_MINUTES))
  const updates = []
  let labeled = 0
  let missingOhlc = 0
  let relabeledLegacy = 0

  for (const doc of docs) {
    const ticker = String(doc.ticker || "").toUpperCase()
    const bars = ohlcMap.get(ticker) || []
    const entryPrice = Number(doc.entry_price || 0)
    if (!entryPrice) continue

    const setFields = { updated_at: new Date() }
    let docLabeled = false
    let docMissingOhlc = false
    for (const horizon of PREDICTION_HORIZONS_MINUTES) {
      const key = `return_${horizon}m`
      const existing = doc.labels?.[key]
      const existingIsOhlc = existing?.label_source === "mongo_ohlcv_bars"
      if (existing?.labeled && (existingIsOhlc || !relabelLegacy)) continue
      const due = nowSec - Number(doc.signal_sec || 0) >= horizon * 60
      if (!due) continue

      const targetSec = Number(doc.signal_sec || 0) + horizon * 60
      const bar = nearestOutcomeBarAtOrAfter(bars, targetSec)
      if (!bar) {
        missingOhlc += 1
        docMissingOhlc = true
        setFields[`labels.${key}`] = {
          labeled: false,
          horizon_minutes: horizon,
          entry_price: Number(entryPrice.toFixed(4)),
          target_sec: targetSec,
          labeled_at: new Date(),
          label_source: "missing_ohlc",
          attempted_label_source: "mongo_ohlcv_bars",
          missing_reason: "missing_ohlc_bar_at_or_after_target",
          outcome_label_version: "ohlc_horizon_close_v1",
        }
        continue
      }

      if (existing?.labeled && !existingIsOhlc) {
        setFields[`legacy_quote_labels.${key}`] = existing
        relabeledLegacy += 1
      }

      const labelPrice = Number(bar._close || 0)
      const returnPct = ((labelPrice - entryPrice) / entryPrice) * 100
      const direction = doc.baseline_signal?.direction || "watch"
      setFields[`labels.${key}`] = {
        labeled: true,
        horizon_minutes: horizon,
        return_pct: Number(returnPct.toFixed(3)),
        entry_price: Number(entryPrice.toFixed(4)),
        label_price: Number(labelPrice.toFixed(4)),
        labeled_at: new Date(),
        label_sec: Number(bar._sec),
        target_sec: targetSec,
        label_delay_seconds: Number(bar._sec) - targetSec,
        label_source: "mongo_ohlcv_bars",
        ohlc_source: bar.source || null,
        provider_interval: bar.providerInterval || bar.interval || null,
        label_volume: Number(bar.volume || 0) || null,
        outcome_label_version: "ohlc_horizon_close_v1",
        quote_source: null,
        direction_correct: direction === "up" ? returnPct > 0 : direction === "down" ? returnPct < 0 : null,
      }
      labeled += 1
      docLabeled = true
    }

    if (Object.keys(setFields).length > 1) {
      if (PREDICTION_HORIZONS_MINUTES.every(h => setFields[`labels.return_${h}m`]?.labeled || doc.labels?.[`return_${h}m`]?.labeled)) {
        setFields.label_status = "complete"
      } else if (docLabeled) {
        setFields.label_status = "partially_labeled"
      } else if (docMissingOhlc) {
        setFields.label_status = "missing_ohlc"
      } else {
        setFields.label_status = doc.label_status || "pending"
      }
      updates.push({ updateOne: { filter: { _id: doc._id }, update: { $set: setFields } } })
    }
  }

  if (updates.length) await db.collection("prediction_signals").bulkWrite(updates, { ordered: false })
  return {
    checked: docs.length,
    labeled,
    source: "mongo_ohlcv_bars",
    missing_ohlc: missingOhlc,
    relabeled_legacy: relabeledLegacy,
    relabel_legacy: Boolean(relabelLegacy),
  }
}

async function trainPredictionModel(db, { limit = 2000, minSamples = 20 } = {}) {
  const docs = await db.collection("prediction_signals").find({
    "labels.return_5m.labeled": true,
    "labels.return_5m.return_pct": { $type: "number" },
    features: { $exists: true },
  }).sort({ signal_sec: -1 }).limit(Math.max(50, Math.min(10000, Number(limit || 2000)))).toArray()

  const samples = docs
    .map(doc => ({
      target: Number(doc.labels?.return_5m?.return_pct),
      features: doc.features || {},
      baseline_direction: doc.baseline_signal?.direction || "watch",
    }))
    .filter(row => Number.isFinite(row.target))

  if (samples.length < minSamples) {
    const model = {
      _id: PREDICTION_MODEL_ID,
      model_id: PREDICTION_MODEL_ID,
      status: "insufficient_samples",
      samples: samples.length,
      min_samples: minSamples,
      feature_keys: PREDICTION_FEATURE_KEYS,
      updated_at: new Date(),
      note: "Collect more labeled prediction_signals before training the statistical model.",
    }
    await db.collection("prediction_models").updateOne({ _id: PREDICTION_MODEL_ID }, { $set: model }, { upsert: true })
    return model
  }

  const targetMean = samples.reduce((sum, row) => sum + row.target, 0) / samples.length
  const targetVar = samples.reduce((sum, row) => sum + (row.target - targetMean) ** 2, 0) / Math.max(1, samples.length - 1)
  const targetStd = Math.sqrt(targetVar) || 1
  const featureStats = {}
  const weights = {}

  for (const key of PREDICTION_FEATURE_KEYS) {
    const vals = samples.map(row => Number(row.features?.[key])).map(value => Number.isFinite(value) ? value : 0)
    const mean = vals.reduce((sum, value) => sum + value, 0) / vals.length
    const variance = vals.reduce((sum, value) => sum + (value - mean) ** 2, 0) / Math.max(1, vals.length - 1)
    const std = Math.sqrt(variance) || 1
    const covariance = vals.reduce((sum, value, index) => sum + ((value - mean) / std) * (samples[index].target - targetMean), 0) / Math.max(1, vals.length - 1)
    const weight = clamp(covariance / targetStd, -0.8, 0.8)
    featureStats[key] = { mean: Number(mean.toFixed(5)), std: Number(std.toFixed(5)) }
    weights[key] = Number((weight * 0.35).toFixed(5))
  }

  const predictions = samples.map(row => {
    const signal = applyPredictionModel(row.features, {
      model_id: PREDICTION_MODEL_ID,
      version: Date.now(),
      intercept: targetMean,
      target_std: targetStd,
      weights,
      feature_stats: featureStats,
      feature_keys: PREDICTION_FEATURE_KEYS,
      samples: samples.length,
    })
    return {
      target: row.target,
      predicted: signal?.predicted_return_5m || 0,
      correct: signal?.direction === "up" ? row.target > 0 : signal?.direction === "down" ? row.target < 0 : null,
    }
  })
  const actionable = predictions.filter(row => row.correct != null)
  const mae = predictions.reduce((sum, row) => sum + Math.abs(row.predicted - row.target), 0) / predictions.length
  const directionalAccuracy = actionable.length
    ? actionable.reduce((sum, row) => sum + (row.correct ? 1 : 0), 0) / actionable.length
    : null

  const model = {
    _id: PREDICTION_MODEL_ID,
    model_id: PREDICTION_MODEL_ID,
    status: "trained",
    version: Date.now(),
    samples: samples.length,
    feature_keys: PREDICTION_FEATURE_KEYS,
    feature_stats: featureStats,
    weights,
    intercept: Number(targetMean.toFixed(5)),
    target_std: Number(targetStd.toFixed(5)),
    metrics: {
      mae_5m: Number(mae.toFixed(3)),
      directional_accuracy_5m: directionalAccuracy == null ? null : Number(directionalAccuracy.toFixed(3)),
      actionable_samples: actionable.length,
      avg_target_return_5m: Number(targetMean.toFixed(3)),
    },
    updated_at: new Date(),
  }
  await db.collection("prediction_models").updateOne({ _id: PREDICTION_MODEL_ID }, { $set: model }, { upsert: true })
  return model
}

async function loadTopMomentumTickerSymbols(db, limit = 10) {
  const requestedLimit = Math.max(1, Math.min(50, Number(limit || 10)))
  const movers = await loadPositiveFinvizMoverRows(db, requestedLimit)
  return normalizeTickerList(movers.map(row => row.ticker), requestedLimit, { ensurePrivate: false })
}

async function loadPredictionInterestTickerSymbols(db, limit = 40) {
  const requestedLimit = Math.max(1, Math.min(150, Number(limit || 40)))
  const sinceSec = Math.floor(Date.now() / 1000) - 7 * 86_400
  const tickerSet = new Set()
  const add = (value) => {
    const ticker = String(value || "").toUpperCase().trim()
    if (!ticker || tickerSet.has(ticker) || NON_STOCK_TICKERS.has(ticker) || ticker.includes(".")) return
    if (/^[A-Z][A-Z0-9]{0,5}$/.test(ticker)) tickerSet.add(ticker)
  }

  const [signals, snapshots, screeners] = await Promise.all([
    db.collection("prediction_signals").find({
      signal_sec: { $gte: sinceSec },
      $or: [
        { "entry_signal.entry_ready": true },
        { "threshold_rule_signal.entry_ready": true },
        { "model_signal.direction": "up" },
        { "baseline_signal.direction": "up" },
      ],
    }, {
      projection: { ticker: 1, signal_sec: 1, rank: 1 },
    }).sort({ signal_sec: -1, rank: 1 }).limit(requestedLimit).toArray().catch(() => []),
    db.collection("daily_prediction_snapshots").find({
      $or: [
        { updated_at: { $gte: new Date(Date.now() - 7 * 86_400_000) } },
        { created_at: { $gte: new Date(Date.now() - 7 * 86_400_000) } },
      ],
    }, {
      projection: { realRows: 1, fallbackRows: 1, high_conviction_rows: 1, rows: 1, updated_at: 1 },
    }).sort({ updated_at: -1, created_at: -1 }).limit(6).toArray().catch(() => []),
    db.collection("screeners").find({
      ticker: { $exists: true, $nin: ["", null], $not: /\./ },
      exchange: { $in: Array.from(US_EXCHANGES) },
      price: { $gt: 0 },
      $or: [
        { threshold_setup_status: { $in: ["entry_passed", "active_setup_already_above_threshold", "near_threshold_setup"] } },
        { news_article_count: { $gt: 0 } },
        { message_count: { $gt: 0 } },
      ],
    }, {
      projection: { ticker: 1, change_pct: 1, rel_volume: 1, message_count: 1, news_article_count: 1, threshold_setup_score: 1 },
    }).sort({ threshold_setup_score: -1, change_pct: -1, rel_volume: -1 }).limit(requestedLimit).toArray().catch(() => []),
  ])

  signals.forEach(row => add(row.ticker))
  for (const snapshot of snapshots) {
    for (const key of ["realRows", "high_conviction_rows", "rows", "fallbackRows"]) {
      const rows = Array.isArray(snapshot?.[key]) ? snapshot[key] : []
      rows.forEach(row => add(row.ticker))
    }
  }
  screeners.forEach(row => add(row.ticker))

  return normalizeTickerList([...tickerSet], requestedLimit, { ensurePrivate: false })
}

function withPrivateSocialTickers(tickers = []) {
  return normalizeTickerList([...tickers, ...Array.from(PRIVATE_TRACKED_TICKERS)], Math.max(tickers.length + PRIVATE_TRACKED_TICKERS.size, 1), { ensurePrivate: false })
}

// ── Routes ────────────────────────────────────────────────
app.post("/api/translate", async (req, res) => {
  try {
    const text = String(req.body.text || "").trim().slice(0, 1200)
    const targetLanguage = String(req.body.target_language || req.body.target || "en").toLowerCase()

    if (!text) return res.status(400).json({ ok: false, error: "text is required" })
    if (!SUPPORTED_TRANSLATION_LANGUAGES.has(targetLanguage)) {
      return res.status(400).json({ ok: false, error: "unsupported target language" })
    }

    try {
      const providerTranslation = await translateWithProvider(text, targetLanguage)
      if (providerTranslation) {
        return res.json({
          ok: true,
          translated_text: providerTranslation,
          target_language: targetLanguage,
          provider: "external",
        })
      }
    } catch (err) {
      console.warn("Translation provider failed, using glossary fallback:", err.message)
    }

    return res.json({
      ok: true,
      translated_text: glossaryTranslate(text, targetLanguage),
      target_language: targetLanguage,
      provider: UNSUPPORTED_TRANSLATION_SCRIPT_RE.test(text) ? "glossary_cjk_fallback" : "glossary",
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.use('/api/articles',    articlesRouter)
app.use('/api/screener',    screenerRouter)
app.use('/api/entry-screener', entryScreenerRouter)
app.use('/api/exit-screener', exitScreenerRouter)
app.use('/api/v11-screener', v11ScreenerRouter)

app.get("/api/momentum/trending", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Number(req.query.days || 2)
    const limit = Number(req.query.limit || 30)
    const movers = await loadPositiveFinvizMoverRows(db, Math.max(1, Math.min(100, limit)))
    const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), days)
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow)
    const tickers = movers.map(row => mergeMoverContext(
      row,
      articleMap.get(row.ticker),
      socialMap.get(row.ticker)
    ))

    res.json({ ok: true, tickers, days, order: "positive_price_change", source: "Finviz Elite top movers", social_window_minutes: socialWindow })
  } catch (err) {
    console.error("GET /api/momentum/trending failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/social/targets", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)))
    const tickers = await loadTopMomentumTickerSymbols(db, limit)
    const rows = await loadPositiveFinvizMoverRows(db, limit)
    res.json({
      ok: true,
      tickers,
      rows: rows.slice(0, limit).map(row => ({
        ticker: row.ticker,
        change_pct: row.change_pct,
        price: row.price,
        volume: row.volume,
        exchange: row.exchange,
        quote_source: row.quote_source,
        finviz_rank: row.finviz_rank,
      })),
      source: "Finviz Elite top positive momentum movers, falling back to stored U.S. screener rows only if Finviz has no rows",
      social_refresh_seconds: 60,
    })
  } catch (err) {
    console.error("GET /api/social/targets failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/trade-watch", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Math.max(0, Math.min(7, Number(req.query.days || 2)))
    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 10)))
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const minScore = Math.max(0, Math.min(1, Number(req.query.min_score || 0)))
    const [rawTradeRows, model] = await Promise.all([
      loadEnrichedTradeWatchRows(db, { limit: Math.max(limit, 10), days, socialWindow }),
      loadLatestPredictionModel(db),
    ])
    const tickers = rawTradeRows
      .map(row => ({
        ...row,
        prediction_signal: applyPredictionModel(predictionFeaturesFromMover(row, socialWindow), model),
      }))
      .filter(row => row.trade_watch.trade_watch_score >= minScore)
      .slice(0, limit)

    res.json({
      ok: true,
      count: tickers.length,
      tickers,
      days,
      social_window_minutes: socialWindow,
      source: "Finviz momentum movers ranked by quote action, relative volume, structured/public news, and social evidence",
      methodology: {
        price_action: "positive Finviz Elite mover list, limited to clean NASDAQ/NYSE/AMEX rows",
        evidence: "structured news is weighted higher than public news; social counts and rolling sentiment are support signals",
        caution: "research-only scoring; broker execution is not connected",
      },
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        updated_at: model.updated_at || null,
        metrics: model.metrics || null,
      } : null,
    })
  } catch (err) {
    console.error("GET /api/trade-watch failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/momentum", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const days = Number(req.query.days || 2)
    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 30)))
    const minNews = Math.max(0, Number(req.query.min_volume || req.query.min_news || 0))
    const minRelVolume = Math.max(0, Number(req.query.min_rel_vol || 0))
    const maxPrice = req.query.max_price ? Number(req.query.max_price) : null
    const sentiment = String(req.query.sentiment || "").toLowerCase()
    const order = String(req.query.order || "absolute_momentum").toLowerCase()

    const movers = await loadPositiveFinvizMoverRows(db, Math.max(limit * 4, 100))
    const articleMap = await loadArticleStatsForTickers(db, movers.map(row => row.ticker), days)
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialMap = await loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow)
    let tickers = movers.map(row => mergeMoverContext(
      row,
      articleMap.get(row.ticker),
      socialMap.get(row.ticker)
    ))

    if (minNews > 0) tickers = tickers.filter(row => (row.article_count || row.message_count || 0) >= minNews)
    if (minRelVolume > 0) tickers = tickers.filter(row => (row.rel_volume || 0) >= minRelVolume)
    if (maxPrice != null && Number.isFinite(maxPrice)) {
      tickers = tickers.filter(row => row.price == null || row.price <= maxPrice)
    }
    if (sentiment === "bullish") tickers = tickers.filter(row => (row.sentiment || 0) > 0)
    if (sentiment === "bearish") tickers = tickers.filter(row => (row.sentiment || 0) < 0)

    tickers.sort((a, b) => {
      if (order === "news") {
        const scoreA = (a.article_count || a.message_count || 0) * (1 + Math.abs(a.sentiment || 0))
        const scoreB = (b.article_count || b.message_count || 0) * (1 + Math.abs(b.sentiment || 0))
        return scoreB - scoreA
      }
      const scoreA = Number(a.change_pct || 0)
      const scoreB = Number(b.change_pct || 0)
      if (scoreB !== scoreA) return scoreB - scoreA
      const relA = Number(a.rel_volume || 0)
      const relB = Number(b.rel_volume || 0)
      if (relB !== relA) return relB - relA
      return (b.volume || 0) - (a.volume || 0)
    })

    const visibleTickers = tickers.slice(0, limit)
    const metadata = await momentumMonitorMetadata(db, visibleTickers, {
      totalRows: tickers.length,
      socialWindow,
      cacheMode: redisReady() ? "mongo-compute-redis-cacheable" : "mongo",
    })
    const finvizStatus = {
      status: metadata.status === "healthy" ? "working" : metadata.status,
      last_count: metadata.finvizRows,
      quote_age_seconds: metadata.quoteAgeSeconds,
      screener_age_seconds: metadata.screenerAgeSeconds,
      is_stale: metadata.status === "stale",
      last_fetch_at: metadata.lastFetchAt,
      live_source_count: metadata.liveSourceCount,
      detail: metadata.label,
    }
    const snapshot = await saveMomentumSnapshot(db, visibleTickers, metadata, {
      source: "Finviz Elite top movers",
      cacheMode: metadata.cacheMode,
    }).catch(() => null)

    res.json({
      ok: true,
      tickers: visibleTickers,
      days,
      order,
      source: "Finviz Elite top movers",
      social_window_minutes: socialWindow,
      finviz_status: finvizStatus,
      monitor: metadata,
      snapshot: snapshot ? {
        snapshot_sec: snapshot.snapshot_sec,
        createdAt: snapshot.createdAt,
        rowCount: snapshot.rowCount,
        top_tickers: snapshot.top_tickers,
      } : null,
      finvizRows: metadata.finvizRows,
      visibleTickerCount: metadata.visibleTickerCount,
      quoteAgeSeconds: metadata.quoteAgeSeconds,
      lastFetchAt: metadata.lastFetchAt,
      cacheMode: metadata.cacheMode,
      cacheHit: false,
      cacheStore: redisReady() ? "redis-response-cache-eligible" : "mongo-only",
      warnings: metadata.status === "missing" ? ["No Finviz Elite rows in database"] : metadata.status === "stale" ? ["Finviz screener cache is stale"] : [],
    })
  } catch (err) {
    console.error("GET /api/momentum failed:", err)
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/alerts", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, alerts: [], error: "MongoDB is not connected" })

    const scope = String(req.query.scope || "all").toLowerCase()
    const limit = Math.max(1, Math.min(30, Number(req.query.limit || 8)))
    const socialWindow = Math.max(1, Math.min(4320, Number(req.query.window_minutes || 1440)))

    if (scope && scope !== "all" && scope !== "momentum") {
      return res.json({
        ok: true,
        alerts: [],
        count: 0,
        scope,
        message: "No active alerts under current thresholds.",
      })
    }

    const movers = await loadPositiveFinvizMoverRows(db, Math.max(limit * 8, 100))
    const [articleMap, socialMap] = await Promise.all([
      loadArticleStatsForTickers(db, movers.map(row => row.ticker), Number(req.query.days || 2)),
      loadSocialStatsForTickers(db, movers.map(row => row.ticker), socialWindow),
    ])
    const rows = movers.map(row => mergeMoverContext(
      row,
      articleMap.get(row.ticker),
      socialMap.get(row.ticker)
    ))
    const alerts = buildMomentumAlerts(rows, { limit, windowMinutes: socialWindow })

    res.json({
      ok: true,
      scope: "momentum",
      alerts,
      count: alerts.length,
      window_minutes: socialWindow,
      thresholds: {
        fresh_mover_max_age_minutes: 30,
        high_relative_volume: 10,
        strong_price_move_pct: 20,
        message_density_per_hour: 10,
        sentiment_abs: 0.25,
      },
      message: alerts.length ? "Momentum alerts generated from current real mover rows." : "No active alerts under current thresholds.",
      source: "current_finviz_momentum_rows",
    })
  } catch (err) {
    console.error("GET /api/alerts failed:", err)
    res.status(500).json({ ok: false, alerts: [], error: String(err.message || err) })
  }
})

app.get("/api/momentum/snapshots", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, snapshots: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(50, Number(req.query.limit || 6)))
    let snapshots = await loadMomentumSnapshots(db, limit)

    if (!snapshots.length) {
      const rows = await loadPositiveFinvizMoverRows(db, 30)
      if (rows.length) {
        const articleMap = await loadArticleStatsForTickers(db, rows.map(row => row.ticker), 2)
        const socialMap = await loadSocialStatsForTickers(db, rows.map(row => row.ticker), 1440)
        const tickers = rows.map(row => mergeMoverContext(row, articleMap.get(row.ticker), socialMap.get(row.ticker)))
        const metadata = await momentumMonitorMetadata(db, tickers, {
          totalRows: tickers.length,
          socialWindow: 1440,
          cacheMode: redisReady() ? "mongo-compute-redis-cacheable" : "mongo",
        })
        await saveMomentumSnapshot(db, tickers, metadata, {
          source: "Finviz Elite top movers",
          cacheMode: metadata.cacheMode,
        })
        snapshots = await loadMomentumSnapshots(db, limit)
      }
    }

    res.json({
      ok: true,
      snapshots,
      count: snapshots.length,
      retention_days: Number(process.env.MOMENTUM_SNAPSHOT_RETENTION_DAYS || 31),
      message: snapshots.length ? "Momentum snapshots loaded from MongoDB." : "Snapshot not created yet.",
      source: "momentum_snapshots",
    })
  } catch (err) {
    console.error("GET /api/momentum/snapshots failed:", err)
    res.status(500).json({ ok: false, snapshots: [], error: String(err.message || err) })
  }
})

app.get("/api/momentum/:ticker/details", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, headlines: [], posts: [], error: "MongoDB is not connected" })

    const ticker = String(req.params.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const match = {
      ...recentArticleMatch(Number(req.query.days || 2)),
      ticker: { $regex: `(^|,\\s*)${escapeRegExp(ticker)}(\\s*,|$)`, $options: "i" },
    }

    const articles = await db.collection("articles").find(
      match,
      { projection: { title: 1, source: 1, sentiment: 1, publish_date: 1, fetched_date: 1, url: 1, category: 1 } }
    ).sort({ publish_date: -1, fetched_date: -1 }).limit(12).toArray()

    const headlines = articles.map(article => ({
      title: article.title || "Untitled headline",
      source: article.source || "News",
      sentiment: article.sentiment || "neutral",
      time: timeLabel(article.publish_date || article.fetched_date),
      catalyst: article.category || undefined,
      url: article.url,
    }))

    const socialRows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _ticker_candidates: ticker } },
      { $sort: { _event_sec: -1 } },
      { $limit: 12 },
      {
        $project: {
          _id: 0,
          platform: "$_norm_platform",
          author: 1,
          content: { $ifNull: ["$text", { $ifNull: ["$content", "$title"] }] },
          sentiment: 1,
          sentiment_score: 1,
          url: 1,
          fetched_at: "$_event_sec",
        },
      },
    ]).toArray()

    const posts = socialRows.map(post => ({
      platform: post.platform || "Social",
      author: post.author || "",
      content: post.content || "",
      sentiment: typeof post.sentiment_score === "number"
        ? post.sentiment_score
        : /bull|positive/i.test(String(post.sentiment || "")) ? 1
        : /bear|negative/i.test(String(post.sentiment || "")) ? -1
        : 0,
      url: post.url,
      time: timeLabel(post.fetched_at),
    }))

    res.json({ ok: true, ticker, headlines, posts })
  } catch (err) {
    console.error("GET /api/momentum/:ticker/details failed:", err)
    res.status(500).json({ ok: false, headlines: [], posts: [], error: String(err.message || err) })
  }
})

app.get("/api/prices/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })

    const ticker = String(req.params.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const doc = await db.collection("screeners").findOne({ ticker })
    const row = normalizeScreenerDoc(doc || { ticker })
    res.json({
      ok: true,
      ticker,
      price: row.price,
      change_pct: row.change_pct,
      volume: row.volume,
      rel_volume: row.rel_volume,
      previous_close: row.previous_close,
      quote_source: row.quote_source,
      quote_time: row.quote_time,
      quote_status: row.quote_status,
      updated_at: doc?.quote_updated_at || doc?.updated_at || doc?.updatedAt || null,
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// SOCIAL_ROLLING_API_V2_START
// Rolling social feed using existing Mongoose connection.
// Supports numeric Unix-second timestamps, JS Date timestamps, and fallback fields.
function socialTimeStages() {
  return [
    {
      $addFields: {
	        _time_raw: {
	          $ifNull: [
	            "$published_at",
	            { $ifNull: [
	              "$timestamp",
	              { $ifNull: [
	                "$created_at_dt",
	                { $ifNull: [
	                  "$created_at",
	                  { $ifNull: [
	                    "$publish_date",
	                    { $ifNull: ["$detected_at", "$fetched_at"] }
	                  ] }
	                ] }
	              ] }
	            ] }
	          ]
        }
      }
    },
    {
      $addFields: {
        _event_sec: {
          $switch: {
            branches: [
              {
                case: { $eq: [{ $type: "$_time_raw" }, "date"] },
                then: { $floor: { $divide: [{ $toLong: "$_time_raw" }, 1000] } }
              },
              {
                case: { $in: [{ $type: "$_time_raw" }, ["int", "long", "double", "decimal"] ] },
                then: { $toLong: "$_time_raw" }
              },
              {
                case: { $eq: [{ $type: "$_time_raw" }, "string"] },
                then: {
                  $floor: {
                    $divide: [
                      { $toLong: { $dateFromString: { dateString: "$_time_raw", onError: new Date(0) } } },
                      1000
                    ]
                  }
                }
              }
            ],
            default: 0
          }
        }
      }
    },
    {
      $addFields: {
        _norm_platform: {
          $switch: {
            branches: [
              {
                case: {
                  $regexMatch: {
                    input: { $toLower: { $ifNull: ["$platform", ""] } },
                    regex: "stocktwits"
                  }
                },
                then: "StockTwits"
              },
              {
                case: {
                  $regexMatch: {
                    input: { $toLower: { $ifNull: ["$platform", ""] } },
                    regex: "bluesky|bsky"
                  }
                },
                then: "Bluesky"
              },
              {
                case: {
                  $or: [
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$platform", ""] } },
                        regex: "reddit"
                      }
                    },
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$collector", ""] } },
                        regex: "reddit"
                      }
                    }
                  ]
                },
                then: "Reddit"
              },
              {
                case: {
                  $or: [
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$platform", ""] } },
                        regex: "twitter|x"
                      }
                    },
                    {
                      $regexMatch: {
                        input: { $toLower: { $ifNull: ["$collector", ""] } },
                        regex: "twitter|x_"
                      }
                    }
                  ]
                },
	                then: "Grok/X"
              }
            ],
            default: { $ifNull: ["$platform", "Unknown"] }
          }
        }
      }
    },
    ...socialTickerCandidateStages(),
  ]
}

function socialTickerCandidateStages() {
  const stringSplit = (field) => ({
    $cond: [
      { $eq: [{ $type: field }, "string"] },
      { $split: [field, ","] },
      [],
    ],
  })
  const arrayOrStringSplit = (field) => ({
    $cond: [
      { $isArray: field },
      field,
      stringSplit(field),
    ],
  })

  return [
    {
      $addFields: {
        _ticker_primary_values_raw: {
          $concatArrays: [
            stringSplit("$ticker"),
            stringSplit("$symbol"),
            stringSplit("$cashtag"),
            arrayOrStringSplit("$tickers_mentioned"),
          ],
        },
        _ticker_text_cashtags: {
          $map: {
            input: {
              $regexFindAll: {
                input: {
                  $concat: [
                    { $toString: { $ifNull: ["$text", ""] } },
                    " ",
                    { $toString: { $ifNull: ["$content", ""] } },
                    " ",
                    { $toString: { $ifNull: ["$title", ""] } },
                  ],
                },
                regex: /\$[A-Za-z][A-Za-z0-9.-]{0,5}\b/,
              },
            },
            as: "tag",
            in: "$$tag.match",
          },
        },
      },
    },
    {
      $addFields: {
        _ticker_values_raw: {
          $cond: [
            {
              $gt: [
                {
                  $size: {
                    $filter: {
                      input: { $ifNull: ["$_ticker_primary_values_raw", []] },
                      as: "raw",
                      cond: { $ne: [{ $trim: { input: { $toString: "$$raw" } } }, ""] },
                    },
                  },
                },
                0,
              ],
            },
            "$_ticker_primary_values_raw",
            "$_ticker_text_cashtags",
          ],
        },
      },
    },
    {
      $addFields: {
        _ticker_candidates: {
          $filter: {
            input: {
              $map: {
                input: "$_ticker_values_raw",
                as: "raw",
                in: {
                  $trim: {
                    input: {
                      $replaceAll: {
                        input: { $toUpper: { $toString: "$$raw" } },
                        find: { $literal: "$" },
                        replacement: "",
                      },
                    },
                    chars: " ,;#",
                  },
                },
              },
            },
            as: "candidate",
            cond: {
              $regexMatch: {
                input: "$$candidate",
                regex: "^[A-Z][A-Z0-9.-]{0,5}$",
              },
            },
          },
        },
      },
    }
  ]
}

function marketSessionForSec(sec) {
  const date = new Date(Number(sec || 0) * 1000)
  if (!Number.isFinite(date.getTime())) return "unknown"
  const ny = new Date(date.toLocaleString("en-US", { timeZone: "America/New_York" }))
  const day = ny.getDay()
  const minutes = ny.getHours() * 60 + ny.getMinutes()
  if (day < 1 || day > 5) return "closed"
  if (minutes >= 4 * 60 && minutes < 9 * 60 + 30) return "pre"
  if (minutes >= 9 * 60 + 30 && minutes < 16 * 60) return "regular"
  if (minutes >= 16 * 60 && minutes < 20 * 60) return "after"
  return "closed"
}

function addSessionScaledSocialFields(rows, bucketMinutes) {
  const maxima = new Map()
  for (const row of rows) {
    const session = row.session || "unknown"
    const current = maxima.get(session) || { count: 0, density: 0 }
    current.count = Math.max(current.count, Number(row.message_count || 0))
    current.density = Math.max(current.density, Number(row.message_density || 0))
    maxima.set(session, current)
  }

  return rows.map(row => {
    const max = maxima.get(row.session || "unknown") || { count: 0, density: 0 }
    const count = Number(row.message_count || 0)
    const density = Number(row.message_density || 0)
    const sentiment = Number(row.sentiment || 0)
    return {
      ...row,
      bucket_minutes: bucketMinutes,
      message_count_scaled: max.count ? Number((count / max.count).toFixed(3)) : 0,
      message_density_scaled: max.density ? Number((density / max.density).toFixed(3)) : 0,
      sentiment_scaled: Number(((sentiment + 1) / 2).toFixed(3)),
    }
  })
}

app.get("/api/finviz/movers", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, tickers: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(100, Number(req.query.limit || 30)))
    const selectedWindowMinutes = normalizeRollingWindowMinutes(req.query.window_minutes, 1440)
    const movers = await loadPositiveFinvizMoverRows(db, limit)
    const [articleMap, socialMap, watcherMap, sourceStatus] = await Promise.all([
      loadArticleStatsForTickers(db, movers.map(row => row.ticker), Number(req.query.days || 2)),
      loadSocialStatsForTickers(db, movers.map(row => row.ticker), selectedWindowMinutes),
      loadLatestMoverWatcherSnapshots(db, movers.map(row => row.ticker), selectedWindowMinutes),
      db.collection('source_status').findOne(
        { source: 'Finviz Elite Screener' },
        { projection: { _id: 0, source: 1, status: 1, detail: 1, count: 1, last_checked_at: 1, last_success_at: 1, source_type: 1 } },
      ).catch(() => null),
    ])
    const tickers = movers.map(row => {
      const merged = mergeMoverContext(row, articleMap.get(row.ticker), socialMap.get(row.ticker))
      const watcher = watcherMap.get(String(row.ticker || '').toUpperCase())
      if (!watcher || !Number.isFinite(Number(watcher.watcher_count))) {
        return {
          ...merged,
          watcher_snapshot_age_seconds: null,
          watcher_snapshot_source: null,
          watcher_feature: {
            status: 'missing_or_expired',
            source: null,
            snapshot_sec: null,
            age_seconds: null,
            missing_behavior: 'not used as a score or catalyst',
          },
        }
      }
      const snapshotSec = Number(watcher.fetched_sec || 0)
      return {
        ...merged,
        stocktwits_watcher_count: Number(watcher.watcher_count),
        watcher_snapshot_age_seconds: Math.max(0, Math.floor(Date.now() / 1000) - snapshotSec),
        watcher_snapshot_source: watcher.source || 'stocktwits_watcher_snapshots',
        watcher_feature: watcher.watcher_feature || {
          status: 'fresh',
          source: watcher.source || 'stocktwits_watcher_snapshots',
          snapshot_sec: snapshotSec,
          age_seconds: Math.max(0, Math.floor(Date.now() / 1000) - snapshotSec),
          definition: 'current StockTwits watchlist count; growth unavailable',
          score_points: 0,
          max_rank_points: 2,
          missing_behavior: 'zero rank points; never creates or rejects a candidate',
        },
      }
    })

    res.json({
      ok: true,
      source: "Finviz Elite top movers",
      finviz_status: sourceStatus || null,
      tickers,
      count: tickers.length,
      social_window_minutes: selectedWindowMinutes,
      watcher_growth_window_minutes: selectedWindowMinutes,
    })
  } catch (err) {
    res.status(500).json({ ok: false, tickers: [], error: String(err.message || err) })
  }
})

app.get("/api/social/rolling", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({ ok: false, error: "MongoDB is not connected", rows: [] })
    }

    const windowMinutes = Math.max(1, Math.min(1440, Number(req.query.window_minutes || 5)))
    const limit = Math.max(1, Math.min(1000, Number(req.query.limit || 500)))
    const platform = String(req.query.platform || "all").toLowerCase()
    const ticker = normalizeTickerList([req.query.ticker || req.query.symbol], 1, { ensurePrivate: false })[0] || ""
    const ranked = ["1", "true", "yes"].includes(String(req.query.ranked || "").toLowerCase())
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60

    const pipeline = [
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      {
        $match: {
          _norm_platform: { $ne: "Unstructured" },
          _ticker_candidates: { $ne: [] },
        },
      },
    ]

    if (platform !== "all") {
      const platformMap = {
        reddit: "Reddit",
        bluesky: "Bluesky",
        bsky: "Bluesky",
	        twitter: "Grok/X",
	        x: "Grok/X",
	        grok: "Grok/X",
        stocktwits: "StockTwits",
      }
      pipeline.push({ $match: { _norm_platform: platformMap[platform] || platform } })
    }

    if (ticker) {
      pipeline.push({
        $match: { _ticker_candidates: ticker },
      })
    }

    pipeline.push(
      {
        $addFields: {
          _display_sentiment_score: {
            $switch: {
              branches: [
                { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment_score" } },
                { case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"] ] }, then: { $toDouble: "$sentiment" } },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
              ],
              default: 0,
            },
          },
          _platform_rank: {
            $switch: {
              branches: [
                { case: { $eq: ["$_norm_platform", "StockTwits"] }, then: 4 },
	                { case: { $eq: ["$_norm_platform", "Grok/X"] }, then: 3 },
                { case: { $eq: ["$_norm_platform", "Reddit"] }, then: 2 },
                { case: { $eq: ["$_norm_platform", "Bluesky"] }, then: 1 },
              ],
              default: 0,
            },
          },
          _sentiment_abs: { $abs: "$_display_sentiment_score" },
        },
      },
      { $sort: ranked ? { _sentiment_abs: -1, _platform_rank: -1, _event_sec: -1 } : { _event_sec: -1 } },
      { $limit: limit },
      {
        $project: {
          _id: 1,
          platform: "$_norm_platform",
          source: 1,
          collector: 1,
          ticker: { $ifNull: ["$ticker", { $arrayElemAt: ["$_ticker_candidates", 0] }] },
          symbol: { $ifNull: ["$symbol", { $arrayElemAt: ["$_ticker_candidates", 0] }] },
          title: 1,
          text: 1,
          content: 1,
          url: 1,
          author: 1,
          sentiment: 1,
          sentiment_score: "$_display_sentiment_score",
          raw_sentiment_score: "$sentiment_score",
          source_sentiment: 1,
          source_sentiment_score: 1,
          sentiment_validation: 1,
          cashtag: 1,
          finance_keywords: 1,
          keywords: 1,
          gossip_keywords: 1,
          gossip_score: 1,
          fetched_at: "$_event_sec",
          detected_at: 1,
          created_at: 1,
          timestamp: 1
        }
      }
    )

    const platformStatusPipeline = [
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec }, _norm_platform: { $ne: "Unstructured" } } },
      {
        $group: {
          _id: "$_norm_platform",
          total: { $sum: 1 },
          ticker_matched: {
            $sum: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ["$_ticker_candidates", []] } }, 0] },
                1,
                0,
              ],
            },
          },
          latest_sec: { $max: "$_event_sec" },
        },
      },
      { $sort: { total: -1 } },
    ]

    const [rows, platformStatusRows] = await Promise.all([
      db.collection("socials").aggregate(pipeline).toArray(),
      db.collection("socials").aggregate(platformStatusPipeline).toArray(),
    ])
    if (ranked) {
	      const platformRank = { StockTwits: 4, "Grok/X": 3, Reddit: 2, Bluesky: 1 }
      rows.sort((a, b) => {
        const sentimentDiff = Math.abs(Number(b.sentiment_score || 0)) - Math.abs(Number(a.sentiment_score || 0))
        if (sentimentDiff) return sentimentDiff
        const platformDiff = (platformRank[b.platform] || 0) - (platformRank[a.platform] || 0)
        if (platformDiff) return platformDiff
        return Number(b.fetched_at || b.timestamp || 0) - Number(a.fetched_at || a.timestamp || 0)
      })
    }

    return res.json({
      ok: true,
      rows,
      count: rows.length,
      platform_status: platformStatusRows.map(row => ({
        platform: row._id || "Unknown",
        total: Number(row.total || 0),
        ticker_matched: Number(row.ticker_matched || 0),
        latest_sec: Number(row.latest_sec || 0) || null,
        status: Number(row.ticker_matched || 0) > 0
          ? "working"
          : Number(row.total || 0) > 0 ? "unmatched" : "empty",
      })),
      window_minutes: windowMinutes,
      platform,
      ticker,
      since_sec: sinceSec,
      now_sec: Math.floor(Date.now() / 1000),
    })
  } catch (err) {
    console.error("GET /api/social/rolling failed:", err)
    return res.status(500).json({ ok: false, error: String(err?.message || err), rows: [] })
  }
})

// Grok/X social analysis. Uses xAI Grok when configured, otherwise a local
// evidence summary so the Social tab remains useful without API credentials.
const GROK_API_KEY = process.env.GROK_API_KEY || ""
const GROK_BASE_URL = process.env.GROK_BASE_URL || "https://api.x.ai/v1"
const GROK_MODEL = process.env.GROK_MODEL || "grok-3-mini"

function socialSentimentLabel(score) {
  const value = Number(score || 0)
  if (value >= 0.2) return "bullish"
  if (value <= -0.2) return "bearish"
  return "neutral/mixed"
}

async function buildLocalGrokSocialAnalysis(db, ticker, context = "") {
  const cleanTicker = String(ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
  if (!db || !cleanTicker) {
    return { text: "No ticker context is available yet. Run a refresh cycle, then try again.", model: "local-social-rules", engine: "local" }
  }

  const sinceSec = Math.floor(Date.now() / 1000) - 24 * 60 * 60
  const tickerRegex = new RegExp(`(^|[,\\s$])${cleanTicker}($|[,\\s])`, "i")
  const [screener, socialRows, articles] = await Promise.all([
    db.collection("screeners").findOne({ ticker: cleanTicker }),
    db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec }, _ticker_candidates: cleanTicker } },
      { $sort: { _event_sec: -1 } },
      { $limit: 80 },
      {
        $project: {
          platform: "$_norm_platform",
          text: { $ifNull: ["$text", { $ifNull: ["$title", "$content"] }] },
          sentiment: 1,
          sentiment_score: 1,
          fetched_at: "$_event_sec",
          gossip_keywords: 1,
          finance_keywords: 1,
        }
      }
    ]).toArray(),
    db.collection("articles")
      .find({ $or: [{ ticker: tickerRegex }, { tickers: cleanTicker }, { symbols: cleanTicker }] })
      .sort({ publish_date: -1, fetched_at: -1 })
      .limit(5)
      .toArray(),
  ])

  const platformCounts = {}
  const sentimentScores = []
  const keywordCounts = new Map()
  for (const row of socialRows) {
    const platform = row.platform || "Social"
    platformCounts[platform] = (platformCounts[platform] || 0) + 1
    const score = Number.isFinite(Number(row.sentiment_score))
      ? Number(row.sentiment_score)
      : /bull|positive/i.test(String(row.sentiment || "")) ? 1 : /bear|negative/i.test(String(row.sentiment || "")) ? -1 : 0
    sentimentScores.push(score)
    for (const word of [...(row.gossip_keywords || []), ...(row.finance_keywords || [])]) {
      const key = String(word || "").trim().toLowerCase()
      if (key) keywordCounts.set(key, (keywordCounts.get(key) || 0) + 1)
    }
  }

  const avgSentiment = sentimentScores.length
    ? sentimentScores.reduce((sum, value) => sum + value, 0) / sentimentScores.length
    : 0
  const platformText = Object.entries(platformCounts)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .map(([platform, count]) => `${platform} ${count}`)
    .join(", ")
  const topKeywords = Array.from(keywordCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([word]) => word)
  const change = Number(screener?.change_pct ?? screener?.change ?? NaN)
  const relVol = Number(screener?.relative_volume ?? screener?.rel_volume ?? screener?.relVol ?? NaN)
  const lines = []

  lines.push(`${cleanTicker}: ${socialRows.length} ticker-matched social posts found in the last 24h${platformText ? ` (${platformText})` : ""}.`)
  lines.push(`Tone is ${socialSentimentLabel(avgSentiment)} with average sentiment ${avgSentiment.toFixed(2)}.`)
  if (Number.isFinite(change)) lines.push(`Current screener move is ${change >= 0 ? "+" : ""}${change.toFixed(2)}%.`)
  if (Number.isFinite(relVol)) lines.push(`Relative volume is ${relVol.toFixed(2)}x.`)
  if (topKeywords.length) lines.push(`Repeated social themes: ${topKeywords.join(", ")}.`)
  if (articles.length) lines.push(`Latest matched catalyst/headline: "${String(articles[0].title || articles[0].headline || "").slice(0, 140)}".`)
  if (!socialRows.length && !articles.length) lines.push("No strong stored social or article support is matched yet, so confidence should stay low.")
  if (context) lines.push(`Context requested: ${context}.`)

  return { text: lines.join(" "), model: "local-social-rules", engine: "local" }
}

async function callGrokSocialAnalysis(ticker, context, prompt) {
  const cleanTicker = String(ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
  const systemMsg = `You are a concise financial social-sentiment analyst for ${cleanTicker}. Use only the provided context. Be factual, explain bull/bear tone, and do not invent posts or catalysts.`
  const userMsg = prompt || `Analyze current social sentiment for ${cleanTicker}. Context: ${context || "No extra context."}`
  const resp = await fetch(`${GROK_BASE_URL}/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": `Bearer ${GROK_API_KEY}` },
    body: JSON.stringify({
      model: GROK_MODEL,
      messages: [
        { role: "system", content: systemMsg },
        { role: "user", content: userMsg },
      ],
      max_tokens: 220,
      temperature: 0.25,
    }),
  })
  if (!resp.ok) throw new Error(`Grok ${resp.status}: ${(await resp.text()).slice(0, 180)}`)
  const data = await resp.json()
  return { text: data.choices?.[0]?.message?.content || "", model: GROK_MODEL, engine: "grok" }
}

app.post("/api/grok/analyze", async (req, res) => {
  const started = Date.now()
  const ticker = normalizeTickerList([req.body?.ticker || req.query.ticker], 1, { ensurePrivate: false })[0] || ""
  const context = String(req.body?.context || req.query.context || "")
  const prompt = String(req.body?.prompt || "")
  if (!ticker) return res.status(400).json({ ok: false, error: "ticker is required", ms: Date.now() - started })

  try {
    if (GROK_API_KEY) {
      const result = await callGrokSocialAnalysis(ticker, context, prompt)
      return res.json({ ok: true, ticker, analysis: result.text, model: result.model, engine: result.engine, ms: Date.now() - started })
    }
    const result = await buildLocalGrokSocialAnalysis(mongoose.connection.db, ticker, context)
    return res.json({ ok: true, ticker, analysis: result.text, model: result.model, engine: result.engine, ms: Date.now() - started })
  } catch (err) {
    try {
      const result = await buildLocalGrokSocialAnalysis(mongoose.connection.db, ticker, context)
      return res.json({ ok: true, ticker, analysis: result.text, model: `${result.model} (api-fallback)`, engine: result.engine, ms: Date.now() - started })
    } catch {
      return res.status(500).json({ ok: false, ticker, error: String(err?.message || err), ms: Date.now() - started })
    }
  }
})

app.get("/api/grok/status", (_req, res) => {
  res.json({
    ok: true,
    configured: Boolean(GROK_API_KEY),
    engine: GROK_API_KEY ? "grok" : "local",
    model: GROK_API_KEY ? GROK_MODEL : "local-social-rules",
  })
})

app.get("/api/social/series/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, ticker: "", rows: [], error: "MongoDB is not connected" })

    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ ok: false, ticker: "", rows: [], error: "ticker is required" })

    const windowMinutes = Math.max(5, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const bucketMinutes = Math.max(1, Math.min(60, Number(req.query.bucket_minutes || 5)))
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60
    const bucketSec = bucketMinutes * 60

    const rows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      { $match: { _ticker_candidates: ticker } },
      {
      $addFields: {
        _bucket_sec: {
          $multiply: [
            { $floor: { $divide: ["$_event_sec", bucketSec] } },
            bucketSec,
          ],
        },
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $group: {
        _id: "$_bucket_sec",
        message_count: { $sum: 1 },
          sentiment: { $avg: "$_score" },
          bullish: { $sum: { $cond: [{ $gt: ["$_score", 0.1] }, 1, 0] } },
          bearish: { $sum: { $cond: [{ $lt: ["$_score", -0.1] }, 1, 0] } },
          platforms: { $addToSet: "$_norm_platform" },
        },
      },
      { $sort: { _id: 1 } },
    ]).toArray()

    const normalized = addSessionScaledSocialFields(rows.map(row => {
      const count = Number(row.message_count || 0)
      return {
        time: new Date(Number(row._id || 0) * 1000).toISOString(),
        bucket_sec: Number(row._id || 0),
        session: marketSessionForSec(row._id),
        message_count: count,
        message_density: Number((count / bucketMinutes).toFixed(3)),
        sentiment: count ? Number(Number(row.sentiment || 0).toFixed(3)) : 0,
        bullish: Number(row.bullish || 0),
        bearish: Number(row.bearish || 0),
        platforms: row.platforms || [],
      }
    }), bucketMinutes)

    res.json({
      ok: true,
      ticker,
      rows: normalized,
      window_minutes: windowMinutes,
      bucket_minutes: bucketMinutes,
      scaling: "per_ticker_per_market_session",
    })
  } catch (err) {
    console.error("GET /api/social/series/:ticker failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

function yahooRangeFor(range, interval) {
  const r = String(range || "3mo").toLowerCase()
  const i = String(interval || "1d").toLowerCase()
  if (i === "1m") return "1d"
  if (["5m", "15m", "30m"].includes(i)) return r === "1d" ? "1d" : "5d"
  if (i === "1h") return ["1d", "5d"].includes(r) ? "5d" : "1mo"
  if (["1mo", "3mo", "6mo", "1y", "2y", "5y"].includes(r)) return r
  return "3mo"
}

function yahooIntervalFor(interval) {
  const i = String(interval || "1d").toLowerCase()
  if (["1m", "5m", "15m", "30m", "1h", "1d", "1wk"].includes(i)) return i
  return "1d"
}

async function fetchYahooCandles(ticker, range, interval, opts = {}) {
  // raw:true passes the given range straight through (used by the multi-timeframe
  // tf= selector, which needs longer history than the default range caps allow).
  const yahooRange = opts.raw ? String(range || "1mo") : yahooRangeFor(range, interval)
  const yahooInterval = yahooIntervalFor(interval)
  const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}`)
  url.searchParams.set("range", yahooRange)
  url.searchParams.set("interval", yahooInterval)
  url.searchParams.set("includePrePost", "true")
  url.searchParams.set("events", "history")

  const resp = await fetch(url, {
    headers: {
      "User-Agent": "FeedFlashStockDashboard/0.1",
      "Accept": "application/json",
    },
  })
  if (!resp.ok) throw new Error(`chart provider HTTP ${resp.status}`)
  const payload = await resp.json()
  const result = payload?.chart?.result?.[0]
  const timestamps = result?.timestamp || []
  const quote = result?.indicators?.quote?.[0] || {}
  const candles = []

  for (let i = 0; i < timestamps.length; i += 1) {
    const open = Number(quote.open?.[i])
    const high = Number(quote.high?.[i])
    const low = Number(quote.low?.[i])
    const close = Number(quote.close?.[i])
    if (![open, high, low, close].every(Number.isFinite)) continue
    if (open <= 0 || high <= 0 || low <= 0 || close <= 0) continue
    if (high < Math.max(open, close, low) || low > Math.min(open, close, high)) continue
    candles.push({
      time: Number(timestamps[i]),
      open: Number(open.toFixed(4)),
      high: Number(high.toFixed(4)),
      low: Number(low.toFixed(4)),
      close: Number(close.toFixed(4)),
      volume: Number.isFinite(Number(quote.volume?.[i])) ? Number(quote.volume[i]) : 0,
    })
  }
  return { candles, provider_range: yahooRange, provider_interval: yahooInterval }
}

async function hydrateCurrentOhlcvBars(db, tickers = [], { limit = 40, concurrency = 6 } = {}) {
  const symbols = normalizeTickerList(tickers, Math.max(1, Math.min(100, Number(limit || 40))), { ensurePrivate: false })
  if (!db || !symbols.length) return { attempted_tickers: 0, fetched_tickers: 0, fetched_bars: 0, persisted_bars: 0, errors: [] }

  const errors = []
  const allCandles = []
  let cursor = 0
  let fetchedTickers = 0
  const workers = Array.from({ length: Math.max(1, Math.min(10, Number(concurrency || 6))) }, async () => {
    while (cursor < symbols.length) {
      const ticker = symbols[cursor++]
      try {
        const result = await fetchYahooCandles(ticker, '1d', '1m', { raw: true })
        const rows = Array.isArray(result.candles) ? result.candles : []
        if (rows.length) fetchedTickers += 1
        for (const candle of rows) {
          const minute = Math.floor(Number(candle.time || 0) / 60) * 60
          if (!minute || ![candle.open, candle.high, candle.low, candle.close].every(value => Number.isFinite(Number(value)) && Number(value) > 0)) continue
          allCandles.push({ ticker, minute, ...candle })
        }
      } catch (err) {
        if (errors.length < 12) errors.push({ ticker, error: String(err.message || err) })
      }
    }
  })
  await Promise.all(workers)

  let persistedBars = 0
  if (allCandles.length) {
    const now = new Date()
    const operations = allCandles.map(candle => ({
      updateOne: {
        filter: { source: 'yahoo_chart_ohlcv', ticker: candle.ticker, minute: candle.minute },
        update: {
          $set: {
            ticker: candle.ticker,
            minute: candle.minute,
            time: new Date(candle.minute * 1000),
            open: Number(candle.open),
            high: Number(candle.high),
            low: Number(candle.low),
            close: Number(candle.close),
            price: Number(candle.close),
            volume: Math.max(0, Number(candle.volume || 0)),
            source: 'yahoo_chart_ohlcv',
            providerRange: '1d',
            providerInterval: '1m',
            providerIntervalSec: 60,
            updatedAt: now,
          },
          $setOnInsert: { createdAt: now },
        },
        upsert: true,
      },
    }))
    const result = await db.collection('ohlcv_bars').bulkWrite(operations, { ordered: false })
    persistedBars = Number(result.upsertedCount || 0) + Number(result.modifiedCount || 0)
  }

  return {
    attempted_tickers: symbols.length,
    fetched_tickers: fetchedTickers,
    fetched_bars: allCandles.length,
    persisted_bars: persistedBars,
    errors,
  }
}

app.get("/api/market-quote/:ticker", async (req, res) => {
  try {
    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ ok: false, error: "ticker is required" })
    const result = await fetchYahooCandles(ticker, "5d", "1d", { raw: true })
    const candles = result.candles || []
    const last = candles[candles.length - 1] || null
    const previous = candles[candles.length - 2] || null
    const price = Number(last?.close)
    const previousClose = Number(previous?.close)
    const changePct = Number.isFinite(price) && Number.isFinite(previousClose) && previousClose > 0
      ? Number((((price - previousClose) / previousClose) * 100).toFixed(2))
      : null
    res.set("Cache-Control", "no-store")
    res.json({
      ok: true,
      ticker,
      price: Number.isFinite(price) ? price : null,
      previousClose: Number.isFinite(previousClose) ? previousClose : null,
      change_pct: changePct,
      volume: Number.isFinite(Number(last?.volume)) ? Number(last.volume) : null,
      quote_time: last?.time || null,
      source: "market_chart_provider",
      provider_range: result.provider_range,
      provider_interval: result.provider_interval,
    })
  } catch (err) {
    res.status(502).json({ ok: false, error: String(err.message || err), source: "market_chart_provider" })
  }
})

function sma(values, period) {
  return values.map((_, index) => {
    if (index < period - 1) return null
    const slice = values.slice(index - period + 1, index + 1)
    return slice.reduce((sum, value) => sum + value, 0) / period
  })
}

function bollinger(candles, period = 20, multiplier = 2) {
  const closes = candles.map(c => Number(c.close))
  const middle = sma(closes, period)
  const upper = []
  const lower = []
  for (let i = 0; i < candles.length; i += 1) {
    if (middle[i] == null) continue
    const slice = closes.slice(i - period + 1, i + 1)
    const variance = slice.reduce((sum, value) => sum + Math.pow(value - middle[i], 2), 0) / period
    const std = Math.sqrt(variance)
    upper.push({ time: candles[i].time, value: Number((middle[i] + multiplier * std).toFixed(4)) })
    lower.push({ time: candles[i].time, value: Number((middle[i] - multiplier * std).toFixed(4)) })
  }
  return { upper, lower }
}

function rsi(candles, period = 14) {
  const closes = candles.map(c => Number(c.close))
  const rows = []
  for (let i = period; i < closes.length; i += 1) {
    let gains = 0
    let losses = 0
    for (let j = i - period + 1; j <= i; j += 1) {
      const diff = closes[j] - closes[j - 1]
      if (diff >= 0) gains += diff
      else losses += Math.abs(diff)
    }
    const rs = losses ? gains / losses : 100
    const value = 100 - (100 / (1 + rs))
    rows.push({ time: candles[i].time, value: Number(value.toFixed(2)) })
  }
  return rows
}

function ema(values, period) {
  const k = 2 / (period + 1)
  let current = values[0]
  return values.map((value, index) => {
    current = index === 0 ? value : value * k + current * (1 - k)
    return current
  })
}

function macd(candles) {
  const closes = candles.map(c => Number(c.close))
  if (closes.length < 35) return { macd: [], signal: [], histogram: [] }
  const ema12 = ema(closes, 12)
  const ema26 = ema(closes, 26)
  const macdValues = closes.map((_, index) => ema12[index] - ema26[index])
  const signalValues = ema(macdValues, 9)
  const macdRows = []
  const signalRows = []
  const histogram = []
  for (let i = 26; i < candles.length; i += 1) {
    macdRows.push({ time: candles[i].time, value: Number(macdValues[i].toFixed(4)) })
    signalRows.push({ time: candles[i].time, value: Number(signalValues[i].toFixed(4)) })
    histogram.push({ time: candles[i].time, value: Number((macdValues[i] - signalValues[i]).toFixed(4)) })
  }
  return { macd: macdRows, signal: signalRows, histogram }
}

function predictedPriceSeries(candles, points = 12) {
  const lookback = candles.slice(-30)
  if (lookback.length < 6) return []

  const n = lookback.length
  const meanX = (n - 1) / 2
  const meanY = lookback.reduce((sum, candle) => sum + Number(candle.close), 0) / n
  let numerator = 0
  let denominator = 0
  for (let i = 0; i < n; i += 1) {
    const dx = i - meanX
    numerator += dx * (Number(lookback[i].close) - meanY)
    denominator += dx * dx
  }
  const slope = denominator ? numerator / denominator : 0
  const last = lookback[lookback.length - 1]
  const prev = lookback[lookback.length - 2]
  const step = Math.max(60, Number(last.time || 0) - Number(prev.time || 0) || 60)
  const start = Number(last.close)
  const rows = [{ time: last.time, value: Number(start.toFixed(4)) }]
  for (let i = 1; i <= points; i += 1) {
    rows.push({
      time: Number(last.time || 0) + step * i,
      value: Number(Math.max(0.0001, start + slope * i).toFixed(4)),
    })
  }
  return rows
}

function timestampSeconds(value) {
  if (!value) return 0
  if (value instanceof Date) return Math.floor(value.getTime() / 1000)
  const n = Number(value)
  if (Number.isFinite(n) && n > 0) {
    return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n)
  }
  const ms = Date.parse(value)
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : 0
}

async function chartNewsEvents(db, ticker, windowMinutes = 1440) {
  const exactWindowMinutes = normalizeRollingWindowMinutes(windowMinutes, 1440)
  const days = Math.max(2, Math.ceil(exactWindowMinutes / 1440))
  const nowSec = Math.floor(Date.now() / 1000)
  const regex = `(^|,\\s*)${escapeRegExp(ticker)}(\\s*,|$)`
  const docs = await db.collection("articles").find(
    {
      ...recentArticleMatch(days),
      ticker: { $regex: regex, $options: "i" },
    },
    { projection: { title: 1, source: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1, event_type: 1, sentiment_reason: 1, publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1, url: 1 } }
  ).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(25).toArray()

  return docs
    .map(article => {
      const time = timestampSeconds(article.publish_date || article.fetched_date || article.detected_at || article.createdAt)
      const sentiment = String(article.sentiment || "neutral").toLowerCase()
      const bullish = /bull|positive/.test(sentiment)
      const bearish = /bear|negative/.test(sentiment)
      return {
        time,
        position: bearish ? "aboveBar" : "belowBar",
        color: bullish ? "#10b981" : bearish ? "#ef4444" : "#f59e0b",
        shape: bullish ? "arrowUp" : bearish ? "arrowDown" : "circle",
        text: article.event_type && article.event_type !== "general_news" ? String(article.event_type).replaceAll("_", " ").slice(0, 14).toUpperCase() : "NEWS",
        title: article.title || "Matched news",
        source: article.source || "News",
        sentiment,
        sentiment_score: Number(article.sentiment_score ?? article.ml_confidence ?? 0) || 0,
        event_type: article.event_type || "general_news",
        reason: article.sentiment_reason || "",
        url: article.url || "",
      }
    })
    .filter(event => event.time > 0 && recordIsInsideRollingWindow(event.time, exactWindowMinutes, nowSec))
    .sort((a, b) => a.time - b.time)
}

function chartSocialEvents(socialRows = []) {
  if (!Array.isArray(socialRows) || !socialRows.length) return []
  const maxCount = Math.max(1, ...socialRows.map(row => Number(row.message_count || 0)))
  return socialRows
    .filter(row => Number(row.message_count || 0) >= Math.max(2, Math.ceil(maxCount * 0.45)) || Math.abs(Number(row.sentiment || 0)) >= 0.45)
    .slice(-14)
    .map(row => {
      const sentiment = Number(row.sentiment || 0)
      return {
        time: Number(row.time || row.bucket_sec || 0),
        position: sentiment < -0.15 ? "aboveBar" : "belowBar",
        color: sentiment > 0.15 ? "#38bdf8" : sentiment < -0.15 ? "#fb7185" : "#a78bfa",
        shape: sentiment < -0.15 ? "arrowDown" : sentiment > 0.15 ? "arrowUp" : "circle",
        text: `SOC ${Number(row.message_count || 0)}`,
        title: `${Number(row.message_count || 0)} social messages; sentiment ${sentiment.toFixed(2)}`,
        source: Array.isArray(row.platforms) && row.platforms.length ? row.platforms.join(", ") : "Social",
        sentiment: sentiment > 0.15 ? "bullish" : sentiment < -0.15 ? "bearish" : "neutral",
        sentiment_score: sentiment,
        event_type: "social_spike",
      }
    })
    .filter(event => event.time > 0)
}

async function chartPredictionEvents(db, ticker, windowMinutes = 1440) {
  const sinceSec = Math.floor(Date.now() / 1000) - normalizeRollingWindowMinutes(windowMinutes, 1440) * 60
  const docs = await db.collection("prediction_signals").find(
    {
      ticker,
      signal_sec: { $gte: sinceSec },
    },
    {
      projection: {
        ticker: 1,
        signal_sec: 1,
        entry_price: 1,
        decision: 1,
        rank: 1,
        trade_watch: 1,
        baseline_signal: 1,
        model_signal: 1,
        threshold_rule_signal: 1,
        entry_signal: 1,
        labels: 1,
      },
    }
  ).sort({ signal_sec: 1 }).limit(40).toArray()

  return docs.map(doc => {
    const modelDirection = doc.model_signal?.direction
    const thresholdDirection = doc.threshold_rule_signal?.direction
    const baselineDirection = doc.baseline_signal?.direction || "watch"
    const direction = modelDirection && modelDirection !== "watch" ? modelDirection : thresholdDirection || baselineDirection
    const label5m = doc.labels?.return_5m
    const correct = label5m?.direction_correct
    const color = correct === true
      ? "#22c55e"
      : correct === false
        ? "#f97316"
        : direction === "down"
          ? "#fb7185"
          : "#f59e0b"
    const predictedReturn = doc.model_signal?.predicted_return_5m
    return {
      time: Number(doc.signal_sec || 0),
      position: direction === "down" ? "aboveBar" : "belowBar",
      color,
      shape: direction === "down" ? "arrowDown" : "arrowUp",
      text: direction === "watch" ? "PRED" : `PRED ${String(direction).toUpperCase()}`,
      title: [
        `Trade Watch ${doc.decision || "signal"}`,
        predictedReturn != null ? `model 5m ${Number(predictedReturn).toFixed(2)}%` : "",
        label5m?.return_pct != null ? `actual 5m ${Number(label5m.return_pct).toFixed(2)}%` : "",
      ].filter(Boolean).join("; "),
      source: "Prediction",
      sentiment: direction === "down" ? "bearish" : direction === "up" ? "bullish" : "neutral",
      sentiment_score: Number(doc.trade_watch?.trade_watch_score || 0),
      event_type: "prediction_signal",
      entry_price: doc.entry_price || null,
      model_signal: doc.model_signal || null,
      threshold_rule_signal: doc.threshold_rule_signal || null,
      baseline_signal: doc.baseline_signal || null,
      entry_signal: doc.entry_signal || null,
      label_5m: label5m || null,
    }
  }).filter(event => event.time > 0)
}

function strategyMarkersFromPredictionEvents(predictionEvents = []) {
  return predictionEvents
    .filter(event => event?.entry_signal?.entry_ready || event?.threshold_rule_signal?.entry_ready || event?.model_signal?.entry_ready)
    .map(event => ({
      time: Number(event.time || 0),
      type: String(event.sentiment || '').toLowerCase() === 'bearish' ? 'exit' : 'entry',
      price: event.entry_price || undefined,
    }))
    .filter(marker => marker.time > 0)
}

function strategyStatsFromPredictionEvents(predictionEvents = [], socialRows = []) {
  const markers = strategyMarkersFromPredictionEvents(predictionEvents)
  const messageCount = socialRows.reduce((sum, row) => sum + Number(row.message_count || 0), 0)
  return {
    trades: markers.length,
    setups: markers.length,
    messages: messageCount,
    threshold: 0.3,
    stop_pct: 5,
    proxy_based: false,
    note: markers.length
      ? `${markers.length} real threshold/model entry setup${markers.length === 1 ? '' : 's'} from prediction_signals.`
      : 'No entry-ready threshold/model setup in this chart window.',
  }
}

async function fetchStocktwitsWatcherCount(ticker) {
  if (typeof fetch !== "function" || !ticker) return null
  const url = `https://api.stocktwits.com/api/2/symbols/show/${encodeURIComponent(ticker)}.json`
  const resp = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent": "FeedFlashStockDashboard/0.1",
    },
  })
  if (!resp.ok) throw new Error(`stocktwits watcher HTTP ${resp.status}`)
  const payload = await resp.json()
  const count = Number(payload?.symbol?.watchlist_count)
  if (!Number.isFinite(count) || count < 0) return null
  return {
    ticker,
    watcher_count: count,
    source: "stocktwits_symbols_show",
    symbol_id: payload?.symbol?.id || null,
    symbol_title: payload?.symbol?.title || "",
  }
}

async function chartWatcherSeries(db, ticker, windowMinutes, options = {}) {
  const requestedStart = timestampSeconds(options.startSec || options.start_sec)
  const requestedEnd = timestampSeconds(options.endSec || options.end_sec)
  const sinceSec = requestedStart || (Math.floor(Date.now() / 1000) - normalizeRollingWindowMinutes(windowMinutes, 1440) * 60)
  const endSec = requestedEnd || 0
  const collection = db.collection("stocktwits_watcher_snapshots")
  let sourceStatus = "history_missing"
  let current = null

  try {
    const nowSec = Math.floor(Date.now() / 1000)
    const latest = await collection.findOne({ ticker, fetched_sec: { $lte: nowSec } }, { sort: { fetched_sec: -1 } })
    if (latest && nowSec - Number(latest.fetched_sec || 0) < 15 * 60) {
      current = latest
      sourceStatus = "cached_snapshot"
    } else {
      const fetched = await fetchStocktwitsWatcherCount(ticker)
      if (fetched) {
        current = await persistWatcherSnapshot(db, fetched, { collector: 'chart_request' })
        sourceStatus = "stocktwits_live_snapshot"
      }
    }
  } catch (err) {
    sourceStatus = `stocktwits_unavailable: ${String(err.message || err).slice(0, 80)}`
  }

  const match = { ticker, fetched_sec: { $gte: sinceSec } }
  if (endSec) match.fetched_sec.$lte = endSec
  match.fetched_sec.$lte = Math.min(endSec || Math.floor(Date.now() / 1000), Math.floor(Date.now() / 1000))
  const rawRows = await collection.find(match, {
    projection: { _id: 0, ticker: 1, fetched_sec: 1, snapshot_minute: 1, watcher_count: 1, source: 1 },
  }).sort({ fetched_sec: 1 }).limit(2000).toArray()
  const rows = dedupeWatcherSeries(rawRows, { startSec: sinceSec, endSec: match.fetched_sec.$lte })

  return {
    status: sourceStatus,
    source: "stocktwits_watchlist_count",
    current_count: Number(current?.watcher_count ?? NaN),
    snapshot_count: rows.length,
    times: rows.map(row => Number(row.fetched_sec || 0)).filter(Boolean),
    watchers: rows.map(row => Number(row.watcher_count || 0)),
    note: rows.length > 1
      ? "Real Stocktwits watcher snapshots."
      : "Watcher overlay starts when repeated real Stocktwits snapshots exist; no history is backfilled.",
  }
}

function sleepMs(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms || 0))))
}

async function recordNodeSourceStatus(db, source, status, { detail = '', count = 0, sourceType = 'internal_monitor', metadata = {} } = {}) {
  if (!db || !source) return
  const now = new Date()
  await db.collection('source_status').updateOne(
    { source },
    {
      $set: {
        source,
        status,
        detail: String(detail || '').slice(0, 500),
        count: Number(count || 0),
        source_type: sourceType,
        last_checked_at: now,
        ...(String(status || '').startsWith('working') ? { last_success_at: now } : {}),
        metadata,
      },
      $inc: { checks: 1 },
      $setOnInsert: { created_at: now },
    },
    { upsert: true },
  ).catch(() => null)
}

async function watcherSnapshotCandidateTickers(db, limit = WATCHER_SNAPSHOT_BATCH_SIZE) {
  const nowSec = Math.floor(Date.now() / 1000)
  const recentCutoff = nowSec - WATCHER_SNAPSHOT_MIN_TICKER_INTERVAL_SEC
  const poolLimit = Math.max(limit, WATCHER_SNAPSHOT_TOP_MOVER_POOL)
  const rows = await db.collection('screeners').find({
    quote_source: 'finviz_elite_screener',
    finviz_status: { $ne: 'dropped' },
    ticker: { $type: 'string' },
    price: { $gt: 0 },
    change_pct: { $gt: 0 },
  }, {
    projection: { _id: 0, ticker: 1, change_pct: 1, rel_volume: 1, volume: 1, finviz_seen_at: 1, quote_updated_at: 1 },
  }).sort({ change_pct: -1, rel_volume: -1, volume: -1 }).limit(poolLimit).toArray().catch(() => [])

  const tickers = Array.from(new Set(rows.map(row => String(row.ticker || '').toUpperCase()).filter(ticker => /^[A-Z][A-Z0-9.-]{0,7}$/.test(ticker)))).slice(0, poolLimit)
  if (!tickers.length) return { tickers: [], skippedRecent: 0 }

  const recentRows = await db.collection('stocktwits_watcher_snapshots').aggregate([
    { $match: { ticker: { $in: tickers }, fetched_sec: { $gte: recentCutoff } } },
    { $group: { _id: '$ticker', fetched_sec: { $max: '$fetched_sec' } } },
  ]).toArray().catch(() => [])
  const recent = new Set(recentRows.map(row => String(row._id || '').toUpperCase()))
  const due = tickers.filter(ticker => !recent.has(ticker)).slice(0, limit)
  return { tickers: due, skippedRecent: recent.size }
}

async function runWatcherSnapshotCycle(reason = 'scheduled') {
  if (!WATCHER_SNAPSHOT_ENABLED) return watcherSnapshotStatus
  if (watcherSnapshotStatus.running) return watcherSnapshotStatus
  const db = mongoose.connection.db
  if (!db) return watcherSnapshotStatus

  watcherSnapshotStatus.running = true
  watcherSnapshotStatus.lastStartedAt = new Date().toISOString()
  watcherSnapshotStatus.lastError = null
  let captured = 0
  let candidates = []
  let skippedRecent = 0
  try {
    const candidateResult = await watcherSnapshotCandidateTickers(db)
    candidates = candidateResult.tickers
    skippedRecent = candidateResult.skippedRecent
    for (const ticker of candidates) {
      try {
        const fetched = await fetchStocktwitsWatcherCount(ticker)
        if (fetched?.watcher_count != null) {
          await persistWatcherSnapshot(db, fetched, {
            collector: 'server_active_mover_watcher_sampler_v1',
            reason,
          })
          captured += 1
        }
      } catch (err) {
        watcherSnapshotStatus.lastError = `${ticker}: ${String(err.message || err).slice(0, 120)}`
      }
      if (WATCHER_SNAPSHOT_REQUEST_DELAY_MS) await sleepMs(WATCHER_SNAPSHOT_REQUEST_DELAY_MS)
    }
    await recordNodeSourceStatus(db, 'StockTwits Watcher Snapshots', captured ? 'working' : 'ready_no_rows_yet', {
      detail: captured
        ? `Captured ${captured}/${candidates.length} active-mover watcher snapshots`
        : `No due active-mover watcher snapshots; ${skippedRecent} tickers were recently sampled`,
      count: captured,
      sourceType: 'social_watchers',
      metadata: {
        reason,
        batch_size: WATCHER_SNAPSHOT_BATCH_SIZE,
        skipped_recent: skippedRecent,
        tickers: candidates,
      },
    })
  } catch (err) {
    watcherSnapshotStatus.lastError = String(err.message || err)
    await recordNodeSourceStatus(db, 'StockTwits Watcher Snapshots', 'error', {
      detail: watcherSnapshotStatus.lastError,
      count: captured,
      sourceType: 'social_watchers',
    })
  } finally {
    watcherSnapshotStatus.running = false
    watcherSnapshotStatus.lastFinishedAt = new Date().toISOString()
    watcherSnapshotStatus.lastTickers = candidates
    watcherSnapshotStatus.lastCaptured = captured
    watcherSnapshotStatus.lastSkippedRecent = skippedRecent
  }
  return watcherSnapshotStatus
}

function startWatcherSnapshotScheduler() {
  if (!WATCHER_SNAPSHOT_ENABLED) {
    console.log('  Watchers → snapshot scheduler disabled')
    return
  }
  setTimeout(() => runWatcherSnapshotCycle('startup').catch(() => {}), 20_000).unref?.()
  const timer = setInterval(() => runWatcherSnapshotCycle('scheduled').catch(() => {}), WATCHER_SNAPSHOT_INTERVAL_MS)
  if (timer.unref) timer.unref()
  console.log(`  Watchers → snapshot scheduler enabled (${Math.round(WATCHER_SNAPSHOT_INTERVAL_MS / 60000)} min, ${WATCHER_SNAPSHOT_BATCH_SIZE}/cycle)`)
}

async function chartSocialSeries(db, ticker, windowMinutes, bucketMinutes, options = {}) {
  const requestedStart = timestampSeconds(options.startSec || options.start_sec)
  const requestedEnd = timestampSeconds(options.endSec || options.end_sec)
  const sinceSec = requestedStart || (Math.floor(Date.now() / 1000) - windowMinutes * 60)
  const endSec = requestedEnd || 0
  const bucketSec = bucketMinutes * 60
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    { $match: { _event_sec: { $gte: sinceSec } } },
    ...(endSec ? [{ $match: { _event_sec: { $lte: endSec } } }] : []),
    { $match: { _ticker_candidates: ticker } },
    {
      $addFields: {
        _bucket_sec: { $multiply: [{ $floor: { $divide: ["$_event_sec", bucketSec] } }, bucketSec] },
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $group: {
        _id: "$_bucket_sec",
        message_count: { $sum: 1 },
        sentiment: { $avg: "$_score" },
        bullish: { $sum: { $cond: [{ $gt: ["$_score", 0.1] }, 1, 0] } },
        bearish: { $sum: { $cond: [{ $lt: ["$_score", -0.1] }, 1, 0] } },
        platforms: { $addToSet: "$_norm_platform" },
      },
    },
    { $sort: { _id: 1 } },
  ]).toArray()

  if (!rows.length) return []

  const bucketMap = new Map(rows.map(row => [Number(row._id || 0), row]))
  const firstBucket = Math.floor(sinceSec / bucketSec) * bucketSec
  const fallbackLastBucket = Math.floor(Date.now() / (bucketSec * 1000)) * bucketSec
  const lastBucket = Math.floor((requestedEnd || Math.max(Number(rows[rows.length - 1]._id || sinceSec), fallbackLastBucket)) / bucketSec) * bucketSec
  const filled = []
  for (let bucket = firstBucket; bucket <= lastBucket; bucket += bucketSec) {
    const row = bucketMap.get(bucket) || { _id: bucket, message_count: 0, sentiment: 0, platforms: [] }
    const count = Number(row.message_count || 0)
    filled.push({
      time: Number(row._id || bucket),
      bucket_sec: Number(row._id || bucket),
      session: marketSessionForSec(row._id || bucket),
      message_count: count,
      message_density: Number((count / bucketMinutes).toFixed(3)),
      sentiment: count > 0 ? Number(Number(row.sentiment || 0).toFixed(3)) : 0,
      bullish: Number(row.bullish || 0),
      bearish: Number(row.bearish || 0),
      platforms: row.platforms || [],
    })
  }

  return addSessionScaledSocialFields(filled, bucketMinutes)
}

// Full multi-timeframe selector → (Yahoo fetch range, base interval, resample-to
// minutes). Odd buckets (3m/10m/2h/5h/12h/2d) are resampled from a finer base on
// the server so the candlestick + RSI + MACD all line up. raw fetch is used so the
// longer histories aren't clipped by the default range caps.
const CHART_TF_MAP = {
  "1m":  { range: "1d",  interval: "1m",  resample: 0 },
  "3m":  { range: "5d",  interval: "1m",  resample: 3 },
  "5m":  { range: "5d",  interval: "5m",  resample: 0 },
  "10m": { range: "1mo", interval: "5m",  resample: 10 },
  "15m": { range: "1mo", interval: "15m", resample: 0 },
  "30m": { range: "1mo", interval: "30m", resample: 0 },
  "1h":  { range: "6mo", interval: "1h",  resample: 0 },
  "2h":  { range: "1y",  interval: "1h",  resample: 120 },
  "5h":  { range: "1y",  interval: "1h",  resample: 300 },
  "12h": { range: "1y",  interval: "1h",  resample: 720 },
  "1d":  { range: "2y",  interval: "1d",  resample: 0 },
  "2d":  { range: "5y",  interval: "1d",  resample: 2880 },
  "1w":  { range: "5y",  interval: "1wk", resample: 0 },
}
function resampleCandlesByMinutes(candles, minutes) {
  if (!minutes || minutes <= 0 || !candles.length) return candles
  const sizeSec = minutes * 60
  const buckets = new Map()
  for (const c of candles) {
    const t = Number(c.time)
    if (!Number.isFinite(t)) continue
    const bStart = Math.floor(t / sizeSec) * sizeSec
    const b = buckets.get(bStart)
    if (!b) buckets.set(bStart, { time: bStart, open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume || 0 })
    else { b.high = Math.max(b.high, c.high); b.low = Math.min(b.low, c.low); b.close = c.close; b.volume += c.volume || 0 }
  }
  return [...buckets.values()].sort((a, b) => a.time - b.time)
}

app.get("/api/charts/:ticker", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, candles: [], error: "MongoDB is not connected" })

    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ ok: false, candles: [], error: "ticker is required" })

    // Three ways to ask for candles, in priority order:
    //   1. tf= (1m,3m,5m,10m,15m,30m,1h,2h,5h,12h,1d,2d,1w) — the full multi-timeframe
    //      selector; server fetches the right history and resamples odd buckets.
    //   2. window= (full/2h/1h) — Aman's intraday 1-min views + density/sentiment overlays.
    //   3. range=&interval= — the original FlashFeed contract (back-compat).
    const tfParam = String(req.query.tf || "").toLowerCase()
    const tfDef = CHART_TF_MAP[tfParam]
    const windowParam = String(req.query.window || "").toLowerCase()
    const intradayWindow = !tfDef && ["full", "2h", "1h"].includes(windowParam)
    let range, interval, resampleMin = 0, rawFetch = false
    if (tfDef) {
      range = tfDef.range; interval = tfDef.interval; resampleMin = tfDef.resample; rawFetch = true
    } else if (intradayWindow) {
      range = "1d"; interval = "1m"
    } else {
      range = String(req.query.range || "3mo"); interval = yahooIntervalFor(req.query.interval || "1d")
    }
    const isMinute = interval.endsWith("m")
    const socialWindow = normalizeRollingWindowMinutes(req.query.window_minutes, isMinute ? 1440 : 4320)
    const requestedViewWindow = req.query.view_window_minutes == null || req.query.view_window_minutes === ''
      ? null
      : normalizeRollingWindowMinutes(req.query.view_window_minutes, socialWindow)
    const socialBucket = Math.max(1, Math.min(60, Number(req.query.bucket_minutes || (interval === "1m" ? 1 : 5))))

    let candleResult = { candles: [], provider_range: null, provider_interval: null }
    let priceStatus = "unavailable"
    let priceDetail = ""
    try {
      candleResult = await fetchYahooCandles(ticker, range, interval, { raw: rawFetch })
      priceStatus = candleResult.candles.length ? "working" : "no_bars_returned"
    } catch (err) {
      priceDetail = String(err.message || err)
    }

    // The multi-timeframe (tf=) path normally only needs OHLC + indicators, but
    // ChartsPage requests events=1 for diagnostics/markers. Honor that request
    // without forcing every chart image/sparkline request to run DB event queries.
    let socialRows = [], newsEvents = [], predictionEvents = [], watcherSeries = null
    const includeEvents = ["1", "true", "yes"].includes(String(req.query.events || "").toLowerCase())
    if (!tfDef || includeEvents) {
      ;[socialRows, newsEvents, predictionEvents, watcherSeries] = await Promise.all([
        chartSocialSeries(db, ticker, socialWindow, socialBucket),
        chartNewsEvents(db, ticker, socialWindow),
        chartPredictionEvents(db, ticker, socialWindow),
        chartWatcherSeries(db, ticker, socialWindow),
      ])
    }
    let candles = candleResult.candles
    if (resampleMin > 0) candles = resampleCandlesByMinutes(candles, resampleMin)
    // Optional intraday window slice (Aman's Last-2h / Last-1h controls).
    let viewCandles = candles
    if (intradayWindow && (windowParam === "2h" || windowParam === "1h") && candles.length) {
      const lastTime = Number(candles[candles.length - 1].time || 0)
      const spanSec = (windowParam === "2h" ? 2 : 1) * 3600
      viewCandles = candles.filter(c => Number(c.time || 0) >= lastTime - spanSec)
    }
    if (requestedViewWindow != null && viewCandles.length) {
      viewCandles = sliceCandlesToRollingWindow(viewCandles, requestedViewWindow)
    }
    // Session date (YYYY-MM-DD, ET) used by Aman's charts header + research views.
    let sessionDate = ""
    try {
      const lastSec = Number((viewCandles[viewCandles.length - 1] || candles[candles.length - 1] || {}).time || 0)
      const d = lastSec ? new Date(lastSec * 1000) : new Date()
      sessionDate = new Intl.DateTimeFormat("en-CA", { timeZone: MARKET_WINDOW_TIME_ZONE, year: "numeric", month: "2-digit", day: "2-digit" }).format(d)
    } catch (_) { sessionDate = new Date().toISOString().slice(0, 10) }
    const chartEvents = [...newsEvents, ...chartSocialEvents(socialRows), ...predictionEvents].sort((a, b) => Number(a.time || 0) - Number(b.time || 0))
    const socialMessageCount = socialRows.reduce((sum, row) => sum + Number(row.message_count || 0), 0)
    const strategyMarkers = strategyMarkersFromPredictionEvents(predictionEvents)
    const strategyStats = strategyStatsFromPredictionEvents(predictionEvents, socialRows)
    res.json({
      ok: true,
      ticker,
      range,
      interval,
      tf: tfParam || undefined,
      window: windowParam || undefined,
      view_window_minutes: requestedViewWindow,
      social_window_minutes: socialWindow,
      news_window_minutes: socialWindow,
      watcher_window_minutes: socialWindow,
      date: sessionDate,
      n: viewCandles.length,
      candles: viewCandles,
      bollinger: viewCandles.length >= 20 ? bollinger(viewCandles) : { upper: [], lower: [] },
      rsi: viewCandles.length >= 15 ? rsi(viewCandles) : [],
      macd: macd(viewCandles),
      predicted: predictedPriceSeries(viewCandles),
      news_events: chartEvents,
      structured_news_events: newsEvents,
      social_events: chartEvents.filter(event => event.event_type === "social_spike"),
      prediction_events: predictionEvents,
      strategy_markers: strategyMarkers,
      strategy_signal_stats: strategyStats,
      watcher_series: watcherSeries,
      sentiment: socialRows.map(row => ({ time: row.time, value: row.sentiment })),
      social_density: socialRows.map(row => ({ time: row.time, value: row.message_density, scaled: row.message_density_scaled, count: row.message_count, session: row.session })),
      social_series: socialRows,
      source_status: {
        price: priceStatus,
        price_source: priceStatus === "working" ? "market_chart_provider" : "unavailable",
        price_detail: priceDetail,
        screener_source: "Listed momentum screener universe",
        social: socialRows.length ? `${socialMessageCount} posts` : "no_social_posts",
        news: newsEvents.length ? `${newsEvents.length} news` : "no_matched_news",
        predictions: predictionEvents.length ? `${predictionEvents.length} signals` : "no_prediction_signals",
        watchers: watcherSeries?.current_count != null && Number.isFinite(Number(watcherSeries.current_count))
          ? `${Number(watcherSeries.current_count).toLocaleString()} watchers`
          : (watcherSeries?.status || "watchers_unavailable"),
        markers: chartEvents.length ? "working" : "no_events",
      },
      provider_range: candleResult.provider_range,
      provider_interval: candleResult.provider_interval,
    })
  } catch (err) {
    console.error("GET /api/charts/:ticker failed:", err)
    res.status(500).json({ ok: false, candles: [], error: String(err.message || err) })
  }
})

// ─────────────────────────────────────────────────────────────────────────────
//  Chart-support endpoints for the swapped-in Aman charts
//  (candlestick + research views + per-ticker enrich panel + grid sparklines).
//  All are additive and reuse FlashFeed's existing chart/social helpers.
// ─────────────────────────────────────────────────────────────────────────────
function etHHMM(sec) {
  const n = Number(sec || 0)
  if (!n) return ""
  try {
    return new Intl.DateTimeFormat("en-GB", { timeZone: MARKET_WINDOW_TIME_ZONE, hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).format(new Date(n * 1000))
  } catch (_) { return "" }
}
function chartHHMM(sec) {
  const n = Number(sec || 0)
  if (!n) return ""
  const d = new Date(n * 1000)
  const hh = String(d.getUTCHours()).padStart(2, "0")
  const mm = String(d.getUTCMinutes()).padStart(2, "0")
  return `${hh}:${mm}`
}
function etDate(sec) {
  const d = sec ? new Date(Number(sec) * 1000) : new Date()
  try { return new Intl.DateTimeFormat("en-CA", { timeZone: MARKET_WINDOW_TIME_ZONE, year: "numeric", month: "2-digit", day: "2-digit" }).format(d) }
  catch (_) { return new Date().toISOString().slice(0, 10) }
}

// GET /api/chart — intraday price series for the research views (ResearchChart).
app.get("/api/chart", async (req, res) => {
  try {
    const ticker = normalizeTickerList([req.query.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ error: "ticker is required" })
    const windowParam = String(req.query.window || "full").toLowerCase()
    let cr = { candles: [] }
    try { cr = await fetchYahooCandles(ticker, "1d", "1m") } catch (_) {}
    let candles = cr.candles || []
    if ((windowParam === "2h" || windowParam === "1h") && candles.length) {
      const lastTime = Number(candles[candles.length - 1].time || 0)
      const spanSec = (windowParam === "2h" ? 2 : 1) * 3600
      candles = candles.filter(c => Number(c.time || 0) >= lastTime - spanSec)
    }
    const date = etDate(Number((candles[candles.length - 1] || {}).time || 0))
    res.json({
      ticker, date,
      labels: candles.map(c => etHHMM(c.time)),
      prices: candles.map(c => Number(c.close ?? 0)),
      volumes: candles.map(c => Number(c.volume ?? 0)),
    })
  } catch (err) {
    res.status(500).json({ error: String(err.message || err) })
  }
})

// GET /api/chart/social — per-minute density + sentiment series for both the
// candlestick overlays and the research views. Derived from FlashFeed's social
// rolling series; returns a graceful empty payload when there is no social data.
app.get("/api/chart/social", async (req, res) => {
  try {
    const db = mongoose.connection.db
    const ticker = normalizeTickerList([req.query.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ error: "ticker is required" })
    const windowMinutes = normalizeRollingWindowMinutes(req.query.window_minutes, 1440)
    const bucketMinutes = Math.max(1, Math.min(60, Number(req.query.bucket_minutes || 1)))
    const startSec = timestampSeconds(req.query.start_sec)
    const endSec = timestampSeconds(req.query.end_sec)
    let rows = []
    try { rows = db ? await chartSocialSeries(db, ticker, windowMinutes, bucketMinutes, { startSec, endSec }) : [] } catch (_) { rows = [] }
    if (!rows.length) {
      return res.json({
        status: "ok", source: "none", messages: 0, bullish: 0, bearish: 0, complete: true,
        labels: [], times: [], density: [], density_smooth: [], sent_labels: [], sent_times: [], scores: [], scores_smooth: [],
        win_density: [], win_density_smooth: [], density_per_minute: [], window_minutes: windowMinutes, bucket_minutes: bucketMinutes,
        density_unit: "raw_messages_per_bucket",
        density_per_minute_unit: "messages_per_minute",
      })
    }
    const times = rows.map(r => Number(r.time || 0))
    const labels = rows.map(r => chartHHMM(r.time))
    const density = rows.map(r => Number(r.message_count ?? 0))
    const densityPerMinute = rows.map(r => Number(r.message_density ?? 0))
    const scores = rows.map(r => Number(r.sentiment ?? 0))
    const messages = rows.reduce((a, r) => a + Number(r.message_count ?? 0), 0)
    const bullish = rows.reduce((a, r) => a + Number(r.bullish ?? 0), 0)
    const bearish = rows.reduce((a, r) => a + Number(r.bearish ?? 0), 0)
    res.json({
      status: "ok", source: "feedflash-social", messages, bullish, bearish, complete: true,
      labels, times, density, density_smooth: density, density_per_minute: densityPerMinute,
      sent_labels: labels, sent_times: times, scores, scores_smooth: scores,
      win_density: density, win_density_smooth: density, window_minutes: windowMinutes, bucket_minutes: bucketMinutes,
      density_unit: "raw_messages_per_bucket",
      density_per_minute_unit: "messages_per_minute",
    })
  } catch (err) {
    res.status(500).json({ error: String(err.message || err) })
  }
})

// GET /api/chart/watchers — real Stocktwits watcher snapshots for chart overlay.
// The first request captures the current watcher count. Historical overlay lines
// appear only after multiple real snapshots exist; no backfill is fabricated.
app.get("/api/chart/watchers", async (req, res) => {
  try {
    const db = mongoose.connection.db
    const ticker = normalizeTickerList([req.query.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.status(400).json({ error: "ticker is required" })
    if (!db) return res.status(503).json({ error: "MongoDB is not connected" })
    const windowMinutes = normalizeRollingWindowMinutes(req.query.window_minutes, 1440)
    const startSec = timestampSeconds(req.query.start_sec)
    const endSec = timestampSeconds(req.query.end_sec)
    const series = await chartWatcherSeries(db, ticker, windowMinutes, { startSec, endSec })
    res.json({
      status: "ok",
      ...series,
      complete: true,
      window_minutes: windowMinutes,
    })
  } catch (err) {
    res.status(500).json({ error: String(err.message || err) })
  }
})

app.get("/api/chart/watchers/status", async (_req, res) => {
  try {
    const db = mongoose.connection.db
    const nowSec = Math.floor(Date.now() / 1000)
    const latest = db
      ? await db.collection('stocktwits_watcher_snapshots')
          .findOne({ fetched_sec: { $lte: nowSec } }, { sort: { fetched_sec: -1 }, projection: { _id: 0, ticker: 1, fetched_sec: 1, snapshot_minute: 1, watcher_count: 1, collector: 1, source: 1 } })
          .catch(() => null)
      : null
    res.json({
      ok: true,
      ...watcherSnapshotStatus,
      interval_ms: WATCHER_SNAPSHOT_INTERVAL_MS,
      min_ticker_interval_sec: WATCHER_SNAPSHOT_MIN_TICKER_INTERVAL_SEC,
      batch_size: WATCHER_SNAPSHOT_BATCH_SIZE,
      latest_snapshot: latest,
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/chart/watchers/run", async (_req, res) => {
  try {
    const status = await runWatcherSnapshotCycle('manual_api')
    res.json({ ok: true, ...status })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// GET /api/ticker/:ticker/enrich — the per-ticker enrichment panel below the
// chart: the last-3-day news feed + a social/gossip summary. Pure DB reads.
async function tickerSocialPlatformMetric(db, ticker, platform, windowHours = 72) {
  const sinceSec = Math.floor(Date.now() / 1000) - Math.max(1, Number(windowHours || 72)) * 3600
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    {
      $match: {
        _event_sec: { $gte: sinceSec },
        _norm_platform: platform,
        _ticker_candidates: ticker,
      },
    },
    {
      $addFields: {
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
              { case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment" } },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $group: {
        _id: null,
        count: { $sum: 1 },
        score_sum: { $sum: "$_score" },
        bull: { $sum: { $cond: [{ $gt: ["$_score", 0.1] }, 1, 0] } },
        bear: { $sum: { $cond: [{ $lt: ["$_score", -0.1] }, 1, 0] } },
        latest_sec: { $max: "$_event_sec" },
      },
    },
  ]).toArray()
  const row = rows[0]
  if (!row || !Number(row.count || 0)) return null
  const count = Number(row.count || 0)
  return {
    sentiment: Number((Number(row.score_sum || 0) / Math.max(1, count)).toFixed(2)),
    density: count,
    bull: Number(row.bull || 0),
    bear: Number(row.bear || 0),
    window_hours: Number(windowHours),
    latest_at: Number(row.latest_sec || 0) || null,
  }
}

async function tickerSocialRumor(db, ticker, windowHours = 72) {
  const sinceSec = Math.floor(Date.now() / 1000) - Math.max(1, Number(windowHours || 72)) * 3600
  const rows = await db.collection("socials").aggregate([
    ...socialTimeStages(),
    {
      $match: {
        _event_sec: { $gte: sinceSec },
        _ticker_candidates: ticker,
        $or: [
          { gossip_score: { $gt: 0 } },
          { gossip_keywords: { $exists: true, $ne: [] } },
          { text: /rumor|buyout|takeover|leak|chatter|whisper/i },
          { content: /rumor|buyout|takeover|leak|chatter|whisper/i },
        ],
      },
    },
    { $sort: { gossip_score: -1, _event_sec: -1 } },
    { $limit: 1 },
    {
      $project: {
        text: { $ifNull: ["$text", "$content"] },
        direction: { $ifNull: ["$direction", "$sentiment"] },
        time: "$_event_sec",
        author: 1,
      },
    },
  ]).toArray()
  const row = rows[0]
  if (!row?.text) return null
  return {
    text: String(row.text || "").slice(0, 500),
    direction: row.direction || "rumor",
    time: Number(row.time || 0) || null,
    author: row.author || null,
  }
}

app.get("/api/ticker/:ticker/enrich", async (req, res) => {
  const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
  const ENRICH_DAYS = 3
  const empty = {
    ticker, news_alert: false, news_alert_count: 0,
    news: { days: ENRICH_DAYS, articles: [], ai: null, sources: [], source_filter_active: false, note: "Last 3 days · FlashFeed structured news" },
    social: { stocktwits: null, bluesky: { configured: true, metrics: null }, reddit: { configured: true, metrics: null }, grok: { configured: true, metrics: null }, rumor: null, future_sources: [] },
  }
  try {
    const db = mongoose.connection.db
    if (!db || !ticker) return res.json(empty)
    const tickerRe = new RegExp(`(^|,)\\s*${escapeRegExp(ticker)}\\s*(,|$)`, "i")
    let docs = []
    try {
      docs = await db.collection("articles").find(
        {
          $and: [
            recentArticleMatch(ENRICH_DAYS),
            { $or: [{ ticker }, { ticker: tickerRe }, { tickers: ticker }, { symbol: ticker }] },
          ],
        },
        { projection: { title: 1, source: 1, url: 1, publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1, sentiment: 1, sentiment_score: 1, finbert_score: 1, vader_score: 1, ml_confidence: 1 } },
      ).sort({ publish_date: -1, fetched_date: -1, detected_at: -1, _id: -1 }).limit(40).toArray()
    } catch (_) { docs = [] }
    const articles = docs.map((d, i) => {
      const when = d.publish_date || d.fetched_date || d.detected_at || d.createdAt
      const sec = timestampSeconds(when) || null
      const explicitScore = Number(d.sentiment_score ?? d.finbert_score ?? d.vader_score)
      const score = Number.isFinite(explicitScore)
        ? explicitScore
        : (d.sentiment === "bullish" ? (d.ml_confidence ?? 0.5) : d.sentiment === "bearish" ? -(d.ml_confidence ?? 0.5) : 0)
      return {
        id: String(d._id || `a-${i}`),
        headline: d.title || "(untitled)",
        source: d.source || "unknown",
        url: d.url && d.url !== "#" ? d.url : null,
        published_at: sec,
        sentiment: d.sentiment || "neutral",
        sentiment_score: Number(score.toFixed(2)),
      }
    })
    const sources = [...new Set(articles.map(a => a.source))].slice(0, 12)

    // Lightweight social summary from the rolling social series.
    let stocktwits = null
    try {
      const rows = await chartSocialSeries(db, ticker, 72 * 60, 5)
      if (rows && rows.length) {
        const msgs = rows.reduce((a, r) => a + Number(r.message_count ?? 0), 0)
        const sVals = rows.map(r => Number(r.sentiment ?? 0)).filter(n => Number.isFinite(n))
        const avg = sVals.length ? sVals.reduce((a, b) => a + b, 0) / sVals.length : null
        const bull = rows.reduce((a, r) => a + (Number(r.sentiment ?? 0) > 0.1 ? Number(r.message_count ?? 0) : 0), 0)
        const bear = rows.reduce((a, r) => a + (Number(r.sentiment ?? 0) < -0.1 ? Number(r.message_count ?? 0) : 0), 0)
        if (msgs > 0) stocktwits = { sentiment: avg == null ? null : Number(avg.toFixed(2)), density: msgs, bull, bear, window_hours: 72 }
      }
    } catch (_) {}
    const [blueskyMetrics, redditMetrics, grokMetrics, rumor] = await Promise.all([
      tickerSocialPlatformMetric(db, ticker, "Bluesky", 72).catch(() => null),
      tickerSocialPlatformMetric(db, ticker, "Reddit", 72).catch(() => null),
      tickerSocialPlatformMetric(db, ticker, "Grok/X", 72).catch(() => null),
      tickerSocialRumor(db, ticker, 72).catch(() => null),
    ])

    res.json({
      ticker,
      news_alert: articles.length > 0,
      news_alert_count: articles.length,
      news: { days: ENRICH_DAYS, articles, ai: null, sources, source_filter_active: false, note: "Last 3 days · FlashFeed structured news" },
      social: {
        stocktwits,
        bluesky: { configured: true, metrics: blueskyMetrics },
        reddit: { configured: true, metrics: redditMetrics },
        grok: { configured: true, metrics: grokMetrics },
        rumor,
        future_sources: [],
      },
    })
  } catch (err) {
    console.error("GET /api/ticker/:ticker/enrich failed:", err.message)
    res.json(empty)
  }
})

// GET /api/charts/grid-image/:ticker — server-rendered SVG sparkline for the
// Charts Grid (Aman's grid used Python PNGs; this is a pure-Node SVG so it needs
// no native canvas). Green/red by net change, white background to match the grid.
const GRID_TF_MAP = {
  "1m": ["1d", "1m"], "3m": ["5d", "5m"], "5m": ["5d", "5m"], "15m": ["1mo", "15m"],
  "1h": ["1mo", "1h"], "d": ["6mo", "1d"], "w": ["1y", "1wk"],
}
app.get("/api/charts/grid-image/:ticker", async (req, res) => {
  const W = 320, H = 132, PAD = 6
  const placeholder = (msg) =>
    `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">` +
    `<rect width="${W}" height="${H}" fill="#ffffff"/>` +
    `<text x="${W / 2}" y="${H / 2}" fill="#94a3b8" font-family="monospace" font-size="12" text-anchor="middle">${msg}</text></svg>`
  res.set("Content-Type", "image/svg+xml")
  res.set("Cache-Control", "public, max-age=45")
  try {
    const ticker = normalizeTickerList([req.params.ticker], 1, { ensurePrivate: false })[0] || ""
    if (!ticker) return res.send(placeholder("no ticker"))
    const tf = String(req.query.tf || "5m").toLowerCase()
    const [range, interval] = GRID_TF_MAP[tf] || GRID_TF_MAP["5m"]
    let cr = { candles: [] }
    try { cr = await fetchYahooCandles(ticker, range, interval) } catch (_) {}
    const closes = (cr.candles || []).map(c => Number(c.close ?? 0)).filter(Number.isFinite)
    if (closes.length < 2) return res.send(placeholder(`${ticker} · no data`))
    const lo = Math.min(...closes), hi = Math.max(...closes), span = (hi - lo) || 1
    const up = closes[closes.length - 1] >= closes[0]
    const stroke = up ? "#10b981" : "#ef4444"
    const fill = up ? "rgba(16,185,129,0.12)" : "rgba(239,68,68,0.12)"
    const x = (i) => PAD + (i / (closes.length - 1)) * (W - 2 * PAD)
    const y = (v) => PAD + (1 - (v - lo) / span) * (H - 2 * PAD - 14)
    const pts = closes.map((v, i) => `${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ")
    const area = `${PAD},${(H - PAD - 14).toFixed(1)} ${pts} ${(W - PAD).toFixed(1)},${(H - PAD - 14).toFixed(1)}`
    const last = closes[closes.length - 1]
    const pct = (((last - closes[0]) / (closes[0] || 1)) * 100)
    const svg =
      `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" viewBox="0 0 ${W} ${H}">` +
      `<rect width="${W}" height="${H}" fill="#ffffff"/>` +
      `<polygon points="${area}" fill="${fill}" stroke="none"/>` +
      `<polyline points="${pts}" fill="none" stroke="${stroke}" stroke-width="1.6"/>` +
      `<text x="${PAD}" y="${H - 4}" fill="#475569" font-family="monospace" font-size="11">${ticker} ${tf}</text>` +
      `<text x="${W - PAD}" y="${H - 4}" fill="${stroke}" font-family="monospace" font-size="11" text-anchor="end">$${last.toFixed(2)} ${pct >= 0 ? "+" : ""}${pct.toFixed(2)}%</text>` +
      `</svg>`
    return res.send(svg)
  } catch (err) {
    return res.send(placeholder("chart error"))
  }
})

function articlePrimaryTicker(article) {
  return normalizeTickerList(String(article?.ticker || "").split(","), 1, { ensurePrivate: false })[0] || ""
}

function nearestCandleAtOrAfter(candles, targetSec) {
  if (!Array.isArray(candles) || !candles.length || !Number.isFinite(targetSec)) return null
  let best = null
  for (const candle of candles) {
    const time = Number(candle.time || 0)
    if (time >= targetSec) {
      best = candle
      break
    }
  }
  return best || candles[candles.length - 1]
}

function postEventReturns(candles, eventSec) {
  const base = nearestCandleAtOrAfter(candles, eventSec)
  const baseClose = Number(base?.close || 0)
  if (!base || !baseClose) return null
  const horizons = [
    ["return_1m", 60],
    ["return_5m", 300],
    ["return_15m", 900],
    ["return_1h", 3600],
  ]
  const out = {
    base_time: Number(base.time),
    base_close: Number(baseClose.toFixed(4)),
  }
  for (const [key, seconds] of horizons) {
    const future = nearestCandleAtOrAfter(candles, eventSec + seconds)
    const futureClose = Number(future?.close || 0)
    out[key] = futureClose ? Number((((futureClose - baseClose) / baseClose) * 100).toFixed(3)) : null
  }
  return out
}

app.get("/api/correlation/post-news", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(120, Number(req.query.limit || 50)))
    const days = Math.max(1, Math.min(5, Number(req.query.days || 3)))
    const requestedTicker = normalizeTickerList([req.query.ticker], 1, { ensurePrivate: false })[0] || ""
    const match = { ...recentArticleMatch(days), ticker: { $exists: true, $nin: ["", null] } }
    if (requestedTicker) match.ticker = { $regex: `(^|,\\s*)${escapeRegExp(requestedTicker)}(\\s*,|$)`, $options: "i" }

    const articles = await db.collection("articles").find(
      match,
      { projection: { title: 1, source: 1, url: 1, ticker: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1, event_type: 1, sentiment_reason: 1, publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1 } }
    ).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(limit).toArray()

    const tickers = Array.from(new Set(articles.map(articlePrimaryTicker).filter(Boolean))).slice(0, 20)
    const candleMap = new Map()
    await Promise.all(tickers.map(async ticker => {
      try {
        const result = await fetchYahooCandles(ticker, "5d", "1m")
        candleMap.set(ticker, result.candles || [])
      } catch {
        candleMap.set(ticker, [])
      }
    }))

    const rows = articles.map(article => {
      const ticker = articlePrimaryTicker(article)
      const eventSec = timestampSeconds(article.publish_date || article.fetched_date || article.detected_at || article.createdAt)
      const score = Number(article.sentiment_score ?? article.ml_confidence ?? 0) || 0
      const returns = postEventReturns(candleMap.get(ticker), eventSec)
      return {
        id: String(article._id),
        ticker,
        title: article.title || "",
        source: article.source || "",
        url: article.url || "",
        sentiment: article.sentiment || "neutral",
        sentiment_score: score,
        event_type: article.event_type || "general_news",
        reason: article.sentiment_reason || "",
        event_time: eventSec,
        ...(returns || { base_time: null, base_close: null, return_1m: null, return_5m: null, return_15m: null, return_1h: null }),
      }
    }).filter(row => row.ticker)

    const withReturns = rows.filter(row => row.return_5m != null)
    const average = key => {
      const vals = withReturns.map(row => Number(row[key])).filter(Number.isFinite)
      return vals.length ? Number((vals.reduce((sum, value) => sum + value, 0) / vals.length).toFixed(3)) : null
    }

    res.json({
      ok: true,
      rows,
      summary: {
        articles: rows.length,
        priced_articles: withReturns.length,
        avg_return_1m: average("return_1m"),
        avg_return_5m: average("return_5m"),
        avg_return_15m: average("return_15m"),
        avg_return_1h: average("return_1h"),
      },
      horizons: ["1m", "5m", "15m", "1h"],
      note: "Returns use nearest available 1-minute market candle at or after the article timestamp.",
    })
  } catch (err) {
    console.error("GET /api/correlation/post-news failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get(["/api/sentiment/audit", "/api/sentiment/snapshot"], async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })
    const days = Math.max(1, Math.min(7, Number(req.query.days || 3)))
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 60)))
    const label = String(req.query.label || "").toLowerCase()
    const recentMatch = recentArticleMatch(days)
    const tickerMatch = { ...recentMatch, ticker: { $exists: true, $nin: ["", null] } }
    const actionableMatch = {
      ...tickerMatch,
      $or: [
        { sentiment: { $nin: ["neutral", null, ""] } },
        { event_type: { $exists: true, $nin: ["general_news", "unknown", null, ""] } },
      ],
    }
    const match = { ...tickerMatch }
    if (["bullish", "positive"].includes(label)) match.sentiment = { $regex: "bull|positive", $options: "i" }
    if (["bearish", "negative"].includes(label)) match.sentiment = { $regex: "bear|negative", $options: "i" }
    if (label === "neutral") match.sentiment = { $regex: "neutral", $options: "i" }

    const scoredProjection = {
      title: 1,
      source: 1,
      url: 1,
      ticker: 1,
      sentiment: 1,
      sentiment_score: 1,
      ml_confidence: 1,
      sentiment_method: 1,
      event_type: 1,
      event_score: 1,
      sentiment_reason: 1,
      publish_date: 1,
      fetched_date: 1,
      detected_at: 1,
    }
    const scoreStages = [
      {
        $addFields: {
          _sentiment_direction: {
            $switch: {
              branches: [
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
              ],
              default: 0,
            },
          },
        },
      },
      {
        $addFields: {
          _score: {
            $switch: {
              branches: [
                { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"]] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
              ],
              default: "$_sentiment_direction",
            },
          },
        },
      },
    ]

    const socialWindowMinutes = Math.max(5, Math.min(4320, Number(req.query.window_minutes || 1440)))
    const socialSinceSec = Math.floor(Date.now() / 1000) - socialWindowMinutes * 60

    const [rows, total, tickerMatched, nonNeutral, eventful, actionable, sentimentSummary, topPositive, topNegative, sourceSummary, eventSummary, tickerSummary, socialSummary] = await Promise.all([
      db.collection("articles").find(
        match,
        { projection: scoredProjection }
      ).sort({ detected_at: -1, fetched_date: -1, publish_date: -1 }).limit(limit).toArray(),
      db.collection("articles").countDocuments(recentMatch),
      db.collection("articles").countDocuments(tickerMatch),
      db.collection("articles").countDocuments({ ...tickerMatch, sentiment: { $nin: ["neutral", null, ""] } }),
      db.collection("articles").countDocuments({ ...tickerMatch, event_type: { $exists: true, $nin: ["general_news", "unknown", null, ""] } }),
      db.collection("articles").countDocuments(actionableMatch),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: null,
            avg_sentiment: { $avg: "$_score" },
            avg_abs_sentiment: { $avg: { $abs: "$_score" } },
            scored: { $sum: { $cond: [{ $gt: [{ $abs: "$_score" }, 0.005] }, 1, 0] } },
          },
        },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        { $match: { _score: { $gt: 0.005 } } },
        { $sort: { _score: -1, detected_at: -1, fetched_date: -1, publish_date: -1 } },
        { $limit: 5 },
        { $project: { ...scoredProjection, sentiment_score: "$_score" } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        { $match: { _score: { $lt: -0.005 } } },
        { $sort: { _score: 1, detected_at: -1, fetched_date: -1, publish_date: -1 } },
        { $limit: 5 },
        { $project: { ...scoredProjection, sentiment_score: "$_score" } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: { $ifNull: ["$source", "Unknown"] },
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
            scored: { $sum: { $cond: [{ $gt: [{ $abs: "$_score" }, 0.005] }, 1, 0] } },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, source: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] }, scored: 1 } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: { $ifNull: ["$event_type", "general_news"] },
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
          },
        },
        { $sort: { count: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, event_type: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] } } },
      ]).toArray(),
      db.collection("articles").aggregate([
        { $match: tickerMatch },
        ...scoreStages,
        {
          $group: {
            _id: "$ticker",
            count: { $sum: 1 },
            avg_sentiment: { $avg: "$_score" },
            latest: { $max: { $ifNull: ["$detected_at", { $ifNull: ["$fetched_date", "$publish_date"] }] } },
          },
        },
        { $sort: { count: -1, latest: -1 } },
        { $limit: 8 },
        { $project: { _id: 0, ticker: "$_id", count: 1, avg_sentiment: { $round: ["$avg_sentiment", 3] }, latest: 1 } },
      ]).toArray(),
      db.collection("socials").aggregate([
        ...socialTimeStages(),
        { $match: { _event_sec: { $gte: socialSinceSec }, _ticker_candidates: { $ne: [] } } },
        {
          $addFields: {
            _social_score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $in: [{ $type: "$sentiment" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment" } },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        {
          $group: {
            _id: null,
            total: { $sum: 1 },
            avg_sentiment: { $avg: "$_social_score" },
            bullish: { $sum: { $cond: [{ $gt: ["$_social_score", 0.05] }, 1, 0] } },
            bearish: { $sum: { $cond: [{ $lt: ["$_social_score", -0.05] }, 1, 0] } },
            neutral: { $sum: { $cond: [{ $lte: [{ $abs: "$_social_score" }, 0.05] }, 1, 0] } },
            platforms: { $addToSet: "$_norm_platform" },
          },
        },
      ]).toArray(),
    ])

    const mapAuditRow = row => ({
      id: String(row._id),
      ticker: row.ticker || "",
      title: row.title || "",
      source: row.source || "",
      url: row.url || "",
      sentiment: row.sentiment || "neutral",
      sentiment_score: Number(articleSentimentValue(row).toFixed(3)),
      confidence: Number(row.ml_confidence ?? Math.abs(articleSentimentValue(row))) || 0,
      method: row.sentiment_method || "unknown",
      event_type: row.event_type || "general_news",
      event_score: Number(row.event_score || 0),
      reason: row.sentiment_reason || "No high-impact phrase matched",
      publish_date: row.publish_date || row.fetched_date || row.detected_at || null,
    })

    const social = socialSummary[0] || {}
    const newsAvg = Number(sentimentSummary[0]?.avg_sentiment || 0)
    const socialAvg = Number(social.avg_sentiment || 0)
    const combinedWeight = Number(tickerMatched || 0) + Number(social.total || 0) * 0.75
    const combinedAvg = combinedWeight
      ? (newsAvg * Number(tickerMatched || 0) + socialAvg * Number(social.total || 0) * 0.75) / combinedWeight
      : 0

    res.json({
      ok: true,
      generated_at: new Date().toISOString(),
      rows: rows.map(mapAuditRow),
      top_positive: topPositive.map(mapAuditRow),
      top_negative: topNegative.map(mapAuditRow),
      sources: sourceSummary,
      event_types: eventSummary,
      ticker_breakdown: tickerSummary,
      days,
      social_window_minutes: socialWindowMinutes,
      summary: {
        total,
        ticker_matched: tickerMatched,
        non_neutral: nonNeutral,
        eventful,
        actionable,
        avg_sentiment: Number((sentimentSummary[0]?.avg_sentiment || 0).toFixed(3)),
        avg_abs_sentiment: Number((sentimentSummary[0]?.avg_abs_sentiment || 0).toFixed(3)),
        scored: Number(sentimentSummary[0]?.scored || 0),
        social_total: Number(social.total || 0),
        social_avg_sentiment: Number((social.avg_sentiment || 0).toFixed(3)),
        social_bullish: Number(social.bullish || 0),
        social_bearish: Number(social.bearish || 0),
        social_neutral: Number(social.neutral || 0),
        social_platforms: social.platforms || [],
        combined_avg_sentiment: Number(combinedAvg.toFixed(3)),
      },
      snapshot_mode: "news_and_social_signed_sentiment_snapshot",
      audit_mode: "deterministic_financial_phrase_with_event_taxonomy",
    })
  } catch (err) {
    console.error("GET /api/sentiment snapshot failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/sentiment/batch-candidates", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, items: [], error: "MongoDB is not connected" })
    const days = Math.max(1, Math.min(7, Number(req.query.days || 3)))
    const limit = Math.max(1, Math.min(150, Number(req.query.limit || 100)))
    const rows = await db.collection("articles").find(
      {
        ...recentArticleMatch(days),
        ticker: { $exists: true, $nin: ["", null] },
        $or: [
          { sentiment: { $regex: "neutral", $options: "i" } },
          { sentiment_score: { $gte: -0.12, $lte: 0.12 } },
          { sentiment_score: { $exists: false } },
        ],
      },
      { projection: { title: 1, content: 1, source: 1, ticker: 1, url: 1 } }
    ).sort({ detected_at: -1, fetched_date: -1, publish_date: -1 }).limit(limit).toArray()

    const items = rows.map((row, index) => ({
      id: String(row._id),
      batch_id: index + 1,
      ticker: row.ticker || "",
      source: row.source || "",
      headline: row.title || "",
      excerpt: String(row.content || "").slice(0, 500),
      url: row.url || "",
    }))

    res.json({
      ok: true,
      items,
      prompt: "Classify each item sentiment for the listed stock ticker. Echo id. Return label as positive, negative, neutral, or mixed; score from -1 to 1; event_type; short reason.",
      response_schema: {
        type: "array",
        items: {
          type: "object",
          required: ["id", "label", "score", "event_type", "reason"],
          properties: {
            id: { type: "string" },
            label: { enum: ["positive", "negative", "neutral", "mixed"] },
            score: { type: "number" },
            event_type: { type: "string" },
            reason: { type: "string" },
          },
        },
      },
      note: "This is the low-volume LLM batch queue for borderline/neutral articles; deterministic scoring remains live.",
    })
  } catch (err) {
    console.error("GET /api/sentiment/batch-candidates failed:", err)
    res.status(500).json({ ok: false, items: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/features", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(10, Math.min(500, Number(req.query.limit || 150)))
    const days = Math.max(1, Math.min(14, Number(req.query.days || 3)))
    const socialWindow = Math.max(5, Math.min(1440, Number(req.query.window_minutes || 60)))
    const sinceSec = Math.floor(Date.now() / 1000) - socialWindow * 60

    const screenerRows = await db.collection("screeners").find(
      {
        ticker: { $exists: true, $nin: ["", null], $not: /\./ },
        exchange: { $in: ["NASDAQ", "NYSE", "AMEX"] },
        price: { $gt: 0 },
        change_pct: { $exists: true },
      },
      {
        projection: {
          ticker: 1,
          company: 1,
          exchange: 1,
          sector: 1,
          industry: 1,
          price: 1,
          change_pct: 1,
          volume: 1,
          avg_volume: 1,
          market_cap: 1,
          shares_float: 1,
          float_short: 1,
          short_interest: 1,
          float_percent: 1,
          rel_volume: 1,
          price_density_correlation: 1,
          previous_price_density_correlation: 1,
          threshold_pre_return_60m_pct: 1,
          threshold_trailing_60m_messages: 1,
          threshold_feature_window_minutes: 1,
          threshold_feature_status: 1,
          threshold_setup_status: 1,
          threshold_setup_score: 1,
          rsi: 1,
          gap: 1,
          perf_week: 1,
          perf_month: 1,
          quote_updated_at: 1,
        },
      }
    ).sort({ volume: -1 }).limit(limit).toArray()

    const tickers = screenerRows.map(row => String(row.ticker || "").toUpperCase()).filter(Boolean)
    const [articleRows, socialRows] = await Promise.all([
      db.collection("articles").aggregate([
        { $match: { ...recentArticleMatch(days), ticker: { $exists: true, $nin: ["", null] } } },
        {
          $addFields: {
            _ticker_parts: {
              $map: {
                input: { $split: [{ $toUpper: { $toString: "$ticker" } }, ","] },
                as: "ticker_part",
                in: { $trim: { input: "$$ticker_part" } },
              },
            },
            _sentiment_direction: {
              $switch: {
                branches: [
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        { $unwind: "$_ticker_parts" },
        { $match: { _ticker_parts: { $in: tickers } } },
        {
          $addFields: {
            _score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $in: [{ $type: "$ml_confidence" }, ["int", "long", "double", "decimal"]] }, then: { $multiply: ["$_sentiment_direction", { $toDouble: "$ml_confidence" }] } },
                ],
                default: "$_sentiment_direction",
              },
            },
          },
        },
        {
          $group: {
            _id: "$_ticker_parts",
            article_count: { $sum: 1 },
            article_sentiment: { $avg: "$_score" },
            article_sentiment_abs: { $avg: { $abs: "$_score" } },
            event_count: {
              $sum: {
                $cond: [
                  { $not: { $in: ["$event_type", ["general_news", "unknown", null, ""]] } },
                  1,
                  0,
                ],
              },
            },
            latest_article_ts: { $max: "$detected_at" },
          },
        },
      ]).toArray(),
      db.collection("socials").aggregate([
        ...socialTimeStages(),
        ...socialTickerCandidateStages(),
        { $match: { _event_sec: { $gte: sinceSec }, _ticker_candidates: { $in: tickers } } },
        { $unwind: "$_ticker_candidates" },
        { $match: { _ticker_candidates: { $in: tickers } } },
        {
          $addFields: {
            _score: {
              $switch: {
                branches: [
                  { case: { $in: [{ $type: "$sentiment_score" }, ["int", "long", "double", "decimal"]] }, then: { $toDouble: "$sentiment_score" } },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bull|positive" } }, then: 1 },
                  { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ["$sentiment", ""] } } }, regex: "bear|negative" } }, then: -1 },
                ],
                default: 0,
              },
            },
          },
        },
        {
          $group: {
            _id: "$_ticker_candidates",
            social_count: { $sum: 1 },
            social_sentiment: { $avg: "$_score" },
            social_sentiment_abs: { $avg: { $abs: "$_score" } },
            latest_social_ts: { $max: "$_event_sec" },
          },
        },
      ]).toArray(),
    ])

    const articleMap = new Map(articleRows.map(row => [String(row._id || "").toUpperCase(), row]))
    const socialMap = new Map(socialRows.map(row => [String(row._id || "").toUpperCase(), row]))

    const rows = screenerRows.map(raw => {
      const row = normalizeScreenerDoc(raw)
      const articles = articleMap.get(row.ticker) || {}
      const social = socialMap.get(row.ticker) || {}
      const volume = Number(row.volume || 0)
      const avgVolume = Number(row.avg_volume || 0)
      const relVolume = Number(row.rel_volume || (avgVolume ? volume / Math.max(1, avgVolume) : 0)) || 0
      const articleSentiment = Number(articles.article_sentiment || 0)
      const socialSentiment = Number(social.social_sentiment || 0)
      const socialCount = Number(social.social_count || 0)
      const articleCount = Number(articles.article_count || 0)
      const eventCount = Number(articles.event_count || 0)
      const momentumScore = Number(row.change_pct || 0)
      const evidenceScore = articleSentiment * Math.min(1, articleCount / 5) + socialSentiment * Math.min(1, socialCount / 20)
      const modelReady = Boolean(row.price && volume && (articleCount || socialCount) && Number.isFinite(momentumScore))
      const featureRow = {
        price: row.price,
        change_pct: row.change_pct,
        volume,
        rel_volume: Number(relVolume.toFixed(3)),
        market_cap: row.market_cap,
        market_cap_bucket: row.market_cap_bucket,
        market_cap_tier: predictionMarketCapTier(row),
        shares_float: row.shares_float ?? null,
        float_bucket: predictionFloatBucket(row),
        float_short: row.float_short ?? null,
        short_interest: row.short_interest ?? null,
        float_percent: row.float_percent ?? null,
        price_density_correlation: row.price_density_correlation ?? null,
        previous_price_density_correlation: row.previous_price_density_correlation ?? null,
        threshold_pre_return_60m_pct: row.threshold_pre_return_60m_pct ?? null,
        threshold_trailing_60m_messages: row.threshold_trailing_60m_messages ?? null,
        threshold_feature_window_minutes: row.threshold_feature_window_minutes ?? null,
        rsi: row.rsi,
        gap: row.gap,
        perf_week: row.perf_week,
        perf_month: row.perf_month,
        article_count: articleCount,
        article_sentiment: Number(articleSentiment.toFixed(3)),
        event_count: eventCount,
        social_count: socialCount,
        social_density_per_minute: Number((socialCount / socialWindow).toFixed(3)),
        social_sentiment: Number(socialSentiment.toFixed(3)),
        evidence_score: Number(evidenceScore.toFixed(3)),
      }
      const thresholdEntry = evaluatePredictionEntryThreshold(row, featureRow)

      return {
        ticker: row.ticker,
        company: row.company,
        exchange: row.exchange,
        sector: row.sector,
        generated_at: new Date().toISOString(),
        features: featureRow,
        threshold_policy: thresholdEntry,
        entry_signal: {
          policy_version: thresholdEntry.policyVersion,
          tier: thresholdEntry.tier,
          status: thresholdEntry.status,
          passed: thresholdEntry.passed,
          entry_ready: Boolean(thresholdEntry.passed),
          reason: thresholdEntry.reason,
          float_bucket: thresholdEntry.floatBucket,
          max_signal_abs_change_pct: thresholdEntry.maxSignalAbsChangePct,
          active_signal_abs_change_pct: thresholdEntry.activeSignalAbsChangePct,
          rejection_reasons: thresholdEntry.rejectionReasons,
        },
        labels: {
          target_return_5m: null,
          target_return_15m: null,
          target_return_60m: null,
        },
        baseline_signal: {
          direction: thresholdEntry.passed && evidenceScore >= 0.15 && momentumScore > 0 ? "up" : thresholdEntry.passed && evidenceScore <= -0.15 && momentumScore < 0 ? "down" : "watch",
          raw_direction: evidenceScore >= 0.15 && momentumScore > 0 ? "up" : evidenceScore <= -0.15 && momentumScore < 0 ? "down" : "watch",
          confidence: Number(Math.min(0.95, Math.abs(evidenceScore) * 0.35 + Math.min(1, relVolume / 5) * 0.25 + Math.min(1, Math.abs(momentumScore) / 20) * 0.25).toFixed(3)),
          model_ready: modelReady && thresholdEntry.passed,
          entry_ready: Boolean(thresholdEntry.passed),
          threshold_status: thresholdEntry.status,
        },
      }
    })

    res.json({
      ok: true,
      rows,
      count: rows.length,
      feature_version: "price_social_news_v1",
      social_window_minutes: socialWindow,
      label_status: "pending_intraday_return_join",
      note: "This endpoint is the stock-price-prediction feature matrix. The next step is joining 1-minute candles after each signal timestamp to fill target_return labels.",
    })
  } catch (err) {
    console.error("GET /api/prediction/features failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/signals", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 100)))
    const ticker = String(req.query.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const status = String(req.query.status || "").toLowerCase()
    const filter = {}
    if (ticker) filter.ticker = ticker
    if (status) filter.label_status = status

    const [rows, summaryRows, model] = await Promise.all([
      db.collection("prediction_signals").find(filter).sort({ signal_sec: -1, rank: 1 }).limit(limit).toArray(),
      db.collection("prediction_signals").aggregate([
        { $match: filter },
        {
          $group: {
            _id: "$label_status",
            count: { $sum: 1 },
            avg_score: { $avg: "$trade_watch.trade_watch_score" },
            avg_5m: { $avg: "$labels.return_5m.return_pct" },
            avg_15m: { $avg: "$labels.return_15m.return_pct" },
            avg_60m: { $avg: "$labels.return_60m.return_pct" },
            correct_5m: {
              $avg: {
                $cond: [
                  { $eq: ["$labels.return_5m.direction_correct", true] },
                  1,
                  { $cond: [{ $eq: ["$labels.return_5m.direction_correct", false] }, 0, null] },
                ],
              },
            },
          },
        },
      ]).toArray(),
      loadLatestPredictionModel(db),
    ])

    res.json({
      ok: true,
      rows: rows.map(row => ({
        ...row,
        id: String(row._id),
        _id: undefined,
      })),
      count: rows.length,
      summary: summaryRows.map(row => ({
        status: row._id || "unknown",
        count: row.count,
        avg_score: Number((row.avg_score || 0).toFixed(3)),
        avg_return_5m: row.avg_5m == null ? null : Number(row.avg_5m.toFixed(3)),
        avg_return_15m: row.avg_15m == null ? null : Number(row.avg_15m.toFixed(3)),
        avg_return_60m: row.avg_60m == null ? null : Number(row.avg_60m.toFixed(3)),
        directional_accuracy_5m: row.correct_5m == null ? null : Number(row.correct_5m.toFixed(3)),
      })),
      horizons_minutes: PREDICTION_HORIZONS_MINUTES,
      feature_version: "trade_watch_prediction_v1",
      threshold_policy: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY,
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        min_samples: model.min_samples,
        metrics: model.metrics || null,
        updated_at: model.updated_at || null,
      } : null,
    })
  } catch (err) {
    console.error("GET /api/prediction/signals failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.get("/api/prediction/active-context", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })
    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 100)))
    const ticker = String(req.query.ticker || "").toUpperCase().replace(/[^A-Z0-9.-]/g, "")
    const entryReady = String(req.query.entry_ready || "").toLowerCase()
    const filter = {}
    if (ticker) filter.ticker = ticker
    if (entryReady === "1" || entryReady === "true") filter["entry_signal.entry_ready"] = true
    if (entryReady === "0" || entryReady === "false") filter["entry_signal.entry_ready"] = false
    const rows = await db.collection("active_ticker_context")
      .find(filter)
      .sort({ signal_sec: -1, rank: 1 })
      .limit(limit)
      .toArray()
    res.json({
      ok: true,
      rows: rows.map(row => ({ ...row, id: String(row._id), _id: undefined })),
      count: rows.length,
      context_version: "active_ticker_context_v1",
      threshold_policy_version: PREDICTION_THRESHOLD_POLICY_VERSION,
    })
  } catch (err) {
    console.error("GET /api/prediction/active-context failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

const PREDICTION_REPLAY_REPORT_ID = "latest_catalyst_missed_mover_replay"
let predictionReplayInFlight = null

async function loadLatestPredictionReplay(db) {
  return db.collection("prediction_replay_reports").findOne(
    { _id: PREDICTION_REPLAY_REPORT_ID },
    { projection: { _id: 0 } },
  ).catch(() => null)
}

async function runPredictionReplayScript(args = []) {
  const { execFile } = await import("node:child_process")
  const sourcePath = path.resolve(process.cwd(), "scripts/shadow_catalyst_replay.js")
  const scriptPath = "/tmp/flashfeed_shadow_catalyst_replay.cjs"
  fs.copyFileSync(sourcePath, scriptPath)
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  const started = Date.now()
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      [scriptPath, ...args],
      {
        cwd: process.cwd(),
        timeout: 240000,
        maxBuffer: 1024 * 1024 * 20,
        env: {
          ...process.env,
          MONGODB_URI: mongoUri,
          MONGO_URI: mongoUri,
          MONGO_DB: "feedflash",
          MONGODB_DB: "feedflash",
          NODE_PATH: path.resolve(process.cwd(), "node_modules"),
        },
      },
      (error, stdout, stderr) => resolve({
        ok: !error,
        stdout: String(stdout || ""),
        stderr: String(stderr || ""),
        error: error ? String(error.message || error) : "",
        ms: Date.now() - started,
      }),
    )
  })
}

app.get("/api/prediction/replay", async (_req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const report = await loadLatestPredictionReplay(db)
    res.json({ ok: true, report })
  } catch (err) {
    console.error("GET /api/prediction/replay failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/prediction/replay/refresh", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })

    const input = { ...(req.query || {}), ...(req.body || {}) }
    const args = ["--persist", "1", "--reportId", PREDICTION_REPLAY_REPORT_ID]
    const addDateArg = (name, value) => {
      if (value && Number.isFinite(Date.parse(String(value)))) args.push(`--${name}`, new Date(String(value)).toISOString())
    }
    const addNumberArg = (name, value, min, max) => {
      const number = Number(value)
      if (Number.isFinite(number)) args.push(`--${name}`, String(Math.max(min, Math.min(max, number))))
    }
    addDateArg("asOf", input.as_of)
    addDateArg("since", input.since)
    addNumberArg("minMove", input.min_move, 1, 500)
    addNumberArg("limit", input.limit, 1, 100)
    addNumberArg("horizonMinutes", input.horizon_minutes, 5, 10_080)

    const joinedExistingRun = Boolean(predictionReplayInFlight)
    const activeRun = predictionReplayInFlight || runPredictionReplayScript(args)
    predictionReplayInFlight = activeRun
    let run
    try {
      run = await activeRun
    } finally {
      if (predictionReplayInFlight === activeRun) predictionReplayInFlight = null
    }
    if (!run.ok) {
      return res.status(500).json({
        ok: false,
        error: run.error || "Missed-mover replay failed",
        stderr: String(run.stderr || "").slice(-4000),
        ms: run.ms,
      })
    }
    const report = await loadLatestPredictionReplay(db)
    res.json({
      ok: true,
      report,
      ms: run.ms,
      joined_existing_run: joinedExistingRun,
      note: "Causal shadow replay refreshed from stored mover snapshots, article availability times, prediction signals, and real Mongo OHLC. Production policy was not changed.",
    })
  } catch (err) {
    console.error("POST /api/prediction/replay/refresh failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.get("/api/prediction/audit", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, rows: [], error: "MongoDB is not connected" })

    const nowSec = Math.floor(Date.now() / 1000)
    const days = Math.max(1, Math.min(30, Number(req.query.days || 7)))
    const limit = Math.max(1, Math.min(500, Number(req.query.limit || 120)))
    const horizon = PREDICTION_HORIZONS_MINUTES.includes(Number(req.query.horizon_minutes))
      ? Number(req.query.horizon_minutes)
      : 60
    const sinceSec = nowSec - days * 86_400
    const labelKey = `labels.return_${horizon}m`
    const returnPath = `$${labelKey}.return_pct`
    const correctPath = `$${labelKey}.direction_correct`
    const validBaseFilter = {
      ticker: { $exists: true, $nin: ["", null] },
      signal_sec: { $exists: true },
      entry_price: { $gt: 0 },
    }
    const validWindowFilter = { ...validBaseFilter, signal_sec: { $gte: sinceSec } }
    const incompleteWindowFilter = {
      signal_sec: { $gte: sinceSec },
      $or: [
        { ticker: { $exists: false } },
        { ticker: { $in: ["", null] } },
        { entry_price: { $exists: false } },
        { entry_price: { $lte: 0 } },
      ],
    }
    const maturePendingFilter = {
      ...validBaseFilter,
      signal_sec: { $lte: nowSec - Math.max(...PREDICTION_HORIZONS_MINUTES) * 60 - 15 * 60 },
      $or: [
        { label_status: "pending" },
        { label_status: { $exists: false } },
      ],
    }

    const [
      totalEstimated,
      incompleteWindowCount,
      rows,
      statusSummary,
      decisionSummary,
      confidenceSummary,
      readinessSummary,
      labelSourceSummary,
      pendingMatureCount,
      latestArchive,
      model,
      missedMoverReplay,
    ] = await Promise.all([
      db.collection("prediction_signals").estimatedDocumentCount().catch(() => 0),
      db.collection("prediction_signals").countDocuments(incompleteWindowFilter).catch(() => 0),
      db.collection("prediction_signals").find(validWindowFilter, {
        projection: {
          _id: 1,
          signal_id: 1,
          ticker: 1,
          company: 1,
          source: 1,
          discovery_source: 1,
          signal_sec: 1,
          signal_at: 1,
          entry_price: 1,
          rank: 1,
          decision: 1,
          label_status: 1,
          labels: 1,
          baseline_signal: 1,
          model_signal: 1,
          threshold_rule_signal: 1,
          entry_signal: 1,
          threshold_policy: 1,
          trade_watch: 1,
          features: 1,
          updated_at: 1,
        },
      }).sort({ signal_sec: -1, rank: 1 }).limit(limit).toArray(),
      db.collection("prediction_signals").aggregate([
        { $match: validWindowFilter },
        {
          $group: {
            _id: "$label_status",
            count: { $sum: 1 },
            avg_score: { $avg: "$trade_watch.trade_watch_score" },
            avg_return: { $avg: returnPath },
            labeled: {
              $sum: { $cond: [{ $eq: [`$${labelKey}.labeled`, true] }, 1, 0] },
            },
            direction_correct: {
              $avg: {
                $cond: [
                  { $eq: [correctPath, true] },
                  1,
                  { $cond: [{ $eq: [correctPath, false] }, 0, null] },
                ],
              },
            },
            win_rate: {
              $avg: {
                $cond: [
                  { $in: [{ $type: returnPath }, ["int", "long", "double", "decimal"]] },
                  { $cond: [{ $gt: [returnPath, 0] }, 1, 0] },
                  null,
                ],
              },
            },
          },
        },
        { $sort: { count: -1 } },
      ]).toArray().catch(() => []),
      db.collection("prediction_signals").aggregate([
        { $match: { ...validWindowFilter, [`labels.return_${horizon}m.labeled`]: true } },
        {
          $group: {
            _id: { $ifNull: ["$decision", "unknown"] },
            count: { $sum: 1 },
            avg_return: { $avg: returnPath },
            win_rate: { $avg: { $cond: [{ $gt: [returnPath, 0] }, 1, 0] } },
            direction_correct: {
              $avg: {
                $cond: [
                  { $eq: [correctPath, true] },
                  1,
                  { $cond: [{ $eq: [correctPath, false] }, 0, null] },
                ],
              },
            },
          },
        },
        { $sort: { count: -1 } },
      ]).toArray().catch(() => []),
      db.collection("prediction_signals").aggregate([
        { $match: { ...validWindowFilter, [`labels.return_${horizon}m.labeled`]: true } },
        {
          $addFields: {
            _confidence: {
              $ifNull: [
                "$model_signal.confidence",
                { $ifNull: ["$baseline_signal.confidence", "$trade_watch.trade_watch_score"] },
              ],
            },
          },
        },
        {
          $addFields: {
            _confidence_tier: {
              $switch: {
                branches: [
                  { case: { $gte: ["$_confidence", 0.75] }, then: "high" },
                  { case: { $gte: ["$_confidence", 0.5] }, then: "medium" },
                  { case: { $gte: ["$_confidence", 0.25] }, then: "low" },
                ],
                default: "unknown",
              },
            },
          },
        },
        {
          $group: {
            _id: "$_confidence_tier",
            count: { $sum: 1 },
            avg_return: { $avg: returnPath },
            win_rate: { $avg: { $cond: [{ $gt: [returnPath, 0] }, 1, 0] } },
            direction_correct: {
              $avg: {
                $cond: [
                  { $eq: [correctPath, true] },
                  1,
                  { $cond: [{ $eq: [correctPath, false] }, 0, null] },
                ],
              },
            },
          },
        },
        { $sort: { _id: 1 } },
      ]).toArray().catch(() => []),
      db.collection("prediction_signals").aggregate([
        { $match: { ...validWindowFilter, [`labels.return_${horizon}m.labeled`]: true } },
        {
          $addFields: {
            _readiness: {
              $cond: [
                { $eq: ["$entry_signal.entry_ready", true] },
                "entry_ready",
                { $ifNull: ["$entry_signal.status", "not_ready_or_missing"] },
              ],
            },
          },
        },
        {
          $group: {
            _id: "$_readiness",
            count: { $sum: 1 },
            avg_return: { $avg: returnPath },
            win_rate: { $avg: { $cond: [{ $gt: [returnPath, 0] }, 1, 0] } },
          },
        },
        { $sort: { count: -1 } },
      ]).toArray().catch(() => []),
      db.collection("prediction_signals").aggregate([
        { $match: { ...validWindowFilter, [`labels.return_${horizon}m.labeled`]: true } },
        {
          $group: {
            _id: { $ifNull: [`$labels.return_${horizon}m.label_source`, "legacy_or_unknown"] },
            count: { $sum: 1 },
            avg_return: { $avg: returnPath },
            win_rate: { $avg: { $cond: [{ $gt: [returnPath, 0] }, 1, 0] } },
            direction_correct: {
              $avg: {
                $cond: [
                  { $eq: [correctPath, true] },
                  1,
                  { $cond: [{ $eq: [correctPath, false] }, 0, null] },
                ],
              },
            },
          },
        },
        { $sort: { count: -1 } },
      ]).toArray().catch(() => []),
      db.collection("prediction_signals").countDocuments(maturePendingFilter).catch(() => 0),
      db.collection("daily_prediction_snapshots")
        .find({}, { projection: { _id: 0, updated_at: 1, predictionDate: 1, targetDate: 1, archive_status: 1, rowCount: 1, metadata: 1 } })
        .sort({ updated_at: -1, created_at: -1 })
        .limit(1)
        .next()
        .catch(() => null),
      loadLatestPredictionModel(db),
      loadLatestPredictionReplay(db),
    ])

    const metricRow = (row) => ({
      key: row._id || "unknown",
      count: Number(row.count || 0),
      labeled: row.labeled == null ? undefined : Number(row.labeled || 0),
      avg_score: row.avg_score == null ? null : Number(row.avg_score.toFixed(3)),
      avg_return_pct: row.avg_return == null ? null : Number(row.avg_return.toFixed(3)),
      win_rate: row.win_rate == null ? null : Number(row.win_rate.toFixed(3)),
      directional_accuracy: row.direction_correct == null ? null : Number(row.direction_correct.toFixed(3)),
    })
    const rowLabel = (row, minutes) => row.labels?.[`return_${minutes}m`] || null
    const auditRows = rows.map(row => {
      const labels = Object.fromEntries(PREDICTION_HORIZONS_MINUTES.map(minutes => [String(minutes), rowLabel(row, minutes)]))
      const qualityFlags = []
      if (!row.ticker) qualityFlags.push("missing_ticker")
      if (!row.signal_sec) qualityFlags.push("missing_signal_time")
      if (!Number(row.entry_price || 0)) qualityFlags.push("missing_entry_price")
      if (!Object.values(labels).some(label => label?.labeled)) qualityFlags.push("no_outcome_labels_yet")
      if (!row.features || !Object.keys(row.features || {}).length) qualityFlags.push("missing_feature_snapshot")
      if (!row.entry_signal && !row.threshold_policy) qualityFlags.push("missing_threshold_snapshot")
      const selectedLabel = rowLabel(row, horizon)
      if (selectedLabel?.labeled && selectedLabel.label_source !== "mongo_ohlcv_bars") qualityFlags.push("legacy_or_non_ohlc_label")
      return {
        id: String(row._id || row.signal_id || ""),
        signal_id: row.signal_id || String(row._id || ""),
        ticker: row.ticker,
        company: row.company || "",
        source: row.source || row.discovery_source || "prediction_signal",
        signal_sec: row.signal_sec,
        signal_at: isoFromSec(row.signal_sec) || row.signal_at || null,
        age_seconds: row.signal_sec ? Math.max(0, nowSec - Number(row.signal_sec)) : null,
        rank: row.rank || null,
        decision: row.decision || "unknown",
        entry_price: row.entry_price || null,
        label_status: row.label_status || "unknown",
        labels,
        selected_horizon_label: selectedLabel,
        baseline_signal: row.baseline_signal || null,
        model_signal: row.model_signal || null,
        threshold_rule_signal: row.threshold_rule_signal || null,
        entry_signal: row.entry_signal || null,
        threshold_policy: row.threshold_policy || null,
        features: row.features || null,
        trade_watch: row.trade_watch || null,
        audit_quality: {
          valid: qualityFlags.length === 0 || qualityFlags.every(flag => flag === "no_outcome_labels_yet"),
          flags: qualityFlags,
        },
      }
    })
    const archiveMeta = latestArchive?.metadata || {}
    const validWindowCount = statusSummary.reduce((sum, row) => sum + Number(row.count || 0), 0)

    res.json({
      ok: true,
      generated_at: new Date().toISOString(),
      days,
      horizon_minutes: horizon,
      rows: auditRows,
      count: auditRows.length,
      data_quality: {
        estimated_total_records: Number(totalEstimated || 0),
        valid_records_in_window: validWindowCount,
        incomplete_records_in_window: Number(incompleteWindowCount || 0),
        mature_pending_labels: Number(pendingMatureCount || 0),
        note: "Validation metrics include only records with ticker, signal_sec, and positive entry_price; outcome labels are generated from Mongo OHLC bars when available.",
      },
      summary: {
        by_status: statusSummary.map(metricRow),
        by_decision: decisionSummary.map(metricRow),
        by_confidence: confidenceSummary.map(metricRow),
        by_readiness: readinessSummary.map(metricRow),
        by_label_source: labelSourceSummary.map(metricRow),
      },
      latest_prediction_archive: latestArchive ? {
        updated_at: latestArchive.updated_at || null,
        predictionDate: latestArchive.predictionDate || null,
        targetDate: latestArchive.targetDate || null,
        archive_status: latestArchive.archive_status || null,
        rowCount: latestArchive.rowCount || 0,
        finalRows: archiveMeta.finalRows || latestArchive.rowCount || 0,
        strictRows: archiveMeta.strictRows || 0,
        candidatePoolRows: archiveMeta.candidatePoolRows || 0,
        warnings: archiveMeta.warnings || [],
        removedByFilterCounts: archiveMeta.removedByFilterCounts || {},
        predictionRiskFlagCounts: archiveMeta.predictionRiskFlagCounts || {},
      } : null,
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        min_samples: model.min_samples,
        metrics: model.metrics || null,
        updated_at: model.updated_at || null,
      } : null,
      threshold_policy: {
        version: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.version,
        status: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.status,
        candidate_rule: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule?.name,
        mechanics: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.mechanics || {},
        candidateRule: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule || null,
        pooledWindows: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.pooledWindows || [],
        source_backtest: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule?.sourceBacktest,
        caveat: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule?.backtestSummary?.caveat,
      },
      missed_mover_replay: missedMoverReplay,
    })
  } catch (err) {
    console.error("GET /api/prediction/audit failed:", err)
    res.status(500).json({ ok: false, rows: [], error: String(err.message || err) })
  }
})

app.post("/api/prediction/audit/refresh", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const labels = await labelMaturePredictionSignals(db, {
      limit: Number(req.query.limit || req.body?.limit || 1500),
      relabelLegacy: String(req.query.relabel_legacy ?? req.body?.relabel_legacy ?? "true") !== "false",
    })
    const model = Number(labels.labeled || 0) > 0
      ? await trainPredictionModel(db, {
          minSamples: Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20),
          limit: Number(req.query.train_limit || req.body?.train_limit || 3000),
        })
      : await loadLatestPredictionModel(db)
    res.json({
      ok: true,
      labels,
      model_updated: Number(labels.labeled || 0) > 0,
      model: model ? {
        status: model.status,
        samples: model.samples || 0,
        metrics: model.metrics || null,
        updated_at: model.updated_at || null,
      } : null,
        note: "Outcome refresh labeled matured prediction_signals from real Mongo OHLC bars and retrained the shadow calibration model. It did not change threshold policy.",
    })
  } catch (err) {
    console.error("POST /api/prediction/audit/refresh failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.get("/api/prediction/model", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const model = await loadLatestPredictionModel(db)
    res.json({ ok: true, model })
  } catch (err) {
    console.error("GET /api/prediction/model failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/prediction/train", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const model = await trainPredictionModel(db, {
      minSamples: Number(req.query.min_samples || req.body?.min_samples || 20),
      limit: Number(req.query.limit || req.body?.limit || 2000),
    })
    res.json({ ok: true, model })
  } catch (err) {
    console.error("POST /api/prediction/train failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/prediction/snapshot", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: "MongoDB is not connected" })
    const labels = await labelMaturePredictionSignals(db)
    const snapshot = await captureTradeWatchPredictionSignals(db, {
      limit: Number(req.query.limit || req.body?.limit || process.env.PREDICTION_SIGNAL_LIMIT || 10),
      socialWindow: Number(req.query.window_minutes || req.body?.window_minutes || process.env.PREDICTION_SOCIAL_WINDOW || 60),
    })
    const model = Number(labels.labeled || 0) > 0
      ? await trainPredictionModel(db, {
          minSamples: Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20),
          limit: Number(process.env.PREDICTION_TRAIN_LIMIT || 3000),
        })
      : await loadLatestPredictionModel(db)
    res.json({ ok: true, labels, snapshot, model, model_updated: Number(labels.labeled || 0) > 0 })
  } catch (err) {
    console.error("POST /api/prediction/snapshot failed:", err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

async function runPythonScriptForRoute(scriptPath, {
  timeout = 180000,
  extraEnv = {},
} = {}) {
  const { execFile } = await import("node:child_process")
  const { existsSync } = await import("node:fs")
  const localPython = `${process.cwd()}/.venv/bin/python`
  const pythonPath = existsSync("/opt/rssvenv/bin/python")
    ? "/opt/rssvenv/bin/python"
    : existsSync(localPython)
      ? localPython
      : "python3"

  if (!existsSync(scriptPath)) {
    return {
      ok: false,
      skipped: true,
      stdout: "",
      stderr: "",
      error: `Script not found at ${scriptPath}`,
    }
  }

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  try {
    return await new Promise((resolve) => {
      execFile(
        pythonPath,
        [scriptPath],
        {
          cwd: process.cwd(),
          timeout,
          maxBuffer: 1024 * 1024 * 20,
          env: {
            ...process.env,
            MONGODB_URI: mongoUri,
            MONGO_URI: mongoUri,
            MONGO_DB: "feedflash",
            MONGODB_DB: "feedflash",
            ...extraEnv,
          },
        },
        (error, stdout, stderr) => {
          resolve({
            ok: !error,
            stdout: String(stdout || ""),
            stderr: String(stderr || ""),
            error: error ? String(error.message || error) : "",
          })
        }
      )
    })
  } catch (err) {
    return { ok: false, stdout: "", stderr: "", error: String(err?.message || err) }
  }
}

function parseSocialFetchForRoute(stdout = "") {
  const text = String(stdout || "")
  const savedMatch = text.match(/saved=(\d+)/i)
  const matchedMatch = text.match(/matched=(\d+)/i)
  const insertedMatch = text.match(/inserted=(\d+)/i)
  const modifiedMatch = text.match(/modified=(\d+)/i)
  return {
    saved: savedMatch ? Number(savedMatch[1]) : undefined,
    matched: matchedMatch ? Number(matchedMatch[1]) : undefined,
    inserted: insertedMatch ? Number(insertedMatch[1]) : undefined,
    modified: modifiedMatch ? Number(modifiedMatch[1]) : undefined,
  }
}

app.post("/api/social/fetch", async (req, res) => {
  const started = Date.now()
  const ticker = normalizeTickerList([req.query.ticker || req.body?.ticker], 1, { ensurePrivate: false })[0] || ""

  if (!ticker) {
    return res.status(400).json({ ok: false, error: "ticker is required", ms: Date.now() - started })
  }

  try {
    const result = await runPythonScriptForRoute("1_News/pipeline/fetch_social_to_mongo.py", {
      timeout: 45000,
      extraEnv: {
        SOCIAL_TICKERS: ticker,
        SOCIAL_MAX_TICKERS: "1",
        SOCIAL_MAX_WORKERS: "1",
      },
    })
    const counts = parseSocialFetchForRoute(result.stdout || "")

    return res.status(result.ok ? 200 : 500).json({
      ok: result.ok,
      ticker,
      ...counts,
      stdout: result.stdout,
      stderr: result.stderr,
      error: result.error,
      ms: Date.now() - started,
    })
  } catch (err) {
    return res.status(500).json({
      ok: false,
      ticker,
      error: String(err?.message || err),
      ms: Date.now() - started,
    })
  }
})

app.get("/api/social/rolling/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({ ok: false, error: "MongoDB is not connected", counts: {} })
    }

    const windowMinutes = Math.max(1, Math.min(1440, Number(req.query.window_minutes || 5)))
    const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60

    const rows = await db.collection("socials").aggregate([
      ...socialTimeStages(),
      { $match: { _event_sec: { $gte: sinceSec } } },
      {
        $match: {
          _norm_platform: { $ne: "Unstructured" },
          _ticker_candidates: { $ne: [] },
        },
      },
      { $group: { _id: "$_norm_platform", count: { $sum: 1 } } },
      { $sort: { count: -1 } }
    ]).toArray()

    const counts = {}
    for (const row of rows) counts[row._id || "Unknown"] = row.count

    return res.json({
      ok: true,
      counts,
      rows,
      total: rows.reduce((sum, row) => sum + row.count, 0),
      window_minutes: windowMinutes,
      since_sec: sinceSec,
      now_sec: Math.floor(Date.now() / 1000),
    })
  } catch (err) {
    console.error("GET /api/social/rolling/stats failed:", err)
    return res.status(500).json({ ok: false, error: String(err?.message || err), counts: {} })
  }
})
// SOCIAL_ROLLING_API_V2_END


app.use('/api/social',      socialRouter)
app.use('/api/correlation', correlationRouter)
app.use('/api/settings',    settingsRouter)
app.use('/api/decision-map', decisionMapRouter)

// ── Health check ──────────────────────────────────────────
app.get('/api/health', (req, res) => {
  const { readyState } = mongoose.connection
  const states = { 0:'disconnected', 1:'connected', 2:'connecting', 3:'disconnecting' }
  res.json({
    status:  'ok',
    db:      states[readyState] || 'unknown',
    time:    new Date().toISOString(),
  })
})

// ── Start ─────────────────────────────────────────────────
async function ensureRuntimeIndexes() {
  const db = mongoose.connection.db
  if (!db) return

  await Promise.allSettled([
    db.collection("articles").createIndex({ ticker: 1, detected_at: -1 }),
    db.collection("articles").createIndex({ ticker: 1, fetched_date: -1 }),
    db.collection("articles").createIndex({ source: 1, fetched_date: -1 }),
    db.collection("articles").createIndex({ sentiment: 1, event_type: 1 }),
    db.collection("socials").createIndex({ ticker: 1, fetched_at: -1 }),
    db.collection("socials").createIndex({ symbol: 1, fetched_at: -1 }),
    db.collection("socials").createIndex({ platform: 1, fetched_at: -1 }),
    db.collection("screeners").createIndex({ exchange: 1, change_pct: -1 }),
    db.collection("screeners").createIndex({ exchange: 1, volume: -1 }),
    db.collection("screeners").createIndex({ quote_source: 1, change_pct: -1 }),
    db.collection("screeners").createIndex({ threshold_feature_policy_version: 1, threshold_feature_status: 1 }),
    db.collection("screeners").createIndex({ threshold_feature_policy_version: 1, threshold_feature_window_minutes: 1, threshold_feature_status: 1, threshold_feature_updated_at: -1 }),
    db.collection("prediction_signals").createIndex({ ticker: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ label_status: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ source: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ signal_sec: -1, rank: 1 }),
    db.collection("prediction_signals").createIndex({ signal_sec: -1, ticker: 1, entry_price: 1 }),
    db.collection("prediction_signals").createIndex({ threshold_policy_version: 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ "entry_signal.policy_version": 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ "labels.return_5m.label_source": 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ "labels.return_15m.label_source": 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ "labels.return_60m.label_source": 1, signal_sec: -1 }),
    db.collection("prediction_signals").createIndex({ "labels.return_5m.missing_reason": 1, signal_sec: -1 }),
    db.collection("prediction_models").createIndex({ model_id: 1, updated_at: -1 }),
    db.collection("active_ticker_context").createIndex({ ticker: 1 }, { unique: true }),
    db.collection("active_ticker_context").createIndex({ threshold_policy_version: 1, signal_sec: -1 }),
    db.collection("active_ticker_context").createIndex({ "entry_signal.status": 1, signal_sec: -1 }),
    db.collection("active_ticker_context").createIndex({ "entry_signal.entry_ready": 1, signal_sec: -1 }),
    db.collection("ohlcv_bars").createIndex({ source: 1, ticker: 1, minute: 1 }, { unique: true }),
    db.collection("ohlcv_bars").createIndex({ ticker: 1, minute: -1 }),
    db.collection("stocktwits_watcher_snapshots").createIndex({ ticker: 1, fetched_sec: -1 }),
    db.collection("stocktwits_watcher_snapshots").createIndex({ fetched_sec: -1 }),
    db.collection("stocktwits_watcher_snapshots").createIndex(
      { ticker: 1, snapshot_minute: 1 },
      { unique: true, partialFilterExpression: { snapshot_minute: { $type: 'number' } } },
    ),
    db.collection("source_status").createIndex({ source: 1 }, { unique: true }),
  ])
}

async function start() {
  await connectDB()
  await ensureRuntimeIndexes()
  startWatcherSnapshotScheduler()

  // Shared guard so the heavy data-refresh cycle never runs twice at once
  // (double Run Now clicks, or Run Now firing while the auto-grabber is mid-cycle).
  let refreshCycleInFlight = false
  let lastPredictionMaintenanceAt = 0
  let lastOhlcvHydrationAt = 0
  const PREDICTION_MAINTENANCE_INTERVAL_MS = Math.max(
    60_000,
    Number(process.env.PREDICTION_MAINTENANCE_INTERVAL_MS || 300_000),
  )
  const OHLCV_HYDRATION_INTERVAL_MS = Math.max(
    60_000,
    Number(process.env.OHLCV_HYDRATION_INTERVAL_MS || 300_000),
  )
  
// Ryan frontend compatibility endpoints
app.get("/api/status", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const articles = db.collection("articles");
    const articleWindow = recentArticleMatch();
    const [totalArticles, recentArticles] = await Promise.all([
      articles.countDocuments({}),
      articles.countDocuments(articleWindow),
    ])

    const latest = await articles.find(
      {},
      { projection: { title: 1, source: 1, publish_date: 1, fetched_date: 1 } }
    ).sort({ fetched_date: -1, publish_date: -1 }).limit(1).toArray();

    res.json({
      ok: true,
      status: "ok",
      connected: true,
      articles: totalArticles,
      total: totalArticles,
      total_all: totalArticles,
      recent_articles: recentArticles,
      article_count: totalArticles,
      database: {
        connected: mongoose.connection.readyState === 1,
        articles: totalArticles,
        total: totalArticles,
        total_all: totalArticles,
        recent_articles: recentArticles,
        article_count: totalArticles
      },
      latest_article: latest[0] || null,
      market_window_start: latestMarketCloseCutoff().toISOString(),
      time: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      status: "error",
      connected: false,
      articles: 0,
      total: 0,
      article_count: 0,
      database: {
        connected: false,
        articles: 0,
        total: 0,
        article_count: 0
      },
      error: "Failed to load status"
    });
  }
});

app.get("/api/market/status", async (req, res) => {
  try {
    const now = new Date();
    const ny = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const day = ny.getDay(); // 0 = Sun .. 6 = Sat
    const hour = ny.getHours();
    const minute = ny.getMinutes();
    const minutes = hour * 60 + minute;

    const preStart = 4 * 60; // 04:00 ET
    const regularStart = 9 * 60 + 30; // 09:30 ET
    const regularEnd = 16 * 60; // 16:00 ET
    const afterEnd = 20 * 60; // 20:00 ET

    const isWeekday = day >= 1 && day <= 5
    const inPreMarket = isWeekday && minutes >= preStart && minutes < regularStart
    const inRegular = isWeekday && minutes >= regularStart && minutes < regularEnd
    const inAfterHours = isWeekday && minutes >= regularEnd && minutes < afterEnd

    const nextOpen = (() => {
      if (inRegular || inPreMarket || inAfterHours) {
        if (inRegular || inPreMarket) return `${String(9).padStart(2, '0')}:30 ET`
        return `${String(9).padStart(2, '0')}:30 ET`
      }

      const isFriday = day === 5
      const nextWeekday = isFriday ? 1 : day === 6 ? 1 : day === 0 ? 1 : day + 1
      return `${String(9).padStart(2, '0')}:30 ET on ${['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'][nextWeekday]}`
    })()

    const nextClose = inRegular ? `${String(16).padStart(2, '0')}:00 ET` : undefined

    let status = 'closed'
    let label = 'Market Closed'
    if (inRegular) {
      status = 'open'
      label = 'Market Open'
    } else if (inPreMarket) {
      status = 'pre'
      label = 'Pre-market'
    } else if (inAfterHours) {
      status = 'after'
      label = 'After-hours'
    }

    res.json({
      open: status === 'open',
      status,
      label,
      timezone: 'America/New_York',
      next_open: nextOpen,
      next_close: nextClose,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      tracked_markets: TRACKED_MARKETS,
      updated_at: ny.toISOString()
    })
  } catch (err) {
    res.json({ open: false, status: 'unknown', label: 'Market Unknown', updated_at: new Date().toISOString() })
  }
});

app.get("/api/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    res.json(await loadArticleStats(db, Number(req.query.days || req.query.recent_days || 0)));
  } catch (err) {
    const trackedTickers = loadTrackedTickers()
    res.status(500).json({
      total: 0,
      total_recent: 0,
      total_all: 0,
      sources: [],
      categories: [],
      sentiment: { bullish: 0, bearish: 0, neutral: 0, unknown: 0 },
      ticker_mentions: [],
      tracked_market_count: TRACKED_MARKETS.length,
      tracked_markets: TRACKED_MARKETS,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
      tracked_ticker_count: trackedTickers.length,
      tracked_tickers: trackedTickers,
      error: "Failed to load stats"
    });
  }
});

app.get("/api/keywords", async (req, res) => {
  res.json({
    keywords: [
      "earnings",
      "guidance",
      "upgrade",
      "downgrade",
      "merger",
      "acquisition",
      "lawsuit",
      "sec",
      "fda",
      "short squeeze",
      "bankruptcy",
      "dividend",
      "offering",
      "partnership"
    ]
  });
});


// Duplicate /api/keywords removed - see settings routes for the authoritative implementation

// Frontend compatibility endpoints
app.get("/api/status", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const articles = db.collection("articles");
    const [totalArticles, recentArticles] = await Promise.all([
      articles.countDocuments({}),
      articles.countDocuments(recentArticleMatch()),
    ]);

    res.json({
      ok: true,
      status: "ok",
      database: {
        connected: mongoose.connection.readyState === 1,
        articles: totalArticles,
        total_all: totalArticles,
        recent_articles: recentArticles,
        market_window_start: latestMarketCloseCutoff().toISOString()
      },
      time: new Date().toISOString()
    });
  } catch (err) {
    res.status(500).json({
      ok: false,
      status: "error",
      database: {
        connected: false,
        articles: 0
      },
      error: "Failed to load status"
    });
  }
});

// Duplicate /api/market/status removed - see line 323 for the authoritative implementation

app.get("/api/stats", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    res.json(await loadArticleStats(db, Number(req.query.days || req.query.recent_days || 0)));
  } catch (err) {
    const trackedTickers = loadTrackedTickers()
    res.status(500).json({
      total: 0,
      total_recent: 0,
      total_all: 0,
      sources: [],
      categories: [],
      sentiment: { bullish: 0, bearish: 0, neutral: 0, unknown: 0 },
      ticker_mentions: [],
      tracked_market_count: TRACKED_MARKETS.length,
      tracked_markets: TRACKED_MARKETS,
      tracked_exchanges: Array.from(US_EXCHANGES),
      tracked_indices: TRACKED_MARKET_INDICES,
      market_universe_label: "NASDAQ / NYSE / AMEX equities plus major US index markets",
      tracked_ticker_count: trackedTickers.length,
      tracked_tickers: trackedTickers,
      error: "Failed to load stats"
    });
  }
});

// Duplicate /api/keywords removed - see settings routes for the authoritative implementation

async function runPythonScript(scriptPath, {
  timeout = 180000,
  extraEnv = {},
} = {}) {
  const { execFile } = await import("node:child_process")
  const { existsSync } = await import("node:fs")

  const localPython = `${process.cwd()}/.venv/bin/python`
  const pythonPath = existsSync("/opt/rssvenv/bin/python")
    ? "/opt/rssvenv/bin/python"
    : existsSync(localPython)
      ? localPython
      : "python3"

  if (!existsSync(scriptPath)) {
    return {
      ok: false,
      skipped: true,
      stdout: "",
      stderr: "",
      error: `Script not found at ${scriptPath}`,
    }
  }

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  const started = Date.now()

  try {
    const result = await new Promise((resolve, reject) => {
      execFile(
        pythonPath,
        [scriptPath],
        {
          cwd: process.cwd(),
          timeout,
          maxBuffer: 1024 * 1024 * 20,
          env: {
            ...process.env,
            MONGODB_URI: mongoUri,
            MONGO_URI: mongoUri,
            MONGO_DB: "feedflash",
            MONGODB_DB: "feedflash",
            RSS_COOLDOWN_SECONDS: "0",
            RSS_STATE_FILE: "/tmp/feedflash_rss_fetch_state.json",
            ...extraEnv,
          },
        },
        (error, stdout, stderr) => {
          if (error) {
            error.stdout = stdout
            error.stderr = stderr
            reject(error)
            return
          }
          resolve({ stdout, stderr })
        }
      )
    })

    return {
      ok: true,
      stdout: String(result.stdout || ""),
      stderr: String(result.stderr || ""),
      ms: Date.now() - started,
    }
  } catch (err) {
    return {
      ok: false,
      stdout: String(err?.stdout || ""),
      stderr: String(err?.stderr || ""),
      error: String(err?.message || err),
      ms: Date.now() - started,
    }
  }
}

async function runNodeScript(scriptPath, {
  timeout = 180000,
  args = [],
  extraEnv = {},
} = {}) {
  const { execFile } = await import("node:child_process")
  const { existsSync } = await import("node:fs")

  if (!existsSync(scriptPath)) {
    return {
      ok: false,
      skipped: true,
      stdout: "",
      stderr: "",
      error: `Script not found at ${scriptPath}`,
      ms: 0,
    }
  }

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongo:27017/feedflash"
  const started = Date.now()
  try {
    return await new Promise((resolve) => {
      execFile(
        process.execPath,
        [scriptPath, ...args],
        {
          cwd: process.cwd(),
          timeout,
          maxBuffer: 1024 * 1024 * 20,
          env: {
            ...process.env,
            MONGODB_URI: mongoUri,
            MONGO_URI: mongoUri,
            MONGO_DB: "feedflash",
            MONGODB_DB: "feedflash",
            ...extraEnv,
          },
        },
        (error, stdout, stderr) => {
          resolve({
            ok: !error,
            stdout: String(stdout || ""),
            stderr: String(stderr || ""),
            error: error ? String(error.message || error) : "",
            ms: Date.now() - started,
          })
        }
      )
    })
  } catch (err) {
    return { ok: false, stdout: "", stderr: "", error: String(err?.message || err), ms: Date.now() - started }
  }
}

function skippedPythonResult(name, reason = "skipped in fast mode") {
  return {
    ok: true,
    skipped: true,
    stdout: `${name} skipped — ${reason}`,
    stderr: "",
    error: "",
    ms: 0,
  }
}

function parseStructuredFetch(stdout, before, after) {
  const match =
    stdout.match(/RSS Mongo import complete\s+—\s+(\d+)\s+new,\s+(\d+)\s+updated,\s+(\d+)\s+unchanged/i) ||
    stdout.match(/RSS Mongo import complete.*?(\d+)\s+new.*?(\d+)\s+updated.*?(\d+)\s+unchanged/is)

  return {
    new_articles: match ? Number(match[1]) : Math.max(0, after - before),
    updated_articles: match ? Number(match[2]) : 0,
    unchanged_articles: match ? Number(match[3]) : 0,
  }
}

function parseUnstructuredFetch(stdout) {
  const found = stdout.match(/['"]found['"]:\s*(\d+)/)
  const upserted = stdout.match(/['"]upserted['"]:\s*(\d+)/)
  const modified = stdout.match(/['"]modified['"]:\s*(\d+)/)
  return {
    unstructured_found: found ? Number(found[1]) : 0,
    unstructured_new: upserted ? Number(upserted[1]) : 0,
    unstructured_updated: modified ? Number(modified[1]) : 0,
  }
}

function parseSocialFetch(stdout) {
  const match = stdout.match(/Social import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    social_found: match ? Number(match[1]) : 0,
    social_new: match ? Number(match[2]) : 0,
    social_updated: match ? Number(match[3]) : 0,
  }
}

function parseQuoteFetch(stdout) {
  const match = stdout.match(/Quote import complete\s+—\s+(\d+)\s+quotes,\s+(\d+)\s+updated/i)
  return {
    quotes_found: match ? Number(match[1]) : 0,
    quotes_updated: match ? Number(match[2]) : 0,
  }
}

function parseFinvizEliteFetch(stdout) {
  const match = stdout.match(/Finviz Elite import complete\s+—\s+(\d+)\s+rows,\s+(\d+)\s+updated,\s+(\d+)\s+dropped/i)
  return {
    finviz_rows: match ? Number(match[1]) : 0,
    finviz_updated: match ? Number(match[2]) : 0,
    finviz_dropped: match ? Number(match[3]) : 0,
  }
}

function parseTradingViewFetch(stdout) {
  const match = stdout.match(/TradingView import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    tradingview_found: match ? Number(match[1]) : 0,
    tradingview_new: match ? Number(match[2]) : 0,
    tradingview_updated: match ? Number(match[3]) : 0,
  }
}

function parseTradingViewScreenerFetch(stdout) {
  const match = stdout.match(/TradingView screener import complete\s+—\s+(\d+)\s+rows,\s+(\d+)\s+updated/i)
  return {
    tradingview_screener_rows: match ? Number(match[1]) : 0,
    tradingview_screener_updated: match ? Number(match[2]) : 0,
  }
}

function parseBenzingaFetch(stdout) {
  const match = stdout.match(/Benzinga import complete\s+—\s+(\d+)\s+found,\s+(\d+)\s+new,\s+(\d+)\s+updated/i)
  return {
    benzinga_found: match ? Number(match[1]) : 0,
    benzinga_new: match ? Number(match[2]) : 0,
    benzinga_updated: match ? Number(match[3]) : 0,
  }
}

function parseSourceDebug(stdout, aliases = []) {
  const aliasSet = new Set(aliases.map((alias) => String(alias || "").toLowerCase()))
  const fallback = {
    attempted: false,
    ok: false,
    fetched: 0,
    inserted: 0,
    updated: 0,
    deduped: 0,
    tickerMatched: 0,
    errors: [],
  }
  for (const line of String(stdout || "").split(/\r?\n/)) {
    const match = line.match(/SOURCE_DEBUG_JSON\s+({.*})/)
    if (!match) continue
    try {
      const parsed = JSON.parse(match[1])
      const source = String(parsed.source || "").toLowerCase()
      if (!aliasSet.has(source)) continue
      return {
        attempted: Boolean(parsed.attempted),
        ok: Boolean(parsed.ok),
        fetched: Number(parsed.fetched || 0),
        inserted: Number(parsed.inserted || parsed.new || 0),
        updated: Number(parsed.updated || 0),
        deduped: Number(parsed.deduped || parsed.unchanged || 0),
        tickerMatched: Number(parsed.tickerMatched || parsed.ticker_matched || 0),
        errors: Array.isArray(parsed.errors) ? parsed.errors.filter(Boolean).map(String) : [],
        warnings: Array.isArray(parsed.warnings) ? parsed.warnings.filter(Boolean).map(String) : [],
      }
    } catch (_) {}
  }
  return fallback
}

async function runDataRefreshCycle(db, { socialMode = "top_momentum", mode = "fast" } = {}) {
  const refreshMode = String(mode || process.env.DEFAULT_FETCH_MODE || "fast").toLowerCase() === "full" ? "full" : "fast"
  const fastMode = refreshMode !== "full"
  const beforeArticles = await db.collection("articles").countDocuments()
  const beforeSocial = await db.collection("socials").countDocuments()
  const socialExtraEnv = {
    SOCIAL_TICKER_SOURCE: "momentum",
    SOCIAL_MOMENTUM_LIMIT: process.env.SOCIAL_MOMENTUM_LIMIT || "40",
    SOCIAL_MAX_TICKERS: process.env.SOCIAL_MAX_TICKERS || "40",
    SOCIAL_MAX_WORKERS: process.env.SOCIAL_MAX_WORKERS || "8",
    SOCIAL_REDDIT_TIMEOUT: process.env.SOCIAL_REDDIT_TIMEOUT || "8",
    SOCIAL_REDDIT_PUBLIC_FALLBACK: process.env.SOCIAL_REDDIT_PUBLIC_FALLBACK || "true",
    SOCIAL_REDDIT_PUBLIC_JSON: process.env.SOCIAL_REDDIT_PUBLIC_JSON || "true",
    SOCIAL_REDDIT_RECENT_SWEEP: process.env.SOCIAL_REDDIT_RECENT_SWEEP || "true",
    SOCIAL_REDDIT_MAX_TICKERS_PER_CYCLE: process.env.SOCIAL_REDDIT_MAX_TICKERS_PER_CYCLE || "8",
  }

  // Pre-load ticker lists first (fast DB queries), then fire ALL scripts in one parallel batch.
  const [trackedMarketTickers, publicSocialTickersRaw, predictionInterestTickers] = await Promise.all([
    fastMode ? Promise.resolve([]) : loadTrackedMarketTickerSymbols(db, Number(process.env.TRACKED_MARKET_TICKER_LIMIT || 5000)),
    socialMode === "top_momentum"
      ? loadTopMomentumTickerSymbols(db, Number(process.env.SOCIAL_MOMENTUM_LIMIT || (fastMode ? 40 : 60)))
      : Promise.resolve([]),
    loadPredictionInterestTickerSymbols(db, Number(process.env.SOCIAL_INTEREST_LIMIT || (fastMode ? 40 : 100))),
  ])

  let publicSocialTickers = normalizeTickerList(
    [...publicSocialTickersRaw, ...predictionInterestTickers],
    Number(process.env.SOCIAL_MAX_TICKERS || (fastMode ? 50 : 120)),
    { ensurePrivate: false },
  )
  let socialTickers = []
  if (socialMode === "top_momentum" && publicSocialTickers.length) {
    socialTickers = withPrivateSocialTickers(publicSocialTickers)
    socialExtraEnv.SOCIAL_TICKERS = socialTickers.join(",")
    socialExtraEnv.SOCIAL_MAX_TICKERS = String(socialTickers.length)
    socialExtraEnv.SOCIAL_PRIVATE_TICKERS = Array.from(PRIVATE_TRACKED_TICKERS).join(",")
    socialExtraEnv.SOCIAL_TICKER_SOURCE = "configured"
    socialExtraEnv.SOCIAL_STRICT_FINVIZ_TOP_MOVERS = "false"
  } else if (socialMode !== "top_momentum") {
    socialExtraEnv.SOCIAL_TICKER_SOURCE = "configured"
    socialExtraEnv.SOCIAL_MAX_TICKERS = process.env.SOCIAL_MAX_TICKERS || "250"
  } else {
    socialExtraEnv.SOCIAL_MAX_TICKERS = "40"
  }

  const tradingViewExtraEnv = publicSocialTickers.length
    ? { TRADINGVIEW_TICKERS: publicSocialTickers.join(","), TRADINGVIEW_MAX_TICKERS: String(publicSocialTickers.length) }
    : {}
  const quoteTickers = fastMode ? publicSocialTickers : trackedMarketTickers
  const quoteExtraEnv = quoteTickers.length
    ? { QUOTE_TICKERS: quoteTickers.join(","), QUOTE_MAX_TICKERS: String(quoteTickers.length) }
    : { QUOTE_MAX_TICKERS: fastMode ? "25" : (process.env.QUOTE_MAX_TICKERS || "5000") }

  // All scripts run in one parallel batch — cuts total time from ~60s to ~30s
  const ohlcvHydrationDue = !fastMode || (Date.now() - lastOhlcvHydrationAt) >= OHLCV_HYDRATION_INTERVAL_MS
  if (ohlcvHydrationDue) lastOhlcvHydrationAt = Date.now()

  const [finvizElite, tradingViewScreener, quotes, structured, tradingView, benzinga, ibkrNews, schwabSignals, unstructured, social, ohlcvHydration] = await Promise.all([
    runPythonScript("2_Screener/pipeline/fetch_finviz_elite_to_mongo.py", {
      timeout: fastMode ? 25000 : 90000,
      extraEnv: { FINVIZ_MAX_WORKERS: process.env.FINVIZ_MAX_WORKERS || (fastMode ? "12" : "6") },
    }),
    fastMode
      ? Promise.resolve(skippedPythonResult("TradingView numeric screener"))
      : runPythonScript("2_Screener/pipeline/fetch_tradingview_screener_to_mongo.py", { timeout: 90000 }),
    runPythonScript("1_News/pipeline/fetch_quotes_to_mongo.py", {
      timeout: fastMode ? 20000 : 90000,
      extraEnv: quoteExtraEnv,
    }),
    runPythonScript("1_News/pipeline/fetch_rss_to_mongo.py", {
      timeout: fastMode ? 22000 : 180000,
      extraEnv: fastMode
        ? { RSS_FAST_MODE: "1", RSS_MAX_WORKERS: process.env.RSS_MAX_WORKERS || "32", RSS_HTTP_TIMEOUT: process.env.RSS_HTTP_TIMEOUT || "5" }
        : { RSS_MAX_WORKERS: process.env.RSS_MAX_WORKERS || "16" },
    }),
    runPythonScript("1_News/pipeline/fetch_tradingview_to_mongo.py", {
      timeout: fastMode ? 20000 : 90000,
      extraEnv: tradingViewExtraEnv,
    }),
    runPythonScript("1_News/pipeline/fetch_benzinga_to_mongo.py", {
      timeout: fastMode ? 25000 : 90000,
    }),
    fastMode
      ? Promise.resolve(skippedPythonResult("IBKR News"))
      : runPythonScript("1_News/pipeline/fetch_ibkr_news_to_mongo.py", { timeout: 30000 }),
    fastMode
      ? Promise.resolve(skippedPythonResult("Schwab signals"))
      : runPythonScript("2_Screener/pipeline/fetch_schwab_signals_to_mongo.py", { timeout: 30000 }),
    runPythonScript("1_News/pipeline/fetch_unstructured_news_titles_to_mongo.py", {
      timeout: fastMode ? 25000 : 90000,
      extraEnv: fastMode
        ? {
            UNSTRUCTURED_SOURCE_FILTER: process.env.FAST_UNSTRUCTURED_SOURCE_FILTER || "Finviz News Flow",
            UNSTRUCTURED_MAX_PER_SOURCE: process.env.FAST_UNSTRUCTURED_MAX_PER_SOURCE || "250",
            UNSTRUCTURED_MAX_WORKERS: "1",
          }
        : {
            UNSTRUCTURED_MAX_PER_SOURCE: process.env.UNSTRUCTURED_MAX_PER_SOURCE || "10",
            ...(trackedMarketTickers.length ? { TRACKED_TICKERS: trackedMarketTickers.join(",") } : {}),
          },
    }),
    runPythonScript("1_News/pipeline/fetch_social_to_mongo.py", {
      timeout: fastMode ? 20000 : 90000,
      extraEnv: socialExtraEnv,
    }),
    ohlcvHydrationDue
      ? hydrateCurrentOhlcvBars(db, publicSocialTickers, {
          limit: Number(process.env.OHLCV_HYDRATION_LIMIT || (fastMode ? 40 : 100)),
          concurrency: Number(process.env.OHLCV_HYDRATION_CONCURRENCY || 6),
        })
      : Promise.resolve({ attempted_tickers: 0, fetched_tickers: 0, fetched_bars: 0, persisted_bars: 0, errors: [], skipped: true }),
  ])

  const afterStructuredArticles = await db.collection("articles").countDocuments()
  const structuredCounts = parseStructuredFetch(structured.stdout || "", beforeArticles, afterStructuredArticles)
  const afterArticles = await db.collection("articles").countDocuments()
  const afterSocial = await db.collection("socials").countDocuments()
  const unstructuredCounts = parseUnstructuredFetch(unstructured.stdout || "")
  const socialCounts = parseSocialFetch(social.stdout || "")
  const quoteCounts = parseQuoteFetch(quotes.stdout || "")
  const finvizCounts = parseFinvizEliteFetch(finvizElite.stdout || "")
  const tradingViewCounts = parseTradingViewFetch(tradingView.stdout || "")
  const tradingViewScreenerCounts = parseTradingViewScreenerFetch(tradingViewScreener.stdout || "")
  const benzingaCounts = parseBenzingaFetch(benzinga.stdout || "")
  const benzingaDebug = {
    ...parseSourceDebug(benzinga.stdout || "", ["Benzinga"]),
    attempted: true,
  }
  if (!benzingaDebug.errors.length && /BENZINGA_API_KEY not set|Benzinga import skipped/i.test(benzinga.stdout || "")) {
    if (benzingaDebug.ok && Number(benzingaDebug.fetched || 0) > 0) {
      benzingaDebug.warnings = Array.from(new Set([...(benzingaDebug.warnings || []), "Benzinga API key missing; used public recent fallback"]))
    } else {
      benzingaDebug.errors = ["Benzinga API key missing"]
    }
  }
  const businessWireDebug = {
    ...parseSourceDebug(structured.stdout || "", ["Business Wire", "BusinessWire"]),
    attempted: true,
  }
  const predictionMaintenanceDue = !fastMode || (Date.now() - lastPredictionMaintenanceAt) >= PREDICTION_MAINTENANCE_INTERVAL_MS
  if (predictionMaintenanceDue) lastPredictionMaintenanceAt = Date.now()
  const thresholdFeatures = predictionMaintenanceDue
    ? await runNodeScript("scripts/update_prediction_threshold_features.js", {
        timeout: fastMode ? 60000 : 180000,
        args: [
          "--maxTickers", process.env.THRESHOLD_FEATURE_MAX_TICKERS || (fastMode ? "800" : "1600"),
          "--windowMinutes", process.env.THRESHOLD_FEATURE_WINDOW_MINUTES || "120",
          "--minObservations", process.env.THRESHOLD_FEATURE_MIN_OBSERVATIONS || "8",
          "--freshMinutes", process.env.THRESHOLD_FEATURE_FRESH_MINUTES || (fastMode ? "720" : "1440"),
          "--chartConcurrency", process.env.THRESHOLD_FEATURE_CHART_CONCURRENCY || (fastMode ? "4" : "6"),
          "--liveCharts", process.env.THRESHOLD_FEATURE_LIVE_CHARTS || (fastMode ? "0" : "1"),
        ],
      })
    : skippedPythonResult("Prediction threshold maintenance", "five-minute maintenance cadence")
  const predictionLabels = predictionMaintenanceDue
    ? await labelMaturePredictionSignals(db)
    : { checked: 0, labeled: 0, skipped: true }
  const predictionModel = predictionMaintenanceDue && Number(predictionLabels.labeled || 0) > 0
    ? await trainPredictionModel(db, {
        minSamples: Number(process.env.PREDICTION_MIN_TRAINING_SAMPLES || 20),
        limit: Number(process.env.PREDICTION_TRAIN_LIMIT || 3000),
      })
    : await loadLatestPredictionModel(db)
  const predictionSnapshot = await captureTradeWatchPredictionSignals(db, {
    limit: Number(process.env.PREDICTION_SIGNAL_LIMIT || 10),
    socialWindow: Number(process.env.PREDICTION_SOCIAL_WINDOW || 60),
  })

  return {
    ok: finvizElite.ok && tradingViewScreener.ok && quotes.ok && structured.ok && tradingView.ok && benzinga.ok && ibkrNews.ok && schwabSignals.ok && unstructured.ok && social.ok,
    ...finvizCounts,
    ...tradingViewScreenerCounts,
    ...quoteCounts,
    ...structuredCounts,
    ...tradingViewCounts,
    ...benzingaCounts,
    benzinga: benzingaDebug,
    businessWire: businessWireDebug,
    ...unstructuredCounts,
    ...socialCounts,
    total_articles: afterArticles,
    total_social: afterSocial,
    tracked_market_ticker_count: trackedMarketTickers.length,
    quote_ticker_count: quoteTickers.length,
    fetch_mode: refreshMode,
    social_delta: Math.max(0, afterSocial - beforeSocial),
    prediction_labels_checked: predictionLabels.checked,
    prediction_labels_added: predictionLabels.labeled,
    prediction_maintenance_ran: predictionMaintenanceDue,
    ohlcv_hydration_ran: ohlcvHydrationDue,
    ohlcv_hydration: ohlcvHydration,
    prediction_signals_saved: predictionSnapshot.saved,
    prediction_model_status: predictionModel.status,
    prediction_model_samples: predictionModel.samples || 0,
    threshold_features_ok: thresholdFeatures.ok,
    threshold_features_error: thresholdFeatures.ok ? "" : thresholdFeatures.error,
    social_mode: socialMode,
    social_target_source: socialMode === "top_momentum" ? "top positive momentum movers" : "configured watchlist",
    social_tickers: socialTickers,
    social_interest_tickers: predictionInterestTickers,
    timings: {
      finviz_ms: finvizElite.ms || 0,
      tradingview_screener_ms: tradingViewScreener.ms || 0,
      quotes_ms: quotes.ms || 0,
      structured_ms: structured.ms || 0,
      tradingview_news_ms: tradingView.ms || 0,
      benzinga_ms: benzinga.ms || 0,
      ibkr_ms: ibkrNews.ms || 0,
      schwab_ms: schwabSignals.ms || 0,
      unstructured_ms: unstructured.ms || 0,
      social_ms: social.ms || 0,
      threshold_features_ms: thresholdFeatures.ms || 0,
    },
    output: [
      structured.stdout,
      finvizElite.stdout,
      tradingViewScreener.stdout,
      tradingView.stdout,
      benzinga.stdout,
      ibkrNews.stdout,
      schwabSignals.stdout,
      unstructured.stdout,
      social.stdout,
      quotes.stdout,
      thresholdFeatures.stdout,
    ].filter(Boolean).join("\n").slice(-6000),
    stderr: [
      structured.stderr,
      finvizElite.stderr,
      tradingViewScreener.stderr,
      tradingView.stderr,
      benzinga.stderr,
      ibkrNews.stderr,
      schwabSignals.stderr,
      unstructured.stderr,
      social.stderr,
      quotes.stderr,
      thresholdFeatures.stderr,
    ].filter(Boolean).join("\n").slice(-3000),
    errors: [
      finvizElite.ok ? null : finvizElite.error,
      tradingViewScreener.ok ? null : tradingViewScreener.error,
      quotes.ok ? null : quotes.error,
      structured.ok ? null : structured.error,
      tradingView.ok ? null : tradingView.error,
      benzinga.ok ? null : benzinga.error,
      ibkrNews.ok ? null : ibkrNews.error,
      schwabSignals.ok ? null : schwabSignals.error,
      unstructured.ok ? null : unstructured.error,
      social.ok ? null : social.error,
    ].filter(Boolean),
  }
}

async function handleApiFetch(req, res) {
  const started = Date.now()

  // Skip duplicate/overlapping cycles instead of stacking expensive work.
  if (refreshCycleInFlight) {
    return res.json({ ok: true, skipped: true, already_running: true, new_articles: 0, updated_articles: 0,
      ms: Date.now() - started, message: "A refresh is already in progress — skipped the duplicate." })
  }

  try {
    const db = mongoose.connection.db
    if (!db) {
      return res.status(503).json({
        ok: false,
        error: "MongoDB is not connected",
        new_articles: 0,
        ms: Date.now() - started,
      })
    }

    refreshCycleInFlight = true
    const result = await runDataRefreshCycle(db, {
      mode: req.query.mode || req.body?.mode || process.env.DEFAULT_FETCH_MODE || "fast",
    })
    persistFetchNewsToDisk(db).catch(() => {})   // Redis+Kafka fetch → hard disk (3d)
    return res.json({
      ...result,
      ms: Date.now() - started,
      message: result.fetch_mode === "full"
        ? "Ran full structured, unstructured, and social importers"
        : "Ran fast trader refresh",
    })
  } catch (err) {
    console.error("Real /api/fetch failed:", err)
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
      new_articles: 0,
      ms: Date.now() - started,
      stdout: String(err?.stdout || "").slice(-3000),
      stderr: String(err?.stderr || "").slice(-3000),
    })
  } finally {
    refreshCycleInFlight = false
  }
}

app.post("/api/fetch", handleApiFetch)
app.get("/api/fetch", handleApiFetch)
// NEWS_RSS_FETCH_API_V3_END

app.get("/api/watch", async (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");

  const interval = 60;

  res.write(`event: start\n`);
  res.write(`data: ${JSON.stringify({ message: `Auto-watch started. Interval: ${interval}s. Social auto-fetch targets the top 10 positive momentum movers.` })}\n\n`);

  let isRunning = false;

  const runFetchCycle = async () => {
    if (isRunning || refreshCycleInFlight) return; // don't overlap with Run Now or another cycle
    isRunning = true;
    refreshCycleInFlight = true;
    
    const cycleStarted = Date.now();
    try {
      const db = mongoose.connection.db;
      const result = await runDataRefreshCycle(db, {
        socialMode: "top_momentum",
        mode: req.query.mode || "fast",
      })
      persistFetchNewsToDisk(db).catch(() => {})   // auto-watch fetch → hard disk (3d)
      const newCount = Number(result.new_articles || 0) + Number(result.unstructured_new || 0)
      const updatedCount = Number(result.updated_articles || 0) + Number(result.unstructured_updated || 0)
      const tradingViewNew = Number(result.tradingview_new || 0)
      const tradingViewUpdated = Number(result.tradingview_updated || 0)
      const socialNew = Number(result.social_new || 0)
      const socialUpdated = Number(result.social_updated || 0)
      const quotesUpdated = Number(result.quotes_updated || 0)
      const trackedMarketTickerCount = Number(result.tracked_market_ticker_count || 0)
      const finvizRows = Number(result.finviz_rows || 0)
      const tradingViewScreenerRows = Number(result.tradingview_screener_rows || 0)
      const ms = Date.now() - cycleStarted;

      res.write(`event: line\n`);
      res.write(`data: ${JSON.stringify({ 
        text: `${finvizRows} Finviz movers; ${tradingViewScreenerRows} TV scanner rows; ${trackedMarketTickerCount || 'all'} tracked market tickers; ${quotesUpdated} quotes; +${newCount} articles${updatedCount > 0 ? `, ${updatedCount} refreshed` : ''}; +${tradingViewNew} TradingView news${tradingViewUpdated > 0 ? `, ${tradingViewUpdated} refreshed` : ''}; +${socialNew} social${socialUpdated > 0 ? `, ${socialUpdated} refreshed` : ''}${result.social_tickers?.length ? ` [${result.social_tickers.join(', ')}]` : ''} (${(ms / 1000).toFixed(1)}s)`,
        new: newCount + tradingViewNew,
        updated: updatedCount + tradingViewUpdated,
        tradingview_new: tradingViewNew,
        tradingview_updated: tradingViewUpdated,
        social_new: socialNew,
        social_updated: socialUpdated,
        social_tickers: result.social_tickers || [],
        finviz_rows: finvizRows,
        tradingview_screener_rows: tradingViewScreenerRows,
        tracked_market_ticker_count: trackedMarketTickerCount,
        quotes_updated: quotesUpdated,
        ms: ms
      })}\n\n`);
    } catch (err) {
      console.error("Auto-watch cycle failed:", err);
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ message: `Auto-watch cycle failed: ${err.message}` })}\n\n`);
    } finally {
      isRunning = false;
      refreshCycleInFlight = false;
    }
  };

  // Run first cycle immediately, then schedule for every interval
  await runFetchCycle();
  
  const timer = setInterval(runFetchCycle, interval * 1000);

  req.on("close", () => {
    clearInterval(timer);
  });
});
// End Ryan frontend compatibility endpoints



app.get("/api/sources/health", async (req, res) => {
  try {
    const db = mongoose.connection.db;
    const registry = readConfigJson("professor_source_registry.json")
    const statuses = await db.collection("source_status").find({}).toArray()
    const statusBySource = new Map(statuses.map((row) => [row.source, row]))

    const sourceAliases = {
      "TradingView News Flow": ["TradingView News Flow", "TradingView"],
      "TradingView News": ["TradingView News", "TradingView"],
      "GlobeNewswire Public Companies": ["GlobeNewswire Public Companies", "GlobeNewswire"],
      "ACCESS Newswire": ["ACCESS Newswire", "AccessWire"],
      "BusinessWire": ["BusinessWire", "Business Wire"],
      "Schwab News": ["Schwab News", "Charles Schwab", "TD Ameritrade"],
      "X/Twitter": ["X/Twitter", "Twitter", "X"],
    }
    const screenerSources = {
      "Finviz Elite Screener": { quote_source: "finviz_elite_screener" },
      "TradingView Numeric Screener": { quote_source: "tradingview_numeric_screener" },
      "Schwab Movers": { source: "Schwab Movers" },
    }
    function liveStatusForSource(source) {
      const aliases = sourceAliases[source] || [source]
      let best = null
      for (const alias of aliases) {
        const row = statusBySource.get(alias)
        if (!row) continue
        const rowTime = new Date(row.last_checked_at || 0).getTime()
        const bestTime = new Date(best?.last_checked_at || 0).getTime()
        if (!best || rowTime >= bestTime) best = row
      }
      return best || statusBySource.get(source)
    }

    async function countSource(entry) {
      const aliases = sourceAliases[entry.source] || [entry.source]
      if (entry.collection === "articles") {
        const pattern = aliases.map((s) => escapeRegExp(s)).join("|")
        const query = { source: { $regex: pattern, $options: "i" } }
        const [count, latest] = await Promise.all([
          db.collection("articles").countDocuments(query),
          db.collection("articles").find(query).sort({ fetched_date: -1, detected_at: -1, publish_date: -1 }).limit(1).project({ fetched_date: 1, detected_at: 1, publish_date: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.fetched_date || latest?.detected_at || null,
          latest_publish: latest?.publish_date || null,
        }
      }
      if (entry.collection === "screeners") {
        const query = screenerSources[entry.source] || { source: entry.source }
        const [count, latest] = await Promise.all([
          db.collection("screeners").countDocuments(query),
          db.collection("screeners").find(query).sort({ quote_updated_at: -1, finviz_seen_at: -1, tradingview_seen_at: -1 }).limit(1).project({ quote_updated_at: 1, finviz_seen_at: 1, tradingview_seen_at: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.quote_updated_at || latest?.finviz_seen_at || latest?.tradingview_seen_at || null,
          latest_publish: null,
        }
      }
      if (entry.collection === "socials") {
        const aliasesLower = aliases.map((s) => s.toLowerCase())
        const query = {
          $or: [
            { platform: { $in: aliasesLower } },
            { platform: { $in: aliases } },
            { source: { $in: aliases } },
          ],
        }
        const [count, latest] = await Promise.all([
          db.collection("socials").countDocuments(query),
          db.collection("socials").find(query).sort({ fetched_date: -1, detected_at: -1, createdAt: -1 }).limit(1).project({ fetched_date: 1, detected_at: 1, createdAt: 1 }).next(),
        ])
        return {
          count,
          latest_fetch: latest?.fetched_date || latest?.detected_at || latest?.createdAt || null,
          latest_publish: null,
        }
      }
      return { count: 0, latest_fetch: null, latest_publish: null }
    }

    const sources = []
    for (const entry of registry) {
      const counted = await countSource(entry)
      const liveStatus = liveStatusForSource(entry.source)
      const envValue = entry.env_var ? String(process.env[entry.env_var] || "").trim() : ""
      const hasRequiredEnv = !entry.env_var || (Boolean(envValue) && !["0", "false", "no"].includes(envValue.toLowerCase()))
      const requiresMissingCredential = Boolean(entry.auth_required && entry.env_var && !hasRequiredEnv)
      let status = liveStatus?.status || entry.status || "unknown"
      if (requiresMissingCredential && counted.count === 0 && !["broker_api_pending", "licensed_feed_required", "planned"].includes(entry.status)) {
        status = "api_key_required"
      } else if (!liveStatus && counted.count === 0 && String(status).startsWith("working")) {
        status = "ready_no_rows_yet"
      } else if (counted.count > 0 && !["planned", "licensed_feed_required", "broker_api_pending"].includes(status)) {
        status = status.startsWith("working") ? status : "working"
      }

      sources.push({
        ...entry,
        status,
        configured: hasRequiredEnv,
        count: counted.count,
        latest_fetch: counted.latest_fetch,
        latest_publish: counted.latest_publish,
        last_checked_at: liveStatus?.last_checked_at || null,
        detail: liveStatus?.detail || "",
      })
    }

    const working = sources.filter((row) => row.count > 0 || (String(row.status).startsWith("working") && row.last_checked_at))
    const ready = sources.filter((row) => row.status === "ready_no_rows_yet")
    const blocked = sources.filter((row) => row.count === 0 && !String(row.status).startsWith("working") && row.status !== "planned" && row.status !== "ready_no_rows_yet")

    res.json({
      working_count: working.length,
      ready_count: ready.length,
      blocked_count: blocked.length,
      planned_count: sources.filter((row) => row.status === "planned").length,
      sources,
    });
  } catch (err) {
    res.status(500).json({ error: "Failed to load source health", detail: err.message });
  }
});


// FEEDFLASH_SETTINGS_KEYWORDS_SOURCES_PATCH_V1

function settingsDb() {
  const d = mongoose.connection.db
  if (!d) throw new Error('MongoDB connection is not ready')
  return d
}

const DEFAULT_SIGNAL_KEYWORDS = [
  ["earnings", "fundamental"],
  ["ipo", "fundamental"],
  ["listing", "fundamental"],
  ["delisting", "fundamental"],
  ["dividend", "fundamental"],
  ["merger", "fundamental"],
  ["acquisition", "fundamental"],
  ["buyout", "fundamental"],
  ["contract", "fundamental"],
  ["partnership", "fundamental"],
  ["fda approval", "regulatory"],
  ["fda rejection", "regulatory"],
  ["clinical trial", "regulatory"],
  ["sec filing", "regulatory"],
  ["short squeeze", "momentum"],
  ["price target", "analyst"],
  ["downgrade", "analyst"],
  ["upgrade", "analyst"],
  ["beat estimates", "fundamental"],
  ["miss estimates", "fundamental"],
  ["guidance", "fundamental"],
  ["recall", "regulatory"],
  ["bankruptcy", "fundamental"],
  ["layoffs", "fundamental"],
  ["restructuring", "fundamental"]
];

async function seedDefaultKeywordsIfEmpty() {
  const keywords = settingsDb().collection("keywords");
  const count = await keywords.countDocuments();
  if (count > 0) return;

  await keywords.insertMany(DEFAULT_SIGNAL_KEYWORDS.map(([keyword, category]) => ({
    keyword,
    word: keyword,
    category,
    enabled: true,
    active: true,
    hits: 0,
    created_at: Math.floor(Date.now() / 1000),
    updated_at: Math.floor(Date.now() / 1000)
  })));
}

function cleanSettingText(v) {
  return String(v || "").trim();
}

function cleanKeyword(v) {
  return cleanSettingText(v).toLowerCase();
}

const DEFAULT_CONNECTION_SETTINGS = {
  finviz: {
    label: "Finviz Elite",
    url: process.env.FINVIZ_URL || "https://elite.finviz.com/screener",
    token: process.env.FINVIZ_TOKEN || "",
    login: "",
  },
  tradingview: {
    label: "TradingView",
    url: process.env.TRADINGVIEW_URL || "https://www.tradingview.com",
    token: process.env.TRADINGVIEW_TOKEN || "",
    login: process.env.TRADINGVIEW_LOGIN || "",
  },
  td_ameritrade: {
    label: "TD Ameritrade / Schwab",
    url: process.env.TD_URL || process.env.SCHWAB_URL || "",
    token: process.env.TD_TOKEN || process.env.SCHWAB_TOKEN || "",
    login: process.env.TD_LOGIN || process.env.SCHWAB_LOGIN || "",
  },
  interactive_brokers: {
    label: "Interactive Brokers",
    url: process.env.IB_URL || "",
    token: process.env.IB_TOKEN || "",
    login: process.env.IB_LOGIN || "",
  },
};

function cleanConnectionPayload(value = {}) {
  const out = {};
  for (const [key, defaults] of Object.entries(DEFAULT_CONNECTION_SETTINGS)) {
    const row = value[key] || {};
    out[key] = {
      label: defaults.label,
      url: cleanSettingText(row.url ?? defaults.url),
      token: cleanSettingText(row.token ?? defaults.token),
      login: cleanSettingText(row.login ?? defaults.login),
    };
  }
  return out;
}

app.get("/api/settings/connections", async (req, res) => {
  try {
    const row = await settingsDb().collection("app_settings").findOne({ key: "connections" });
    res.json({
      ok: true,
      connections: cleanConnectionPayload(row?.value || {}),
    });
  } catch (err) {
    console.error("GET /api/settings/connections failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/settings/connections", async (req, res) => {
  try {
    const connections = cleanConnectionPayload(req.body.connections || req.body || {});
    await settingsDb().collection("app_settings").updateOne(
      { key: "connections" },
      {
        $set: {
          key: "connections",
          value: connections,
          updated_at: Math.floor(Date.now() / 1000),
        },
        $setOnInsert: { created_at: Math.floor(Date.now() / 1000) },
      },
      { upsert: true }
    );
    res.json({ ok: true, connections });
  } catch (err) {
    console.error("PATCH /api/settings/connections failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.get("/api/keywords", async (req, res) => {
  try {
    await seedDefaultKeywordsIfEmpty();
    const rows = await settingsDb().collection("keywords")
      .find({})
      .sort({ enabled: -1, category: 1, keyword: 1, word: 1 })
      .toArray();

    res.json({
      ok: true,
      keywords: rows.map(r => ({
        id: String(r._id),
        keyword: r.keyword || r.word,
        word: r.word || r.keyword,
        category: r.category || "custom",
        enabled: r.enabled !== false && r.active !== false,
        active: r.enabled !== false && r.active !== false,
        hits: r.hits || 0
      }))
    });
  } catch (err) {
    console.error("GET /api/keywords failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/keywords", async (req, res) => {
  try {
    const keyword = cleanKeyword(req.body.keyword || req.body.word);
    const category = cleanSettingText(req.body.category || "custom").toLowerCase();

    if (!keyword) return res.status(400).json({ ok: false, error: "keyword is required" });

    const now = Math.floor(Date.now() / 1000);
    await settingsDb().collection("keywords").updateOne(
      { keyword },
      {
        $set: { keyword, word: keyword, category, enabled: true, active: true, updated_at: now },
        $setOnInsert: { hits: 0, created_at: now }
      },
      { upsert: true }
    );

    res.json({ ok: true, keyword, category });
  } catch (err) {
    console.error("POST /api/keywords failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/keywords/:keyword", async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword));
    const enabled = req.body.enabled !== false && req.body.active !== false;
    const result = await settingsDb().collection("keywords").updateOne(
      { $or: [{ keyword }, { word: keyword }] },
      { $set: { enabled, active: enabled, updated_at: Math.floor(Date.now() / 1000) } }
    );
    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount });
  } catch (err) {
    console.error("PATCH /api/keywords/:keyword failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.delete("/api/keywords/:keyword", async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword));
    const result = await settingsDb().collection("keywords").deleteOne({ $or: [{ keyword }, { word: keyword }] });
    res.json({ ok: true, deleted: result.deletedCount });
  } catch (err) {
    console.error("DELETE /api/keywords/:keyword failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

const PROFESSOR_STRUCTURED_SOURCES = [
  { source: "PR Newswire", status: "public_feed", method: "rss", editable: false },
  { source: "GlobeNewswire", status: "public_feed", method: "rss", editable: false },
  { source: "SEC EDGAR", status: "public_api", method: "official_sec_atom", editable: false },
  { source: "FDA", status: "public_feed", method: "official_fda_rss", editable: false },
  { source: "Business Wire", status: "valid_rss_channel_required", method: "official_businesswire_rss_or_media_partner_feed", editable: false },
  { source: "ACCESS Newswire / AccessWire", status: "public_endpoint", method: "accessnewswire_newsroom_json", editable: false },
  { source: "Benzinga", status: "api_key_required", method: "official_benzinga_stock_news_api", editable: false },
  { source: "Dow Jones Newswires", status: "contract_required", method: "licensed_api", editable: false },
  { source: "TradingView News Flow", status: "public_endpoint", method: "news_mediator_symbol_endpoint", editable: false },
  { source: "Interactive Brokers News", status: "broker_api_required", method: "broker_api", editable: false },
  { source: "Charles Schwab / TD Ameritrade News", status: "broker_api_required", method: "broker_api", editable: false }
];

async function countArticlesForSourceLabel(label) {
  const parts = label.split("/").map(s => s.trim()).filter(Boolean);
  const pattern = parts.length ? parts.join("|") : label;
  return settingsDb().collection("articles").countDocuments({ source: new RegExp(pattern, "i") });
}

app.get("/api/settings/sources", async (req, res) => {
  try {
    const db = settingsDb()
    const [custom, favDocs] = await Promise.all([
      db.collection("rss_sources").find({}).sort({ enabled: -1, name: 1 }).toArray(),
      db.collection("source_favorites").find({}).toArray(),
    ])
    const favSet = new Set(favDocs.map(f => f.name))

    const structured = [];
    for (const s of PROFESSOR_STRUCTURED_SOURCES) {
      structured.push({
        ...s,
        is_favorite: favSet.has(s.source),
        count: await countArticlesForSourceLabel(s.source)
      });
    }

    res.json({
      ok: true,
      structured,
      favorites: Array.from(favSet),
      custom_rss_sources: custom.map(s => ({
        id: String(s._id),
        name: s.name,
        source: s.name,
        url: s.url,
        category: s.category || "custom",
        enabled: s.enabled !== false,
        is_favorite: favSet.has(s.name),
        status: s.enabled === false ? "disabled" : "enabled",
        editable: true
      }))
    });
  } catch (err) {
    console.error("GET /api/settings/sources failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/settings/sources/:name/favorite", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name))
    await settingsDb().collection("source_favorites").updateOne(
      { name },
      { $set: { name, favorited_at: Math.floor(Date.now() / 1000) } },
      { upsert: true }
    )
    res.json({ ok: true, name, favorited: true })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.delete("/api/settings/sources/:name/favorite", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name))
    await settingsDb().collection("source_favorites").deleteOne({ name })
    res.json({ ok: true, name, favorited: false })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.get("/api/settings/sources/favorites", async (req, res) => {
  try {
    const docs = await settingsDb().collection("source_favorites").find({}).sort({ favorited_at: -1 }).toArray()
    res.json({ ok: true, favorites: docs.map(d => d.name) })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post("/api/settings/sources", async (req, res) => {
  try {
    const name = cleanSettingText(req.body.name || req.body.source);
    const url = cleanSettingText(req.body.url);
    const category = cleanSettingText(req.body.category || "custom").toLowerCase();

    if (!name || !url) return res.status(400).json({ ok: false, error: "name and url are required" });
    if (!/^https?:\/\//i.test(url)) return res.status(400).json({ ok: false, error: "url must start with http:// or https://" });

    const now = Math.floor(Date.now() / 1000);
    await settingsDb().collection("rss_sources").updateOne(
      { name },
      {
        $set: { name, url, category, enabled: true, updated_at: now },
        $setOnInsert: { created_at: now }
      },
      { upsert: true }
    );

    res.json({ ok: true, name, url, category });
  } catch (err) {
    console.error("POST /api/settings/sources failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.patch("/api/settings/sources/:name", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name));
    const enabled = req.body.enabled !== false;
    const result = await settingsDb().collection("rss_sources").updateOne(
      { name },
      { $set: { enabled, updated_at: Math.floor(Date.now() / 1000) } }
    );
    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount });
  } catch (err) {
    console.error("PATCH /api/settings/sources/:name failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.delete("/api/settings/sources/:name", async (req, res) => {
  try {
    const name = cleanSettingText(decodeURIComponent(req.params.name));
    const result = await settingsDb().collection("rss_sources").deleteOne({ name });
    res.json({ ok: true, deleted: result.deletedCount });
  } catch (err) {
    console.error("DELETE /api/settings/sources/:name failed:", err);
    res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});


// FEEDFLASH_SETTINGS_KEYWORDS_ALIAS_PATCH_V1
app.get('/api/settings/keywords', async (req, res) => {
  try {
    await seedDefaultKeywordsIfEmpty()

    const rows = await settingsDb().collection('keywords')
      .find({})
      .sort({ enabled: -1, category: 1, keyword: 1, word: 1 })
      .toArray()

    res.json({
      ok: true,
      keywords: rows.map(r => ({
        id: String(r._id),
        keyword: r.keyword || r.word,
        word: r.word || r.keyword,
        category: r.category || 'custom',
        enabled: r.enabled !== false && r.active !== false,
        active: r.enabled !== false && r.active !== false,
        hits: r.hits || 0
      }))
    })
  } catch (err) {
    console.error('GET /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.post('/api/settings/keywords', async (req, res) => {
  try {
    const keyword = cleanKeyword(req.body?.keyword || req.body?.word)
    const category = cleanSettingText(req.body?.category || 'custom').toLowerCase()

    if (!keyword) return res.status(400).json({ ok: false, error: 'keyword is required' })

    const now = Math.floor(Date.now() / 1000)
    await settingsDb().collection('keywords').updateOne(
      { keyword },
      {
        $set: { keyword, word: keyword, category, enabled: true, active: true, updated_at: now },
        $setOnInsert: { hits: 0, created_at: now }
      },
      { upsert: true }
    )

    res.json({ ok: true, keyword, category })
  } catch (err) {
    console.error('POST /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.patch('/api/settings/keywords/:keyword', async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword))
    const enabled = req.body?.enabled !== false && req.body?.active !== false

    const result = await settingsDb().collection('keywords').updateOne(
      { $or: [{ keyword }, { word: keyword }] },
      { $set: { enabled, active: enabled, updated_at: Math.floor(Date.now() / 1000) } }
    )

    res.json({ ok: true, matched: result.matchedCount, modified: result.modifiedCount })
  } catch (err) {
    console.error('PATCH /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

app.delete('/api/settings/keywords/:keyword', async (req, res) => {
  try {
    const keyword = cleanKeyword(decodeURIComponent(req.params.keyword))

    const result = await settingsDb().collection('keywords').deleteOne({
      $or: [{ keyword }, { word: keyword }]
    })

    res.json({ ok: true, deleted: result.deletedCount })
  } catch (err) {
    console.error('DELETE /api/settings/keywords failed:', err)
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

  // ═══════════════════════════════════════════════════════════════════════════
  //  HARD-DISK DATABASE — on-disk persistence + retention + auto-grab
  //  Companion to the RAM layer (Redis). See diskdb.js. Buckets:
  //    manual (3d) · auto (2d, away-mode) · fetch (3d, Redis+Kafka path)
  // ═══════════════════════════════════════════════════════════════════════════
  await diskdb.init()
  const diskFetchDays = diskdb.stats().retention_days.fetch
  const diskAutoDays  = diskdb.stats().retention_days.auto

  // Shape the latest Mongo news for the disk store.
  async function collectRecentNewsFromMongo(db, days = 3, limit = 5000) {
    if (!db) return []
    // publish_date is stored as Unix seconds (integer) — compare as number, not Date
    const cutoffSec = Math.floor((Date.now() - Math.max(1, days) * 86_400_000) / 1000)
    const projection = { title: 1, source: 1, url: 1, publish_date: 1, fetched_date: 1, sentiment: 1, ml_confidence: 1, ticker: 1, content: 1 }
    let docs = []
    try {
      docs = await db.collection('articles').find({
        $or: [
          { publish_date: { $gte: cutoffSec } },
          { fetched_date: { $gte: cutoffSec } },
        ]
      }, { projection })
        .sort({ publish_date: -1 }).limit(Math.max(1, Math.min(20000, limit))).toArray()
    } catch (_) {
      try { docs = await db.collection('articles').find({}, { projection }).sort({ _id: -1 }).limit(2000).toArray() }
      catch (__) { docs = [] }
    }
    return docs.map(d => {
      const when = d.publish_date || d.fetched_date
      const sec = when ? Math.floor(new Date(when).getTime() / 1000) : null
      const score = d.sentiment === 'bullish' ? (d.ml_confidence ?? 0.5) : d.sentiment === 'bearish' ? -(d.ml_confidence ?? 0.5) : 0
      return {
        ticker: articlePrimaryTicker(d) || String(d.ticker || '').split(',')[0] || '',
        title: d.title || '',
        source: d.source || '',
        url: d.url && d.url !== '#' ? d.url : '',
        summary: String(d.content || '').slice(0, 400),
        sentiment: d.sentiment || 'neutral',
        sentiment_score: Number(Number(score).toFixed(3)),
        published_at: sec,
      }
    })
  }

  // Redis+Kafka fetch path → hard disk (bucket 'fetch', 3-day retention).
  async function persistFetchNewsToDisk(db) {
    if (!diskdb.isEnabled()) return { stored: 0 }
    try { return diskdb.storeNews(await collectRecentNewsFromMongo(db, diskFetchDays, 5000), 'fetch') }
    catch (e) { console.warn('persistFetchNewsToDisk error:', e.message); return { stored: 0 } }
  }

  // ── Presence: the frontend pings while open; absence ⇒ auto-grabber archives.
  let lastPresenceAt = 0
  const PRESENCE_TIMEOUT_MS = Number(process.env.PRESENCE_TIMEOUT_MS || 90_000)
  const siteOpen = () => (Date.now() - lastPresenceAt) < PRESENCE_TIMEOUT_MS
  // Reassigned below once the on-site auto-fetch is set up; the ping triggers a check.
  let triggerOnSiteAutoFetch = () => {}
  app.post('/api/presence/ping', (req, res) => {
    lastPresenceAt = Date.now()
    res.json({ ok: true, last_presence_at: lastPresenceAt, site_open: true })
    // While someone is on the site, grab fresh news on arrival and then every interval.
    try { triggerOnSiteAutoFetch() } catch (_) {}
  })

  // ── Disk DB REST API ───────────────────────────────────────────────────────
  // Save the last N days of news to disk (manual button + on-exit beacon).
  app.post('/api/disk/save-news', async (req, res) => {
    const db = mongoose.connection.db
    const days = Math.max(1, Math.min(30, Number(req.query.days || req.body?.days || 3)))
    if (!diskdb.isEnabled()) return res.status(503).json({ ok: false, error: 'Hard-disk database is not available', saved: 0 })
    try {
      const r = diskdb.storeNews(await collectRecentNewsFromMongo(db, days, 5000), 'manual')
      res.json({ ok: true, saved: r.stored, bucket: 'manual', days, retention_days: diskdb.stats().retention_days.manual })
    } catch (e) { res.status(500).json({ ok: false, error: String(e.message || e), saved: 0 }) }
  })

  app.get('/api/disk/news', (req, res) => {
    const news = diskdb.listNews({ bucket: req.query.bucket, ticker: req.query.ticker, limit: req.query.limit })
    res.json({ ok: true, count: news.length, news })
  })

  app.get('/api/disk/stats', (req, res) => {
    res.json({
      ...diskdb.stats(),
      presence: { site_open: siteOpen(), last_presence_at: lastPresenceAt || null },
      auto_fetch: {
        onsite_enabled: ONSITE_FETCH_ENABLED,
        onsite_interval_min: Math.round(ONSITE_FETCH_INTERVAL_MS / 60000),
        onsite_last_at: lastOnSiteFetchAt,
        onsite_retention_days: diskFetchDays,
        away_enabled: AUTO_GRAB_ENABLED,
        away_interval_min: Math.round(AUTO_GRAB_INTERVAL_MS / 60000),
        away_retention_days: diskAutoDays,
      },
    })
  })

  // ── Redis (RAM) stats ─────────────────────────────────────────────────────
  app.get('/api/redis/stats', async (req, res) => {
    if (!redisReady()) {
      return res.json({ available: false, error: 'Redis not connected' })
    }
    try {
      const raw = await redis.info()
      const parse = (key) => {
        const m = raw.match(new RegExp(`^${key}:(.+)$`, 'm'))
        return m ? m[1].trim() : null
      }
      const usedMem   = Number(parse('used_memory') || 0)
      const peakMem   = Number(parse('used_memory_peak') || 0)
      const maxMem    = Number(parse('maxmemory') || 0)
      const keyspaceHits   = Number(parse('keyspace_hits') || 0)
      const keyspaceMisses = Number(parse('keyspace_misses') || 0)
      const totalCmds = Number(parse('total_commands_processed') || 0)
      const hitRate   = (keyspaceHits + keyspaceMisses) > 0
        ? Math.round(keyspaceHits / (keyspaceHits + keyspaceMisses) * 100)
        : null
      // Count total keys across all DBs
      const dbSection = raw.match(/^db\d+:keys=(\d+)/mg) || []
      const totalKeys = dbSection.reduce((s, l) => s + Number(l.match(/keys=(\d+)/)[1]), 0)
      const uptimeSecs = Number(parse('uptime_in_seconds') || 0)
      res.json({
        available: true,
        mode: 'RAM-only (no disk persistence)',
        policy: parse('maxmemory_policy') || 'allkeys-lru',
        used_memory_bytes: usedMem,
        peak_memory_bytes: peakMem,
        max_memory_bytes: maxMem,
        used_pct: maxMem > 0 ? Math.round(usedMem / maxMem * 100) : null,
        total_keys: totalKeys,
        keyspace_hits: keyspaceHits,
        keyspace_misses: keyspaceMisses,
        hit_rate_pct: hitRate,
        total_commands: totalCmds,
        uptime_seconds: uptimeSecs,
        version: parse('redis_version'),
        connected_clients: Number(parse('connected_clients') || 0),
      })
    } catch (e) {
      res.json({ available: false, error: e.message })
    }
  })

  // Download saved news as a real local JSON file ("locally save").
  app.get('/api/disk/export', (req, res) => {
    const days = Math.max(1, Math.min(30, Number(req.query.days || 3)))
    const bucket = req.query.bucket || null
    const rows = diskdb.recentForExport(days, bucket)
    const stamp = new Date().toISOString().slice(0, 10)
    res.set('Content-Type', 'application/json')
    res.set('Content-Disposition', `attachment; filename="flashfeed-news-${stamp}.json"`)
    res.send(JSON.stringify({ exported_at: new Date().toISOString(), days, bucket: bucket || 'all', count: rows.length, news: rows }, null, 2))
  })

  app.post('/api/disk/sweep', (req, res) => {
    res.json({ ok: true, deleted: diskdb.sweep(), stats: diskdb.stats() })
  })

  // ── Auto-grab background job ────────────────────────────────────────────────
  // On the site (recent ping) → live UI handles news. Away → grab + archive to
  // the 'auto' bucket (2-day retention, then the sweeper auto-deletes it).
  const AUTO_GRAB_ENABLED = process.env.AUTO_GRAB_ENABLED !== 'false'
  const AUTO_GRAB_INTERVAL_MS = Math.max(60_000, Number(process.env.AUTO_GRAB_INTERVAL_MS || 60_000))
  const AUTO_GRAB_RUN_FETCH = process.env.AUTO_GRAB_RUN_FETCH !== 'false'
  let autoGrabRunning = false
  async function autoGrabTick() {
    if (autoGrabRunning || siteOpen() || !diskdb.isEnabled() || refreshCycleInFlight) return
    const db = mongoose.connection.db
    if (!db) return
    autoGrabRunning = true
    try {
      if (AUTO_GRAB_RUN_FETCH) {
        refreshCycleInFlight = true
        try { await runDataRefreshCycle(db, { mode: 'fast' }) }
        catch (_) {}
        finally { refreshCycleInFlight = false }
      }
      const r = diskdb.storeNews(await collectRecentNewsFromMongo(db, diskAutoDays, 3000), 'auto')
      if (r.stored) console.log(`  AutoGrab → archived ${r.stored} news to hard disk (away mode, ${diskAutoDays}d)`)
    } catch (e) { console.warn('autoGrabTick error:', e.message) }
    finally { autoGrabRunning = false }
  }
  if (AUTO_GRAB_ENABLED && diskdb.isEnabled()) {
    const t = setInterval(autoGrabTick, AUTO_GRAB_INTERVAL_MS)
    if (t.unref) t.unref()
    console.log(`  AutoGrab → enabled (every ${Math.round(AUTO_GRAB_INTERVAL_MS / 1000)}s while away → 'auto' ${diskAutoDays}d)`)
  }

  // ── On-site auto-fetch ──────────────────────────────────────────────────────
  // While someone is USING the website (recent presence ping), automatically grab
  // new articles every ONSITE_FETCH_INTERVAL_MS (default 1 min) and mirror them to
  // the hard-disk 'fetch' bucket (deleted after DISK_TTL_FETCH_DAYS = 3 days).
  //
  // It's driven two ways so the update is responsive: (1) the presence ping triggers
  // a check on each heartbeat, so a fresh visit grabs news right away, and (2) a
  // lightweight timer checks once a minute as a backstop. A due-check limits the
  // actual fetch to at most once per interval, and the shared refreshCycleInFlight
  // guard keeps it from overlapping Run Now / Auto-watch / the away auto-grabber.
  const ONSITE_FETCH_ENABLED = process.env.ONSITE_FETCH_ENABLED !== 'false'
  const ONSITE_FETCH_INTERVAL_MS = Math.max(60_000, Number(process.env.ONSITE_FETCH_INTERVAL_MS || 60_000))
  const ONSITE_FETCH_CHECK_MS = 60_000
  let onSiteFetchRunning = false
  let lastOnSiteFetchAt = null
  const onSiteFetchDue = () =>
    ONSITE_FETCH_ENABLED && siteOpen() && (Date.now() - (lastOnSiteFetchAt || 0)) >= ONSITE_FETCH_INTERVAL_MS
  async function onSiteAutoFetchTick() {
    if (onSiteFetchRunning || refreshCycleInFlight) return
    if (!onSiteFetchDue()) return                 // on-site + at most once per interval
    const db = mongoose.connection.db
    if (!db) return
    onSiteFetchRunning = true
    refreshCycleInFlight = true
    try {
      await runDataRefreshCycle(db, { mode: process.env.ONSITE_FETCH_MODE || 'fast' })  // grab new articles
      persistFetchNewsToDisk(db).catch(() => {})   // → 'fetch' bucket (3-day retention, then auto-deleted)
      lastOnSiteFetchAt = Date.now()
      console.log(`  OnSiteAutoFetch → grabbed new articles (on-site, every ${Math.round(ONSITE_FETCH_INTERVAL_MS / 60000)} min; hard-disk 'fetch' ${diskFetchDays}d)`)
    } catch (e) { console.warn('onSiteAutoFetchTick error:', e.message) }
    finally { refreshCycleInFlight = false; onSiteFetchRunning = false }
  }
  app.get('/api/auto-refresh/status', (req, res) => {
    const dashboardPresent = siteOpen()
    const nextDueMs = ONSITE_FETCH_ENABLED && dashboardPresent
      ? Math.max(Date.now(), (lastOnSiteFetchAt || 0) + ONSITE_FETCH_INTERVAL_MS)
      : null
    res.json({
      ok: true,
      updated_at: new Date().toISOString(),
      refresh_cycle_in_flight: Boolean(refreshCycleInFlight),
      market: {
        label: dashboardPresent ? 'Dashboard present' : 'Dashboard absent',
      },
      onsite_fetch: {
        enabled: Boolean(ONSITE_FETCH_ENABLED),
        running: Boolean(onSiteFetchRunning),
        due: Boolean(onSiteFetchDue()),
        dashboard_present: dashboardPresent,
        interval_minutes: Math.round(ONSITE_FETCH_INTERVAL_MS / 60000),
        check_seconds: Math.round(ONSITE_FETCH_CHECK_MS / 1000),
        last_run_at: lastOnSiteFetchAt ? new Date(lastOnSiteFetchAt).toISOString() : null,
        last_run_epoch_ms: lastOnSiteFetchAt || null,
        next_due_at: nextDueMs ? new Date(nextDueMs).toISOString() : null,
      },
      away_fetch: {
        enabled: Boolean(AUTO_GRAB_ENABLED),
        running: Boolean(autoGrabRunning),
        interval_minutes: Math.round(AUTO_GRAB_INTERVAL_MS / 60000),
        dashboard_present: dashboardPresent,
      },
      presence: {
        site_open: dashboardPresent,
        last_presence_at: lastPresenceAt || null,
      },
    })
  })

  app.get('/api/dashboard/freshness', async (req, res) => {
    try {
      const db = mongoose.connection.db
      if (!db) return res.status(503).json({ ok: false, status: 'unavailable', error: 'MongoDB is not connected' })

      const nowSec = Math.floor(Date.now() / 1000)
      const ageSeconds = (value) => {
        const sec = timestampSeconds(value)
        return sec ? Math.max(0, nowSec - sec) : null
      }
      const sourceStatus = (age, count, staleAfterSec) => {
        if (!Number(count || 0)) return 'empty'
        if (age == null) return 'unknown'
        return age <= staleAfterSec ? 'fresh' : 'stale'
      }
      const latestDoc = async (collection, sort, projection = {}) =>
        db.collection(collection).find({}, { projection }).sort(sort).limit(1).next().catch(() => null)
      const countDocs = async (collection, query = {}) =>
        db.collection(collection).countDocuments(query).catch(() => 0)

      const [latestArticle, latestScreener, latestSocial, latestDecision, articleCount, screenerCount, socialCount, decisionCount] = await Promise.all([
        latestDoc('articles', { feed_sort_time: -1, fetched_date: -1, publish_date: -1, createdAt: -1 }, { feed_sort_time: 1, fetched_date: 1, publish_date: 1, createdAt: 1 }),
        latestDoc('screeners', { quote_updated_at: -1, finviz_seen_at: -1, tradingview_seen_at: -1, updated_at: -1, createdAt: -1 }, { quote_updated_at: 1, finviz_seen_at: 1, tradingview_seen_at: 1, updated_at: 1, createdAt: 1 }),
        latestDoc('socials', { fetched_at: -1, timestamp: -1, created_at: -1 }, { fetched_at: 1, timestamp: 1, created_at: 1 }),
        latestDoc('decision_map_points', { timestamp_sec: -1, created_at: -1 }, { timestamp_sec: 1, created_at: 1 }),
        countDocs('articles'),
        countDocs('screeners'),
        countDocs('socials'),
        countDocs('decision_map_points'),
      ])

      const sources = {
        news: {
          count: articleCount,
          age_seconds: ageSeconds(latestArticle?.feed_sort_time ?? latestArticle?.fetched_date ?? latestArticle?.publish_date ?? latestArticle?.createdAt),
        },
        screener: {
          count: screenerCount,
          age_seconds: ageSeconds(latestScreener?.quote_updated_at ?? latestScreener?.finviz_seen_at ?? latestScreener?.tradingview_seen_at ?? latestScreener?.updated_at ?? latestScreener?.createdAt),
        },
        social: {
          count: socialCount,
          age_seconds: ageSeconds(latestSocial?.fetched_at ?? latestSocial?.timestamp ?? latestSocial?.created_at),
        },
        decision_map: {
          count: decisionCount,
          age_seconds: ageSeconds(latestDecision?.timestamp_sec ?? latestDecision?.created_at),
        },
      }
      sources.news.status = sourceStatus(sources.news.age_seconds, sources.news.count, 90 * 60)
      sources.screener.status = sourceStatus(sources.screener.age_seconds, sources.screener.count, 20 * 60)
      sources.social.status = sourceStatus(sources.social.age_seconds, sources.social.count, 90 * 60)
      sources.decision_map.status = sourceStatus(sources.decision_map.age_seconds, sources.decision_map.count, 30 * 60)

      const staleSources = Object.values(sources).filter(source => source.status === 'stale' || source.status === 'empty')
      res.json({
        ok: true,
        status: staleSources.length ? 'stale' : 'fresh',
        generated_at: new Date().toISOString(),
        market: {
          label: siteOpen() ? 'Dashboard present' : 'Dashboard absent',
        },
        sources,
        auto_refresh: {
          refresh_cycle_in_flight: Boolean(refreshCycleInFlight),
          onsite_fetch_enabled: Boolean(ONSITE_FETCH_ENABLED),
          onsite_fetch_running: Boolean(onSiteFetchRunning),
          onsite_fetch_interval_minutes: Math.round(ONSITE_FETCH_INTERVAL_MS / 60000),
          away_fetch_enabled: Boolean(AUTO_GRAB_ENABLED),
          away_fetch_running: Boolean(autoGrabRunning),
        },
      })
    } catch (err) {
      console.error('GET /api/dashboard/freshness failed:', err)
      res.status(500).json({ ok: false, status: 'error', error: String(err?.message || err) })
    }
  })

  app.get('/api/system/health', async (req, res) => {
    const started = Date.now()
    const nowSec = Math.floor(started / 1000)
    const now = new Date(started)
    const marketParts = easternParts(now)
    const marketWeekday = localWeekday(marketParts.year, marketParts.month, marketParts.day)
    const marketMinutes = marketParts.hour * 60 + marketParts.minute
    const extendedMarketOpen = marketWeekday >= 1 && marketWeekday <= 5 && marketMinutes >= 4 * 60 && marketMinutes < 20 * 60
    const latestExpectedMarketCloseSec = Math.floor(latestMarketCloseCutoff(now).getTime() / 1000)
    const staleSeconds = {
      articles: 90 * 60,
      screeners: 20 * 60,
      socials: 90 * 60,
      ohlcv_bars: 30 * 60,
      prediction_signals: 6 * 60 * 60,
      source_status: 90 * 60,
      daily_prediction_snapshots: 30 * 60 * 60,
    }
    const warnings = []

    const ageSeconds = (value) => {
      const sec = timestampSeconds(value)
      return sec ? Math.max(0, nowSec - sec) : null
    }
    const healthStatus = (age, staleAfter, count = 0) => {
      if (!Number(count || 0)) return 'empty'
      if (age == null) return 'unknown_age'
      return age > staleAfter ? 'stale' : 'fresh'
    }
    const collectionHealth = async (db, {
      name,
      latestSort,
      projection = {},
      staleAfter = 3600,
      countQuery = {},
      latestQuery = countQuery,
      latestTime,
    }) => {
      try {
        const collection = db.collection(name)
        const hasCountFilter = Boolean(countQuery && Object.keys(countQuery).length)
        const [count, latest] = await Promise.all([
          hasCountFilter ? collection.countDocuments(countQuery) : collection.estimatedDocumentCount(),
          collection.find(latestQuery, { projection }).sort(latestSort).limit(1).next(),
        ])
        const latestValue = typeof latestTime === 'function' ? latestTime(latest) : null
        const age = ageSeconds(latestValue)
        const status = healthStatus(age, staleAfter, count)
        return {
          name,
          status,
          count,
          latest_at: latestValue ? new Date(timestampSeconds(latestValue) * 1000).toISOString() : null,
          age_seconds: age,
          stale_after_seconds: staleAfter,
          latest_sample: latest || null,
        }
      } catch (err) {
        return { name, status: 'error', count: 0, latest_at: null, age_seconds: null, stale_after_seconds: staleAfter, error: String(err.message || err) }
      }
    }

    try {
      const db = mongoose.connection.db
      if (!db) {
        return res.status(503).json({
          ok: false,
          status: 'degraded',
          error: 'MongoDB connection is not ready',
          generated_at: new Date().toISOString(),
        })
      }

      const collectionSpecs = [
        {
          name: 'articles',
          latestSort: { feed_sort_time: -1, event_sec: -1, publish_sec: -1, fetched_date: -1, detected_at: -1 },
          projection: { _id: 0, ticker: 1, tickers: 1, source: 1, title: 1, feed_sort_time: 1, event_sec: 1, publish_sec: 1, publish_date: 1, fetched_date: 1, detected_at: 1 },
          staleAfter: staleSeconds.articles,
          latestTime: row => row?.feed_sort_time || row?.event_sec || row?.publish_sec || row?.publish_date || row?.fetched_date || row?.detected_at,
        },
        {
          name: 'screeners',
          latestSort: { quote_updated_at: -1, updated_at: -1, finviz_seen_at: -1, tradingview_seen_at: -1 },
          projection: { _id: 0, ticker: 1, source: 1, quote_source: 1, quote_updated_at: 1, updated_at: 1, finviz_seen_at: 1, tradingview_seen_at: 1, change_pct: 1, rel_volume: 1 },
          staleAfter: staleSeconds.screeners,
          latestTime: row => row?.quote_updated_at || row?.updated_at || row?.finviz_seen_at || row?.tradingview_seen_at,
        },
        {
          name: 'socials',
          latestSort: { event_sec: -1, fetched_at: -1, timestamp: -1, detected_at: -1, createdAt: -1 },
          projection: { _id: 0, ticker: 1, symbol: 1, platform: 1, source: 1, event_sec: 1, fetched_at: 1, timestamp: 1, detected_at: 1, createdAt: 1, sentiment: 1, sentiment_score: 1 },
          staleAfter: staleSeconds.socials,
          latestTime: row => row?.event_sec || row?.fetched_at || row?.timestamp || row?.detected_at || row?.createdAt,
        },
        {
          name: 'ohlcv_bars',
          latestSort: { minute: -1, timestamp: -1 },
          projection: { _id: 0, ticker: 1, minute: 1, timestamp: 1, source: 1, providerInterval: 1, close: 1, volume: 1 },
          staleAfter: staleSeconds.ohlcv_bars,
          latestTime: row => row?.minute || row?.timestamp,
        },
        {
          name: 'prediction_signals',
          latestSort: { signal_sec: -1, created_at: -1 },
          projection: { _id: 0, ticker: 1, signal_sec: 1, created_at: 1, source: 1, decision: 1, label_status: 1, probability_up: 1 },
          staleAfter: staleSeconds.prediction_signals,
          latestTime: row => row?.signal_sec || row?.created_at,
        },
        {
          name: 'daily_prediction_snapshots',
          latestSort: { updated_at: -1, created_at: -1 },
          projection: { _id: 0, updated_at: 1, created_at: 1, predictionDate: 1, targetDate: 1, archive_status: 1, rowCount: 1, metadata: 1 },
          staleAfter: staleSeconds.daily_prediction_snapshots,
          latestTime: row => row?.updated_at || row?.created_at,
        },
        {
          name: 'source_status',
          latestSort: { last_checked_at: -1, last_success_at: -1 },
          projection: { _id: 0, source: 1, type: 1, status: 1, last_checked_at: 1, last_success_at: 1, last_count: 1, records_received: 1, records_accepted: 1, records_new: 1, records_updated: 1, records_duplicates: 1, records_malformed: 1, detail: 1, error: 1 },
          staleAfter: staleSeconds.source_status,
          latestTime: row => row?.last_checked_at || row?.last_success_at,
        },
      ]

      const collections = {}
      const collectionRows = await Promise.all(collectionSpecs.map(spec => collectionHealth(db, spec)))
      for (const row of collectionRows) {
        const latestSec = row.latest_at ? Math.floor(new Date(row.latest_at).getTime() / 1000) : 0
        const hasLastMarketSession = latestSec >= latestExpectedMarketCloseSec - 15 * 60
        if (!extendedMarketOpen && row.name === 'ohlcv_bars' && row.status === 'stale' && hasLastMarketSession) {
          row.status = 'market_closed'
          row.expected_freshness = 'latest_completed_market_session'
        }
        if (!extendedMarketOpen && row.name === 'daily_prediction_snapshots' && row.status === 'stale' && Number(row.age_seconds || Infinity) <= 72 * 60 * 60) {
          row.status = 'market_closed'
          row.expected_freshness = 'next_session_archive_not_due'
        }
        collections[row.name] = row
        if (['stale', 'empty', 'error'].includes(row.status)) warnings.push(`${row.name}_${row.status}`)
      }

      const [sourceRows, latestPredictionArchive, signalCounts, labelCounts] = await Promise.all([
        db.collection('source_status')
          .find({}, { projection: { _id: 0, source: 1, type: 1, status: 1, detail: 1, error: 1, last_checked_at: 1, last_success_at: 1, last_count: 1, records_received: 1, records_accepted: 1, records_new: 1, records_updated: 1, records_duplicates: 1, records_malformed: 1 } })
          .sort({ last_checked_at: -1, last_success_at: -1 })
          .limit(40)
          .toArray()
          .catch(() => []),
        db.collection('daily_prediction_snapshots')
          .find({}, { projection: { _id: 0, updated_at: 1, predictionDate: 1, targetDate: 1, archive_status: 1, rowCount: 1, fallbackRows: 1, metadata: 1 } })
          .sort({ updated_at: -1, created_at: -1 })
          .limit(1)
          .next()
          .catch(() => null),
        db.collection('prediction_signals').aggregate([
          { $match: { signal_sec: { $gte: nowSec - 24 * 60 * 60 } } },
          { $group: { _id: '$decision', count: { $sum: 1 } } },
        ]).toArray().catch(() => []),
        db.collection('prediction_signals').aggregate([
          { $match: { signal_sec: { $gte: nowSec - 24 * 60 * 60 } } },
          { $group: { _id: '$label_status', count: { $sum: 1 } } },
        ]).toArray().catch(() => []),
      ])

      const sourceSummary = sourceRows.reduce((acc, row) => {
        const status = String(row.status || 'unknown').toLowerCase()
        if (status.includes('working') || status.includes('healthy') || status === 'ok') acc.working += 1
        else if (status.includes('required') || status.includes('error') || status.includes('failed') || status.includes('blocked')) acc.blocked += 1
        else if (status.includes('ready')) acc.ready += 1
        else acc.other += 1
        if (ageSeconds(row.last_checked_at || row.last_success_at) > staleSeconds.source_status) acc.stale += 1
        return acc
      }, { working: 0, ready: 0, blocked: 0, stale: 0, other: 0, total: sourceRows.length })
      if (sourceSummary.blocked) warnings.push(`blocked_sources_${sourceSummary.blocked}`)
      if (sourceSummary.stale) warnings.push(`stale_sources_${sourceSummary.stale}`)

      const archiveMeta = latestPredictionArchive?.metadata || {}
      const thresholdPolicy = archiveMeta.thresholdPolicy || null
      const predictionPipeline = {
        latest_archive_at: latestPredictionArchive?.updated_at ? new Date(latestPredictionArchive.updated_at).toISOString() : null,
        archive_status: latestPredictionArchive?.archive_status || null,
        prediction_date: latestPredictionArchive?.predictionDate || null,
        target_date: latestPredictionArchive?.targetDate || null,
        final_rows: Number(latestPredictionArchive?.rowCount || archiveMeta.finalRows || 0),
        strict_rows: Number(archiveMeta.strictRows || 0),
        developing_candidate_rows: Number(archiveMeta.candidatePoolRows || 0),
        fallback_rows: Number(latestPredictionArchive?.fallbackRows?.length || archiveMeta.fallbackRows || 0),
        stored_prediction_rows: Number(archiveMeta.storedPredictionRows || 0),
        live_signal_rows: Number(archiveMeta.liveSignalRows || 0),
        evidence_prediction_rows: Number(archiveMeta.evidencePredictionRows || 0),
        threshold_policy: thresholdPolicy ? {
          version: thresholdPolicy.version || archiveMeta.thresholdPolicyVersion || null,
          status: thresholdPolicy.status || null,
          candidate_rule: thresholdPolicy.candidateRule?.name || null,
          source_backtest: thresholdPolicy.candidateRule?.sourceBacktest || null,
        } : null,
        threshold_policy_version: archiveMeta.thresholdPolicyVersion || null,
        warnings: archiveMeta.warnings || [],
        removed_by_filter_counts: archiveMeta.removedByFilterCounts || {},
        risk_flag_counts: archiveMeta.predictionRiskFlagCounts || {},
        readiness_counts: archiveMeta.predictionReadinessCounts || {},
        catalyst_reaction_counts: archiveMeta.catalystReactionCounts || {},
        catalyst_quality_counts: archiveMeta.catalystQualityCounts || {},
        first_reaction_state_counts: archiveMeta.firstReactionStateCounts || {},
        signal_counts_24h: Object.fromEntries(signalCounts.map(row => [row._id || 'unknown', row.count])),
        label_counts_24h: Object.fromEntries(labelCounts.map(row => [row._id || 'unknown', row.count])),
      }
      if (!predictionPipeline.final_rows && !predictionPipeline.developing_candidate_rows) warnings.push('prediction_pipeline_no_current_candidates')
      if (predictionPipeline.warnings?.length) warnings.push('prediction_pipeline_has_warnings')

      let redisHealth = { available: false, status: 'unavailable' }
      if (redisReady()) {
        const redisStarted = Date.now()
        try {
          const pong = await redis.ping()
          redisHealth = {
            available: true,
            status: pong === 'PONG' ? 'healthy' : 'warning',
            latency_ms: Date.now() - redisStarted,
            connection_status: redis.status,
          }
        } catch (err) {
          redisHealth = { available: false, status: 'error', error: String(err.message || err), latency_ms: Date.now() - redisStarted }
          warnings.push('redis_error')
        }
      } else {
        warnings.push('redis_unavailable')
      }

      const mongoHealth = {
        status: mongoose.connection.readyState === 1 ? 'healthy' : 'degraded',
        ready_state: mongoose.connection.readyState,
        database: db.databaseName,
      }
      const autoRefresh = {
        refresh_cycle_in_flight: Boolean(refreshCycleInFlight),
        onsite_enabled: Boolean(ONSITE_FETCH_ENABLED),
        onsite_running: Boolean(onSiteFetchRunning),
        onsite_interval_seconds: Math.round(ONSITE_FETCH_INTERVAL_MS / 1000),
        onsite_check_seconds: Math.round(ONSITE_FETCH_CHECK_MS / 1000),
        onsite_last_run_at: lastOnSiteFetchAt ? new Date(lastOnSiteFetchAt).toISOString() : null,
        onsite_due: Boolean(onSiteFetchDue()),
        away_enabled: Boolean(AUTO_GRAB_ENABLED),
        away_running: Boolean(autoGrabRunning),
        away_interval_seconds: Math.round(AUTO_GRAB_INTERVAL_MS / 1000),
        dashboard_present: siteOpen(),
        cadence_floor_seconds: 60,
        cadence_ok: ONSITE_FETCH_INTERVAL_MS >= 60_000 && AUTO_GRAB_INTERVAL_MS >= 60_000 && ONSITE_FETCH_CHECK_MS >= 60_000,
      }
      if (!autoRefresh.cadence_ok) warnings.push('auto_refresh_faster_than_one_minute')

      const kafkaHealth = {
        configured: Boolean(process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS || process.env.REDPANDA_BROKERS),
        status: process.env.KAFKA_BROKERS || process.env.KAFKA_BOOTSTRAP_SERVERS || process.env.REDPANDA_BROKERS
          ? 'configured_external_worker_status_via_source_status'
          : 'not_configured_in_backend',
      }

      const hardFailures = warnings.filter(w => w.includes('_error') || w.includes('mongodb') || w === 'auto_refresh_faster_than_one_minute')
      const staleFailures = warnings.filter(w => w.includes('_stale') || w.startsWith('stale_') || w.includes('_empty'))
      const status = hardFailures.length ? 'degraded' : staleFailures.length || warnings.length ? 'warning' : 'healthy'

      res.json({
        ok: status !== 'degraded',
        status,
        generated_at: new Date().toISOString(),
        ms: Date.now() - started,
        mongo: mongoHealth,
        redis: redisHealth,
        kafka: kafkaHealth,
        auto_refresh: autoRefresh,
        collections,
        sources: {
          summary: sourceSummary,
          rows: sourceRows.map(row => ({
            ...row,
            age_seconds: ageSeconds(row.last_checked_at || row.last_success_at),
          })),
        },
        prediction_pipeline: predictionPipeline,
        warnings: Array.from(new Set(warnings)),
      })
    } catch (err) {
      console.error('GET /api/system/health failed:', err)
      res.status(500).json({
        ok: false,
        status: 'degraded',
        error: String(err.message || err),
        generated_at: new Date().toISOString(),
        ms: Date.now() - started,
      })
    }
  })
  // expose so the presence-ping handler can trigger an immediate check on each heartbeat
  triggerOnSiteAutoFetch = () => { onSiteAutoFetchTick().catch(() => {}) }
  if (ONSITE_FETCH_ENABLED) {
    const t = setInterval(onSiteAutoFetchTick, ONSITE_FETCH_CHECK_MS)
    if (t.unref) t.unref()
    console.log(`  OnSiteAutoFetch → enabled (every ${Math.round(ONSITE_FETCH_INTERVAL_MS / 60000)} min while on-site → 'fetch' ${diskFetchDays}d)`)
  }

app.listen(PORT, () => {
    console.log()
    console.log('  ⚡ FlashFeed API')
    console.log('  ─────────────────────────────────────')
    console.log('  Server  →  http://localhost:' + PORT)
    console.log('  Health  →  http://localhost:' + PORT + '/api/health')
    console.log('  Docs    →  README-MONGODB.md')
    console.log()
  })
}

start()
