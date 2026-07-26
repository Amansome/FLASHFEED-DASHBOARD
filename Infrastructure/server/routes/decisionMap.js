import { Router } from 'express'
import mongoose from 'mongoose'
import { spawn } from 'node:child_process'
import { createHash } from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { analyzeDecisionMapPath, analyzeDecisionMapRows, decisionMapPathScope, decisionMapRowLimit } from '../lib/decisionMapRows.js'

const router = Router()

const US_EXCHANGES = new Set(['NASDAQ', 'NYSE', 'AMEX'])
const NON_STOCK_TICKERS = new Set([
  'BTC', 'ETH', 'LTC', 'DOGE', 'SOL', 'ADA', 'XRP', 'BNB', 'DOT', 'AVAX',
  'MATIC', 'SHIB', 'TRX', 'BCH', 'LINK', 'ATOM', 'UNI', 'ETC', 'FIL',
  'USD', 'USDT', 'USDC', 'SPOT',
])

const DEFAULT_THRESHOLDS = Object.freeze({
  minRelativeVolume: Number(process.env.DECISION_MAP_MIN_REL_VOLUME || 1),
  minAbsPriceChange: Number(process.env.DECISION_MAP_MIN_ABS_CHANGE || 0.5),
  positiveSentiment: Number(process.env.DECISION_MAP_POSITIVE_SENTIMENT || 0.12),
  negativeSentiment: Number(process.env.DECISION_MAP_NEGATIVE_SENTIMENT || -0.12),
  priceChange: Number(process.env.DECISION_MAP_PRICE_THRESHOLD || 0.5),
  minActivityScore: Number(process.env.DECISION_MAP_MIN_ACTIVITY_SCORE || 0),
  rollingWindowHours: Number(process.env.DECISION_MAP_ROLLING_WINDOW_HOURS || 4),
  newsWindowHours: Number(process.env.DECISION_MAP_NEWS_WINDOW_HOURS || process.env.DECISION_MAP_ROLLING_WINDOW_HOURS || 4),
  socialWindowHours: Number(process.env.DECISION_MAP_SOCIAL_WINDOW_HOURS || process.env.DECISION_MAP_ROLLING_WINDOW_HOURS || 4),
  pathWindowHours: Number(process.env.DECISION_MAP_PATH_WINDOW_HOURS || process.env.DECISION_MAP_ROLLING_WINDOW_HOURS || 4),
  finvizStaleSeconds: Number(process.env.DECISION_MAP_FINVIZ_STALE_SECONDS || 45 * 60),
  maxAbsPriceChange: Number(process.env.DECISION_MAP_MAX_ABS_CHANGE || process.env.MAX_SIGNAL_CHANGE_PCT || 300),
})

const STRUCTURED_SOURCE_RE = /finviz|tradingview|pr newswire|business wire|access newswire|benzinga|sec|edgar|fda|globenewswire|dow jones|interactive brokers|td ameritrade|schwab/i
const SOCIAL_SOURCE_RE = /stocktwits|twitter|x\.com|reddit|bluesky|social/i
const MARKET_CAP_BUCKETS = new Set(['mega', 'large', 'mid', 'small', 'micro', 'nano'])
const FLOAT_BUCKETS = new Set(['ultra_low', 'low', 'mid', 'high', 'very_high', 'unknown'])
const DECISION_MAP_REDIS_TTL = Math.max(30, Number(process.env.DECISION_MAP_REDIS_TTL || 300))
const DECISION_MAP_PATH_TTL = Math.max(3 * 86_400, Number(process.env.DECISION_MAP_PATH_TTL || 4 * 86_400))
const DECISION_MAP_HISTORY_SECONDS = Math.max(DECISION_MAP_PATH_TTL, Number(process.env.DECISION_MAP_HISTORY_SECONDS || 4 * 86_400))
const DECISION_MAP_MARKET_DAY_PATH_HISTORY_SECONDS = Math.max(3 * 86_400, Number(process.env.DECISION_MAP_MARKET_DAY_PATH_HISTORY_SECONDS || 4 * 86_400))
const DECISION_MAP_PATH_MAX = Math.max(12, Math.min(500, Number(process.env.DECISION_MAP_PATH_MAX || 180)))
const DECISION_MAP_PERSIST_MONGO = process.env.DECISION_MAP_PERSIST_MONGO !== 'false'
const DECISION_MAP_KAFKA_PUBLISH = process.env.DECISION_MAP_KAFKA_PUBLISH !== 'false'
const DECISION_MAP_POINT_INTERVAL_SECONDS = Math.max(15, Number(process.env.DECISION_MAP_POINT_INTERVAL_SECONDS || 60))
const DECISION_MAP_DEFAULT_PATH_POINTS = Math.max(12, Math.min(96, Number(process.env.DECISION_MAP_DEFAULT_PATH_POINTS || 60)))
const DECISION_MAP_MAX_PATH_POINTS = Math.max(DECISION_MAP_DEFAULT_PATH_POINTS, Math.min(240, Number(process.env.DECISION_MAP_MAX_PATH_POINTS || 120)))
const DECISION_MAP_MIN_FULL_PATH_POINTS = Math.max(8, Number(process.env.DECISION_MAP_MIN_FULL_PATH_POINTS || 20))
const DECISION_MAP_SPARSE_PATH_THRESHOLD = Math.max(2, Number(process.env.DECISION_MAP_SPARSE_PATH_THRESHOLD || 4))
const DECISION_MAP_CHART_FETCH_CONCURRENCY = Math.max(1, Math.min(8, Number(process.env.DECISION_MAP_CHART_FETCH_CONCURRENCY || 4)))
const DECISION_MAP_CATALYST_LOOKBACK_HOURS = Math.max(1, Number(process.env.DECISION_MAP_CATALYST_LOOKBACK_HOURS || 24))
const DECISION_MAP_WEEKEND_CATALYST_LOOKBACK_HOURS = Math.max(
  DECISION_MAP_CATALYST_LOOKBACK_HOURS,
  Number(process.env.DECISION_MAP_WEEKEND_CATALYST_LOOKBACK_HOURS || 72),
)
const DECISION_MAP_DB_NAME = String(process.env.DECISION_MAP_DB_NAME || process.env.DECISION_MAP_STORAGE_DB_NAME || 'feedflash_decision_map').trim()
const DECISION_MAP_UI_REFRESH_SECONDS = Math.max(15, Number(process.env.DECISION_MAP_UI_REFRESH_SECONDS || 60))
const DECISION_MAP_HEALTH_CHECK_MS = Math.max(30_000, Number(process.env.DECISION_MAP_HEALTH_CHECK_MS || 60_000))
let latestDecisionMapHealth = null
let decisionMapHealthTimer = null
const NY_DATE_FORMAT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
})
const NY_WEEKDAY_FORMAT = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  weekday: 'short',
})
const NY_TIME_FORMAT = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  month: 'short',
  day: '2-digit',
  hour: 'numeric',
  minute: '2-digit',
  hour12: true,
})
const KNOWN_MARKET_CLOSED_DATES = new Set([
  '2026-07-03',
])

function clamp(value, min, max) {
  const n = Number(value)
  if (!Number.isFinite(n)) return min
  return Math.max(min, Math.min(max, n))
}

function toNumber(value, fallback = null) {
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function toSec(value) {
  if (!value) return 0
  if (value instanceof Date) return Math.floor(value.getTime() / 1000)
  const n = Number(value)
  if (Number.isFinite(n) && n > 0) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n)
  const ms = Date.parse(String(value))
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : 0
}

function isoFromSec(sec) {
  const value = Number(sec || 0)
  return Number.isFinite(value) && value > 0 ? new Date(value * 1000).toISOString() : null
}

function displayTimeFromSec(sec) {
  const value = Number(sec || 0)
  return Number.isFinite(value) && value > 0 ? NY_TIME_FORMAT.format(new Date(value * 1000)) : null
}

async function fetchChartProviderCandles(ticker, range = '1d', interval = '1m') {
  const symbol = normalizeTicker(ticker)
  if (!symbol) return []
  const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`)
  url.searchParams.set('range', range)
  url.searchParams.set('interval', interval)
  url.searchParams.set('includePrePost', 'true')
  url.searchParams.set('events', 'history')
  const resp = await fetch(url, {
    headers: {
      'User-Agent': 'FeedFlashStockDashboard/0.1',
      Accept: 'application/json',
    },
  })
  if (!resp.ok) return []
  const payload = await resp.json().catch(() => null)
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
    candles.push({
      time: Number(timestamps[i]),
      open,
      high,
      low,
      close,
      volume: Number.isFinite(Number(quote.volume?.[i])) ? Math.max(0, Number(quote.volume[i])) : null,
    })
  }
  return candles.sort((a, b) => Number(a.time || 0) - Number(b.time || 0))
}

function formatWindowHours(hours) {
  const value = Number(hours || 0)
  if (!Number.isFinite(value) || value <= 0) return 'unknown'
  const minutes = Math.round(value * 60)
  if (minutes < 60) return `${minutes}m`
  if (minutes % 1440 === 0) return `${minutes / 1440}d`
  return `${Number(value.toFixed(value < 10 && value % 1 ? 2 : 0))}h`
}

function rollingWindowHoursFromQuery(query = {}, key, fallbackHours = 4) {
  const aliases = {
    rollingWindowHours: query.rolling_window_hours ?? query.rollingWindowHours ?? query.window_hours ?? query.windowHours,
    newsWindowHours: query.news_window_hours ?? query.newsWindowHours ?? query.rolling_window_hours ?? query.rollingWindowHours ?? query.window_hours ?? query.windowHours,
    socialWindowHours: query.social_window_hours ?? query.socialWindowHours ?? query.rolling_window_hours ?? query.rollingWindowHours ?? query.window_hours ?? query.windowHours,
    pathWindowHours: query.path_window_hours ?? query.pathWindowHours ?? query.rolling_window_hours ?? query.rollingWindowHours ?? query.window_hours ?? query.windowHours,
    catalystLookbackHours: query.catalyst_lookback_hours ?? query.catalystLookbackHours,
  }
  const raw = aliases[key] ?? fallbackHours
  return clamp(Number(raw), 5 / 60, 168)
}

function nySessionForNow(now = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(now)
  const byType = Object.fromEntries(parts.filter(part => part.type !== 'literal').map(part => [part.type, part.value]))
  const weekday = byType.weekday
  const minutes = Number(byType.hour || 0) * 60 + Number(byType.minute || 0)
  return {
    weekday,
    minutes,
    weekend: weekday === 'Sat' || weekday === 'Sun',
    mondayPremarket: weekday === 'Mon' && minutes < 9 * 60 + 30,
  }
}

function defaultCatalystLookbackHours(now = new Date()) {
  const session = nySessionForNow(now)
  if (session.weekend || session.mondayPremarket) return DECISION_MAP_WEEKEND_CATALYST_LOOKBACK_HOURS
  return DECISION_MAP_CATALYST_LOOKBACK_HOURS
}

function rollingWindowMeta(thresholds = {}, nowSec = Math.floor(Date.now() / 1000)) {
  const rollingWindowHours = rollingWindowHoursFromQuery({}, 'rollingWindowHours', thresholds.rollingWindowHours || thresholds.pathWindowHours || 4)
  const newsWindowHours = rollingWindowHoursFromQuery({}, 'newsWindowHours', thresholds.newsWindowHours || rollingWindowHours)
  const socialWindowHours = rollingWindowHoursFromQuery({}, 'socialWindowHours', thresholds.socialWindowHours || rollingWindowHours)
  const pathWindowHours = rollingWindowHoursFromQuery({}, 'pathWindowHours', thresholds.pathWindowHours || rollingWindowHours)
  const catalystLookbackHours = rollingWindowHoursFromQuery({}, 'catalystLookbackHours', thresholds.catalystLookbackHours || defaultCatalystLookbackHours(new Date(nowSec * 1000)))
  const sourceNewsWindowStartSec = nowSec - Math.round(newsWindowHours * 3600)
  const sourceSocialWindowStartSec = nowSec - Math.round(socialWindowHours * 3600)
  return {
    rollingWindowHours,
    rollingWindowUsed: formatWindowHours(rollingWindowHours),
    newsWindowHours,
    newsWindowUsed: formatWindowHours(newsWindowHours),
    socialWindowHours,
    socialWindowUsed: formatWindowHours(socialWindowHours),
    pathWindowHours,
    pathWindowUsed: formatWindowHours(pathWindowHours),
    catalystLookbackHours,
    catalystLookbackUsed: formatWindowHours(catalystLookbackHours),
    sourceNewsWindowStart: isoFromSec(sourceNewsWindowStartSec),
    sourceNewsWindowEnd: isoFromSec(nowSec),
    sourceSocialWindowStart: isoFromSec(sourceSocialWindowStartSec),
    sourceSocialWindowEnd: isoFromSec(nowSec),
    sourceNewsWindowStartSec,
    sourceNewsWindowEndSec: nowSec,
    sourceSocialWindowStartSec,
    sourceSocialWindowEndSec: nowSec,
  }
}

function nyDateKey(sec) {
  const value = Number(sec || 0)
  if (!Number.isFinite(value) || value <= 0) return ''
  return NY_DATE_FORMAT.format(new Date(value * 1000))
}

function isLikelyMarketMovementDate(sec) {
  const key = nyDateKey(sec)
  if (!key || KNOWN_MARKET_CLOSED_DATES.has(key)) return false
  const weekday = NY_WEEKDAY_FORMAT.format(new Date(Number(sec) * 1000))
  return weekday !== 'Sat' && weekday !== 'Sun'
}

function marketDayPathsOnly(query = {}) {
  const raw = query.market_day_paths ?? query.marketDayPaths ?? query.market_days_only ?? query.marketDaysOnly
  if (raw == null || raw === '') return true
  return !['0', 'false', 'no', 'off'].includes(String(raw).toLowerCase())
}

async function mapWithConcurrency(items = [], limit = 4, worker) {
  const queue = [...items]
  const workers = Array.from({ length: Math.min(Math.max(1, limit), queue.length || 1) }, async () => {
    while (queue.length) {
      const item = queue.shift()
      await worker(item)
    }
  })
  await Promise.all(workers)
}

function pathSinceSec(windowHours = DEFAULT_THRESHOLDS.pathWindowHours, marketDayOnly = false) {
  const nowSec = Math.floor(Date.now() / 1000)
  if (marketDayOnly) return nowSec - DECISION_MAP_MARKET_DAY_PATH_HISTORY_SECONDS
  return nowSec - Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.pathWindowHours)) * 3600)
}

function redisFromReq(req) {
  const redis = req?.app?.locals?.redis
  const ready = typeof req?.app?.locals?.redisReady === 'function' ? req.app.locals.redisReady() : redis?.status === 'ready'
  return ready ? redis : null
}

function decisionMapStorageDb(primaryDb) {
  if (!primaryDb) return null
  if (!DECISION_MAP_DB_NAME || DECISION_MAP_DB_NAME === primaryDb.databaseName) return primaryDb
  const client = primaryDb.client || mongoose.connection.client
  return client?.db ? client.db(DECISION_MAP_DB_NAME) : primaryDb
}

async function buildDecisionMapHealth(redis, db, reason = 'manual') {
  const checkedAt = new Date()
  const warnings = []
  const status = {
    ok: true,
    status: 'healthy',
    reason,
    checked_at: checkedAt.toISOString(),
    source_db: db?.databaseName || null,
    storage_db: null,
    collection: 'decision_map_points',
    expected_ui_refresh_seconds: DECISION_MAP_UI_REFRESH_SECONDS,
    redis_ttl_seconds: DECISION_MAP_REDIS_TTL,
    path_ttl_seconds: DECISION_MAP_PATH_TTL,
    point_interval_seconds: DECISION_MAP_POINT_INTERVAL_SECONDS,
    redis_available: Boolean(redis),
    redis: null,
    mongo: null,
    auto_refresh: {
      expected_ui_refresh_seconds: DECISION_MAP_UI_REFRESH_SECONDS,
      backend_hot_cache_ttl_seconds: DECISION_MAP_REDIS_TTL,
      health_check_seconds: Math.round(DECISION_MAP_HEALTH_CHECK_MS / 1000),
      ok: true,
    },
    warnings,
  }

  if (redis) {
    const [meta, activeCount] = await Promise.all([
      redis.hgetall('decision_map:meta').catch(() => ({})),
      redis.zcard('decision_map:active').catch(() => 0),
    ])
    const latestSnapshotSec = Number(meta.latest_snapshot_sec || 0)
    const latestAge = latestSnapshotSec ? Math.max(0, Math.floor(Date.now() / 1000) - latestSnapshotSec) : null
    status.redis = {
      latest_snapshot_at: meta.latest_snapshot_at || null,
      latest_snapshot_sec: latestSnapshotSec || null,
      latest_age_seconds: latestAge,
      latest_count: Number(meta.latest_count || 0),
      active_tickers: Number(activeCount || 0),
      latest_signature: meta.latest_signature || null,
    }
    if (latestAge == null) warnings.push('redis_has_no_decision_map_snapshot')
    else if (latestAge > Math.max(DECISION_MAP_REDIS_TTL * 2, DECISION_MAP_UI_REFRESH_SECONDS * 3)) warnings.push('redis_snapshot_stale_for_refresh_rate')
    if (!Number(activeCount || 0)) warnings.push('redis_has_no_active_decision_map_tickers')
  } else {
    warnings.push('redis_unavailable_hot_paths_disabled')
  }

  if (db) {
    const pointDb = decisionMapStorageDb(db)
    status.storage_db = pointDb?.databaseName || DECISION_MAP_DB_NAME || db.databaseName || null
    const [latest, pointCount] = await Promise.all([
      pointDb.collection('decision_map_points')
        .findOne({}, { sort: { snapshot_sec: -1 }, projection: { ticker: 1, snapshot_sec: 1, generated_at: 1 } })
        .catch(() => null),
      pointDb.collection('decision_map_points').estimatedDocumentCount().catch(() => 0),
    ])
    const latestSec = Number(latest?.snapshot_sec || 0)
    const latestAge = latestSec ? Math.max(0, Math.floor(Date.now() / 1000) - latestSec) : null
    status.mongo = {
      source_db: db.databaseName || null,
      storage_db: pointDb.databaseName || DECISION_MAP_DB_NAME || null,
      collection: 'decision_map_points',
      point_count_estimate: Number(pointCount || 0),
      latest_ticker: latest?.ticker || null,
      latest_snapshot_sec: latestSec || null,
      latest_snapshot_at: latest?.generated_at || null,
      latest_age_seconds: latestAge,
    }
    if (!pointCount) warnings.push('mongo_decision_map_points_empty')
    if (latestAge != null && latestAge > DECISION_MAP_HISTORY_SECONDS) warnings.push('mongo_decision_map_points_outside_history_window')
  } else {
    warnings.push('mongo_unavailable')
  }

  if (warnings.length) {
    status.ok = false
    status.status = warnings.some(item => item.includes('unavailable') || item.includes('empty')) ? 'degraded' : 'warning'
  }
  latestDecisionMapHealth = status
  return status
}

function ensureDecisionMapHealthMonitor(req, db) {
  if (decisionMapHealthTimer || !db) return
  const redis = redisFromReq(req)
  buildDecisionMapHealth(redis, db, 'startup').catch(() => {})
  decisionMapHealthTimer = setInterval(() => {
    buildDecisionMapHealth(redis, db, 'background').catch(() => {})
  }, DECISION_MAP_HEALTH_CHECK_MS)
  if (decisionMapHealthTimer.unref) decisionMapHealthTimer.unref()
}

export function stableDecisionMapQuerySignature(query = {}) {
  const ignored = new Set(['fresh', '_', '_r', 't', 'cache_bust', 'cacheBust'])
  const normalized = { __cache_version: 'finviz-mover-top30-v13-float-filter' }
  for (const key of Object.keys(query).sort()) {
    if (ignored.has(key)) continue
    normalized[key] = String(query[key])
  }
  return `v12-${createHash('sha256').update(JSON.stringify(normalized)).digest('hex').slice(0, 40)}`
}

function decisionMapCacheFields({
  cacheMode,
  cacheHit,
  redisAvailable,
  builtAt,
  ttlSeconds,
  cacheSignature,
  store,
}) {
  const ttl = Number(ttlSeconds || 0)
  const positiveTtl = Number.isFinite(ttl) && ttl > 0 ? ttl : 0
  return {
    cacheMode,
    cacheHit: Boolean(cacheHit),
    redisAvailable: Boolean(redisAvailable),
    builtAt: builtAt || null,
    ttlSeconds: positiveTtl,
    expiresAt: positiveTtl ? new Date(Date.now() + positiveTtl * 1000).toISOString() : null,
    cacheSignature,
    store,
  }
}

function quantizedNowSec() {
  const now = Math.floor(Date.now() / 1000)
  return Math.floor(now / DECISION_MAP_POINT_INTERVAL_SECONDS) * DECISION_MAP_POINT_INTERVAL_SECONDS
}

function pointJson(point = {}) {
  return JSON.stringify(point)
}

function parsePoint(raw) {
  if (!raw) return null
  try {
    const point = typeof raw === 'string' ? JSON.parse(raw) : raw
    return point && typeof point === 'object' ? point : null
  } catch (_) {
    return null
  }
}

function decisionMapPointForStorage(row = {}, snapshotSec = quantizedNowSec(), pathScope = decisionMapPathScope()) {
  const point = decisionPointFromValues({
    timestamp: snapshotSec,
    combinedSentiment: row.combinedSentiment,
    priceChangePct: row.priceChangePct,
    marketCapRelativeVolumeScore: row.marketCapRelativeVolumeScore,
    relativeVolume: row.relativeVolume,
    currentDollarVolume: row.currentDollarVolume,
    price: row.price,
    currentVolume: row.currentVolume,
    convictionScore: row.convictionScore,
  }, row)
  return {
    ...point,
    ticker: row.ticker,
    pathScope,
    company: row.company || '',
    snapshotSec,
    timestamp: snapshotSec,
    generatedAt: new Date(snapshotSec * 1000).toISOString(),
    quadrant: row.quadrant || 'Neutral',
    pathDirection: row.path_direction || 'insufficient_history',
    pathColor: row.path_color || 'gray',
    activityScore: Number(row.activityScore || 0),
    convictionScore: Number(row.convictionScore || 0),
    relativeVolume: Number(row.relativeVolume || 0),
    currentDollarVolume: Number(row.currentDollarVolume || 0),
    liquidityScore: Number(row.liquidityScore || 0),
    liquidityStatus: row.liquidityStatus || '',
    liquidityDollarVolumeRatio: Number(row.liquidityDollarVolumeRatio || 0),
    marketCapBucket: row.marketCapBucket || '',
    catalystLabel: row.catalystLabel || '',
    riskFlags: Array.isArray(row.riskFlags) ? row.riskFlags.slice(0, 12) : [],
  }
}

function decisionMapPathPointForStorage(row = {}, point = {}, fallbackSnapshotSec = quantizedNowSec(), pathScope = decisionMapPathScope()) {
  const timestamp = Number(point.timestamp || point.time || point.snapshotSec || point.snapshot_sec || fallbackSnapshotSec)
  const normalized = decisionPointFromValues({
    ...point,
    ticker: row.ticker || point.ticker,
    timestamp,
    combinedSentiment: point.combinedSentiment,
    priceChangePct: point.priceChangePct,
    marketCapRelativeVolumeScore: point.marketCapRelativeVolumeScore,
    relativeVolume: point.relativeVolume,
    currentDollarVolume: point.currentDollarVolume,
    convictionScore: point.convictionScore ?? row.convictionScore,
    sentimentEstimated: point.sentimentEstimated,
  }, row)
  return {
    ...normalized,
    ticker: normalizeTicker(row.ticker || point.ticker),
    pathScope,
    company: row.company || '',
    snapshotSec: timestamp,
    timestamp,
    generatedAt: new Date(timestamp * 1000).toISOString(),
    quadrant: row.quadrant || 'Neutral',
    pathDirection: row.path_direction || 'insufficient_history',
    pathColor: row.path_color || 'gray',
    activityScore: Number(row.activityScore || 0),
    convictionScore: Number(point.convictionScore ?? row.convictionScore ?? 0),
    liquidityScore: Number(row.liquidityScore || 0),
    liquidityStatus: row.liquidityStatus || '',
    liquidityDollarVolumeRatio: Number(row.liquidityDollarVolumeRatio || 0),
    marketCapBucket: row.marketCapBucket || '',
    catalystLabel: row.catalystLabel || '',
    source: point.timestampSource || point.timestamp_source || 'decision_map_path_frame',
    riskFlags: Array.isArray(row.riskFlags) ? row.riskFlags.slice(0, 12) : [],
  }
}

function decisionMapStoragePoints(rows = [], snapshotSec = quantizedNowSec(), marketDayOnly = false, pathScope = decisionMapPathScope()) {
  const byKey = new Map()
  for (const row of rows) {
    const ticker = normalizeTicker(row?.ticker)
    if (!ticker) continue
    const pathPoints = Array.isArray(row.path_points) ? row.path_points : []
    for (const point of pathPoints.slice(-DECISION_MAP_PATH_MAX)) {
      const timestamp = Number(point?.timestamp || point?.time || 0)
      if (!timestamp) continue
      if (marketDayOnly && !isLikelyMarketMovementDate(timestamp)) continue
      const storagePoint = decisionMapPathPointForStorage(row, point, snapshotSec, pathScope)
      if (storagePoint.ticker) byKey.set(`${pathScope}:${storagePoint.ticker}:${storagePoint.snapshotSec}`, storagePoint)
    }
    if (!pathPoints.length && (!marketDayOnly || isLikelyMarketMovementDate(snapshotSec))) {
      const storagePoint = decisionMapPointForStorage(row, snapshotSec, pathScope)
      if (storagePoint.ticker) byKey.set(`${pathScope}:${storagePoint.ticker}:${storagePoint.snapshotSec}`, storagePoint)
    }
  }
  return [...byKey.values()].sort((a, b) => Number(a.snapshotSec || 0) - Number(b.snapshotSec || 0))
}

function ageSeconds(sec) {
  const value = Number(sec || 0)
  if (!value) return null
  return Math.max(0, Math.floor(Date.now() / 1000) - value)
}

function normalizeMarketCap(raw, source = '') {
  const cap = Number(raw || 0)
  if (!Number.isFinite(cap) || cap <= 0) return null
  // FinViz exports commonly store market cap in millions. Example: 5984.29
  // means about $5.98B, not $5,984.
  if (/finviz/i.test(String(source || '')) && cap > 0 && cap < 10_000_000) return cap * 1_000_000
  return cap
}

function marketCapBucket(marketCap) {
  const cap = Number(marketCap || 0)
  if (cap >= 200e9) return 'Mega cap'
  if (cap >= 10e9) return 'Large cap'
  if (cap >= 2e9) return 'Mid cap'
  if (cap >= 300e6) return 'Small cap'
  if (cap >= 50e6) return 'Micro cap'
  if (cap > 0) return 'Nano cap'
  return 'Unknown cap'
}

function marketCapRelVolumeTarget(bucket) {
  const key = String(bucket || '').toLowerCase()
  if (key.includes('mega')) return 1.2
  if (key.includes('large')) return 1.5
  if (key.includes('mid')) return 1.8
  if (key.includes('small')) return 2.2
  if (key.includes('micro')) return 5
  if (key.includes('nano')) return 8
  return 2
}

function dollarVolumeTarget(bucket) {
  const key = String(bucket || '').toLowerCase()
  if (key.includes('mega')) return 30_000_000
  if (key.includes('large')) return 15_000_000
  if (key.includes('mid')) return 8_000_000
  if (key.includes('small')) return 3_000_000
  if (key.includes('micro')) return 1_000_000
  if (key.includes('nano')) return 500_000
  return 2_000_000
}

function capAdjustedRelVolumeScore(relativeVolume, bucket, dollarVolume = null) {
  const target = marketCapRelVolumeTarget(bucket)
  const relScore = clamp((Number(relativeVolume || 0) / target) * 62, 0, 100)
  const dollarTarget = dollarVolumeTarget(bucket)
  const dollarScore = clamp((Number(dollarVolume || 0) / dollarTarget) * 38, 0, 100)
  return clamp(relScore + dollarScore, 0, 100)
}

function decisionMapPathPointLimit(query = {}) {
  return Math.max(4, Math.min(DECISION_MAP_MAX_PATH_POINTS, Number(query.path_points || query.pathPoints || DECISION_MAP_DEFAULT_PATH_POINTS)))
}

const DECISION_MAP_VOLUME_TIMEFRAMES = {
  '1m': 1,
  '3m': 3,
  '5m': 5,
  '30m': 30,
  '1h': 60,
}

function decisionMapVolumeTimeframe(query = {}) {
  const raw = String(query.volume_timeframe || query.volumeTimeframe || query.volume_tf || query.volumeTf || '5m').toLowerCase()
  return DECISION_MAP_VOLUME_TIMEFRAMES[raw] ? raw : '5m'
}

function bucketStartSec(sec, minutes) {
  const n = Number(sec || 0)
  const bucket = Math.max(1, Number(minutes || 1)) * 60
  return Number.isFinite(n) && n > 0 ? Math.floor(n / bucket) * bucket : 0
}

function bucketCandlesForVolumeTimeframe(candles = [], timeframeKey = '5m') {
  const minutes = DECISION_MAP_VOLUME_TIMEFRAMES[timeframeKey] || 5
  const buckets = new Map()
  for (const candle of candles) {
    const time = Number(candle.time || 0)
    const start = bucketStartSec(time, minutes)
    if (!start) continue
    const existing = buckets.get(start)
    const rawVolume = Number(candle.volume)
    const hasVolume = Number.isFinite(rawVolume)
    const volume = hasVolume ? Math.max(0, rawVolume) : null
    const close = Number(candle.close || 0)
    if (!existing) {
      buckets.set(start, {
        ...candle,
        time: start,
        chartBarTime: isoFromSec(start),
        close,
        volume,
        sourceBars: 1,
        volumeBars: hasVolume ? 1 : 0,
      })
    } else {
      existing.close = close || existing.close
      if (hasVolume) existing.volume = (existing.volume == null ? 0 : existing.volume) + volume
      existing.sourceBars += 1
      existing.volumeBars += hasVolume ? 1 : 0
    }
  }
  return [...buckets.values()].sort((a, b) => Number(a.time || 0) - Number(b.time || 0))
}

function relativeVolumeAxisValue(relativeVolume) {
  const rv = Number(relativeVolume)
  if (!Number.isFinite(rv)) return null
  if (rv === 1) return 0
  if (rv > 1) return (Math.log1p(Math.max(0, rv - 1)) / Math.log1p(999)) * 7
  return -(Math.log1p(Math.max(0, 1 - rv)) / Math.log1p(1)) * 2.2
}

function liquidityProfile(row = {}) {
  const currentDollarVolume = Number(row.currentDollarVolume || 0)
  const target = Math.max(1, Number(row.dollarVolumeTarget || dollarVolumeTarget(row.marketCapBucket)))
  const score = clamp((currentDollarVolume / target) * 100, 0, 100)
  const ratio = currentDollarVolume / target
  const status = ratio >= 1.2
    ? 'excellent'
    : ratio >= 0.8
      ? 'acceptable'
      : ratio >= 0.45
        ? 'thin'
        : 'poor'
  const risk = status === 'poor'
    ? 'POOR_LIQUIDITY'
    : status === 'thin'
      ? 'THIN_LIQUIDITY'
      : ''
  return {
    liquidityScore: Number(score.toFixed(1)),
    liquidityStatus: status,
    liquidityDollarVolume: Number(currentDollarVolume.toFixed(2)),
    liquidityTargetDollarVolume: Number(target.toFixed(2)),
    liquidityDollarVolumeRatio: Number(ratio.toFixed(3)),
    liquidityRiskFlag: risk,
  }
}

function normalizedMarketCapBucket(value = '') {
  const text = String(value || '').toLowerCase()
  for (const bucket of MARKET_CAP_BUCKETS) {
    if (text.includes(bucket)) return bucket
  }
  return ''
}

function normalizeSharesFloat(raw) {
  const value = Number(raw)
  if (!Number.isFinite(value) || value <= 0) return null
  return value
}

function sharesFloatBucket(raw) {
  const value = normalizeSharesFloat(raw)
  if (value == null) return 'Unknown float'
  if (value < 10_000_000) return 'Ultra-low float'
  if (value < 25_000_000) return 'Low float'
  if (value < 50_000_000) return 'Mid float'
  if (value < 100_000_000) return 'High float'
  return 'Very high float'
}

function normalizedFloatBucket(value = '') {
  const text = String(value || '').toLowerCase().replace(/[\s-]+/g, '_')
  if (!text || text === 'all') return ''
  if (text.includes('ultra') || text === 'under_10m' || text === 'lt10m') return 'ultra_low'
  if (text === 'low' || text.includes('10_25') || text === '10m_25m') return 'low'
  if (text === 'mid' || text.includes('25_50') || text === '25m_50m') return 'mid'
  if (text === 'high' || text.includes('50_100') || text === '50m_100m') return 'high'
  if (text.includes('very') || text.includes('100m') || text.includes('100_')) return 'very_high'
  if (text.includes('unknown')) return 'unknown'
  for (const bucket of FLOAT_BUCKETS) {
    if (text.includes(bucket)) return bucket
  }
  return ''
}

function relVolumeBucket(relativeVolume) {
  const rv = Number(relativeVolume || 0)
  if (rv >= 20) return 'extreme'
  if (rv >= 5) return 'high'
  if (rv >= 2) return 'medium'
  return 'low'
}

function sessionMetrics(doc = {}, requestedSession = 'auto') {
  const regular = {
    session: 'regular',
    price: toNumber(doc.price, null),
    changePct: toNumber(doc.change_pct ?? doc.change_percent, 0) || 0,
    volume: toNumber(doc.volume, 0) || 0,
  }
  const premarket = {
    session: 'premarket',
    price: toNumber(doc.premarket_price ?? doc.pre_market_price, null),
    changePct: toNumber(doc.premarket_change_pct ?? doc.pre_market_change_pct, null),
    volume: toNumber(doc.premarket_volume ?? doc.pre_market_volume, 0) || 0,
  }
  const postmarket = {
    session: 'postmarket',
    price: toNumber(doc.postmarket_price ?? doc.afterhours_price ?? doc.after_hours_price, null),
    changePct: toNumber(doc.postmarket_change_pct ?? doc.afterhours_change_pct ?? doc.after_hours_change_pct, null),
    volume: toNumber(doc.postmarket_volume ?? doc.afterhours_volume ?? doc.after_hours_volume, 0) || 0,
  }
  const all = [premarket, regular, postmarket].map(item => ({
    ...item,
    price: item.price ?? regular.price,
    changePct: item.changePct == null ? 0 : item.changePct,
    volume: item.volume || (item.session === 'regular' ? regular.volume : 0),
  }))
  const requested = String(requestedSession || 'auto').toLowerCase()
  let active = all.find(item => item.session === requested)
  if (!active) active = all.reduce((best, item) => Math.abs(item.changePct) > Math.abs(best.changePct) ? item : best, regular)
  return { active, premarket: all[0], regular: all[1], postmarket: all[2] }
}

function normalizeTicker(value) {
  const ticker = String(value || '').trim().replace(/^\$/, '').toUpperCase()
  return /^[A-Z][A-Z0-9.-]{0,5}$/.test(ticker) && !NON_STOCK_TICKERS.has(ticker) ? ticker : ''
}

function tickerValues(doc = {}) {
  const out = new Set()
  const push = (value) => {
    if (Array.isArray(value)) return value.forEach(push)
    String(value || '').split(/[,\s]+/).forEach(part => {
      const ticker = normalizeTicker(part)
      if (ticker) out.add(ticker)
    })
  }
  push(doc.ticker)
  push(doc.symbol)
  push(doc.tickers)
  push(doc.symbols)
  push(doc.matched_mover_tickers)
  push(doc.tickers_mentioned)
  const text = `${doc.text || ''} ${doc.content || ''} ${doc.title || ''}`
  for (const match of text.matchAll(/\$([A-Za-z][A-Za-z0-9.-]{0,5})\b/g)) {
    const ticker = normalizeTicker(match[1])
    if (ticker) out.add(ticker)
  }
  return Array.from(out)
}

function companySignalTokens(row = {}) {
  const company = String(row.company || row.name || row.company_name || '').toLowerCase()
  return company
    .replace(/[^a-z0-9 ]+/g, ' ')
    .split(/\s+/)
    .filter(Boolean)
    .filter(token => token.length >= 4)
    .filter(token => ![
      'inc', 'corp', 'corporation', 'company', 'limited', 'ltd', 'holdings', 'holding',
      'group', 'plc', 'class', 'common', 'stock', 'american', 'technologies', 'technology',
      'therapeutics', 'pharmaceuticals', 'biopharma', 'systems', 'international',
      'ordinary', 'shares', 'share', 'depositary', 'adr',
    ].includes(token))
    .slice(0, 5)
}

function articleMentionsTickerOrCompany(row = {}, doc = {}) {
  const text = `${doc.title || ''} ${doc.url || ''} ${doc.text || ''} ${doc.content || ''}`.toLowerCase()
  if (!text.trim()) return false
  const ticker = normalizeTicker(row.ticker)
  if (ticker && new RegExp(`(^|[^a-z0-9])${ticker.toLowerCase()}([^a-z0-9]|$)`, 'i').test(text)) return true
  return companySignalTokens(row).some(token => text.includes(token))
}

function sentimentScore(doc = {}) {
  const direct = toNumber(doc.sentiment_score ?? doc.finbert_score ?? doc.vader_score ?? doc.gemini_sentiment, null)
  if (direct != null) return clamp(direct, -1, 1)
  const text = String(doc.sentiment || doc.label || '').toLowerCase()
  const confidence = clamp(toNumber(doc.ml_confidence ?? doc.confidence, 0.65) || 0.65, 0, 1)
  if (/bull|positive|buy|beat|raise|approval|award/.test(text)) return confidence
  if (/bear|negative|sell|miss|cut|reject|offering|bankrupt/.test(text)) return -confidence
  return 0
}

function recencyWeight(eventSec, windowSec) {
  const age = Math.max(0, Math.floor(Date.now() / 1000) - Number(eventSec || 0))
  if (!eventSec || !windowSec) return 0.35
  return clamp(Math.exp(-age / Math.max(3600, windowSec / 2)), 0.1, 1)
}

function classifyCatalyst(doc = {}) {
  const text = `${doc.event_type || ''} ${doc.catalystCategory || ''} ${doc.category || ''} ${doc.sentiment_reason || ''} ${doc.title || ''}`.toLowerCase()
  if (/sec|8-k|10-q|10-k|filing|edgar/.test(`${doc.source || ''} ${text}`)) return 'SEC filing'
  if (/fda|approval|clearance|clinical|trial|phase|endpoint|drug|therapy|510\(k\)|ind|nda|bla|fast track|orphan|breakthrough/.test(text)) return 'FDA/clinical'
  if (/earnings|eps|revenue|guidance|quarter|ebit|ebitda|operating income|net income|financial results|previous year|growth_catalyst|growth catalyst/.test(text)) return 'earnings/growth'
  if (/contract|award|partnership|collaboration|customer|order|purchase order|supply agreement|license agreement|distribution agreement/.test(text)) return 'contract/partnership'
  if (/launch|commercial|expansion|rollout|facility|market expansion/.test(text)) return 'launch/expansion'
  if (/patent|intellectual property/.test(text)) return 'patent/IP'
  if (/analyst|upgrade|downgrade|price target|buy rating|sell rating|outperform|underperform/.test(text)) return 'analyst action'
  if (/buyback|dividend|repurchase/.test(text)) return 'capital return'
  if (/offering|dilution|bankrupt|default|delist|noncompliance|going concern|convertible|warrant/.test(text)) return 'financing/risk'
  if (/merger|acquisition|takeover|ipo/.test(text)) return 'corporate action'
  if (/lawsuit|class action|investigation|probe|subpoena|fraud|short report/.test(text)) return 'legal/regulatory risk'
  return doc.title ? 'news catalyst' : ''
}

function isSyntheticOrPrivate(row = {}) {
  const text = `${row.ticker || ''} ${row.company || ''} ${row.industry || ''}`.toLowerCase()
  return /etf|2x|3x|daily target|tradr|leverage shares|direxion|proshares|graniteshares|defiance|spacex|private/.test(text)
}

function sourceFilter(universe) {
  const value = String(universe || 'active_finviz').toLowerCase()
  if (value === 'all') return {}
  if (value === 'numeric_all') {
    return {
      quote_source: { $in: ['finviz_elite_screener', 'tradingview_numeric_screener'] },
      $or: [
        { finviz_status: { $exists: false } },
        { finviz_status: { $ne: 'dropped' } },
        { quote_source: 'tradingview_numeric_screener' },
      ],
    }
  }
  if (value === 'tradingview') return { quote_source: 'tradingview_numeric_screener' }
  return {
    quote_source: 'finviz_elite_screener',
    finviz_status: { $in: [null, 'same', 'added'] },
  }
}

function activeScreenerMatch(query = {}, thresholds) {
  const minRel = Math.max(0, Number(query.min_rel_volume ?? query.minRelativeVolume ?? thresholds.minRelativeVolume))
  const universe = String(query.universe || 'active_finviz').toLowerCase()
  const source = sourceFilter(query.universe)
  const match = {
    ...source,
    ticker: { $not: /\./ },
    price: { $gt: 0 },
    volume: { $gt: 0 },
  }
  match.$and = [
    ...(Array.isArray(match.$and) ? match.$and : []),
    universe === 'active_finviz'
      ? {
          $or: [
            { exchange: { $in: Array.from(US_EXCHANGES) } },
            { exchange: { $exists: false } },
            { exchange: null },
            { exchange: '' },
          ],
        }
      : { exchange: { $in: Array.from(US_EXCHANGES) } },
  ]
  if (minRel > 0) {
    const relVolumeOr = universe === 'active_finviz'
      ? [
          { rel_volume: { $gte: minRel } },
          { relative_volume: { $gte: minRel } },
          {
            quote_source: 'finviz_elite_screener',
            change_pct: { $exists: true, $ne: null },
          },
        ]
      : [
          { rel_volume: { $gte: minRel } },
          { relative_volume: { $gte: minRel } },
        ]
    if (Array.isArray(source.$or)) {
      delete match.$or
      match.$and = [...(match.$and || []), { $or: source.$or }, { $or: relVolumeOr }]
    } else {
      match.$or = relVolumeOr
    }
  }
  return match
}

function normalizeScreenerRow(doc = {}, requestedSession = 'auto') {
  const volume = toNumber(doc.volume, 0) || 0
  const avgVolume = toNumber(doc.avg_volume, null)
  const relVolume = toNumber(doc.rel_volume ?? doc.relative_volume, null) ?? (avgVolume ? volume / Math.max(1, avgVolume) : 0)
  const source = doc.quote_source || doc.screener_source || doc.source || ''
  const isFinvizSource = /finviz/i.test(String(source || ''))
  const marketCap = normalizeMarketCap(doc.market_cap, source)
  const bucket = marketCapBucket(marketCap)
  const sharesFloat = normalizeSharesFloat(doc.shares_float ?? doc.float ?? doc.float_shares)
  const sessions = sessionMetrics(doc, requestedSession)
  const currentVolume = sessions.active.volume || volume
  const currentDollarVolume = Number(((sessions.active.price || 0) * currentVolume).toFixed(2))
  const quoteUpdatedAt = toSec(doc.quote_updated_at)
  const finvizSeenAt = isFinvizSource ? toSec(doc.finviz_seen_at) : 0
  return {
    ticker: normalizeTicker(doc.ticker),
    company: doc.company || '',
    price: sessions.active.price,
    regularPrice: sessions.regular.price,
    premarketPrice: sessions.premarket.price,
    postmarketPrice: sessions.postmarket.price,
    marketCap,
    rawMarketCap: toNumber(doc.market_cap, null),
    marketCapBucket: bucket,
    sharesFloat,
    rawSharesFloat: toNumber(doc.shares_float ?? doc.float ?? doc.float_shares, null),
    floatBucket: sharesFloatBucket(sharesFloat),
    floatShort: toNumber(doc.float_short ?? doc.short_interest_pct_float ?? doc.short_interest_pct, null),
    floatPercent: toNumber(doc.float_percent, null),
    marketCapRelVolumeTarget: marketCapRelVolumeTarget(bucket),
    dollarVolumeTarget: dollarVolumeTarget(bucket),
    currentDollarVolume,
    marketCapRelativeVolumeScore: Number(capAdjustedRelVolumeScore(relVolume, bucket, currentDollarVolume).toFixed(2)),
    priceChangePct: sessions.active.changePct,
    regularChangePct: sessions.regular.changePct,
    premarketChangePct: sessions.premarket.changePct,
    postmarketChangePct: sessions.postmarket.changePct,
    relativeVolume: toNumber(relVolume, 0) || 0,
    currentVolume,
    regularVolume: sessions.regular.volume,
    premarketVolume: sessions.premarket.volume,
    postmarketVolume: sessions.postmarket.volume,
    averageVolume: avgVolume,
    exchange: doc.exchange || '',
    sector: doc.sector || '',
    industry: doc.industry || '',
    screenerSource: source,
    screenerStatus: isFinvizSource ? (doc.finviz_status || null) : null,
    quoteUpdatedAt,
    finvizSeenAt,
    quoteAgeSeconds: ageSeconds(quoteUpdatedAt),
    finvizAgeSeconds: ageSeconds(finvizSeenAt),
    activeSession: sessions.active.session,
    sessionMovement: sessions.active,
  }
}

async function activeScreenerRows(db, query, thresholds) {
  const limit = decisionMapRowLimit(query)
  const internalLimit = Math.max(600, Math.min(1600, limit * 4))
  const sortField = String(query.sort || 'activity').toLowerCase()
  const focusTicker = normalizeTicker(query.focusTicker || query.ticker || query.search || query.q || '')

  const loadRows = (match) => db.collection('screeners').aggregate([
      { $match: focusTicker ? { ...match, ticker: focusTicker } : match },
      {
        $addFields: {
          _sourcePriority: {
            $switch: {
              branches: [
                { case: { $and: [{ $eq: ['$quote_source', 'finviz_elite_screener'] }, { $in: ['$finviz_status', ['same', 'added', null]] }] }, then: 3 },
                { case: { $eq: ['$quote_source', 'finviz_elite_screener'] }, then: 2 },
                { case: { $eq: ['$quote_source', 'tradingview_numeric_screener'] }, then: 1 },
              ],
              default: 0,
            },
          },
          activitySort: {
            $multiply: [
              { $ln: { $add: [{ $ifNull: ['$rel_volume', 0] }, 1] } },
              { $add: [{ $abs: { $ifNull: ['$change_pct', 0] } }, 1] },
            ],
          },
          absChangeSort: { $abs: { $ifNull: ['$change_pct', 0] } },
        },
      },
      { $sort: { ticker: 1, _sourcePriority: -1, quote_updated_at: -1, finviz_seen_at: -1, activitySort: -1 } },
      { $group: { _id: '$ticker', doc: { $first: '$$ROOT' } } },
      { $replaceRoot: { newRoot: '$doc' } },
      { $sort: { absChangeSort: -1, activitySort: -1, volume: -1 } },
      { $limit: internalLimit },
    ]).toArray()

  let rows = await loadRows(activeScreenerMatch(query, thresholds))
  if (!rows.length && String(query.universe || 'active_finviz').toLowerCase() === 'active_finviz') {
    rows = await loadRows(activeScreenerMatch({ ...query, universe: 'numeric_all' }, thresholds))
    rows = rows.map(row => ({ ...row, decision_map_universe_fallback: 'numeric_all' }))
  }

  const direction = String(query.direction || 'all').toLowerCase()
  const minAbsChange = Math.max(0, Number(query.min_abs_change ?? query.minAbsPriceChange ?? thresholds.minAbsPriceChange))
  const maxAbsChange = Math.max(minAbsChange, Number(thresholds.maxAbsPriceChange || 300))
  const normalized = rows.map(row => normalizeScreenerRow(row, query.session)).filter(row => {
    if (!row.ticker) return false
    const change = Number(row.priceChangePct || 0)
    if (Math.abs(change) > maxAbsChange) return false
    if (direction === 'up') return change >= minAbsChange
    if (direction === 'down') return change <= -minAbsChange
    return Math.abs(change) >= minAbsChange
  })
  const marketCapFilter = normalizedMarketCapBucket(query.market_cap_bucket || query.market_cap || '')
  const floatFilter = normalizedFloatBucket(query.float_bucket || query.shares_float_bucket || query.floatBucket || '')
  const relBucketFilter = String(query.rel_volume_bucket || query.relative_volume_bucket || '').toLowerCase()
  let filtered = normalized.filter(row => {
    if (marketCapFilter && normalizedMarketCapBucket(row.marketCapBucket) !== marketCapFilter) return false
    if (floatFilter && normalizedFloatBucket(row.floatBucket) !== floatFilter) return false
    if (['low', 'medium', 'high', 'extreme'].includes(relBucketFilter) && relVolumeBucket(row.relativeVolume) !== relBucketFilter) return false
    return true
  })
  if (focusTicker) filtered = filtered.filter(row => row.ticker === focusTicker)
  filtered.sort((a, b) => {
    if (sortField === 'rel_volume') return b.relativeVolume - a.relativeVolume
    if (sortField === 'change' || sortField === 'postmarket_change' || sortField === 'premarket_change') return b.priceChangePct - a.priceChangePct
    if (sortField === 'market_cap_adjusted_relvol') return b.marketCapRelativeVolumeScore - a.marketCapRelativeVolumeScore
    return (b.marketCapRelativeVolumeScore * Math.max(1, Math.abs(b.priceChangePct))) - (a.marketCapRelativeVolumeScore * Math.max(1, Math.abs(a.priceChangePct)))
  })
  return filtered.slice(0, limit)
}

async function articleEvidence(db, tickersOrRows, windowHours, catalystLookbackHours = null) {
  const wantedRows = new Map()
  for (const item of tickersOrRows || []) {
    const ticker = normalizeTicker(typeof item === 'string' ? item : item?.ticker)
    if (ticker) wantedRows.set(ticker, typeof item === 'string' ? { ticker } : item)
  }
  const wanted = new Set(wantedRows.keys())
  const windowSec = Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.newsWindowHours)) * 3600)
  const windowMinutes = Math.max(1, Math.round(windowSec / 60))
  const lookbackHours = Math.max(
    Number(windowHours || DEFAULT_THRESHOLDS.newsWindowHours || 0),
    Number(catalystLookbackHours || defaultCatalystLookbackHours()),
  )
  const catalystLookbackSec = Math.max(windowSec, Math.round(lookbackHours * 3600))
  const sinceSec = Math.floor(Date.now() / 1000) - windowSec
  const catalystSinceSec = Math.floor(Date.now() / 1000) - catalystLookbackSec
  const docs = await db.collection('articles').find({
    $or: [
      { publish_date: { $gte: catalystSinceSec } },
      { fetched_date: { $gte: catalystSinceSec } },
      { detected_at: { $gte: catalystSinceSec } },
      { createdAt: { $gte: new Date(catalystSinceSec * 1000) } },
    ],
  }, {
    projection: {
      ticker: 1, tickers: 1, matched_mover_tickers: 1, tickers_mentioned: 1,
      title: 1, source: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1,
      event_type: 1, catalystCategory: 1, category: 1, sentiment_reason: 1, article_kind: 1, publish_date: 1, fetched_date: 1, detected_at: 1, url: 1,
    },
  }).sort({ publish_date: -1, fetched_date: -1, detected_at: -1 }).limit(12000).toArray()

  const map = new Map()
  for (const doc of docs) {
    const score = sentimentScore(doc)
    const eventSec = toSec(doc.publish_date) || toSec(doc.fetched_date) || toSec(doc.detected_at)
    const inRollingWindow = eventSec >= sinceSec
    const weight = recencyWeight(eventSec, windowSec)
    const catalyst = classifyCatalyst(doc)
    for (const ticker of tickerValues(doc)) {
      if (!wanted.has(ticker)) continue
      const wantedRow = wantedRows.get(ticker)
      if (wantedRow?.company && !articleMentionsTickerOrCompany(wantedRow, doc)) continue
      const current = map.get(ticker) || {
        count: 0, weightedScore: 0, weight: 0, bullish: 0, bearish: 0,
        structuredCount: 0, unstructuredCount: 0,
        latestSec: 0, latestTitles: [], catalysts: new Map(), sources: new Set(),
        catalystLatestSec: 0, catalystTitles: [], recentCatalysts: new Map(),
        structuredSources: new Set(), unstructuredSources: new Set(),
        windowHours: Number((windowSec / 3600).toFixed(3)),
        windowMinutes,
        windowStartSec: sinceSec,
        windowEndSec: Math.floor(Date.now() / 1000),
        catalystLookbackHours: Number((catalystLookbackSec / 3600).toFixed(3)),
        catalystWindowStartSec: catalystSinceSec,
        catalystWindowEndSec: Math.floor(Date.now() / 1000),
      }
      const sourceText = `${doc.source || ''} ${doc.article_kind || ''}`
      const isStructured = STRUCTURED_SOURCE_RE.test(sourceText) || !SOCIAL_SOURCE_RE.test(sourceText)
      if (inRollingWindow) {
        current.count += 1
        if (isStructured) current.structuredCount += 1
        else current.unstructuredCount += 1
        current.weightedScore += score * weight
        current.weight += weight
        if (score > 0.12) current.bullish += 1
        if (score < -0.12) current.bearish += 1
        current.latestSec = Math.max(current.latestSec, eventSec)
        current.sources.add(doc.source || 'Unknown')
        if (isStructured) current.structuredSources.add(doc.source || 'Unknown')
        else current.unstructuredSources.add(doc.source || 'Unknown')
        if (doc.title && current.latestTitles.length < 3) {
          current.latestTitles.push({ title: doc.title, source: doc.source || '', url: doc.url || '', publishedAt: eventSec, sentiment: score, kind: isStructured ? 'structured' : 'unstructured' })
        }
        if (catalyst) current.catalysts.set(catalyst, (current.catalysts.get(catalyst) || 0) + 1)
      }
      if (catalyst) {
        current.recentCatalysts.set(catalyst, (current.recentCatalysts.get(catalyst) || 0) + 1)
        current.catalystLatestSec = Math.max(current.catalystLatestSec, eventSec)
        current.sources.add(doc.source || 'Unknown')
        if (isStructured) current.structuredSources.add(doc.source || 'Unknown')
        else current.unstructuredSources.add(doc.source || 'Unknown')
        if (doc.title && current.catalystTitles.length < 3) {
          current.catalystTitles.push({ title: doc.title, source: doc.source || '', url: doc.url || '', publishedAt: eventSec, sentiment: score, kind: isStructured ? 'structured' : 'unstructured', catalyst })
        }
      }
      map.set(ticker, current)
    }
  }
  for (const current of map.values()) {
    current.densityPerMinute = Number((current.count / windowMinutes).toFixed(4))
  }
  return map
}

async function socialEvidence(db, tickers, windowHours) {
  const wanted = new Set(tickers)
  const windowSec = Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.socialWindowHours)) * 3600)
  const windowMinutes = Math.max(1, Math.round(windowSec / 60))
  const sinceSec = Math.floor(Date.now() / 1000) - windowSec
  const docs = await db.collection('socials').find({
    $or: [
      { timestamp: { $gte: sinceSec } },
      { fetched_at: { $gte: sinceSec } },
      { created_at: { $gte: sinceSec } },
      { detected_at: { $gte: sinceSec } },
    ],
  }, {
    projection: {
      ticker: 1, symbol: 1, tickers_mentioned: 1, text: 1, content: 1, title: 1,
      platform: 1, source: 1, sentiment: 1, sentiment_score: 1, timestamp: 1, fetched_at: 1, created_at: 1, detected_at: 1,
    },
  }).sort({ timestamp: -1, fetched_at: -1, created_at: -1 }).limit(15000).toArray()

  const map = new Map()
  for (const doc of docs) {
    const score = sentimentScore(doc)
    const eventSec = toSec(doc.timestamp) || toSec(doc.fetched_at) || toSec(doc.created_at) || toSec(doc.detected_at)
    const weight = recencyWeight(eventSec, windowSec)
    for (const ticker of tickerValues(doc)) {
      if (!wanted.has(ticker)) continue
      const current = map.get(ticker) || {
        count: 0, weightedScore: 0, weight: 0, bullish: 0, bearish: 0,
        platforms: new Set(), latestSec: 0,
        windowHours: Number((windowSec / 3600).toFixed(3)),
        windowMinutes,
        windowStartSec: sinceSec,
        windowEndSec: Math.floor(Date.now() / 1000),
      }
      current.count += 1
      current.weightedScore += score * weight
      current.weight += weight
      if (score > 0.12) current.bullish += 1
      if (score < -0.12) current.bearish += 1
      current.platforms.add(doc.platform || doc.source || 'Unknown')
      current.latestSec = Math.max(current.latestSec, eventSec)
      map.set(ticker, current)
    }
  }
  for (const current of map.values()) {
    current.messageDensityPerMinute = Number((current.count / windowMinutes).toFixed(4))
  }
  return map
}

async function intradayEvidenceTimeline(db, scoredRows = [], sinceSec = 0, endSec = Math.floor(Date.now() / 1000)) {
  if (!db || !Array.isArray(scoredRows) || !scoredRows.length) return new Map()
  const rowsByTicker = new Map()
  for (const row of scoredRows) {
    const ticker = normalizeTicker(row.ticker)
    if (ticker) rowsByTicker.set(ticker, row)
  }
  const wanted = new Set(rowsByTicker.keys())
  if (!wanted.size) return new Map()

  const [socialDocs, articleDocs] = await Promise.all([
    db.collection('socials').find({
      $or: [
        { timestamp: { $gte: sinceSec, $lte: endSec } },
        { fetched_at: { $gte: sinceSec, $lte: endSec } },
        { created_at: { $gte: sinceSec, $lte: endSec } },
        { detected_at: { $gte: sinceSec, $lte: endSec } },
      ],
    }, {
      projection: {
        ticker: 1, symbol: 1, tickers_mentioned: 1,
        sentiment: 1, sentiment_score: 1, platform: 1, source: 1,
        timestamp: 1, fetched_at: 1, created_at: 1, detected_at: 1,
      },
    }).sort({ timestamp: 1, fetched_at: 1, created_at: 1 }).limit(25000).toArray().catch(() => []),
    db.collection('articles').find({
      $or: [
        { publish_date: { $gte: sinceSec, $lte: endSec } },
        { fetched_date: { $gte: sinceSec, $lte: endSec } },
        { detected_at: { $gte: sinceSec, $lte: endSec } },
        { createdAt: { $gte: new Date(sinceSec * 1000), $lte: new Date(endSec * 1000) } },
      ],
    }, {
      projection: {
        ticker: 1, tickers: 1, matched_mover_tickers: 1, tickers_mentioned: 1,
        title: 1, source: 1, sentiment: 1, sentiment_score: 1, ml_confidence: 1,
        publish_date: 1, fetched_date: 1, detected_at: 1, createdAt: 1,
      },
    }).sort({ publish_date: 1, fetched_date: 1, detected_at: 1 }).limit(25000).toArray().catch(() => []),
  ])

  const timeline = new Map()
  const addEvent = (ticker, event) => {
    if (!wanted.has(ticker)) return
    const rows = timeline.get(ticker) || []
    rows.push(event)
    timeline.set(ticker, rows)
  }

  for (const doc of socialDocs) {
    const timestamp = toSec(doc.timestamp) || toSec(doc.fetched_at) || toSec(doc.created_at) || toSec(doc.detected_at)
    if (!timestamp || timestamp < sinceSec || timestamp > endSec) continue
    for (const ticker of tickerValues(doc)) {
      addEvent(ticker, {
        timestamp,
        sentiment: sentimentScore(doc),
        kind: 'social',
        source: doc.platform || doc.source || 'social',
      })
    }
  }

  for (const doc of articleDocs) {
    const timestamp = toSec(doc.publish_date) || toSec(doc.fetched_date) || toSec(doc.detected_at) || toSec(doc.createdAt)
    if (!timestamp || timestamp < sinceSec || timestamp > endSec) continue
    for (const ticker of tickerValues(doc)) {
      const row = rowsByTicker.get(ticker)
      if (row?.company && !articleMentionsTickerOrCompany(row, doc)) continue
      addEvent(ticker, {
        timestamp,
        sentiment: sentimentScore(doc),
        kind: 'news',
        source: doc.source || 'news',
      })
    }
  }

  for (const [ticker, rows] of timeline.entries()) {
    rows.sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
    timeline.set(ticker, rows)
  }
  return timeline
}

function evidenceSnapshotAt(timeline = new Map(), ticker = '', timestamp = 0, fallbackSentiment = 0, windowMinutes = 60) {
  const rows = timeline.get(ticker) || []
  const ts = Number(timestamp || 0)
  const windowSec = Math.max(900, Number(windowMinutes || 60) * 60)
  const start = ts - windowSec
  const pointWindowMeta = {
    sourceSocialWindowStart: isoFromSec(start),
    sourceSocialWindowEnd: isoFromSec(ts),
    sourceNewsWindowStart: isoFromSec(start),
    sourceNewsWindowEnd: isoFromSec(ts),
  }
  const events = rows.filter(event => Number(event.timestamp || 0) <= ts && Number(event.timestamp || 0) >= start)
  if (!events.length) {
    return {
      combinedSentiment: fallbackSentiment,
      sentimentEstimated: true,
      evidenceEventCount: 0,
      messageDensityPerHour: 0,
      newsDensityPerHour: 0,
      socialDensityPerHour: 0,
      evidenceWindowMinutes: Math.round(windowSec / 60),
      ...pointWindowMeta,
    }
  }
  let weighted = 0
  let weight = 0
  let news = 0
  let social = 0
  for (const event of events) {
    const age = Math.max(0, ts - Number(event.timestamp || 0))
    const w = Math.exp(-age / Math.max(900, windowSec / 2))
    weighted += Number(event.sentiment || 0) * w
    weight += w
    if (event.kind === 'news') news += 1
    else social += 1
  }
  const hours = windowSec / 3600
  return {
    combinedSentiment: weight ? weighted / weight : fallbackSentiment,
    sentimentEstimated: false,
    evidenceEventCount: events.length,
    messageDensityPerHour: Number(((news + social) / hours).toFixed(3)),
    newsDensityPerHour: Number((news / hours).toFixed(3)),
    socialDensityPerHour: Number((social / hours).toFixed(3)),
    evidenceWindowMinutes: Math.round(windowSec / 60),
    ...pointWindowMeta,
  }
}

async function rollingVolumeEvidence(db, tickers, windowHours = DEFAULT_THRESHOLDS.rollingWindowHours) {
  const windowSec = Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.rollingWindowHours)) * 3600)
  const sinceSec = Math.floor(Date.now() / 1000) - windowSec
  const wanted = new Set(tickers)
  const snapshots = await filteredMomentumSnapshots(
    db,
    wanted,
    sinceSec,
    Math.max(12, Math.ceil(windowSec / DECISION_MAP_POINT_INTERVAL_SECONDS) + 4),
    -1,
  )
  const map = new Map()
  for (const snapshot of snapshots.reverse()) {
    for (const row of snapshot.rows || []) {
      const ticker = normalizeTicker(row.ticker)
      if (!wanted.has(ticker)) continue
      const current = map.get(ticker) || []
      current.push({ snapshotSec: Number(snapshot.snapshot_sec || 0), volume: toNumber(row.volume, 0) || 0, relVolume: toNumber(row.rel_volume, 0) || 0 })
      map.set(ticker, current)
    }
  }
  const result = new Map()
  for (const [ticker, rows] of map.entries()) {
    const first = rows[0] || {}
    const last = rows[rows.length - 1] || {}
    result.set(ticker, {
      points: rows,
      rollingVolume: toNumber(last.volume, 0) || 0,
      volumeAcceleration: first.volume ? Number(((toNumber(last.volume, 0) || 0) / Math.max(1, first.volume)).toFixed(3)) : toNumber(last.relVolume, 0) || 0,
    })
  }
  return result
}

async function filteredMomentumSnapshots(db, tickers = new Set(), sinceSec = 0, limit = 120, sortDir = -1) {
  if (!db || !tickers?.size) return []
  const wantedTickers = [...tickers].map(normalizeTicker).filter(Boolean)
  if (!wantedTickers.length) return []
  return db.collection('finviz_momentum_snapshots').aggregate([
    { $match: { snapshot_sec: { $gte: sinceSec } } },
    { $sort: { snapshot_sec: sortDir >= 0 ? 1 : -1 } },
    { $limit: Math.max(1, Number(limit) || 120) },
    {
      $project: {
        snapshot_sec: 1,
        rows: {
          $filter: {
            input: { $ifNull: ['$rows', []] },
            as: 'row',
            cond: {
              $in: [
                { $toUpper: { $ifNull: ['$$row.ticker', ''] } },
                wantedTickers,
              ],
            },
          },
        },
      },
    },
  ], { allowDiskUse: true }).toArray()
}

function quadrantFor(combinedSentiment, priceChangePct, thresholds) {
  const positiveSentiment = Number(thresholds.positiveSentiment)
  const negativeSentiment = Number(thresholds.negativeSentiment)
  const priceThreshold = Number(thresholds.priceChange)
  if (combinedSentiment >= positiveSentiment && priceChangePct >= priceThreshold) {
    return { quadrant: 'Q1', alignmentStatus: 'sentiment/price aligned bullish', aligned: true, direction: 'bullish' }
  }
  if (combinedSentiment <= negativeSentiment && priceChangePct <= -priceThreshold) {
    return { quadrant: 'Q3', alignmentStatus: 'sentiment/price aligned bearish', aligned: true, direction: 'bearish' }
  }
  if (combinedSentiment <= negativeSentiment && priceChangePct >= priceThreshold) {
    return { quadrant: 'Q2', alignmentStatus: 'divergence: negative sentiment but price rising', aligned: false, direction: 'contrarian_up' }
  }
  if (combinedSentiment >= positiveSentiment && priceChangePct <= -priceThreshold) {
    return { quadrant: 'Q4', alignmentStatus: 'divergence: positive sentiment but price falling', aligned: false, direction: 'failed_positive' }
  }
  return { quadrant: 'Neutral', alignmentStatus: 'neutral/mixed', aligned: false, direction: 'mixed' }
}

function scoreRow(row, article, social, volume, thresholds) {
  const liquidity = liquidityProfile(row)
  const windowMeta = rollingWindowMeta(thresholds)
  const newsSentiment = article?.weight ? article.weightedScore / article.weight : 0
  const socialSentiment = social?.weight ? social.weightedScore / social.weight : 0
  const newsCount = article?.count || 0
  const socialCount = social?.count || 0
  const socialDensityPerMinute = social?.messageDensityPerMinute || 0
  const newsDensityPerMinute = article?.densityPerMinute || 0
  const combinedWeight = (newsCount ? 0.62 : 0) + (socialCount ? 0.38 : 0)
  const combinedSentiment = combinedWeight
    ? ((newsSentiment * (newsCount ? 0.62 : 0)) + (socialSentiment * (socialCount ? 0.38 : 0))) / combinedWeight
    : 0
  const quadrant = quadrantFor(combinedSentiment, row.priceChangePct, thresholds)
  const catalystEntries = article?.catalysts?.size ? article.catalysts : article?.recentCatalysts
  const catalystLabel = catalystEntries?.size
    ? Array.from(catalystEntries.entries()).sort((a, b) => b[1] - a[1])[0][0]
    : ''
  const catalystFromLookback = Boolean(catalystLabel && !article?.catalysts?.size && article?.recentCatalysts?.size)
  const latestNewsSec = article?.latestSec || article?.catalystLatestSec || 0
  const newsAgeHours = latestNewsSec ? (Math.floor(Date.now() / 1000) - latestNewsSec) / 3600 : null
  const rollingVolume = volume?.rollingVolume || row.currentVolume || 0
  const volumeAcceleration = volume?.volumeAcceleration || row.relativeVolume || 0
  const marketCapRelVolume = clamp(Number(row.marketCapRelativeVolumeScore || 0) / 100, 0, 1)
  const relVolumeScore = clamp((Math.log1p(row.relativeVolume) / Math.log(8)) * 0.55 + marketCapRelVolume * 0.45, 0, 1)
  const priceMoveScore = clamp(Math.abs(row.priceChangePct) / 12, 0, 1)
  const volumeScore = clamp(Math.log10(Math.max(1, row.currentVolume)) / 8, 0, 1)
  const rollingEvidenceActivity = clamp(
    Math.log1p(socialDensityPerMinute * 60) / Math.log(20) * 0.55 +
    Math.log1p(newsDensityPerMinute * 60) / Math.log(10) * 0.45,
    0,
    1,
  )
  const activityScore = Math.round((relVolumeScore * 36 + priceMoveScore * 34 + volumeScore * 18 + rollingEvidenceActivity * 12) * 100) / 100
  const evidenceScore =
    (quadrant.aligned ? 18 : quadrant.quadrant === 'Neutral' ? 2 : -20) +
    (catalystLabel ? 12 : -18) +
    (marketCapRelVolume >= 0.75 ? 8 : marketCapRelVolume >= 0.5 ? 3 : -4) +
    Math.min(10, newsCount * 2) +
    Math.min(7, socialCount * 0.6) +
    (newsAgeHours != null && newsAgeHours <= 6 ? 4 : newsAgeHours != null && newsAgeHours > 18 ? -6 : 0)
  const convictionScore = Math.round(clamp(activityScore * 0.42 + evidenceScore, 0, 100))
  const riskFlags = []
  const reasons = []
  if (row.relativeVolume < thresholds.minRelativeVolume) riskFlags.push('WEAK_RELATIVE_VOLUME')
  if (row.relativeVolume < row.marketCapRelVolumeTarget) riskFlags.push('BELOW_MARKET_CAP_REL_VOLUME_TARGET')
  if (Number(row.currentDollarVolume || 0) < Number(row.dollarVolumeTarget || 0)) riskFlags.push('WEAK_CURRENT_DOLLAR_VOLUME')
  if (liquidity.liquidityRiskFlag) riskFlags.push(liquidity.liquidityRiskFlag)
  if (Math.abs(row.priceChangePct) < thresholds.minAbsPriceChange) riskFlags.push('LOW_PRICE_ACTIVITY')
  if (!catalystLabel) riskFlags.push('NO_CATALYST')
  if (!newsCount) riskFlags.push('NO_STRUCTURED_NEWS')
  if (!socialCount) riskFlags.push('NO_SOCIAL_CONFIRMATION')
  if (!quadrant.aligned && quadrant.quadrant !== 'Neutral') riskFlags.push('SENTIMENT_PRICE_DIVERGENCE')
  if (newsAgeHours != null && newsAgeHours > 18) riskFlags.push('STALE_NEWS')
  if (row.screenerStatus === 'dropped') riskFlags.push('NOT_ACTIVE_ON_FINVIZ')
  if (isSyntheticOrPrivate(row)) riskFlags.push('SYNTHETIC_OR_PRIVATE_EXPOSURE')
  if (row.finvizAgeSeconds != null && row.finvizAgeSeconds > thresholds.finvizStaleSeconds) riskFlags.push('STALE_FINVIZ_SCREENER')
  if (activityScore >= 65) reasons.push('high numerical activity from screener')
  if (socialDensityPerMinute > 0) reasons.push(`${Number((socialDensityPerMinute * 60).toFixed(2))} social msgs/hour in ${windowMeta.socialWindowUsed} window`)
  reasons.push(`${row.activeSession} move ${row.priceChangePct >= 0 ? '+' : ''}${row.priceChangePct.toFixed(2)}%`)
    reasons.push(`${row.marketCapBucket}: ${row.relativeVolume.toFixed(2)}x rel vol vs ${row.marketCapRelVolumeTarget.toFixed(1)}x bucket target; $${Math.round(row.currentDollarVolume || 0).toLocaleString()} moment dollar volume`)
  if (quadrant.aligned) reasons.push(quadrant.alignmentStatus)
  if (catalystLabel) reasons.push(`catalyst: ${catalystLabel}${catalystFromLookback ? ' (recent lookback)' : ''}`)
  if (socialCount) reasons.push('social confirmation present')
  if (newsCount) reasons.push('structured news present')
  const movementDrivers = [
    `${row.activeSession || 'session'} price ${row.priceChangePct >= 0 ? '+' : ''}${row.priceChangePct.toFixed(2)}%`,
    `${row.relativeVolume.toFixed(2)}x relative volume versus ${row.marketCapRelVolumeTarget.toFixed(1)}x ${row.marketCapBucket || 'cap bucket'} target`,
    `$${Math.round(row.currentDollarVolume || 0).toLocaleString()} current dollar volume versus $${Math.round(row.dollarVolumeTarget || 0).toLocaleString()} target`,
    `${quadrant.quadrant}: ${quadrant.alignmentStatus}`,
    catalystLabel ? `Catalyst: ${catalystLabel}${catalystFromLookback ? ` within ${formatWindowHours(article?.catalystLookbackHours || DECISION_MAP_CATALYST_LOOKBACK_HOURS)} lookback` : ''}` : 'No structured catalyst attached yet',
    newsCount ? `${newsCount} news/article evidence item${newsCount === 1 ? '' : 's'}` : 'No structured news confirmation',
    socialCount ? `${socialCount} social/unstructured mention${socialCount === 1 ? '' : 's'}` : 'No social confirmation',
    liquidity.liquidityStatus ? `Liquidity ${liquidity.liquidityStatus}: ${Math.round(liquidity.liquidityScore || 0)}/100` : null,
  ].filter(Boolean)
  const movementSummary = [
    row.priceChangePct >= 0 ? 'price up' : 'price down',
    row.relativeVolume >= row.marketCapRelVolumeTarget ? 'rel volume above cap target' : 'rel volume below cap target',
    catalystLabel ? 'catalyst present' : 'catalyst missing',
    quadrant.aligned ? 'sentiment/price aligned' : quadrant.quadrant === 'Neutral' ? 'mixed signal' : 'sentiment/price diverged',
  ].join(' · ')
  const hasNews = newsCount > 0
  const hasSocial = socialCount > 0
  const decisionState = !catalystLabel && !hasNews && !hasSocial
    ? 'weak_no_catalyst'
    : (!quadrant.aligned && quadrant.quadrant !== 'Neutral')
      ? 'risky_uncertain'
      : quadrant.quadrant === 'Q1' && convictionScore >= 65
        ? 'strong_bullish_candidate'
        : quadrant.quadrant === 'Q1'
          ? 'moderate_bullish_candidate'
          : quadrant.quadrant === 'Q3'
            ? 'aligned_bearish_candidate'
            : 'neutral_watchlist'
  const supportLabel = hasNews && hasSocial
    ? 'structured news + social support'
    : hasNews
      ? 'structured news support'
      : hasSocial
        ? 'social/unstructured support'
        : 'movement without catalyst support'

  return {
    ...row,
    ...liquidity,
    rollingVolume,
    volumeAcceleration,
    structuredNewsSentiment: Number(newsSentiment.toFixed(3)),
    socialSentiment: Number(socialSentiment.toFixed(3)),
    combinedSentiment: Number(combinedSentiment.toFixed(3)),
    articleCount: newsCount,
    socialCount,
    socialMessageDensityPerMinute: Number(socialDensityPerMinute.toFixed(4)),
    socialMessageDensityPerHour: Number((socialDensityPerMinute * 60).toFixed(3)),
    newsDensityPerMinute: Number(newsDensityPerMinute.toFixed(4)),
    newsDensityPerHour: Number((newsDensityPerMinute * 60).toFixed(3)),
    structuredArticleCount: article?.structuredCount || 0,
    unstructuredArticleCount: article?.unstructuredCount || 0,
    rollingWindowHours: windowMeta.rollingWindowHours,
    rollingWindowUsed: windowMeta.rollingWindowUsed,
    newsWindowHours: windowMeta.newsWindowHours,
    newsWindowUsed: windowMeta.newsWindowUsed,
    socialWindowHours: windowMeta.socialWindowHours,
    socialWindowUsed: windowMeta.socialWindowUsed,
    pathWindowHours: windowMeta.pathWindowHours,
    pathWindowUsed: windowMeta.pathWindowUsed,
    sourceNewsWindowStart: article?.windowStartSec ? isoFromSec(article.windowStartSec) : windowMeta.sourceNewsWindowStart,
    sourceNewsWindowEnd: article?.windowEndSec ? isoFromSec(article.windowEndSec) : windowMeta.sourceNewsWindowEnd,
    catalystLookbackUsed: article?.catalystLookbackHours ? formatWindowHours(article.catalystLookbackHours) : formatWindowHours(DECISION_MAP_CATALYST_LOOKBACK_HOURS),
    catalystSourceWindowStart: article?.catalystWindowStartSec ? isoFromSec(article.catalystWindowStartSec) : null,
    catalystSourceWindowEnd: article?.catalystWindowEndSec ? isoFromSec(article.catalystWindowEndSec) : null,
    catalystFromLookback,
    catalystAgeHours: latestNewsSec ? Number(((Math.floor(Date.now() / 1000) - latestNewsSec) / 3600).toFixed(2)) : null,
    sourceSocialWindowStart: social?.windowStartSec ? isoFromSec(social.windowStartSec) : windowMeta.sourceSocialWindowStart,
    sourceSocialWindowEnd: social?.windowEndSec ? isoFromSec(social.windowEndSec) : windowMeta.sourceSocialWindowEnd,
    relativeVolumeBucket: relVolumeBucket(row.relativeVolume),
    catalystLabel,
    quadrant: quadrant.quadrant,
    alignmentStatus: quadrant.alignmentStatus,
    decisionState,
    supportLabel,
    activityScore,
    convictionScore,
    riskFlags,
    reasons,
    movementDrivers,
    movementSummary,
    latestNewsTitles: article?.latestTitles?.length ? article.latestTitles : (article?.catalystTitles || []),
    newsSources: article ? Array.from(article.sources).slice(0, 8) : [],
    structuredNewsSources: article ? Array.from(article.structuredSources).slice(0, 8) : [],
    unstructuredNewsSources: article ? Array.from(article.unstructuredSources).slice(0, 8) : [],
    socialPlatforms: social ? Array.from(social.platforms).slice(0, 8) : [],
    lastUpdated: new Date(Math.max(row.quoteUpdatedAt || 0, latestNewsSec || 0, social?.latestSec || 0) * 1000).toISOString(),
    screenerFirst: true,
  }
}

function decisionPointFromValues(values = {}, current = {}) {
  const sentiment = toNumber(values.combinedSentiment ?? values.avg_sentiment ?? values.structured_sentiment ?? values.news_sentiment ?? values.social_sentiment, null)
  const sentimentEstimated = values.sentimentEstimated == null ? sentiment == null : Boolean(values.sentimentEstimated)
  const xSentiment = sentimentEstimated ? Number(current.combinedSentiment || 0) : sentiment
  const rawChangePct = toNumber(values.priceChangePct ?? values.change_pct ?? values.change_percent, null)
  const changePct = rawChangePct ?? toNumber(current.priceChangePct, 0) ?? 0
  const rawRelVolume = toNumber(values.relativeVolume ?? values.rel_volume ?? values.relative_volume, null)
  const relVolume = rawRelVolume ?? toNumber(current.relativeVolume, null)
  const marketCapBucketValue = values.marketCapBucket || current.marketCapBucket
  const price = toNumber(values.price ?? values.currentPrice, null)
  const volume = toNumber(values.volume ?? values.currentVolume, null)
  const dollarVolumeDirect = toNumber(values.currentDollarVolume, null)
  const dollarVolume = dollarVolumeDirect ?? (price != null && volume != null ? price * volume : null)
  const zScore = toNumber(values.marketCapRelativeVolumeScore, null) ?? (relVolume == null ? null : capAdjustedRelVolumeScore(relVolume, marketCapBucketValue, dollarVolume))

  // Determine which fields are missing/estimated
  const missingFields = []
  if (sentiment == null) missingFields.push('sentiment')
  if (values.priceChangePct == null && values.change_pct == null && values.change_percent == null) missingFields.push('priceChangePct')
  if (values.relativeVolume == null && values.rel_volume == null && values.relative_volume == null) missingFields.push('relativeVolume')
  if (values.price == null && values.currentPrice == null) missingFields.push('price')
  if (values.volume == null && values.currentVolume == null) missingFields.push('volume')
  if (values.currentDollarVolume == null) missingFields.push('currentDollarVolume')
  if (values.convictionScore == null) missingFields.push('convictionScore')

  // Y-axis: use log1p scaling so small moves are visible but extreme moves don't saturate
  // log1p(|changePct|) / log1p(160) * 4.8 — maps 0%→0, 5%→1.2, 25%→2.8, 100%→4.3, 300%→4.8
  const yRaw = Math.abs(changePct)
  const ySign = changePct < 0 ? -1 : 1
  const yVisual = ySign * (Math.log1p(yRaw) / Math.log1p(160)) * 4.8

  // Z-axis: 1x relative volume is the neutral baseline. Excess RelVol above 1x
  // is compressed with log1p so extreme movers do not swamp the map.
  const zVisual = relativeVolumeAxisValue(relVolume)
  const timestamp = Number(values.timestamp || values.snapshotSec || values.snapshot_sec || values.time || 0)
  const timestampUtc = isoFromSec(timestamp)

  return {
    timestamp,
    time: timestamp,
    ticker: values.ticker || current.ticker || undefined,
    timestampUtc,
    displayTime: values.displayTime || displayTimeFromSec(timestamp),
    rollingWindowUsed: values.rollingWindowUsed || current.rollingWindowUsed || undefined,
    sourceSocialWindowStart: values.sourceSocialWindowStart || current.sourceSocialWindowStart || undefined,
    sourceSocialWindowEnd: values.sourceSocialWindowEnd || current.sourceSocialWindowEnd || undefined,
    sourceNewsWindowStart: values.sourceNewsWindowStart || current.sourceNewsWindowStart || undefined,
    sourceNewsWindowEnd: values.sourceNewsWindowEnd || current.sourceNewsWindowEnd || undefined,
    chartBarTime: values.chartBarTime || timestampUtc,
    volumeTimeframe: values.volumeTimeframe || values.volume_timeframe || undefined,
    volumeTimeframeMinutes: values.volumeTimeframeMinutes ?? values.volume_timeframe_minutes ?? undefined,
    timeframeVolume: values.timeframeVolume == null ? undefined : Number(values.timeframeVolume),
    expectedTimeframeVolume: values.expectedTimeframeVolume == null ? undefined : Number(values.expectedTimeframeVolume),
    relativeVolumeBasis: values.relativeVolumeBasis || values.relative_volume_basis || undefined,
    evidenceEventCount: values.evidenceEventCount == null ? undefined : Number(values.evidenceEventCount),
    messageDensityPerHour: values.messageDensityPerHour == null ? undefined : Number(values.messageDensityPerHour),
    newsDensityPerHour: values.newsDensityPerHour == null ? undefined : Number(values.newsDensityPerHour),
    socialDensityPerHour: values.socialDensityPerHour == null ? undefined : Number(values.socialDensityPerHour),
    evidenceWindowMinutes: values.evidenceWindowMinutes == null ? undefined : Number(values.evidenceWindowMinutes),
    timestampSource: values.timestampSource || values.timestamp_source || (values.snapshotSec || values.snapshot_sec ? 'snapshot_sec' : values.timestamp ? 'timestamp' : values.time ? 'chart_time' : 'unknown'),
    x: Number((clamp(xSentiment, -1, 1) * 7).toFixed(3)),
    y: Number(clamp(yVisual, -4.8, 4.8).toFixed(3)),
    z: Number(clamp(zVisual ?? 0, -2.2, 7).toFixed(3)),
    combinedSentiment: Number(clamp(xSentiment, -1, 1).toFixed(3)),
    priceChangePct: Number(changePct.toFixed(3)),
    marketCapRelativeVolumeScore: zScore == null ? null : Number(clamp(zScore, 0, 100).toFixed(2)),
    relativeVolume: relVolume == null ? null : Number(relVolume.toFixed(3)),
    currentDollarVolume: dollarVolume == null ? null : Number(dollarVolume.toFixed(2)),
    convictionScore: values.convictionScore == null ? null : Number(values.convictionScore),
    sentimentEstimated,
    dataQuality: missingFields.length === 0 ? 'complete' : missingFields.length <= 2 ? 'partial' : 'minimal',
    missingFields: missingFields.length > 0 ? missingFields : undefined,
    visualScale: {
      y: 'log1p(|changePct|) / log1p(160) * 4.8, clamped ±4.8',
      z: 'signed relative volume: below 1x maps negative, 1x maps 0, above 1x maps positive with log compression',
      x: 'sentiment -1..1 * 7, clamped ±7',
    },
  }
}

function pathDirectionFromPoints(points = [], current = {}, meta = {}) {
  if (!Array.isArray(points) || points.length < 2) {
    const count = Array.isArray(points) ? points.length : 0
    return {
      path_direction: 'insufficient_history',
      path_direction_score: 0,
      path_color: 'gray',
      path_explanation: count === 1
        ? 'Only one data point available today. More intraday history needed to determine direction.'
        : 'No intraday history available for this ticker.',
      latest_point: points[points.length - 1] || null,
      previous_point: null,
      path_points_count: count,
      path_window_minutes: 0,
      time_lapse_available: false,
      path_missing_reason: count === 1 ? 'only_one_point_today' : 'no_intraday_history',
      path_quality: 'minimal',
      path_coverage: meta.coverage || null,
      path_data_source: meta.source || 'decision_map_points_and_snapshots',
    }
  }
  if (points.length < DECISION_MAP_SPARSE_PATH_THRESHOLD) {
    const firstTimestamp = Number(points[0]?.timestamp || 0)
    const latestTimestamp = Number(points[points.length - 1]?.timestamp || 0)
    return {
      path_direction: 'insufficient_history',
      path_direction_score: 0,
      path_color: 'gray',
      path_explanation: `Sparse real path history (${points.length} points). More intraday snapshots are needed before calling direction.`,
      latest_point: points[points.length - 1] || null,
      previous_point: points[points.length - 2] || null,
      path_start_point: points[0] || null,
      path_points_count: points.length,
      path_window_minutes: firstTimestamp && latestTimestamp ? Math.max(0, Math.round((latestTimestamp - firstTimestamp) / 60)) : 0,
      time_lapse_available: false,
      path_missing_reason: 'sparse_intraday_history',
      path_quality: 'sparse',
      path_coverage: meta.coverage || null,
      path_data_source: meta.source || 'decision_map_points_and_snapshots',
    }
  }
  const latest = points[points.length - 1]
  const previous = points[points.length - 2]
  const composite = (point) => (
    clamp(point.combinedSentiment, -1, 1) * 0.34 +
    clamp(point.priceChangePct / 12, -1, 1) * 0.32 +
    clamp(point.marketCapRelativeVolumeScore / 100, 0, 1) * 0.22 +
    clamp((point.convictionScore ?? current.convictionScore ?? 50) / 100, 0, 1) * 0.12
  )
  const recent = points.slice(-Math.min(8, points.length))
  const midpoint = Math.max(1, Math.floor(recent.length / 2))
  const firstHalf = recent.slice(0, midpoint)
  const secondHalf = recent.slice(midpoint)
  const avg = (items) => items.reduce((sum, point) => sum + composite(point), 0) / Math.max(1, items.length)
  const halfTrend = avg(secondHalf) - avg(firstHalf)
  const latestStep = composite(latest) - composite(previous)
  const wholePathTrend = composite(latest) - composite(recent[0])
  const delta = Number((halfTrend * 0.52 + latestStep * 0.28 + wholePathTrend * 0.20).toFixed(3))
  const direction = delta > 0.045 ? 'correct_direction' : delta < -0.045 ? 'wrong_direction' : 'neutral'
  const firstTimestamp = Number(points[0]?.timestamp || 0)
  const latestTimestamp = Number(latest?.timestamp || 0)
  const pathWindowMinutes = firstTimestamp && latestTimestamp
    ? Math.max(0, Math.round((latestTimestamp - firstTimestamp) / 60))
    : 0
  return {
    path_direction: direction,
    path_direction_score: delta,
    path_color: direction === 'correct_direction' ? 'blue' : direction === 'wrong_direction' ? 'red' : 'gray',
    path_explanation: direction === 'correct_direction'
      ? 'Recent multi-point path improved on prediction-aligned sentiment, price movement, and market-cap-adjusted relative volume.'
      : direction === 'wrong_direction'
        ? 'Recent multi-point path deteriorated across the prediction-aligned movement composite.'
        : 'Recent movement is too small or mixed to call.',
    latest_point: latest,
    previous_point: previous,
    path_start_point: recent[0],
    path_points_count: points.length,
    path_window_minutes: pathWindowMinutes,
    path_delta_components: {
      half_trend: Number(halfTrend.toFixed(3)),
      latest_step: Number(latestStep.toFixed(3)),
      whole_path_trend: Number(wholePathTrend.toFixed(3)),
    },
    path_quality: meta.quality || (points.length >= DECISION_MAP_MIN_FULL_PATH_POINTS ? 'full' : 'partial'),
    path_coverage: meta.coverage || null,
    path_data_source: meta.source || 'decision_map_points_and_snapshots',
    time_lapse_available: true,
  }
}

function pointMotionKey(point = {}) {
  const n = (value, digits = 3) => {
    if (value == null || value === '') return 'na'
    const numeric = Number(value)
    return Number.isFinite(numeric) ? Number(numeric.toFixed(digits)) : 'na'
  }
  return [
    n(point.combinedSentiment, 4),
    n(point.priceChangePct, 3),
    n(point.relativeVolume, 3),
    n(point.currentDollarVolume, 0),
    n(point.x, 3),
    n(point.y, 3),
    n(point.z, 3),
  ].join(':')
}

function pathMovementStats(points = []) {
  const valid = points.filter(point => point && Number.isFinite(Number(point.timestamp || 0)))
  const keys = new Set(valid.map(pointMotionKey))
  const fields = ['combinedSentiment', 'priceChangePct', 'relativeVolume', 'currentDollarVolume', 'x', 'y', 'z']
  const ranges = {}
  for (const field of fields) {
    const values = valid.map(point => fieldValue(point, field)).filter(value => value != null)
    ranges[field] = values.length ? Math.max(...values) - Math.min(...values) : 0
  }
  return {
    count: valid.length,
    unique: keys.size,
    ranges,
    maxRange: Math.max(0, ...Object.values(ranges)),
  }
}

function pathHasVisibleMovement(points = []) {
  const stats = pathMovementStats(points)
  if (stats.unique < 2) return false
  return (
    stats.ranges.combinedSentiment >= 0.002 ||
    stats.ranges.priceChangePct >= 0.05 ||
    stats.ranges.relativeVolume >= 0.05 ||
    stats.ranges.currentDollarVolume >= 1000 ||
    stats.ranges.x >= 0.01 ||
    stats.ranges.y >= 0.01 ||
    stats.ranges.z >= 0.01
  )
}

function fieldValue(point = {}, field) {
  const raw = point?.[field]
  if (raw == null || raw === '') return null
  const value = Number(raw)
  return Number.isFinite(value) ? value : null
}

function addCandidate(scoreByIndex, index, score) {
  if (!Number.isInteger(index) || index < 0) return
  scoreByIndex.set(index, Math.max(Number(scoreByIndex.get(index) || 0), score))
}

function shapePreservingSample(points = [], maxPoints = 14) {
  if (points.length <= maxPoints) return points
  const lastIndex = points.length - 1
  const scoreByIndex = new Map()
  addCandidate(scoreByIndex, 0, 10_000)
  addCandidate(scoreByIndex, lastIndex, 10_000)

  const fields = ['priceChangePct', 'relativeVolume', 'combinedSentiment', 'currentDollarVolume', 'y', 'z', 'x']
  for (const field of fields) {
    const values = points.map(point => fieldValue(point, field)).filter(value => value != null)
    if (values.length < 3) continue
    const range = Math.max(...values) - Math.min(...values)
    const extremaEpsilon = Math.max(field === 'currentDollarVolume' ? 5000 : 0.001, range * 0.015)
    const majorStep = Math.max(field === 'currentDollarVolume' ? 25000 : 0.01, range * 0.075)

    for (let i = 1; i < lastIndex; i += 1) {
      const prev = fieldValue(points[i - 1], field)
      const curr = fieldValue(points[i], field)
      const next = fieldValue(points[i + 1], field)
      if (prev == null || curr == null || next == null) continue
      const isHigh = curr - prev >= extremaEpsilon && curr - next >= extremaEpsilon
      const isLow = prev - curr >= extremaEpsilon && next - curr >= extremaEpsilon
      if (isHigh || isLow) addCandidate(scoreByIndex, i, 120 + Math.min(80, Math.abs(curr - ((prev + next) / 2)) / Math.max(extremaEpsilon, 0.001)))

      const prevDelta = curr - prev
      const nextDelta = next - curr
      if (Math.sign(prevDelta) && Math.sign(nextDelta) && Math.sign(prevDelta) !== Math.sign(nextDelta)) {
        addCandidate(scoreByIndex, i, 95)
      }
      if (Math.abs(prevDelta) >= majorStep) {
        addCandidate(scoreByIndex, i - 1, 70)
        addCandidate(scoreByIndex, i, 75)
      }
      if (Math.abs(nextDelta) >= majorStep) {
        addCandidate(scoreByIndex, i, 75)
        addCandidate(scoreByIndex, i + 1, 70)
      }
    }
  }

  for (let i = 0; i < maxPoints; i += 1) {
    addCandidate(scoreByIndex, Math.round((i / Math.max(1, maxPoints - 1)) * lastIndex), 45)
  }

  return [...scoreByIndex.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, maxPoints)
    .map(([index]) => index)
    .sort((a, b) => a - b)
    .map(index => points[index])
}

function compactPathPoints(points = [], maxPoints = 14) {
  const sorted = [...points]
    .filter(point => point && Number.isFinite(Number(point.timestamp || 0)))
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))

  const byTimestamp = new Map()
  for (const point of sorted) {
    byTimestamp.set(Number(point.timestamp || 0), point)
  }

  const compacted = Array.from(byTimestamp.values())

  return shapePreservingSample(compacted, maxPoints)
}

function latestMarketMovementDateKey(points = []) {
  const keys = points
    .map(point => Number(point?.timestamp || 0))
    .filter(isLikelyMarketMovementDate)
    .map(nyDateKey)
    .filter(Boolean)
    .sort()
  return keys[keys.length - 1] || ''
}

function selectPathMovementWindow(points = [], windowHours = DEFAULT_THRESHOLDS.pathWindowHours, marketDayOnly = false) {
  const sorted = [...points]
    .filter(point => point && Number.isFinite(Number(point.timestamp || 0)))
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
  const windowSec = Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.pathWindowHours)) * 3600)
  const latestPointSec = Number(sorted[sorted.length - 1]?.timestamp || 0)
  const windowStartSec = latestPointSec ? latestPointSec - windowSec : 0
  const marketSorted = marketDayOnly ? sorted.filter(point => isLikelyMarketMovementDate(point.timestamp)) : sorted
  const effectiveLatestSec = Number(marketSorted[marketSorted.length - 1]?.timestamp || latestPointSec || 0)
  const effectiveWindowStartSec = effectiveLatestSec ? effectiveLatestSec - windowSec : windowStartSec
  const windowed = effectiveWindowStartSec ? marketSorted.filter(point => Number(point.timestamp || 0) >= effectiveWindowStartSec) : marketSorted
  const groups = new Map()
  for (const point of windowed) {
    const key = isLikelyMarketMovementDate(point.timestamp) ? nyDateKey(point.timestamp) : ''
    if (!key) continue
    const rows = groups.get(key) || []
    rows.push(point)
    groups.set(key, rows)
  }
  const days = [...groups.entries()].sort((a, b) => b[0].localeCompare(a[0]))
  if (!days.length) {
    const allGroups = new Map()
    for (const point of marketSorted) {
      const key = isLikelyMarketMovementDate(point.timestamp) ? nyDateKey(point.timestamp) : ''
      if (!key) continue
      const rows = allGroups.get(key) || []
      rows.push(point)
      allGroups.set(key, rows)
    }
    const allDays = [...allGroups.entries()].sort((a, b) => b[0].localeCompare(a[0]))
    const full = allDays.find(([, rows]) => rows.length >= DECISION_MAP_MIN_FULL_PATH_POINTS && pathHasVisibleMovement(rows))
    const partial = allDays.find(([, rows]) => rows.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD && pathHasVisibleMovement(rows))
    const fallback = full || partial || allDays[0]
    if (fallback) {
      const selectedRows = fallback[1]
      return {
        points: selectedRows,
        marketDate: fallback[0],
        coverage: 'latest_market_day_fallback',
        quality: selectedRows.length >= DECISION_MAP_MIN_FULL_PATH_POINTS ? 'full' : selectedRows.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD ? 'partial' : 'sparse',
        requestedWindowHours: Number((windowSec / 3600).toFixed(3)),
        requestedWindowUsed: formatWindowHours(windowSec / 3600),
        windowStartSec: Number(selectedRows[0]?.timestamp || 0) || effectiveWindowStartSec,
        windowEndSec: Number(selectedRows[selectedRows.length - 1]?.timestamp || 0) || effectiveLatestSec || null,
        availableMarketDates: allDays.map(([date, rows]) => ({ date, points: rows.length })).slice(0, 6),
      }
    }
    return {
      points: windowed,
      marketDate: latestMarketMovementDateKey(windowed) || null,
      coverage: 'no_market_day_window',
      quality: windowed.length >= DECISION_MAP_MIN_FULL_PATH_POINTS ? 'partial' : windowed.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD ? 'sparse' : 'minimal',
      requestedWindowHours: Number((windowSec / 3600).toFixed(3)),
      requestedWindowUsed: formatWindowHours(windowSec / 3600),
      windowStartSec: effectiveWindowStartSec,
      windowEndSec: effectiveLatestSec || null,
    }
  }

  const latestDate = days[0][0]
  const full = days.find(([, rows]) => rows.length >= DECISION_MAP_MIN_FULL_PATH_POINTS && pathHasVisibleMovement(rows))
  const partial = days.find(([, rows]) => rows.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD && pathHasVisibleMovement(rows))
  const selected = full || partial || days[0]
  const selectedDate = selected[0]
  const selectedRows = selected[1]
  const quality = selectedRows.length >= DECISION_MAP_MIN_FULL_PATH_POINTS
    ? 'full'
    : selectedRows.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD
      ? 'partial'
      : 'sparse'

  return {
    points: selectedRows,
    marketDate: selectedDate,
    coverage: selectedDate === latestDate ? 'latest_market_day' : 'latest_dense_market_day',
    quality,
    requestedWindowHours: Number((windowSec / 3600).toFixed(3)),
    requestedWindowUsed: formatWindowHours(windowSec / 3600),
    windowStartSec: Number(selectedRows[0]?.timestamp || 0) || effectiveWindowStartSec,
    windowEndSec: Number(selectedRows[selectedRows.length - 1]?.timestamp || 0) || effectiveLatestSec || null,
    availableMarketDates: days.map(([date, rows]) => ({ date, points: rows.length })).slice(0, 6),
  }
}

async function redisTickerPathEvidence(redis, scoredRows = [], maxPoints = 14, windowHours = DEFAULT_THRESHOLDS.pathWindowHours, marketDayOnly = false, pathScope = decisionMapPathScope()) {
  if (!redis || !Array.isArray(scoredRows) || !scoredRows.length) return new Map()
  const tickers = scoredRows.map(row => normalizeTicker(row.ticker)).filter(Boolean)
  if (!tickers.length) return new Map()
  const sinceSec = pathSinceSec(windowHours, marketDayOnly)
  try {
    const zpipe = redis.pipeline()
    for (const ticker of tickers) {
      zpipe.zrevrange(`decision_map:path:${pathScope}:${ticker}`, 0, Math.max(0, maxPoints - 1))
    }
    const zResults = await zpipe.exec()
    const keysByTicker = new Map()
    const getPipe = redis.pipeline()
    zResults.forEach((result, index) => {
      const keys = Array.isArray(result?.[1]) ? result[1].reverse() : []
      const ticker = tickers[index]
      keysByTicker.set(ticker, keys)
      keys.forEach(key => getPipe.get(key))
    })
    const getResults = await getPipe.exec()
    let cursor = 0
    const out = new Map()
    for (const ticker of tickers) {
      const keys = keysByTicker.get(ticker) || []
      const points = []
      for (let i = 0; i < keys.length; i += 1) {
        const parsed = parsePoint(getResults[cursor]?.[1])
        cursor += 1
        const ts = Number(parsed?.timestamp || parsed?.time || 0)
        if (parsed && ts >= sinceSec && (!marketDayOnly || isLikelyMarketMovementDate(ts))) points.push(parsed)
      }
      if (points.length) out.set(ticker, points)
    }
    return out
  } catch (_) {
    return new Map()
  }
}

async function mongoDecisionMapPointEvidence(db, scoredRows = [], maxPoints = 14, windowHours = DEFAULT_THRESHOLDS.pathWindowHours, marketDayOnly = false, pathScope = decisionMapPathScope()) {
  if (!db || !Array.isArray(scoredRows) || !scoredRows.length) return new Map()
  const tickers = scoredRows.map(row => normalizeTicker(row.ticker)).filter(Boolean)
  if (!tickers.length) return new Map()
  const pointDb = decisionMapStorageDb(db)
  if (!pointDb) return new Map()
  const currentByTicker = new Map(scoredRows.map(row => [normalizeTicker(row.ticker), row]))
  const sinceSec = pathSinceSec(windowHours, marketDayOnly)
  try {
    const docs = await pointDb.collection('decision_map_points')
      .find({
        ticker: { $in: tickers },
        path_scope: pathScope,
        $or: [
          { snapshot_sec: { $gte: sinceSec } },
          { snapshotSec: { $gte: sinceSec } },
          { timestamp: { $gte: sinceSec } },
        ],
      }, {
        projection: {
          ticker: 1,
          path_scope: 1,
          snapshot_sec: 1,
          snapshotSec: 1,
          timestamp: 1,
          x: 1,
          y: 1,
          z: 1,
          combinedSentiment: 1,
          priceChangePct: 1,
          marketCapRelativeVolumeScore: 1,
          relativeVolume: 1,
          currentDollarVolume: 1,
          convictionScore: 1,
          volumeTimeframe: 1,
          volumeTimeframeMinutes: 1,
          timeframeVolume: 1,
          expectedTimeframeVolume: 1,
          relativeVolumeBasis: 1,
          evidenceEventCount: 1,
          messageDensityPerHour: 1,
          newsDensityPerHour: 1,
          socialDensityPerHour: 1,
          evidenceWindowMinutes: 1,
          sentimentEstimated: 1,
          timestampSource: 1,
        },
      })
      .sort({ snapshot_sec: 1, snapshotSec: 1, timestamp: 1 })
      .toArray()

    const out = new Map()
    for (const doc of docs) {
      const ticker = normalizeTicker(doc.ticker)
      if (!ticker) continue
      const current = currentByTicker.get(ticker) || {}
      const timestamp = Number(doc.snapshot_sec || doc.snapshotSec || doc.timestamp || 0)
      if (marketDayOnly && !isLikelyMarketMovementDate(timestamp)) continue
      const point = decisionPointFromValues({ ...doc, timestamp, timestampSource: 'decision_map_points' }, current)
      const points = out.get(ticker) || []
      points.push(point)
      if (points.length > maxPoints * 2) points.splice(0, points.length - maxPoints * 2)
      out.set(ticker, points)
    }
    for (const [ticker, points] of out.entries()) {
      out.set(ticker, points.slice(-maxPoints))
    }
    return out
  } catch (_) {
    return new Map()
  }
}

async function screenerHistoricalPathEvidence(db, scoredRows = [], maxPoints = 14, windowHours = DEFAULT_THRESHOLDS.pathWindowHours, marketDayOnly = false) {
  if (!db || !Array.isArray(scoredRows) || !scoredRows.length) return new Map()
  const tickers = scoredRows.map(row => normalizeTicker(row.ticker)).filter(Boolean)
  if (!tickers.length) return new Map()
  const currentByTicker = new Map(scoredRows.map(row => [normalizeTicker(row.ticker), row]))
  const sinceSec = pathSinceSec(windowHours, marketDayOnly)
  try {
    const docs = await db.collection('screeners').find({
      ticker: { $in: tickers },
      $or: [
        { quote_updated_at: { $gte: sinceSec } },
        { finviz_seen_at: { $gte: sinceSec } },
        { updated_at: { $gte: sinceSec } },
      ],
    }, {
      projection: {
        ticker: 1,
        quote_updated_at: 1,
        finviz_seen_at: 1,
        updated_at: 1,
        change_pct: 1,
        change_percent: 1,
        price: 1,
        volume: 1,
        rel_volume: 1,
        relative_volume: 1,
        market_cap: 1,
        avg_sentiment: 1,
        combinedSentiment: 1,
        structured_sentiment: 1,
        social_sentiment: 1,
        convictionScore: 1,
        marketCapRelativeVolumeScore: 1,
        currentDollarVolume: 1,
        quote_source: 1,
        finviz_status: 1,
      },
    }).sort({ quote_updated_at: -1, finviz_seen_at: -1, updated_at: -1 }).limit(5000).toArray()

    const out = new Map()
    for (const doc of docs) {
      const ticker = normalizeTicker(doc.ticker)
      if (!ticker) continue
      const current = currentByTicker.get(ticker) || {}
      const timestamp = toSec(doc.quote_updated_at) || toSec(doc.finviz_seen_at) || toSec(doc.updated_at) || 0
      if (!timestamp) continue
      if (marketDayOnly && !isLikelyMarketMovementDate(timestamp)) continue
      const point = decisionPointFromValues({
        timestamp,
        combinedSentiment: doc.combinedSentiment ?? doc.avg_sentiment ?? doc.structured_sentiment ?? doc.social_sentiment,
        priceChangePct: doc.change_pct ?? doc.change_percent,
        relativeVolume: doc.rel_volume ?? doc.relative_volume,
        price: doc.price,
        volume: doc.volume,
        convictionScore: doc.convictionScore,
        marketCapRelativeVolumeScore: doc.marketCapRelativeVolumeScore,
        currentDollarVolume: doc.currentDollarVolume,
      }, current)
      const points = out.get(ticker) || []
      points.push(point)
      out.set(ticker, points)
    }
    for (const [ticker, points] of out.entries()) {
      points.sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
      out.set(ticker, points.slice(-maxPoints * 2))
    }
    return out
  } catch (_) {
    return new Map()
  }
}

async function chartCandlePathEvidence(db, scoredRows = [], maxPoints = 120, windowHours = DEFAULT_THRESHOLDS.pathWindowHours, volumeTimeframe = '5m', marketDayOnly = false) {
  const out = new Map()
  const requestedMax = Math.max(4, Math.min(DECISION_MAP_MAX_PATH_POINTS, Number(maxPoints || DECISION_MAP_DEFAULT_PATH_POINTS)))
  const windowSec = Math.round(Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.pathWindowHours)) * 3600)
  const sinceSec = pathSinceSec(windowHours, marketDayOnly)
  const timeline = await intradayEvidenceTimeline(db, scoredRows, sinceSec, Math.floor(Date.now() / 1000))
  const volumeTf = DECISION_MAP_VOLUME_TIMEFRAMES[volumeTimeframe] ? volumeTimeframe : '5m'
  const volumeTfMinutes = DECISION_MAP_VOLUME_TIMEFRAMES[volumeTf] || 5
  await mapWithConcurrency(scoredRows, DECISION_MAP_CHART_FETCH_CONCURRENCY, async row => {
    const ticker = normalizeTicker(row.ticker)
    if (!ticker) return
    const candles = await fetchChartProviderCandles(ticker, marketDayOnly ? '5d' : '1d', '1m').catch(() => [])
    if (!Array.isArray(candles) || candles.length < 2) return
    const byDate = new Map()
    for (const candle of candles) {
      const key = nyDateKey(candle.time)
      if (!key || !isLikelyMarketMovementDate(candle.time)) continue
      const rows = byDate.get(key) || []
      rows.push(candle)
      byDate.set(key, rows)
    }
    const marketDays = [...byDate.entries()].sort((a, b) => b[0].localeCompare(a[0]))
    const selected = marketDays.find(([, rows]) => rows.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD) || marketDays[0]
    if (!selected) return
    const dayCandles = selected[1].sort((a, b) => Number(a.time || 0) - Number(b.time || 0))
    const latestSec = Number(dayCandles[dayCandles.length - 1]?.time || 0)
    const windowStart = latestSec - windowSec
    const windowCandles = dayCandles.filter(candle => Number(candle.time || 0) >= windowStart)
    const rawPathCandles = windowCandles.length >= DECISION_MAP_SPARSE_PATH_THRESHOLD ? windowCandles : dayCandles
    const pathCandles = bucketCandlesForVolumeTimeframe(rawPathCandles, volumeTf)
    const firstClose = Number(pathCandles[0]?.close || 0)
    const averageVolume = Number(row.averageVolume || 0)
    const sameDayVolumes = pathCandles
      .map(candle => Number(candle.volume))
      .filter(value => Number.isFinite(value) && value > 0)
    const sameDayExpectedTimeframeVolume = sameDayVolumes.length
      ? sameDayVolumes.reduce((sum, value) => sum + value, 0) / sameDayVolumes.length
      : null
    const expectedTimeframeVolume = averageVolume > 0
      ? averageVolume * (volumeTfMinutes / 390)
      : sameDayExpectedTimeframeVolume
    const points = []
    for (const candle of pathCandles) {
      const close = Number(candle.close || 0)
      if (!firstClose || !close) continue
      const rawTimeframeVolume = Number(candle.volume)
      const timeframeVolume = Number.isFinite(rawTimeframeVolume) ? Math.max(0, rawTimeframeVolume) : null
      const timeframeRelVolume = timeframeVolume != null && expectedTimeframeVolume && expectedTimeframeVolume > 0
        ? timeframeVolume / expectedTimeframeVolume
        : null
      const evidence = evidenceSnapshotAt(timeline, ticker, Number(candle.time || 0), Number(row.combinedSentiment || 0), Math.max(5, Math.round(windowHours * 60)))
      points.push(decisionPointFromValues({
        ticker,
        timestamp: Number(candle.time || 0),
        timestampSource: `chart_provider_${volumeTf}_volume_bucket`,
        chartBarTime: isoFromSec(candle.time),
        combinedSentiment: evidence.combinedSentiment,
        sentimentEstimated: evidence.sentimentEstimated,
        sourceSocialWindowStart: evidence.sourceSocialWindowStart,
        sourceSocialWindowEnd: evidence.sourceSocialWindowEnd,
        sourceNewsWindowStart: evidence.sourceNewsWindowStart,
        sourceNewsWindowEnd: evidence.sourceNewsWindowEnd,
        evidenceEventCount: evidence.evidenceEventCount,
        messageDensityPerHour: evidence.messageDensityPerHour,
        newsDensityPerHour: evidence.newsDensityPerHour,
        socialDensityPerHour: evidence.socialDensityPerHour,
        evidenceWindowMinutes: evidence.evidenceWindowMinutes,
        priceChangePct: ((close - firstClose) / firstClose) * 100,
        relativeVolume: timeframeRelVolume,
        currentDollarVolume: timeframeVolume == null ? null : close * timeframeVolume,
        price: close,
        volume: timeframeVolume,
        timeframeVolume,
        expectedTimeframeVolume,
        volumeTimeframe: volumeTf,
        volumeTimeframeMinutes: volumeTfMinutes,
        relativeVolumeBasis: timeframeVolume == null
          ? `chart ${volumeTf} bar volume unavailable`
          : averageVolume > 0
          ? `chart ${volumeTf} bar volume / expected ${volumeTf} volume from average daily volume`
          : sameDayExpectedTimeframeVolume
            ? `chart ${volumeTf} bar volume / average ${volumeTf} bar volume from selected market day`
            : `chart ${volumeTf} bar volume; average daily volume unavailable`,
        convictionScore: row.convictionScore,
      }, row))
    }
    if (points.length >= 2) {
      out.set(ticker, compactPathPoints(points, requestedMax).map(point => ({
        ...point,
        dataQuality: point.dataQuality === 'complete' ? 'partial' : point.dataQuality,
        chartPathDerived: true,
      })))
    }
  })
  return out
}

function annotatePathWindowPoint(point = {}, row = {}, selectedWindow = {}) {
  const timestamp = Number(point.timestamp || point.time || 0)
  return {
    ...point,
    ticker: row.ticker || point.ticker,
    timestampUtc: point.timestampUtc || isoFromSec(timestamp),
    displayTime: point.displayTime || displayTimeFromSec(timestamp),
    rollingWindowUsed: row.rollingWindowUsed || selectedWindow.requestedWindowUsed || point.rollingWindowUsed,
    sourceSocialWindowStart: point.sourceSocialWindowStart || row.sourceSocialWindowStart,
    sourceSocialWindowEnd: point.sourceSocialWindowEnd || row.sourceSocialWindowEnd,
    sourceNewsWindowStart: point.sourceNewsWindowStart || row.sourceNewsWindowStart,
    sourceNewsWindowEnd: point.sourceNewsWindowEnd || row.sourceNewsWindowEnd,
    chartBarTime: point.chartBarTime || isoFromSec(timestamp),
  }
}

async function tickerPathEvidence(db, scoredRows = [], maxPoints = 14, redis = null, windowHours = DEFAULT_THRESHOLDS.pathWindowHours, volumeTimeframe = '5m', forceChartVolume = false, marketDayOnly = false) {
  const wanted = new Set(scoredRows.map(row => normalizeTicker(row.ticker)).filter(Boolean))
  if (!wanted.size) return new Map()
  const currentByTicker = new Map(scoredRows.map(row => [normalizeTicker(row.ticker), row]))
  const pathWindowHours = Math.max(5 / 60, Number(windowHours || DEFAULT_THRESHOLDS.pathWindowHours))
  const pathScope = decisionMapPathScope({ windowHours: pathWindowHours, volumeTimeframe, marketDayOnly })
  const sinceSec = pathSinceSec(pathWindowHours, marketDayOnly)
  const redisPaths = await redisTickerPathEvidence(redis, scoredRows, maxPoints, pathWindowHours, marketDayOnly, pathScope)
  const mongoPointPaths = await mongoDecisionMapPointEvidence(db, scoredRows, maxPoints, pathWindowHours, marketDayOnly, pathScope)
  const tickersNeedingSnapshots = new Set([...wanted].filter(ticker => {
    const existing = [...(redisPaths.get(ticker) || []), ...(mongoPointPaths.get(ticker) || [])]
    return existing.length < maxPoints || !pathHasVisibleMovement(existing)
  }))
  const snapshots = tickersNeedingSnapshots.size
    ? await filteredMomentumSnapshots(db, tickersNeedingSnapshots, sinceSec, Math.max(12, maxPoints * 4), -1).catch(() => [])
    : []

  const map = new Map()
  for (const [ticker, points] of redisPaths.entries()) {
    const current = currentByTicker.get(ticker) || {}
    map.set(ticker, points.map(point => decisionPointFromValues({ ...point, timestampSource: point.timestampSource || 'redis_path' }, current)).slice(-maxPoints * 2))
  }
  for (const [ticker, points] of mongoPointPaths.entries()) {
    const existing = map.get(ticker) || []
    map.set(ticker, [...existing, ...points].slice(-maxPoints * 2))
  }
  for (const snapshot of snapshots.reverse()) {
    const snapshotSec = Number(snapshot.snapshot_sec || 0)
    if (marketDayOnly && !isLikelyMarketMovementDate(snapshotSec)) continue
    for (const raw of snapshot.rows || []) {
      const ticker = normalizeTicker(raw.ticker)
      if (!tickersNeedingSnapshots.has(ticker)) continue
      const current = currentByTicker.get(ticker) || {}
      const points = map.get(ticker) || []
      points.push(decisionPointFromValues({ ...raw, snapshotSec, timestampSource: 'finviz_momentum_snapshots' }, current))
      map.set(ticker, points)
    }
  }

  // Fallback: query screener collection for any tickers still missing path history
  const tickersStillMissing = [...wanted].filter(ticker => {
    const existing = map.get(ticker) || []
    return existing.length < 2
  })
  if (tickersStillMissing.length) {
    const screenerPaths = await screenerHistoricalPathEvidence(db, scoredRows.filter(row => tickersStillMissing.includes(normalizeTicker(row.ticker))), maxPoints, pathWindowHours, marketDayOnly)
    for (const [ticker, points] of screenerPaths.entries()) {
      const existing = map.get(ticker) || []
      map.set(ticker, [...existing, ...points].slice(-maxPoints * 2))
    }
  }

  const tickersNeedingChartBars = [...wanted].filter(ticker => {
    const existing = map.get(ticker) || []
    return existing.length < DECISION_MAP_MIN_FULL_PATH_POINTS || !pathHasVisibleMovement(existing)
  })
  if (tickersNeedingChartBars.length) {
    const chartPaths = await chartCandlePathEvidence(
      db,
      scoredRows.filter(row => tickersNeedingChartBars.includes(normalizeTicker(row.ticker))),
      maxPoints,
      pathWindowHours,
      volumeTimeframe,
      marketDayOnly,
    )
    for (const [ticker, points] of chartPaths.entries()) {
      const existing = map.get(ticker) || []
      const shouldReplace = existing.length < DECISION_MAP_MIN_FULL_PATH_POINTS || !pathHasVisibleMovement(existing)
      map.set(ticker, (shouldReplace ? points : existing).slice(-maxPoints * 2))
    }
  }

  if (forceChartVolume) {
    const chartPaths = await chartCandlePathEvidence(db, scoredRows, maxPoints, pathWindowHours, volumeTimeframe, marketDayOnly)
    for (const [ticker, points] of chartPaths.entries()) {
      if (points.length >= 2) map.set(ticker, points.slice(-maxPoints * 2))
    }
  }

  for (const row of scoredRows) {
    const ticker = normalizeTicker(row.ticker)
    const points = map.get(ticker) || []
    const latestTs = toSec(row.quoteUpdatedAt) || Math.floor(Date.now() / 1000)
    const hasChartTimeframePath = points.some(point => String(point.timestampSource || '').includes('chart_provider_') && point.volumeTimeframe === volumeTimeframe)
    const appendCurrentPoint = (!forceChartVolume || !hasChartTimeframePath) && (!marketDayOnly || isLikelyMarketMovementDate(latestTs))
    if (appendCurrentPoint) {
      points.push(decisionPointFromValues({
        timestamp: latestTs,
        timestampSource: row.quoteUpdatedAt ? 'screener_quote_updated_at' : 'server_now',
        combinedSentiment: row.combinedSentiment,
        priceChangePct: row.priceChangePct,
        marketCapRelativeVolumeScore: row.marketCapRelativeVolumeScore,
        relativeVolume: row.relativeVolume,
        currentDollarVolume: row.currentDollarVolume,
        price: row.price,
        volume: row.currentVolume,
        volumeTimeframe,
        relativeVolumeBasis: 'current screener relative volume; chart timeframe bar unavailable for latest point',
        convictionScore: row.convictionScore,
      }, row))
    }
    const selectedWindow = selectPathMovementWindow(points, pathWindowHours, marketDayOnly)
    const pathPoints = compactPathPoints(selectedWindow.points, maxPoints)
      .map(point => annotatePathWindowPoint(point, row, selectedWindow))
    const pathIdentity = analyzeDecisionMapPath(pathPoints, ticker)
    const direction = pathDirectionFromPoints(pathPoints, row, {
      quality: selectedWindow.quality,
      coverage: selectedWindow.coverage,
      source: pathPoints.some(point => point.chartPathDerived) ? 'chart_provider_1m_fallback' : 'redis_mongo_decision_points_finviz_snapshots',
    })
    const pathMarketDate = selectedWindow.marketDate || latestMarketMovementDateKey(pathPoints)
    const pathStats = pathMovementStats(pathPoints)
    map.set(ticker, {
      ticker_path: pathPoints,
      path_points: pathPoints,
      path_scope: pathScope,
      path_ticker: ticker,
      path_chronological: pathIdentity.chronological,
      path_wrong_ticker_rows: pathIdentity.wrong_ticker_rows,
      path_current_timestamp: pathIdentity.latest_timestamp,
      ...direction,
      path_raw_points_count: points.length,
      path_window_raw_points_count: selectedWindow.points.length,
      path_window_hours: selectedWindow.requestedWindowHours || pathWindowHours,
      path_window_used: selectedWindow.requestedWindowUsed || formatWindowHours(pathWindowHours),
      path_window_start: isoFromSec(selectedWindow.windowStartSec),
      path_window_end: isoFromSec(selectedWindow.windowEndSec),
      path_unique_points_count: pathStats.unique,
      path_market_date: pathMarketDate || null,
      path_sampling: pathPoints.length < selectedWindow.points.length ? 'shape_preserving' : 'none',
      path_requested_points: maxPoints,
      path_min_full_points: DECISION_MAP_MIN_FULL_PATH_POINTS,
      path_sparse_threshold: DECISION_MAP_SPARSE_PATH_THRESHOLD,
      path_available_market_dates: selectedWindow.availableMarketDates || [],
      path_volume_timeframe: volumeTimeframe,
      path_volume_timeframe_minutes: DECISION_MAP_VOLUME_TIMEFRAMES[volumeTimeframe] || 5,
      path_volume_basis: `chart bar volume bucketed to ${volumeTimeframe}; 1x = expected ${volumeTimeframe} volume from average daily volume when available`,
      path_market_day_only: Boolean(marketDayOnly),
      path_static: !pathHasVisibleMovement(pathPoints),
    })
  }
  return map
}

async function finvizFreshness(db, staleSeconds) {
  const latest = await db.collection('screeners').findOne(
    { quote_source: 'finviz_elite_screener', finviz_status: { $ne: 'dropped' } },
    { sort: { finviz_seen_at: -1, quote_updated_at: -1 }, projection: { ticker: 1, finviz_seen_at: 1, quote_updated_at: 1, finviz_status: 1 } }
  ).catch(() => null)
  const seenSec = toSec(latest?.finviz_seen_at) || toSec(latest?.quote_updated_at)
  const age = ageSeconds(seenSec)
  return {
    latestTicker: latest?.ticker || null,
    latestSeenAt: seenSec ? new Date(seenSec * 1000).toISOString() : null,
    ageSeconds: age,
    staleSeconds,
    isStale: age == null ? true : age > staleSeconds,
    note: age == null
      ? 'No FinViz screener timestamp found.'
      : age > staleSeconds
        ? 'FinViz screener data is stale; treat it as stored mover context, not live market truth.'
        : 'FinViz screener data is fresh enough for active mover context.',
  }
}

async function loadHotDecisionMap(req, db) {
  const redis = redisFromReq(req)
  if (!redis || req.query.fresh === '1') return null
  const signature = stableDecisionMapQuerySignature(req.query)
  const key = `decision_map:latest:${signature}`
  try {
    const [raw, redisTtl] = await Promise.all([
      redis.get(key),
      redis.ttl(key).catch(() => 0),
    ])
    const parsed = raw ? JSON.parse(raw) : null
    if (parsed?.ok && Array.isArray(parsed.rows)) {
      if (!parsed.rows.length && !req.query.search && !req.query.ticker && !req.query.focusTicker) {
        return null
      }
      // Return cached payload as-is (no additional MongoDB queries)
      // The payload already includes path_points from the previous build
      const builtAt = parsed.builtAt || parsed.generatedAt || parsed.hot_data?.snapshot_at || null
      const ttlSeconds = Number(redisTtl) > 0 ? Number(redisTtl) : Number(parsed.ttlSeconds || DECISION_MAP_REDIS_TTL)
      return {
        ...parsed,
        ...decisionMapCacheFields({
          cacheMode: 'redis',
          cacheHit: true,
          redisAvailable: true,
          builtAt,
          ttlSeconds,
          cacheSignature: signature,
          store: 'redis-hot',
        }),
        hot_data: {
          ...(parsed.hot_data || {}),
          served_from: 'redis_decision_map_hot_snapshot',
          cache_hit: true,
          path_hydrated: true,
          cache_signature: signature,
          redis_ttl_seconds: DECISION_MAP_REDIS_TTL,
          redis_ttl_remaining_seconds: ttlSeconds,
        },
      }
    }
  } catch (_) {}
  return null
}

async function persistDecisionMapHotState(req, db, payload) {
  const redis = redisFromReq(req)
  const rows = Array.isArray(payload?.rows) ? payload.rows : []
  if (!rows.length) return { redis_points: 0, mongo_points: 0, kafka_queued: 0 }

  const snapshotSec = quantizedNowSec()
  const signature = stableDecisionMapQuerySignature(req.query)
  const marketDayOnly = marketDayPathsOnly(req.query)
  const pathWindowHours = rollingWindowHoursFromQuery(req.query, 'pathWindowHours', DEFAULT_THRESHOLDS.pathWindowHours)
  const volumeTimeframe = decisionMapVolumeTimeframe(req.query)
  const pathScope = decisionMapPathScope({ windowHours: pathWindowHours, volumeTimeframe, marketDayOnly })
  const points = decisionMapStoragePoints(rows, snapshotSec, marketDayOnly, pathScope)

  let redisPoints = 0
  if (redis) {
    try {
      const pipe = redis.pipeline()
      const snapshotPayload = {
        ...payload,
        hot_data: {
          enabled: true,
          source: 'redis_decision_map_hot_snapshot',
          cache_signature: signature,
          snapshot_sec: snapshotSec,
          snapshot_at: new Date(snapshotSec * 1000).toISOString(),
          redis_ttl_seconds: DECISION_MAP_REDIS_TTL,
          path_ttl_seconds: DECISION_MAP_PATH_TTL,
          path_max_points: DECISION_MAP_PATH_MAX,
        },
      }
      pipe.set(`decision_map:latest:${signature}`, JSON.stringify(snapshotPayload), 'EX', DECISION_MAP_REDIS_TTL)
      pipe.set('decision_map:latest', JSON.stringify(snapshotPayload), 'EX', DECISION_MAP_REDIS_TTL)
      pipe.hset('decision_map:meta', {
        latest_signature: signature,
        latest_snapshot_sec: String(snapshotSec),
        latest_snapshot_at: new Date(snapshotSec * 1000).toISOString(),
        latest_count: String(rows.length),
        redis_ttl_seconds: String(DECISION_MAP_REDIS_TTL),
        path_ttl_seconds: String(DECISION_MAP_PATH_TTL),
        path_max_points: String(DECISION_MAP_PATH_MAX),
      })
      pipe.expire('decision_map:meta', DECISION_MAP_PATH_TTL)
      for (const point of points) {
        const pointKey = `decision_map:point:${pathScope}:${point.ticker}:${point.snapshotSec}`
        pipe.set(pointKey, pointJson(point), 'EX', DECISION_MAP_PATH_TTL)
        pipe.zadd(`decision_map:path:${pathScope}:${point.ticker}`, point.snapshotSec, pointKey)
        pipe.zadd('decision_map:active', point.snapshotSec, point.ticker)
        pipe.set(`decision_map:ticker:${pathScope}:${point.ticker}:latest`, pointJson(point), 'EX', DECISION_MAP_PATH_TTL)
        pipe.expire(`decision_map:path:${pathScope}:${point.ticker}`, DECISION_MAP_PATH_TTL)
        pipe.zremrangebyrank(`decision_map:path:${pathScope}:${point.ticker}`, 0, -(DECISION_MAP_PATH_MAX + 1))
      }
      pipe.expire('decision_map:active', DECISION_MAP_PATH_TTL)
      await pipe.exec()
      redisPoints = points.length
    } catch (err) {
      console.warn('DecisionMap Redis hot write failed:', err.message)
    }
  }

  let mongoPoints = 0
  if (DECISION_MAP_PERSIST_MONGO) {
    try {
      const pointDb = decisionMapStorageDb(db)
      await pointDb.collection('decision_map_points').createIndex({ path_scope: 1, ticker: 1, snapshot_sec: -1 })
      await pointDb.collection('decision_map_points').createIndex({ snapshot_sec: -1 })
      const result = await pointDb.collection('decision_map_points').bulkWrite(points.map(point => ({
        updateOne: {
          filter: { _id: `${pathScope}:${point.ticker}:${point.snapshotSec}` },
          update: {
            $set: {
              ...point,
              path_scope: pathScope,
              snapshot_sec: point.snapshotSec,
              generated_at: new Date(point.snapshotSec * 1000),
              updated_at: new Date(),
              source: 'decision_map_api',
            },
          },
          upsert: true,
        },
      })), { ordered: false })
      mongoPoints = Number(result.upsertedCount || 0) + Number(result.modifiedCount || 0)
    } catch (err) {
      console.warn('DecisionMap Mongo point persist failed:', err.message)
    }
  }

  publishDecisionMapPoints(points)
  return { redis_points: redisPoints, mongo_points: mongoPoints, kafka_queued: points.length }
}

function publishDecisionMapPoints(points = []) {
  if (!DECISION_MAP_KAFKA_PUBLISH || !Array.isArray(points) || !points.length) return
  const scriptCandidates = [
    path.join(process.cwd(), 'scripts', 'publish_decision_map_points.py'),
    path.join(process.cwd(), '..', 'scripts', 'publish_decision_map_points.py'),
  ]
  const scriptPath = scriptCandidates.find(candidate => fs.existsSync(candidate))
  if (!scriptPath) return
  const pythonCandidates = [
    process.env.PYTHON_BIN,
    '/opt/rssvenv/bin/python',
    'python3',
  ].filter(Boolean)
  const pythonBin = pythonCandidates.find(candidate => candidate === 'python3' || fs.existsSync(candidate)) || 'python3'
  try {
    const child = spawn(pythonBin, [scriptPath], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        KAFKA_PUBLISH_NEWS: 'true',
      },
      stdio: ['pipe', 'ignore', 'ignore'],
    })
    child.stdin.end(JSON.stringify({ points }))
    child.unref?.()
  } catch (_) {}
}

router.get('/ram/status', async (req, res) => {
  try {
    const db = mongoose.connection.db
    const redis = redisFromReq(req)
    ensureDecisionMapHealthMonitor(req, db)
    const status = {
      ok: true,
      redis_available: Boolean(redis),
      kafka_publish_enabled: DECISION_MAP_KAFKA_PUBLISH,
      mongo_persist_enabled: DECISION_MAP_PERSIST_MONGO,
      mongo_storage_db: DECISION_MAP_DB_NAME || db?.databaseName || null,
      redis_ttl_seconds: DECISION_MAP_REDIS_TTL,
      path_ttl_seconds: DECISION_MAP_PATH_TTL,
      history_seconds: DECISION_MAP_HISTORY_SECONDS,
      path_max_points: DECISION_MAP_PATH_MAX,
      default_path_points: DECISION_MAP_DEFAULT_PATH_POINTS,
      max_path_points: DECISION_MAP_MAX_PATH_POINTS,
      point_interval_seconds: DECISION_MAP_POINT_INTERVAL_SECONDS,
    }
    const health = await buildDecisionMapHealth(redis, db, 'status')
    status.health = {
      status: health.status,
      ok: health.ok,
      checked_at: health.checked_at,
      warnings: health.warnings,
      auto_refresh: health.auto_refresh,
    }
    if (redis) {
      const [meta, activeCount] = await Promise.all([
        redis.hgetall('decision_map:meta').catch(() => ({})),
        redis.zcard('decision_map:active').catch(() => 0),
      ])
      status.redis = {
        latest_snapshot_at: meta.latest_snapshot_at || null,
        latest_count: Number(meta.latest_count || 0),
        active_tickers: Number(activeCount || 0),
        latest_signature: meta.latest_signature || null,
      }
    }
    if (db) {
      const pointDb = decisionMapStorageDb(db)
      const latest = await pointDb.collection('decision_map_points')
        .findOne({}, { sort: { snapshot_sec: -1 }, projection: { ticker: 1, snapshot_sec: 1, generated_at: 1 } })
        .catch(() => null)
      status.mongo = {
        source_db: db.databaseName || null,
        storage_db: pointDb.databaseName || DECISION_MAP_DB_NAME || null,
        collection: 'decision_map_points',
        latest_ticker: latest?.ticker || null,
        latest_snapshot_sec: latest?.snapshot_sec || null,
        latest_snapshot_at: latest?.generated_at || null,
      }
    }
    res.json(status)
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

router.get('/health', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, status: 'degraded', error: 'MongoDB not connected' })
    ensureDecisionMapHealthMonitor(req, db)
    const health = await buildDecisionMapHealth(redisFromReq(req), db, 'manual')
    res.status(health.ok ? 200 : 207).json({
      ...health,
      last_background_check: latestDecisionMapHealth?.checked_at || null,
    })
  } catch (err) {
    res.status(500).json({ ok: false, status: 'error', error: String(err.message || err) })
  }
})

router.get('/', async (req, res) => {
  try {
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB not connected' })
    ensureDecisionMapHealthMonitor(req, db)
    const hot = await loadHotDecisionMap(req, db)
    if (hot) {
      res.set('X-Decision-Map-Store', 'redis-hot')
      return res.json(hot)
    }
    const redis = redisFromReq(req)
    const signature = stableDecisionMapQuerySignature(req.query)
    const builtAt = new Date().toISOString()

    const thresholds = {
      ...DEFAULT_THRESHOLDS,
      minRelativeVolume: Math.max(0, Number(req.query.min_rel_volume ?? req.query.minRelativeVolume ?? DEFAULT_THRESHOLDS.minRelativeVolume)),
      minAbsPriceChange: Math.max(0, Number(req.query.min_abs_change ?? req.query.minAbsPriceChange ?? DEFAULT_THRESHOLDS.minAbsPriceChange)),
      positiveSentiment: Number(req.query.positive_sentiment ?? DEFAULT_THRESHOLDS.positiveSentiment),
      negativeSentiment: Number(req.query.negative_sentiment ?? DEFAULT_THRESHOLDS.negativeSentiment),
      priceChange: Math.max(0, Number(req.query.price_threshold ?? DEFAULT_THRESHOLDS.priceChange)),
      minActivityScore: Math.max(0, Number(req.query.min_activity_score ?? DEFAULT_THRESHOLDS.minActivityScore)),
      rollingWindowHours: rollingWindowHoursFromQuery(req.query, 'rollingWindowHours', DEFAULT_THRESHOLDS.rollingWindowHours),
      newsWindowHours: rollingWindowHoursFromQuery(req.query, 'newsWindowHours', DEFAULT_THRESHOLDS.newsWindowHours),
      socialWindowHours: rollingWindowHoursFromQuery(req.query, 'socialWindowHours', DEFAULT_THRESHOLDS.socialWindowHours),
      pathWindowHours: rollingWindowHoursFromQuery(req.query, 'pathWindowHours', DEFAULT_THRESHOLDS.pathWindowHours),
      catalystLookbackHours: rollingWindowHoursFromQuery(req.query, 'catalystLookbackHours', defaultCatalystLookbackHours()),
      finvizStaleSeconds: Math.max(300, Number(req.query.finviz_stale_seconds ?? DEFAULT_THRESHOLDS.finvizStaleSeconds)),
      maxAbsPriceChange: Math.max(1, Number(req.query.max_abs_change ?? req.query.maxAbsPriceChange ?? DEFAULT_THRESHOLDS.maxAbsPriceChange)),
    }
    const windowMeta = rollingWindowMeta(thresholds)
    const volumeTimeframe = decisionMapVolumeTimeframe(req.query)
    const marketDayPathMode = marketDayPathsOnly(req.query)
    const activePathScope = decisionMapPathScope({ windowHours: thresholds.pathWindowHours, volumeTimeframe, marketDayOnly: marketDayPathMode })

    const rows = await activeScreenerRows(db, req.query, thresholds)
    const tickers = rows.map(row => row.ticker)
    const [articles, socials, volumes, finviz] = await Promise.all([
      articleEvidence(db, rows, thresholds.newsWindowHours, thresholds.catalystLookbackHours),
      socialEvidence(db, tickers, thresholds.socialWindowHours),
      rollingVolumeEvidence(db, tickers, thresholds.rollingWindowHours),
      finvizFreshness(db, thresholds.finvizStaleSeconds),
    ])

    let scored = rows.map(row => scoreRow(row, articles.get(row.ticker), socials.get(row.ticker), volumes.get(row.ticker), thresholds))
    const scoredRowsBeforeActivity = scored.length
    const stageExclusions = []
    scored = scored.filter(row => {
      const keep = row.activityScore >= thresholds.minActivityScore
      if (!keep) stageExclusions.push({ ticker: row.ticker, reasons: ['below_min_activity_score'] })
      return keep
    })
    const rowsAfterActivity = scored.length
    const search = normalizeTicker(req.query.search || req.query.q || '')
    const explicitChartVolume = ['1', 'true', 'yes'].includes(String(req.query.force_chart_volume || req.query.forceChartVolume || '').toLowerCase())
    const requestedChartVolumeTimeframe = req.query.volume_timeframe != null || req.query.volumeTimeframe != null
    const forceChartVolume = Boolean(search || explicitChartVolume || (marketDayPathMode && requestedChartVolumeTimeframe))
    if (search) scored = scored.filter(row => {
      const keep = row.ticker === search
      if (!keep) stageExclusions.push({ ticker: row.ticker, reasons: ['ticker_search_filter'] })
      return keep
    })
    const rowsAfterTickerFilter = scored.length
    const alignment = String(req.query.alignment || '').toLowerCase()
    if (alignment === 'aligned') scored = scored.filter(row => {
      const keep = row.quadrant === 'Q1' || row.quadrant === 'Q3'
      if (!keep) stageExclusions.push({ ticker: row.ticker, reasons: ['alignment_filter'] })
      return keep
    })
    if (alignment === 'divergence') scored = scored.filter(row => {
      const keep = row.quadrant === 'Q2' || row.quadrant === 'Q4'
      if (!keep) stageExclusions.push({ ticker: row.ticker, reasons: ['divergence_filter'] })
      return keep
    })
    const rowsAfterAlignmentFilter = scored.length

    const pathMap = await tickerPathEvidence(db, scored, decisionMapPathPointLimit(req.query), redis, thresholds.pathWindowHours, volumeTimeframe, forceChartVolume, marketDayPathMode)
    scored = scored.map(row => ({ ...row, ...(pathMap.get(row.ticker) || {
      ticker_path: [],
      path_points: [],
      path_direction: 'insufficient_history',
      path_direction_score: 0,
      path_color: 'gray',
      path_explanation: 'No intraday history available for this ticker.',
      latest_point: null,
      previous_point: null,
      time_lapse_available: false,
      path_missing_reason: 'no_intraday_history',
    }) }))

    const requestedUniverseSort = String(req.query.sort || '').toLowerCase()
    const defaultSortBy = ['postmarket_change', 'premarket_change', 'change'].includes(requestedUniverseSort)
      ? 'priceChangePct'
      : 'convictionScore'
    const sortBy = String(req.query.orderBy || req.query.sortBy || defaultSortBy)
    const sortDir = String(req.query.orderDir || 'desc').toLowerCase() === 'asc' ? 1 : -1
    scored.sort((a, b) => {
      const av = a[sortBy] ?? 0
      const bv = b[sortBy] ?? 0
      if (typeof av === 'string') return sortDir === 1 ? av.localeCompare(String(bv)) : String(bv).localeCompare(av)
      return sortDir === 1 ? Number(av || 0) - Number(bv || 0) : Number(bv || 0) - Number(av || 0)
    })

    const rowDiagnostics = analyzeDecisionMapRows(scored)
    scored = rowDiagnostics.validRows

    const summary = scored.reduce((acc, row) => {
      acc[row.quadrant] = (acc[row.quadrant] || 0) + 1
      if (row.riskFlags.includes('SENTIMENT_PRICE_DIVERGENCE')) acc.divergence += 1
      if (row.catalystLabel) acc.withCatalyst += 1
      if (row.socialCount) acc.withSocial += 1
      return acc
    }, { Q1: 0, Q2: 0, Q3: 0, Q4: 0, Neutral: 0, divergence: 0, withCatalyst: 0, withSocial: 0 })

    const payload = {
      ok: true,
      generatedAt: builtAt,
      universe: String(req.query.universe || 'active_finviz'),
      session: String(req.query.session || 'auto'),
      rollingWindowUsed: windowMeta.rollingWindowUsed,
      rollingWindowHours: windowMeta.rollingWindowHours,
      newsWindowUsed: windowMeta.newsWindowUsed,
      socialWindowUsed: windowMeta.socialWindowUsed,
      pathWindowUsed: windowMeta.pathWindowUsed,
      catalystLookbackUsed: windowMeta.catalystLookbackUsed,
      volumeTimeframe,
      volumeTimeframeMinutes: DECISION_MAP_VOLUME_TIMEFRAMES[volumeTimeframe] || 5,
      marketDayPathsOnly: marketDayPathMode,
      screener_first: true,
      no_fake_rows: true,
      ...decisionMapCacheFields({
        cacheMode: redis ? 'computed-redis-warmed' : 'mongo',
        cacheHit: false,
        redisAvailable: Boolean(redis),
        builtAt,
        ttlSeconds: redis ? DECISION_MAP_REDIS_TTL : 0,
        cacheSignature: signature,
        store: redis ? 'mongo-compute-redis-warmed' : 'mongo-compute',
      }),
      hot_data: {
        enabled: Boolean(redis),
        source: redis ? 'computed_and_warming_redis' : 'mongo_compute_no_redis',
        cache_hit: false,
        cache_signature: signature,
        kafka_publish_enabled: DECISION_MAP_KAFKA_PUBLISH,
        mongo_persist_enabled: DECISION_MAP_PERSIST_MONGO,
        mongo_storage_db: DECISION_MAP_DB_NAME || db?.databaseName || null,
        mongo_point_collection: 'decision_map_points',
        path_scope: activePathScope,
        path_store: `decision_map:path:${activePathScope}:{ticker}`,
        point_store: `decision_map:point:${activePathScope}:{ticker}:{snapshotSec}`,
      },
      freshness: { finviz },
      rolling_window: {
        active_context: windowMeta.rollingWindowUsed,
        rolling_window_hours: windowMeta.rollingWindowHours,
        news_window_hours: thresholds.newsWindowHours,
        social_window_hours: thresholds.socialWindowHours,
        path_window_hours: thresholds.pathWindowHours,
        catalyst_lookback_hours: thresholds.catalystLookbackHours,
        catalyst_lookback_used: windowMeta.catalystLookbackUsed,
        volume_timeframe: volumeTimeframe,
        volume_timeframe_minutes: DECISION_MAP_VOLUME_TIMEFRAMES[volumeTimeframe] || 5,
        volume_basis: `3D path Z uses chart bar volume bucketed to ${volumeTimeframe}; D/W/M are intentionally excluded for day-trading paths.`,
        market_day_paths_only: marketDayPathMode,
        source_news_window_start: windowMeta.sourceNewsWindowStart,
        source_news_window_end: windowMeta.sourceNewsWindowEnd,
        source_social_window_start: windowMeta.sourceSocialWindowStart,
        source_social_window_end: windowMeta.sourceSocialWindowEnd,
      },
      thresholds,
      count: scored.length,
      diagnostics: {
        screener_rows_loaded: rows.length,
        scored_rows_before_activity_filter: scoredRowsBeforeActivity,
        rows_after_activity_filter: rowsAfterActivity,
        removed_by_activity_filter: scoredRowsBeforeActivity - rowsAfterActivity,
        rows_after_ticker_filter: rowsAfterTickerFilter,
        removed_by_ticker_filter: rowsAfterActivity - rowsAfterTickerFilter,
        rows_after_alignment_filter: rowsAfterAlignmentFilter,
        removed_by_alignment_filter: rowsAfterTickerFilter - rowsAfterAlignmentFilter,
        scored_rows_before_coordinate_validation: rowDiagnostics.input_rows,
        valid_coordinate_rows: rowDiagnostics.valid_coordinate_rows,
        unique_tickers: rowDiagnostics.unique_tickers,
        duplicate_ticker_rows: rowDiagnostics.duplicate_ticker_rows,
        excluded_coordinate_rows: rowDiagnostics.excluded_rows,
        excluded: ['1', 'true', 'yes'].includes(String(req.query.debug || '').toLowerCase())
          ? [...stageExclusions, ...rowDiagnostics.excluded].slice(0, 100)
          : undefined,
      },
      summary,
      rows: scored,
      methodology: {
        first_step: 'Query current numeric screener rows using price, relative volume, liquidity, exchange, and source freshness.',
        second_step: 'Attach structured news, social evidence, catalysts, and rolling volume evidence only after the screener universe exists.',
        rolling_window: `${windowMeta.rollingWindowUsed} active context by default; news, social, activity, rolling volume, and path slices use the selected window without fake rows.`,
        axes: {
          x: 'combinedSentiment',
          y: 'priceChangePct',
          z: `timeframe relative volume for path points (${volumeTimeframe}); 1x maps to z=0`,
          bubbleSize: 'rollingVolume / volumeAcceleration',
          marketCap: 'marketCapBucket + marketCap value are shown in row context and hover tooltip',
        },
      },
    }
    const persisted = await persistDecisionMapHotState(req, db, payload)
    payload.hot_data = { ...payload.hot_data, ...persisted }
    buildDecisionMapHealth(redis, db, 'post_compute').catch(() => {})
    res.set('X-Decision-Map-Store', redis ? 'computed-redis-warmed' : 'computed-mongo')
    res.json(payload)
  } catch (err) {
    console.error('GET /api/decision-map failed:', err)
    res.status(500).json({ ok: false, error: err.message })
  }
})

export default router
