import { Router } from 'express'
import mongoose from 'mongoose'
import Screener from '../models/Screener.js'
import { normalizeScreenerRow, isCleanListedUsRow, loadAdaptiveSocialStatsForRows } from './screener.js'

// GET /api/exit-screener?stopPct=5&limit=30
//
// There is no positions store: open/simulated positions are derived live by the
// chart-service (/api/sentchart/positions/batch), which runs the SAME strategy
// simulation that draws the chart's entry/exit markers: rolling-corr entry and
// post-entry peak x (1 - stopPct/100) trailing stop. A correlation-break exit
// can be enabled with corrExit for professor-note validation, but it is off by
// default so this route preserves the chart strategy's trailing-stop behavior.
// An entry whose exit is "session_end" is Holding; risk exits are Stopped Out.
// This route only picks candidates, joins Mongo quote rows, and flattens trades.
//
// CONFIDENTIALITY BOUNDARY: the entry/exit screeners must never read from or
// import anything under ~/dev/research-students (confidential student research
// data — not for distribution). The strategy math they rely on is this repo's
// own clean reimplementation in chart-service/chart_service.py; keep it that way.

const router = Router()

// Dev default 5055 matches the Vite proxy comment (5050 is held by the local
// sentiment-scout dashboard); docker-compose overrides to http://chart-service:5050.
const CHART_SERVICE_URL = (process.env.CHART_SERVICE_URL || 'http://localhost:5055').replace(/\/+$/, '')
const CORR_WINDOW_MINUTES = 360          // strategy rolling window (candidate-selection window too)
const DEFAULT_ENTRY_THRESHOLD = 0.10     // the strategy's confirmed entry threshold
const DEFAULT_CORR_EXIT_THRESHOLD = null // optional validation exit; disabled by default
const UNIVERSE_SCAN_LIMIT = Number(process.env.SCREENER_UNIVERSE_SCAN_LIMIT || 6000)
const DEFAULT_LIMIT = 30
const MAX_LIMIT = 50                     // chart-service batch cap

function clamp(value, min, max) {
  const n = Number(value)
  if (!Number.isFinite(n)) return min
  return Math.max(min, Math.min(max, n))
}

function round(value, decimals = 2) {
  const n = Number(value)
  return Number.isFinite(n) ? Number(n.toFixed(decimals)) : null
}

async function fetchPositionsBatch(tickers, stopPct, threshold, corrExitThreshold) {
  const params = new URLSearchParams({
    tickers: tickers.join(','),
    stop_pct: String(stopPct),
    threshold: String(threshold),
  })
  if (Number.isFinite(corrExitThreshold)) {
    params.set('corr_exit_threshold', String(corrExitThreshold))
  }
  const url = `${CHART_SERVICE_URL}/api/sentchart/positions/batch?${params.toString()}`
  const controller = new AbortController()
  // A cold batch is one Finviz bars fetch per ticker on the chart-service side;
  // warm batches answer from its 120s row cache.
  const timer = setTimeout(() => controller.abort(), 60_000)
  try {
    const res = await fetch(url, { signal: controller.signal })
    if (!res.ok) throw new Error(`chart-service responded ${res.status}`)
    const data = await res.json()
    return data?.results || {}
  } finally {
    clearTimeout(timer)
  }
}

router.get('/', async (req, res) => {
  try {
    const stopPct = clamp(req.query.stopPct ?? 5, 5, 30)
    const threshold = clamp(req.query.threshold ?? DEFAULT_ENTRY_THRESHOLD, 0.01, 1)
    const corrExitRaw = req.query.corrExit ?? req.query.corr_exit_threshold
    const corrExitThreshold = corrExitRaw == null || corrExitRaw === ''
      ? DEFAULT_CORR_EXIT_THRESHOLD
      : clamp(corrExitRaw, -1, 1)
    const limit = Math.round(clamp(req.query.limit ?? DEFAULT_LIMIT, 1, MAX_LIMIT))

    // 1. Same clean listed-US universe as /api/screener
    const filter = {
      exchange: { $in: ['NASDAQ', 'NYSE', 'AMEX'] },
      ticker: { $not: /\./ },
      price: { $ne: null },
    }
    const universe = (await Screener.find(filter).limit(UNIVERSE_SCAN_LIMIT).lean())
      .map(normalizeScreenerRow)
      .filter(isCleanListedUsRow)

    // 2. Candidates = most StockTwits-active tickers within the strategy window
    const db = mongoose.connection.db
    let candidates = []
    if (db && universe.length) {
      const socialMap = await loadAdaptiveSocialStatsForRows(db, universe, CORR_WINDOW_MINUTES)
      candidates = universe
        .map(row => ({ row, social: socialMap.get(row.ticker) }))
        .filter(c => Number(c.social?.stocktwits_count || c.social?.count || 0) > 0)
        .sort((a, b) =>
          (Number(b.social?.stocktwits_count || 0) - Number(a.social?.stocktwits_count || 0)) ||
          (Number(b.social?.count || 0) - Number(a.social?.count || 0)))
        .slice(0, limit)
    }
    if (!candidates.length) {
      return res.json({
        ok: true, stopPct, threshold, corrExitThreshold, count: 0, rows: [],
        sorted_by: 'distance_to_stop_pct asc',
        note: `No tickers with StockTwits activity in the last ${CORR_WINDOW_MINUTES} minutes.`,
      })
    }

    // 3. Batch positions from the chart-service (never fail the whole page on it)
    let positions = {}
    let chartServiceOk = true
    try {
      positions = await fetchPositionsBatch(candidates.map(c => c.row.ticker), stopPct, threshold, corrExitThreshold)
    } catch (err) {
      chartServiceOk = false
      console.error('GET /api/exit-screener positions batch failed:', err.message)
    }

    // 4. Flatten trades: one row per simulated position, stop math from the
    //    sim's tracked post-entry peak (stop = peak × (1 − stopPct/100)).
    //    peak_price + current_price are returned so the frontend slider can
    //    recompute stop/distance client-side without a refetch.
    const rows = []
    let warming = 0
    for (const { row } of candidates) {
      const result = positions[row.ticker]
      if (!result) continue
      if (result.status === 'warming') warming += 1
      const currentPrice = result.current_price ?? row.price ?? null
      for (const trade of result.trades || []) {
        const stopPrice = trade.peak_price != null ? trade.peak_price * (1 - stopPct / 100) : null
        // Holding rows measure against the live price; Stopped Out rows are
        // frozen at their exit fill
        const refPrice = trade.status === 'Stopped Out' ? trade.exit_price : currentPrice
        rows.push({
          ticker: row.ticker,
          company: row.company,
          date: result.date,
          entry_price: trade.entry_price,
          entry_time: trade.entry_time,
          entry_epoch: trade.entry_epoch,
          entry_corr: trade.entry_corr,
          exit_corr: trade.exit_corr,
          current_price: currentPrice,
          pnl_pct: trade.entry_price
            ? round(((refPrice - trade.entry_price) / trade.entry_price) * 100, 2)
            : null,
          trailing_stop_pct: stopPct,
          peak_price: trade.peak_price,
          stop_price: round(stopPrice, 4),
          distance_to_stop_pct: refPrice && stopPrice != null
            ? round(((refPrice - stopPrice) / refPrice) * 100, 2)
            : null,
          status: trade.status,
          exit_price: trade.status === 'Stopped Out' ? trade.exit_price : null,
          exit_time: trade.status === 'Stopped Out' ? trade.exit_time : null,
          exit_reason: trade.exit_reason || (trade.status === 'Stopped Out' ? 'risk_exit' : 'session_end'),
          corr_status: result.status,
        })
      }
    }
    rows.sort((a, b) => (a.distance_to_stop_pct ?? Infinity) - (b.distance_to_stop_pct ?? Infinity))

    res.json({
      ok: true,
      stopPct,
      threshold,
      corrExitThreshold,
      chart_service_ok: chartServiceOk,
      count: rows.length,
      tickers_scanned: candidates.length,
      tickers_warming: warming,
      rows,
      sorted_by: 'distance_to_stop_pct asc',
      note: chartServiceOk
        ? undefined
        : 'chart-service unreachable — no simulated positions could be derived',
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message })
  }
})

export default router
