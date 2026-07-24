'use client'
import { useState, useEffect, useCallback, useMemo, useRef } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { clsx } from 'clsx'
import { CandlestickChart } from './CandlestickChart'
import { RSIChart } from './RSIChart'
import { MACDChart } from './MACDChart'
import { ResearchChart, type ResearchMode } from './ResearchChart'
import { TickerEnrichPanels, type EnrichData } from './TickerEnrichPanels'
import { resampleCandles, bollingerFromCandles, rsiFromCandles, macdFromCandles, overlaySeries, bucketStart, ROLL_WINDOW_DEFAULT, type SocialSeries } from './chartAgg'
import type { StrategyMarker, NewsMarker } from './CandlestickChart'

// Price-chart bar timeframes. The backend serves ONLY 1-minute extended-hours
// intraday bars (one session) — no daily/weekly — so the options stop at 1h and
// are all client-side resamples of the same fetched 1-min data.
const TIMEFRAMES: Array<{ min: number; label: string }> = [
  { min: 1, label: '1m' }, { min: 5, label: '5m' }, { min: 15, label: '15m' },
  { min: 30, label: '30m' }, { min: 60, label: '1h' },
]

interface ChartData {
  date?: string
  n?: number
  error?: string
  candles: Array<{ time: number; open: number; high: number; low: number; close: number; volume?: number }>
  bollinger?: { upper: Array<{ time: number; value: number }>; lower: Array<{ time: number; value: number }> }
  rsi?: Array<{ time: number; value: number }>
  macd?: { macd: Array<{ time: number; value: number }>; signal: Array<{ time: number; value: number }>; histogram: Array<{ time: number; value: number }> }
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

function hasCandles(json: ChartData | null | undefined) {
  return Array.isArray(json?.candles) && json.candles.length > 0
}

async function fetchCandlesWithRetry(ticker: string, win: Win, urlDate: string) {
  const params = new URLSearchParams({ window: win })
  if (urlDate) params.set('date', urlDate)
  const url = `/api/sentchart/charts/${encodeURIComponent(ticker)}?${params.toString()}`
  let last: ChartData | null = null

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const response = await fetch(url)
    const json = await response.json() as ChartData
    last = json
    if (hasCandles(json)) return json
    if (attempt < 2) await sleep(650 * (attempt + 1))
  }

  return last || { error: 'Failed to load chart data.', candles: [] }
}

// Two jobs on one page, one ticker input:
//   • candles — native lightweight-charts OHLC + RSI/MACD/Bollinger from /api/sentchart/charts
//   • pd|sent|ds — the high schoolers' research views, embedded Chart.js (ResearchChart)
type View = 'candles' | ResearchMode

const VIEWS: Array<{ key: View; label: string }> = [
  { key: 'candles', label: 'Candlestick + Indicators' },
  { key: 'pd',      label: 'Price + Density' },
  { key: 'sent',    label: 'Sentiment Score' },
  { key: 'ds',      label: 'Density vs Sentiment' },
]

// The data is 1-min EXTENDED-HOURS intraday only (no daily/weekly history, no
// fundamentals), so the controls are scoped to the intraday windows it supports.
type Win = 'full' | '2h' | '1h'
const WINDOWS: Array<{ key: Win; label: string }> = [
  { key: 'full', label: 'Full Day' },
  { key: '2h',   label: 'Last 2h' },
  { key: '1h',   label: 'Last 1h' },
]

const OVERLAY_ROLLING_WINDOWS = [
  { value: 15, label: '15m' },
  { value: 30, label: '30m' },
  { value: 60, label: '1h' },
  { value: 120, label: '2h' },
  { value: 180, label: '3h' },
  { value: 240, label: '4h' },
  { value: 360, label: '6h' },
]

interface WatcherSeries {
  times?: number[]
  watchers?: number[]
  current_count?: number
  snapshot_count?: number
  status?: string
}

function bucketMeanSeries(times: number[] = [], values: number[] = [], tfMin = 1) {
  const buckets = new Map<number, { sum: number; count: number }>()
  times.forEach((time, index) => {
    const value = Number(values[index])
    if (!Number.isFinite(time) || !Number.isFinite(value)) return
    const bucket = bucketStart(Number(time), tfMin)
    const current = buckets.get(bucket) || { sum: 0, count: 0 }
    current.sum += value
    current.count += 1
    buckets.set(bucket, current)
  })
  return Array.from(buckets.entries())
    .filter(([, item]) => item.count > 0)
    .map(([time, item]) => ({ time, value: Number((item.sum / item.count).toFixed(4)) }))
    .sort((a, b) => a.time - b.time)
}

export function ChartsPage() {
  // Ticker can arrive via ?t= (the Charts Grid links here for the clicked ticker).
  const [sp, setSp] = useSearchParams()
  const urlTicker = (sp.get('t') || '').toUpperCase().trim()
  // Optional ?d=YYYY-MM-DD pins a historical session (phase-3 overlay demo:
  // aligns candles with the historical social snapshot). Absent = latest session.
  const urlDate = (sp.get('d') || '').trim()
  const [input, setInput] = useState(urlTicker)
  const [ticker, setTicker] = useState<string | null>(urlTicker || null)
  const [view, setView] = useState<View>('candles')
  const [win, setWin] = useState<Win>('full')
  const [data, setData] = useState<ChartData | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [enrich, setEnrich] = useState<EnrichData | null>(null)
  const [enrichLoaded, setEnrichLoaded] = useState(false)  // distinguishes "loading" from "no enrichment endpoint"
  // Recent prediction signals — market-wide feed backed by /api/prediction.
  const [predictions, setPredictions] = useState<PredictionRow[] | null>(null)

  // Price-chart bar timeframe (client-side resample) + density/sentiment overlays.
  // All three recompute from already-fetched data — no server round-trip.
  const [tf, setTf] = useState(1)
  const [showDensity, setShowDensity] = useState(false)
  const [showSentiment, setShowSentiment] = useState(false)
  const [showWatchers, setShowWatchers] = useState(false)
  const [overlayRollingMinutes, setOverlayRollingMinutes] = useState(240)
  const [social, setSocial] = useState<SocialSeries | null>(null)
  const [socialMsg, setSocialMsg] = useState('')
  const [watchers, setWatchers] = useState<WatcherSeries | null>(null)
  const [watcherMsg, setWatcherMsg] = useState('')
  const socialCache = useRef<Record<string, SocialSeries>>({})
  const watcherCache = useRef<Record<string, WatcherSeries>>({})
  const loadedTopAiTickerRef = useRef(false)

  // Strategy indicator (entry/exit arrows) — a chart-only overlay like density/
  // sentiment. Fetched from /api/sentchart/signals once enabled, per ticker/window.
  const [showStrategy, setShowStrategy] = useState(false)
  // Trailing-stop % for the strategy exit. 30 is the professor's specified
  // default; tighter values (2/5/10) let the stop actually fire intraday.
  const [stopPct, setStopPct] = useState(30)
  const [signals, setSignals] = useState<StrategyMarker[] | null>(null)
  const [signalsMsg, setSignalsMsg] = useState('')

  const load = useCallback(() => {
    const t = input.trim().toUpperCase()
    if (t) { setTicker(t); setSp({ t }, { replace: true }) }
  }, [input, setSp])

  // Follow ?t= changes (e.g. a grid cell clicked while this page is already open).
  useEffect(() => {
    const t = (sp.get('t') || '').toUpperCase().trim()
    if (t && t !== ticker) { setInput(t); setTicker(t) }
  }, [sp])  // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (ticker || urlTicker || loadedTopAiTickerRef.current) return
    loadedTopAiTickerRef.current = true
    let cancelled = false
    fetch('/api/ai/rankings?days=3&limit=1&window_minutes=1440&min_score=0')
      .then(r => r.json())
      .then(json => {
        if (cancelled) return
        const topTicker = String(json?.rows?.[0]?.ticker || '').toUpperCase().trim()
        if (!topTicker) return
        setInput(topTicker)
        setTicker(topTicker)
        const nextParams = new URLSearchParams(sp)
        nextParams.set('t', topTicker)
        setSp(nextParams, { replace: true })
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [ticker, urlTicker, sp, setSp])

  // Per-ticker enrichments (news alert + 3-day news + social/gossip). DB reads.
  // Note: this endpoint is optional — FlashFeed's backend may not implement it.
  // We check r.ok before parsing so a 404 degrades to a tidy empty state
  // (enrichLoaded=true, enrich=null) instead of a console error or stuck spinner.
  useEffect(() => {
    if (!ticker) { setEnrich(null); setEnrichLoaded(false); return }
    let cancelled = false
    setEnrich(null); setEnrichLoaded(false)
    fetch(`/api/ticker/${ticker}/enrich`)
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (!cancelled) { setEnrich(d); setEnrichLoaded(true) } })
      .catch(() => { if (!cancelled) { setEnrich(null); setEnrichLoaded(true) } })
    return () => { cancelled = true }
  }, [ticker])

  // Recent prediction signals (market-wide) — backed by /api/prediction.
  // Global feed, ticker-independent, so fetched once on mount. Degrades to an
  // empty (hidden) panel if the endpoint is unavailable.
  useEffect(() => {
    let cancelled = false
    fetch('/api/prediction/signals')
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (!cancelled) setPredictions(Array.isArray(d?.rows) ? d.rows : []) })
      .catch(() => { if (!cancelled) setPredictions([]) })
    return () => { cancelled = true }
  }, [])

  // Candlestick view fetches its own OHLC+indicators; research views are driven
  // by <ResearchChart> off the same ticker/window.
  useEffect(() => {
    if (!ticker || view !== 'candles') return
    let cancelled = false
    setLoading(true); setError(null)
    fetchCandlesWithRetry(ticker, win, urlDate)
      .then((json: ChartData) => {
        if (cancelled) return
        if (json.error && !hasCandles(json)) { setError(json.error); setData(null) }
        else setData(json)
      })
      .catch(() => { if (!cancelled) setError('Failed to load chart data.') })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [ticker, view, win, urlDate])

  // Lazily fetch the social density/sentiment series — only once an overlay is
  // enabled on the candles view, and cached per (ticker, date) so timeframe
  // changes and toggles never re-fetch. Polls through the server's "walking"
  // StockTwits backfill the same way ResearchChart does.
  const wantOverlay = view === 'candles' && (showDensity || showSentiment)
  const wantWatcherOverlay = view === 'candles' && showWatchers
  const chartDate = data?.date
  const candleBounds = useMemo(() => {
    const candles = data?.candles || []
    if (!candles.length) return { start: null as number | null, end: null as number | null }
    return {
      start: Number(candles[0].time),
      end: Number(candles[candles.length - 1].time),
    }
  }, [data])
  useEffect(() => {
    if (!wantOverlay || !ticker || !chartDate) return
    const key = `${ticker}|${chartDate}|${overlayRollingMinutes}|${candleBounds.start}|${candleBounds.end}`
    if (socialCache.current[key]) { setSocial(socialCache.current[key]); setSocialMsg(''); return }
    let cancelled = false
    let timer: number | null = null
    setSocial(null); setSocialMsg('Loading social data…')
    const poll = async () => {
      try {
        const params = new URLSearchParams({
          ticker,
          date: chartDate,
          window_minutes: String(overlayRollingMinutes),
          bucket_minutes: '1',
        })
        if (candleBounds.start && candleBounds.end) {
          params.set('start_sec', String(candleBounds.start))
          params.set('end_sec', String(candleBounds.end))
        }
        let s = await fetch(`/api/chart/social?${params}`).then(r => r.json())
        if (cancelled) return
        if (s.error) { setSocialMsg('Social: ' + s.error); return }
        if (s.status === 'walking') { setSocialMsg(`Loading social history, ${s.count || 0} messages…`); timer = window.setTimeout(poll, 1500); return }
        if (!s.messages) {
          s = await fetch(`/api/sentchart/chart/social?${new URLSearchParams({ ticker, date: chartDate })}`).then(r => r.json())
          if (cancelled) return
        }
        if (!s.messages) {
          const emptySeries: SocialSeries = { labels: [], density: [], sent_labels: [], scores_smooth: [], roll_window: overlayRollingMinutes }
          socialCache.current[key] = emptySeries
          setSocial(emptySeries)
          setSocialMsg('No social data for this chart window.')
          return
        }
        const series: SocialSeries = {
          labels: s.labels || [],
          density: s.density || s.win_density || [],
          sent_labels: s.sent_labels || s.labels || [],
          scores_smooth: s.scores_smooth || [],
          roll_window: Number(s.window_minutes || s.roll_window || overlayRollingMinutes),
        }
        socialCache.current[key] = series
        setSocial(series); setSocialMsg(`Evidence: ${s.source} · ${s.messages} rows`)
      } catch { if (!cancelled) setSocialMsg('Social data: error') }
    }
    poll()
    return () => { cancelled = true; if (timer) clearTimeout(timer) }
  }, [wantOverlay, ticker, chartDate, overlayRollingMinutes, candleBounds.start, candleBounds.end])

  useEffect(() => {
    if (!wantWatcherOverlay || !ticker || !chartDate) return
    const key = `${ticker}|watchers|${chartDate}|${overlayRollingMinutes}|${candleBounds.start}|${candleBounds.end}`
    if (watcherCache.current[key]) { setWatchers(watcherCache.current[key]); setWatcherMsg(''); return }
    let cancelled = false
    setWatchers(null); setWatcherMsg('Loading watcher history…')
    const params = new URLSearchParams({
      ticker,
      window_minutes: String(overlayRollingMinutes),
    })
    if (candleBounds.start && candleBounds.end) {
      params.set('start_sec', String(candleBounds.start))
      params.set('end_sec', String(candleBounds.end))
    }
    fetch(`/api/chart/watchers?${params}`)
      .then(r => r.json())
      .then((json: WatcherSeries & { error?: string }) => {
        if (cancelled) return
        if (json.error) { setWatcherMsg('Watchers: ' + json.error); return }
        watcherCache.current[key] = json
        setWatchers(json)
        const count = Number(json.current_count)
        const current = Number.isFinite(count) ? `${count.toLocaleString()} watchers` : 'watcher count unavailable'
        const snapshots = Number(json.snapshot_count || 0)
        setWatcherMsg(snapshots > 1 ? `Watchers: ${current} · ${snapshots} snapshots` : `Watchers: ${current} · collecting real history`)
      })
      .catch(() => { if (!cancelled) setWatcherMsg('Watchers: error') })
    return () => { cancelled = true }
  }, [wantWatcherOverlay, ticker, chartDate, overlayRollingMinutes, candleBounds.start, candleBounds.end])

  // Strategy entry/exit markers — fetched only once the indicator is toggled on,
  // for the candles view, per (ticker, window, date). The backend computes on the
  // full session and returns markers already filtered to the requested window.
  const wantStrategy = view === 'candles' && showStrategy
  useEffect(() => {
    if (!wantStrategy || !ticker) return
    let cancelled = false
    setSignals(null); setSignalsMsg('Loading strategy signals…')
    fetch(`/api/sentchart/signals/${ticker}?window=${win}&stop_pct=${stopPct}${urlDate ? `&date=${urlDate}` : ''}`)
      .then(r => r.json())
      .then((j: { markers?: StrategyMarker[]; trades?: number; error?: string; note?: string }) => {
        if (cancelled) return
        if (j.error) { setSignals(null); setSignalsMsg('Strategy: ' + j.error); return }
        const markers = j.markers ?? []
        setSignals(markers)
        setSignalsMsg(markers.length
          ? `Strategy: ${j.trades ?? 0} trade(s) · ${markers.length} markers`
          : (j.note || 'Strategy: no signals for this session'))
      })
      .catch(() => { if (!cancelled) setSignalsMsg('Strategy: error') })
    return () => { cancelled = true }
  }, [wantStrategy, ticker, win, urlDate, stopPct])

  // Snap marker times onto the active timeframe's bucket so the arrows stay
  // registered to the resampled candles (1m is a no-op). Off => undefined.
  const strategyMarkers = useMemo(() => {
    if (!showStrategy || !signals) return undefined
    return signals.map(m => ({ ...m, time: bucketStart(m.time, tf) }))
  }, [showStrategy, signals, tf])

  // Resample candles + (re)compute Bollinger + build overlays from already-fetched
  // data. Pure client-side: re-runs on timeframe / toggle / data change only.
  const priceView = useMemo(() => {
    const raw = (data?.candles ?? []) as any[]
    const candles = resampleCandles(raw as any, tf)
    // Default 1m keeps the server's Bollinger exactly; coarser timeframes recompute
    // it on the resampled closes so the band stays aligned to the bars.
    const bollinger = tf === 1 ? (data?.bollinger as any) : bollingerFromCandles(candles, 20, 2)
    // RSI/MACD recompute on the resampled closes at coarser timeframes so they
    // sit on the same time buckets as the candles (server values kept at 1m).
    const rsi = tf === 1 ? (data?.rsi as any) : rsiFromCandles(candles, 14)
    const macd = tf === 1 ? (data?.macd as any) : macdFromCandles(candles, 12, 26, 9)
    const ov = overlaySeries(raw as any, social, tf, overlayRollingMinutes || social?.roll_window || ROLL_WINDOW_DEFAULT)
    const watcherOverlay = bucketMeanSeries(watchers?.times || [], watchers?.watchers || [], tf)
    return {
      candles, bollinger, rsi, macd,
      density: showDensity ? ov.density : undefined,
      sentiment: showSentiment ? ov.sentiment : undefined,
      watchers: showWatchers ? watcherOverlay : undefined,
      count: candles.length,
    }
  }, [data, tf, social, watchers, showDensity, showSentiment, showWatchers, overlayRollingMinutes])

  // News-on-candle markers — reuse the already-fetched enrich News feed. A dot
  // above the bar at each article's publish time, colored by sentiment, snapped
  // to the active timeframe grid. Only articles whose publish time falls within
  // the charted session are shown.
  const newsMarkers = useMemo<NewsMarker[] | undefined>(() => {
    const arts = enrich?.news?.articles
    const candles = priceView.candles as Array<{ time: number }>
    if (!arts?.length || !candles.length) return undefined
    const first = candles[0].time
    const last = candles[candles.length - 1].time
    const out: NewsMarker[] = []
    for (const a of arts) {
      const ts = Number(a.published_at || a.publish_ts || a.detected_at)
      if (!Number.isFinite(ts) || ts < first || ts > last) continue
      out.push({ time: bucketStart(ts, tf), sentiment: a.sentiment ?? null, headline: a.headline || a.title })
    }
    return out.length ? out : undefined
  }, [enrich, priceView, tf])

  return (
    <div>
      {/* Toolbar */}
      <div className="flex items-center gap-3 mb-2 flex-wrap">
        <input
          value={input}
          onChange={e => setInput(e.target.value.toUpperCase())}
          onKeyDown={e => e.key === 'Enter' && load()}
          placeholder="Ticker (e.g. AAPL)"
          className="w-[140px] bg-bg border border-border text-sm text-white rounded px-3 py-2 font-mono focus:outline-none focus:border-accent placeholder:text-slate-600"
        />
        <button
          onClick={load}
          disabled={!input.trim()}
          className="px-4 py-2 bg-accent text-white text-sm font-medium rounded hover:bg-sky-400 disabled:opacity-50 transition-colors"
        >
          {loading ? 'Loading…' : 'Load Chart'}
        </button>

        {/* Window selector (intraday only) */}
        <div className="flex items-stretch rounded overflow-hidden border border-border">
          {WINDOWS.map(w => (
            <button key={w.key} onClick={() => setWin(w.key)}
              className={`px-3 py-1.5 text-xs transition-colors ${win === w.key ? 'bg-accent text-white' : 'bg-surface text-neutral hover:text-white'}`}>
              {w.label}
            </button>
          ))}
        </div>

        {ticker && <span className="text-accent font-mono font-bold text-lg ml-1">{ticker}</span>}
        {/* Structured-news alert — fires only when FeedFlash has recent news for the ticker */}
        {enrich?.news_alert && (
          <span
            title={`${enrich.news_alert_count} structured news item(s) in the last 3 days`}
            className="flex items-center gap-1 text-[11px] font-semibold text-red-400 bg-red-500/10 border border-red-500/40 rounded px-2 py-0.5 animate-pulse"
          >
            ▲ NEWS {enrich.news_alert_count}
          </span>
        )}
        {data?.date && view === 'candles' && (
          <span className="text-xs text-neutral">{data.date} · {data.n} bars</span>
        )}
        <Link
          to="/charts-grid"
          title="Multi-ticker chart wall — top movers"
          className="ml-auto flex items-center gap-1.5 rounded border border-border px-3 py-2 text-xs text-neutral hover:border-accent hover:text-white transition-colors"
        >
          <span aria-hidden>▦</span> Grid
        </Link>
      </div>

      {/* View selector */}
      <div className="flex items-center gap-1 mb-3 border-b border-border flex-wrap">
        {VIEWS.map(v => (
          <button key={v.key} onClick={() => setView(v.key)}
            className={`px-3 py-1.5 text-xs transition-colors border-b-2 -mb-px ${
              view === v.key ? 'text-white border-accent' : 'text-neutral border-transparent hover:text-white'
            }`}>
            {v.label}
          </button>
        ))}
        <span className="ml-auto text-[10px] text-neutral pr-1">1-min intraday · extended hours 04:00–20:00 ET</span>
      </div>

      {!ticker ? (
        <div className="text-center py-20 text-neutral">
          <div className="text-4xl mb-3">📊</div>
          <div className="text-sm">Enter a ticker symbol and click Load Chart.</div>
        </div>
      ) : (
        <>
          {view === 'candles' ? (
            error ? (
              <div className="bg-surface border border-border rounded-lg p-8 text-center text-neutral">
                <div className="text-sm">{error}</div>
              </div>
            ) : data ? (
              <div className="space-y-3">
                {/* Price-chart controls: client-side timeframe resample + overlays */}
                <div className="flex items-center gap-3 flex-wrap text-xs">
                  <div className="flex items-center gap-1">
                    <span className="text-neutral mr-1">Timeframe</span>
                    <div className="flex items-stretch rounded overflow-hidden border border-border">
                      {TIMEFRAMES.map(t => (
                        <button key={t.min} onClick={() => setTf(t.min)}
                          className={clsx('px-2.5 py-1 transition-colors',
                            tf === t.min ? 'bg-accent text-white' : 'bg-surface text-neutral hover:text-white')}>
                          {t.label}
                        </button>
                      ))}
                    </div>
                    <span className="text-neutral ml-1 tabular-nums">{priceView.count} bars</span>
                  </div>
                  <label className="flex items-center gap-1.5 cursor-pointer select-none">
                    <input type="checkbox" checked={showDensity} onChange={e => setShowDensity(e.target.checked)}
                      className="accent-orange-500 cursor-pointer" />
                    <span style={{ color: '#FF9800' }}>Density</span>
                  </label>
                  <label className="flex items-center gap-1.5 cursor-pointer select-none">
                    <input type="checkbox" checked={showSentiment} onChange={e => setShowSentiment(e.target.checked)}
                      className="accent-green-500 cursor-pointer" />
                    <span style={{ color: '#4CAF50' }}>Sentiment</span>
                  </label>
                  <label className="flex items-center gap-1.5 cursor-pointer select-none">
                    <input type="checkbox" checked={showStrategy} onChange={e => setShowStrategy(e.target.checked)}
                      className="accent-sky-500 cursor-pointer" />
                    <span className="text-accent">Strategy ▲▼</span>
                  </label>
                  <label className="flex items-center gap-1.5 text-neutral">
                    Rolling Window
                    <select
                      value={overlayRollingMinutes}
                      onChange={event => setOverlayRollingMinutes(Number(event.target.value))}
                      className="rounded border border-border bg-bg px-2 py-1 text-xs text-white focus:border-accent focus:outline-none"
                    >
                      {OVERLAY_ROLLING_WINDOWS.map(option => (
                        <option key={option.value} value={option.value}>{option.label}</option>
                      ))}
                    </select>
                  </label>
                  {showStrategy && (
                    <label className="flex items-center gap-1 select-none">
                      <span className="text-neutral">Stop</span>
                      <select value={stopPct} onChange={e => setStopPct(Number(e.target.value))}
                        className="bg-surface border border-border rounded px-1 py-0.5 text-xs text-white cursor-pointer">
                        {[2, 5, 10, 30].map(p => (
                          <option key={p} value={p}>{p}%{p === 30 ? ' (spec)' : ''}</option>
                        ))}
                      </select>
                    </label>
                  )}
                  {(showDensity || showSentiment) && socialMsg && (
                    <span className="text-neutral">{socialMsg}</span>
                  )}
                  <label className="flex items-center gap-1.5 cursor-pointer select-none">
                    <input type="checkbox" checked={showWatchers} onChange={e => setShowWatchers(e.target.checked)}
                      className="accent-blue-400 cursor-pointer" />
                    <span className="text-blue-300">Watchers</span>
                  </label>
                  {showWatchers && watcherMsg && (
                    <span className="text-neutral">{watcherMsg}</span>
                  )}
                  {showStrategy && signalsMsg && (
                    <span className="text-neutral">{signalsMsg}</span>
                  )}
                  <span className="ml-auto text-[10px] text-neutral">1-min intraday only · resampled client-side</span>
                </div>
                <ChartCard title="Candlestick + Bollinger Bands (20,2)" height={300}>
                  <CandlestickChart candles={priceView.candles as any} bollinger={priceView.bollinger as any}
                    densityOverlay={priceView.density} sentimentOverlay={priceView.sentiment}
                    watcherOverlay={priceView.watchers}
                    strategyMarkers={strategyMarkers} newsMarkers={newsMarkers} />
                </ChartCard>
                <ChartCard title="RSI (14)" height={130}>
                  <RSIChart data={(priceView.rsi ?? []) as any} />
                </ChartCard>
                <ChartCard title="MACD (12, 26, 9)" height={150}>
                  <MACDChart data={priceView.macd as any} />
                </ChartCard>
              </div>
            ) : (
              <div className="text-neutral text-sm animate-pulse p-4">Loading chart…</div>
            )
          ) : (
            <div className="bg-surface border border-border rounded-lg overflow-hidden" style={{ height: 460 }}>
              <ResearchChart ticker={ticker} mode={view} window={win} date={urlDate} />
            </div>
          )}

          {/* Per-ticker enrichments below the chart: 3-day news + social/gossip */}
          <TickerEnrichPanels ticker={ticker} enrich={enrich} loaded={enrichLoaded} />
          <PredictionSignals rows={predictions} />
        </>
      )}
    </div>
  )
}

function ChartCard({ title, height, children }: { title: string; height: number; children: React.ReactNode }) {
  return (
    <div className="bg-surface border border-border rounded-lg overflow-hidden">
      <div className="px-3 py-2 border-b border-border">
        <span className="text-xs text-neutral font-medium uppercase tracking-wide">{title}</span>
      </div>
      <div style={{ height }}>{children}</div>
    </div>
  )
}

// ── Recent Prediction Signals panel ───────────────────────────────────────────
// Market-wide model signals from /api/prediction/signals, restyled to this page.
interface PredictionRow {
  ticker: string
  company?: string
  decision?: string
  created_at?: string
  entry_price?: number
  baseline_signal?: { direction?: string; confidence?: number } | null
  features?: { change_pct?: number } | null
}

function fmtSignalTime(iso?: string): string {
  if (!iso) return ''
  const d = new Date(iso)
  return isNaN(d.getTime()) ? '' : d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function PredictionSignals({ rows }: { rows: PredictionRow[] | null }) {
  if (!rows || !rows.length) return null
  return (
    <div className="bg-surface border border-border rounded-lg overflow-hidden mt-3">
      <div className="px-3 py-2 border-b border-border flex items-center gap-2">
        <span className="text-xs text-neutral font-medium uppercase">Recent Prediction Signals</span>
        <span className="text-[10px] text-neutral">market-wide · latest {Math.min(rows.length, 8)}</span>
      </div>
      <div className="divide-y divide-border/60">
        {rows.slice(0, 8).map((row, index) => {
          const chg = row.features?.change_pct
          const conf = row.baseline_signal?.confidence
          return (
            <div key={`${row.ticker}-${index}`} className="grid grid-cols-[120px_60px_1fr_84px] gap-2 px-3 py-2 text-xs items-center">
              <span className="font-mono text-neutral truncate">{fmtSignalTime(row.created_at)}</span>
              <span className="font-mono text-white font-semibold">{row.ticker}</span>
              <span className="text-slate-200 truncate">{row.decision || row.baseline_signal?.direction || 'signal'}{row.company ? ` · ${row.company}` : ''}</span>
              <span className={clsx('font-mono text-right', chg != null && chg > 0 ? 'text-emerald-400' : chg != null && chg < 0 ? 'text-orange-400' : 'text-neutral')}>
                {chg == null ? (conf != null ? `${(conf * 100).toFixed(0)}%` : '—') : `${chg > 0 ? '+' : ''}${Number(chg).toFixed(1)}%`}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
