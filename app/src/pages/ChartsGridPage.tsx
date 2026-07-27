'use client'
import useSWR from 'swr'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { clsx } from 'clsx'
import { CandlestickChart } from './CandlestickChart'
import type { ScreenerRow } from '@/lib/types'

const fetcher = (url: string) => fetch(url, { cache: 'no-store' }).then(r => r.json())
const INITIAL_VISIBLE_COUNT = 25
const VISIBLE_BATCH_COUNT = 25
const DEFAULT_SIGNAL = 'top_gainers'
const MIRROR_DATA_VERSION = 'finviz_mover_identity_v3'
const MIRROR_QUOTE_SOURCE = 'finviz_elite_screener'
const GRID_WINDOW_MIN = 5
const GRID_WINDOW_MAX = 360

const ROLLING_WINDOWS = [
  { value: 'adaptive', label: 'Adaptive' },
  { value: '5', label: '5m' },
  { value: '15', label: '15m' },
  { value: '30', label: '30m' },
  { value: '60', label: '1h' },
  { value: '120', label: '2h' },
  { value: '180', label: '3h' },
  { value: '240', label: '4h' },
  { value: '360', label: '6h' },
]

const LATEST_NEWS = [
  { value: '0', label: 'Any' },
  { value: '1', label: 'Today' },
  { value: '3', label: '3 Days' },
  { value: '7', label: 'This Week' },
  { value: '30', label: 'This Month' },
]

const SIGNALS = [
  { value: 'top_gainers', label: 'Top Gainers' },
  { value: 'top_losers', label: 'Top Losers' },
  { value: 'most_active', label: 'Most Active' },
  { value: 'unusual_volume', label: 'Unusual Volume' },
  { value: 'most_volatile', label: 'Most Volatile' },
]

function num(value: unknown): number | null {
  if (value == null || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function compact(value: unknown): string {
  const n = num(value)
  if (n == null || n === 0) return '--'
  const a = Math.abs(n)
  if (a >= 1e12) return `${(n / 1e12).toFixed(2)}T`
  if (a >= 1e9) return `${(n / 1e9).toFixed(2)}B`
  if (a >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (a >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return n.toLocaleString()
}

function pct(value: unknown, signed = true): string {
  const n = num(value)
  if (n == null) return '--'
  return `${signed && n > 0 ? '+' : ''}${n.toFixed(2)}%`
}

function money(value: unknown): string {
  const n = num(value)
  if (n == null || n <= 0) return '--'
  return `$${n.toFixed(n < 10 ? 3 : 2)}`
}

function whenLabel(value?: number | string | null): string {
  if (value == null || value === '') return ''
  const raw = typeof value === 'string' && Number.isNaN(Number(value)) ? Date.parse(value) / 1000 : Number(value)
  if (!Number.isFinite(raw) || raw <= 0) return ''
  const sec = raw > 1_000_000_000_000 ? Math.floor(raw / 1000) : raw
  return new Date(sec * 1000).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}

function ageLabel(value?: number | string | null): string {
  if (value == null || value === '') return '--'
  const raw = typeof value === 'string' && Number.isNaN(Number(value)) ? Date.parse(value) / 1000 : Number(value)
  if (!Number.isFinite(raw) || raw <= 0) return '--'
  const sec = raw > 1_000_000_000_000 ? Math.floor(raw / 1000) : raw
  const age = Math.max(0, Math.floor(Date.now() / 1000) - sec)
  if (age < 60) return 'now'
  if (age < 3600) return `${Math.floor(age / 60)}m`
  if (age < 86_400) return `${Math.floor(age / 3600)}h`
  return `${Math.floor(age / 86_400)}d`
}

function secondsFromTimestamp(value?: number | string | null): number | null {
  if (value == null || value === '') return null
  const raw = typeof value === 'string' && Number.isNaN(Number(value)) ? Date.parse(value) / 1000 : Number(value)
  if (!Number.isFinite(raw) || raw <= 0) return null
  const sec = raw > 1_000_000_000_000 ? Math.floor(raw / 1000) : Math.floor(raw)
  return sec > 0 ? sec : null
}

function companySignalTokens(company?: string | null): string[] {
  return String(company || '')
    .toLowerCase()
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

function articleMatchesTickerOrCompany(row: ScreenerRow, article: any): boolean {
  const text = [
    article?.title,
    article?.headline,
    article?.summary,
    article?.bodyText,
    article?.content,
    article?.company,
    article?.url,
  ].filter(Boolean).join(' ').toLowerCase()
  if (!text.trim()) return false
  const ticker = String(row.ticker || '').toLowerCase()
  const companyMatched = companySignalTokens(row.company).some(token => text.includes(token))
  if (companyMatched) return true
  if (!ticker) return false

  // Prefer explicit article text/cashtags over backend ticker fields alone.
  // Some symbols collide across exchanges, so a stored ticker match without
  // visible ticker/company evidence can attach the wrong company's news.
  if (new RegExp(`\\$${ticker}\\b`, 'i').test(text)) return true
  return new RegExp(`(^|[^a-z0-9])${ticker}([^a-z0-9]|$)`, 'i').test(text)
}

function uniqueRows(rows: ScreenerRow[]): ScreenerRow[] {
  const seen = new Set<string>()
  const out: ScreenerRow[] = []
  for (const row of rows) {
    const ticker = String(row?.ticker || '').toUpperCase()
    if (!ticker || seen.has(ticker)) continue
    seen.add(ticker)
    out.push({ ...row, ticker })
  }
  return out
}

function rowList(payload: any): ScreenerRow[] {
  const candidates = [
    payload?.tickers,
    payload?.rows,
    payload?.data,
    Array.isArray(payload) ? payload : null,
  ]
  const rows = candidates.find(candidate => Array.isArray(candidate) && candidate.length)
    ?? candidates.find(candidate => Array.isArray(candidate))
    ?? []
  return uniqueRows(Array.isArray(rows) ? rows : [])
}

function resolvedRollingWindowMinutes(value: string): number {
  if (value === 'adaptive') return GRID_WINDOW_MAX
  const minutes = Number(value)
  return Number.isFinite(minutes) ? Math.max(GRID_WINDOW_MIN, Math.min(GRID_WINDOW_MAX, Math.round(minutes))) : GRID_WINDOW_MAX
}

type ChartsGridPageProps = {
  embedded?: boolean
  socialWindow?: string
  onSocialWindowChange?: (value: string) => void
}

export function ChartsGridPage({ embedded = false, socialWindow, onSocialWindowChange }: ChartsGridPageProps = {}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const [signal, setSignal] = useState(searchParams.get('signal') || DEFAULT_SIGNAL)
  const [localSocialWindow, setLocalSocialWindow] = useState(searchParams.get('window_minutes') || 'adaptive')
  const [recentDays, setRecentDays] = useState(searchParams.get('recent_days') || '0')
  const [keyword, setKeyword] = useState(searchParams.get('keyword') || '')
  const [search, setSearch] = useState(searchParams.get('search') || '')
  const [visibleCount, setVisibleCount] = useState(INITIAL_VISIBLE_COUNT)
  const [refreshNonce, setRefreshNonce] = useState(() => Date.now())
  const loadMoreRef = useRef<HTMLDivElement>(null)
  const activeSocialWindow = socialWindow ?? localSocialWindow
  const activeWindowMinutes = resolvedRollingWindowMinutes(activeSocialWindow)
  const activeWindowLabel = ROLLING_WINDOWS.find(option => option.value === activeSocialWindow)?.label || `${activeWindowMinutes}m`

  const setActiveSocialWindow = (value: string) => {
    onSocialWindowChange?.(value)
    if (!onSocialWindowChange) setLocalSocialWindow(value)
    setVisibleCount(INITIAL_VISIBLE_COUNT)
  }

  useEffect(() => {
    if (embedded) {
      setVisibleCount(INITIAL_VISIBLE_COUNT)
      return
    }
    const next = new URLSearchParams(searchParams)
    signal === DEFAULT_SIGNAL ? next.delete('signal') : next.set('signal', signal)
    activeSocialWindow === 'adaptive' ? next.delete('window_minutes') : next.set('window_minutes', activeSocialWindow)
    recentDays === '0' ? next.delete('recent_days') : next.set('recent_days', recentDays)
    keyword.trim() ? next.set('keyword', keyword.trim()) : next.delete('keyword')
    search.trim() ? next.set('search', search.trim().toUpperCase()) : next.delete('search')
    setSearchParams(next, { replace: true })
    setVisibleCount(INITIAL_VISIBLE_COUNT)
    // Keep the URL shareable without making searchParams a dependency loop.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [embedded, signal, activeSocialWindow, recentDays, keyword, search, setSearchParams])

  const screenerUrl = useMemo(() => {
    const params = new URLSearchParams({
      mirror: '1',
      compact: '1',
      limit: '5000',
      // Keep this broad screener query as a fallback; top-gainer chart cards
      // should be anchored to the FinViz mover endpoint below.
      include_stale: '1',
      quote_source: MIRROR_QUOTE_SOURCE,
      signal,
      orderBy: 'change_pct',
      orderDir: signal === 'top_losers' ? 'asc' : 'desc',
      _v: MIRROR_DATA_VERSION,
      _r: String(refreshNonce),
    })
    params.set('window_minutes', String(activeWindowMinutes))
    if (search.trim()) params.set('search', search.trim().toUpperCase())
    return `/api/screener?${params.toString()}`
  }, [signal, activeWindowMinutes, search, refreshNonce])

  const finvizUrl = useMemo(() => {
    if (signal !== 'top_gainers') return null
    const params = new URLSearchParams({
      limit: '100',
      days: recentDays && recentDays !== '0' ? recentDays : '2',
      window_minutes: String(activeWindowMinutes),
      _v: MIRROR_DATA_VERSION,
      _r: String(refreshNonce),
    })
    return `/api/finviz/movers?${params.toString()}`
  }, [signal, activeWindowMinutes, recentDays, refreshNonce])

  const { data, error, isLoading, mutate } = useSWR(screenerUrl, fetcher, { revalidateOnFocus: false })
  const { data: finvizData, error: finvizError, isLoading: finvizLoading, mutate: mutateFinviz } = useSWR(finvizUrl, fetcher, { revalidateOnFocus: false })
  const rows = useMemo(() => {
    const screenerRows = rowList(data)
    const finvizRows = rowList(finvizData)
    const baseRows = signal === 'top_gainers' && finvizRows.length ? finvizRows : screenerRows
    const searchTicker = search.trim().toUpperCase()
    return searchTicker ? baseRows.filter(row => String(row.ticker || '').toUpperCase() === searchTicker) : baseRows
  }, [data, finvizData, search, signal])
  const keywordClean = keyword.trim().toLowerCase()
  const visibleRows = rows.slice(0, Math.min(visibleCount, rows.length))
  const hasMoreRows = visibleRows.length < rows.length
  const lastUpdated = new Date().toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' })
  const gridError = error || finvizError
  const gridLoading = isLoading || Boolean(finvizUrl && finvizLoading && !rows.length)

  const refresh = () => {
    setRefreshNonce(n => n + 1)
    mutate()
    mutateFinviz()
  }

  const showMoreRows = () => {
    setVisibleCount(count => Math.min(rows.length, count + VISIBLE_BATCH_COUNT))
  }

  useEffect(() => {
    const el = loadMoreRef.current
    if (!el || !hasMoreRows) return
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting)) showMoreRows()
    }, { rootMargin: '900px' })
    observer.observe(el)
    return () => observer.disconnect()
  }, [hasMoreRows, rows.length, visibleRows.length])

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold text-white">Charts Grid</h1>
          <p className="mt-0.5 text-xs text-neutral">Top-mover chart wall with live candles, watcher support, social context, and ticker-specific news.</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-neutral">
          <span>FinViz top movers</span>
          <span>rolling {activeWindowLabel}</span>
          <span>{visibleRows.length.toLocaleString()} shown of {rows.length.toLocaleString()} stocks</span>
          <span>updated {lastUpdated}</span>
          <span className={clsx('rounded border px-2 py-1', gridError ? 'border-red-500/40 text-red-300' : gridLoading ? 'border-amber-500/40 text-amber-300' : 'border-emerald-500/40 text-emerald-300')}>
            {gridError ? 'partial' : gridLoading ? 'loading' : 'live'}
          </span>
        </div>
      </div>

      <section className="rounded-lg border border-border bg-[#111317] px-3 py-2">
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-1.5 text-xs text-neutral">
            Signal
            <select value={signal} onChange={event => setSignal(event.target.value)} className="rounded border border-border bg-bg px-2 py-1.5 text-sm text-white focus:border-accent focus:outline-none">
              {SIGNALS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-xs text-neutral">
            Rolling Window
            <select value={activeSocialWindow} onChange={event => setActiveSocialWindow(event.target.value)} className="rounded border border-border bg-bg px-2 py-1.5 text-sm text-white focus:border-accent focus:outline-none">
              {ROLLING_WINDOWS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-xs text-neutral">
            Latest News
            <select value={recentDays} onChange={event => setRecentDays(event.target.value)} className="rounded border border-border bg-bg px-2 py-1.5 text-sm text-white focus:border-accent focus:outline-none">
              {LATEST_NEWS.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="flex items-center gap-1.5 text-xs text-neutral">
            News Keywords
            <input value={keyword} onChange={event => setKeyword(event.target.value)} placeholder="headline filter" className="w-[180px] rounded border border-border bg-bg px-2 py-1.5 text-sm text-white placeholder:text-slate-600 focus:border-accent focus:outline-none" />
          </label>
          <label className="flex items-center gap-1.5 text-xs text-neutral">
            Stock
            <input value={search} onChange={event => setSearch(event.target.value.toUpperCase())} placeholder="ticker" className="w-[120px] rounded border border-border bg-bg px-2 py-1.5 font-mono text-sm text-white placeholder:text-slate-600 focus:border-accent focus:outline-none" />
          </label>
          <button onClick={refresh} className="rounded border border-border px-3 py-1.5 text-xs text-neutral hover:border-accent hover:text-white">
            Refresh
          </button>
          <span className="ml-auto text-xs text-neutral">Scroll loads more stocks</span>
        </div>
      </section>

      {gridError && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-200">Chart grid data could not refresh.</div>}

      {visibleRows.length ? (
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-2 2xl:grid-cols-3">
          {visibleRows.map(row => (
            <ChartGridCard key={row.ticker} row={row} signal={signal} recentDays={recentDays} keyword={keywordClean} refreshNonce={refreshNonce} />
          ))}
        </div>
      ) : (
        <div className="rounded-lg border border-border bg-surface py-16 text-center text-sm text-neutral">
          {gridLoading ? 'Loading stocks...' : 'No stocks match the current chart filters.'}
        </div>
      )}

      {visibleRows.length > 0 && (
        <div className="flex items-center justify-center gap-3 py-2 text-xs">
          <div ref={loadMoreRef} className="h-8 w-px" />
          {hasMoreRows ? (
            <button onClick={showMoreRows} className="rounded border border-border px-3 py-1.5 text-neutral hover:border-accent hover:text-white">
              Load more stocks
            </button>
          ) : (
            <span className="text-neutral">End of current screener universe</span>
          )}
        </div>
      )}
    </div>
  )
}

function ChartGridCard({ row, signal, recentDays, keyword, refreshNonce }: {
  row: ScreenerRow
  signal: string
  recentDays: string
  keyword: string
  refreshNonce: number
}) {
  const ticker = String(row.ticker || '').toUpperCase()
  const cardRef = useRef<HTMLDivElement>(null)
  const [visible, setVisible] = useState(false)

  useEffect(() => {
    const el = cardRef.current
    if (!el || visible) return
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting)) {
        setVisible(true)
        observer.disconnect()
      }
    }, { rootMargin: '360px' })
    observer.observe(el)
    return () => observer.disconnect()
  }, [visible])

  const chartUrl = ticker && visible ? `/api/charts/${encodeURIComponent(ticker)}?tf=1d&_r=${refreshNonce}` : null
  const articleDays = recentDays && recentDays !== '0' ? recentDays : '30'
  const articleUrl = ticker && visible ? `/api/articles?ticker=${encodeURIComponent(ticker)}&limit=12&recent_days=${articleDays}&facets=0&_r=${refreshNonce}` : null
  const { data: chart, error: chartError, isLoading: chartLoading } = useSWR(chartUrl, fetcher, { revalidateOnFocus: false })
  const { data: articleData } = useSWR(articleUrl, fetcher, { revalidateOnFocus: false })

  const candles = Array.isArray(chart?.candles) ? chart.candles : []
  const displayPrice = num(row.price)
  const displayChange = num(row.change_pct)
  const displayVolume = num(row.volume)
  // `row.source` never existed on ScreenerRow — the backend emits `sources`, a
  // string[] (screener.js normalises it as `sources: doc.sources || []`). The
  // old fallback therefore read undefined and always dropped through to the
  // literal, so a row with a null quote_source but a real source list was
  // mislabelled "FinViz top mover".
  const quoteSourceLabel = row.quote_source || row.sources?.[0] || 'FinViz top mover'
  const rawArticles = useMemo(() => {
    const source = Array.isArray(articleData?.articles) ? articleData.articles : []
    const tickerSpecific = source.filter((article: any) => articleMatchesTickerOrCompany(row, article))
    if (!keyword) return tickerSpecific
    return tickerSpecific.filter((article: any) => String(article.title || article.headline || '').toLowerCase().includes(keyword))
  }, [articleData, keyword, row])
  const rawArticleCount = Array.isArray(articleData?.articles) ? articleData.articles.length : 0
  const rejectedAmbiguousArticles = Math.max(0, rawArticleCount - rawArticles.length)
  const articles = rawArticles

  const change = displayChange
  const up = Number(change || 0) >= 0
  const topArticle = articles[0]
  const cardStats: Array<[string, string]> = [
    ['Vol', compact(displayVolume)],
    ['RelVol', row.rel_volume != null ? `${Number(row.rel_volume).toFixed(2)}x` : '--'],
    ['MCap', compact(row.market_cap)],
    ['RSI', row.rsi != null ? Number(row.rsi).toFixed(1) : '--'],
  ]

  return (
    <article ref={cardRef} className="overflow-hidden rounded-lg border border-border bg-surface">
      <div className="border-b border-border bg-[#1d2635] px-3 py-2.5">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex flex-wrap items-baseline gap-2">
              <Link to={`/charts?t=${encodeURIComponent(ticker)}`} className="font-mono text-xl font-bold text-accent hover:text-sky-300">{ticker}</Link>
              <span className={clsx('font-mono text-sm font-semibold', change == null ? 'text-amber-200' : up ? 'text-emerald-400' : 'text-red-400')}>{change == null ? '--' : pct(change)}</span>
              <span className="truncate text-sm font-medium text-slate-200">{row.company || ticker}</span>
            </div>
            <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-neutral">
              <span className="font-mono text-slate-200">{money(displayPrice)}</span>
              <span>{row.exchange || '--'}</span>
              <span>{row.sector || 'Sector unavailable'}</span>
              <span>{ageLabel(row.quote_updated_at)} old</span>
            </div>
          </div>
          <Link to={`/charts?t=${encodeURIComponent(ticker)}`} className="shrink-0 rounded border border-border px-2.5 py-1.5 text-xs text-neutral hover:border-accent hover:text-white">
            Open
          </Link>
        </div>
      </div>

      <div className="relative bg-bg" style={{ height: 255 }}>
        {!visible || chartLoading ? (
          <div className="flex h-full items-center justify-center text-xs text-neutral animate-pulse">Loading chart...</div>
        ) : chartError || chart?.error ? (
          <div className="flex h-full items-center justify-center text-xs text-red-300">Chart unavailable</div>
        ) : candles.length ? (
          <CandlestickChart
            candles={candles as any}
            bollinger={chart?.bollinger as any}
            chartStyle="candles"
            showBollinger
            showMarkers={false}
            minHeight={0}
          />
        ) : (
          <div className="flex h-full items-center justify-center text-xs text-neutral">No candle data</div>
        )}
      </div>

      <div className="border-t border-border px-3 py-2">
        <div className="grid grid-cols-4 gap-1.5 text-center text-[11px]">
          {cardStats.map(([label, value]) => <MetricBadge key={label} label={label} value={value} />)}
        </div>
        <div className="mt-2 grid grid-cols-4 gap-1.5 text-center text-[11px]">
          <MetricBadge label="News" value={compact(articles.length)} />
          <MetricBadge label="Msgs" value={compact(row.message_count)} />
          <MetricBadge label="Watch" value={compact((row as any).stocktwits_watcher_count)} />
          <MetricBadge label="Sent" value={row.avg_sentiment != null ? Number(row.avg_sentiment).toFixed(2) : '--'} />
        </div>
        <div className="mt-2 rounded border border-border/60 bg-bg/50 px-2 py-2">
          {topArticle ? (
            <a href={topArticle.url || '#'} target="_blank" rel="noreferrer" className="block hover:text-sky-200">
              <div className="mb-1 flex items-center justify-between gap-2 text-[10px] text-neutral">
                <span className="truncate">{topArticle.source || 'News'}</span>
                <span className="shrink-0 font-mono">{whenLabel(topArticle.publish_date || topArticle.detected_at || topArticle.fetched_date) || '--'}</span>
              </div>
              <div className="line-clamp-2 text-xs text-slate-100">{topArticle.title || topArticle.headline || 'Untitled article'}</div>
            </a>
          ) : (
            <div className="text-xs text-neutral">
              No ticker-specific news in this window.
              {rejectedAmbiguousArticles > 0 && (
                <span className="ml-1 text-amber-200">{rejectedAmbiguousArticles} ambiguous row{rejectedAmbiguousArticles === 1 ? '' : 's'} rejected.</span>
              )}
            </div>
          )}
          <div className="mt-1 text-[10px] text-slate-500">
            {quoteSourceLabel}
          </div>
        </div>
      </div>
    </article>
  )
}

function MetricBadge({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border border-border bg-bg/60 px-2 py-2">
      <div className="text-[10px] uppercase text-neutral">{label}</div>
      <div className="mt-1 font-mono text-slate-100">{value}</div>
    </div>
  )
}
