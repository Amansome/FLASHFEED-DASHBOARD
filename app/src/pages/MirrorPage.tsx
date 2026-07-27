'use client'
import useSWR from 'swr'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { clsx } from 'clsx'
import { CandlestickChart } from './CandlestickChart'
import { DecisionMapPanel } from './DecisionMapPanel'
import { overlaySeries, type LinePoint, type SocialSeries } from '@/lib/chartAgg'
import type { ScreenerRow } from '@/lib/types'
import { useTickerDatalistOptions } from '@/lib/useTickerUniverse'

const fetcher = (url: string) => fetch(url, { cache: 'no-store' }).then(r => r.json())
const INITIAL_VISIBLE_COUNT = 25
const VISIBLE_BATCH_COUNT = 25
const DEFAULT_SIGNAL = 'top_gainers'
const MIRROR_DATA_VERSION = 'finviz_mover_identity_v3'
const MIRROR_QUOTE_SOURCE = 'finviz_elite_screener'
const MIRROR_WINDOW_MIN = 5
const MIRROR_WINDOW_MAX = 360
const MIRROR_WINDOW_STEP = 5

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

function fixed(value: unknown, digits = 2): string {
  const n = num(value)
  return n == null ? '--' : n.toFixed(digits)
}

function sentimentLabel(value: unknown): string {
  if (value == null || value === '') return '--'
  const raw = String(value).trim()
  if (!raw) return '--'
  return raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase()
}

function sentimentClass(value: unknown): string {
  const raw = String(value || '').toLowerCase()
  if (raw.includes('bull') || raw.includes('positive')) return 'border-emerald-400/35 bg-emerald-500/10 text-emerald-300'
  if (raw.includes('bear') || raw.includes('negative')) return 'border-red-400/35 bg-red-500/10 text-red-300'
  return 'border-slate-500/35 bg-slate-500/10 text-slate-300'
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

function chartQuoteFromCandles(candles: any[]) {
  if (!Array.isArray(candles) || candles.length < 2) return null
  const last = candles[candles.length - 1]
  const previous = candles[candles.length - 2]
  const price = num(last?.close)
  const previousClose = num(previous?.close)
  if (price == null || previousClose == null || previousClose <= 0) return null
  const changePct = ((price - previousClose) / previousClose) * 100
  return {
    price,
    previousClose,
    changePct,
    volume: num(last?.volume),
    timestamp: secondsFromTimestamp(last?.time),
  }
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
  if (value === 'adaptive') return MIRROR_WINDOW_MAX
  const minutes = Number(value)
  return Number.isFinite(minutes) ? Math.max(MIRROR_WINDOW_MIN, Math.min(MIRROR_WINDOW_MAX, Math.round(minutes))) : MIRROR_WINDOW_MAX
}

function formatWindowMinutes(minutes: number): string {
  if (!Number.isFinite(minutes)) return '--'
  if (minutes >= 1440 && minutes % 1440 === 0) return `${minutes / 1440}d`
  if (minutes >= 60 && minutes % 60 === 0) return `${minutes / 60}h`
  return `${minutes}m`
}

function formatNewsWindow(days: string): string {
  if (days === '1') return 'today'
  if (days === '3') return '3 days'
  if (days === '7') return 'this week'
  if (days === '30') return '30 days'
  return '30 days'
}

function watcherMetricValue(row: ScreenerRow): string {
  const current = compact((row as any).stocktwits_watcher_count)
  const delta = num((row as any).watcher_feature?.delta)
  if (delta == null || delta === 0) return current
  const sign = delta > 0 ? '+' : ''
  return `${current} (${sign}${compact(delta)})`
}

type MirrorPageProps = {
  embedded?: boolean
  socialWindow?: string
  onSocialWindowChange?: (value: string) => void
}

export function MirrorPage({ embedded = false, socialWindow, onSocialWindowChange }: MirrorPageProps = {}) {
  const [searchParams, setSearchParams] = useSearchParams()
  const [signal, setSignal] = useState(searchParams.get('signal') || DEFAULT_SIGNAL)
  const [localSocialWindow, setLocalSocialWindow] = useState(searchParams.get('window_minutes') || 'adaptive')
  const [draftWindowMinutes, setDraftWindowMinutes] = useState(() => resolvedRollingWindowMinutes(searchParams.get('window_minutes') || 'adaptive'))
  const [recentDays, setRecentDays] = useState(searchParams.get('recent_days') || '0')
  const [keyword, setKeyword] = useState(searchParams.get('keyword') || '')
  const [search, setSearch] = useState(searchParams.get('search') || '')
  const tickerOptions = useTickerDatalistOptions(search)
  const [visibleCount, setVisibleCount] = useState(INITIAL_VISIBLE_COUNT)
  const [refreshNonce, setRefreshNonce] = useState(() => Date.now())
  const loadMoreRef = useRef<HTMLDivElement>(null)
  const activeSocialWindow = socialWindow ?? localSocialWindow
  const activeWindowMinutes = resolvedRollingWindowMinutes(activeSocialWindow)
  const selectedWindowLabel = ROLLING_WINDOWS.find(option => option.value === activeSocialWindow)?.label || `${activeSocialWindow}m`
  const activeWindowLabel = activeSocialWindow === 'adaptive' ? `${selectedWindowLabel} (${formatWindowMinutes(activeWindowMinutes)})` : selectedWindowLabel
  const draftWindowLabel = formatWindowMinutes(draftWindowMinutes)
  const windowApplying = draftWindowMinutes !== activeWindowMinutes

  const setActiveSocialWindow = (value: string) => {
    onSocialWindowChange?.(value)
    if (!onSocialWindowChange) setLocalSocialWindow(value)
    setVisibleCount(INITIAL_VISIBLE_COUNT)
  }

  const updateDraftWindowMinutes = (value: string | number) => {
    const minutes = Number(value)
    if (!Number.isFinite(minutes)) return
    setDraftWindowMinutes(Math.max(MIRROR_WINDOW_MIN, Math.min(MIRROR_WINDOW_MAX, Math.round(minutes))))
  }

  useEffect(() => {
    setDraftWindowMinutes(activeWindowMinutes)
  }, [activeWindowMinutes])

  useEffect(() => {
    if (draftWindowMinutes === activeWindowMinutes) return
    const timer = window.setTimeout(() => {
      setActiveSocialWindow(String(draftWindowMinutes))
    }, 300)
    return () => window.clearTimeout(timer)
    // setActiveSocialWindow intentionally stays local to this component; the
    // debounced commit should follow only the selected numeric value.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [draftWindowMinutes, activeWindowMinutes])

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
      // Keep this broad screener query as a fallback; top-gainer Mirror cards
      // should be anchored to the FinViz mover endpoint below.
      include_stale: '1',
      quote_source: MIRROR_QUOTE_SOURCE,
      signal,
      orderBy: 'change_pct',
      orderDir: signal === 'top_losers' ? 'asc' : 'desc',
      window_minutes: String(activeWindowMinutes),
      _v: MIRROR_DATA_VERSION,
      _r: String(refreshNonce),
    })
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

  const mirrorSWR = { revalidateOnFocus: false, refreshInterval: 60_000, keepPreviousData: false }
  const { data, error, isLoading, mutate } = useSWR(screenerUrl, fetcher, mirrorSWR)
  const { data: finvizData, error: finvizError, isLoading: finvizLoading, mutate: mutateFinviz } = useSWR(finvizUrl, fetcher, mirrorSWR)
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
  const mirrorError = error || finvizError
  const mirrorLoading = isLoading || Boolean(finvizUrl && finvizLoading && !rows.length)

  const refresh = () => {
    setRefreshNonce(Date.now())
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
          <h1 className="text-lg font-semibold text-white">Mirror</h1>
          <p className="mt-0.5 text-xs text-neutral">FinViz top-mover mirror: one ticker card at a time, chart first, latest source news underneath.</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-neutral">
          <span>FinViz top movers</span>
          <span>rolling {activeWindowLabel}</span>
          <span>{visibleRows.length.toLocaleString()} shown of {rows.length.toLocaleString()} stocks</span>
          <span>updated {lastUpdated}</span>
          <span className={clsx('rounded border px-2 py-1', mirrorError ? 'border-red-500/40 text-red-300' : mirrorLoading ? 'border-amber-500/40 text-amber-300' : 'border-emerald-500/40 text-emerald-300')}>
            {mirrorError ? 'partial' : mirrorLoading ? 'loading' : 'live'}
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
          <div className="flex min-w-[420px] flex-[1_1_560px] items-center gap-2 text-xs text-neutral lg:max-w-[720px]">
            <span className="whitespace-nowrap">Rolling Window</span>
            <button
              type="button"
              onClick={() => setActiveSocialWindow('adaptive')}
              className={clsx(
                'rounded border px-2 py-1.5 text-xs transition-colors',
                activeSocialWindow === 'adaptive'
                  ? 'border-accent/60 bg-accent/10 text-sky-200'
                  : 'border-border text-neutral hover:border-accent hover:text-white',
              )}
            >
              Adaptive
            </button>
            <input
              type="range"
              min={MIRROR_WINDOW_MIN}
              max={MIRROR_WINDOW_MAX}
              step={MIRROR_WINDOW_STEP}
              value={draftWindowMinutes}
              onInput={event => updateDraftWindowMinutes(event.currentTarget.value)}
              onChange={event => updateDraftWindowMinutes(event.currentTarget.value)}
              className="min-w-[240px] flex-1 accent-orange-500 cursor-pointer"
              aria-label="Mirror rolling window in minutes"
            />
            <span className="w-12 text-right font-mono text-sm text-white">{draftWindowLabel}</span>
            <span className={clsx('w-20 text-[11px]', windowApplying ? 'text-sky-300' : 'text-neutral')}>
              {windowApplying ? 'applying' : 'applied'}
            </span>
          </div>
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
            <input
              value={search}
              list="mirror-ticker-universe"
              onChange={event => setSearch(event.target.value.toUpperCase())}
              placeholder="ticker"
              className="w-[120px] rounded border border-border bg-bg px-2 py-1.5 font-mono text-sm text-white placeholder:text-slate-600 focus:border-accent focus:outline-none"
            />
            <datalist id="mirror-ticker-universe">
              {tickerOptions.map(row => (
                <option key={row.ticker} value={row.ticker}>{row.company || row.exchange || row.ticker}</option>
              ))}
            </datalist>
          </label>
          <button onClick={refresh} className="rounded border border-border px-3 py-1.5 text-xs text-neutral hover:border-accent hover:text-white">
            Refresh
          </button>
          <span className="ml-auto text-xs text-neutral">Scroll loads more stocks</span>
        </div>
      </section>

      {mirrorError && <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-200">Mirror data could not refresh.</div>}

      {visibleRows.length ? (
        <div className="space-y-4">
          {visibleRows.map(row => (
            <MirrorCard
              key={row.ticker}
              row={row}
              signal={signal}
              recentDays={recentDays}
              keyword={keywordClean}
              refreshNonce={refreshNonce}
              rollingWindowMinutes={activeWindowMinutes}
            />
          ))}
        </div>
      ) : (
        <div className="rounded-lg border border-border bg-surface py-16 text-center text-sm text-neutral">
          {mirrorLoading ? 'Loading stocks...' : 'No stocks match the current Mirror filters.'}
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

function MirrorCard({ row, signal, recentDays, keyword, refreshNonce, rollingWindowMinutes }: {
  row: ScreenerRow
  signal: string
  recentDays: string
  keyword: string
  refreshNonce: number
  rollingWindowMinutes: number
}) {
  const ticker = String(row.ticker || '').toUpperCase()
  const cardRef = useRef<HTMLDivElement>(null)
  const [visible, setVisible] = useState(false)
  const [show3d, setShow3d] = useState(false)

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

  // Keep Mirror density on 1-minute buckets so the rolling-window slider uses
  // the same trailing message-count definition as the full Charts view.
  const chartBucketMinutes = 1
  const chartUrl = ticker && visible
    ? `/api/charts/${encodeURIComponent(ticker)}?tf=1m&events=1&window_minutes=${rollingWindowMinutes}&view_window_minutes=${rollingWindowMinutes}&bucket_minutes=${chartBucketMinutes}&_r=${refreshNonce}`
    : null
  const articleDays = recentDays && recentDays !== '0' ? recentDays : '30'
  const fallbackArticleDays = Number(articleDays) < 30 ? '30' : ''
  const articleUrl = ticker && visible
    ? `/api/articles?ticker=${encodeURIComponent(ticker)}&limit=12&recent_days=${articleDays}&facets=0&_r=${refreshNonce}`
    : null
  const cardSWR = { revalidateOnFocus: false, refreshInterval: 60_000, keepPreviousData: false }
  const { data: chart, error: chartError, isLoading: chartLoading } = useSWR(chartUrl, fetcher, cardSWR)
  const { data: articleData } = useSWR(articleUrl, fetcher, cardSWR)

  const candles = Array.isArray(chart?.candles) ? chart.candles : []
  const screenerQuoteSec = secondsFromTimestamp(row.quote_updated_at)
  const screenerAgeSeconds = screenerQuoteSec ? Math.max(0, Math.floor(Date.now() / 1000) - screenerQuoteSec) : null
  const displayPrice = num(row.price)
  const displayChange = num(row.change_pct)
  const displayVolume = num(row.volume)
  // See ChartsGridPage: ScreenerRow has `sources: string[]`, never `source`, so
  // the old fallback read undefined and the "Quote Source" cell said
  // "FinViz top mover" whenever quote_source was null.
  const quoteSourceLabel = row.quote_source || row.sources?.[0] || 'FinViz top mover'
  const selectedArticles = useMemo(() => {
    const source = Array.isArray(articleData?.articles) ? articleData.articles : []
    const tickerSpecific = source.filter((article: any) => articleMatchesTickerOrCompany(row, article))
    if (!keyword) return tickerSpecific
    return tickerSpecific.filter((article: any) => String(article.title || article.headline || '').toLowerCase().includes(keyword))
  }, [articleData, keyword, row])
  const fallbackArticleUrl = ticker && visible && fallbackArticleDays && !selectedArticles.length
    ? `/api/articles?ticker=${encodeURIComponent(ticker)}&limit=12&recent_days=${fallbackArticleDays}&facets=0&_r=${refreshNonce}`
    : null
  const { data: fallbackArticleData } = useSWR(fallbackArticleUrl, fetcher, cardSWR)
  const fallbackArticles = useMemo(() => {
    if (selectedArticles.length) return []
    const source = Array.isArray(fallbackArticleData?.articles) ? fallbackArticleData.articles : []
    const tickerSpecific = source.filter((article: any) => articleMatchesTickerOrCompany(row, article))
    if (!keyword) return tickerSpecific
    return tickerSpecific.filter((article: any) => String(article.title || article.headline || '').toLowerCase().includes(keyword))
  }, [fallbackArticleData, keyword, row, selectedArticles.length])
  const rawArticleCount = Array.isArray(articleData?.articles) ? articleData.articles.length : 0
  const rejectedAmbiguousArticles = Math.max(0, rawArticleCount - selectedArticles.length)
  const showingFallbackArticles = !selectedArticles.length && fallbackArticles.length > 0
  const articles = showingFallbackArticles ? fallbackArticles : selectedArticles
  const matchedNewsCount = articles.length || Number((row as any).news_article_count ?? (row as any).article_count ?? 0)
  const displaySentiment = num((row as any).sentiment ?? row.avg_sentiment)
  const chartOverlays = useMemo(() => {
    if (!candles.length) return { density: undefined, sentiment: undefined, watchers: undefined }
    const socialRows = Array.isArray((chart as any)?.social_series)
      ? (chart as any).social_series
      : Array.isArray((chart as any)?.social_density)
        ? (chart as any).social_density
        : []
    const socialSeries: SocialSeries = {
      labels: [],
      density: socialRows.map((point: any) => Number(point?.count ?? point?.message_count ?? point?.value ?? 0)).filter(Number.isFinite),
      times: socialRows.map((point: any) => {
        const raw = point?.time ?? point?.bucket_sec
        const parsed = typeof raw === 'string' ? Date.parse(raw) / 1000 : Number(raw)
        return Number.isFinite(parsed) ? parsed : 0
      }).filter(Boolean),
      sent_labels: [],
      scores: Array.isArray((chart as any)?.sentiment)
        ? (chart as any).sentiment.map((point: any) => Number(point?.value ?? 0)).filter(Number.isFinite)
        : socialRows.map((point: any) => Number(point?.sentiment ?? 0)).filter(Number.isFinite),
      sentiment_weights: socialRows.map((point: any) => Number(point?.count ?? point?.message_count ?? point?.value ?? 0)).filter(Number.isFinite),
      scores_smooth: Array.isArray((chart as any)?.sentiment)
        ? (chart as any).sentiment.map((point: any) => Number(point?.value ?? 0)).filter(Number.isFinite)
        : socialRows.map((point: any) => Number(point?.sentiment ?? 0)).filter(Number.isFinite),
      sent_times: Array.isArray((chart as any)?.sentiment)
        ? (chart as any).sentiment.map((point: any) => {
            const raw = point?.time
            const parsed = typeof raw === 'string' ? Date.parse(raw) / 1000 : Number(raw)
            return Number.isFinite(parsed) ? parsed : 0
          }).filter(Boolean)
        : socialRows.map((point: any) => {
            const raw = point?.time ?? point?.bucket_sec
            const parsed = typeof raw === 'string' ? Date.parse(raw) / 1000 : Number(raw)
            return Number.isFinite(parsed) ? parsed : 0
          }).filter(Boolean),
    }
    const overlays = overlaySeries(candles as any, socialSeries, 1, rollingWindowMinutes)
    const watcherSource = (chart as any)?.watcher_series
    const watchers = (watcherSource?.times || []).map((time: number, index: number) => ({
      time,
      value: Number((watcherSource?.watchers || [])[index]),
    })).filter((point: any) => Number.isFinite(point.time) && Number.isFinite(point.value))
    return {
      density: overlays.density,
      sentiment: overlays.sentiment,
      watchers,
    }
  }, [candles, chart, rollingWindowMinutes])

  const miniPriceDensity = useMemo(() => {
    if (!candles.length) return null
    const density = (chartOverlays.density || []) as LinePoint[]
    const densityByTime = new Map(density.map(point => [Number(point.time), Number(point.value || 0)]))
    const source = candles
      .map((candle: any) => ({
        time: Number(candle.time),
        price: Number(candle.close),
        density: Number(densityByTime.get(Number(candle.time)) || 0),
      }))
      .filter(point => Number.isFinite(point.time) && Number.isFinite(point.price) && Number.isFinite(point.density))
    if (source.length < 2) return null
    const maxPoints = 96
    const step = Math.max(1, Math.ceil(source.length / maxPoints))
    const sampled = source.filter((_, index) => index % step === 0 || index === source.length - 1)
    return { ticker, rollingWindowMinutes, points: sampled }
  }, [candles, chartOverlays.density, rollingWindowMinutes, ticker])

  const change = displayChange
  const up = Number(change || 0) >= 0
  const fields: Array<[string, string]> = [
    ['Current Price', money(displayPrice)],
    ['Current Move', pct(displayChange)],
    ['Current Volume', compact(displayVolume)],
    ['Quote Source', quoteSourceLabel],
    ['Screener Age', ageLabel(row.quote_updated_at)],
    ['Market Cap', compact(row.market_cap)],
    ['P/E', fixed((row as any).pe_ratio)],
    ['Forward P/E', fixed((row as any).forward_pe)],
    ['PEG', fixed((row as any).peg)],
    ['P/S', fixed((row as any).ps_ratio)],
    ['P/B', fixed((row as any).pb_ratio)],
    ['Dividend', pct((row as any).dividend_yield, false)],
    ['EPS next Y', pct((row as any).eps_growth_next_y)],
    ['EPS this Y', pct((row as any).eps_growth_this_y)],
    ['Sales Q/Q', pct((row as any).sales_growth)],
    ['Insider Own', pct((row as any).insider_own, false)],
    ['Inst Own', pct((row as any).inst_own, false)],
    ['Short Float', pct((row as any).float_short, false)],
    ['ROE', pct((row as any).roe)],
    ['Beta', fixed((row as any).beta)],
    ['Avg Volume', compact(row.avg_volume)],
    ['Rel Volume', row.rel_volume != null ? `${Number(row.rel_volume).toFixed(2)}x` : '--'],
    ['RSI', row.rsi != null ? Number(row.rsi).toFixed(1) : '--'],
    ['Target Price', money((row as any).target_price)],
  ]

  return (
    <article ref={cardRef} className="overflow-hidden rounded-lg border border-border bg-surface">
      <div className="grid grid-cols-1 xl:grid-cols-[minmax(0,1.55fr)_minmax(320px,0.85fr)]">
        <div className="border-b border-border xl:border-b-0 xl:border-r">
          <div className="border-b border-border bg-[#1d2635] px-4 py-3">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex flex-wrap items-baseline gap-2">
                  <Link to={`/charts?t=${encodeURIComponent(ticker)}`} className="font-mono text-2xl font-bold text-accent hover:text-sky-300">{ticker}</Link>
                  <span className={clsx('font-mono text-sm font-semibold', change == null ? 'text-amber-200' : up ? 'text-emerald-400' : 'text-red-400')}>{change == null ? 'chart quote pending' : pct(change)}</span>
                  <span className="text-sm font-medium text-slate-200">{row.company || ticker}</span>
                </div>
                <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-neutral">
                  <span className="font-mono text-slate-200">{money(displayPrice)}</span>
                  <span>Vol {compact(displayVolume)}</span>
                  <span>{row.exchange || '--'}</span>
                  <span>{row.country || 'USA'}</span>
                  <span>{row.sector || 'Sector unavailable'}</span>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button onClick={() => setShow3d(open => !open)} className="rounded border border-sky-500/40 bg-sky-500/10 px-3 py-1.5 text-xs font-medium text-sky-100 hover:border-sky-300">
                  {show3d ? 'Hide 3D' : 'Show 3D'}
                </button>
                <Link to={`/charts?t=${encodeURIComponent(ticker)}`} className="rounded border border-border px-3 py-1.5 text-xs text-neutral hover:border-accent hover:text-white">
                  Chart
                </Link>
              </div>
            </div>
          </div>

          <div className="relative bg-bg" style={{ height: 340 }}>
            {!visible || chartLoading ? (
              <div className="flex h-full items-center justify-center text-xs text-neutral animate-pulse">Loading chart...</div>
            ) : chartError || chart?.error ? (
              <div className="flex h-full items-center justify-center text-xs text-red-300">Chart unavailable</div>
            ) : candles.length ? (
              <CandlestickChart
                candles={candles as any}
                bollinger={chart?.bollinger as any}
                densityOverlay={chartOverlays.density as any}
                sentimentOverlay={chartOverlays.sentiment as any}
                watcherOverlay={chartOverlays.watchers as any}
                chartStyle="candles"
                showBollinger
                showDensity
                showSentiment
                showWatchers
                showMarkers={false}
                minHeight={0}
              />
            ) : (
              <div className="flex h-full items-center justify-center text-xs text-neutral">No candle data</div>
            )}
          </div>
        </div>

        <div className="p-4">
          <div className="text-sm font-semibold text-white">{row.company || ticker}</div>
          <div className="mt-1 text-[11px] text-neutral">{[row.sector, row.industry].filter(Boolean).join(' - ') || 'Fundamentals from current screener row'}</div>
          <div className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px]">
            {fields.map(([label, value]) => (
              <div key={label} className="flex justify-between gap-2 border-b border-border/35 py-1">
                <span className="text-neutral">{label}</span>
                <span className="truncate font-mono text-slate-200">{value}</span>
              </div>
            ))}
          </div>
          <div className="mt-3 grid grid-cols-2 gap-2 text-center text-[11px] sm:grid-cols-4">
            <MetricBadge label="News" value={compact(matchedNewsCount)} />
            <MetricBadge label="Messages" value={compact(row.message_count)} />
            <MetricBadge label="Watchers (Δ)" value={watcherMetricValue(row)} />
            <MetricBadge label="Sentiment" value={displaySentiment != null ? displaySentiment.toFixed(2) : '--'} />
          </div>
        </div>
      </div>

      <section className="border-t border-border bg-[#1b2432]">
        <div className="flex items-center justify-between border-b border-border px-4 py-2">
          <h2 className="text-xs font-semibold uppercase tracking-wide text-slate-200">Latest News Sources</h2>
          <span className={clsx('text-[11px]', showingFallbackArticles ? 'text-amber-200' : 'text-neutral')}>
            {articles.length
              ? `${articles.length} ${showingFallbackArticles ? 'older ' : ''}ticker-specific`
              : 'No ticker-specific article rows'}
          </span>
        </div>
        <div className="divide-y divide-border">
          {showingFallbackArticles && (
            <div className="border-b border-border bg-amber-500/5 px-4 py-2 text-[11px] text-amber-100">
              No ticker-specific rows in {formatNewsWindow(articleDays)}; showing the latest ticker-specific rows from 30 days.
            </div>
          )}
          {articles.length ? articles.map((article: any, index: number) => (
            <a
              key={article.id || article.article_id || article.url || `${ticker}-${index}`}
              href={article.url || '#'}
              target="_blank"
              rel="noreferrer"
              className="grid gap-2 px-4 py-2 text-xs hover:bg-sky-500/5 md:grid-cols-[118px_180px_1fr_96px]"
            >
              <span className="font-mono text-neutral">{whenLabel(article.publish_date || article.detected_at || article.fetched_date) || '--'}</span>
              <span className="truncate text-sky-200">{article.source || 'Unknown source'}</span>
              <span className="min-w-0 truncate text-slate-100">{article.title || article.headline || 'Untitled article'}</span>
              <span className="text-right">
                <span className={clsx('inline-flex min-w-[72px] justify-center rounded border px-2 py-0.5 text-[11px] font-semibold', sentimentClass(article.sentiment || article.article_kind))}>
                  {sentimentLabel(article.sentiment || article.article_kind)}
                </span>
              </span>
            </a>
          )) : (
            <div className="px-4 py-6 text-center text-xs text-neutral">
              No ticker-specific news found for {ticker} in {formatNewsWindow(articleDays)}.
              {rejectedAmbiguousArticles > 0 && (
                <span className="ml-1 text-amber-200">{rejectedAmbiguousArticles} ambiguous source row{rejectedAmbiguousArticles === 1 ? '' : 's'} rejected.</span>
              )}
            </div>
          )}
        </div>
      </section>

      <section className="border-t border-border bg-bg/30">
        <div className="flex items-center justify-between px-4 py-2">
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide text-slate-200">3D Decision Journey</div>
            <div className="text-[11px] text-neutral">Single-ticker Decision Map path for {ticker}; loaded on demand so the Mirror stays fast.</div>
          </div>
          <button onClick={() => setShow3d(open => !open)} className="rounded border border-border px-3 py-1 text-xs text-neutral hover:border-accent hover:text-white">
            {show3d ? 'Collapse' : 'Load 3D'}
          </button>
        </div>
        {show3d && (
          <div className="border-t border-border p-3">
            <DecisionMapPanel
              key={`mirror-decision-map-${ticker}-${rollingWindowMinutes}`}
              focusTicker={ticker}
              single
              embedded
              rollingWindowMinutes={rollingWindowMinutes}
              miniPriceDensity={miniPriceDensity}
            />
          </div>
        )}
      </section>
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
