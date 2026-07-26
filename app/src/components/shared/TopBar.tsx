import useSWR, { useSWRConfig } from 'swr'
import { useState, useRef, useCallback, useEffect } from 'react'
import { createPortal } from 'react-dom'
import { NavLink, useLocation } from 'react-router-dom'
import { clsx } from 'clsx'
import { StatusBadge } from './StatusBadge'
import { useToast } from '@/components/shared/Toast'
import { SentimentModal } from '@/components/shared/SentimentModal'

const fetcher = (url: string) => fetch(url).then(r => r.json())

const FETCH_COOLDOWN_SECONDS = 60
const LAST_FETCH_KEY = 'flashfeed:lastFetchAt'

const NAV = [
  { href: '/overview', label: 'Overview' },
  { href: '/ai', label: 'AI' },
  { href: '/news', label: 'News' },
  { href: '/screener', label: 'Screener' },
  { href: '/decision-map', label: 'Decision Map' },
  { href: '/social', label: 'Social' },
  { href: '/charts', label: 'Charts' },
  { href: '/entry-screener', label: 'Entry Screener' },
  { href: '/exit-screener', label: 'Exit Screener' },
  { href: '/momentum', label: 'Momentum' },
  { href: '/correlation', label: 'Correlation' },
  { href: '/v11-screener', label: 'v11 Profile (test)' },
  { href: '/prediction-audit', label: 'Prediction Audit' },
  { href: '/system-health', label: 'System Health' },
  { href: '/settings', label: 'Settings' },
]
const PRIMARY_NAV = NAV.slice(0, 11)
const MORE_NAV = NAV.slice(11)
const ROUTE_PREFETCHERS: Record<string, () => Promise<unknown>> = {
  '/overview': () => import('@/pages/OverviewPage'),
  '/ai': () => import('@/pages/AIPage'),
  '/news': () => import('@/pages/NewsPage'),
  '/screener': () => import('@/pages/ScreenerPage'),
  '/decision-map': () => import('@/pages/DecisionMapPanel'),
  '/social': () => import('@/pages/SocialPage'),
  '/charts': () => import('@/pages/sentchart/ChartsPage'),
  '/entry-screener': () => import('@/pages/sentchart/EntryScreenerPage'),
  '/exit-screener': () => import('@/pages/sentchart/ExitScreenerPage'),
  '/momentum': () => import('@/pages/MomentumPage'),
  '/correlation': () => import('@/pages/CorrelationPage'),
  '/v11-screener': () => import('@/pages/sentchart/V11ScreenerPage'),
  '/prediction-audit': () => import('@/pages/PredictionAuditPage'),
  '/system-health': () => import('@/pages/SystemHealthPage'),
  '/settings': () => import('@/pages/SettingsPage'),
}
const prefetchedRoutes = new Set<string>()

function prefetchRoute(href: string) {
  if (prefetchedRoutes.has(href)) return
  const prefetcher = ROUTE_PREFETCHERS[href]
  if (!prefetcher) return
  prefetchedRoutes.add(href)
  prefetcher().catch(() => prefetchedRoutes.delete(href))
}

function compactCount(value: unknown): string {
  const n = Number(value || 0)
  if (!Number.isFinite(n)) return '0'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 10_000) return `${Math.round(n / 1_000)}k`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return String(n)
}

export function TopBar() {
  const { pathname } = useLocation()
  const { toast } = useToast()
  const { mutate } = useSWRConfig()
  const { data: status, mutate: mutateStatus } = useSWR('/api/status', fetcher, { refreshInterval: 60_000 })
  const { data: stats } = useSWR('/api/stats?days=3', fetcher, { refreshInterval: 60_000 })
  const { data: marketStatus } = useSWR('/api/market/status', fetcher, { refreshInterval: 60_000 })
  const { data: autoRefreshStatus } = useSWR('/api/auto-refresh/status', fetcher, { refreshInterval: 15_000 })
  // Hard-disk database status (RAM = Redis; this is the on-disk SQLite companion).
  const { data: diskStats, mutate: mutateDisk } = useSWR('/api/disk/stats', fetcher, { refreshInterval: 60_000 })
  const [savingDisk, setSavingDisk] = useState(false)
  const [savingDaily, setSavingDaily] = useState(false)
  const [fetchElapsed, setFetchElapsed] = useState(0)
  const fetchTimerRef = useRef<number | null>(null)

  // Presence heartbeat: ping while the tab is visible so the server's auto-grabber
  // knows we're here (and archives to disk only while we're away).
  useEffect(() => {
    const ping = () => {
      if (document.visibilityState === 'visible') {
        fetch('/api/presence/ping', { method: 'POST' }).catch(() => {})
      }
    }
    ping()
    const id = window.setInterval(ping, 60_000)
    document.addEventListener('visibilitychange', ping)
    return () => { window.clearInterval(id); document.removeEventListener('visibilitychange', ping) }
  }, [])

  // On exit / tab-hide, save the last 3 days of news to the hard disk via beacon
  // (reliable during unload). Mirrors the manual "Save 3d → Disk" button.
  useEffect(() => {
    const saveOnExit = () => {
      try { navigator.sendBeacon('/api/disk/save-news?days=3') } catch { /* best effort */ }
    }
    const onHide = () => { if (document.visibilityState === 'hidden') saveOnExit() }
    window.addEventListener('beforeunload', saveOnExit)
    window.addEventListener('pagehide', saveOnExit)
    document.addEventListener('visibilitychange', onHide)
    return () => {
      window.removeEventListener('beforeunload', saveOnExit)
      window.removeEventListener('pagehide', saveOnExit)
      document.removeEventListener('visibilitychange', onHide)
    }
  }, [])

  const saveToDisk = async () => {
    if (savingDisk) return
    setSavingDisk(true)
    try {
      const res = await fetch('/api/disk/save-news?days=3', { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        toast(`Saved ${data.saved} news items to hard disk`, `Last 3 days · auto-deletes after ${data.retention_days ?? 3} days`, 'success')
        mutateDisk()
      } else {
        toast('Disk save failed', data.error || 'Hard-disk database unavailable', 'error')
      }
    } catch {
      toast('Disk save failed', 'Could not reach API', 'error')
    } finally {
      setSavingDisk(false)
    }
  }

  const saveDailyArchive = async () => {
    if (savingDaily) return
    setSavingDaily(true)
    try {
      const res = await fetch('/api/disk/save-daily', { method: 'POST' })
      const data = await res.json()
      if (data.ok) {
        toast(`Archived ${data.saved} articles for ${data.date}`, `Rolling ${data.retention_days ?? 31}-day daily archive`, 'success')
        mutateDisk()
      } else {
        toast('Daily archive failed', data.error || 'Hard-disk database unavailable', 'error')
      }
    } catch {
      toast('Daily archive failed', 'Could not reach API', 'error')
    } finally {
      setSavingDaily(false)
    }
  }

  const [fetching, setFetching] = useState(false)
  const [fetchResult, setFetchResult] = useState<{ new_articles?: number; updated_articles?: number; unchanged_articles?: number; total_articles?: number; ms?: number } | null>(null)
  const [cooldownRemaining, setCooldownRemaining] = useState(0)
  const [watching, setWatching] = useState(false)
  const [watchInterval, setWatchInterval] = useState('60')
  const [fetchMode, setFetchMode] = useState<'fast' | 'full'>('fast')
  const [watchLines, setWatchLines] = useState<Array<{ text: string; type: string; ts: number }>>([])
  const [showSentiment, setShowSentiment] = useState(false)
  const [showStorage, setShowStorage] = useState(false)
  const [showControls, setShowControls] = useState(false)
  const [showMoreNav, setShowMoreNav] = useState(false)
  const moreNavButtonRef = useRef<HTMLButtonElement | null>(null)
  const [moreNavPosition, setMoreNavPosition] = useState<{ top: number; left: number } | null>(null)
  const [lastAutoResult, setLastAutoResult] = useState<{ new?: number; updated?: number; ms?: number; at: number } | null>(null)
  const [autoStatus, setAutoStatus] = useState<{ text: string; at: number; nextAt?: number | null; running?: boolean; skipped?: boolean; error?: boolean } | null>(null)
  const [autoQueueStartedAt, setAutoQueueStartedAt] = useState<number | null>(null)
  const [autoProgress, setAutoProgress] = useState(0)
  const [serverProgressNowMs, setServerProgressNowMs] = useState(() => Date.now())
  const watchRef = useRef<EventSource | null>(null)

  useEffect(() => {
    const updateCooldown = () => {
      const lastFetchAt = Number(localStorage.getItem(LAST_FETCH_KEY) || 0)
      const elapsed = Math.floor((Date.now() - lastFetchAt) / 1000)
      setCooldownRemaining(Math.max(0, FETCH_COOLDOWN_SECONDS - elapsed))
    }

    updateCooldown()
    const timer = window.setInterval(updateCooldown, 1000)

    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    if (!watching || !autoQueueStartedAt) {
      setAutoProgress(0)
      return
    }
    const intervalMs = Math.max(60, Number(watchInterval || 60)) * 1000
    const update = () => {
      const elapsed = Date.now() - autoQueueStartedAt
      setAutoProgress(Math.max(0, Math.min(100, (elapsed / intervalMs) * 100)))
    }
    update()
    const timer = window.setInterval(update, 1000)
    return () => window.clearInterval(timer)
  }, [watching, autoQueueStartedAt, watchInterval])

  useEffect(() => {
    if (!autoRefreshStatus?.onsite_fetch?.enabled) return
    const timer = window.setInterval(() => setServerProgressNowMs(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [autoRefreshStatus?.onsite_fetch?.enabled])

  useEffect(() => {
    if (!showMoreNav) return
    const updatePosition = () => {
      const rect = moreNavButtonRef.current?.getBoundingClientRect()
      if (!rect) return
      const menuWidth = 192
      const left = Math.max(8, Math.min(rect.left, window.innerWidth - menuWidth - 8))
      const top = Math.max(8, rect.bottom + 8)
      setMoreNavPosition({ top, left })
    }
    const closeOnOutsideClick = (event: MouseEvent) => {
      const target = event.target as Node | null
      if (target && moreNavButtonRef.current?.contains(target)) return
      const menu = document.getElementById('flashfeed-more-nav-menu')
      if (target && menu?.contains(target)) return
      setShowMoreNav(false)
    }
    updatePosition()
    window.addEventListener('resize', updatePosition)
    window.addEventListener('scroll', updatePosition, true)
    document.addEventListener('mousedown', closeOnOutsideClick)
    return () => {
      window.removeEventListener('resize', updatePosition)
      window.removeEventListener('scroll', updatePosition, true)
      document.removeEventListener('mousedown', closeOnOutsideClick)
    }
  }, [showMoreNav])

  const revalidateDashboardData = useCallback(() => {
    mutate(
      key => typeof key === 'string' && (
        key.startsWith('/api/articles') ||
        key.startsWith('/api/stats') ||
        key.startsWith('/api/status') ||
        key.startsWith('/api/screener') ||
        key.startsWith('/api/decision-map') ||
        key.startsWith('/api/momentum') ||
        key.startsWith('/api/prices') ||
        key.startsWith('/api/prediction') ||
        key.startsWith('/api/sentiment') ||
        key.startsWith('/api/social/rolling') ||
        key.startsWith('/api/correlation')
      ),
      undefined,
      { revalidate: true }
    )
  }, [mutate])

  const doFetch = async () => {
    if (fetching || cooldownRemaining > 0) {
      return
    }

    localStorage.setItem(LAST_FETCH_KEY, String(Date.now()))
    setCooldownRemaining(FETCH_COOLDOWN_SECONDS)

    setFetching(true)
    setFetchResult(null)
    setFetchElapsed(0)
    const t0 = Date.now()
    // Live elapsed counter so the button stays responsive during a long refresh.
    if (fetchTimerRef.current) window.clearInterval(fetchTimerRef.current)
    fetchTimerRef.current = window.setInterval(() => setFetchElapsed(Math.floor((Date.now() - t0) / 1000)), 1000)
    // Safety timeout — a hung request can never lock the button permanently.
    const ctrl = new AbortController()
    const timeoutId = window.setTimeout(() => ctrl.abort(), 180_000)
    try {
      const res = await fetch(`/api/fetch?mode=${fetchMode}`, { method: 'POST', signal: ctrl.signal })
      const data = await res.json()
      const latency = Date.now() - t0
      // Backend de-dups overlapping cycles (Run Now + Auto + auto-grab) — surface that.
      if (data.skipped || data.already_running) {
        toast('Refresh already running', 'A fetch cycle is already in progress — skipped the duplicate.', 'info')
        return
      }
      setFetchResult(data)
      const socialNew = data.social_new ?? 0
      const socialUpdated = data.social_updated ?? 0
      const trackedMarketCount = data.tracked_market_ticker_count ? `; ${data.tracked_market_ticker_count} market tickers` : ''
      const refreshDetail = Array.isArray(data.errors) && data.errors.length
        ? data.errors.slice(0, 2).join(' | ')
        : data.error || undefined
      toast(
        data.ok === false
          ? 'Refresh needs attention'
          : `${data.quotes_updated ?? 0} quotes${trackedMarketCount}; +${data.new_articles ?? 0} new articles${data.updated_articles !== undefined ? `, ${data.updated_articles} refreshed` : ''}; +${socialNew} social${socialUpdated ? `, ${socialUpdated} refreshed` : ''}`,
        data.ok === false ? refreshDetail : undefined,
        data.ok === false
          ? 'error'
          : ((data.new_articles ?? 0) + (data.updated_articles ?? data.refreshed_articles ?? 0) + socialNew + socialUpdated) > 0 ? 'success' : 'info',
        latency
      )
      mutateStatus()
      revalidateDashboardData()
      mutateDisk()   // Run Now also mirrors news to the hard disk — refresh the disk badge
      setTimeout(() => setFetchResult(null), 8000)
    } catch (err) {
      if ((err as any)?.name === 'AbortError') {
        toast('Fetch timed out', 'The refresh took too long and was cancelled. Try again or use Fast mode.', 'error')
      } else {
        toast('Fetch failed', 'Could not reach API', 'error')
      }
    } finally {
      window.clearTimeout(timeoutId)
      if (fetchTimerRef.current) { window.clearInterval(fetchTimerRef.current); fetchTimerRef.current = null }
      setFetching(false)
    }
  }

  const toggleWatch = useCallback(() => {
    if (watching) {
      watchRef.current?.close()
      watchRef.current = null
      setWatching(false)
      setAutoQueueStartedAt(null)
      setAutoProgress(0)
      setAutoStatus({ text: 'Auto-watch stopped.', at: Date.now() })
    } else {
      setWatchLines([])
      setAutoQueueStartedAt(Date.now())
      setAutoStatus({ text: 'Connecting auto-watch...', at: Date.now(), running: true })
      const es = new EventSource(`/api/watch?interval=${watchInterval}&mode=${fetchMode}`)

      es.addEventListener('start', (e) => {
        const d = JSON.parse(e.data)
        setAutoStatus({ text: d.message || 'Auto-watch started.', at: Date.now(), nextAt: d.next_run_at ?? null, running: false })
        setAutoQueueStartedAt(Date.now())
        setWatchLines(l => [...l, { text: d.message, type: 'info', ts: Date.now() }])
        toast('Auto-watch started', `Every ${d.interval ?? watchInterval}s in ${d.mode ?? fetchMode} mode`, 'info')
      })
      es.addEventListener('heartbeat', (e) => {
        const d = JSON.parse(e.data)
        setAutoStatus({
          text: d.message || (d.running ? 'Auto-watch refresh started.' : 'Auto-watch waiting.'),
          at: Date.now(),
          nextAt: d.next_run_at ?? null,
          running: Boolean(d.running),
          skipped: Boolean(d.skipped),
        })
        setAutoQueueStartedAt(Date.now())
        setWatchLines(l => [...l.slice(-200), { text: d.message || 'Auto-watch heartbeat.', type: d.skipped ? 'info' : 'new', ts: Date.now() }])
      })
      es.addEventListener('line', (e) => {
        const d = JSON.parse(e.data)
        const isNew = d.new !== undefined && d.new > 0
        setWatchLines(l => [...l.slice(-200), { text: d.text, type: isNew ? 'new' : '', ts: Date.now() }])
        // Show toast notification with cycle results
        if (d.new !== undefined) {
          setLastAutoResult({ new: d.new, updated: d.updated, ms: d.ms, at: Date.now() })
          setAutoStatus({ text: d.text || 'Auto-watch refresh complete.', at: Date.now(), nextAt: d.next_run_at ?? null, running: false })
          setAutoQueueStartedAt(Date.now())
          toast(
            `${d.quotes_updated ?? 0} quotes${d.tracked_market_ticker_count ? `; ${d.tracked_market_ticker_count} market tickers` : ''}; +${d.new} new articles${d.updated > 0 ? `, ${d.updated} refreshed` : ''}; +${d.social_new ?? 0} social${d.social_updated > 0 ? `, ${d.social_updated} refreshed` : ''}`,
            undefined,
            (d.new + d.updated + (d.social_new ?? 0) + (d.social_updated ?? 0)) > 0 ? 'success' : 'info',
            d.ms
          )
          mutateStatus()
          revalidateDashboardData()
          mutateDisk()
        }
      })
      es.addEventListener('error', (e) => {
        try {
          const d = JSON.parse((e as any).data)
          setAutoStatus({ text: d.message || 'Auto-watch error.', at: Date.now(), error: true })
          setWatchLines(l => [...l, { text: d.message, type: 'err', ts: Date.now() }])
        } catch {}
      })
      es.addEventListener('end', (e) => {
        const d = JSON.parse(e.data)
        setAutoStatus({ text: d.message || 'Auto-watch ended.', at: Date.now() })
        setWatchLines(l => [...l, { text: d.message, type: 'info', ts: Date.now() }])
        setWatching(false)
        setAutoQueueStartedAt(null)
        setAutoProgress(0)
      })
      es.onerror = () => {
        setAutoStatus({ text: 'Auto-watch connection lost.', at: Date.now(), error: true })
        setWatchLines(l => [...l, { text: 'Connection lost.', type: 'err', ts: Date.now() }])
        setWatching(false)
        setAutoQueueStartedAt(null)
        setAutoProgress(0)
        watchRef.current = null
      }

      watchRef.current = es
      setWatching(true)
    }
  }, [watching, watchInterval, fetchMode, mutateStatus, revalidateDashboardData, mutateDisk])

  const autoLabel = watching
    ? autoStatus?.running
      ? 'Auto refreshing...'
      : autoStatus?.skipped
        ? 'Auto waiting'
        : lastAutoResult
          ? `Auto +${lastAutoResult.new ?? 0} new${lastAutoResult.updated ? `, ${lastAutoResult.updated} upd` : ''}`
          : 'Auto starting...'
    : null
  const serverAuto = autoRefreshStatus?.onsite_fetch
  const serverAutoOn = Boolean(serverAuto?.enabled)
  const serverAutoRunning = Boolean(serverAuto?.running || autoRefreshStatus?.away_fetch?.running)
  const backendRefreshInFlight = Boolean(autoRefreshStatus?.refresh_cycle_in_flight)
  const serverAutoIntervalMs = Math.max(
    60_000,
    Number(serverAuto?.interval_minutes || autoRefreshStatus?.away_fetch?.interval_minutes || 1) * 60_000
  )
  const serverAutoLastRunMs = Number(serverAuto?.last_run_epoch_ms || 0)
  const serverAutoNextRunMs = serverAuto?.next_due_at ? Date.parse(serverAuto.next_due_at) : 0
  const serverAutoQueueStartMs = serverAutoLastRunMs || (serverAutoNextRunMs ? serverAutoNextRunMs - serverAutoIntervalMs : 0)
  const serverAutoProgress = serverAutoQueueStartMs
    ? Math.max(0, Math.min(100, ((serverProgressNowMs - serverAutoQueueStartMs) / serverAutoIntervalMs) * 100))
    : backendRefreshInFlight ? 100 : 0
  const showAutoProgressLine = Boolean(
    watching ||
    serverAutoRunning ||
    backendRefreshInFlight ||
    (serverAutoOn && serverAutoQueueStartMs)
  )
  const autoProgressWidth = watching
    ? (autoQueueStartedAt ? autoProgress : 100)
    : (backendRefreshInFlight || serverAutoRunning)
      ? 100
      : serverAutoProgress
  const autoProgressTitle = watching
    ? (autoQueueStartedAt ? `Auto refresh queue: ${Math.round(autoProgress)}% until next cycle` : 'Auto refresh is running the first cycle')
    : (backendRefreshInFlight || serverAutoRunning)
      ? 'Server auto-refresh cycle is running'
      : `Server auto-refresh queue: ${Math.round(serverAutoProgress)}% until next cycle`
  const serverAutoLabel = serverAutoRunning
    ? 'Server auto refreshing'
    : serverAutoOn
      ? serverAuto?.dashboard_present
        ? `Server auto ${serverAuto.interval_minutes ?? 20}m`
        : 'Server auto standby'
      : 'Server auto off'
  const serverAutoTitle = serverAutoOn
    ? `Backend auto-refresh is ${serverAutoRunning ? 'currently running' : 'enabled'} · refresh lock ${backendRefreshInFlight ? 'active' : 'clear'} · dashboard ${serverAuto.dashboard_present ? 'present' : 'absent'} · next due ${serverAuto.next_due_at || 'waiting'} · market ${autoRefreshStatus?.market?.label || 'unknown'}`
    : 'Backend auto-refresh is disabled'
  const moreNavMenu = showMoreNav && typeof document !== 'undefined'
    ? createPortal(
      <div
        id="flashfeed-more-nav-menu"
        className="fixed z-[1000] w-48 overflow-y-auto rounded-lg border border-border bg-surface p-1 shadow-2xl shadow-black/40"
        style={{
          top: moreNavPosition?.top ?? 56,
          left: moreNavPosition?.left ?? 0,
          maxHeight: `calc(100vh - ${(moreNavPosition?.top ?? 56) + 8}px)`,
        }}
      >
        {MORE_NAV.map(({ href, label }) => (
          <NavLink
            key={href}
            to={href}
            onMouseEnter={() => prefetchRoute(href)}
            onFocus={() => prefetchRoute(href)}
            onClick={() => setShowMoreNav(false)}
            className={({ isActive }) => clsx(
              'block rounded px-3 py-2 text-xs transition-colors',
              isActive ? 'bg-accent/15 text-white' : 'text-neutral hover:bg-bg hover:text-white'
            )}
          >
            {label}
          </NavLink>
        ))}
      </div>,
      document.body
    )
    : null

  return (
    <>
      <header className="relative bg-surface border-b border-border flex-shrink-0">
        {showAutoProgressLine && (
          <div
            data-testid="auto-progress-line"
            className="absolute left-0 top-0 z-40 h-1 w-full overflow-hidden bg-bg"
            title={autoProgressTitle}
          >
            <div
              className="relative h-full overflow-hidden rounded-r-full bg-gradient-to-r from-sky-400 via-emerald-400 to-yellow-300 shadow-[0_0_12px_rgba(56,189,248,0.65)] transition-[width] duration-200 ease-linear"
              style={{ width: `${autoProgressWidth}%` }}
            >
              <div className="absolute inset-y-0 right-0 w-16 animate-[auto-progress-glint_1.15s_linear_infinite] bg-gradient-to-r from-transparent via-white/70 to-transparent" />
            </div>
          </div>
        )}
        <div className="flex min-h-14 items-center gap-2 px-3 py-2 md:px-4">
          <NavLink
            to="/overview"
            onMouseEnter={() => prefetchRoute('/overview')}
            onFocus={() => prefetchRoute('/overview')}
            className="flex-shrink-0"
          >
            <div className="text-accent font-bold text-lg tracking-tight font-mono leading-none">FlashFeed</div>
            <div className="text-neutral text-[10px] mt-1 uppercase tracking-wide">Financial Intelligence</div>
          </NavLink>

          <nav className="hidden min-w-0 flex-1 items-center gap-1 overflow-x-auto lg:flex">
            {PRIMARY_NAV.map(({ href, label }) => {
              const active = pathname === href || pathname.startsWith(`${href}/`)
              return (
                <NavLink
                  key={href}
                  to={href}
                  onMouseEnter={() => prefetchRoute(href)}
                  onFocus={() => prefetchRoute(href)}
                  className={clsx(
                    'flex-none whitespace-nowrap px-2 py-2 text-xs rounded-md border transition-colors xl:px-3',
                    active
                      ? 'bg-accent/15 border-accent/50 text-white'
                      : 'border-transparent text-neutral hover:text-white hover:bg-bg/60'
                  )}
                >
                  {label}
                </NavLink>
              )
            })}
            <div className="relative flex-none">
              <button
                ref={moreNavButtonRef}
                onMouseEnter={() => MORE_NAV.forEach(({ href }) => prefetchRoute(href))}
                onFocus={() => MORE_NAV.forEach(({ href }) => prefetchRoute(href))}
                onClick={() => setShowMoreNav(v => !v)}
                className={clsx(
                  'whitespace-nowrap px-2 py-2 text-xs rounded-md border transition-colors xl:px-3',
                  MORE_NAV.some(({ href }) => pathname === href || pathname.startsWith(`${href}/`))
                    ? 'bg-accent/15 border-accent/50 text-white'
                    : 'border-transparent text-neutral hover:text-white hover:bg-bg/60'
                )}
              >
                More
              </button>
            </div>
          </nav>

          <div className="ml-auto flex flex-none items-center justify-end gap-2">
            {fetchResult && (
              <span className="hidden max-w-[12rem] truncate text-xs text-emerald-400 animate-in lg:inline">
                +{fetchResult.new_articles ?? 0} new{fetchResult.updated_articles !== undefined ? `, ${fetchResult.updated_articles} refreshed` : fetchResult.refreshed_articles !== undefined ? `, ${fetchResult.refreshed_articles} refreshed` : ''} ({((fetchResult.ms ?? 0) / 1000).toFixed(1)}s)
              </span>
            )}
            <button
              onClick={doFetch}
              disabled={fetching || cooldownRemaining > 0}
              title={cooldownRemaining > 0 ? `Fetch available in ${cooldownRemaining}s` : `${fetchMode === 'fast' ? 'Fast trader refresh' : 'Full source refresh'}`}
              className="min-w-[6.75rem] px-3 py-1.5 bg-accent text-white text-xs font-medium rounded hover:bg-sky-400 disabled:opacity-50 transition-colors whitespace-nowrap"
            >
              {fetching ? `Fetching ${fetchElapsed}s...` : cooldownRemaining > 0 ? `Fetch ${cooldownRemaining}s` : 'Run Now'}
            </button>

            <div className="relative">
              <button
                onClick={() => setShowControls(v => !v)}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded border border-border text-neutral hover:text-white hover:border-accent transition-colors"
                title={`Refresh, auto-watch, sentiment, and storage controls. ${serverAutoTitle}`}
              >
                Controls
                <span className={watching || serverAutoOn ? 'text-emerald-300' : 'text-neutral'}>{watching ? 'Watch' : serverAutoOn ? 'Auto' : fetchMode}</span>
              </button>

              {showControls && (
                <div className="absolute right-0 top-full z-50 mt-2 w-80 rounded-lg border border-border bg-surface shadow-xl">
                  <div className="space-y-3 p-3">
                    <div className="grid grid-cols-2 gap-2">
                      <label className="text-[10px] uppercase text-neutral">
                        Mode
                        <select
                          value={fetchMode}
                          onChange={e => setFetchMode(e.target.value as 'fast' | 'full')}
                          disabled={fetching || watching}
                          className="mt-1 w-full bg-bg border border-border text-xs text-neutral rounded px-2 py-1.5 focus:outline-none disabled:opacity-50"
                          title="Fast refresh is optimized for top movers. Full refresh runs every broader source sweep."
                        >
                          <option value="fast">Fast</option>
                          <option value="full">Full</option>
                        </select>
                      </label>
                      <label className="text-[10px] uppercase text-neutral">
                        Auto interval
                        <select
                          value={watchInterval}
                          onChange={e => setWatchInterval(e.target.value)}
                          disabled={watching}
                          className="mt-1 w-full bg-bg border border-border text-xs text-neutral rounded px-2 py-1.5 focus:outline-none disabled:opacity-50"
                        >
                          <option value="60">1m</option>
                          <option value="120">2m</option>
                          <option value="300">5m</option>
                        </select>
                      </label>
                    </div>

                    <div className="rounded-lg border border-border bg-bg/40 p-2 text-xs">
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-semibold text-white">Server auto-refresh</span>
                        <span className={clsx('font-mono text-[11px]', serverAutoRunning ? 'text-sky-300' : serverAutoOn ? 'text-emerald-300' : 'text-neutral')}>
                          {serverAutoRunning ? 'running' : serverAutoOn ? 'enabled' : 'off'}
                        </span>
                      </div>
                      <div className="mt-1 text-[11px] text-neutral">
                        {serverAutoOn
                          ? `Runs every ${serverAuto.interval_minutes ?? 20}m while the dashboard is present; checks every ${serverAuto.check_seconds ?? 60}s.`
                          : 'Backend auto-refresh is disabled.'}
                      </div>
                      {serverAutoOn && (
                        <div className="mt-1 text-[11px] text-slate-400">
                          Next due: {serverAuto.next_due_at ? new Date(serverAuto.next_due_at).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) : 'waiting'} · Dashboard: {serverAuto.dashboard_present ? 'present' : 'absent'}
                        </div>
                      )}
                    </div>

                    <div className="grid grid-cols-2 gap-2">
                      <button
                        onClick={toggleWatch}
                        className={`px-3 py-2 text-xs font-medium rounded border transition-colors ${
                          watching
                            ? 'bg-emerald-500/20 border-emerald-500/50 text-emerald-300'
                            : 'bg-bg border-border text-neutral hover:text-white hover:border-accent'
                        }`}
                      >
                        {watching ? 'Stop auto-watch' : 'Start auto-watch'}
                      </button>
                      <button
                        onClick={() => { setShowSentiment(true); setShowControls(false) }}
                        className="px-3 py-2 text-xs font-medium rounded border border-border bg-bg text-neutral hover:text-white hover:border-accent transition-colors"
                      >
                        Sentiment settings
                      </button>
                    </div>

                    <div className="rounded-lg border border-border bg-bg/40 p-2">
                      <div className="mb-2 flex items-center justify-between">
                        <span className="text-xs font-semibold text-white">Storage</span>
                        {diskStats?.available && <span className="font-mono text-xs text-emerald-300">{compactCount(diskStats.total)}</span>}
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          onClick={() => { saveDailyArchive(); setShowControls(false) }}
                          disabled={savingDaily}
                          className="px-3 py-2 text-xs font-medium rounded bg-emerald-500/15 border border-emerald-500/40 text-emerald-200 hover:bg-emerald-500/25 disabled:opacity-50 transition-colors"
                        >
                          {savingDaily ? 'Archiving...' : 'Archive 24h now'}
                        </button>
                        <button
                          onClick={() => { saveToDisk(); setShowControls(false); setShowStorage(false) }}
                          disabled={savingDisk}
                          className="px-3 py-2 text-xs font-medium rounded bg-amber-500/15 border border-amber-500/40 text-amber-200 hover:bg-amber-500/25 disabled:opacity-50 transition-colors"
                        >
                          {savingDisk ? 'Saving...' : 'Save 3d snapshot'}
                        </button>
                      </div>
                      <div className="mt-2 rounded border border-emerald-500/20 bg-emerald-500/5 px-2 py-1.5">
                        <div className="flex items-center justify-between gap-2">
                          <span className="text-[10px] uppercase text-neutral">Daily archive</span>
                          <span className="font-mono text-xs text-emerald-300">
                            {diskStats?.daily_archive?.days ?? 0}/{diskStats?.daily_archive?.retention_days ?? diskStats?.daily_archive?.max_days ?? 31} days
                          </span>
                        </div>
                        <div className="mt-0.5 truncate text-[10px] text-neutral">
                          {diskStats?.daily_archive?.newest_day
                            ? `Newest ${diskStats.daily_archive.newest_day} · ${compactCount(diskStats.daily_archive.total_articles)} articles`
                            : 'Saves the last 24h once per day'}
                        </div>
                      </div>
                      <div className="mt-2 grid grid-cols-3 gap-2 text-center">
                        <StorageMetric label="Manual" value={diskStats?.by_bucket?.manual ?? 0} />
                        <StorageMetric label="Auto" value={diskStats?.by_bucket?.auto ?? 0} />
                        <StorageMetric label="Fetch" value={diskStats?.by_bucket?.fetch ?? 0} />
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="hidden min-w-0 items-center justify-end gap-2 sm:flex">
            {(status || stats) && <StatusBadge ok={status?.ok !== false} label={`${compactCount(stats?.total ?? status?.database?.recent_articles ?? status?.database?.articles)} 3d cache`} />}
            {marketStatus && <StatusBadge ok={marketStatus.open} label={marketStatus.open ? 'Open' : 'Closed'} />}
          </div>
        </div>

        <nav className="lg:hidden flex items-center gap-1 overflow-x-auto px-3 pb-2 md:px-4">
          {NAV.map(({ href, label }) => {
            const active = pathname === href || pathname.startsWith(`${href}/`)
            return (
              <NavLink
                key={href}
                to={href}
                onMouseEnter={() => prefetchRoute(href)}
                onFocus={() => prefetchRoute(href)}
                className={clsx(
                  'flex-shrink-0 px-3 py-1.5 text-xs rounded-md border transition-colors',
                  active
                    ? 'bg-accent/15 border-accent/50 text-white'
                    : 'border-border text-neutral hover:text-white'
                )}
              >
                {label}
              </NavLink>
            )
          })}
        </nav>
      </header>
      {moreNavMenu}
      <SentimentModal open={showSentiment} onClose={() => setShowSentiment(false)} />
    </>
  )
}

function StorageMetric({ label, value }: { label: string; value: unknown }) {
  return (
    <div className="rounded border border-border bg-bg px-2 py-1.5">
      <div className="font-mono text-xs text-white">{compactCount(value)}</div>
      <div className="text-[9px] uppercase text-neutral">{label}</div>
    </div>
  )
}
