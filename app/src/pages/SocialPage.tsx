import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { useTickerDatalistOptions } from '@/lib/useTickerUniverse'

type SocialPost = {
  platform?: string
  source?: string
  collector?: string
  ticker?: string
  symbol?: string
  ticker_candidates?: string[]
  tickers?: string[]
  title?: string
  text?: string
  content?: string
  url?: string
  author?: string
  sentiment?: string
  sentiment_score?: number
  source_sentiment?: string
  source_sentiment_score?: number
  sentiment_validation?: {
    method?: string
    label?: string
    score?: number
    confidence?: number
    agreement?: string
    text_label?: string
    text_score?: number
    source_label?: string
    source_score?: number
    signals?: string[]
  }
  sentiment_confidence?: number
  ml_confidence?: number
  finance_keywords?: string[]
  keywords?: string[]
  gossip_keywords?: string[]
  gossip_score?: number
  fetched_at?: number
  detected_at?: number
  created_at?: number
  timestamp?: number
}

type PlatformStatus = {
  platform: string
  total: number
  ticker_matched: number
  latest_sec?: number
  status: string
}

const tabs = [
  { id: 'all', label: 'All' },
  { id: 'reddit', label: 'Reddit' },
  { id: 'bluesky', label: 'Bluesky' },
  { id: 'grok', label: 'Grok' },
  { id: 'stocktwits', label: 'StockTwits' },
]

function GrokSocialAnalysis() {
  const [input, setInput] = useState('AAPL')
  const tickerOptions = useTickerDatalistOptions(input)
  const [ticker, setTicker] = useState('')
  const [analysis, setAnalysis] = useState('')
  const [model, setModel] = useState('')
  const [engine, setEngine] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    let cancelled = false
    fetch('/api/grok/status')
      .then(res => res.ok ? res.json() : null)
      .then(data => {
        if (cancelled || !data) return
        setEngine(data.engine || '')
        setModel(data.model || '')
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [])

  async function analyze(sym = input) {
    const cleanTicker = sym.trim().toUpperCase().replace(/[^A-Z0-9.-]/g, '')
    if (!cleanTicker) return
    setTicker(cleanTicker)
    setLoading(true)
    setError('')
    setAnalysis('')

    try {
      const res = await fetch('/api/grok/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ticker: cleanTicker,
          context: 'social sentiment',
          prompt: `Analyze the current social-media sentiment for $${cleanTicker}. Focus on what traders are saying, Reddit/StockTwits/Bluesky confirmation, bull versus bear tone, notable rumors, and whether attention is actionable or noisy.`,
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok || data.ok === false) throw new Error(data.error || `HTTP ${res.status}`)
      setAnalysis(data.analysis || 'No analysis returned.')
      setModel(data.model || model)
      setEngine(data.engine || engine)
    } catch (err: any) {
      setError(err?.message || 'Grok analysis failed.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="border border-slate-700 bg-slate-800/60 rounded-xl p-6">
      <div className="flex flex-wrap items-center gap-3">
        <div>
          <h2 className="text-xl font-semibold">Grok Social Analysis</h2>
          <p className="text-sm text-neutral mt-1">
            Reads the stored social/news context for one ticker. Uses Grok when configured, otherwise a local fallback.
          </p>
        </div>
        <div className="ml-auto flex items-center gap-2">
          <input
            value={input}
            list="social-analysis-ticker-universe"
            onChange={e => setInput(e.target.value.toUpperCase())}
            onKeyDown={e => { if (e.key === 'Enter') analyze() }}
            placeholder="Ticker"
            className="w-28 bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm uppercase font-mono"
          />
          <datalist id="social-analysis-ticker-universe">
            {tickerOptions.map(row => (
              <option key={row.ticker} value={row.ticker}>{row.company || row.exchange || row.ticker}</option>
            ))}
          </datalist>
          <button
            type="button"
            onClick={() => analyze()}
            disabled={loading}
            className="bg-sky-600 hover:bg-sky-500 disabled:opacity-50 rounded-md px-4 py-2 text-sm font-medium"
          >
            {loading ? 'Analyzing...' : 'Analyze'}
          </button>
        </div>
      </div>

      {(engine || model) && (
        <div className="mt-4 flex flex-wrap gap-2 text-xs text-neutral">
          {engine && <span className="rounded border border-slate-600 px-2 py-1">Engine: {engine}</span>}
          {model && <span className="rounded border border-slate-600 px-2 py-1">Model: {model}</span>}
        </div>
      )}

      {error && (
        <div className="mt-4 rounded-lg border border-red-500/40 bg-red-950/30 p-3 text-sm text-red-200">
          {error}
        </div>
      )}

      <div className="mt-4 rounded-lg border border-slate-700 bg-slate-900/60 p-4 min-h-28">
        {ticker && (
          <div className="mb-2 flex items-center gap-2">
            <span className="text-lg font-bold text-sky-300">${ticker}</span>
            {loading && <span className="text-xs text-neutral">reading social context...</span>}
          </div>
        )}
        {loading ? (
          <div className="text-sm text-neutral animate-pulse">Analyzing stored social/news context...</div>
        ) : analysis ? (
          <p className="whitespace-pre-line text-sm leading-relaxed text-slate-100">{analysis}</p>
        ) : (
          <p className="text-sm text-neutral">Enter a ticker to get a social sentiment read.</p>
        )}
      </div>
    </div>
  )
}

function ts(post: SocialPost) {
  return post.fetched_at || post.timestamp || post.detected_at || post.created_at || 0
}

function timeAgo(epoch?: number) {
  if (!epoch) return ''
  const diff = Math.max(0, Math.floor(Date.now() / 1000) - epoch)
  if (diff < 60) return `${diff}s ago`
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`
  return `${Math.floor(diff / 3600)}h ago`
}

function displayText(post: SocialPost) {
  return post.text || post.title || post.content || ''
}

function sourceLabel(post: SocialPost) {
  return post.platform || post.source || 'Social'
}

function primaryTicker(post: SocialPost) {
  return post.ticker || post.symbol || post.ticker_candidates?.[0] || post.tickers?.[0] || ''
}

const TRENDING_STOP_WORDS = new Set([
  'about', 'after', 'again', 'all', 'also', 'and', 'any', 'are', 'because', 'been',
  'before', 'being', 'but', 'can', 'could', 'day', 'did', 'does', 'doing', 'down',
  'each', 'few', 'for', 'from', 'get', 'gets', 'getting', 'got', 'had', 'has',
  'have', 'here', 'how', 'http', 'https', 'into', 'its', 'just', 'like', 'make',
  'many', 'more', 'much', 'new', 'now', 'only', 'other', 'our', 'out', 'over',
  'really', 'said', 'same', 'see', 'should', 'some', 'still', 'stock', 'than',
  'that', 'the', 'their', 'them', 'then', 'there', 'these', 'they', 'thing',
  'think', 'this', 'those', 'through', 'today', 'tomorrow', 'too', 'under',
  'very', 'want', 'was', 'way', 'were', 'what', 'when', 'where', 'which',
  'while', 'who', 'why', 'will', 'with', 'would', 'www', 'com', 'you', 'your',
])

function trendingPhrases(posts: SocialPost[]) {
  const counts = new Map<string, number>()

  for (const post of posts) {
    const supplied = [
      ...(post.finance_keywords || []),
      ...(post.gossip_keywords || []),
      ...(post.keywords || []),
    ]
      .map(value => String(value || '').trim().toLowerCase())
      .filter(value => value.length >= 2)

    const text = displayText(post)
    const cashtags = (text.match(/\$[a-z][a-z0-9.-]{0,9}/gi) || []).map(tag => tag.toLowerCase())
    const symbols = new Set([
      String(primaryTicker(post)).toLowerCase(),
      ...cashtags.map(tag => tag.slice(1)),
      ...(text.match(/\b[A-Z]{2,5}\b/g) || []).map(tag => tag.toLowerCase()),
    ])
    const words = (text.toLowerCase().match(/[a-z][a-z'-]{2,}/g) || [])
      .filter(token => !TRENDING_STOP_WORDS.has(token) && !symbols.has(token))
      .slice(0, 80)
    const phrases = words.slice(0, -1).map((word, index) => `${word} ${words[index + 1]}`)

    // Count once per post so one repetitive account cannot own the trend list.
    for (const phrase of new Set([...supplied, ...cashtags, ...phrases])) {
      counts.set(phrase, (counts.get(phrase) || 0) + 1)
    }
  }

  const repeated = Array.from(counts.entries()).filter(([, count]) => count >= 2)
  const candidates = repeated.length ? repeated : Array.from(counts.entries())

  const ranked = (rows: Array<[string, number]>) => rows.sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
  const phrases = ranked(candidates.filter(([phrase]) => phrase.includes(' ') && !phrase.startsWith('$'))).slice(0, 8)
  const cashtags = ranked(candidates.filter(([phrase]) => phrase.startsWith('$'))).slice(0, 4)
  const suppliedWords = ranked(candidates.filter(([phrase]) => !phrase.includes(' ') && !phrase.startsWith('$')))

  return [...phrases, ...cashtags, ...suppliedWords].slice(0, 12)
}


function sentimentBadgeClass(sentiment?: string) {
  const s = String(sentiment || '').toLowerCase()

  if (s.includes('bull') || s.includes('positive')) {
    return 'border-emerald-500/40 bg-emerald-500/15 text-emerald-300'
  }

  if (s.includes('bear') || s.includes('negative')) {
    return 'border-red-500/40 bg-red-500/15 text-red-300'
  }

  return 'border-yellow-500/40 bg-yellow-500/15 text-yellow-300'
}

function sentimentDisplay(sentiment?: string) {
  const s = String(sentiment || 'neutral').toLowerCase()

  if (s.includes('bull') || s.includes('positive')) return 'Bullish'
  if (s.includes('bear') || s.includes('negative')) return 'Bearish'
  return 'Neutral'
}

function validationLabel(post: SocialPost) {
  const audit = post.sentiment_validation
  if (!audit) return ''
  const agreement = String(audit.agreement || '').replaceAll('_', ' ')
  const confidence = typeof audit.confidence === 'number' ? `${Math.round(audit.confidence * 100)}%` : ''
  const textScore = typeof audit.text_score === 'number' ? `text ${audit.text_score >= 0 ? '+' : ''}${audit.text_score.toFixed(2)}` : ''
  return [agreement, confidence, textScore].filter(Boolean).join(' · ')
}

export default function SocialPage() {
  const [active, setActive] = useState('all')
  const [windowMinutes, setWindowMinutes] = useState('1440')
  const [posts, setPosts] = useState<SocialPost[]>([])
  const [loading, setLoading] = useState(false)
  const [searching, setSearching] = useState(false)
  const [lastUpdated, setLastUpdated] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tickerSearch, setTickerSearch] = useState('')
  const tickerSearchOptions = useTickerDatalistOptions(tickerSearch)
  const [tickerFilter, setTickerFilter] = useState('')
  const [phraseFilter, setPhraseFilter] = useState('')
  const [platformStatus, setPlatformStatus] = useState<PlatformStatus[]>([])

  async function loadSocial(filterTicker = tickerFilter) {
    if (active === 'grok') {
      setLoading(false)
      setPosts([])
      setError(null)
      return
    }
    setLoading(true)
    setError(null)

    try {
      const params = new URLSearchParams()
      params.set('window_minutes', windowMinutes)
      params.set('limit', '200')
      if (active !== 'all') params.set('platform', active)
      if (filterTicker) params.set('ticker', filterTicker)

      const res = await fetch(`/api/social/rolling?${params.toString()}`)
      if (!res.ok) throw new Error(`HTTP ${res.status}`)

      const data = await res.json()
      setPosts(Array.isArray(data.rows) ? data.rows : [])
      setPlatformStatus(Array.isArray(data.platform_status) ? data.platform_status : [])
      setLastUpdated(Date.now())
    } catch (err: any) {
      setError(err?.message || 'Failed to load social feed')
      setPosts([])
      setPlatformStatus([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadSocial()
    const id = window.setInterval(loadSocial, 60000)
    return () => window.clearInterval(id)
  }, [active, windowMinutes, tickerFilter])

  async function searchTicker(event: FormEvent) {
    event.preventDefault()
    const ticker = tickerSearch.trim().toUpperCase().replace(/[^A-Z0-9.$-]/g, '').replace(/^\$/, '')
    if (!ticker) {
      setTickerFilter('')
      await loadSocial('')
      return
    }

    setSearching(true)
    setTickerFilter(ticker)
    setError(null)

    try {
      await loadSocial(ticker)
      const res = await fetch(`/api/social/fetch?ticker=${encodeURIComponent(ticker)}`, { method: 'POST' })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        setError(data?.error || `Fresh fetch returned HTTP ${res.status}; showing stored rows.`)
      } else {
        await loadSocial(ticker)
      }
    } catch (err: any) {
      setError(err?.message || 'Fresh ticker fetch failed; showing stored rows if available.')
    } finally {
      setSearching(false)
    }
  }

  async function clearTickerSearch() {
    setTickerSearch('')
    setTickerFilter('')
    await loadSocial('')
  }

  const trending = useMemo(() => trendingPhrases(posts), [posts])
  const visiblePosts = useMemo(() => {
    const phrase = phraseFilter.trim().toLowerCase()
    if (!phrase) return posts
    return posts.filter(post => [
      displayText(post),
      post.ticker,
      post.symbol,
      ...(post.ticker_candidates || []),
      ...(post.tickers || []),
      ...(post.finance_keywords || []),
      ...(post.gossip_keywords || []),
      ...(post.keywords || []),
    ].filter(Boolean).join(' ').toLowerCase().includes(phrase.replace(/^\$/, '')))
  }, [posts, phraseFilter])

  useEffect(() => setPhraseFilter(''), [active, windowMinutes, tickerFilter])

  const platformCards = useMemo(() => {
    const byPlatform = new Map(platformStatus.map(row => [row.platform.toLowerCase(), row]))
    return tabs.filter(tab => tab.id !== 'all' && tab.id !== 'grok').map(tab => {
      const row = byPlatform.get(tab.label.toLowerCase()) || byPlatform.get(tab.id)
      return {
        ...tab,
        total: row?.total ?? 0,
        matched: row?.ticker_matched ?? 0,
        status: row?.status || 'no_rows_in_window',
        latest: row?.latest_sec,
      }
    })
  }, [platformStatus])

  return (
    <div className="p-6 md:p-8 text-white">
      <div className="flex items-center justify-between gap-4 mb-8">
        <div>
          <h1 className="text-3xl font-bold">Social Feed</h1>
          <p className="text-sm text-neutral mt-1">
            Rolling {windowMinutes}m window
            {lastUpdated ? ` • updated ${new Date(lastUpdated).toLocaleTimeString()}` : ''}
          </p>
        </div>

        <div className="flex items-center gap-3">
          <form onSubmit={searchTicker} className="flex items-center gap-2">
            <input
              value={tickerSearch}
              list="social-feed-ticker-universe"
              onChange={e => setTickerSearch(e.target.value.toUpperCase())}
              placeholder="Search ticker"
              className="w-32 bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm uppercase"
            />
            <datalist id="social-feed-ticker-universe">
              {tickerSearchOptions.map(row => (
                <option key={row.ticker} value={row.ticker}>{row.company || row.exchange || row.ticker}</option>
              ))}
            </datalist>
            <button
              type="submit"
              disabled={searching}
              className="bg-sky-600 hover:bg-sky-500 disabled:opacity-50 rounded-md px-3 py-2 text-sm font-medium"
            >
              {searching ? 'Searching...' : 'Search'}
            </button>
            {tickerFilter && (
              <button
                type="button"
                onClick={clearTickerSearch}
                className="border border-slate-600 hover:border-sky-500 rounded-md px-3 py-2 text-sm text-neutral"
              >
                Clear
              </button>
            )}
          </form>
          <span className="text-[10px] text-neutral uppercase tracking-wider">Window</span>
          <select
            value={windowMinutes}
            onChange={e => setWindowMinutes(e.target.value)}
            className="bg-slate-900 border border-slate-600 rounded-md px-3 py-2 text-sm"
          >
            <option value="5">5m</option>
            <option value="15">15m</option>
            <option value="30">30m</option>
            <option value="60">60m</option>
            <option value="120">2h</option>
            <option value="1440">24h</option>
          </select>
          <div className="text-neutral text-lg">
            {phraseFilter ? `${visiblePosts.length} of ${posts.length} posts` : `${posts.length} posts`}
          </div>
        </div>
      </div>

      <div className="flex gap-8 border-b border-slate-700 mb-6">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActive(tab.id)}
            className={
              active === tab.id
                ? 'pb-4 text-white border-b-4 border-sky-500 font-semibold'
                : 'pb-4 text-neutral hover:text-white'
            }
          >
            {tab.label}
          </button>
        ))}
      </div>

      {active === 'grok' ? (
        <GrokSocialAnalysis />
      ) : (
        <>
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-3 mb-6">
        {platformCards.map(row => (
          <div key={row.id} className="border border-slate-700 bg-slate-900/60 rounded-lg px-3 py-2">
            <div className="flex items-center justify-between gap-2">
              <span className="text-sm font-semibold">{row.label}</span>
              <span className={row.matched > 0 ? 'text-emerald-300 text-xs' : row.total > 0 ? 'text-yellow-300 text-xs' : 'text-neutral text-xs'}>
                {row.matched > 0 ? 'working' : row.total > 0 ? 'unmatched' : 'empty'}
              </span>
            </div>
            <div className="mt-1 text-xs text-neutral">
              {row.matched}/{row.total} ticker matched{row.latest ? ` · latest ${timeAgo(row.latest)}` : ''}
            </div>
          </div>
        ))}
      </div>

      <div className="border border-slate-700 bg-slate-800/60 rounded-xl p-6 mb-6">
        <div className="flex items-center justify-between gap-3 mb-4">
          <h2 className="text-xl font-semibold">Trending Phrases</h2>
          {phraseFilter && (
            <button
              type="button"
              onClick={() => setPhraseFilter('')}
              className="rounded-md border border-sky-500/60 px-3 py-1 text-sm text-sky-200 hover:bg-sky-500/10"
            >
              Reset feed
            </button>
          )}
        </div>
        {phraseFilter && <p className="mb-3 text-sm text-neutral">Showing posts containing “{phraseFilter}”</p>}
        {trending.length ? (
          <div className="flex flex-wrap gap-2">
            {trending.map(([phrase, count]) => (
              <button
                key={phrase}
                type="button"
                onClick={() => setPhraseFilter(phrase)}
                className={`rounded-full border px-3 py-1 text-sm transition ${phraseFilter === phrase ? 'border-sky-400 bg-sky-500/20 text-sky-100' : 'border-slate-700 bg-slate-900 hover:border-sky-500/60'}`}
              >
                {phrase.startsWith('$') ? phrase.toUpperCase() : phrase} <span className="text-neutral">×{count}</span>
              </button>
            ))}
          </div>
        ) : (
          <p className="text-neutral text-sm">No trending phrases</p>
        )}
      </div>

      {error && (
        <div className="border border-red-500/40 bg-red-950/30 rounded-xl p-4 mb-4 text-red-200">
          Social feed error: {error}
        </div>
      )}

      {loading && posts.length === 0 ? (
        <div className="text-neutral text-center py-20">Loading social posts...</div>
      ) : visiblePosts.length === 0 ? (
        <div className="text-neutral text-center py-20">
          <div className="text-5xl mb-4">💬</div>
          <div>{phraseFilter ? `No posts contain “${phraseFilter}”` : 'No posts found for current filters'}</div>
          <div className="text-sm mt-2">{phraseFilter ? 'Reset the feed or choose another phrase.' : 'Try 24h, or run the social collector to populate the live 5m window.'}</div>
        </div>
      ) : (
        <div className="space-y-3">
          {visiblePosts.map((post, idx) => {
            const text = displayText(post)
            const ticker = primaryTicker(post)
            const validation = validationLabel(post)
            return (
              <a
                key={`${post.platform}-${post.url}-${idx}`}
                href={post.url || '#'}
                target="_blank"
                rel="noreferrer"
                className="block border border-slate-700 bg-slate-900/60 rounded-xl p-4 hover:border-sky-500/60 transition"
              >
                <div className="flex items-center justify-between gap-4 mb-2">
                  <div className="flex items-center gap-2">
                    <span className="font-semibold">{sourceLabel(post)}</span>
                    {ticker && <span className="text-sky-300 font-semibold">${ticker}</span>}
                    {post.sentiment && (
                      <span className={`rounded-full border px-2 py-0.5 text-xs font-semibold ${sentimentBadgeClass(post.sentiment)}`}>
                        {sentimentDisplay(post.sentiment)}
                        {typeof post.sentiment_score === 'number' ? ` ${post.sentiment_score.toFixed(2)}` : ''}
                      </span>
                    )}
                  </div>
                  <span className="text-xs text-neutral">{timeAgo(ts(post))}</span>
                </div>

                <div className="text-sm leading-relaxed text-slate-100 line-clamp-3">
                  {ticker ? `$${ticker}: ${text}` : text}
                </div>

                <div className="mt-3 flex flex-wrap gap-2 text-xs text-neutral">
                  {post.author && <span>@{post.author}</span>}
                  {(post.gossip_keywords || []).slice(0, 4).map(k => (
                    <span key={k} className="text-amber-300">#{k}</span>
                  ))}
                  {(post.finance_keywords || post.keywords || []).slice(0, 4).map(k => (
                    <span key={k}>#{k}</span>
                  ))}
                  {validation && <span className="text-sky-300">{validation}</span>}
                </div>
              </a>
            )
          })}
        </div>
      )}
        </>
      )}
    </div>
  )
}
