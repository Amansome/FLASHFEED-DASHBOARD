import { useEffect, useMemo, useState } from 'react'

export interface TickerUniverseItem {
  ticker: string
  company?: string
  exchange?: string
  price?: number | null
}

let tickerUniverseCache: TickerUniverseItem[] | null = null

export function useTickerUniverse(limit = 7000) {
  const [tickers, setTickers] = useState<TickerUniverseItem[]>(tickerUniverseCache || [])

  useEffect(() => {
    if (tickerUniverseCache) return
    let cancelled = false
    fetch(`/api/screener/tickers?limit=${limit}`, { cache: 'no-store' })
      .then(res => res.ok ? res.json() : null)
      .then(data => {
        if (cancelled || !Array.isArray(data?.tickers)) return
        tickerUniverseCache = data.tickers
        setTickers(data.tickers)
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [limit])

  return tickers
}

export function useTickerDatalistOptions(query: string, limit = 160) {
  const universe = useTickerUniverse()
  return useMemo(() => {
    const q = String(query || '').trim().toUpperCase()
    const rows = q
      ? universe.filter(row => row.ticker.startsWith(q) || String(row.company || '').toUpperCase().includes(q))
      : universe
    return rows.slice(0, limit)
  }, [query, universe, limit])
}
