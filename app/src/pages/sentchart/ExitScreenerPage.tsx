'use client'
import useSWR from 'swr'
import { useState, useMemo } from 'react'
import { clsx } from 'clsx'
import type { ExitScreenerRow } from '@/lib/types'

// Exit Screener — simulated positions derived live from the strategy sim in the
// chart-service (rolling-corr entry and post-entry-peak trailing stop; joined
// and flattened by /api/exit-screener). Correlation-break exits are available
// only when explicitly enabled for validation. The Trailing Stop % slider recalculates
// stop_price and distance_to_stop_pct CLIENT-SIDE from peak_price/current_price
// — no refetch, same pattern as the Entry Screener's threshold slider. Rows
// within 2% of their stop get an amber background.

const fetcher = (url: string) => fetch(url).then(r => r.json())

const NEAR_STOP_PCT = 2

const COLUMNS: Array<{ key: string; label: string }> = [
  { key: 'ticker', label: 'TICKER' },
  { key: 'entry_price', label: 'ENTRY PRICE' },
  { key: 'entry_time', label: 'ENTRY TIME' },
  { key: 'current_price', label: 'CURRENT PRICE' },
  { key: 'pnl_pct', label: 'P&L %' },
  { key: 'trailing_stop_pct', label: 'TRAILING STOP %' },
  { key: 'stop_price', label: 'STOP PRICE' },
  { key: 'distance_to_stop_pct', label: 'DIST TO STOP' },
  { key: 'exit_reason', label: 'EXIT REASON' },
  { key: 'status', label: 'STATUS' },
]

function exitReasonLabel(value?: string | null): string {
  if (value === 'price_trailing_stop') return 'Price stop'
  if (value === 'correlation_break') return 'Corr break'
  if (value === 'session_end') return 'Session end'
  return value || '--'
}

export function ExitScreenerPage() {
  const [stopPct, setStopPct] = useState(5)
  const [orderBy, setOrderBy] = useState<string>('distance_to_stop_pct')
  const [orderDir, setOrderDir] = useState<'asc' | 'desc'>('asc')

  // stopPct is intentionally NOT in the SWR key — the server sims at the default
  // 5% (which decides Holding vs Stopped Out); the slider re-derives stop price
  // and distance client-side from the sim's tracked peak, so moving it never refetches
  const { data, isLoading } = useSWR('/api/exit-screener?limit=30', fetcher, { refreshInterval: 30_000 })

  const rows = useMemo(() => {
    const all = ((data?.rows ?? []) as ExitScreenerRow[]).map(r => {
      const stopPrice = r.peak_price != null ? r.peak_price * (1 - stopPct / 100) : null
      const refPrice = r.status === 'Stopped Out' ? r.exit_price : r.current_price
      const distance = refPrice && stopPrice != null ? ((refPrice - stopPrice) / refPrice) * 100 : null
      return { ...r, trailing_stop_pct: stopPct, stop_price: stopPrice, distance_to_stop_pct: distance }
    })
    return all.sort((a, b) => {
      const av = (a as any)[orderBy]
      const bv = (b as any)[orderBy]
      if (av == null && bv == null) return 0
      if (av == null) return 1                    // nulls always last
      if (bv == null) return -1
      if (typeof av === 'string') return orderDir === 'asc' ? av.localeCompare(bv) : bv.localeCompare(av)
      return orderDir === 'asc' ? Number(av) - Number(bv) : Number(bv) - Number(av)
    })
  }, [data, stopPct, orderBy, orderDir])

  const holding = rows.filter(r => r.status === 'Holding').length
  const nearStop = rows.filter(r =>
    r.status === 'Holding' && r.distance_to_stop_pct != null && r.distance_to_stop_pct <= NEAR_STOP_PCT).length
  const warming = data?.tickers_warming ?? 0

  const toggleSort = (key: string) => {
    if (orderBy === key) setOrderDir(d => (d === 'asc' ? 'desc' : 'asc'))
    else { setOrderBy(key); setOrderDir(key === 'ticker' || key === 'status' || key === 'entry_time' ? 'asc' : 'desc') }
  }

  return (
    <div>
      <div className="flex items-center justify-between gap-3 mb-3">
        <h1 className="text-white font-semibold text-lg">Exit Screener</h1>
        <span className="text-neutral text-sm">
          {rows.length} simulated position{rows.length === 1 ? '' : 's'} across {data?.tickers_scanned ?? 0} scanned tickers
          · {holding} holding
        </span>
      </div>

      {/* Trailing stop slider */}
      <div className="bg-surface border border-border rounded-lg px-4 py-3 mb-3">
        <div className="flex flex-wrap items-center gap-4">
          <span className="text-[10px] text-neutral uppercase tracking-wide font-medium whitespace-nowrap">
            Trailing Stop %
          </span>
          <input
            type="range"
            min={5}
            max={30}
            step={5}
            value={stopPct}
            onChange={e => setStopPct(Number(e.target.value))}
            className="flex-1 min-w-[160px] accent-sky-500"
          />
          <span className="font-mono text-white text-sm w-12 text-right">{stopPct}%</span>
          <span className="text-xs text-neutral whitespace-nowrap">
            {nearStop} within {NEAR_STOP_PCT}% of stop
          </span>
        </div>
        <div className="text-[10px] text-slate-500 mt-2">
          <span className="text-slate-400 italic">
            Default 5% is the sweep-optimal value from the professor sweep analysis; provisional, pending validation against the corrected backtest.
          </span>
          <br />
          Positions are simulated by the Charts strategy (rolling-corr entry at {data?.threshold ?? 0.1}, trailing stop exit
          {data?.corrExitThreshold != null ? `, plus correlation-break validation at ${data.corrExitThreshold}` : ''}).
          Stop Price = post-entry peak x (1 - stop%).
          The slider re-derives stop price and distance live; Holding vs Stopped Out reflects the {data?.stopPct ?? 5}%
          sim. Amber rows are within {NEAR_STOP_PCT}% of their stop.
          {warming > 0 && ` · ${warming} ticker${warming > 1 ? 's' : ''} still collecting messages`}
        </div>
      </div>

      {/* Table */}
      {isLoading ? (
        <div className="text-neutral text-sm animate-pulse p-4">Loading exit screener...</div>
      ) : rows.length === 0 ? (
        <div className="text-center py-12 text-neutral">
          <div className="text-3xl mb-2">📉</div>
          <div className="text-sm">{data?.note || 'No simulated positions in the current session'}</div>
        </div>
      ) : (
        <div className="bg-surface border border-border rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead className="border-b border-border bg-bg/50">
                <tr>
                  {COLUMNS.map(col => (
                    <th
                      key={col.key}
                      onClick={() => toggleSort(col.key)}
                      className="px-2 py-2 text-left text-[10px] text-neutral uppercase tracking-wide font-medium whitespace-nowrap cursor-pointer hover:text-white select-none"
                    >
                      {col.label}
                      {orderBy === col.key && <span className="ml-0.5">{orderDir === 'desc' ? '▾' : '▴'}</span>}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-700/30">
                {rows.map(row => {
                  const nearStopRow = row.status === 'Holding'
                    && row.distance_to_stop_pct != null && row.distance_to_stop_pct <= NEAR_STOP_PCT
                  return (
                    <tr
                      key={`${row.ticker}-${row.entry_epoch}`}
                      className={clsx('hover:bg-card-hover transition-colors', nearStopRow && 'bg-amber-500/10')}
                    >
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono font-bold text-accent">{row.ticker}</span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-white">{row.entry_price != null ? `$${row.entry_price.toFixed(2)}` : '—'}</span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-neutral">{row.entry_time ?? '—'}<span className="text-slate-500 ml-1">{row.date ?? ''}</span></span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-white">{row.current_price != null ? `$${row.current_price.toFixed(2)}` : '—'}</span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className={clsx('font-mono', (row.pnl_pct ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400')}>
                          {row.pnl_pct != null ? `${row.pnl_pct >= 0 ? '+' : ''}${row.pnl_pct.toFixed(2)}%` : '—'}
                        </span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-neutral">{row.trailing_stop_pct}%</span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-neutral">{row.stop_price != null ? `$${row.stop_price.toFixed(2)}` : '—'}</span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className="font-mono text-white">
                          {row.distance_to_stop_pct != null ? `${row.distance_to_stop_pct.toFixed(2)}%` : '—'}
                        </span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span
                          className={clsx(
                            'font-mono',
                            row.exit_reason === 'correlation_break' ? 'text-amber-300' :
                              row.exit_reason === 'price_trailing_stop' ? 'text-red-300' : 'text-neutral',
                          )}
                          title={row.exit_corr != null ? `Exit corr ${row.exit_corr.toFixed(3)}` : undefined}
                        >
                          {exitReasonLabel(row.exit_reason)}
                        </span>
                      </td>
                      <td className="px-2 py-2 whitespace-nowrap">
                        <span className={clsx('font-mono', row.status === 'Holding' ? 'text-emerald-400' : 'text-neutral')}>
                          {row.status ?? '—'}
                        </span>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
