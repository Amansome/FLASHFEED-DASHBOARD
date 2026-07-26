'use client'
import { useState } from 'react'
import { useTickerDatalistOptions } from '@/lib/useTickerUniverse'

interface Props {
  filters: Record<string, string>
  onChange: (f: Record<string, string>) => void
}

export function ArticleFilters({ filters, onChange }: Props) {
  const [open, setOpen] = useState(false)
  const tickerOptions = useTickerDatalistOptions(filters.ticker || '')

  const setFilter = (key: string, value: string) => {
    if (value) onChange({ ...filters, [key]: value })
    else { const f = { ...filters }; delete f[key]; onChange(f) }
  }

  const activePills = Object.entries(filters)

  return (
    <div>
      <div className="flex items-center gap-2 flex-wrap">
        <button
          onClick={() => setOpen(o => !o)}
          className="text-xs border border-border text-neutral hover:text-white hover:border-accent px-3 py-1.5 rounded transition-colors"
        >
          + Add Filter
        </button>
        {activePills.map(([k, v]) => (
          <span key={k} className="flex items-center gap-1 text-xs bg-accent/10 border border-accent/30 text-accent px-2 py-1 rounded">
            {k}: {v}
            <button onClick={() => setFilter(k, '')} className="hover:text-white ml-1">×</button>
          </span>
        ))}
      </div>
      {open && (
        <div className="mt-2 p-3 bg-surface border border-border rounded-lg grid grid-cols-2 sm:grid-cols-4 gap-3">
          <FilterSelect label="Sentiment" k="sentiment" options={['bullish','bearish','neutral']} filters={filters} setFilter={setFilter} />
          <FilterSelect label="Article Type" k="article_kind" options={['structured','public','filings']} filters={filters} setFilter={setFilter} />
          <FilterInput label="Source" k="source" filters={filters} setFilter={setFilter} placeholder="e.g. PR Newswire" />
          <FilterInput label="Ticker" k="ticker" filters={filters} setFilter={setFilter} placeholder="e.g. AAPL" datalistId="news-filter-ticker-universe" />
          <datalist id="news-filter-ticker-universe">
            {tickerOptions.map(row => (
              <option key={row.ticker} value={row.ticker}>{row.company || row.exchange || row.ticker}</option>
            ))}
          </datalist>
          <FilterInput label="Search" k="search" filters={filters} setFilter={setFilter} placeholder="keyword..." />
        </div>
      )}
    </div>
  )
}

function FilterSelect({ label, k, options, filters, setFilter }: any) {
  return (
    <div>
      <label className="label block mb-1">{label}</label>
      <select
        value={filters[k] ?? ''}
        onChange={e => setFilter(k, e.target.value)}
        className="w-full bg-bg border border-border text-sm text-neutral rounded px-2 py-1.5 focus:outline-none focus:border-accent"
      >
        <option value="">All</option>
        {options.map((o: string) => <option key={o} value={o}>{o}</option>)}
      </select>
    </div>
  )
}

function FilterInput({ label, k, filters, setFilter, placeholder, datalistId }: any) {
  return (
    <div>
      <label className="label block mb-1">{label}</label>
      <input
        value={filters[k] ?? ''}
        list={datalistId}
        onChange={e => setFilter(k, e.target.value)}
        placeholder={placeholder}
        className="w-full bg-bg border border-border text-sm text-neutral rounded px-2 py-1.5 focus:outline-none focus:border-accent placeholder:text-slate-600"
      />
    </div>
  )
}
