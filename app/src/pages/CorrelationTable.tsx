'use client'
import { useState } from 'react'
import { CorrelationRow } from './CorrelationRow'
import type { CorrelationEntry } from '@/lib/types'

interface Props { entries: CorrelationEntry[] }

// Only these columns have headers, so only these can ever reach the comparator.
// Typing the sort key as the full `keyof CorrelationEntry` pulled in the three
// boolean fields (price_move_valid, flat_previous_close, generated), which is
// why `bv - av` would not typecheck. Narrowing to the reachable keys is what
// makes the subtraction sound, and it means adding a boolean column to the
// header below is now a compile error instead of a silent 1/0 sort.
type SortKey = 'ticker' | 'correlation' | 'direction' | 'price'
  | 'change_pct' | 'combined_sentiment' | 'sample_size' | 'reliability_weight'

const SORT_COLUMNS: Array<{ key: SortKey; label: string }> = [
  { key: 'ticker', label: 'TICKER' },
  { key: 'correlation', label: 'CORRELATION' },
  { key: 'direction', label: 'DIRECTION' },
  { key: 'price', label: 'PRICE' },
  { key: 'change_pct', label: 'CHG%' },
  { key: 'combined_sentiment', label: 'SENT' },
  { key: 'sample_size', label: 'EVIDENCE' },
  { key: 'reliability_weight', label: 'REL' },
]

export function CorrelationTable({ entries }: Props) {
  const [sort, setSort] = useState<{ key: SortKey; dir: 'asc' | 'desc' }>({ key: 'correlation', dir: 'desc' })

  const sorted = [...entries].sort((a, b) => {
    const av = a[sort.key] ?? 0
    const bv = b[sort.key] ?? 0
    if (typeof av === 'string' || typeof bv === 'string') {
      return sort.dir === 'desc' ? String(bv).localeCompare(String(av)) : String(av).localeCompare(String(bv))
    }
    return sort.dir === 'desc' ? bv - av : av - bv
  })

  return (
    <div className="bg-surface border border-border rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead className="border-b border-border">
          <tr>
            {SORT_COLUMNS.map(({ key, label }) => (
              <th key={key} onClick={() => setSort(s => ({ key, dir: s.key === key && s.dir === 'desc' ? 'asc' : 'desc' }))}
                className="px-3 py-2 text-left label cursor-pointer hover:text-neutral select-none">
                {label} {sort.key === key ? (sort.dir === 'desc' ? '↓' : '↑') : ''}
              </th>
            ))}
            <th className="px-3 py-2 label">VISUAL</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map(e => <CorrelationRow key={e.ticker} entry={e} />)}
        </tbody>
      </table>
    </div>
  )
}
