'use client'
import { useEffect, useRef, useState } from 'react'
import Chart from 'chart.js/auto'
import zoomPlugin from 'chartjs-plugin-zoom'
import { smoothSame, trailingCount, ROLL_WINDOW_DEFAULT } from './chartAgg'

// Registered here rather than in a global chart bootstrap: ResearchChart is the
// only Chart.js consumer in the app, and the plugin is inert on any chart that
// doesn't opt in via options.plugins.zoom (pan/wheel/pinch all default off), so
// only the Price+Density view becomes zoomable.
Chart.register(zoomPlugin)

// ── Embedded Chart.js research views ─────────────────────────────────────────
// Faithful React port of the legacy dashboard's three research charts (the views
// the professor pushed on): Price+Density, Sentiment Score, and Density-vs-
// Sentiment. The smoothing/windowing math is NOT re-derived here — it stays in
// the Flask backend (_smooth_same, 5-min sliding sentiment); this component only
// plots the server-computed series from /api/sentchart/chart + /api/sentchart/chart/social, so it
// can't silently diverge from the research scripts.
//
// A single <canvas> is driven by Chart.js on a ref. The instance is destroyed
// before every rebuild and on unmount (the multi-instance lifecycle discipline
// from NOTES.md — never leave a live Chart.js on a reused canvas).

export type ResearchMode = 'pd' | 'sent' | 'ds'
type Win = 'full' | '2h' | '1h'

interface SocialData {
  labels: string[]; density: number[]; density_smooth: number[]
  sent_labels: string[]; scores: number[]; scores_smooth: number[]
  win_density: number[]; win_density_smooth: number[]
  roll_window?: number
  messages: number; bullish: number; bearish: number
  source?: string; complete?: boolean; coverage_start?: string
  status?: string; count?: number; error?: string
}
interface PriceData { ticker: string; date: string; labels: string[]; prices: number[]; volumes: number[]; prev_close?: number | null; prev_date?: string | null; error?: string }

const fetchJSON = (url: string) => fetch(url).then(r => r.json())

const mapBy = (labels: string[], values: number[]) => {
  const m: Record<string, number> = {}
  labels.forEach((l, i) => { m[l] = values[i] })
  return m
}

// smoothSame (the backend _smooth_same port) lives in @/lib/chartAgg now, shared
// with the candlestick overlays so both views smooth density identically.
const atMap = (m: Record<string, number>) => (l: string): number | null => (l in m ? m[l] : null)

// Research x-axis: full session uses the social per-minute timeline; 2h/1h follow
// the price window so the zoom controls keep working (legacy _researchLabels).
function researchLabels(d: PriceData, social: SocialData, win: Win): string[] {
  if (win === 'full') return social.labels
  const lo = d.labels[0], hi = d.labels[d.labels.length - 1]
  return social.labels.filter(l => l >= lo && l <= hi)
}

// Red dashed at 09:30, purple dashed at 16:00 (research scripts' axvline cues).
const marketLinesPlugin = {
  id: 'marketLines',
  afterDatasetsDraw(chart: any) {
    const { ctx, chartArea, scales: { x } } = chart
    if (!chartArea) return
    const labels: string[] = chart.data.labels || []
    ;[['09:30', 'rgba(239,68,68,.75)'], ['16:00', 'rgba(139,92,246,.85)']].forEach(([t, col]) => {
      // Exact match only: the 24-h timeline starts at the prior evening
      // ("20:00"), so lexicographic fallbacks/guards would misfire. The
      // per-minute timeline always contains HH:MM exactly when it's in view.
      const i = labels.indexOf(t)
      if (i < 0) return
      const px = x.getPixelForValue(i)
      ctx.save()
      ctx.strokeStyle = col; ctx.setLineDash([4, 3]); ctx.lineWidth = 1.2
      ctx.beginPath(); ctx.moveTo(px, chartArea.top); ctx.lineTo(px, chartArea.bottom); ctx.stroke()
      ctx.restore()
    })
  },
}

// Zero baseline for sentiment axes (the scripts' axhline(0)).
const zeroLinePlugin = {
  id: 'zeroLine',
  afterDatasetsDraw(chart: any, _a: any, opts: any) {
    if (!opts || !opts.scale) return
    const sc = chart.scales[opts.scale], area = chart.chartArea
    if (!sc || !area) return
    const y = sc.getPixelForValue(0)
    if (y < area.top || y > area.bottom) return
    const ctx = chart.ctx
    ctx.save()
    ctx.strokeStyle = 'rgba(200,200,200,.45)'; ctx.lineWidth = .8
    ctx.beginPath(); ctx.moveTo(area.left, y); ctx.lineTo(area.right, y); ctx.stroke()
    ctx.restore()
  },
}

// High/Low peak markers on the price series (Combined script's scatter+annotate).
const hiLoPlugin = {
  id: 'hiLo',
  afterDatasetsDraw(chart: any, _a: any, opts: any) {
    if (!opts || !opts.enabled) return
    const data: (number | null)[] = chart.data.datasets[0].data, labels: string[] = chart.data.labels
    // Carried (non-traded) minutes are not a real high/low. Absent the flags,
    // every point counts — the plugin is inert on charts that don't pass them.
    const isReal: boolean[] = opts.isReal || []
    let hi = -1, lo = -1
    data.forEach((v, i) => {
      if (v == null || isReal[i] === false) return
      if (hi < 0 || v > (data[hi] as number)) hi = i
      if (lo < 0 || v < (data[lo] as number)) lo = i
    })
    if (hi < 0) return
    const meta = chart.getDatasetMeta(0), ctx = chart.ctx
    const draw = (i: number, col: string, txt: string, dy: number) => {
      const el = meta.data[i]; if (!el) return
      ctx.save()
      ctx.fillStyle = col
      ctx.beginPath(); ctx.arc(el.x, el.y, 4, 0, Math.PI * 2); ctx.fill()
      ctx.font = '9px monospace'; ctx.textAlign = 'left'
      const tx = Math.min(el.x + 8, chart.chartArea.right - 90)
      ctx.fillText(txt, tx, el.y + dy)
      ctx.fillText(labels[i], tx, el.y + dy + 10)
      ctx.restore()
    }
    draw(hi, '#2ea043', `High: $${(data[hi] as number).toFixed(2)}`, -14)
    draw(lo, '#e5534b', `Low: $${(data[lo] as number).toFixed(2)}`, 16)
  },
}

// ── X-axis tick presentation (shared by all three views) ─────────────────────
// The x scale is a CATEGORY scale over the backend's "HH:MM" strings, one per
// minute. That label array is load-bearing: marketLinesPlugin does
// labels.indexOf('09:30'), hiLoPlugin prints labels[i], and every series joins
// on those exact keys. So all clarity work happens at DISPLAY time — via
// afterBuildTicks + ticks.callback — and the array is never rewritten.

const AXIS_TICK_COLOR = '#cbd5e1'   // slate-300, ~12:1 on --bg #0F172A (was #4e5567, ~2.4:1)
const AXIS_TICK_SIZE = 11           // was 8
const AXIS_TITLE_SIZE = 10

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

// 'YYYY-MM-DD' -> 'Jul 23'. Parsed straight off the string, never through
// Date(), so the viewer's timezone can't shift an ET session date by a day.
function shortDate(iso?: string | null): string | null {
  if (!iso) return null
  const [, m, d] = iso.split('-').map(Number)
  return m && d ? `${MONTHS[m - 1]} ${d}` : null
}

const minutesOf = (label: string): number => +label.slice(0, 2) * 60 + +label.slice(3, 5)

// Visible span (minutes) -> tick spacing, targeting ~12-16 labels at any zoom.
// Because labels are consecutive minutes, selecting on HH:MM % step === 0 puts
// every tick on a clean boundary — the old maxTicksLimit:16 autoSkip picked
// evenly spaced INDICES instead, which on the 24-h view landed on arbitrary
// half-hour offsets (20:00, 21:30, 23:00, …).
function tickStepMinutes(span: number): number {
  if (span > 1080) return 120
  if (span > 480) return 60
  if (span > 240) return 30
  if (span > 120) return 15
  if (span > 60) return 10
  if (span > 20) return 5
  return 1
}

const MAX_VISIBLE_TICKS = 24

function xScale(labels: string[], d: PriceData): any {
  // The social timeline is [prev_date 20:00, date 20:00), so any label at or
  // after 20:00 belongs to the PREVIOUS calendar day. That crossing is exactly
  // why a bare "03:00" used to be ambiguous.
  const SESSION_END_MIN = 20 * 60
  const today = shortDate(d.date)
  const yesterday = shortDate(d.prev_date)
  const dayOf = (label: string) => (minutesOf(label) >= SESSION_END_MIN ? yesterday : today)

  return {
    grid: { color: '#1e2330' },
    ticks: {
      color: AXIS_TICK_COLOR,
      font: { size: AXIS_TICK_SIZE },
      // afterBuildTicks has already chosen a clean, non-overlapping set;
      // autoSkip would re-thin it and break the boundary snapping.
      autoSkip: false,
      maxRotation: 0,
      minRotation: 0,
      callback(value: any, i: number) {
        const label = labels[value as number]
        if (!label) return ''
        // Date context where it actually resolves ambiguity: the leftmost tick
        // in view, and the midnight rollover. Returning an array renders as
        // stacked lines, so it never crowds the neighbouring times.
        const day = (i === 0 || minutesOf(label) === 0) ? dayOf(label) : null
        return day ? [label, day] : label
      },
    },
    // Zoom-aware: axis.min/max are the current visible index bounds, which
    // chartjs-plugin-zoom updates on wheel/pan, so the granularity re-derives
    // itself on every frame (hourly zoomed out -> per-minute zoomed all the way in).
    afterBuildTicks(axis: any) {
      const last = labels.length - 1
      const lo = Math.max(0, Math.ceil(axis.min ?? 0))
      const hi = Math.min(last, Math.floor(axis.max ?? last))
      if (hi <= lo) return
      const step = tickStepMinutes(hi - lo + 1)
      const picked: Array<{ value: number }> = []
      for (let i = lo; i <= hi; i++) {
        if (minutesOf(labels[i]) % step === 0) picked.push({ value: i })
      }
      if (!picked.length) return          // leave Chart.js's own ticks rather than blanking the axis
      const thin = Math.ceil(picked.length / MAX_VISIBLE_TICKS)
      axis.ticks = thin > 1 ? picked.filter((_, k) => k % thin === 0) : picked
    },
  }
}

// Wheel/drag/pinch zoom on the x axis only — the y scales are pinned to
// data-derived bounds (price min/max, density headroom), so letting them zoom
// would just desynchronise the two axes. Price+Density only.
function zoomOpts(onZoomChange?: (zoomed: boolean) => void): any {
  const notify = ({ chart }: any) => onZoomChange?.(chart.isZoomedOrPanned())
  return {
    pan: { enabled: true, mode: 'x', threshold: 4, onPanComplete: notify },
    zoom: {
      wheel: { enabled: true, speed: 0.12 },
      pinch: { enabled: true },
      mode: 'x',
      onZoomComplete: notify,
    },
    // Never pan past the loaded session, and stop at a 10-minute floor so the
    // chart can't be zoomed into a single bar with no context.
    limits: { x: { min: 'original', max: 'original', minRange: 10 } },
  }
}

function buildConfig(
  mode: ResearchMode, d: PriceData, social: SocialData, win: Win, windowMin = 15,
  onZoomChange?: (zoomed: boolean) => void,
): any {
  const labels = researchLabels(d, social, win)
  // Legend text was the last thing still carrying the old dim #4e5567 at 9px —
  // the same ~2.4:1 contrast the axis ticks were just moved off. It now reuses
  // the axis constants so the two can't drift apart again.
  const legend = {
    display: true,
    labels: { color: AXIS_TICK_COLOR, font: { size: AXIS_TICK_SIZE }, boxWidth: 12 },
  }
  const baseOpts = {
    responsive: true, maintainAspectRatio: false, animation: false as const,
    interaction: { mode: 'index' as const, intersect: false },
  }

  if (mode === 'pd') {
    const prices = labels.map(atMap(mapBy(d.labels, d.prices)))
    // Synthetic price fill (owner request: a 24-h axis that never invents price
    // action). Every minute without a real bar is carried FLAT from the last
    // known price — the prior session's close before the first bar, the last
    // real bar after it — and drawn dashed.
    //
    // This used to carry only a LEADING null run, and only when it started at
    // index 1+. Two ways that failed: a gap that isn't at the start (the
    // 20:00->04:00 overnight window whenever no overnight tape exists) fell
    // through to spanGaps, which draws one straight line between the edges and
    // reads as a real 4-hour trend; and a single stray bar at index 0 set
    // firstReal to 0, which switched the carry off for the whole session.
    // `isReal` keeps the two apart everywhere it matters: the dash segments
    // below, and the High/Low markers, which must only ever mark traded prices.
    const isReal = prices.map(v => v != null)
    let carry = d.prev_close ?? null
    for (let i = 0; i < prices.length; i++) {
      if (prices[i] != null) carry = prices[i]
      else if (carry != null) prices[i] = carry
    }
    const dens   = labels.map(atMap(mapBy(social.labels, social.density)))
    // Trailing rolling COUNT (owner's spec): value at minute t = total messages
    // in the past `windowMin` minutes, inclusive of the t-windowMin edge.
    // Recomputed client-side over the raw per-minute `social.density` for the
    // slider's window (1..480 min). Price is window-independent.
    const trail = trailingCount(social.density, windowMin)
    const densSm = labels.map(atMap(mapBy(social.labels, trail)))
    const pVals = prices.filter((v): v is number => v != null)
    const pMin = Math.min(...pVals), pMax = Math.max(...pVals)
    // y2 must fit BOTH the per-minute bars and the (much larger) trailing count
    const maxDen = Math.max(...social.density, 1)
    const maxTrail = Math.max(...trail, 1)
    return {
      type: 'line',
      data: { labels, datasets: [
        { label: 'Close price', data: prices, yAxisID: 'y1', spanGaps: true,
          borderColor: '#2196F3', borderWidth: 1.4, tension: .1, pointRadius: 0,
          // Dashed wherever either endpoint is synthetic, so a carried stretch
          // can never be mistaken for traded price at a glance.
          segment: { borderDash: (c: any) =>
            (isReal[c.p0DataIndex] && isReal[c.p1DataIndex]) ? undefined : [4, 3] },
          fill: { target: { value: pMin } }, backgroundColor: 'rgba(33,150,243,.08)' },
        { label: 'Messages/min', type: 'bar', data: dens, yAxisID: 'y2',
          backgroundColor: 'rgba(144,202,249,.75)', borderWidth: 0,
          barPercentage: 1, categoryPercentage: 1 },
        { label: windowMin <= 1 ? 'Trailing 1-min msg count' : `Trailing ${windowMin}-min msg count`,
          data: densSm, yAxisID: 'y2', spanGaps: true,
          borderColor: '#FF9800', borderWidth: 2, tension: windowMin <= 1 ? 0 : .1, pointRadius: 0 },
      ] },
      plugins: [marketLinesPlugin, hiLoPlugin],
      options: { ...baseOpts,
        plugins: { legend,
                   // skip every synthetic minute — High/Low mark traded bars only
                   hiLo: { enabled: true, isReal },
                   zoom: zoomOpts(onZoomChange) },
        scales: {
          x: xScale(labels, d),
          y1: { position: 'left', min: pMin * 0.97, max: pMax * 1.08,
                grid: { color: '#1e2330' }, ticks: { font: { size: AXIS_TICK_SIZE }, color: '#2196F3' },
                title: { display: true, text: 'Close Price ($)', color: '#2196F3', font: { size: AXIS_TITLE_SIZE } } },
          y2: { position: 'right', beginAtZero: true,
                max: Math.max(maxDen * 2.5, maxTrail * 1.15),
                grid: { display: false }, ticks: { font: { size: AXIS_TICK_SIZE }, color: '#FF9800' },
                title: { display: true, text: 'Messages (bars: per min · line: per window)', color: '#FF9800', font: { size: AXIS_TITLE_SIZE } } },
        },
      },
    }
  }

  if (mode === 'sent') {
    const raw    = labels.map(atMap(mapBy(social.sent_labels, social.scores)))
    const smooth = labels.map(atMap(mapBy(social.sent_labels, social.scores_smooth)))
    return {
      type: 'line',
      data: { labels, datasets: [
        { label: 'Raw score', data: raw, yAxisID: 'ys', spanGaps: true,
          borderColor: 'rgba(160,160,160,.4)', borderWidth: .6, tension: 0, pointRadius: 0,
          fill: { target: 'origin', above: 'rgba(76,175,80,.2)', below: 'rgba(244,67,54,.2)' } },
        { label: '15-min smoothed score', data: smooth, yAxisID: 'ys', spanGaps: true,
          borderColor: '#4CAF50', borderWidth: 2, tension: .1, pointRadius: 0 },
      ] },
      plugins: [marketLinesPlugin, zeroLinePlugin],
      options: { ...baseOpts,
        plugins: { legend, zeroLine: { scale: 'ys' } },
        scales: {
          x: xScale(labels, d),
          // ys was the one y axis left on the dim global default; colored to its
          // smoothed-score series, matching how pd/ds tint their axes.
          ys: { position: 'left', min: -1.1, max: 1.1,
                grid: { color: '#1e2330' }, ticks: { font: { size: AXIS_TICK_SIZE }, color: '#4CAF50' },
                title: { display: true, text: 'Sentiment Score  (−1 = Bearish | +1 = Bullish)', color: '#4CAF50', font: { size: AXIS_TITLE_SIZE } } },
        },
      },
    }
  }

  // mode === 'ds'
  const dens   = labels.map(atMap(mapBy(social.sent_labels, social.win_density)))
  const densSm = labels.map(atMap(mapBy(social.sent_labels, social.win_density_smooth)))
  const smooth = labels.map(atMap(mapBy(social.sent_labels, social.scores_smooth)))
  const maxDen = Math.max(...social.win_density, 1)
  return {
    type: 'line',
    data: { labels, datasets: [
      { label: 'Messages/window', type: 'bar', data: dens, yAxisID: 'y1',
        backgroundColor: 'rgba(33,150,243,.3)', borderWidth: 0, barPercentage: 1, categoryPercentage: 1 },
      { label: `${social.roll_window ?? ROLL_WINDOW_DEFAULT}-min avg density`, data: densSm, yAxisID: 'y1', spanGaps: true,
        borderColor: '#2196F3', borderWidth: 1.8, tension: .1, pointRadius: 0 },
      { label: 'Sentiment score (smoothed)', data: smooth, yAxisID: 'ys', spanGaps: true,
        borderColor: '#FF5722', borderWidth: 2, tension: .1, pointRadius: 0,
        fill: { target: 'origin', above: 'rgba(76,175,80,.12)', below: 'rgba(244,67,54,.12)' } },
    ] },
    plugins: [marketLinesPlugin, zeroLinePlugin],
    options: { ...baseOpts,
      plugins: { legend, zeroLine: { scale: 'ys' } },
      scales: {
        x: xScale(labels, d),
        y1: { position: 'left', beginAtZero: true, max: maxDen * 2.2,
              grid: { color: '#1e2330' }, ticks: { font: { size: AXIS_TICK_SIZE }, color: '#2196F3' },
              title: { display: true, text: 'Messages per 5-min window', color: '#2196F3', font: { size: AXIS_TITLE_SIZE } } },
        ys: { position: 'right', min: -1.5, max: 1.5,
              grid: { display: false }, ticks: { font: { size: AXIS_TICK_SIZE }, color: '#FF5722' },
              title: { display: true, text: 'Sentiment Score  (−1 to +1)', color: '#FF5722', font: { size: AXIS_TITLE_SIZE } } },
      },
    },
  }
}

const TITLES: Record<ResearchMode, string> = {
  pd: 'Close Price vs Message Density',
  sent: 'Sentiment Score — (Bullish − Bearish) / Tagged · 5-min window, slides 1 min',
  ds: 'Message Density vs Sentiment Score',
}

// Rolling-window density slider bounds (Price+Density view only). Window=1 is
// the raw per-minute series; the professor-facing default is 240m, and the
// slider remains live so shorter lead/lag windows can still be inspected.
const WIN_MIN = 1, WIN_MAX = 480, WIN_DEFAULT = 240

interface Props {
  ticker: string
  mode: ResearchMode
  window: Win
  date?: string
  // Zoom control wiring for the toolbar's "Reset zoom" button, which lives up in
  // ChartsPage next to the Full/2h/1h presets. The parent hands down a ref we
  // fill with a reset closure, plus a setter so the button can disable itself
  // while the chart sits at its original extent.
  zoomResetRef?: { current: (() => void) | null }
  onZoomedChange?: (zoomed: boolean) => void
}

export function ResearchChart({ ticker, mode, window: win, date, zoomResetRef, onZoomedChange }: Props) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const chartRef = useRef<Chart | null>(null)
  const pollRef = useRef<number | null>(null)
  const [status, setStatus] = useState('')
  // The fetched data is held in state so a window change re-plots from it WITHOUT
  // re-fetching; `windowMin` drives the client-side rolling density (pd view).
  const [bundle, setBundle] = useState<{ d: PriceData; s: SocialData } | null>(null)
  const [windowMin, setWindowMin] = useState(WIN_DEFAULT)
  // Visible x range carried across a rebuild, so dragging the rolling-window
  // slider while zoomed in doesn't yank the view back to the full session.
  // Only valid while the label array is identical — cleared on any refetch.
  const zoomStateRef = useRef<{ min: number; max: number } | null>(null)

  const destroyChart = () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null } }

  // Fetch + poll only. Re-runs when ticker / mode / price-window change — never
  // on the rolling-window slider, so dragging the slider hits no backend.
  useEffect(() => {
    let cancelled = false
    setStatus('Loading…'); setBundle(null)
    // ticker/mode/win/date all change the label array, so saved index bounds
    // would point at the wrong minutes. Start the new series unzoomed.
    zoomStateRef.current = null
    onZoomedChange?.(false)

    const run = async () => {
      const d: PriceData = await fetchJSON(`/api/sentchart/chart?${new URLSearchParams({ ticker, window: win, ...(date ? { date } : {}) })}`)
      if (cancelled) return
      if (d.error) { setStatus(d.error); return }

      const poll = async () => {
        const s: SocialData = await fetchJSON(`/api/sentchart/chart/social?${new URLSearchParams({ ticker: d.ticker, date: d.date })}`)
        if (cancelled) return
        if (s.error) { setStatus('Social data: ' + s.error); return }
        if (s.status === 'walking') {
          setStatus(`Loading social history, ${s.count || 0} messages…`)
          pollRef.current = window.setTimeout(poll, 1500) as unknown as number
          return
        }
        if (!s.messages) { setStatus('Not enough social data for this day.'); return }
        let txt = `Social: ${s.source} · ${s.messages} msgs (${s.bullish}B/${s.bearish}B tagged)`
        if (!s.complete && s.coverage_start) txt += ` · partial, from ${s.coverage_start}`
        setStatus(txt)
        setBundle({ d, s })
      }
      poll()
    }
    run().catch(() => { if (!cancelled) setStatus('Error loading chart data.') })

    return () => {
      cancelled = true
      if (pollRef.current) { clearTimeout(pollRef.current); pollRef.current = null }
    }
  }, [ticker, mode, win, date])

  // Build/rebuild from already-fetched data on new data OR a window change.
  // Lightly debounced (40ms) so a fast slider drag coalesces; animation is off,
  // so the redraw is effectively instant. No fetch, no page reload.
  useEffect(() => {
    if (!bundle) { destroyChart(); return }
    const t = window.setTimeout(() => {
      if (!canvasRef.current) return
      // Carry the current visible range over the destroy/rebuild.
      const prevX = chartRef.current?.scales?.x
      if (prevX && (chartRef.current as any).isZoomedOrPanned?.()) {
        zoomStateRef.current = { min: prevX.min, max: prevX.max }
      }
      destroyChart()
      const chart = new Chart(
        canvasRef.current,
        buildConfig(mode, bundle.d, bundle.s, win, windowMin, onZoomedChange),
      )
      chartRef.current = chart
      const saved = zoomStateRef.current
      if (mode === 'pd' && saved) {
        // 'none' — reapply silently, no animation and no zoom-complete callback.
        ;(chart as any).zoomScale('x', { min: saved.min, max: saved.max }, 'none')
      }
      if (zoomResetRef) {
        zoomResetRef.current = mode === 'pd'
          ? () => {
              zoomStateRef.current = null
              ;(chart as any).resetZoom()
              onZoomedChange?.(false)
            }
          : null
      }
      onZoomedChange?.(mode === 'pd' && !!saved)
    }, 40)
    return () => window.clearTimeout(t)
  }, [bundle, windowMin, mode, win])

  // Destroy the Chart.js instance on unmount (NOTES.md: never leave one on a reused
  // canvas) and drop the parent's reset handle with it.
  useEffect(() => () => {
    destroyChart()
    if (zoomResetRef) zoomResetRef.current = null
  }, [])

  return (
    <div className="flex flex-col h-full">
      <div className="px-3 py-2 border-b border-border flex items-center justify-between">
        <span className="text-xs text-neutral font-medium uppercase tracking-wide">{ticker} — {TITLES[mode]}</span>
        <span className="text-[11px] text-neutral">{status}</span>
      </div>
      {mode === 'pd' && (
        <div className="px-3 py-2 border-b border-border flex items-center gap-3">
          <label htmlFor="pd-window" className="text-[11px] text-neutral whitespace-nowrap">
            Rolling window: <span className="text-white font-medium tabular-nums">{windowMin} min</span>
          </label>
          <input
            id="pd-window" type="range" min={WIN_MIN} max={WIN_MAX} step={1} value={windowMin}
            onChange={e => setWindowMin(Number(e.target.value))}
            className="flex-1 accent-orange-500 cursor-pointer"
            aria-label="Density rolling window in minutes"
          />
          <span className="text-[10px] text-neutral whitespace-nowrap">scroll to zoom · drag to pan</span>
        </div>
      )}
      <div className="flex-1 min-h-0 p-2">
        <canvas ref={canvasRef} />
      </div>
    </div>
  )
}
