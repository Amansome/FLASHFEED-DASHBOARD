'use client'
import { useEffect, useRef } from 'react'

interface Candle { time: string | number; open: number; high: number; low: number; close: number; volume?: number }
interface BollingerData { upper: Array<{ time: string; value: number }>; lower: Array<{ time: string; value: number }> }
interface LinePoint { time: number; value: number }
// Strategy indicator marker: an entry (up-arrow) or exit (down-arrow) at a given
// candle time. `price` is informational; the arrow rides above/below the bar.
export interface StrategyMarker { time: number; type: 'entry' | 'exit'; price: number; exit_reason?: string; corr?: number | null }

// News event marker: a dot above the bar at an article's publish time, colored
// by sentiment. Sourced from the enrich News feed and merged with strategy marks.
export interface NewsMarker { time: number; sentiment?: 'bullish' | 'bearish' | 'neutral' | null; headline?: string; count?: number }

interface Props {
  candles: Candle[]
  bollinger?: BollingerData
  // Optional overlays on independent secondary scales (their units differ from
  // price): smoothed message density (msgs/min) and sentiment score (−1..+1).
  // Undefined = not shown. Mirrors the research views' orange/green styling.
  densityOverlay?: LinePoint[]
  sentimentOverlay?: LinePoint[]
  // Strategy entry/exit arrows (lightweight-charts v5 series markers). Undefined
  // = indicator off. Up-arrow (green) at entries, down-arrow (red) at exits.
  strategyMarkers?: StrategyMarker[]
  // News dots share the candle marker array with strategy markers.
  newsMarkers?: NewsMarker[]
  // StockTwits watcher history, drawn on its own scale.
  watcherOverlay?: LinePoint[]
}

export function CandlestickChart({ candles, bollinger, densityOverlay, sentimentOverlay, strategyMarkers, newsMarkers, watcherOverlay }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<any>(null)

  useEffect(() => {
    if (!containerRef.current || candles.length === 0) return

    let disposed = false
    import('lightweight-charts').then(({ createChart, ColorType, CrosshairMode }) => {
      if (disposed || !containerRef.current) return

      // Clear previous chart
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }

      const chart = createChart(containerRef.current, {
        width: containerRef.current.clientWidth,
        height: containerRef.current.clientHeight,
        layout: {
          background: { type: ColorType.Solid, color: 'transparent' },
          textColor: '#94a3b8',
          fontSize: 11,
        },
        grid: {
          vertLines: { color: '#1e293b' },
          horzLines: { color: '#1e293b' },
        },
        crosshair: { mode: CrosshairMode.Normal },
        rightPriceScale: { borderColor: '#334155' },
        timeScale: { borderColor: '#334155', timeVisible: true },
      })
      chartRef.current = chart

      const candleSeries = chart.addCandlestickSeries({
        upColor: '#10b981',
        downColor: '#ef4444',
        borderUpColor: '#10b981',
        borderDownColor: '#ef4444',
        wickUpColor: '#10b981',
        wickDownColor: '#ef4444',
      })
      candleSeries.setData(candles as any)

      // Volume histogram — up/down-colored bars on their own bottom-anchored
      // scale so magnitude never distorts the price axis.
      const volumeData = candles
        .filter(c => (c.volume ?? 0) > 0)
        .map(c => ({
          time: c.time as any,
          value: c.volume ?? 0,
          color: c.close >= c.open ? 'rgba(16, 185, 129, 0.35)' : 'rgba(239, 68, 68, 0.32)',
        }))
      if (volumeData.length) {
        const volumeSeries = chart.addHistogramSeries({
          priceScaleId: 'volume',
          priceFormat: { type: 'volume' },
          priceLineVisible: false,
          lastValueVisible: false,
        })
        volumeSeries.setData(volumeData as any)
        chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.88, bottom: 0 } })
      }

      // Bollinger bands
      if (bollinger) {
        const upperSeries = chart.addLineSeries({
          color: 'rgba(139, 92, 246, 0.5)',
          lineWidth: 1,
          lineStyle: 2,
        })
        upperSeries.setData(bollinger.upper as any)

        const lowerSeries = chart.addLineSeries({
          color: 'rgba(139, 92, 246, 0.5)',
          lineWidth: 1,
          lineStyle: 2,
        })
        lowerSeries.setData(bollinger.lower as any)
      }

      // Density overlay (msgs/min, smoothed) — orange, lower band, own scale so
      // its magnitude never distorts the price axis.
      if (densityOverlay && densityOverlay.length) {
        const dens = chart.addLineSeries({
          color: '#FF9800', lineWidth: 3, priceScaleId: 'density',
          priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
        })
        dens.setData(densityOverlay as any)
        chart.priceScale('density').applyOptions({ scaleMargins: { top: 0.62, bottom: 0.08 } })
      }

      // Sentiment overlay (−1..+1, 15-min smoothed) — green, own scale.
      if (sentimentOverlay && sentimentOverlay.length) {
        const sent = chart.addLineSeries({
          color: '#4CAF50', lineWidth: 2, priceScaleId: 'sentiment',
          priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
        })
        sent.setData(sentimentOverlay as any)
        chart.priceScale('sentiment').applyOptions({ scaleMargins: { top: 0.05, bottom: 0.72 } })
      }

      if (watcherOverlay && watcherOverlay.length) {
        const watcher = chart.addLineSeries({
          color: '#60a5fa', lineWidth: 2, priceScaleId: 'watchers',
          priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false,
        })
        watcher.setData(watcherOverlay as any)
        chart.priceScale('watchers').applyOptions({ scaleMargins: { top: 0.18, bottom: 0.58 } })
      }

      // Strategy entry/exit arrows and news event dots share ONE candle series,
      // so they MUST be merged into a SINGLE setMarkers() call.
      const stratMk = (strategyMarkers ?? []).map(m => m.type === 'entry'
        ? { time: m.time as any, position: 'belowBar' as const, color: '#10b981', shape: 'arrowUp' as const, text: 'ENTRY' }
        : {
            time: m.time as any,
            position: 'aboveBar' as const,
            color: m.exit_reason === 'correlation_break' ? '#f59e0b' : '#ef4444',
            shape: 'arrowDown' as const,
            text: m.exit_reason === 'correlation_break' ? 'CORR EXIT' : 'EXIT',
          })
      const newsMk = (newsMarkers ?? []).map(n => ({
        time: n.time as any,
        position: 'aboveBar' as const,
        color: n.sentiment === 'bullish' ? '#10b981'
          : n.sentiment === 'bearish' ? '#ef4444' : '#94a3b8',
        shape: 'circle' as const,
        text: `${n.sentiment === 'bearish' ? 'BAD' : n.sentiment === 'bullish' ? 'GOOD' : 'NEWS'}${(n.count || 1) > 1 ? ` x${n.count}` : ''}`,
      }))
      const sessionMk = [
        { hhmm: '09:30', text: 'OPEN', color: '#fb7185', position: 'belowBar' as const },
        { hhmm: '16:00', text: 'CLOSE', color: '#a78bfa', position: 'aboveBar' as const },
      ].flatMap(({ hhmm, text, color, position }) => {
        const [h, m] = hhmm.split(':').map(Number)
        const hit = candles.find(c => {
          const t = Number(c.time)
          if (!Number.isFinite(t)) return false
          const d = new Date(t * 1000)
          return d.getUTCHours() === h && d.getUTCMinutes() === m
        })
        return hit ? [{ time: hit.time as any, position, color, shape: 'circle' as const, text }] : []
      })
      const allMarkers = [...sessionMk, ...stratMk, ...newsMk].sort((a, b) => (a.time as number) - (b.time as number))
      if (allMarkers.length) candleSeries.setMarkers(allMarkers as any)

      chart.timeScale().fitContent()

      // Resize observer
      const ro = new ResizeObserver(entries => {
        for (const entry of entries) {
          chart.applyOptions({ width: entry.contentRect.width })
        }
      })
      ro.observe(containerRef.current)

      return () => { ro.disconnect() }
    })

    return () => {
      disposed = true
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [candles, bollinger, densityOverlay, sentimentOverlay, strategyMarkers, newsMarkers, watcherOverlay])

  return <div ref={containerRef} className="w-full h-full" />
}
