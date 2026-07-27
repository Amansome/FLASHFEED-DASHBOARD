'use client'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'
import useSWR from 'swr'
import { clsx } from 'clsx'
import * as THREE from 'three'
import { useTickerDatalistOptions } from '@/lib/useTickerUniverse'

const fetcher = (url: string) => fetch(url).then(r => r.json())
const JOURNEY_PLAYBACK_SPEED_MULTIPLIER = 0.28125 // 0.5x the prior selected-journey playback speed.
const DISPLAY_PATH_SAMPLE_SECONDS = 10 * 60
const DISPLAY_PATH_MAX_POINTS = 44
const MAP_FRAME_MIN = new THREE.Vector3(-8, -6, -2)
const MAP_FRAME_MAX = new THREE.Vector3(8, 7, 8)

type DecisionMapRow = {
  ticker: string
  company?: string
  price?: number
  marketCap?: number
  rawMarketCap?: number
  marketCapBucket?: string
  sharesFloat?: number | null
  rawSharesFloat?: number | null
  floatBucket?: string
  floatShort?: number | null
  floatPercent?: number | null
  marketCapRelVolumeTarget?: number
  marketCapRelativeVolumeScore?: number
  dollarVolumeTarget?: number
  currentDollarVolume?: number
  liquidityScore?: number
  liquidityStatus?: 'excellent' | 'acceptable' | 'thin' | 'poor' | string
  liquidityDollarVolumeRatio?: number
  liquidityTargetDollarVolume?: number
  relativeVolumeBucket?: string
  priceChangePct: number
  regularPrice?: number
  regularChangePct?: number
  regularVolume?: number
  premarketPrice?: number
  premarketChangePct?: number
  premarketVolume?: number
  postmarketPrice?: number
  postmarketChangePct?: number
  postmarketVolume?: number
  activeSession?: 'premarket' | 'regular' | 'postmarket'
  relativeVolume: number
  currentVolume: number
  rollingVolume: number
  volumeAcceleration: number
  structuredNewsSentiment: number
  socialSentiment: number
  combinedSentiment: number
  articleCount: number
  socialCount: number
  catalystLabel?: string
  catalystFromLookback?: boolean
  catalystLookbackUsed?: string
  catalystAgeHours?: number | null
  structuredArticleCount?: number
  unstructuredArticleCount?: number
  quadrant: 'Q1' | 'Q2' | 'Q3' | 'Q4' | 'Neutral'
  alignmentStatus: string
  decisionState?: 'strong_bullish_candidate' | 'moderate_bullish_candidate' | 'aligned_bearish_candidate' | 'neutral_watchlist' | 'risky_uncertain' | 'weak_no_catalyst'
  supportLabel?: string
  activityScore: number
  convictionScore: number
  riskFlags: string[]
  reasons: string[]
  movementDrivers?: string[]
  movementSummary?: string
  latestNewsTitles: Array<{ title: string; source?: string; publishedAt?: number; sentiment?: number }>
  newsSources?: string[]
  structuredNewsSources?: string[]
  unstructuredNewsSources?: string[]
  socialPlatforms?: string[]
  rollingWindowHours?: number
  rollingWindowUsed?: string
  newsWindowUsed?: string
  socialWindowUsed?: string
  pathWindowUsed?: string
  sourceNewsWindowStart?: string
  sourceNewsWindowEnd?: string
  sourceSocialWindowStart?: string
  sourceSocialWindowEnd?: string
  socialMessageDensityPerHour?: number
  newsDensityPerHour?: number
  ticker_path?: Array<{ timestamp: number; time?: number; timestampUtc?: string; displayTime?: string; rollingWindowUsed?: string; sourceSocialWindowStart?: string; sourceSocialWindowEnd?: string; sourceNewsWindowStart?: string; sourceNewsWindowEnd?: string; chartBarTime?: string; x: number; y: number; z: number; combinedSentiment?: number; priceChangePct?: number; marketCapRelativeVolumeScore?: number | null; currentDollarVolume?: number | null; relativeVolume?: number | null; sentimentEstimated?: boolean; dataQuality?: string; missingFields?: string[]; timestampSource?: string; volumeTimeframe?: string; volumeTimeframeMinutes?: number; timeframeVolume?: number | null; expectedTimeframeVolume?: number | null; relativeVolumeBasis?: string; messageDensityPerHour?: number | null; socialDensityPerHour?: number | null; newsDensityPerHour?: number | null; evidenceEventCount?: number | null; evidenceWindowMinutes?: number | null }>
  path_points?: Array<{ timestamp: number; time?: number; timestampUtc?: string; displayTime?: string; rollingWindowUsed?: string; sourceSocialWindowStart?: string; sourceSocialWindowEnd?: string; sourceNewsWindowStart?: string; sourceNewsWindowEnd?: string; chartBarTime?: string; x: number; y: number; z: number; combinedSentiment?: number; priceChangePct?: number; marketCapRelativeVolumeScore?: number | null; currentDollarVolume?: number | null; relativeVolume?: number | null; sentimentEstimated?: boolean; dataQuality?: string; missingFields?: string[]; timestampSource?: string; volumeTimeframe?: string; volumeTimeframeMinutes?: number; timeframeVolume?: number | null; expectedTimeframeVolume?: number | null; relativeVolumeBasis?: string; messageDensityPerHour?: number | null; socialDensityPerHour?: number | null; newsDensityPerHour?: number | null; evidenceEventCount?: number | null; evidenceWindowMinutes?: number | null }>
  path_direction?: 'correct_direction' | 'wrong_direction' | 'neutral' | 'insufficient_history' | string
  path_direction_score?: number
  path_color?: 'blue' | 'red' | 'gray' | string
  path_explanation?: string
  path_points_count?: number
  path_window_minutes?: number
  path_market_date?: string | null
  path_missing_reason?: 'only_one_point_today' | 'no_intraday_history' | string
  path_quality?: 'full' | 'partial' | 'sparse' | 'minimal' | string
  path_coverage?: string | null
  path_data_source?: string
  path_sampling?: string
  path_window_raw_points_count?: number
  path_window_hours?: number
  path_window_used?: string
  path_window_start?: string
  path_window_end?: string
  path_volume_timeframe?: string
  path_volume_timeframe_minutes?: number
  path_volume_basis?: string
  path_static?: boolean
  time_lapse_available?: boolean
  screenerSource?: string
  screenerStatus?: string | null
  finvizAgeSeconds?: number | null
  lastUpdated?: string
}

type SortKey = 'convictionScore' | 'activityScore' | 'relativeVolume' | 'marketCapRelativeVolumeScore' | 'liquidityScore' | 'priceChangePct' | 'combinedSentiment' | 'ticker'
type PathPoint = NonNullable<DecisionMapRow['path_points']>[number]
type VolumePointLike = Partial<PathPoint> & Partial<DecisionMapRow>
type DecisionTooltip = { x: number; y: number; row: DecisionMapRow; point?: PathPoint; pointIndex?: number; compact?: boolean }
type MiniPriceDensityPoint = { time: number; price: number; density: number }
type MiniPriceDensity = {
  ticker: string
  rollingWindowMinutes: number
  points: MiniPriceDensityPoint[]
  priceSource?: 'chart' | 'path'
  densitySource?: 'chart' | 'path' | 'none'
  limitedPrice?: boolean
}

const VOLUME_TIMEFRAMES = [
  { value: '1m', label: '1m' },
  { value: '3m', label: '3m' },
  { value: '5m', label: '5m' },
  { value: '30m', label: '30m' },
  { value: '1h', label: '1h' },
]

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value))
}

function finiteMapCoordinate(value: unknown) {
  return value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value))
}

function uniquePlottableRows(rows: DecisionMapRow[]) {
  const seen = new Set<string>()
  return rows.filter(row => {
    const ticker = String(row.ticker || '').trim().toUpperCase()
    if (!ticker || seen.has(ticker)) return false
    if (!finiteMapCoordinate(row.combinedSentiment) || !finiteMapCoordinate(row.priceChangePct) || !finiteMapCoordinate(row.relativeVolume)) return false
    seen.add(ticker)
    return true
  })
}

function compact(n: number | null | undefined) {
  if (n == null || Number.isNaN(Number(n))) return '--'
  const value = Number(n)
  if (Math.abs(value) >= 1e9) return `${(value / 1e9).toFixed(1)}B`
  if (Math.abs(value) >= 1e6) return `${(value / 1e6).toFixed(1)}M`
  if (Math.abs(value) >= 1e3) return `${(value / 1e3).toFixed(1)}K`
  return value.toFixed(value < 10 ? 2 : 0)
}

function money(n: number | null | undefined) {
  if (n == null || Number.isNaN(Number(n))) return '--'
  return `$${compact(n)}`
}

function signedPct(value: number | null | undefined) {
  if (value == null || Number.isNaN(Number(value))) return '--'
  const n = Number(value)
  return `${n >= 0 ? '+' : ''}${n.toFixed(2)}%`
}

function numberLabel(value: number | null | undefined, digits = 2, suffix = '') {
  if (value == null || Number.isNaN(Number(value))) return '--'
  return `${Number(value).toFixed(digits)}${suffix}`
}

function timestampLabel(seconds?: number | null) {
  if (!seconds || Number.isNaN(Number(seconds))) return '--'
  return new Date(Number(seconds) * 1000).toLocaleString()
}

function compactTimestamp(seconds?: number | null) {
  if (!seconds || Number.isNaN(Number(seconds))) return '--'
  return new Date(Number(seconds) * 1000).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

function windowLabel(hours?: number | null) {
  if (hours == null || Number.isNaN(Number(hours))) return '--'
  const minutes = Math.round(Number(hours) * 60)
  if (minutes < 60) return `${minutes}m`
  if (minutes === 24 * 60) return '1d'
  if (minutes % 60 === 0) return `${minutes / 60}h`
  return `${Number(hours).toFixed(2)}h`
}

function shortDateTime(value?: string | null) {
  if (!value) return '--'
  const ms = Date.parse(value)
  if (!Number.isFinite(ms)) return value
  return new Date(ms).toLocaleString([], { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' })
}

function ageLabel(seconds?: number | null) {
  if (seconds == null || Number.isNaN(Number(seconds))) return '--'
  const s = Math.max(0, Number(seconds))
  if (s < 60) return 'now'
  if (s < 3600) return `${Math.floor(s / 60)}m`
  return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
}

function colorForDecisionState(state?: string) {
  if (state === 'strong_bullish_candidate') return 0x10b981
  if (state === 'moderate_bullish_candidate') return 0x38bdf8
  if (state === 'aligned_bearish_candidate') return 0xf87171
  if (state === 'risky_uncertain') return 0xf59e0b
  if (state === 'weak_no_catalyst') return 0x64748b
  return 0xa78bfa
}

function marketCapColor(bucket?: string) {
  const key = String(bucket || '').toLowerCase()
  if (key.includes('mega')) return 0xf8fafc
  if (key.includes('large')) return 0x93c5fd
  if (key.includes('mid')) return 0xc084fc
  if (key.includes('small')) return 0xfbbf24
  if (key.includes('micro')) return 0xfb7185
  return 0x94a3b8
}

function dotClass(state: string) {
  if (state === 'strong_bullish_candidate') return 'bg-emerald-400'
  if (state === 'moderate_bullish_candidate') return 'bg-sky-400'
  if (state === 'aligned_bearish_candidate') return 'bg-red-400'
  if (state === 'risky_uncertain') return 'bg-amber-400'
  if (state === 'weak_no_catalyst') return 'bg-slate-500'
  return 'bg-violet-400'
}

function labelForDecisionState(state?: string) {
  if (state === 'strong_bullish_candidate') return 'Strong bullish candidate'
  if (state === 'moderate_bullish_candidate') return 'Moderate bullish candidate'
  if (state === 'aligned_bearish_candidate') return 'Aligned bearish mover'
  if (state === 'risky_uncertain') return 'Risky/uncertain'
  if (state === 'weak_no_catalyst') return 'Weak/no catalyst'
  return 'Neutral/watchlist'
}

function labelForQuadrant(q: string) {
  if (q === 'Q1') return 'Sentiment/price aligned bullish'
  if (q === 'Q3') return 'Sentiment/price aligned bearish'
  if (q === 'Q2') return 'Divergence: news not matching price'
  if (q === 'Q4') return 'Divergence: news not moving market'
  return 'Neutral/mixed'
}

function pathQualityLabel(row?: DecisionMapRow | null) {
  if (!row) return 'Snapshot only'
  if (row.path_static && row.path_missing_reason) return 'Static: missing path data'
  if (row.path_static) return 'Static: low movement'
  if (row.path_quality === 'full') return 'Full journey'
  if (row.path_quality === 'partial') return 'Partial journey'
  if (row.path_quality === 'sparse') return 'Snapshot only'
  if (row.path_quality === 'minimal') return 'Snapshot only'
  return row.path_points_count && row.path_points_count > 1 ? 'Partial journey' : 'Snapshot only'
}

function pathQualityClass(row?: DecisionMapRow | null) {
  if (!row) return 'border-slate-600 bg-slate-900/70 text-slate-300'
  if (row.path_quality === 'full' && !row.path_static) return 'border-sky-400/50 bg-sky-500/10 text-sky-100'
  if (row.path_quality === 'partial' && !row.path_static) return 'border-cyan-400/40 bg-cyan-500/10 text-cyan-100'
  if (row.path_static || row.path_missing_reason) return 'border-amber-400/40 bg-amber-500/10 text-amber-100'
  return 'border-slate-600 bg-slate-900/70 text-slate-300'
}

function stateClass(state?: string) {
  if (state === 'strong_bullish_candidate') return 'border-emerald-400/40 bg-emerald-500/10 text-emerald-100'
  if (state === 'moderate_bullish_candidate') return 'border-sky-400/40 bg-sky-500/10 text-sky-100'
  if (state === 'aligned_bearish_candidate') return 'border-red-400/40 bg-red-500/10 text-red-100'
  if (state === 'risky_uncertain') return 'border-amber-400/40 bg-amber-500/10 text-amber-100'
  if (state === 'weak_no_catalyst') return 'border-slate-500/50 bg-slate-800/80 text-slate-200'
  return 'border-violet-400/30 bg-violet-500/10 text-violet-100'
}

function sourceList(row: DecisionMapRow) {
  const sources = [
    ...(row.structuredNewsSources || []),
    ...(row.newsSources || []),
    ...(row.socialPlatforms || []),
    ...(row.unstructuredNewsSources || []),
  ].filter(Boolean)
  return Array.from(new Set(sources)).slice(0, 4)
}

function firstPathPoint(row?: DecisionMapRow | null) {
  const path = row ? rowPath(row) : []
  return path[0] || null
}

function lastPathPoint(row?: DecisionMapRow | null) {
  const path = row ? rowPath(row) : []
  return path[path.length - 1] || null
}

function missingDataLabels(row: DecisionMapRow) {
  const latest = lastPathPoint(row)
  const labels = new Set<string>()
  if (row.liquidityScore == null || row.liquidityStatus == null) labels.add('missing liquidity')
  if (!row.catalystLabel && !row.latestNewsTitles?.length) labels.add('missing catalyst')
  if (!row.path_points_count || row.path_points_count < 2) labels.add('missing chart frames')
  if (row.currentVolume == null || Number(row.currentVolume) <= 0) labels.add('missing volume')
  if (row.price == null || Number(row.price) <= 0) labels.add('missing price')
  ;(latest?.missingFields || []).forEach(field => labels.add(`missing ${String(field).replace(/([A-Z])/g, ' $1').toLowerCase()}`))
  if (!labels.size) labels.add('complete core fields')
  return Array.from(labels).slice(0, 6)
}

function pathSourceLabel(value?: string | null) {
  if (!value) return 'source unavailable'
  return value.replaceAll('_', ' ')
}

function signedLogAxis(value: number, maxAbs: number, radius: number) {
  const n = Number(value || 0)
  const sign = n < 0 ? -1 : 1
  const magnitude = Math.min(maxAbs, Math.abs(n))
  return sign * (Math.log1p(magnitude) / Math.log1p(maxAbs)) * radius
}

function sentimentAxisValue(value: number) {
  return clamp(Number(value || 0), -1, 1) * 7
}

function priceAxisValue(value: number) {
  // Y-axis: log1p(|changePct|) / log1p(160) * 4.8 — maps 0%→0, 5%→1.2, 25%→2.8, 100%→4.3, 300%→4.8
  const raw = Math.abs(Number(value || 0))
  const sign = value < 0 ? -1 : 1
  return sign * (Math.log1p(raw) / Math.log1p(160)) * 4.8
}

function volumePressureAxisValue(point: VolumePointLike, row?: DecisionMapRow | null) {
  const pressure = volumePressurePlotScore(point, row)
  return clamp(((pressure - 50) / 50) * 5.2, -2.2, 7)
}

function pointPosition(row: DecisionMapRow) {
  return new THREE.Vector3(
    sentimentAxisValue(row.combinedSentiment),
    priceAxisValue(row.priceChangePct),
    volumePressureAxisValue(row, row),
  )
}

function pathPointVector(point: PathPoint, row: DecisionMapRow) {
  const sentiment = Number.isFinite(Number(point.combinedSentiment))
    ? Number(point.combinedSentiment)
    : Number(point.x || 0) / 7
  const hasRawPriceChange = Number.isFinite(Number(point.priceChangePct))
  const hasPressureInputs = (
    (point.relativeVolume != null && Number.isFinite(Number(point.relativeVolume))) ||
    (point.currentDollarVolume != null && Number.isFinite(Number(point.currentDollarVolume))) ||
    (point.timeframeVolume != null && Number.isFinite(Number(point.timeframeVolume)))
  )
  const fallbackY = Number.isFinite(Number(point.y)) ? Number(point.y) : 0
  const fallbackZ = Number.isFinite(Number(point.z)) ? Number(point.z) : volumePressureAxisValue(row, row)
  return new THREE.Vector3(
    sentimentAxisValue(sentiment),
    hasRawPriceChange ? priceAxisValue(Number(point.priceChangePct)) : fallbackY,
    hasPressureInputs ? volumePressureAxisValue(point, row) : fallbackZ,
  )
}

function marketCapBucketKey(row: DecisionMapRow) {
  const bucket = String(row.marketCapBucket || '').toLowerCase()
  if (bucket.includes('nano')) return 'nano'
  if (bucket.includes('micro')) return 'micro'
  if (bucket.includes('small')) return 'small'
  if (bucket.includes('mid')) return 'mid'
  if (bucket.includes('large')) return 'large'
  if (bucket.includes('mega')) return 'mega'

  const cap = Number(row.marketCap || row.rawMarketCap || 0)
  if (cap > 0 && cap < 50e6) return 'nano'
  if (cap > 0 && cap < 300e6) return 'micro'
  if (cap > 0 && cap < 2e9) return 'small'
  if (cap > 0 && cap < 10e9) return 'mid'
  if (cap > 0 && cap < 200e9) return 'large'
  if (cap >= 200e9) return 'mega'
  return 'unknown'
}

function marketCapPathSpan(row: DecisionMapRow) {
  const key = marketCapBucketKey(row)
  if (key === 'nano') return 8.6
  if (key === 'micro') return 8.0
  if (key === 'small') return 7.2
  if (key === 'mid') return 6.2
  if (key === 'large') return 5.4
  if (key === 'mega') return 4.8
  return 6.4
}

function bucketVolumeScale(row?: DecisionMapRow | null) {
  const key = row ? marketCapBucketKey(row) : 'unknown'
  if (key === 'mega') return { relTarget: 1.2, dollarTarget: 30_000_000, relCeiling: 18, dollarCeiling: 12 }
  if (key === 'large') return { relTarget: 1.5, dollarTarget: 15_000_000, relCeiling: 28, dollarCeiling: 18 }
  if (key === 'mid') return { relTarget: 1.8, dollarTarget: 8_000_000, relCeiling: 55, dollarCeiling: 26 }
  if (key === 'small') return { relTarget: 2.2, dollarTarget: 3_000_000, relCeiling: 120, dollarCeiling: 40 }
  if (key === 'micro') return { relTarget: 5, dollarTarget: 1_000_000, relCeiling: 650, dollarCeiling: 80 }
  if (key === 'nano') return { relTarget: 8, dollarTarget: 500_000, relCeiling: 1200, dollarCeiling: 120 }
  return { relTarget: 2, dollarTarget: 2_000_000, relCeiling: 90, dollarCeiling: 32 }
}

function volumePressurePlotScore(point: VolumePointLike, row?: DecisionMapRow | null) {
  const scale = bucketVolumeScale(row)
  const relVolume = Math.max(0, Number(point.relativeVolume ?? row?.relativeVolume ?? 0))
  const dollarVolume = Math.max(0, Number(point.currentDollarVolume ?? row?.currentDollarVolume ?? 0))
  const relRatio = relVolume / Math.max(0.001, scale.relTarget)
  const dollarRatio = dollarVolume / Math.max(1, scale.dollarTarget)
  const relPart = Math.log1p(relRatio) / Math.log1p(scale.relCeiling)
  const dollarPart = Math.log1p(dollarRatio) / Math.log1p(scale.dollarCeiling)
  return clamp((relPart * 0.74 + dollarPart * 0.26) * 100, 0, 100)
}

function autoZoomForMarketCap(row?: DecisionMapRow | null) {
  if (!row) return 1
  const key = marketCapBucketKey(row)
  if (key === 'nano') return 1.8
  if (key === 'micro') return 1.7
  if (key === 'small') return 1.55
  if (key === 'mid') return 1.35
  if (key === 'large') return 1.15
  if (key === 'mega') return 0.95
  return 1.25
}

function rowPath(row: DecisionMapRow): PathPoint[] {
  return ((row.path_points || row.ticker_path || []) as PathPoint[])
    .filter(point => Number.isFinite(point.x) && Number.isFinite(point.y) && Number.isFinite(point.z))
    .sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0))
}

function pathPointTime(point: PathPoint, fallbackIndex = 0) {
  const seconds = Number(point.timestamp || point.time || 0)
  return Number.isFinite(seconds) && seconds > 0 ? seconds : fallbackIndex * DISPLAY_PATH_SAMPLE_SECONDS
}

function hasDisplayTurningPoint(rawPath: PathPoint[], index: number, row: DecisionMapRow) {
  if (index <= 0 || index >= rawPath.length - 1) return true
  const previous = rawPath[index - 1]
  const current = rawPath[index]
  const next = rawPath[index + 1]
  const before = pathPointMovementScore(previous, current, row)
  const after = pathPointMovementScore(current, next, row)
  const priceMove = Math.abs((finiteValue(current.priceChangePct) ?? 0) - (finiteValue(previous.priceChangePct) ?? 0))
  const sentimentMove = Math.abs((finiteValue(current.combinedSentiment) ?? 0) - (finiteValue(previous.combinedSentiment) ?? 0))
  const pressureMove = Math.abs(volumePressurePlotScore(current, row) - volumePressurePlotScore(previous, row))
  const strongTurn = Math.sign(before) !== Math.sign(after) && Math.max(Math.abs(before), Math.abs(after)) >= 0.55
  return strongTurn || priceMove >= 3.5 || sentimentMove >= 0.18 || pressureMove >= 8
}

function displayPathPoints(row: DecisionMapRow): PathPoint[] {
  const raw = rowPath(row).slice(-120)
  if (raw.length <= 2) return raw

  const kept = new Map<number, PathPoint>()
  const keep = (index: number) => {
    if (index >= 0 && index < raw.length) kept.set(index, raw[index])
  }

  keep(0)
  keep(raw.length - 1)

  let lastSampleTime = pathPointTime(raw[0], 0)
  for (let i = 1; i < raw.length - 1; i += 1) {
    const currentTime = pathPointTime(raw[i], i)
    if (currentTime - lastSampleTime >= DISPLAY_PATH_SAMPLE_SECONDS) {
      keep(i)
      lastSampleTime = currentTime
    }
    if (hasDisplayTurningPoint(raw, i, row)) keep(i)
  }

  let sampled = Array.from(kept.entries()).sort((a, b) => a[0] - b[0])
  if (sampled.length > DISPLAY_PATH_MAX_POINTS) {
    const required = new Set<number>([0, raw.length - 1])
    sampled.forEach(([index]) => {
      if (hasDisplayTurningPoint(raw, index, row)) required.add(index)
    })
    const candidates = sampled.map(([index]) => index)
    const targetStride = Math.max(1, Math.ceil(candidates.length / DISPLAY_PATH_MAX_POINTS))
    sampled = sampled.filter(([index], order) => required.has(index) || order % targetStride === 0)
    if (!sampled.some(([index]) => index === raw.length - 1)) sampled.push([raw.length - 1, raw[raw.length - 1]])
    sampled.sort((a, b) => a[0] - b[0])
  }

  return sampled.map(([, point]) => point)
}

function resampleVectors(points: THREE.Vector3[], targetCount: number) {
  if (points.length < 2) return points.map(point => point.clone())
  const keyframes: THREE.Vector3[] = []
  for (const point of points) {
    const last = keyframes[keyframes.length - 1]
    if (!last || last.distanceTo(point) > 0.0008) keyframes.push(point)
  }
  if (keyframes.length < 2) return points.map(point => point.clone())

  const distances = [0]
  for (let i = 1; i < keyframes.length; i += 1) {
    distances.push(distances[i - 1] + keyframes[i - 1].distanceTo(keyframes[i]))
  }
  const totalDistance = distances[distances.length - 1]
  if (totalDistance <= 0.001) return points.map(point => point.clone())

  const count = Math.max(points.length, targetCount)
  const out: THREE.Vector3[] = []
  let segment = 0
  for (let i = 0; i < count; i += 1) {
    const travel = (i / Math.max(1, count - 1)) * totalDistance
    while (segment < distances.length - 2 && travel > distances[segment + 1]) segment += 1
    const start = keyframes[segment]
    const end = keyframes[Math.min(keyframes.length - 1, segment + 1)]
    const segmentDistance = Math.max(0.001, distances[segment + 1] - distances[segment])
    const t = clamp((travel - distances[segment]) / segmentDistance, 0, 1)
    const easedT = t * t * (3 - 2 * t)
    out.push(start.clone().lerp(end, easedT))
  }
  return out
}

function displayPathVectors(row: DecisionMapRow, amplify: boolean) {
  const path = displayPathPoints(row)
  const raw = path.map(point => pathPointVector(point, row))
  if (!amplify || raw.length < 2) return raw
  const box = new THREE.Box3().setFromPoints(raw)
  const spans = {
    x: box.max.x - box.min.x,
    y: box.max.y - box.min.y,
    z: box.max.z - box.min.z,
  }
  const rail = {
    x0: MAP_FRAME_MIN.x + 0.72,
    x1: MAP_FRAME_MAX.x - 0.5,
    y0: MAP_FRAME_MIN.y + 0.65,
    y1: MAP_FRAME_MAX.y - 0.55,
    z0: MAP_FRAME_MIN.z + 0.72,
    z1: MAP_FRAME_MAX.z - 0.55,
  }
  const spread = marketCapPathSpan(row)
  const normalize = (value: number, min: number, span: number, fallback: number) => {
    if (span <= 0.08) return fallback
    return clamp((value - min) / span, 0, 1)
  }
  const lerp = (a: number, b: number, t: number) => a + (b - a) * clamp(t, 0, 1)
  // Visual-only inspection lens: a selected ticker is fit onto the outside rails
  // of the cube so its journey is large enough to inspect point-by-point.
  return raw.map((point, index) => {
    const progress = raw.length > 1 ? index / (raw.length - 1) : 1
    const xNorm = normalize(point.x, box.min.x, spans.x, progress)
    const yNorm = normalize(point.y, box.min.y, spans.y, 0.44 + (progress - 0.5) * 0.2)
    const zNorm = normalize(point.z, box.min.z, spans.z, 0.2 + progress * 0.72)
    const timeShear = raw.length >= 6 ? (progress - 0.5) * Math.min(1.9, spread * 0.22) : 0
    const arcLift = raw.length >= 6 ? Math.sin(progress * Math.PI) * Math.min(1.65, spread * 0.2) : 0
    return new THREE.Vector3(
      lerp(rail.x0, rail.x1, xNorm) + timeShear,
      lerp(rail.y0, rail.y1, yNorm),
      lerp(rail.z0, rail.z1, zNorm) + arcLift,
    )
  })
}

function pathFrame(row?: DecisionMapRow | null) {
  if (!row) return { center: new THREE.Vector3(0, 0, 3), span: 16 }
  const points = displayPathVectors(row, true)
  if (points.length < 2) return { center: visualPointPosition(row, true), span: 8 }
  const box = new THREE.Box3().setFromPoints(points)
  const center = box.getCenter(new THREE.Vector3())
  const size = box.getSize(new THREE.Vector3())
  const span = Math.max(size.x, size.y, size.z, 4)
  return { center, span }
}

function journeyMovementSpan(row: DecisionMapRow) {
  const path = displayPathVectors(row, true)
  if (path.length < 2) return 0
  const box = new THREE.Box3().setFromPoints(path)
  return Math.max(box.max.x - box.min.x, box.max.y - box.min.y, box.max.z - box.min.z)
}

function interpolatedPathPosition(row: DecisionMapRow, progress: number | null | undefined, amplifyJourney: boolean) {
  const path = displayPathVectors(row, amplifyJourney)
  if (!amplifyJourney || !path.length) return pointPosition(row)
  if (path.length === 1) return path[0].clone()
  const rawProgress = progress == null ? path.length - 1 : progress
  const bounded = clamp(rawProgress, 0, path.length - 1)
  const index = Math.floor(bounded)
  const next = Math.min(path.length - 1, index + 1)
  const localT = bounded - index
  const easedT = localT * localT * (3 - 2 * localT)
  return path[index].clone().lerp(path[next], easedT)
}

function visibleTrailPoints(row: DecisionMapRow, progress: number | null | undefined, amplifyJourney: boolean) {
  const path = displayPathVectors(row, amplifyJourney)
  if (!amplifyJourney || progress == null || path.length < 2) return path
  const bounded = clamp(progress, 0, path.length - 1)
  const index = Math.floor(bounded)
  const points = path.slice(0, Math.min(path.length, index + 1))
  const current = interpolatedPathPosition(row, progress, amplifyJourney)
  if (!points.length || points[points.length - 1].distanceTo(current) > 0.001) points.push(current)
  return points
}

function visualPointPosition(row: DecisionMapRow, amplifyJourney: boolean) {
  const path = displayPathVectors(row, amplifyJourney)
  if (amplifyJourney && path.length) return path[path.length - 1].clone()
  return pointPosition(row)
}

function pathColorValue(color?: string) {
  if (color === 'blue') return 0x38bdf8
  if (color === 'red') return 0xf87171
  return 0x94a3b8
}

function finiteValue(value: unknown): number | null {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function pathPointMovementScore(previous: PathPoint | null | undefined, current: PathPoint | null | undefined, row: DecisionMapRow) {
  if (!previous || !current) return 0
  const previousPrice = finiteValue(previous.priceChangePct) ?? finiteValue(previous.y) ?? row.priceChangePct ?? 0
  const currentPrice = finiteValue(current.priceChangePct) ?? finiteValue(current.y) ?? row.priceChangePct ?? 0
  const previousSentiment = finiteValue(previous.combinedSentiment) ?? (finiteValue(previous.x) ?? 0) / 7
  const currentSentiment = finiteValue(current.combinedSentiment) ?? (finiteValue(current.x) ?? 0) / 7
  const previousRelVolume = Math.log1p(Math.max(0, finiteValue(previous.relativeVolume) ?? row.relativeVolume ?? 0))
  const currentRelVolume = Math.log1p(Math.max(0, finiteValue(current.relativeVolume) ?? row.relativeVolume ?? 0))
  const previousDollarVolume = Math.log1p(Math.max(0, finiteValue(previous.currentDollarVolume) ?? row.currentDollarVolume ?? 0))
  const currentDollarVolume = Math.log1p(Math.max(0, finiteValue(current.currentDollarVolume) ?? row.currentDollarVolume ?? 0))
  return (currentPrice - previousPrice) * 0.72 +
    (currentSentiment - previousSentiment) * 9 +
    (currentRelVolume - previousRelVolume) * 0.55 +
    (currentDollarVolume - previousDollarVolume) * 0.18
}

function segmentColorValue(score: number) {
  if (score > 0.08) return 0x22c55e
  if (score < -0.08) return 0xef4444
  return 0x94a3b8
}

function visibleTrailSegments(row: DecisionMapRow, points: THREE.Vector3[]) {
  const rawPath = displayPathPoints(row)
  if (points.length < 2 || rawPath.length < 2) return []
  const segmentCount = points.length - 1
  return Array.from({ length: segmentCount }, (_, index) => {
    const rawIndex = clamp(Math.round((index / Math.max(1, segmentCount - 1)) * (rawPath.length - 2)), 0, rawPath.length - 2)
    return {
      start: points[index],
      end: points[index + 1],
      color: segmentColorValue(pathPointMovementScore(rawPath[rawIndex], rawPath[rawIndex + 1], row)),
    }
  })
}

function isMajorPathPoint(rawPath: PathPoint[], index: number, row: DecisionMapRow) {
  if (!rawPath.length) return false
  if (index <= 0 || index >= rawPath.length - 1) return true
  const stride = Math.max(1, Math.floor(rawPath.length / 8))
  if (index % stride === 0) return true
  const previous = rawPath[index - 1]
  const current = rawPath[index]
  const next = rawPath[index + 1]
  const before = pathPointMovementScore(previous, current, row)
  const after = pathPointMovementScore(current, next, row)
  const priceMove = Math.abs((finiteValue(current.priceChangePct) ?? 0) - (finiteValue(previous.priceChangePct) ?? 0))
  const sentimentMove = Math.abs((finiteValue(current.combinedSentiment) ?? 0) - (finiteValue(previous.combinedSentiment) ?? 0))
  return Math.sign(before) !== Math.sign(after) || priceMove >= 2.5 || sentimentMove >= 0.12
}

function lastSegmentColor(row: DecisionMapRow, fallbackColor: number) {
  const path = displayPathPoints(row)
  if (path.length < 2) return fallbackColor
  return segmentColorValue(pathPointMovementScore(path[path.length - 2], path[path.length - 1], row))
}

function liquidityClass(status?: string) {
  if (status === 'excellent') return 'text-emerald-300'
  if (status === 'acceptable') return 'text-sky-300'
  if (status === 'thin') return 'text-amber-300'
  if (status === 'poor') return 'text-red-300'
  return 'text-slate-300'
}

function bubbleSize(row: DecisionMapRow) {
  const pressurePart = volumePressurePlotScore(row, row) / 420
  const volumePart = Math.sqrt(Math.max(1, Number(row.rollingVolume || row.currentVolume || 1))) / 76000
  const accelerationPart = Math.log1p(Math.max(0, Number(row.volumeAcceleration || 0))) / 34
  return Math.max(0.09, Math.min(0.26, 0.09 + pressurePart + volumePart + accelerationPart))
}

function makeLabelSprite(text: string) {
  const canvas = document.createElement('canvas')
  canvas.width = 160
  canvas.height = 48
  const ctx = canvas.getContext('2d')
  if (ctx) {
    ctx.clearRect(0, 0, canvas.width, canvas.height)
    ctx.font = '700 22px ui-monospace, SFMono-Regular, Menlo, monospace'
    ctx.textAlign = 'center'
    ctx.textBaseline = 'middle'
    ctx.fillStyle = 'rgba(15, 23, 42, 0.82)'
    ctx.fillRect(20, 7, 120, 34)
    ctx.strokeStyle = 'rgba(56, 189, 248, 0.7)'
    ctx.strokeRect(20.5, 7.5, 119, 33)
    ctx.fillStyle = '#e2e8f0'
    ctx.fillText(text, 80, 25)
  }
  const texture = new THREE.CanvasTexture(canvas)
  const material = new THREE.SpriteMaterial({ map: texture, transparent: true, depthTest: false })
  const sprite = new THREE.Sprite(material)
  sprite.scale.set(1.7, 0.5, 1)
  return sprite
}

function disposeObject(object: THREE.Object3D) {
  object.traverse(child => {
    const mesh = child as THREE.Mesh
    if (mesh.geometry) mesh.geometry.dispose()
    const material = mesh.material as THREE.Material | THREE.Material[] | undefined
    if (Array.isArray(material)) material.forEach(item => item.dispose())
    else material?.dispose()
  })
}

function StatusPill({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <span className={clsx('inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-medium', className)}>
      {children}
    </span>
  )
}

function MetricCell({ label, value, tone }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className="rounded border border-slate-700/70 bg-slate-900/70 px-2 py-1.5">
      <div className="text-[9px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={clsx('mt-0.5 min-w-0 font-mono text-[11px] text-slate-100', tone)}>{value}</div>
    </div>
  )
}

function MiniPriceDensityChart({ data }: { data?: MiniPriceDensity | null }) {
  const points = (data?.points || []).filter(point =>
    Number.isFinite(point.time) && Number.isFinite(point.price) && Number.isFinite(point.density),
  )
  if (points.length < 2) return null
  const width = 256
  const height = 96
  const pad = { left: 8, right: 8, top: 10, bottom: 18 }
  const x0 = points[0].time
  const x1 = points[points.length - 1].time
  const prices = points.map(point => point.price)
  const densities = points.map(point => point.density)
  const pMin = Math.min(...prices)
  const pMax = Math.max(...prices)
  const dMax = Math.max(1, ...densities)
  const sx = (time: number) => pad.left + ((time - x0) / Math.max(1, x1 - x0)) * (width - pad.left - pad.right)
  const syPrice = (price: number) => pad.top + (1 - ((price - pMin) / Math.max(0.0001, pMax - pMin))) * (height - pad.top - pad.bottom)
  const syDensity = (density: number) => pad.top + (1 - (density / dMax)) * (height - pad.top - pad.bottom)
  const pricePath = points.map((point, index) => `${index === 0 ? 'M' : 'L'}${sx(point.time).toFixed(1)},${syPrice(point.price).toFixed(1)}`).join(' ')
  const densityPath = points.map((point, index) => `${index === 0 ? 'M' : 'L'}${sx(point.time).toFixed(1)},${syDensity(point.density).toFixed(1)}`).join(' ')
  const firstLabel = compactTimestamp(points[0].time)
  const lastLabel = compactTimestamp(points[points.length - 1].time)
  return (
    <div className="rounded border border-slate-700/70 bg-slate-900/70 px-2 py-1.5">
      <div className="mb-1 flex items-center justify-between gap-2 text-[10px]">
        <span className="font-semibold uppercase tracking-wide text-sky-300">Price + Density</span>
        <span className="font-mono text-slate-400">{data?.rollingWindowMinutes || '--'}m</span>
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="h-24 w-full" role="img" aria-label={`${data?.ticker || ''} mini price and density chart`}>
        <rect x="0" y="0" width={width} height={height} fill="rgba(15,23,42,0.68)" />
        {[0.25, 0.5, 0.75].map(t => (
          <line key={t} x1={pad.left} x2={width - pad.right} y1={pad.top + t * (height - pad.top - pad.bottom)} y2={pad.top + t * (height - pad.top - pad.bottom)} stroke="rgba(51,65,85,0.55)" strokeWidth="1" />
        ))}
        {points.map((point, index) => (
          <line
            key={`${point.time}-${index}`}
            x1={sx(point.time)}
            x2={sx(point.time)}
            y1={height - pad.bottom}
            y2={Math.max(pad.top, syDensity(point.density))}
            stroke="rgba(251,146,60,0.28)"
            strokeWidth="1"
          />
        ))}
        <path d={densityPath} fill="none" stroke="#f97316" strokeWidth="1.8" strokeLinejoin="round" strokeLinecap="round" />
        <path d={pricePath} fill="none" stroke="#38bdf8" strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
        <text x={pad.left} y={height - 4} fill="#94a3b8" fontSize="9" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace">{firstLabel}</text>
        <text x={width - pad.right} y={height - 4} fill="#94a3b8" fontSize="9" fontFamily="ui-monospace, SFMono-Regular, Menlo, monospace" textAnchor="end">{lastLabel}</text>
      </svg>
    </div>
  )
}

function clockLabelFromSec(timeSec: number) {
  const d = new Date(timeSec * 1000)
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mm = String(d.getUTCMinutes()).padStart(2, '0')
  return `${hh}:${mm}`
}

function trailingMessageCounts(values: number[], windowMinutes: number) {
  const out: number[] = []
  const window = Math.max(1, Math.round(windowMinutes || 1))
  let sum = 0
  for (let i = 0; i < values.length; i += 1) {
    sum += Number(values[i] || 0)
    if (i - window >= 0) sum -= Number(values[i - window] || 0)
    out.push(Number(sum.toFixed(4)))
  }
  return out
}

function pathDensityAt(path: PathPoint[], timeSec: number, rollingWindowMinutes: number) {
  if (!path.length) return null
  let bestPast: PathPoint | null = null
  let nearest: PathPoint | null = null
  let nearestDistance = Infinity
  for (const point of path) {
    const pointTime = Number(point.timestamp || point.time || 0)
    if (!Number.isFinite(pointTime) || pointTime <= 0) continue
    const distance = Math.abs(pointTime - timeSec)
    if (distance < nearestDistance) {
      nearestDistance = distance
      nearest = point
    }
    if (pointTime <= timeSec) bestPast = point
  }
  const source = bestPast || nearest
  if (!source) return null
  const perHour = Number(source.socialDensityPerHour ?? source.messageDensityPerHour ?? 0)
  if (!Number.isFinite(perHour) || perHour <= 0) return null
  return Number((perHour * (Math.max(1, rollingWindowMinutes) / 60)).toFixed(3))
}

function miniPriceDensityFromPath(row: DecisionMapRow | null | undefined, ticker: string, rollingWindowMinutes: number): MiniPriceDensity | null {
  if (!row || !ticker) return null
  const path = rowPath(row)
  if (path.length < 2) return null
  const last = path[path.length - 1]
  const latestPrice = finiteValue(row.price) ?? finiteValue(row.regularPrice) ?? finiteValue(row.postmarketPrice) ?? finiteValue(row.premarketPrice)
  const latestMove = finiteValue(last.priceChangePct) ?? 0
  const basePrice = latestPrice != null && Math.abs(1 + latestMove / 100) > 0.0001
    ? latestPrice / (1 + latestMove / 100)
    : latestPrice
  if (basePrice == null || basePrice <= 0) return null
  const points = path
    .map((point, index) => {
      const move = finiteValue(point.priceChangePct) ?? 0
      const densityPerHour = Number(point.socialDensityPerHour ?? point.messageDensityPerHour ?? 0)
      return {
        time: pathPointTime(point, index),
        price: Number((basePrice * (1 + move / 100)).toFixed(4)),
        density: Number.isFinite(densityPerHour) && densityPerHour > 0
          ? Number((densityPerHour * (Math.max(1, rollingWindowMinutes) / 60)).toFixed(3))
          : 0,
      }
    })
    .filter(point => Number.isFinite(point.time) && Number.isFinite(point.price) && Number.isFinite(point.density))
  if (points.length < 2) return null
  const hasDensity = points.some(point => point.density > 0)
  return {
    ticker: ticker.toUpperCase(),
    rollingWindowMinutes,
    points: points.slice(-DISPLAY_PATH_MAX_POINTS),
    priceSource: 'path',
    densitySource: hasDensity ? 'path' : 'none',
    limitedPrice: true,
  }
}

function miniPriceDensityFromChartPayload(payload: any, ticker: string, rollingWindowMinutes: number, row?: DecisionMapRow | null): MiniPriceDensity | null {
  const candles = Array.isArray(payload?.candles) ? payload.candles : []
  if (!ticker || candles.length < 2) return miniPriceDensityFromPath(row, ticker, rollingWindowMinutes)
  const socialRows = Array.isArray(payload?.social_series)
    ? payload.social_series
    : Array.isArray(payload?.social_density)
      ? payload.social_density
      : []
  const densityByTime = new Map<number, number>()
  const densityByLabel = new Map<string, number>()
  for (const row of socialRows) {
    const time = Number(row?.time ?? row?.timestamp ?? row?.t)
    const value = Number(row?.count ?? row?.message_count ?? row?.value ?? row?.density ?? 0)
    if (Number.isFinite(time) && Number.isFinite(value)) {
      densityByTime.set(time, value)
      const label = row?.label || row?.time_label || clockLabelFromSec(time)
      if (label) densityByLabel.set(String(label), value)
    }
  }
  const candlePoints = candles
    .map((candle: any) => ({ time: Number(candle?.time), price: Number(candle?.close) }))
    .filter((point: { time: number; price: number }) => Number.isFinite(point.time) && Number.isFinite(point.price))
  if (candlePoints.length < 2) return null

  const rawCounts = candlePoints.map(point => {
    if (densityByTime.has(point.time)) return Number(densityByTime.get(point.time) || 0)
    return Number(densityByLabel.get(clockLabelFromSec(point.time)) || 0)
  })
  const chartRollingCounts = trailingMessageCounts(rawCounts, rollingWindowMinutes)
  const path = row ? rowPath(row) : []
  const pathHasDensity = path.some(point => Number(point.socialDensityPerHour ?? point.messageDensityPerHour ?? 0) > 0)
  const chartMessageCount = rawCounts.reduce((sum, value) => sum + Number(value || 0), 0)
  const usePathDensity = pathHasDensity && (
    chartMessageCount <= 0 ||
    (Number(row?.socialCount || 0) > 0 && chartMessageCount < Number(row?.socialCount || 0) * 0.35)
  )

  const points = candlePoints
    .map((point, index) => ({
      ...point,
      density: usePathDensity
        ? Number(pathDensityAt(path, point.time, rollingWindowMinutes) || 0)
        : Number(chartRollingCounts[index] || 0),
    }))
    .slice(-180)
    .filter(point => Number.isFinite(point.time) && Number.isFinite(point.price) && Number.isFinite(point.density))
  if (points.length < 2) return null
  const hasDensity = points.some(point => point.density > 0)
  return {
    ticker: ticker.toUpperCase(),
    rollingWindowMinutes,
    points,
    priceSource: 'chart',
    densitySource: hasDensity ? (usePathDensity ? 'path' : 'chart') : 'none',
    limitedPrice: candlePoints.length < Math.min(90, Math.max(20, rollingWindowMinutes * 0.35)),
  }
}

function DetailRow({ label, value, tone }: { label: string; value: ReactNode; tone?: string }) {
  return (
    <div className="flex min-w-0 items-start justify-between gap-3 border-b border-slate-800/70 py-1 last:border-b-0">
      <span className="shrink-0 text-[10px] uppercase tracking-wide text-slate-500">{label}</span>
      <span className={clsx('min-w-0 text-right text-[11px] text-slate-200', tone)}>{value}</span>
    </div>
  )
}

function TooltipSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded border border-slate-700/70 bg-slate-950/60 px-2 py-1.5">
      <div className="mb-1 text-[10px] font-semibold uppercase tracking-wide text-sky-300">{title}</div>
      {children}
    </div>
  )
}

function ThreeDecisionMap({
  rows,
  zoom,
  resetKey,
  isLoading,
  selectedTicker,
  playbackProgress,
  miniPriceDensity,
  activeMiniTicker,
  isMiniChartLoading,
  onSelectTicker,
  onHoverTicker,
}: {
  rows: DecisionMapRow[]
  zoom: number
  resetKey: number
  isLoading: boolean
  selectedTicker?: string
  playbackProgress?: number | null
  miniPriceDensity?: MiniPriceDensity | null
  activeMiniTicker?: string
  isMiniChartLoading?: boolean
  onSelectTicker?: (ticker: string) => void
  onHoverTicker?: (ticker: string) => void
}) {
  const hostRef = useRef<HTMLDivElement | null>(null)
  const stateRef = useRef({ theta: -0.7, phi: 1.12, radius: 18, dragging: false, x: 0, y: 0 })
  const selectedTickerRef = useRef(selectedTicker || '')
  const playbackRef = useRef<{ ticker?: string; progress: number | null }>({ ticker: selectedTicker, progress: playbackProgress ?? null })
  const trailApiRef = useRef<{ show: (ticker: string) => void; clear: () => void } | null>(null)
  const [tooltip, setTooltip] = useState<DecisionTooltip | null>(null)

  useEffect(() => {
    selectedTickerRef.current = selectedTicker || ''
    playbackRef.current = { ticker: selectedTicker, progress: playbackProgress ?? null }
    if (selectedTicker) trailApiRef.current?.show(selectedTicker)
    else trailApiRef.current?.clear()
  }, [selectedTicker, playbackProgress])

  useEffect(() => {
    const host = hostRef.current
    if (!host) return

    const scene = new THREE.Scene()
    scene.background = new THREE.Color(0x0f172a)
    const camera = new THREE.PerspectiveCamera(48, 1, 0.1, 1000)
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false })
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2))
    renderer.setSize(host.clientWidth, host.clientHeight)
    host.appendChild(renderer.domElement)

    const group = new THREE.Group()
    scene.add(group)
    scene.add(new THREE.AmbientLight(0xffffff, 0.72))
    const light = new THREE.DirectionalLight(0xffffff, 1.2)
    light.position.set(8, 12, 8)
    scene.add(light)

    const frameBox = new THREE.Box3(MAP_FRAME_MIN, MAP_FRAME_MAX)
    const frameHelper = new THREE.Box3Helper(frameBox, 0x334155)
    group.add(frameHelper)
    const backGrid = new THREE.GridHelper(16, 16, 0x334155, 0x1e293b)
    backGrid.position.set(0, 0.5, MAP_FRAME_MAX.z)
    backGrid.rotation.x = Math.PI / 2
    group.add(backGrid)
    const floorGrid = new THREE.GridHelper(16, 16, 0x334155, 0x1e293b)
    floorGrid.position.set(0, MAP_FRAME_MIN.y, 3)
    group.add(floorGrid)
    const addAxisLine = (start: THREE.Vector3, end: THREE.Vector3, color: number) => {
      const geometry = new THREE.BufferGeometry().setFromPoints([start, end])
      const line = new THREE.Line(geometry, new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.92 }))
      group.add(line)
    }
    addAxisLine(new THREE.Vector3(MAP_FRAME_MIN.x, MAP_FRAME_MIN.y, MAP_FRAME_MIN.z), new THREE.Vector3(MAP_FRAME_MAX.x, MAP_FRAME_MIN.y, MAP_FRAME_MIN.z), 0x0ea5e9)
    addAxisLine(new THREE.Vector3(MAP_FRAME_MIN.x, MAP_FRAME_MIN.y, MAP_FRAME_MIN.z), new THREE.Vector3(MAP_FRAME_MIN.x, MAP_FRAME_MAX.y, MAP_FRAME_MIN.z), 0x84cc16)
    addAxisLine(new THREE.Vector3(MAP_FRAME_MIN.x, MAP_FRAME_MIN.y, MAP_FRAME_MIN.z), new THREE.Vector3(MAP_FRAME_MIN.x, MAP_FRAME_MIN.y, MAP_FRAME_MAX.z), 0xf59e0b)
    const xLabel = makeLabelSprite('Sentiment')
    xLabel.position.set(MAP_FRAME_MAX.x + 0.5, MAP_FRAME_MIN.y - 0.2, MAP_FRAME_MIN.z)
    group.add(xLabel)
    const yLabel = makeLabelSprite('Move %')
    yLabel.position.set(MAP_FRAME_MIN.x - 0.8, MAP_FRAME_MAX.y + 0.15, MAP_FRAME_MIN.z)
    group.add(yLabel)
    const zLabel = makeLabelSprite('Cap Press')
    zLabel.position.set(MAP_FRAME_MIN.x - 0.8, MAP_FRAME_MIN.y - 0.2, MAP_FRAME_MAX.z + 0.35)
    group.add(zLabel)

    const sphereGeometry = new THREE.SphereGeometry(1, 24, 16)
    const haloGeometry = new THREE.TorusGeometry(1.22, 0.045, 6, 24)
    const rocketNoseGeometry = new THREE.ConeGeometry(0.22, 0.7, 22)
    const rocketPlumeGeometry = new THREE.ConeGeometry(0.18, 0.52, 18)
    const meshes: THREE.Mesh[] = []
    const trailGroup = new THREE.Group()
    const trailHitObjects: THREE.Object3D[] = []
    group.add(trailGroup)
    let hoveredMesh: THREE.Mesh | null = null
    let hoveredTicker = ''
    let pointerDownX = 0
    let pointerDownY = 0
    let didDrag = false
    let cameraTarget = new THREE.Vector3(0, 0, 3)
    const findRowForTicker = (ticker?: string) => rows.find(row => row.ticker === ticker)
    const frameTickerPath = (ticker?: string) => {
      const row = findRowForTicker(ticker)
      if (!row) {
        cameraTarget = new THREE.Vector3(0, 0, 3)
        stateRef.current.radius = 18
        updateCamera()
        return
      }
      const frame = pathFrame(row)
      cameraTarget = frame.center
      stateRef.current.radius = clamp(frame.span * 1.12 + 5.2, 8.5, 24)
      updateCamera()
    }
    renderer.domElement.style.touchAction = 'none'
    renderer.domElement.style.cursor = 'grab'
    const setClampedTooltip = (clientX: number, clientY: number, row: DecisionMapRow, extra: Partial<DecisionTooltip> = {}) => {
      const rect = renderer.domElement.getBoundingClientRect()
      const compactTooltip = Boolean(extra.compact)
      const width = compactTooltip ? Math.min(280, Math.max(240, rect.width - 24)) : Math.min(430, Math.max(320, rect.width - 24))
      const height = compactTooltip ? Math.min(190, Math.max(150, rect.height - 24)) : Math.min(430, Math.max(300, rect.height - 24))
      const x = clamp(clientX - rect.left + 14, 8, Math.max(8, rect.width - width - 8))
      const y = clamp(clientY - rect.top + 14, 8, Math.max(8, rect.height - height - 8))
      setTooltip({ x, y, row, ...extra })
    }
    const clearTrailChildren = () => {
      trailHitObjects.length = 0
      while (trailGroup.children.length) {
        const child = trailGroup.children.pop()
        if (child) disposeObject(child)
      }
    }
    const clearTrail = () => {
      clearTrailChildren()
      if (hoveredMesh) {
        hoveredMesh.scale.setScalar(hoveredMesh.userData.baseSize || 1)
        const material = hoveredMesh.material as THREE.MeshStandardMaterial
        material.emissiveIntensity = 0.11
      }
      hoveredMesh = null
      hoveredTicker = ''
    }
    const showTrail = (row: DecisionMapRow, mesh: THREE.Mesh) => {
      if (hoveredMesh && hoveredMesh !== mesh) {
        hoveredMesh.scale.setScalar(hoveredMesh.userData.baseSize || 1)
        const previousMaterial = hoveredMesh.material as THREE.MeshStandardMaterial
        previousMaterial.emissiveIntensity = 0.11
      }
      clearTrailChildren()
      hoveredTicker = row.ticker
      hoveredMesh = mesh
      mesh.scale.setScalar((mesh.userData.baseSize || 1) * 1.55)
      const material = mesh.material as THREE.MeshStandardMaterial
      material.emissiveIntensity = 0.42
      const currentSelectedTicker = selectedTickerRef.current
      const amplify = Boolean(currentSelectedTicker && row.ticker === currentSelectedTicker)
      const progress = amplify ? playbackRef.current.progress : null
      const points = visibleTrailPoints(row, progress, amplify)
      if (points.length < 2) return
      const rawPath = displayPathPoints(row)
      const color = pathColorValue(row.path_color)
      const segments = visibleTrailSegments(row, points)
      const playbackMode = amplify && progress != null
      const activeSegmentIndex = Math.max(0, segments.length - 1)
      const activeSegmentColor = segments[activeSegmentIndex]?.color ?? lastSegmentColor(row, color)
      segments.forEach((segment, index) => {
        const geometry = new THREE.BufferGeometry().setFromPoints([segment.start, segment.end])
        const line = new THREE.Line(geometry, new THREE.LineBasicMaterial({
          color: segment.color,
          transparent: true,
          opacity: playbackMode ? (index === activeSegmentIndex ? 1 : 0.7) : amplify ? 0.86 : 0.76,
        }))
        trailGroup.add(line)
      })
      const dotGeometry = new THREE.SphereGeometry(amplify ? 0.18 : 0.065, 18, 12)
      points.forEach((point, index) => {
        const rawIndex = rawPath.length
          ? clamp(Math.round((index / Math.max(1, points.length - 1)) * (rawPath.length - 1)), 0, rawPath.length - 1)
          : 0
        const isCurrentPoint = index === points.length - 1
        const isPreviousPoint = index === points.length - 2
        if (playbackMode && !isCurrentPoint && !isPreviousPoint) return
        const checkpoint = amplify && isMajorPathPoint(rawPath, rawIndex, row)
        const localColor = playbackMode
          ? (isCurrentPoint ? activeSegmentColor : 0x94a3b8)
          : index === 0 ? color : (segments[Math.min(segments.length - 1, index - 1)]?.color ?? color)
        const dot = new THREE.Mesh(dotGeometry, new THREE.MeshBasicMaterial({
          color: localColor,
          transparent: true,
          opacity: playbackMode ? (isCurrentPoint ? 1 : 0.7) : index === points.length - 1 ? 1 : checkpoint ? 0.95 : amplify ? 0.78 : 0.28,
        }))
        dot.position.copy(point)
        dot.scale.setScalar(playbackMode
          ? (isCurrentPoint ? 2.15 : 1.08)
          : index === points.length - 1 ? (amplify ? 2.05 : 1.45) : checkpoint ? 1.55 : amplify ? 1.08 : 0.72)
        dot.userData.row = row
        dot.userData.isPathPoint = true
        dot.userData.pathPoint = rawPath[rawIndex] || null
        dot.userData.pathPointIndex = rawIndex + 1
        if (amplify) trailHitObjects.push(dot)
        trailGroup.add(dot)
      })
      const start = points[0]
      const end = points[points.length - 1]
      const previous = points[Math.max(0, points.length - 2)]
      const direction = new THREE.Vector3().subVectors(end, playbackMode ? previous : start)
      if (amplify) {
        const marker = new THREE.Mesh(
          new THREE.SphereGeometry(0.16, 20, 12),
          new THREE.MeshBasicMaterial({ color: activeSegmentColor, transparent: true, opacity: 0.95 }),
        )
        marker.position.copy(end)
        trailGroup.add(marker)
        const rocketDirection = direction.length() > 0.01
          ? direction.clone().normalize()
          : new THREE.Vector3(1, 0, 0)
        const nose = new THREE.Mesh(
          rocketNoseGeometry,
          new THREE.MeshBasicMaterial({ color: activeSegmentColor, transparent: true, opacity: 0.98 }),
        )
        nose.position.copy(end).add(rocketDirection.clone().multiplyScalar(0.46))
        nose.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), rocketDirection)
        nose.userData.row = row
        nose.userData.isPathPoint = true
        nose.userData.pathPoint = rawPath[rawPath.length - 1] || null
        nose.userData.pathPointIndex = rawPath.length
        trailHitObjects.push(nose)
        trailGroup.add(nose)
        const plume = new THREE.Mesh(
          rocketPlumeGeometry,
          new THREE.MeshBasicMaterial({ color: 0xf97316, transparent: true, opacity: 0.52 }),
        )
        plume.position.copy(end).add(rocketDirection.clone().multiplyScalar(-0.34))
        plume.quaternion.setFromUnitVectors(new THREE.Vector3(0, 1, 0), rocketDirection.clone().multiplyScalar(-1))
        trailGroup.add(plume)
      }
      if (direction.length() > 0.01) {
        const arrow = new THREE.ArrowHelper(direction.clone().normalize(), end, 0.55, activeSegmentColor, 0.2, 0.12)
        trailGroup.add(arrow)
      }
    }
    const restoreSelectedTrail = () => {
      const selected = selectedTickerRef.current
      if (!selected) return false
      const selectedMesh = meshes.find(mesh => mesh.userData.row?.ticker === selected)
      if (!selectedMesh) return false
      showTrail(selectedMesh.userData.row, selectedMesh)
      return true
    }
    rows.forEach((row, index) => {
      const currentSelectedTicker = selectedTickerRef.current
      const isSelected = Boolean(currentSelectedTicker && row.ticker === currentSelectedTicker)
      const material = new THREE.MeshStandardMaterial({
        color: colorForDecisionState(row.decisionState),
        roughness: 0.42,
        metalness: 0.12,
        emissive: colorForDecisionState(row.decisionState),
        emissiveIntensity: 0.11,
        transparent: true,
        opacity: currentSelectedTicker && !isSelected ? 0.34 : 0.95,
      })
      const mesh = new THREE.Mesh(sphereGeometry, material)
      mesh.position.copy(interpolatedPathPosition(row, isSelected ? playbackRef.current.progress : null, isSelected))
      const size = bubbleSize(row)
      mesh.scale.setScalar(currentSelectedTicker && !isSelected ? size * 0.78 : size)
      mesh.userData.row = row
      mesh.userData.baseSize = currentSelectedTicker && !isSelected ? size * 0.78 : size
      mesh.userData.rawBaseSize = size
      group.add(mesh)
      meshes.push(mesh)
      const halo = new THREE.Mesh(haloGeometry, new THREE.MeshBasicMaterial({
        color: marketCapColor(row.marketCapBucket),
        transparent: true,
        opacity: currentSelectedTicker && !isSelected ? 0.24 : 0.82,
        depthTest: true,
      }))
      halo.position.copy(mesh.position)
      halo.scale.setScalar(size * 1.15)
      halo.rotation.x = Math.PI / 2
      group.add(halo)
      mesh.userData.halo = halo
      const showLabel = rows.length <= 18 || index < 14 || isSelected
      if (showLabel) {
        const label = makeLabelSprite(row.ticker)
        label.position.copy(mesh.position).add(new THREE.Vector3(0, size + 0.32, 0))
        group.add(label)
        mesh.userData.label = label
      }
    })

    const raycaster = new THREE.Raycaster()
    const pointer = new THREE.Vector2()

    const updateCamera = () => {
      const s = stateRef.current
      const radius = s.radius / Math.max(0.7, zoom)
      camera.position.x = cameraTarget.x + radius * Math.sin(s.phi) * Math.cos(s.theta)
      camera.position.y = cameraTarget.y + radius * Math.cos(s.phi)
      camera.position.z = cameraTarget.z + radius * Math.sin(s.phi) * Math.sin(s.theta)
      camera.lookAt(cameraTarget)
    }
    const applySelectionStyle = () => {
      const currentSelectedTicker = selectedTickerRef.current
      meshes.forEach(mesh => {
        const row = mesh.userData.row as DecisionMapRow
        const rawBaseSize = Number(mesh.userData.rawBaseSize || mesh.userData.baseSize || 0.2)
        const isSelected = Boolean(currentSelectedTicker && row.ticker === currentSelectedTicker)
        const isDimmed = Boolean(currentSelectedTicker && !isSelected)
        const baseSize = isDimmed ? rawBaseSize * 0.78 : rawBaseSize
        mesh.userData.baseSize = baseSize
        mesh.scale.setScalar(isSelected ? baseSize * 1.08 : baseSize)
        const material = mesh.material as THREE.MeshStandardMaterial
        material.opacity = isDimmed ? 0.34 : 0.95
        material.emissiveIntensity = isSelected ? 0.34 : 0.11
        const halo = mesh.userData.halo as THREE.Mesh | undefined
        if (halo) {
          halo.scale.setScalar(baseSize * (isSelected ? 1.55 : 1.15))
          const haloMaterial = halo.material as THREE.MeshBasicMaterial
          haloMaterial.opacity = isDimmed ? 0.24 : 0.82
        }
      })
    }

    const resize = () => {
      const width = Math.max(1, host.clientWidth)
      const height = Math.max(1, host.clientHeight)
      camera.aspect = width / height
      camera.updateProjectionMatrix()
      renderer.setSize(width, height)
    }

    const onPointerDown = (event: PointerEvent) => {
      renderer.domElement.style.cursor = 'grabbing'
      stateRef.current.dragging = true
      stateRef.current.x = event.clientX
      stateRef.current.y = event.clientY
      pointerDownX = event.clientX
      pointerDownY = event.clientY
      didDrag = false
      host.setPointerCapture(event.pointerId)
    }
    const onPointerUp = (event: PointerEvent) => {
      const wasClick = !didDrag && Math.hypot(event.clientX - pointerDownX, event.clientY - pointerDownY) < 6
      stateRef.current.dragging = false
      renderer.domElement.style.cursor = 'grab'
      try { host.releasePointerCapture(event.pointerId) } catch (_) {}
      if (wasClick) {
        const rect = renderer.domElement.getBoundingClientRect()
        pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
        pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1
        raycaster.setFromCamera(pointer, camera)
        const hit = raycaster.intersectObjects([...trailHitObjects, ...meshes], false)[0]
        if (hit?.object?.userData?.row) {
          const row = hit.object.userData.row as DecisionMapRow
          onHoverTicker?.(row.ticker)
          if (hit.object.userData.isPathPoint) {
            restoreSelectedTrail()
            setClampedTooltip(event.clientX, event.clientY, row, {
              compact: true,
              point: hit.object.userData.pathPoint,
              pointIndex: hit.object.userData.pathPointIndex,
            })
          } else {
            showTrail(row, hit.object as THREE.Mesh)
            setClampedTooltip(event.clientX, event.clientY, row)
            onSelectTicker?.(row.ticker)
          }
        } else if (selectedTickerRef.current) {
          onSelectTicker?.('')
          setTooltip(null)
        }
      }
    }
    const onPointerMove = (event: PointerEvent) => {
      const rect = renderer.domElement.getBoundingClientRect()
      pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1
      pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1

      if (stateRef.current.dragging) {
        const dx = event.clientX - stateRef.current.x
        const dy = event.clientY - stateRef.current.y
        if (Math.hypot(event.clientX - pointerDownX, event.clientY - pointerDownY) >= 6) didDrag = true
        stateRef.current.theta -= dx * 0.008
        stateRef.current.phi = Math.max(0.42, Math.min(2.25, stateRef.current.phi + dy * 0.006))
        stateRef.current.x = event.clientX
        stateRef.current.y = event.clientY
        if (!selectedTickerRef.current) clearTrail()
        setTooltip(null)
        updateCamera()
        return
      }

      raycaster.setFromCamera(pointer, camera)
      const hit = raycaster.intersectObjects([...trailHitObjects, ...meshes], false)[0]
      if (hit?.object?.userData?.row) {
        const row = hit.object.userData.row as DecisionMapRow
        onHoverTicker?.(row.ticker)
        if (hit.object.userData.isPathPoint) {
          setClampedTooltip(event.clientX, event.clientY, row, {
            compact: true,
            point: hit.object.userData.pathPoint,
            pointIndex: hit.object.userData.pathPointIndex,
          })
          return
        }
        if (!selectedTickerRef.current || selectedTickerRef.current === row.ticker) {
          showTrail(row, hit.object as THREE.Mesh)
        } else {
          restoreSelectedTrail()
        }
        setClampedTooltip(event.clientX, event.clientY, row)
      } else {
        if (!restoreSelectedTrail()) clearTrail()
        onHoverTicker?.('')
        setTooltip(null)
      }
    }
    const onWheel = (event: WheelEvent) => {
      event.preventDefault()
      const trackpadPinch = event.ctrlKey || Math.abs(event.deltaY) < 8
      const speed = trackpadPinch ? 0.045 : 0.018
      stateRef.current.radius = clamp(stateRef.current.radius + event.deltaY * speed, 4.8, 42)
      updateCamera()
    }
    const onPointerLeave = (event: PointerEvent) => {
      onPointerUp(event)
      if (!selectedTickerRef.current) clearTrail()
      onHoverTicker?.('')
      setTooltip(null)
    }

    let frame = 0
    const render = () => {
      frame = requestAnimationFrame(render)
      meshes.forEach(mesh => {
        const row = mesh.userData.row as DecisionMapRow
        const currentSelectedTicker = selectedTickerRef.current
        const isSelected = Boolean(currentSelectedTicker && row.ticker === currentSelectedTicker)
        if (!isSelected) return
        const target = interpolatedPathPosition(row, playbackRef.current.progress, true)
        const path = displayPathVectors(row, true)
        const progress = playbackRef.current.progress
        const isAtLatest = progress == null || (path.length > 1 && progress >= path.length - 1 - 0.001)
        if (isAtLatest) mesh.position.copy(target)
        else mesh.position.lerp(target, 0.5)
        const halo = mesh.userData.halo as THREE.Mesh | undefined
        if (halo) halo.position.copy(mesh.position)
        const label = mesh.userData.label as THREE.Sprite | undefined
        if (label) label.position.copy(mesh.position).add(new THREE.Vector3(0, (mesh.userData.baseSize || 0.2) + 0.32, 0))
      })
      renderer.render(scene, camera)
    }

    stateRef.current.theta = -0.7
    stateRef.current.phi = 1.12
    stateRef.current.radius = 18
    resize()
    frameTickerPath(selectedTickerRef.current)
    trailApiRef.current = {
      show: (ticker: string) => {
        selectedTickerRef.current = ticker
        applySelectionStyle()
        const selectedMesh = meshes.find(mesh => mesh.userData.row?.ticker === ticker)
        if (selectedMesh) showTrail(selectedMesh.userData.row, selectedMesh)
        frameTickerPath(ticker)
      },
      clear: () => {
        selectedTickerRef.current = ''
        clearTrail()
        applySelectionStyle()
        frameTickerPath('')
      },
    }
    applySelectionStyle()
    if (selectedTickerRef.current) {
      const selectedMesh = meshes.find(mesh => mesh.userData.row?.ticker === selectedTickerRef.current)
      if (selectedMesh) showTrail(selectedMesh.userData.row, selectedMesh)
    }
    render()

    const observer = new ResizeObserver(resize)
    observer.observe(host)
    host.addEventListener('pointerdown', onPointerDown)
    host.addEventListener('pointerup', onPointerUp)
    host.addEventListener('pointerleave', onPointerLeave as EventListener)
    host.addEventListener('pointermove', onPointerMove)
    host.addEventListener('wheel', onWheel, { passive: false })

    return () => {
      cancelAnimationFrame(frame)
      observer.disconnect()
      host.removeEventListener('pointerdown', onPointerDown)
      host.removeEventListener('pointerup', onPointerUp)
      host.removeEventListener('pointerleave', onPointerLeave as EventListener)
      host.removeEventListener('pointermove', onPointerMove)
      host.removeEventListener('wheel', onWheel)
      try {
        trailApiRef.current = null
        clearTrailChildren()
        sphereGeometry.dispose()
        haloGeometry.dispose()
        rocketNoseGeometry.dispose()
        rocketPlumeGeometry.dispose()
        renderer.dispose()
        renderer.domElement.remove()
      } catch (_) {}
    }
  }, [rows, resetKey, zoom, onSelectTicker])

  return (
    <div
      ref={hostRef}
      data-testid="decision-map-canvas"
      data-plotted-points={rows.length}
      data-unique-tickers={new Set(rows.map(row => row.ticker)).size}
      data-tickers={rows.map(row => row.ticker).join(',')}
      aria-label={rows.length === 1
        ? `3D Decision Map for ${rows[0].ticker} with 1 ticker point`
        : `3D Decision Map with ${rows.length} ticker points`}
      className="relative h-[560px] min-h-[420px] overflow-hidden rounded bg-bg border border-border"
    >
      <div className="pointer-events-none absolute left-3 top-3 z-20 rounded border border-border bg-surface/90 px-2 py-1 text-[11px] text-neutral">
        {isLoading ? 'Loading real screener rows...' : `${rows.length} active screener-first rows`}
      </div>
      <div className="pointer-events-none absolute right-3 top-3 z-20 max-w-[300px] rounded border border-sky-500/30 bg-slate-950/88 px-2 py-1.5 text-[10px] text-slate-300">
        <div className="font-semibold uppercase tracking-wide text-sky-300">Map legend</div>
        <div className="mt-1 grid grid-cols-[auto_1fr] gap-x-2 gap-y-0.5">
          <span className="font-mono text-sky-200">X</span><span>rolling sentiment/catalyst pressure</span>
          <span className="font-mono text-sky-200">Y</span><span>log-scaled session move %</span>
          <span className="font-mono text-sky-200">Z</span><span>cap-adjusted volume pressure</span>
          <span className="font-mono text-sky-200">Trail</span><span>green improves · red weakens · dots inspect</span>
        </div>
      </div>
      <div className="pointer-events-none absolute bottom-3 left-3 z-20 rounded border border-border bg-surface/90 px-2 py-1 text-[10px] text-slate-400">
        Drag rotate · wheel zoom · click ticker · hover/click movement dots
      </div>
      {tooltip && (
        <div
          className={clsx(
            'pointer-events-none absolute z-30 max-h-[calc(100%-16px)] overflow-y-auto rounded border border-cyan-400/40 bg-slate-950/96 p-3 text-xs shadow-2xl shadow-cyan-950/40',
            tooltip.compact ? 'w-[min(280px,calc(100%-16px))]' : 'w-[min(430px,calc(100%-16px))]',
          )}
          style={{ left: tooltip.x, top: tooltip.y }}
        >
          {(() => {
            const row = tooltip.row
            const matchingMiniChart = miniPriceDensity && String(miniPriceDensity.ticker || '').toUpperCase() === row.ticker
              ? miniPriceDensity
              : null
            const isActiveMiniTicker = String(activeMiniTicker || '').toUpperCase() === row.ticker
            if (tooltip.compact && tooltip.point) {
              const point = tooltip.point
              const rawPath = rowPath(row)
              const pointIndex = Math.max(1, Number(tooltip.pointIndex || 1))
              const previousPoint = rawPath[pointIndex - 2] || null
              const stepScore = pathPointMovementScore(previousPoint, point, row)
              const stepTone = stepScore > 0.08 ? 'text-emerald-300' : stepScore < -0.08 ? 'text-red-300' : 'text-slate-300'
              const stepLabel = stepScore > 0.08 ? 'improving' : stepScore < -0.08 ? 'weakening' : 'flat/mixed'
              return (
                <div className="space-y-2">
                  <div className="flex items-center justify-between gap-3">
                    <span className="font-mono text-base font-semibold text-accent">{row.ticker}</span>
                    <StatusPill className="border-sky-400/40 bg-sky-500/10 text-sky-100">
                      point {pointIndex}
                    </StatusPill>
                  </div>
                  <div className="font-mono text-sm text-slate-100">
                    {point.displayTime || compactTimestamp(point.timestamp)}
                  </div>
                  <div className="grid grid-cols-3 gap-1.5">
                    <MetricCell label="Sent" value={numberLabel(point.combinedSentiment, 2)} tone={(point.combinedSentiment ?? 0) >= 0 ? 'text-emerald-300' : 'text-red-300'} />
                    <MetricCell label="Move" value={signedPct(point.priceChangePct)} tone={(point.priceChangePct ?? 0) >= 0 ? 'text-emerald-300' : 'text-red-300'} />
                    <MetricCell label="RelVol" value={numberLabel(point.relativeVolume, 2, 'x')} />
                    <MetricCell label="Pressure" value={`${numberLabel(volumePressurePlotScore(point, row), 0)}/100`} />
                    <MetricCell label="Step" value={stepLabel} tone={stepTone} />
                    <MetricCell label="Delta" value={numberLabel(stepScore, 2)} tone={stepTone} />
                  </div>
                  <div className="text-[10px] leading-snug text-slate-400">
                    {point.chartBarTime || point.timestampUtc || 'Timestamp from selected Decision Map path.'} · {pathSourceLabel(point.timestampSource)}
                  </div>
                  <div className="text-[10px] leading-snug text-slate-300">
                    {previousPoint
                      ? `${stepLabel}: price ${signedPct(previousPoint.priceChangePct)} -> ${signedPct(point.priceChangePct)}, sentiment ${numberLabel(previousPoint.combinedSentiment, 2)} -> ${numberLabel(point.combinedSentiment, 2)}`
                      : 'First visible movement point in this selected journey.'}
                  </div>
                </div>
              )
            }
            return (
              <div className="space-y-2">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-mono text-lg font-semibold text-accent">{row.ticker}</span>
                      <StatusPill className={stateClass(row.decisionState)}>{labelForDecisionState(row.decisionState)}</StatusPill>
                      <StatusPill className={pathQualityClass(row)}>{pathQualityLabel(row)}</StatusPill>
                    </div>
                    <div className="mt-0.5 truncate text-slate-300">{row.company || row.screenerSource || 'Company unavailable'}</div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-[10px] uppercase tracking-wide text-slate-500">Score</div>
                    <div className="font-mono text-xl font-semibold text-emerald-300">{row.convictionScore}</div>
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-1.5">
                  <MetricCell label="Price" value={money(row.price)} />
                  <MetricCell label={row.activeSession || 'Session'} value={signedPct(row.priceChangePct)} tone={row.priceChangePct >= 0 ? 'text-emerald-300' : 'text-red-300'} />
                  <MetricCell label="Sentiment" value={numberLabel(row.combinedSentiment, 2)} tone={row.combinedSentiment >= 0 ? 'text-emerald-300' : 'text-red-300'} />
                  <MetricCell label="Rel Vol" value={numberLabel(row.relativeVolume, 2, 'x')} />
                  <MetricCell label="Raw Vol" value={compact(row.currentVolume)} />
                  <MetricCell label="$ Vol" value={money(row.currentDollarVolume)} />
                  <MetricCell label="Float" value={compact(row.sharesFloat)} />
                  <MetricCell label="Float bucket" value={row.floatBucket || '--'} />
                  <MetricCell label="Short float" value={row.floatShort == null ? '--' : `${Number(row.floatShort).toFixed(2)}%`} />
                </div>
                {matchingMiniChart ? (
                  <MiniPriceDensityChart data={matchingMiniChart} />
                ) : isActiveMiniTicker && isMiniChartLoading ? (
                  <div className="rounded border border-slate-700/70 bg-slate-900/70 px-2 py-1.5 text-[10px] text-slate-400">
                    Price + Density chart loading for {row.ticker}.
                  </div>
                ) : isActiveMiniTicker ? (
                  <div className="rounded border border-slate-700/70 bg-slate-900/70 px-2 py-1.5 text-[10px] text-slate-400">
                    No aligned Price + Density chart available for {row.ticker}.
                  </div>
                ) : null}
              </div>
            )
          })()}
        </div>
      )}
    </div>
  )
}

type DecisionMapPanelProps = {
  focusTicker?: string
  single?: boolean
  embedded?: boolean
  rollingWindowMinutes?: number
  miniPriceDensity?: MiniPriceDensity | null
}

export function DecisionMapPanel({ focusTicker: forcedFocusTicker = '', single = false, embedded = false, rollingWindowMinutes, miniPriceDensity }: DecisionMapPanelProps = {}) {
  const [urlParams] = useSearchParams()
  const requestedSingleTickerMode = ['1', 'true', 'yes'].includes(String(urlParams.get('single') || '').toLowerCase())
  const focusTicker = useMemo(() => {
    const forced = String(forcedFocusTicker || '').toUpperCase().replace(/^\$/, '')
    if (/^[A-Z][A-Z0-9.-]{0,7}$/.test(forced)) return forced
    const raw = String(
      urlParams.get('focusTicker') ||
      (requestedSingleTickerMode ? urlParams.get('ticker') || urlParams.get('search') : '') ||
      '',
    ).toUpperCase().replace(/^\$/, '')
    return /^[A-Z][A-Z0-9.-]{0,7}$/.test(raw) ? raw : ''
  }, [urlParams, forcedFocusTicker, requestedSingleTickerMode])
  const singleTickerMode = (single || requestedSingleTickerMode) && Boolean(focusTicker)
  const [minRelVolume, setMinRelVolume] = useState(1)
  const [minAbsChange, setMinAbsChange] = useState(0.5)
  const [minSentiment, setMinSentiment] = useState(0.12)
  const controlledWindowHours = Number.isFinite(Number(rollingWindowMinutes)) && Number(rollingWindowMinutes) > 0
    ? Number(rollingWindowMinutes) / 60
    : null
  const [windowHours, setWindowHours] = useState(controlledWindowHours ?? 4)
  const [volumeTimeframe, setVolumeTimeframe] = useState('5m')
  const [universe, setUniverse] = useState('active_finviz')
  const [session, setSession] = useState('auto')
  const [marketCapBucket, setMarketCapBucket] = useState('all')
  const [floatBucket, setFloatBucket] = useState('all')
  const [relVolumeBucket, setRelVolumeBucket] = useState('all')
  const [sortKey, setSortKey] = useState<SortKey>('convictionScore')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc')
  const [zoom, setZoom] = useState(1)
  const [resetKey, setResetKey] = useState(0)
  const [selectedTicker, setSelectedTicker] = useState(focusTicker)
  const [hoveredTicker, setHoveredTicker] = useState('')
  const [tickerQuery, setTickerQuery] = useState('')
  const [searchedTicker, setSearchedTicker] = useState('')
  const tickerOptions = useTickerDatalistOptions(tickerQuery)
  const [isPlayingJourney, setIsPlayingJourney] = useState(false)
  const [playbackProgress, setPlaybackProgress] = useState<number | null>(null)
  const [blankRefreshKey, setBlankRefreshKey] = useState('')

  const requestTicker = focusTicker || searchedTicker
  const tickerSearchMode = !singleTickerMode && Boolean(searchedTicker)
  const singleTickerRequest = singleTickerMode || tickerSearchMode

  useEffect(() => {
    if (requestTicker) {
      setSelectedTicker(requestTicker)
      return
    }
    if (!singleTickerRequest) {
      setSelectedTicker('')
      setPlaybackProgress(null)
      setIsPlayingJourney(false)
    }
  }, [requestTicker, singleTickerRequest])

  useEffect(() => {
    if (controlledWindowHours != null) setWindowHours(controlledWindowHours)
  }, [controlledWindowHours])

  const params = useMemo(() => {
    const next = new URLSearchParams({
      universe,
      path_points: '120',
      session,
      limit: singleTickerRequest ? '1' : '30',
      min_rel_volume: String(singleTickerRequest ? 0 : relVolumeBucket === 'all' ? minRelVolume : 0),
      min_abs_change: String(singleTickerRequest ? 0 : minAbsChange),
      positive_sentiment: String(singleTickerRequest ? -1 : minSentiment),
      negative_sentiment: String(singleTickerRequest ? 1 : -minSentiment),
      price_threshold: String(singleTickerRequest ? 0 : minAbsChange),
      market_cap_bucket: singleTickerRequest ? 'all' : marketCapBucket,
      float_bucket: singleTickerRequest ? 'all' : floatBucket,
      rel_volume_bucket: singleTickerRequest ? 'all' : relVolumeBucket,
      rolling_window_hours: String(windowHours),
      news_window_hours: String(windowHours),
      social_window_hours: String(windowHours),
      path_window_hours: String(windowHours),
      volume_timeframe: volumeTimeframe,
      force_chart_volume: '1',
      market_day_paths: '1',
      sortBy: sortKey,
      orderDir: sortDir,
    })
    if (singleTickerRequest && requestTicker) {
      next.set('search', requestTicker)
      next.set('ticker', requestTicker)
      next.set('focusTicker', requestTicker)
      next.set('single', '1')
      next.set('cache_scope', `single-${requestTicker}`)
    }
    return next
  }, [minRelVolume, minAbsChange, minSentiment, windowHours, volumeTimeframe, universe, session, marketCapBucket, floatBucket, relVolumeBucket, sortKey, sortDir, requestTicker, singleTickerRequest])

  const decisionMapKey = `/api/decision-map?${params.toString()}`
  const { data, isLoading, mutate } = useSWR(decisionMapKey, fetcher, { refreshInterval: 60_000, keepPreviousData: false })
  const rawRows: DecisionMapRow[] = data?.rows ?? []
  const rows: DecisionMapRow[] = useMemo(() => {
    if (!singleTickerRequest || !requestTicker) return rawRows
    return rawRows.filter(row => String(row.ticker || '').toUpperCase() === requestTicker)
  }, [rawRows, requestTicker, singleTickerRequest])
  const plottedRows = useMemo(() => uniquePlottableRows(rows), [rows])
  const singleTickerMismatch = singleTickerRequest && requestTicker && rawRows.length > 0 && rows.length === 0
  const selectedRow = useMemo(
    () => rows.find(row => row.ticker === selectedTicker) || null,
    [rows, selectedTicker],
  )
  const selectedPath = useMemo(
    () => selectedRow ? rowPath(selectedRow) : [],
    [selectedRow],
  )
  const selectedVisualPath = useMemo(
    () => selectedRow ? displayPathVectors(selectedRow, true) : [],
    [selectedRow],
  )
  const selectedMovementSpan = useMemo(
    () => selectedRow ? journeyMovementSpan(selectedRow) : 0,
    [selectedRow],
  )
  const activeMiniTicker = hoveredTicker || selectedTicker || focusTicker
  const activeMiniRow = useMemo(
    () => activeMiniTicker ? rows.find(row => row.ticker === activeMiniTicker) || null : null,
    [activeMiniTicker, rows],
  )
  const activeMiniWindowMinutes = Math.max(5, Math.round(windowHours * 60))
  const activeMiniChartUrl = activeMiniTicker
    ? `/api/charts/${encodeURIComponent(activeMiniTicker)}?tf=1m&events=1&window_minutes=${activeMiniWindowMinutes}&view_window_minutes=${activeMiniWindowMinutes}&bucket_minutes=1`
    : null
  const { data: activeMiniChartPayload, isLoading: isMiniChartLoading } = useSWR(activeMiniChartUrl, fetcher, {
    refreshInterval: 60_000,
    keepPreviousData: true,
    revalidateOnFocus: false,
  })
  const activeMiniPriceDensity = useMemo(() => {
    if (miniPriceDensity && String(miniPriceDensity.ticker || '').toUpperCase() === String(activeMiniTicker || '').toUpperCase()) {
      return miniPriceDensity
    }
    return miniPriceDensityFromChartPayload(activeMiniChartPayload, String(activeMiniTicker || '').toUpperCase(), activeMiniWindowMinutes, activeMiniRow)
  }, [activeMiniChartPayload, activeMiniRow, activeMiniTicker, activeMiniWindowMinutes, miniPriceDensity])
  const pathPointSummary = useMemo(() => {
    const counts = rows.map(row => Number(row.path_points_count || row.path_points?.length || 0)).sort((a, b) => a - b)
    const quality = rows.reduce<Record<string, number>>((acc, row) => {
      const key = pathQualityLabel(row)
      acc[key] = (acc[key] || 0) + 1
      return acc
    }, {})
    return {
      min: counts[0] || 0,
      median: counts[Math.floor(counts.length / 2)] || 0,
      max: counts[counts.length - 1] || 0,
      quality,
    }
  }, [rows])

  useEffect(() => {
    if (!isPlayingJourney || selectedVisualPath.length < 2) return
    let frame = 0
    const startedAt = performance.now()
    const duration = clamp(selectedVisualPath.length * 175, 4300, 23000) / JOURNEY_PLAYBACK_SPEED_MULTIPLIER
    const step = (now: number) => {
      const elapsed = now - startedAt
      const t = clamp(elapsed / duration, 0, 1)
      const eased = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2
      setPlaybackProgress(eased * (selectedVisualPath.length - 1))
      if (t < 1) {
        frame = window.requestAnimationFrame(step)
      } else {
        setPlaybackProgress(null)
        setIsPlayingJourney(false)
      }
    }
    setPlaybackProgress(0)
    frame = window.requestAnimationFrame(step)
    return () => window.cancelAnimationFrame(frame)
  }, [isPlayingJourney, selectedVisualPath.length])

  const selectTicker = useCallback((ticker: string) => {
    if (!ticker) {
      setSelectedTicker('')
      setPlaybackProgress(null)
      setIsPlayingJourney(false)
      setZoom(1)
      setResetKey(v => v + 1)
      return
    }
    setSelectedTicker(ticker)
    setPlaybackProgress(null)
    setIsPlayingJourney(false)
  }, [rows])

  const resetJourney = () => {
    setSelectedTicker('')
    setPlaybackProgress(null)
    setIsPlayingJourney(false)
    setZoom(1)
    setResetKey(v => v + 1)
  }

  const playJourney = () => {
    if (!selectedRow || selectedVisualPath.length < 2) return
    setPlaybackProgress(0)
    setIsPlayingJourney(true)
  }

  useEffect(() => {
    if (singleTickerRequest || isLoading || blankRefreshKey === decisionMapKey) return
    if (!data?.ok || !Array.isArray(data.rows) || data.rows.length > 0) return
    setBlankRefreshKey(decisionMapKey)
    const freshParams = new URLSearchParams(params)
    freshParams.set('fresh', '1')
    fetcher(`/api/decision-map?${freshParams.toString()}`)
      .then(rebuilt => mutate(rebuilt, { revalidate: false }))
      .catch(() => {})
  }, [blankRefreshKey, data, decisionMapKey, isLoading, mutate, params, singleTickerRequest])

  const sortedRows = useMemo(() => {
    const copy = [...rows]
    copy.sort((a, b) => {
      const av = a[sortKey]
      const bv = b[sortKey]
      if (typeof av === 'string') return sortDir === 'asc' ? av.localeCompare(String(bv)) : String(bv).localeCompare(av)
      return sortDir === 'asc' ? Number(av || 0) - Number(bv || 0) : Number(bv || 0) - Number(av || 0)
    })
    return copy.slice(0, 40)
  }, [rows, sortDir, sortKey])

  const setSort = (key: SortKey) => {
    if (key === sortKey) setSortDir(v => v === 'asc' ? 'desc' : 'asc')
    else {
      setSortKey(key)
      setSortDir(key === 'ticker' ? 'asc' : 'desc')
    }
  }
  const submitTickerSearch = () => {
    const normalized = tickerQuery.toUpperCase().replace(/^\$/, '').trim()
    if (!/^[A-Z][A-Z0-9.-]{0,7}$/.test(normalized)) return
    setTickerQuery(normalized)
    setSearchedTicker(normalized)
    setSelectedTicker(normalized)
    setPlaybackProgress(null)
    setIsPlayingJourney(false)
    setResetKey(value => value + 1)
  }
  const clearTickerSearch = () => {
    setTickerQuery('')
    setSearchedTicker('')
    setSelectedTicker('')
    setPlaybackProgress(null)
    setIsPlayingJourney(false)
    setZoom(1)
    setResetKey(value => value + 1)
  }
  const validTickerQuery = /^[A-Z][A-Z0-9.-]{0,7}$/.test(tickerQuery.toUpperCase().replace(/^\$/, '').trim())
  const playbackFrame = selectedVisualPath.length
    ? Math.min(selectedVisualPath.length, Math.floor(playbackProgress ?? selectedVisualPath.length - 1) + 1)
    : 0
  const selectedScaleLabel = selectedRow
    ? `${marketCapBucketKey(selectedRow)} cap lens · ${selectedMovementSpan.toFixed(1)}u move · ${selectedRow.path_market_date || 'latest market day'} · ${zoom.toFixed(1)}x`
    : ''
  const selectedFirstPoint = firstPathPoint(selectedRow)
  const selectedLastPoint = lastPathPoint(selectedRow)

  if (singleTickerMode) {
    return (
      <div className="space-y-3">
        <section className="bg-surface border border-border rounded-lg overflow-hidden">
          <div className="border-b border-border bg-slate-950/45 px-3 py-2">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-xs uppercase text-neutral font-medium">3D Decision Journey</span>
                  <span className="font-mono text-lg font-semibold text-accent">{focusTicker}</span>
                  {selectedRow && <StatusPill className={stateClass(selectedRow.decisionState)}>{labelForDecisionState(selectedRow.decisionState)}</StatusPill>}
                  {selectedRow && <StatusPill className={pathQualityClass(selectedRow)}>{pathQualityLabel(selectedRow)}</StatusPill>}
                </div>
                <div className="mt-1 text-[11px] text-slate-300">
                  {selectedRow
                    ? `${selectedVisualPath.length || 0} display frames · ${selectedPath.length || 0} raw journey points · ${selectedRow.path_window_used || selectedRow.pathWindowUsed || windowLabel(windowHours)} · volume ${selectedRow.path_volume_timeframe || volumeTimeframe} · ${selectedRow.path_market_date || 'latest market day'}`
                    : isLoading ? 'Loading ticker journey...' : 'No Decision Map row found for this ticker.'}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={playJourney}
                  disabled={!selectedRow || selectedVisualPath.length < 2 || isPlayingJourney}
                  className="rounded border border-sky-500/50 bg-sky-500/10 px-3 py-1 text-xs text-sky-100 hover:bg-sky-500/20 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {isPlayingJourney ? 'Playing' : 'Play'}
                </button>
                <button
                  type="button"
                  onClick={resetJourney}
                  className="rounded border border-border bg-bg px-3 py-1 text-xs text-slate-200 hover:text-white"
                >
                  Reset
                </button>
                {!embedded && (
                  <button
                    type="button"
                    onClick={() => window.close()}
                    className="rounded border border-border bg-bg px-3 py-1 text-xs text-neutral hover:text-white"
                  >
                    Close
                  </button>
                )}
              </div>
            </div>
            {selectedRow && (
              <div className="mt-2 grid gap-2 text-[11px] lg:grid-cols-3">
                <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                  <span className="text-slate-500">Evidence</span>
                  <div className="mt-0.5 text-slate-200">{selectedRow.catalystLabel ? `${selectedRow.catalystLabel}${selectedRow.catalystFromLookback ? ` · ${selectedRow.catalystLookbackUsed || 'recent'} lookback` : ''}` : 'No catalyst'} · {(selectedRow.structuredArticleCount ?? selectedRow.articleCount ?? 0)} structured · {selectedRow.socialCount || 0} social</div>
                </div>
                <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                  <span className="text-slate-500">Path window</span>
                  <div className="mt-0.5 text-slate-200">{selectedRow.path_window_minutes || 0}m covered · {selectedRow.path_window_raw_points_count || selectedRow.path_points_count || 0} raw · volume {selectedRow.path_volume_timeframe || volumeTimeframe}</div>
                </div>
                <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                  <span className="text-slate-500">Data quality</span>
                  <div className="mt-0.5 text-slate-200">{missingDataLabels(selectedRow).slice(0, 3).join(' · ')}</div>
                </div>
              </div>
            )}
          </div>
          <div className="p-3">
            <ThreeDecisionMap
              key={singleTickerMode ? `single-${focusTicker}-${data?.cacheSignature || data?.generatedAt || rows[0]?.ticker || 'loading'}` : `multi-${resetKey}-${data?.cacheSignature || rows.length}`}
              rows={plottedRows}
              zoom={zoom}
              resetKey={resetKey}
              isLoading={isLoading}
              selectedTicker={selectedTicker || focusTicker}
              playbackProgress={playbackProgress}
              miniPriceDensity={activeMiniPriceDensity}
              activeMiniTicker={activeMiniTicker}
              isMiniChartLoading={isMiniChartLoading}
              onSelectTicker={selectTicker}
              onHoverTicker={setHoveredTicker}
            />
          </div>
        </section>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <section className="bg-surface border border-border rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-border flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-xs uppercase text-neutral font-medium">Three.js Decision Map</div>
            <div className="text-[11px] text-slate-400">X rolling sentiment · Y log session move · Z cap-adjusted pressure · trail green/red movement · ring market-cap bucket</div>
          </div>
          <div className="flex flex-wrap items-center gap-2 text-[11px] text-neutral">
            {[
              'strong_bullish_candidate',
              'moderate_bullish_candidate',
              'neutral_watchlist',
              'risky_uncertain',
              'weak_no_catalyst',
            ].map(state => (
              <span key={state} className="inline-flex items-center gap-1">
                <span className={clsx('h-2 w-2 rounded-full', dotClass(state))} />
                {labelForDecisionState(state)}
              </span>
            ))}
            <StatusPill className="border-sky-400/40 bg-sky-500/10 text-sky-100">full journey {pathPointSummary.quality['Full journey'] || 0}</StatusPill>
            <StatusPill className="border-cyan-400/40 bg-cyan-500/10 text-cyan-100">partial {pathPointSummary.quality['Partial journey'] || 0}</StatusPill>
            <StatusPill className="border-amber-400/40 bg-amber-500/10 text-amber-100">snapshot/static {(pathPointSummary.quality['Snapshot only'] || 0) + (pathPointSummary.quality['Static: low movement'] || 0) + (pathPointSummary.quality['Static: missing path data'] || 0)}</StatusPill>
          </div>
        </div>
        <div className="border-b border-border bg-bg/40 px-3 py-2">
          <form
            className="flex flex-wrap items-end gap-2"
            onSubmit={event => {
              event.preventDefault()
              submitTickerSearch()
            }}
          >
            <label className="block min-w-[180px] flex-1 sm:max-w-xs">
              <span className="mb-1 block text-[10px] uppercase text-neutral">Ticker journey</span>
              <input
                type="search"
                value={tickerQuery}
                list="decision-map-ticker-universe"
                maxLength={8}
                autoComplete="off"
                placeholder="Ticker"
                onChange={event => setTickerQuery(event.target.value.toUpperCase().replace(/[^A-Z0-9.-]/g, ''))}
                className="w-full rounded border border-border bg-bg px-2 py-1.5 font-mono text-xs text-white placeholder:text-slate-600 focus:border-sky-400 focus:outline-none"
              />
              <datalist id="decision-map-ticker-universe">
                {tickerOptions.map(row => (
                  <option key={row.ticker} value={row.ticker}>{row.company || row.exchange || row.ticker}</option>
                ))}
              </datalist>
            </label>
            <button
              type="submit"
              disabled={!validTickerQuery || tickerQuery === searchedTicker}
              className="rounded border border-sky-500/50 bg-sky-500/10 px-3 py-1.5 text-xs text-sky-100 hover:bg-sky-500/20 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Search
            </button>
            {searchedTicker && (
              <button
                type="button"
                onClick={clearTickerSearch}
                className="rounded border border-border bg-bg px-3 py-1.5 text-xs text-slate-200 hover:text-white"
              >
                Clear
              </button>
            )}
            <StatusPill className="border-slate-600 bg-slate-900/70 text-slate-300">
              {searchedTicker ? `${searchedTicker} journey` : 'Top 30 candidates'}
            </StatusPill>
          </form>
        </div>
        <div className="border-b border-border bg-bg/40 px-3 py-2 flex flex-wrap items-center justify-between gap-2">
          <div className="text-xs text-slate-300">
            {selectedRow ? (
              <>
                <span className="font-mono text-accent font-semibold">{selectedRow.ticker}</span>
                <span className="text-neutral"> isolated · {selectedVisualPath.length || 0} display frames · {selectedPath.length || 0} raw points</span>
                {selectedVisualPath.length > 0 && (
                  <span className="text-neutral"> · frame {playbackFrame}/{selectedVisualPath.length}</span>
                )}
                <span className="text-sky-300"> · {selectedScaleLabel}</span>
              </>
            ) : (
              <span>Click any ticker bubble or row to isolate one stock, then play its path through the 3D map.</span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={playJourney}
              disabled={!selectedRow || selectedVisualPath.length < 2 || isPlayingJourney}
              className="rounded border border-sky-500/50 bg-sky-500/10 px-3 py-1 text-xs text-sky-100 hover:bg-sky-500/20 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {isPlayingJourney ? 'Playing' : 'Play'}
            </button>
            <button
              type="button"
              onClick={resetJourney}
              className="rounded border border-border bg-bg px-3 py-1 text-xs text-slate-200 hover:text-white"
            >
              Reset
            </button>
          </div>
        </div>

        {selectedRow && (
          <div className="border-b border-border bg-slate-950/45 px-3 py-2">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-mono text-sm font-semibold text-accent">{selectedRow.ticker}</span>
                  <StatusPill className={stateClass(selectedRow.decisionState)}>{labelForDecisionState(selectedRow.decisionState)}</StatusPill>
                  <StatusPill className={pathQualityClass(selectedRow)}>{pathQualityLabel(selectedRow)}</StatusPill>
                  {selectedRow.path_sampling === 'shape_preserving' && <StatusPill className="border-sky-400/40 bg-sky-500/10 text-sky-100">shape preserved</StatusPill>}
                </div>
                <div className="mt-1 max-w-4xl text-[11px] text-slate-300">
                  {selectedRow.movementSummary || 'Movement summary unavailable.'}
                </div>
              </div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] text-slate-300 sm:grid-cols-4">
                <div><span className="text-slate-500">Frames / raw</span><br /><span className="font-mono text-slate-100">{selectedVisualPath.length || 0}/{selectedRow.path_points_count || selectedPath.length}</span></div>
                <div><span className="text-slate-500">First</span><br /><span className="font-mono text-slate-100">{compactTimestamp(selectedFirstPoint?.timestamp)}</span></div>
                <div><span className="text-slate-500">Last</span><br /><span className="font-mono text-slate-100">{compactTimestamp(selectedLastPoint?.timestamp)}</span></div>
                <div><span className="text-slate-500">3D Vol</span><br /><span className="font-mono text-slate-100">{selectedRow.path_volume_timeframe || volumeTimeframe}</span></div>
              </div>
            </div>
            <div className="mt-2 grid gap-2 text-[11px] lg:grid-cols-3">
              <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                <span className="text-slate-500">Evidence</span>
                <div className="mt-0.5 text-slate-200">{selectedRow.catalystLabel ? `${selectedRow.catalystLabel}${selectedRow.catalystFromLookback ? ` · ${selectedRow.catalystLookbackUsed || 'recent'} lookback` : ''}` : 'No catalyst'} · {(selectedRow.structuredArticleCount ?? selectedRow.articleCount ?? 0)} structured · {selectedRow.socialCount || 0} social · {selectedRow.rollingWindowUsed || windowLabel(windowHours)}</div>
              </div>
              <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                <span className="text-slate-500">Path window</span>
                <div className="mt-0.5 text-slate-200">{selectedRow.path_window_used || selectedRow.pathWindowUsed || windowLabel(windowHours)} selected · {selectedRow.path_window_minutes || 0}m covered · {selectedRow.path_window_raw_points_count || selectedRow.path_points_count || 0} raw · volume {selectedRow.path_volume_timeframe || volumeTimeframe}</div>
              </div>
              <div className="rounded border border-slate-700/70 bg-slate-900/60 px-2 py-1">
                <span className="text-slate-500">Data quality</span>
                <div className="mt-0.5 text-slate-200">{missingDataLabels(selectedRow).slice(0, 3).join(' · ')}</div>
              </div>
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 xl:grid-cols-[260px_minmax(0,1fr)] gap-0">
          <div className="border-b xl:border-b-0 xl:border-r border-border p-3 space-y-3">
            <Control label="Market cap" value={marketCapBucket}>
              <select value={marketCapBucket} onChange={e => setMarketCapBucket(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value="all">All buckets</option>
                <option value="mega">Mega cap</option>
                <option value="large">Large cap</option>
                <option value="mid">Mid cap</option>
                <option value="small">Small cap</option>
                <option value="micro">Micro cap</option>
                <option value="nano">Nano cap</option>
              </select>
            </Control>
            <Control label="Float" value={floatBucket === 'all' ? 'All floats' : floatBucket.replace('_', ' ')}>
              <select value={floatBucket} onChange={e => setFloatBucket(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value="all">All floats</option>
                <option value="ultra_low">Ultra-low (&lt;10M)</option>
                <option value="low">Low (10-25M)</option>
                <option value="mid">Mid (25-50M)</option>
                <option value="high">High (50-100M)</option>
                <option value="very_high">Very high (100M+)</option>
                <option value="unknown">Unknown float</option>
              </select>
            </Control>
            <Control label="Relative volume" value={relVolumeBucket === 'all' ? `${minRelVolume.toFixed(1)}x min` : relVolumeBucket}>
              <select value={relVolumeBucket} onChange={e => setRelVolumeBucket(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value="all">All, use min slider</option>
                <option value="low">Low (&lt;1x)</option>
                <option value="medium">Medium (1-3x)</option>
                <option value="high">High (3-10x)</option>
                <option value="extreme">Extreme (10x+)</option>
              </select>
              {relVolumeBucket === 'all' && <input type="range" min="0" max="10" step="0.1" value={minRelVolume} onChange={e => setMinRelVolume(Number(e.target.value))} className="mt-2 w-full" />}
            </Control>
            <Control label="Min abs price change" value={`${minAbsChange.toFixed(1)}%`}>
              <input type="range" min="0" max="15" step="0.1" value={minAbsChange} onChange={e => setMinAbsChange(Number(e.target.value))} className="w-full" />
            </Control>
            <Control label="Min sentiment strength" value={minSentiment.toFixed(2)}>
              <input type="range" min="0" max="0.7" step="0.01" value={minSentiment} onChange={e => setMinSentiment(Number(e.target.value))} className="w-full" />
            </Control>
            <Control label="Rolling window" value={windowLabel(windowHours)}>
              <select value={windowHours} onChange={e => setWindowHours(Number(e.target.value))} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value={0.25}>15m</option>
                <option value={0.5}>30m</option>
                <option value={1}>1h</option>
                <option value={2}>2h</option>
                <option value={4}>4h active default</option>
                <option value={6}>6h</option>
                <option value={12}>12h</option>
                <option value={24}>1d</option>
                <option value={48}>48h</option>
                <option value={72}>72h</option>
              </select>
            </Control>
            <Control label="3D volume timeframe" value={volumeTimeframe}>
              <select value={volumeTimeframe} onChange={e => setVolumeTimeframe(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                {VOLUME_TIMEFRAMES.map(tf => (
                  <option key={tf.value} value={tf.value}>{tf.label}</option>
                ))}
              </select>
            </Control>
            <Control label="Session movement" value={session}>
              <select value={session} onChange={e => setSession(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value="auto">Auto strongest session</option>
                <option value="premarket">Premarket</option>
                <option value="regular">Regular market</option>
                <option value="postmarket">Post-market</option>
              </select>
            </Control>
            <Control label="Universe" value={universe.replace('_', ' ')}>
              <select value={universe} onChange={e => setUniverse(e.target.value)} className="w-full bg-bg border border-border rounded px-2 py-1 text-xs text-slate-200">
                <option value="active_finviz">Active Finviz Elite</option>
                <option value="numeric_all">Finviz + TradingView numeric</option>
                <option value="tradingview">TradingView numeric</option>
              </select>
            </Control>
            <Control label="Zoom" value={`${zoom.toFixed(1)}x`}>
              <input type="range" min="0.7" max="1.8" step="0.05" value={zoom} onChange={e => setZoom(Number(e.target.value))} className="w-full" />
            </Control>
            <button
              className="w-full rounded border border-border bg-bg px-2 py-1 text-xs text-neutral hover:text-white"
              onClick={() => { setZoom(1); setResetKey(v => v + 1) }}
            >
              Reset View
            </button>
          </div>

          <div className="p-3">
            <ThreeDecisionMap
              rows={plottedRows}
              zoom={zoom}
              resetKey={resetKey}
              isLoading={isLoading}
              selectedTicker={selectedTicker || undefined}
              playbackProgress={playbackProgress}
              miniPriceDensity={activeMiniPriceDensity}
              activeMiniTicker={activeMiniTicker}
              isMiniChartLoading={isMiniChartLoading}
              onSelectTicker={selectTicker}
              onHoverTicker={setHoveredTicker}
            />
            {singleTickerMismatch && (
              <div className="mt-2 rounded border border-amber-400/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
                Loaded Decision Map cache did not match {requestTicker}; waiting for the ticker-specific payload.
              </div>
            )}
          </div>
        </div>
      </section>

      <section className="bg-surface border border-border rounded-lg overflow-hidden">
        <div className="px-3 py-2 border-b border-border flex items-center justify-between">
          <div>
            <div className="text-xs uppercase text-neutral font-medium">Decision Rows</div>
            <div className="text-[11px] text-slate-400">High activity + catalyst confirmation should rise to the top.</div>
          </div>
          <div className="text-[11px] text-neutral">
            Q1 {data?.summary?.Q1 ?? 0} · Q3 {data?.summary?.Q3 ?? 0} · divergence {data?.summary?.divergence ?? 0}
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="bg-bg/50 border-b border-border">
              <tr>
                {[
                  ['ticker', 'Ticker'],
                  ['activeSession', 'Session'],
                  ['priceChangePct', 'Chg%'],
                  ['relativeVolume', 'Rel Vol'],
                  ['floatBucket', 'Float'],
                  ['marketCapRelativeVolumeScore', 'Cap RelVol'],
                  ['liquidityScore', 'Liquidity'],
                  ['currentDollarVolume', '$ Vol'],
                  ['combinedSentiment', 'Sent'],
                  ['decisionState', 'State'],
                  ['activityScore', 'Activity'],
                  ['convictionScore', 'Score'],
                  ['catalystLabel', 'Catalyst'],
                  ['headline', 'Top headline'],
                ].map(([key, label]) => (
                  <th
                    key={key}
                    onClick={() => ['ticker', 'priceChangePct', 'relativeVolume', 'marketCapRelativeVolumeScore', 'liquidityScore', 'combinedSentiment', 'activityScore', 'convictionScore'].includes(key) && setSort(key as SortKey)}
                    className="px-3 py-2 text-left text-[10px] uppercase text-neutral whitespace-nowrap cursor-pointer hover:text-white"
                  >
                    {label}{sortKey === key ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/30">
              {sortedRows.map(row => (
                <tr
                  key={row.ticker}
                  onClick={() => selectTicker(row.ticker)}
                  className={clsx('hover:bg-bg/40 cursor-pointer', selectedTicker === row.ticker && 'bg-sky-500/10')}
                >
	                  <td className="px-3 py-2">
	                    <div className="font-mono font-semibold text-accent">{row.ticker}</div>
	                    <div className="text-[10px] text-neutral truncate max-w-[160px]">{row.company || row.screenerSource}</div>
	                  </td>
                  <td className="px-3 py-2">
                    <div className="font-mono text-slate-200">{row.activeSession || '--'}</div>
                    <div className="text-[10px] text-neutral">{row.marketCapBucket || '--'} · ${compact(row.marketCap)} · float {compact(row.sharesFloat)}</div>
                  </td>
	                  <td className={clsx('px-3 py-2 font-mono', row.priceChangePct >= 0 ? 'text-emerald-400' : 'text-red-400')}>
	                    {row.priceChangePct >= 0 ? '+' : ''}{row.priceChangePct.toFixed(2)}%
	                  </td>
	                  <td className="px-3 py-2 font-mono text-slate-200">{row.relativeVolume.toFixed(2)}x</td>
                  <td className="px-3 py-2">
                    <div className="font-mono text-slate-200">{compact(row.sharesFloat)}</div>
                    <div className="text-[10px] text-sky-300">{row.floatBucket || 'Unknown float'}</div>
                    <div className="text-[10px] text-neutral">short {row.floatShort == null ? '--' : `${Number(row.floatShort).toFixed(2)}%`}</div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="font-mono text-slate-200">{Number(row.marketCapRelativeVolumeScore || 0).toFixed(0)}/100</div>
                    <div className="text-[10px] text-sky-300">{row.relativeVolumeBucket || 'bucket'}</div>
                    <div className="text-[10px] text-neutral">target {Number(row.marketCapRelVolumeTarget || 0).toFixed(1)}x</div>
                  </td>
                  <td className="px-3 py-2">
                    <div className={clsx('font-mono', liquidityClass(row.liquidityStatus))}>{Number(row.liquidityScore || 0).toFixed(0)}/100</div>
                    <div className="text-[10px] uppercase text-neutral">{row.liquidityStatus || 'unknown'}</div>
                    <div className="text-[10px] text-neutral">{Number(row.liquidityDollarVolumeRatio || 0).toFixed(2)}x target</div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="font-mono text-slate-200">${compact(row.currentDollarVolume)}</div>
                    <div className="text-[10px] text-neutral">target ${compact(row.dollarVolumeTarget)}</div>
                  </td>
	                  <td className={clsx('px-3 py-2 font-mono', row.combinedSentiment >= minSentiment ? 'text-emerald-400' : row.combinedSentiment <= -minSentiment ? 'text-red-400' : 'text-neutral')}>
	                    {row.combinedSentiment.toFixed(2)}
	                  </td>
	                  <td className="px-3 py-2">
                    <div className="flex items-center gap-1 text-slate-200">
                      <span className={clsx('h-2 w-2 rounded-full', dotClass(row.decisionState || 'neutral_watchlist'))} />
                      {labelForDecisionState(row.decisionState)}
                    </div>
	                    <div className="text-[10px] text-neutral">{row.quadrant} · {labelForQuadrant(row.quadrant)}</div>
	                  </td>
                  <td className="px-3 py-2 font-mono text-slate-200">{row.activityScore.toFixed(0)}</td>
                  <td className="px-3 py-2 font-mono text-emerald-300">{row.convictionScore}</td>
                  <td className="px-3 py-2">
	                    <div className="text-slate-200 whitespace-nowrap">{row.catalystLabel || 'No catalyst'}</div>
	                    <div className="text-[10px] text-neutral">{row.structuredArticleCount ?? row.articleCount} structured · {row.unstructuredArticleCount ?? 0} unstructured · {row.socialCount} social</div>
                  </td>
                  <td className="px-3 py-2">
                    <div className="max-w-[360px] truncate text-slate-300">{row.latestNewsTitles?.[0]?.title || 'No recent headline'}</div>
                    {row.riskFlags.length > 0 && <div className="mt-1 text-[10px] text-yellow-300 truncate">{row.riskFlags.slice(0, 3).join(', ')}</div>}
                  </td>
                </tr>
              ))}
              {!sortedRows.length && (
                <tr>
	                  <td colSpan={14} className="px-3 py-10 text-center text-neutral">
                    No real screener-first rows match the current thresholds.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

function Control({ label, value, children }: { label: string; value: string; children: ReactNode }) {
  return (
    <label className="block">
      <div className="mb-1 flex items-center justify-between gap-2">
        <span className="text-[10px] uppercase text-neutral">{label}</span>
        <span className="font-mono text-[11px] text-slate-300">{value}</span>
      </div>
      {children}
    </label>
  )
}
