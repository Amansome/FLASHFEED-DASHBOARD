// Client-side chart aggregation shared by the Charts views.
//
// The backend serves ONE resolution: 1-minute extended-hours intraday OHLC
// (/api/sentchart/charts) and per-minute social density + 5-min sliding sentiment
// (/api/sentchart/chart/social). These helpers re-bucket and overlay that fine data in the
// browser so the price-chart timeframe selector and density/sentiment overlays
// recompute instantly with no extra server calls. There is no daily/weekly data,
// so timeframes are intraday only.

export interface Candle { time: number; open: number; high: number; low: number; close: number; volume?: number }
export interface LinePoint { time: number; value: number }
export interface BollingerBands { upper: LinePoint[]; lower: LinePoint[] }

// Social payload subset we read (from /api/sentchart/chart/social — see ResearchChart).
export interface SocialSeries {
  labels: string[]; density: number[]
  density_per_minute?: number[]
  sent_labels: string[]; scores?: number[]; scores_smooth: number[]; sentiment_weights?: number[]; win_density?: number[]
  times?: number[]
  sent_times?: number[]
  roll_window?: number
}

// Default display rolling window (minutes) when the server payload doesn't say.
// Entry/exit strategy math still owns its 360m correlation horizon separately;
// the professor-facing Price + Density display defaults to 240m.
export const ROLL_WINDOW_DEFAULT = 240

// CENTERED k-wide mean with zero padding at the edges (np.convolve
// mode='same' port of the backend's _smooth_same), skipping smoothing when
// len < k. Restored to centered at the owner's request (2026-07-10) — NOTE:
// at each plotted minute this window includes up to k/2 minutes of FUTURE
// messages, so the smoothed line is not causally interpretable for lead/lag
// timing (see research-students/FRESH_OTIS_ENTRY_FINDER_REPORT.md §1).
// Trailing rolling COUNT (owner's spec, 2026-07-13): the value at minute t is
// the total number of messages over the past k minutes, INCLUSIVE of the t-k
// edge — a 120-min window at 08:00 counts messages from 06:00 through 08:00,
// so posts at 06:00 and 07:00 give exactly 2. Backward-looking only; partial
// windows near the start of the timeline sum what exists. Replaces the
// centered average for the density overlay line.
export function trailingCount(values: number[], k: number): number[] {
  const n = values.length
  const out: number[] = []
  let sum = 0
  const window = Math.max(1, Math.round(k))
  for (let i = 0; i < n; i += 1) {
    sum += Number(values[i] || 0)
    if (i - window - 1 >= 0) sum -= Number(values[i - window - 1] || 0)
    out.push(sum)
  }
  return out
}

export function trailingWeightedAverage(values: number[], weights: number[], k: number): number[] {
  const out: number[] = []
  let weighted = 0
  let totalWeight = 0
  const window = Math.max(1, Math.round(k))
  for (let i = 0; i < values.length; i += 1) {
    const w = Number(weights[i] || 0)
    const v = Number(values[i] || 0)
    weighted += v * w
    totalWeight += w
    if (i - window - 1 >= 0) {
      const oldW = Number(weights[i - window - 1] || 0)
      weighted -= Number(values[i - window - 1] || 0) * oldW
      totalWeight -= oldW
    }
    out.push(totalWeight > 0 ? Number((weighted / totalWeight).toFixed(4)) : 0)
  }
  return out
}

export function smoothSame(values: number[], k: number): number[] {
  const n = values.length
  if (k <= 1 || n < k) return values.slice()
  const lead = Math.floor((k - 1) / 2)
  const out: number[] = []
  for (let i = 0; i < n; i++) {
    let s = 0
    for (let j = i - lead; j < i - lead + k; j++) {
      if (j >= 0 && j < n) s += values[j]
    }
    out.push(s / k)
  }
  return out
}

// Bucket start (unix seconds) for a clock-aligned timeframe of `tfMin` minutes.
export const bucketStart = (timeSec: number, tfMin: number): number => {
  const span = tfMin * 60
  return Math.floor(timeSec / span) * span
}

// Resample 1-minute candles into `tfMin`-minute buckets: open=first, high=max,
// low=min, close=last, volume=sum. Clock-aligned, newest order preserved. tfMin=1
// returns the input unchanged (deduped by bucket) so the default is a no-op.
export function resampleCandles(candles: Candle[], tfMin: number): Candle[] {
  if (tfMin <= 1 || candles.length === 0) return candles
  const buckets = new Map<number, Candle>()
  for (const c of candles) {
    const t = bucketStart(c.time, tfMin)
    const b = buckets.get(t)
    if (!b) {
      buckets.set(t, { time: t, open: c.open, high: c.high, low: c.low, close: c.close, volume: c.volume ?? 0 })
    } else {
      b.high = Math.max(b.high, c.high)
      b.low = Math.min(b.low, c.low)
      b.close = c.close                      // candles are in ascending time order
      b.volume = (b.volume ?? 0) + (c.volume ?? 0)
    }
  }
  return Array.from(buckets.values()).sort((a, b) => a.time - b.time)
}

// Bollinger(period, mult) from a candle series' closes — same convention as the
// server's _bollinger_series (SMA basis, ±mult·population stddev, first point at
// index period-1). Used to keep the band aligned when the timeframe is resampled.
export function bollingerFromCandles(candles: Candle[], period = 20, mult = 2): BollingerBands {
  const upper: LinePoint[] = [], lower: LinePoint[] = []
  if (candles.length < period) return { upper, lower }
  for (let i = period - 1; i < candles.length; i++) {
    let sum = 0
    for (let j = i - period + 1; j <= i; j++) sum += candles[j].close
    const m = sum / period
    let sq = 0
    for (let j = i - period + 1; j <= i; j++) sq += (candles[j].close - m) ** 2
    const sd = Math.sqrt(sq / period)
    upper.push({ time: candles[i].time, value: +(m + mult * sd).toFixed(4) })
    lower.push({ time: candles[i].time, value: +(m - mult * sd).toFixed(4) })
  }
  return { upper, lower }
}

// EMA over a series, seeded with values[0] — matches the server's _ema_list.
export function emaList(values: number[], period: number): number[] {
  if (!values.length) return []
  const k = 2 / (period + 1)
  const out: number[] = []
  let ema: number | null = null
  for (const v of values) { ema = ema === null ? v : (v - ema) * k + ema; out.push(ema) }
  return out
}

// Wilder's RSI(period) on a candle series — same convention as the server's
// _rsi_series (first value at close index `period`, 2dp). Recomputed on the
// resampled closes so RSI sits on the same time buckets as the candles.
export function rsiFromCandles(candles: Candle[], period = 14): LinePoint[] {
  const closes = candles.map(c => c.close), times = candles.map(c => c.time)
  const n = closes.length
  if (n < period + 1) return []
  const ch: number[] = []
  for (let i = 1; i < n; i++) ch.push(closes[i] - closes[i - 1])
  const gains = ch.map(c => (c > 0 ? c : 0))
  const losses = ch.map(c => (c < 0 ? -c : 0))
  let ag = gains.slice(0, period).reduce((a, b) => a + b, 0) / period
  let al = losses.slice(0, period).reduce((a, b) => a + b, 0) / period
  const val = (g: number, l: number) => (l === 0 ? 100 : 100 - 100 / (1 + g / l))
  const out: LinePoint[] = [{ time: times[period], value: +val(ag, al).toFixed(2) }]
  for (let i = period; i < ch.length; i++) {
    ag = (ag * (period - 1) + gains[i]) / period
    al = (al * (period - 1) + losses[i]) / period
    out.push({ time: times[i + 1], value: +val(ag, al).toFixed(2) })
  }
  return out
}

export interface MacdSeries { macd: LinePoint[]; signal: LinePoint[]; histogram: LinePoint[] }

// MACD(fast,slow,signal) on a candle series — same convention as the server's
// _macd_series (emit from index slow-1, 4dp). Recomputed on the resampled closes.
export function macdFromCandles(candles: Candle[], fast = 12, slow = 26, signal = 9): MacdSeries {
  const closes = candles.map(c => c.close), times = candles.map(c => c.time)
  const n = closes.length
  if (n < slow) return { macd: [], signal: [], histogram: [] }
  const ef = emaList(closes, fast), es = emaList(closes, slow)
  const line = closes.map((_, i) => ef[i] - es[i])
  const sig = emaList(line, signal)
  const macd: LinePoint[] = [], signalOut: LinePoint[] = [], histogram: LinePoint[] = []
  for (let i = slow - 1; i < n; i++) {
    macd.push({ time: times[i], value: +line[i].toFixed(4) })
    signalOut.push({ time: times[i], value: +sig[i].toFixed(4) })
    histogram.push({ time: times[i], value: +(line[i] - sig[i]).toFixed(4) })
  }
  return { macd, signal: signalOut, histogram }
}

// "HH:MM" ET wall-clock label for a candle's unix time. The backend encodes the
// naive ET minute as UTC, so reading the time in UTC yields the ET label that
// keys the social series.
function etLabel(timeSec: number): string {
  const d = new Date(timeSec * 1000)
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mm = String(d.getUTCMinutes()).padStart(2, '0')
  return `${hh}:${mm}`
}

// Build density / sentiment overlay lines aligned to the SAME timeframe buckets
// as resampleCandles(rawCandles, tfMin). Density is the trailing windowMin-min
// message COUNT; sentiment is the message-weighted trailing sentiment over that
// same window. Each bucket value is the mean of the per-minute rolling values
// over the minutes it spans, so the overlays stay registered to the candle x-axis.
export function overlaySeries(
  rawCandles: Candle[],
  social: SocialSeries | null,
  tfMin: number,
  windowMin = 15,
): { density: LinePoint[]; sentiment: LinePoint[] } {
  if (!social || !rawCandles.length) return { density: [], sentiment: [] }

  const candleBuckets = Array.from(new Set(
    rawCandles
      .map(c => bucketStart(Number(c.time), tfMin))
      .filter(t => Number.isFinite(t)),
  )).sort((a, b) => a - b)
  const minTime = Number(candleBuckets[0] ?? 0) - tfMin * 60
  const maxTime = Number(candleBuckets[candleBuckets.length - 1] ?? 0) + tfMin * 60
  const inVisibleRange = (t: number) => Number.isFinite(t) && t >= minTime && t <= maxTime
  const add = (acc: Map<number, { sum: number; n: number }>, time: number, value: number | undefined) => {
    if (!Number.isFinite(time) || !Number.isFinite(value as number) || !inVisibleRange(time)) return
    const bucket = bucketStart(time, tfMin)
    const a = acc.get(bucket) || { sum: 0, n: 0 }
    a.sum += value as number
    a.n += 1
    acc.set(bucket, a)
  }

  const rawDensity = Array.isArray(social.density) ? social.density.map(v => Number(v || 0)) : []
  const rawSentiment = social.scores?.length ? social.scores : (social.scores_smooth || [])
  const sentimentWeights = social.sentiment_weights?.length ? social.sentiment_weights : (social.density || [])
  const densSmooth = trailingCount(rawDensity, windowMin)
  const sentSmooth = trailingWeightedAverage(rawSentiment, sentimentWeights, windowMin)
  const dAcc = new Map<number, { sum: number; n: number }>()
  const sAcc = new Map<number, { sum: number; n: number }>()

  if (Array.isArray(social.times) && social.times.length) {
    social.times.forEach((t, i) => add(dAcc, Number(t), densSmooth[i]))
  } else {
    const densByLabel = new Map<string, number>()
    ;(social.labels || []).forEach((l, i) => densByLabel.set(l, densSmooth[i]))
    for (const c of rawCandles) {
      const label = etLabel(c.time)
      if (densByLabel.has(label)) add(dAcc, c.time, densByLabel.get(label))
    }
  }

  if (Array.isArray(social.sent_times) && social.sent_times.length) {
    social.sent_times.forEach((t, i) => add(sAcc, Number(t), sentSmooth[i]))
  } else {
    const sentByLabel = new Map<string, number>()
    ;(social.sent_labels || []).forEach((l, i) => sentByLabel.set(l, sentSmooth[i] ?? (social.scores_smooth || [])[i] ?? 0))
    for (const c of rawCandles) {
      const label = etLabel(c.time)
      if (sentByLabel.has(label)) add(sAcc, c.time, sentByLabel.get(label))
    }
  }

  const toLine = (acc: Map<number, { sum: number; n: number }>): LinePoint[] =>
    candleBuckets.map(time => {
      const v = acc.get(time)
      return { time, value: v && v.n > 0 ? Number((v.sum / v.n).toFixed(4)) : 0 }
    })

  return { density: toLine(dAcc), sentiment: toLine(sAcc) }
}
