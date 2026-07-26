export const PREDICTION_THRESHOLD_POLICY_VERSION = 'density_corr_float_guarded_w120_c038_pre60le4_msg3_partial_runner_open_guard_v11'
export const PREDICTION_THRESHOLD_FEATURE_SOURCE = 'mongo_ohlc_social_density_v11'

export const PREDICTION_THRESHOLD_POLICY = {
  version: PREDICTION_THRESHOLD_POLICY_VERSION,
  status: 'optimized_correlation_threshold_requires_validated_evidence',
  mechanics: {
    entry_execution: 'signal at end of minute t; execute at close of next real bar (t+1)',
    exit_rule: 'sell 50% at +5%; hold runner until 5% profit giveback after +10%; keep 3% protective stop and end-of-day flatten',
    correlation_definition: 'causal 120-minute rolling Pearson corr(price, trailing-smoothed message density), using the optimized post-backtest profile',
    late_entry_gate: 'reject entries when the ticker already moved more than 4% in the 60 minutes before the signal',
    active_move_gate: 'prefer controlled positive momentum at the signal; reject rows below 0% or above 12% active-session change before entry-ready status',
    validation_gate: 'current move alone is never enough; require recognized catalyst, verified squeeze/social-interest evidence, or a real message-density setup; low-float/Nano rows require stronger confirmation',
    float_gate: 'ultra-low and low-float rows require stronger message/catalyst/social/short-interest support; high-float rows require stronger liquidity/relative-volume support',
    overextension_gate: 'reject active-session moves that are already beyond the float-adjusted move cap before the signal',
    session_gate: 'premarket/weekend catalysts can queue candidates, but live trading entries require market-session confirmation unless explicitly shown as watch-only',
    opening_volatility_gate: 'during the first 20 regular-session minutes, require stronger message evidence, catalyst/social support, and a tighter pre-signal move so open-spike reversals do not promote as entry-ready',
    ohlc_note: 'v11 promotes the 120m / C=0.38 guarded threshold from the final Mongo OHLC sweep and keeps float-aware squeeze/overextension/evidence controls; exit mechanics are policy metadata for bracket planning and monitoring',
  },
  candidateRule: {
    name: 'float_guarded_w120_c0.38_pre60le4_msg3_partial50_runner_v11',
    entrySignal: 'corr_crosses_above_with_float_guarded_pre_move_message_evidence_gate',
    windowMinutes: 120,
    smoothingMinutes: 120,
    thresholdC: 0.38,
    setupNearThresholdBand: 0.04,
    maxPreSignalReturn60mPct: 4,
    minTrailing60Messages: 3,
    minSignalChangePct: 0,
    maxSignalChangePct: 12,
    openingVolatilityGuardMinutes: 20,
    openingMaxPreSignalReturn60mPct: 1.5,
    openingMinTrailing60MessagesMultiplier: 1.5,
    openingMaxSignalAbsChangePct: 8,
    exitStrategy: 'partial_profit_then_profit_giveback_runner',
    partialExitFraction: 0.5,
    partialProfitTargetPct: 5,
    profitGivebackPct: 5,
    profitGivebackActivationPct: 10,
    runnerTrailingStopPct: 99,
    legacyFallbackTrailingStopPct: 10,
    trailingStopPct: 99,
    protectiveStopPct: 3,
    maxSignalAbsChangePct: 30,
    maxSignalAbsChangePctByFloatBucket: {
      ultra_low: 20,
      low: 25,
      mid: 30,
      high: 40,
      very_high: 50,
      unknown: 30,
    },
    minRelVolumeByFloatBucket: {
      high: 1.5,
      very_high: 2,
    },
    floatEvidenceGates: {
      ultra_low: { minTrailing60Messages: 12, requireCatalystOrPositiveSocial: true, minSocialBullBearDelta: 2, minShortFloatPct: 8 },
      low: { minTrailing60Messages: 10, requireCatalystOrPositiveSocial: true, minSocialBullBearDelta: 1, minShortFloatPct: 10 },
      nano: { minTrailing60Messages: 12, requireCatalystOrPositiveSocial: true, minSocialBullBearDelta: 2, minShortFloatPct: 8 },
      small: { minTrailing60Messages: 8, requireCatalystOrPositiveSocial: true, minSocialBullBearDelta: 1, minShortFloatPct: 10 },
    },
    exitPlan: 'enter on the next real bar after the 120m correlation cross; sell 50% at +5%, keep a runner until 5% giveback after +10%, preserve 3% protective stop, flatten by end of day',
    sourceBacktest: 'backtests/message_density_thresholds/outputs_v11_final_candidate_mongo_ohlc',
    backtestSummary: {
      trades: 30,
      winRate: 0.6667,
      meanNetReturnPct: 1.6465,
      medianNetReturnPct: 0.8541,
      profitFactor: 3.0116,
      maxDrawdownPctPoints: -7.8,
      companionChecks: [
        '89-trade broader any-momentum companion: +0.9534% mean net, 50.56% win rate, PF 1.6963',
        '60-trade stricter pre60<=1 companion: +1.6572% mean net, 45.00% win rate, PF 2.0447',
      ],
      caveat: 'Final v11 candidate has the best balance of win rate, median return, drawdown, and expectancy; trade count remains moderate, so live labels should continue to be monitored before widening the gate.',
    },
  },
  tierRules: {
    Mega: {
      tier: 'Mega',
      name: 'tier_mega_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_with_news_validation_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 3,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      rationale: 'final v11 sweep favored a 120m correlation cross above 0.38 with controlled 0%-12% active momentum and a partial-profit runner exit',
    },
    Large: {
      tier: 'Large',
      name: 'tier_large_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_with_news_validation_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 3,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      rationale: 'final v11 sweep favored a 120m correlation cross above 0.38 with controlled 0%-12% active momentum and a partial-profit runner exit',
    },
    Mid: {
      tier: 'Mid',
      name: 'tier_mid_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_with_catalyst_or_density_validation_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 3,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      rationale: 'final v11 sweep favored a 120m correlation cross above 0.38 with controlled 0%-12% active momentum and a partial-profit runner exit',
    },
    Small: {
      tier: 'Small',
      name: 'tier_small_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_with_catalyst_or_squeeze_validation_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 8,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      rationale: 'final v11 sweep favored a 120m correlation cross above 0.38 with controlled 0%-12% active momentum; small/low-float rows stay evidence-gated',
    },
    Nano: {
      tier: 'Nano',
      name: 'tier_nano_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_plus_message_squeeze_gate_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 12,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      backtestSummary: {
        sourceBacktest: 'backtests/message_density_thresholds/outputs_v6_old_anchor_mongo_ohlc',
        caveat: 'old nano exact rule was strongly negative; v11 keeps nano on the global improved threshold but requires stronger message validation before entry-ready status',
      },
      rationale: 'final v11 sweep favored a 120m correlation cross above 0.38 with controlled 0%-12% active momentum; nano remains evidence-gated',
    },
    Unknown: {
      tier: 'Unknown',
      name: 'tier_unknown_optimized_w120_c0.38_pre60le4_msg3_partial_runner',
      entrySignal: 'corr_crosses_above_with_conservative_missing_cap_gate_and_partial_runner_exit',
      windowMinutes: 120,
      smoothingMinutes: 120,
      thresholdC: 0.38,
      setupNearThresholdBand: 0.04,
      maxPreSignalReturn60mPct: 4,
      minTrailing60Messages: 3,
      minSignalChangePct: 0,
      maxSignalChangePct: 12,
      exitStrategy: 'partial_profit_then_profit_giveback_runner',
      trailingStopPct: 99,
      protectiveStopPct: 3,
      rationale: 'missing market cap cannot be tiered honestly, so use the optimized global v11 gate while preserving the missing-cap label',
    },
  },
  priorCandidateRule: {
    name: 'broad_w120_c038_pre4_msg3_runner_anymom_partial50_companion',
    windowMinutes: 120,
    smoothingMinutes: 120,
    thresholdC: 0.38,
    maxPreSignalReturn60mPct: 4,
    minTrailing60Messages: 3,
    minSignalChangePct: null,
    maxSignalChangePct: null,
    trailingStopPct: 99,
    protectiveStopPct: 3,
    backtestSummary: {
      trades: 89,
      winRate: 0.5056,
      meanNetReturnPct: 0.9534,
      medianNetReturnPct: 0.1,
      profitFactor: 1.6963,
      maxDrawdownPctPoints: -17.2,
    },
  },
  aggressiveResearchRule: {
    name: 'current_live_w180_c036_msg8_runner_anymom_high_mean_reference',
    windowMinutes: 180,
    smoothingMinutes: 180,
    thresholdC: 0.36,
    maxPreSignalReturn60mPct: 1,
    minTrailing60Messages: 8,
    trailingStopPct: 99,
    protectiveStopPct: 3,
    backtestSummary: {
      trades: 28,
      winRate: 0.5,
      meanNetReturnPct: 3.4193,
      medianNetReturnPct: 0.0744,
      profitFactor: 3.6036,
      maxDrawdownPctPoints: -11.909,
      caveat: 'Higher average return than promoted v11, but thinner sample and much weaker median.',
    },
  },
  submittedBaseline: {
    Mega: { tier: 'Mega', entrySignal: 'corr_crosses_above', windowMinutes: 240, smoothingMinutes: 240, thresholdC: 0.1, trailingStopPct: 3, protectiveStopPct: 3 },
    Large: { tier: 'Large', entrySignal: 'corr_crosses_above', windowMinutes: 480, smoothingMinutes: 480, thresholdC: 0.1, trailingStopPct: 2, protectiveStopPct: 3 },
    Mid: { tier: 'Mid', entrySignal: 'corr_crosses_above', windowMinutes: 60, smoothingMinutes: 60, thresholdC: 0.3, trailingStopPct: 2, protectiveStopPct: 3 },
    Small: { tier: 'Small', entrySignal: 'corr_crosses_above', windowMinutes: 240, smoothingMinutes: 240, thresholdC: 0.1, trailingStopPct: 2, protectiveStopPct: 3 },
    Nano: { tier: 'Nano', entrySignal: 'corr_crosses_above', windowMinutes: 60, smoothingMinutes: 60, thresholdC: 0.1, trailingStopPct: 5, protectiveStopPct: 3 },
  },
  pooledWindows: [
    { tier: 'Mega', windowMinutes: 120, thresholdC: 0.38, trailingStopPct: 99, maxPreSignalReturn60mPct: 4, minTrailing60Messages: 3, minSignalChangePct: 0, maxSignalChangePct: 12, status: 'optimized_w120_c0.38_partial_runner_pre60le4_msg3' },
    { tier: 'Large', windowMinutes: 120, thresholdC: 0.38, trailingStopPct: 99, maxPreSignalReturn60mPct: 4, minTrailing60Messages: 3, minSignalChangePct: 0, maxSignalChangePct: 12, status: 'optimized_w120_c0.38_partial_runner_pre60le4_msg3' },
    { tier: 'Mid', windowMinutes: 120, thresholdC: 0.38, trailingStopPct: 99, maxPreSignalReturn60mPct: 4, minTrailing60Messages: 3, minSignalChangePct: 0, maxSignalChangePct: 12, status: 'optimized_w120_c0.38_partial_runner_pre60le4_msg3' },
    { tier: 'Small', windowMinutes: 120, thresholdC: 0.38, trailingStopPct: 99, maxPreSignalReturn60mPct: 4, minTrailing60Messages: 8, minSignalChangePct: 0, maxSignalChangePct: 12, status: 'optimized_w120_c0.38_partial_runner_pre60le4_msg3_float_guarded' },
    { tier: 'Nano', windowMinutes: 120, thresholdC: 0.38, trailingStopPct: 99, maxPreSignalReturn60mPct: 4, minTrailing60Messages: 12, minSignalChangePct: 0, maxSignalChangePct: 12, status: 'optimized_w120_c0.38_partial_runner_pre60le4_msg3_float_guarded' },
  ],
}

export function clonePlain(value) {
  return JSON.parse(JSON.stringify(value))
}

const PROFILE_OVERRIDE_KEYS = new Set([
  'policyVersion',
  'label',
  'name',
  'entrySignal',
  'windowMinutes',
  'smoothingMinutes',
  'thresholdC',
  'setupNearThresholdBand',
  'maxPreSignalReturn60mPct',
  'minTrailing60Messages',
  'minSignalChangePct',
  'maxSignalChangePct',
  'maxSignalAbsChangePct',
  'maxSignalAbsChangePctByFloatBucket',
  'minRelVolumeByFloatBucket',
  'floatEvidenceGates',
  'exitStrategy',
  'partialExitFraction',
  'partialProfitTargetPct',
  'profitGivebackPct',
  'profitGivebackActivationPct',
  'runnerTrailingStopPct',
  'legacyFallbackTrailingStopPct',
  'trailingStopPct',
  'protectiveStopPct',
  'exitPlan',
])

const FEATURE_KEYS = new Set([
  'price_density_correlation',
  'previous_price_density_correlation',
  'threshold_pre_return_60m_pct',
  'pre_signal_return_60m_pct',
  'pre_return_60m_pct',
  'threshold_trailing_60m_messages',
  'trailing_60m_messages',
  'trailing60Messages',
])

function isPlainObject(value) {
  return value && typeof value === 'object' && !Array.isArray(value)
}

function looksLikeProfileOverride(value) {
  if (!isPlainObject(value)) return false
  const keys = Object.keys(value)
  if (!keys.length) return false
  const hasProfileKey = keys.some(key => PROFILE_OVERRIDE_KEYS.has(key))
  const hasFeatureKey = keys.some(key => FEATURE_KEYS.has(key))
  return hasProfileKey && !hasFeatureKey
}

function splitFeaturesAndProfileOverride(featuresOrOverride = {}, profileOverride = null) {
  if (profileOverride && isPlainObject(profileOverride)) {
    return { features: isPlainObject(featuresOrOverride) ? featuresOrOverride : {}, profileOverride }
  }
  if (looksLikeProfileOverride(featuresOrOverride)) {
    return { features: {}, profileOverride: featuresOrOverride }
  }
  return { features: isPlainObject(featuresOrOverride) ? featuresOrOverride : {}, profileOverride: null }
}

export function marketCapBucket(marketCap) {
  const cap = Number(marketCap || 0)
  if (cap >= 200e9) return 'Mega'
  if (cap >= 10e9) return 'Large'
  if (cap >= 2e9) return 'Mid'
  if (cap >= 300e6) return 'Small'
  if (cap > 0) return 'Micro'
  return 'Unknown'
}

export function predictionMarketCapTier(row = {}) {
  const explicit = String(row.market_cap_tier || row.finviz_market_cap_tier || '').trim().toLowerCase()
  const bucket = String(row.market_cap_bucket || marketCapBucket(row.market_cap)).trim().toLowerCase()
  if (explicit === 'mega' || bucket === 'mega') return 'Mega'
  if (explicit === 'large' || bucket === 'large') return 'Large'
  if (explicit === 'mid' || bucket === 'mid') return 'Mid'
  if (explicit === 'small' || bucket === 'small') return 'Small'
  if (explicit === 'nano' || explicit === 'micro' || bucket === 'nano' || bucket === 'micro') return 'Nano'
  return 'Unknown'
}

function nullableNumber(value) {
  if (value == null || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function clamp(value, min = -1, max = 1) {
  const n = Number(value)
  if (!Number.isFinite(n)) return NaN
  return Math.max(min, Math.min(max, n))
}

export function normalizedSharesFloat(row = {}) {
  const value = nullableNumber(row.shares_float ?? row.float ?? row.float_shares)
  return value != null && value > 0 ? value : null
}

export function predictionFloatBucket(row = {}) {
  const floatShares = normalizedSharesFloat(row)
  if (floatShares == null) return 'unknown'
  if (floatShares < 10_000_000) return 'ultra_low'
  if (floatShares < 25_000_000) return 'low'
  if (floatShares < 50_000_000) return 'mid'
  if (floatShares < 100_000_000) return 'high'
  return 'very_high'
}

export function floatBucketLabel(bucket = 'unknown') {
  if (bucket === 'ultra_low') return 'ultra-low float'
  if (bucket === 'low') return 'low float'
  if (bucket === 'mid') return 'mid float'
  if (bucket === 'high') return 'high float'
  if (bucket === 'very_high') return 'very high float'
  return 'unknown float'
}

export function socialBullBearDelta(row = {}) {
  return Number(row.bullish_count || 0) - Number(row.bearish_count || 0)
}

export function catalystCount(row = {}) {
  return Number(row.article_count ?? row.news_article_count ?? 0)
}

export function positiveSocialSupport(row = {}, minDelta = 1) {
  return Number(row.message_count || 0) > 0 &&
    (socialBullBearDelta(row) >= minDelta || Number(row.social_sentiment || row.sentiment || 0) >= 0.12)
}

function signalMinuteOfDayEt(row = {}) {
  const rawSec = row.threshold_feature_snapshot_sec ?? row.thresholdFeatureSnapshotSec ?? row.latest_signal_epoch ?? row.signal_epoch
  const sec = Number(rawSec)
  if (Number.isFinite(sec) && sec > 0) {
    try {
      const parts = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }).formatToParts(new Date(sec * 1000))
      const hour = Number(parts.find(part => part.type === 'hour')?.value)
      const minute = Number(parts.find(part => part.type === 'minute')?.value)
      if (Number.isFinite(hour) && Number.isFinite(minute)) return (hour % 24) * 60 + minute
    } catch {
      // Fall through to string-based fields.
    }
  }

  const rawTime = row.threshold_feature_minute ?? row.signal_minute ?? row.corr_minute ?? row.entry_time
  const match = typeof rawTime === 'string' ? rawTime.match(/(\d{1,2}):(\d{2})/) : null
  if (match) {
    const hour = Number(match[1])
    const minute = Number(match[2])
    if (Number.isFinite(hour) && Number.isFinite(minute)) return (hour % 24) * 60 + minute
  }
  return null
}

export function thresholdGuardEvaluation(row = {}, profile = {}, baseMinTrailing60Messages = 0) {
  const floatBucket = predictionFloatBucket(row)
  const tier = predictionMarketCapTier(row)
  const lowFloat = floatBucket === 'ultra_low' || floatBucket === 'low'
  const smallOrNano = tier === 'Nano' || tier === 'Small'
  const gates = profile.floatEvidenceGates || {}
  const floatGate = gates[floatBucket] || {}
  const tierGate = tier === 'Nano' ? gates.nano : tier === 'Small' ? gates.small : {}
  const activeGate = { ...tierGate, ...floatGate }
  const requiredTrailingMessages = Math.max(baseMinTrailing60Messages, Number(activeGate.minTrailing60Messages || 0))
  const trailing60Messages = nullableNumber(row.threshold_trailing_60m_messages ?? row.trailing_60m_messages ?? row.trailing60Messages)
  const activeChange = Number(row.change_pct || 0)
  const pre60 = nullableNumber(row.threshold_pre_return_60m_pct ?? row.pre_signal_return_60m_pct ?? row.pre_return_60m_pct)
  const absChange = Math.abs(activeChange)
  const capByFloat = profile.maxSignalAbsChangePctByFloatBucket || {}
  const maxAbsChange = Number(capByFloat[floatBucket] ?? profile.maxSignalAbsChangePct ?? 30)
  const minSignalChange = profile.minSignalChangePct == null ? null : Number(profile.minSignalChangePct)
  const maxSignalChange = profile.maxSignalChangePct == null ? null : Number(profile.maxSignalChangePct)
  const relVolume = Number(row.rel_volume || 0)
  const minRelByFloat = profile.minRelVolumeByFloatBucket || {}
  const minRelVolume = Number(minRelByFloat[floatBucket] || 0)
  const floatShort = nullableNumber(row.float_short ?? row.short_interest_pct_float ?? row.short_interest_pct) || 0
  const hasCatalyst = catalystCount(row) > 0
  const socialOk = positiveSocialSupport(row, Number(activeGate.minSocialBullBearDelta || 1))
  const shortOk = floatShort >= Number(activeGate.minShortFloatPct || Infinity)
  const requireEvidence = Boolean(activeGate.requireCatalystOrPositiveSocial || lowFloat || smallOrNano)
  const evidenceOk = !requireEvidence || hasCatalyst || socialOk || shortOk
  const relVolumeOk = !minRelVolume || relVolume >= minRelVolume
  const overextensionOk = !Number.isFinite(maxAbsChange) || maxAbsChange <= 0 || absChange <= maxAbsChange
  const minSignalChangeOk = minSignalChange == null || !Number.isFinite(minSignalChange) || activeChange >= minSignalChange
  const maxSignalChangeOk = maxSignalChange == null || !Number.isFinite(maxSignalChange) || activeChange <= maxSignalChange
  const signalChangeBandOk = minSignalChangeOk && maxSignalChangeOk
  const signalMinuteEt = signalMinuteOfDayEt(row)
  const marketOpenMinuteEt = 9 * 60 + 30
  const openingGuardMinutes = Math.max(0, Number(profile.openingVolatilityGuardMinutes || 0))
  const minutesSinceOpen = signalMinuteEt == null ? null : signalMinuteEt - marketOpenMinuteEt
  const inOpeningVolatilityGuard = minutesSinceOpen != null && minutesSinceOpen >= 0 && minutesSinceOpen < openingGuardMinutes
  const openingRequiredMessages = Math.ceil(requiredTrailingMessages * Math.max(1, Number(profile.openingMinTrailing60MessagesMultiplier || 1)))
  const openingMaxPre60 = Number(profile.openingMaxPreSignalReturn60mPct ?? profile.maxPreSignalReturn60mPct ?? Infinity)
  const openingMaxAbsChange = Number(profile.openingMaxSignalAbsChangePct ?? maxAbsChange)
  const openingMessageOk = trailing60Messages == null || trailing60Messages >= openingRequiredMessages
  const openingPreMoveOk = pre60 == null || !Number.isFinite(openingMaxPre60) || pre60 <= openingMaxPre60
  const openingAbsMoveOk = !Number.isFinite(openingMaxAbsChange) || openingMaxAbsChange <= 0 || absChange <= openingMaxAbsChange
  const openingEvidenceOk = hasCatalyst || socialOk || shortOk
  const openingVolatilityOk = !inOpeningVolatilityGuard ||
    (openingMessageOk && openingPreMoveOk && openingAbsMoveOk && openingEvidenceOk)
  const rejectionReasons = []
  if (!overextensionOk) rejectionReasons.push(`overextended_${absChange.toFixed(2)}pct_gt_${maxAbsChange}pct_${floatBucket}`)
  if (!minSignalChangeOk) rejectionReasons.push(`active_move_${activeChange.toFixed(2)}pct_lt_${minSignalChange}pct`)
  if (!maxSignalChangeOk) rejectionReasons.push(`active_move_${activeChange.toFixed(2)}pct_gt_${maxSignalChange}pct`)
  if (!evidenceOk) rejectionReasons.push(`${floatBucketLabel(floatBucket).replace(/\s+/g, '_')}_${tier.toLowerCase()}_needs_catalyst_positive_social_or_short_interest`)
  if (!relVolumeOk) rejectionReasons.push(`${floatBucketLabel(floatBucket).replace(/\s+/g, '_')}_needs_rel_volume_${minRelVolume}`)
  if (!openingVolatilityOk) {
    const misses = []
    if (!openingMessageOk) misses.push(`messages_${trailing60Messages ?? 'missing'}_lt_${openingRequiredMessages}`)
    if (!openingPreMoveOk) misses.push(`pre60_${pre60 ?? 'missing'}_gt_${openingMaxPre60}`)
    if (!openingAbsMoveOk) misses.push(`active_abs_${absChange.toFixed(2)}pct_gt_${openingMaxAbsChange}pct`)
    if (!openingEvidenceOk) misses.push('needs_catalyst_social_or_short_interest')
    rejectionReasons.push(`opening_volatility_guard_${minutesSinceOpen}m_after_open_${misses.join('_')}`)
  }
  return {
    floatBucket,
    floatBucketLabel: floatBucketLabel(floatBucket),
    sharesFloat: normalizedSharesFloat(row),
    lowFloat,
    tier,
    requiredTrailingMessages,
    trailing60Messages,
    minSignalChangePct: Number.isFinite(minSignalChange) ? minSignalChange : null,
    maxSignalChangePct: Number.isFinite(maxSignalChange) ? maxSignalChange : null,
    activeSignalChangePct: Number(activeChange.toFixed(3)),
    maxSignalAbsChangePct: maxAbsChange,
    activeSignalAbsChangePct: Number(absChange.toFixed(3)),
    minRelVolumeAtSignal: minRelVolume || null,
    relVolumeAtSignal: Number.isFinite(relVolume) ? Number(relVolume.toFixed(3)) : null,
    requireEvidence,
    hasCatalyst,
    positiveSocialSupport: socialOk,
    shortInterestSupport: shortOk,
    floatShortPct: floatShort || null,
    socialBullBearDelta: socialBullBearDelta(row),
    catalystCount: catalystCount(row),
    signalMinuteEt,
    minutesSinceRegularOpen: minutesSinceOpen,
    inOpeningVolatilityGuard,
    openingGuardMinutes,
    openingRequiredTrailingMessages: inOpeningVolatilityGuard ? openingRequiredMessages : null,
    openingMaxPreSignalReturn60mPct: inOpeningVolatilityGuard ? openingMaxPre60 : null,
    openingMaxSignalAbsChangePct: inOpeningVolatilityGuard ? openingMaxAbsChange : null,
    overextensionOk,
    minSignalChangeOk,
    maxSignalChangeOk,
    signalChangeBandOk,
    evidenceOk,
    relVolumeOk,
    openingVolatilityOk,
    passed: overextensionOk && signalChangeBandOk && evidenceOk && relVolumeOk && openingVolatilityOk,
    rejectionReasons,
  }
}

export function predictionThresholdProfile(row = {}, profileOverride = null) {
  const tier = predictionMarketCapTier(row)
  const baseProfile = PREDICTION_THRESHOLD_POLICY.candidateRule || {}
  const tierProfile = PREDICTION_THRESHOLD_POLICY.tierRules?.[tier] || {}
  const override = isPlainObject(profileOverride) ? profileOverride : {}
  const profile = {
    ...baseProfile,
    ...tierProfile,
    ...override,
    exitStrategy: override.exitStrategy ?? baseProfile.exitStrategy,
    partialExitFraction: override.partialExitFraction ?? baseProfile.partialExitFraction,
    partialProfitTargetPct: override.partialProfitTargetPct ?? baseProfile.partialProfitTargetPct,
    profitGivebackPct: override.profitGivebackPct ?? baseProfile.profitGivebackPct,
    profitGivebackActivationPct: override.profitGivebackActivationPct ?? baseProfile.profitGivebackActivationPct,
    runnerTrailingStopPct: override.runnerTrailingStopPct ?? baseProfile.runnerTrailingStopPct,
    legacyFallbackTrailingStopPct: override.legacyFallbackTrailingStopPct ?? baseProfile.legacyFallbackTrailingStopPct,
    maxSignalAbsChangePct: override.maxSignalAbsChangePct ?? baseProfile.maxSignalAbsChangePct,
    maxSignalAbsChangePctByFloatBucket: override.maxSignalAbsChangePctByFloatBucket ?? baseProfile.maxSignalAbsChangePctByFloatBucket,
    minSignalChangePct: override.minSignalChangePct ?? baseProfile.minSignalChangePct,
    maxSignalChangePct: override.maxSignalChangePct ?? baseProfile.maxSignalChangePct,
    openingVolatilityGuardMinutes: override.openingVolatilityGuardMinutes ?? baseProfile.openingVolatilityGuardMinutes,
    openingMaxPreSignalReturn60mPct: override.openingMaxPreSignalReturn60mPct ?? baseProfile.openingMaxPreSignalReturn60mPct,
    openingMinTrailing60MessagesMultiplier: override.openingMinTrailing60MessagesMultiplier ?? baseProfile.openingMinTrailing60MessagesMultiplier,
    openingMaxSignalAbsChangePct: override.openingMaxSignalAbsChangePct ?? baseProfile.openingMaxSignalAbsChangePct,
    minRelVolumeByFloatBucket: override.minRelVolumeByFloatBucket ?? baseProfile.minRelVolumeByFloatBucket,
    floatEvidenceGates: override.floatEvidenceGates ?? baseProfile.floatEvidenceGates,
  }
  return {
    policyVersion: override.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
    tier,
    overrideProfile: override.label || override.name || null,
    profile: clonePlain(profile),
    pooledBacktestProfile: clonePlain(PREDICTION_THRESHOLD_POLICY.candidateRule),
    tierRules: clonePlain(PREDICTION_THRESHOLD_POLICY.tierRules),
    pooledWindows: clonePlain(PREDICTION_THRESHOLD_POLICY.pooledWindows),
    submittedBaseline: clonePlain(PREDICTION_THRESHOLD_POLICY.submittedBaseline),
    priorCandidateRule: clonePlain(PREDICTION_THRESHOLD_POLICY.priorCandidateRule),
    aggressiveResearchRule: clonePlain(PREDICTION_THRESHOLD_POLICY.aggressiveResearchRule),
    mechanics: clonePlain(PREDICTION_THRESHOLD_POLICY.mechanics),
  }
}

export function evaluatePredictionEntryThreshold(row = {}, featuresOrOverride = {}, profileOverride = null) {
  const split = splitFeaturesAndProfileOverride(featuresOrOverride, profileOverride)
  const features = split.features
  const threshold = predictionThresholdProfile(row, split.profileOverride)
  const profile = threshold.profile
  const rawCorr = row.price_density_correlation ?? row.priceDensityCorrelation ?? features.price_density_correlation
  const rawPrevCorr = row.previous_price_density_correlation ?? row.prevPriceDensityCorrelation ?? features.previous_price_density_correlation
  const rawPre60 = row.threshold_pre_return_60m_pct ?? row.pre_signal_return_60m_pct ?? row.pre_return_60m_pct ?? features.threshold_pre_return_60m_pct
  const rawTrailing60Messages = row.threshold_trailing_60m_messages ?? row.trailing_60m_messages ?? row.trailing60Messages ?? features.threshold_trailing_60m_messages
  const corr = rawCorr == null || rawCorr === '' ? NaN : clamp(Number(rawCorr), -1, 1)
  const prevCorr = rawPrevCorr == null || rawPrevCorr === '' ? NaN : clamp(Number(rawPrevCorr), -1, 1)
  const pre60 = rawPre60 == null || rawPre60 === '' ? NaN : Number(rawPre60)
  const trailing60Messages = rawTrailing60Messages == null || rawTrailing60Messages === '' ? NaN : Number(rawTrailing60Messages)
  const hasCorr = Number.isFinite(corr)
  const hasPrev = Number.isFinite(prevCorr)
  const hasPre60 = Number.isFinite(pre60)
  const hasTrailing60Messages = Number.isFinite(trailing60Messages)
  const crossed = hasCorr && hasPrev && prevCorr <= profile.thresholdC && corr > profile.thresholdC
  const preMoveOk = hasPre60 && pre60 <= profile.maxPreSignalReturn60mPct
  const guard = thresholdGuardEvaluation(row, profile, Number(profile.minTrailing60Messages || 0))
  const minTrailing60Messages = guard.requiredTrailingMessages
  const messagesOk = minTrailing60Messages <= 0 || (hasTrailing60Messages && trailing60Messages >= minTrailing60Messages)
  const passed = crossed && preMoveOk && messagesOk && guard.passed
  const nearBand = Number(profile.setupNearThresholdBand || 0.05)
  const aboveThreshold = hasCorr && corr > profile.thresholdC
  const nearThreshold = hasCorr && corr >= profile.thresholdC - nearBand && corr <= profile.thresholdC
  const setupReady = preMoveOk && messagesOk && guard.passed
  const status = !hasCorr || !hasPrev
    ? 'missing_price_density_correlation_history'
    : !hasPre60
      ? 'missing_pre_signal_60m_return'
      : minTrailing60Messages > 0 && !hasTrailing60Messages
        ? 'missing_trailing_60m_message_count'
        : crossed && !preMoveOk
          ? 'late_entry_rejected'
          : crossed && !messagesOk
            ? 'low_message_density_rejected'
            : crossed && !guard.overextensionOk
              ? 'overextended_move_rejected'
              : crossed && !guard.signalChangeBandOk
                ? 'active_momentum_band_rejected'
                : crossed && !guard.evidenceOk
                  ? 'float_or_tier_evidence_rejected'
                  : crossed && !guard.relVolumeOk
                    ? 'high_float_liquidity_rejected'
                    : crossed && !guard.openingVolatilityOk
                      ? 'opening_volatility_rejected'
                      : passed
                        ? 'entry_passed'
                        : 'entry_not_crossed'
  const setupStatus = !hasCorr || !hasPrev
    ? 'missing_price_density_correlation_history'
    : !hasPre60
      ? 'missing_pre_signal_60m_return'
      : passed
        ? 'entry_passed'
        : aboveThreshold && setupReady
          ? 'active_setup_already_above_threshold'
          : nearThreshold && setupReady
            ? 'near_threshold_setup'
            : status === 'late_entry_rejected'
              ? 'late_setup_rejected'
              : status === 'low_message_density_rejected'
                ? 'low_message_density_rejected'
                : status === 'active_momentum_band_rejected'
                  ? 'active_momentum_band_rejected'
                  : status === 'opening_volatility_rejected'
                    ? 'opening_volatility_rejected'
                  : 'inactive'
  const setupScore = setupStatus === 'entry_passed'
    ? 100
    : setupStatus === 'active_setup_already_above_threshold'
      ? 75
      : setupStatus === 'near_threshold_setup'
        ? 55
        : setupStatus === 'late_setup_rejected'
          ? 25
          : 0
  const setupReason = hasCorr && hasPrev && hasPre60
    ? setupStatus === 'active_setup_already_above_threshold'
      ? `Active setup: ${profile.windowMinutes}m corr(price,density) is ${corr.toFixed(3)}, above ${profile.thresholdC}, and prior 60m move is ${pre60.toFixed(2)}%; no fresh cross on the latest bar.`
      : setupStatus === 'near_threshold_setup'
        ? `Near setup: ${profile.windowMinutes}m corr(price,density) is ${corr.toFixed(3)}, within ${nearBand.toFixed(2)} of the ${profile.thresholdC} entry threshold, and prior 60m move is ${pre60.toFixed(2)}%.`
        : setupStatus === 'late_setup_rejected'
          ? `Late setup rejected: correlation crossed above ${profile.thresholdC}, but prior 60m move was ${pre60.toFixed(2)}%, above the ${profile.maxPreSignalReturn60mPct}% limit.`
          : setupStatus === 'active_momentum_band_rejected'
            ? `Active momentum band rejected: active move was ${guard.activeSignalChangePct}%, outside the ${guard.minSignalChangePct ?? '-inf'}% to ${guard.maxSignalChangePct ?? '+inf'}% v11 range.`
            : setupStatus === 'entry_passed'
              ? `Entry passed: correlation crossed above ${profile.thresholdC}, prior 60m move was ${pre60.toFixed(2)}%, and trailing messages met the ${minTrailing60Messages} minimum.`
              : setupStatus === 'opening_volatility_rejected'
                ? `Opening volatility rejected: signal formed ${guard.minutesSinceRegularOpen} minutes after 9:30 ET and failed the stricter open guard.`
              : `Inactive: ${profile.windowMinutes}m corr(price,density) ${prevCorr.toFixed(3)} -> ${corr.toFixed(3)} has not formed an entry setup.`
    : 'Setup diagnostics require current/previous rolling corr(price,density), prior 60m price return, and trailing 60m message count.'

  return {
    ...threshold,
    applied: true,
    passed,
    status,
    correlation: hasCorr ? Number(corr.toFixed(3)) : null,
    previousCorrelation: hasPrev ? Number(prevCorr.toFixed(3)) : null,
    preSignalReturn60mPct: hasPre60 ? Number(pre60.toFixed(3)) : null,
    trailing60Messages: hasTrailing60Messages ? trailing60Messages : null,
    thresholdC: profile.thresholdC,
    setupNearThresholdBand: nearBand,
    minTrailing60Messages,
    maxPreSignalReturn60mPct: profile.maxPreSignalReturn60mPct,
    minSignalChangePct: guard.minSignalChangePct,
    maxSignalChangePct: guard.maxSignalChangePct,
    activeSignalChangePct: guard.activeSignalChangePct,
    signalChangeBandOk: guard.signalChangeBandOk,
    openingVolatilityOk: guard.openingVolatilityOk,
    openingVolatilityGuard: {
      active: guard.inOpeningVolatilityGuard,
      minutes_since_regular_open: guard.minutesSinceRegularOpen,
      guard_minutes: guard.openingGuardMinutes,
      required_trailing_messages: guard.openingRequiredTrailingMessages,
      max_pre_signal_return_60m_pct: guard.openingMaxPreSignalReturn60mPct,
      max_signal_abs_change_pct: guard.openingMaxSignalAbsChangePct,
    },
    maxSignalAbsChangePct: guard.maxSignalAbsChangePct,
    activeSignalAbsChangePct: guard.activeSignalAbsChangePct,
    floatBucket: guard.floatBucket,
    floatBucketLabel: guard.floatBucketLabel,
    sharesFloat: guard.sharesFloat,
    floatGate: guard,
    rejectionReasons: guard.rejectionReasons,
    setupStatus,
    setupScore,
    setupReady: setupStatus === 'entry_passed' || setupStatus === 'active_setup_already_above_threshold' || setupStatus === 'near_threshold_setup',
    setupReason,
    distanceToEntry: hasCorr ? Number((corr - profile.thresholdC).toFixed(3)) : null,
    exitStrategy: profile.exitStrategy || null,
    exitPlan: profile.exitPlan || null,
    partialExitFraction: profile.partialExitFraction ?? null,
    partialProfitTargetPct: profile.partialProfitTargetPct ?? null,
    profitGivebackPct: profile.profitGivebackPct ?? null,
    profitGivebackActivationPct: profile.profitGivebackActivationPct ?? null,
    runnerTrailingStopPct: profile.runnerTrailingStopPct ?? null,
    legacyFallbackTrailingStopPct: profile.legacyFallbackTrailingStopPct ?? null,
    trailingStopPct: profile.trailingStopPct,
    protectiveStopPct: profile.protectiveStopPct,
    reason: hasCorr && hasPrev && hasPre60
      ? `${profile.windowMinutes}m corr(price,density) ${prevCorr.toFixed(3)} -> ${corr.toFixed(3)}; required cross above ${profile.thresholdC}; prior 60m move ${pre60.toFixed(2)}% must be <= ${profile.maxPreSignalReturn60mPct}%; trailing 60m messages ${hasTrailing60Messages ? trailing60Messages : 'missing'} must be >= ${minTrailing60Messages}; active move ${guard.activeSignalChangePct}% must be between ${guard.minSignalChangePct ?? '-inf'}% and ${guard.maxSignalChangePct ?? '+inf'}% and abs move ${guard.activeSignalAbsChangePct}% must be <= ${guard.maxSignalAbsChangePct}% for ${guard.floatBucketLabel}; opening guard ${guard.inOpeningVolatilityGuard ? (guard.openingVolatilityOk ? 'passed' : `failed (${guard.rejectionReasons.join(', ')})`) : 'not active'}; evidence gate ${guard.evidenceOk ? 'passed' : `failed (${guard.rejectionReasons.join(', ')})`}.`
      : 'Candidate threshold requires current/previous rolling corr(price,density), prior 60m price return, and trailing 60m message count; one or more inputs are unavailable.',
  }
}

export function predictionPolicyCacheNamespace() {
  return `prediction:${PREDICTION_THRESHOLD_POLICY_VERSION}`
}
