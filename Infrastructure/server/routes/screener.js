import { Router } from 'express'
import mongoose from 'mongoose'
import Screener from '../models/Screener.js'
import { loadWatcherFeatureMap, persistWatcherSnapshot, watcherRankScore } from '../lib/watcherSnapshots.js'
import * as predictionThresholdPolicy from '../lib/predictionThresholdPolicy.js'

const router = Router()
const NON_STOCK_TICKERS = new Set([
  'BTC', 'ETH', 'LTC', 'DOGE', 'SOL', 'ADA', 'XRP', 'BNB', 'DOT', 'AVAX',
  'MATIC', 'SHIB', 'TRX', 'BCH', 'LINK', 'ATOM', 'UNI', 'ETC', 'FIL',
  'USD', 'USDT', 'USDC', 'SPOT',
])
const US_EXCHANGES = new Set(['NASDAQ', 'NYSE', 'AMEX'])
const MAX_SIGNAL_CHANGE_PCT = Math.max(10, Number(process.env.MAX_SIGNAL_CHANGE_PCT || 300))
const SQUEEZE_WATCHER_MIN = Math.max(1000, Number(process.env.SQUEEZE_WATCHER_MIN || 5000))
const SQUEEZE_SUPPLEMENT_LIMIT = Math.max(1, Math.min(50, Number(process.env.SQUEEZE_SUPPLEMENT_LIMIT || 25)))
const PREDICTION_ALREADY_PRICED_IN_MOVE_PCT = Math.max(5, Number(process.env.PREDICTION_ALREADY_PRICED_IN_MOVE_PCT || 35))
const PREDICTION_EXTENDED_MOVE_REQUIRES_ENTRY_PCT = Math.max(5, Number(process.env.PREDICTION_EXTENDED_MOVE_REQUIRES_ENTRY_PCT || 20))
const PREDICTION_FRESH_CATALYST_MAX_AGE_MINUTES = Math.max(15, Number(process.env.PREDICTION_FRESH_CATALYST_MAX_AGE_MINUTES || 6 * 60))
const PREDICTION_SESSION_CATALYST_MAX_AGE_MINUTES = Math.max(60, Number(process.env.PREDICTION_SESSION_CATALYST_MAX_AGE_MINUTES || 18 * 60))
const PREDICTION_POST_CATALYST_FADE_REJECT_PCT = Math.max(2, Number(process.env.PREDICTION_POST_CATALYST_FADE_REJECT_PCT || 8))
const PREDICTION_POST_CATALYST_MIN_RUNUP_FOR_FADE_PCT = Math.max(2, Number(process.env.PREDICTION_POST_CATALYST_MIN_RUNUP_FOR_FADE_PCT || 10))
const PREDICTION_POST_CATALYST_MAX_RUNUP_WITHOUT_ENTRY_PCT = Math.max(5, Number(process.env.PREDICTION_POST_CATALYST_MAX_RUNUP_WITHOUT_ENTRY_PCT || 25))
const PREDICTION_CATALYST_REACTION_MAX_TICKERS = Math.max(10, Math.min(500, Number(process.env.PREDICTION_CATALYST_REACTION_MAX_TICKERS || 250)))
const PREDICTION_CATALYST_REACTION_MIN_BARS = Math.max(1, Number(process.env.PREDICTION_CATALYST_REACTION_MIN_BARS || 2))
const PREDICTION_CATALYST_REACTION_MIN_MINUTES = Math.max(1, Number(process.env.PREDICTION_CATALYST_REACTION_MIN_MINUTES || 10))
const PREDICTION_MUTED_REACTION_MAX_RUNUP_PCT = Math.max(0.25, Number(process.env.PREDICTION_MUTED_REACTION_MAX_RUNUP_PCT || 1.5))
const PREDICTION_MUTED_REACTION_MAX_ABS_LATEST_PCT = Math.max(0.25, Number(process.env.PREDICTION_MUTED_REACTION_MAX_ABS_LATEST_PCT || 1.25))
const PREDICTION_BUILDING_REACTION_MIN_LATEST_PCT = Math.max(0.25, Number(process.env.PREDICTION_BUILDING_REACTION_MIN_LATEST_PCT || 1))
const PREDICTION_STRONG_REACTION_RUNUP_PCT = Math.max(3, Number(process.env.PREDICTION_STRONG_REACTION_RUNUP_PCT || 8))
const PREDICTION_CATALYST_REACTION_LIVE_OHLC = !['0', 'false', 'no'].includes(String(process.env.PREDICTION_CATALYST_REACTION_LIVE_OHLC || 'true').toLowerCase())
const PREDICTION_CATALYST_REACTION_LIVE_FETCH_MAX = Math.max(0, Math.min(80, Number(process.env.PREDICTION_CATALYST_REACTION_LIVE_FETCH_MAX || 30)))
const PREDICTION_CATALYST_REACTION_FETCH_TIMEOUT_MS = Math.max(1000, Math.min(15000, Number(process.env.PREDICTION_CATALYST_REACTION_FETCH_TIMEOUT_MS || 6000)))
const PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST = ['1', 'true', 'yes'].includes(String(process.env.PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST || 'false').toLowerCase())
const PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE = Math.max(20, Number(process.env.PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE || 45))
const PREDICTION_DEVELOPING_CANDIDATE_MAX_ROWS = Math.max(10, Math.min(250, Number(process.env.PREDICTION_DEVELOPING_CANDIDATE_MAX_ROWS || 100)))
const PREDICTION_DEVELOPING_MIN_CONFIDENCE = Math.max(0.35, Math.min(0.85, Number(process.env.PREDICTION_DEVELOPING_MIN_CONFIDENCE || 0.48)))
const PREDICTION_DEVELOPING_MIN_REL_VOLUME = Math.max(1, Number(process.env.PREDICTION_DEVELOPING_MIN_REL_VOLUME || 1.2))
const PREDICTION_DEVELOPING_MIN_CHANGE_PCT = Math.max(0.1, Number(process.env.PREDICTION_DEVELOPING_MIN_CHANGE_PCT || 0.35))
const PREDICTION_DEVELOPING_OVEREXTENDED_PCT = Math.max(10, Number(process.env.PREDICTION_DEVELOPING_OVEREXTENDED_PCT || 28))
const PREDICTION_DEVELOPING_SEVERE_OVEREXTENDED_PCT = Math.max(PREDICTION_DEVELOPING_OVEREXTENDED_PCT, Number(process.env.PREDICTION_DEVELOPING_SEVERE_OVEREXTENDED_PCT || 55))
const PREDICTION_UNIVERSE_LIMIT = Math.max(3000, Math.min(20000, Number(process.env.PREDICTION_UNIVERSE_LIMIT || process.env.TRADINGVIEW_SCREENER_LIMIT || 10000)))
const WATCHER_SNAPSHOT_MAX_AGE_SECONDS = Math.max(3600, Number(process.env.WATCHER_SNAPSHOT_MAX_AGE_SECONDS || 72 * 60 * 60))
const WATCHER_GROWTH_WINDOW_SECONDS = Math.max(3600, Number(process.env.WATCHER_GROWTH_WINDOW_SECONDS || 24 * 60 * 60))
const PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT = Math.max(0, Number(process.env.PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT || 0.75))
const PREDICTION_UNAFFECTED_MAX_RUNUP_PCT = Math.max(0.5, Number(process.env.PREDICTION_UNAFFECTED_MAX_RUNUP_PCT || 3))
const PREDICTION_PENDING_OPEN_MIN_QUALITY = Math.max(45, Number(process.env.PREDICTION_PENDING_OPEN_MIN_QUALITY || 68))
const PREDICTION_PENDING_OPEN_STRONG_QUALITY = Math.max(PREDICTION_PENDING_OPEN_MIN_QUALITY, Number(process.env.PREDICTION_PENDING_OPEN_STRONG_QUALITY || 78))
const PREDICTION_PENDING_OPEN_PAYOFF_MARGIN = Math.max(0, Number(process.env.PREDICTION_PENDING_OPEN_PAYOFF_MARGIN || 0.02))
const PREDICTION_PENDING_OPEN_MIN_REL_VOLUME = Math.max(1, Number(process.env.PREDICTION_PENDING_OPEN_MIN_REL_VOLUME || 1.5))
const PREDICTION_PENDING_OPEN_MIN_SOCIAL = Math.max(0, Number(process.env.PREDICTION_PENDING_OPEN_MIN_SOCIAL || 8))
const PREDICTION_ACTIONABLE_CATALYST_MAX_AGE_MINUTES = Math.max(30, Number(process.env.PREDICTION_ACTIONABLE_CATALYST_MAX_AGE_MINUTES || 3 * 60))
const PREDICTION_PEOPLE_BACKED_CATALYST_MAX_AGE_MINUTES = Math.max(PREDICTION_ACTIONABLE_CATALYST_MAX_AGE_MINUTES, Number(process.env.PREDICTION_PEOPLE_BACKED_CATALYST_MAX_AGE_MINUTES || 12 * 60))
const PREDICTION_PEOPLE_MIN_MESSAGES = Math.max(1, Number(process.env.PREDICTION_PEOPLE_MIN_MESSAGES || 12))
const PREDICTION_PEOPLE_STRONG_MESSAGES = Math.max(PREDICTION_PEOPLE_MIN_MESSAGES, Number(process.env.PREDICTION_PEOPLE_STRONG_MESSAGES || 25))
const PREDICTION_PEOPLE_MIN_DENSITY_PER_MIN = Math.max(0, Number(process.env.PREDICTION_PEOPLE_MIN_DENSITY_PER_MIN || 0.05))
const PREDICTION_PEOPLE_STRONG_DENSITY_SCORE = Math.max(1, Number(process.env.PREDICTION_PEOPLE_STRONG_DENSITY_SCORE || 22))
const PREDICTION_HIGH_CONVICTION_REQUIRE_POSTMORTEM_GATES = !['0', 'false', 'no'].includes(String(process.env.PREDICTION_HIGH_CONVICTION_REQUIRE_POSTMORTEM_GATES || 'true').toLowerCase())
const PREDICTION_THRESHOLD_POLICY_VERSION = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY_VERSION
const PREDICTION_THRESHOLD_POLICY = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY

const TICKER_LIST_LIMIT = Math.max(1000, Math.min(12000, Number(process.env.SCREENER_TICKER_LIST_LIMIT || 7000)))

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

function normalizeExchange(value) {
  const raw = String(value || '').trim().toUpperCase()
  if (raw === 'NYSEAMERICAN' || raw === 'NYSE AMERICAN') return 'AMEX'
  if (raw === 'NAS') return 'NASDAQ'
  return raw
}

function isCleanListedUsRow(row) {
  if (!row?.ticker || row.ticker.includes('.')) return false
  if (NON_STOCK_TICKERS.has(row.ticker)) return false
  if (row.quote_status && row.quote_status !== 'priced') return false
  if (row.price == null || row.change_pct == null) return false
  if (Number(row.price) <= 0) return false
  if (!Number.isFinite(Number(row.change_pct))) return false
  if (Math.abs(Number(row.change_pct)) > MAX_SIGNAL_CHANGE_PCT) return false
  const exchange = normalizeExchange(row.exchange)
  return US_EXCHANGES.has(exchange)
}

function recentArticleMatch(days = 3) {
  const n = Number(days || 0)
  if (!Number.isFinite(n) || n <= 0) return {}

  const cutoffMs = Date.now() - n * 86_400_000
  const cutoffSec = Math.floor(cutoffMs / 1000)
  const cutoffDate = new Date(cutoffMs)

  return {
    $or: [
      { publish_date: { $gte: cutoffDate } },
      { publish_date: { $gte: cutoffSec } },
      { fetched_date: { $gte: cutoffDate } },
      { fetched_date: { $gte: cutoffSec } },
      { detected_at: { $gte: cutoffDate } },
      { detected_at: { $gte: cutoffSec } },
      { createdAt: { $gte: cutoffDate } },
    ],
  }
}

function sentimentScore(row) {
  const total = Math.max(1, Number(row.count || 0))
  if (row.weighted_score_sum != null) {
    const denominator = Number(row.weight_sum || total)
    return denominator ? Number((Number(row.weighted_score_sum || 0) / (denominator + 1.5)).toFixed(3)) : 0
  }
  if (row.score_sum != null) return Number((Number(row.score_sum || 0) / (total + 2)).toFixed(3))
  const priorNeutralWeight = 2
  return Number((((row.bullish || 0) - (row.bearish || 0)) / (total + priorNeutralWeight)).toFixed(3))
}

function stableHash(value) {
  let hash = 0
  const text = String(value || '')
  for (let i = 0; i < text.length; i += 1) hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0
  return Math.abs(hash)
}

function derivedNumber(ticker, min, max, decimals = 2, salt = '') {
  const pct = (stableHash(`${ticker}:${salt}`) % 10000) / 10000
  return Number((min + (max - min) * pct).toFixed(decimals))
}

function nullableNumber(value) {
  if (value == null || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function pctReturn(from, to) {
  const a = Number(from)
  const b = Number(to)
  return Number.isFinite(a) && Number.isFinite(b) && a > 0 ? ((b - a) / a) * 100 : null
}

function firstPresent(...values) {
  return values.find(value => value != null && value !== '')
}

const PREDICTION_EVIDENCE_FIELDS = [
  'evidence_score',
  'evidenceScore',
  'social_score',
  'socialScore',
  'newsScore',
  'news_score',
  'catalystScore',
  'catalyst_score',
  'catalyst_power_score',
  'momentum_score',
  'momentumScore',
  'technicalScore',
  'ai_score',
  'correlation_score',
  'correlation_context',
  'sec_filing_contributed',
  'filing_used_count',
  'filing_sentiment',
  'social_message_sentiment',
  'social_message_density',
  'stocktwits_message_sentiment',
  'stocktwits_message_density',
  'stocktwits_message_count',
  'message_count',
  'message_density_now',
  'message_density_5m',
  'message_density_15m',
  'message_density_30m',
  'message_density_60m',
  'message_density_prev_window',
  'message_density_change',
  'message_density_change_pct',
  'message_density_trend',
  'message_density_rising',
  'message_density_supported',
  'message_density_score',
  'message_density_live_score',
  'message_density_carry_score',
  'message_density_session_count',
  'message_density_session_minutes',
  'message_density_session_density',
  'message_density_last_event_age_minutes',
  'message_density_active_15m',
  'message_density_active_60m',
  'news_article_count',
  'catalyst_window_article_count',
  'structured_sentiment',
  'social_sentiment',
  'avg_sentiment',
  'short_squeeze_score',
  'short_squeeze_available',
  'short_squeeze_reason',
  'squeeze_signal',
  'squeeze_proxy_used',
  'short_interest_pct',
  'short_interest_pct_float',
  'short_interest_pct_shares_out',
  'float_short',
  'stocktwits_watcher_count',
  'correlation_available',
  'raw_correlation_score',
  'correlation_source',
  'threshold_feature_updated_at',
  'price_density_correlation',
  'previous_price_density_correlation',
  'threshold_pre_return_60m_pct',
  'threshold_trailing_60m_messages',
  'threshold_feature_window_minutes',
  'threshold_feature_min_observations',
  'threshold_feature_status',
  'threshold_setup_status',
  'threshold_setup_score',
  'threshold_setup_distance_to_entry',
  'threshold_feature_policy_version',
  'threshold_feature_source',
  'threshold_feature_snapshot_sec',
]

const PREDICTION_NUMERIC_MAX_FIELDS = new Set([
  'evidence_score',
  'evidenceScore',
  'social_score',
  'socialScore',
  'newsScore',
  'catalystScore',
  'catalyst_power_score',
  'momentum_score',
  'momentumScore',
  'technicalScore',
  'ai_score',
  'filing_used_count',
  'social_message_density',
  'stocktwits_message_density',
  'stocktwits_message_count',
  'message_count',
  'message_density_now',
  'message_density_5m',
  'message_density_15m',
  'message_density_30m',
  'message_density_60m',
  'message_density_change',
  'message_density_change_pct',
  'message_density_score',
  'message_density_live_score',
  'message_density_carry_score',
  'message_density_session_count',
  'message_density_session_minutes',
  'message_density_session_density',
  'message_density_active_15m',
  'message_density_active_60m',
  'news_article_count',
  'catalyst_window_article_count',
  'short_squeeze_score',
  'short_interest_pct',
  'short_interest_pct_float',
  'short_interest_pct_shares_out',
  'float_short',
  'stocktwits_watcher_count',
  'threshold_trailing_60m_messages',
  'threshold_feature_window_minutes',
  'threshold_feature_min_observations',
  'threshold_setup_score',
  'threshold_setup_distance_to_entry',
  'threshold_feature_snapshot_sec',
])

const PREDICTION_BOOLEAN_OR_FIELDS = new Set([
  'sec_filing_contributed',
  'message_density_rising',
  'message_density_supported',
  'short_squeeze_available',
  'squeeze_proxy_used',
  'correlation_available',
])

function mergePredictionEvidenceField(field, base = {}, supplement = {}) {
  if (PREDICTION_BOOLEAN_OR_FIELDS.has(field)) {
    return Boolean(base[field] || supplement[field])
  }
  if (PREDICTION_NUMERIC_MAX_FIELDS.has(field)) {
    const baseNumber = nullableNumber(base[field])
    const supplementNumber = nullableNumber(supplement[field])
    if (baseNumber == null) return supplementNumber
    if (supplementNumber == null) return baseNumber
    return Math.max(baseNumber, supplementNumber)
  }
  return firstPresent(base[field], supplement[field])
}

function mergePredictionEvidenceFields(base = {}, supplement = {}) {
  const merged = {}
  for (const field of PREDICTION_EVIDENCE_FIELDS) {
    const value = mergePredictionEvidenceField(field, base, supplement)
    if (value != null && value !== '') merged[field] = value
  }
  const baseDebug = base.prediction_debug && typeof base.prediction_debug === 'object' ? base.prediction_debug : {}
  const supplementDebug = supplement.prediction_debug && typeof supplement.prediction_debug === 'object' ? supplement.prediction_debug : {}
  if (Object.keys(baseDebug).length || Object.keys(supplementDebug).length) {
    merged.prediction_debug = { ...supplementDebug, ...baseDebug }
  }
  return merged
}

function clampCorrelation(value) {
  const n = Number(value)
  if (!Number.isFinite(n)) return NaN
  return Math.max(-1, Math.min(1, n))
}

function sigmoid(value) {
  const n = Math.max(-20, Math.min(20, Number(value) || 0))
  return 1 / (1 + Math.exp(-n))
}

function predictionModelFeature(row = {}, key = '') {
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const catalystCount = nullableNumber(row.catalyst_window_article_count ?? row.news_article_count) || 0
  const session = String(row.prediction_session || row.prediction?.predictionSession || row.catalyst_session_context?.session || '').toLowerCase()
  switch (key) {
    case 'final_prediction_score': return nullableNumber(row.final_prediction_score ?? row.convictionScore) || 0
    case 'prediction_confidence': return nullableNumber(row.prediction_confidence ?? row.confidence ?? row.prediction?.confidence) || 0
    case 'probability_up': return nullableNumber(row.probability_up ?? row.prediction?.probabilityUp) ?? 0.5
    case 'predicted_return': return nullableNumber(row.predicted_return ?? row.predictedReturnPct ?? row.prediction?.predictedReturn) || 0
    case 'change_pct': return nullableNumber(row.change_pct) || 0
    case 'abs_change_pct': return Math.abs(nullableNumber(row.change_pct) || 0)
    case 'rel_volume': return nullableNumber(row.rel_volume) || 0
    case 'risk_count': return riskFlags.length
    case 'has_no_risk_flags': return riskFlags.length ? 0 : 1
    case 'has_large_move_flag': return riskFlags.some(flag => ['RECENT_LARGE_MOVE', 'RECENT_EXTREME_MOVE_ALREADY_OCCURRED', 'EXTENDED_MOVE_REQUIRES_FRESH_VALIDATION'].includes(flag)) ? 1 : 0
    case 'has_extreme_volatility_flag': return riskFlags.includes('EXTREME_VOLATILITY') ? 1 : 0
    case 'has_catalyst_mismatch_flag': return riskFlags.includes('CATALYST_TICKER_MISMATCH') ? 1 : 0
    case 'has_private_exposure_flag': return riskFlags.includes('NON_ACTIONABLE_PRIVATE_EXPOSURE') ? 1 : 0
    case 'ai_score': return nullableNumber(row.ai_score) ?? 50
    case 'ai_confidence': return nullableNumber(row.ai_context?.ai_confidence ?? row.prediction_debug?.ai_confidence) || 0
    case 'ai_article_count': return nullableNumber(row.ai_context?.ai_article_count ?? row.prediction_debug?.ai_article_count) || 0
    case 'momentum_score': return nullableNumber(row.momentum_score) ?? 50
    case 'correlation_score': return nullableNumber(row.correlation_score ?? row.price_density_correlation) || 0
    case 'news_sentiment': return nullableNumber(row.sentiment_breakdown?.newsSentiment ?? row.structured_sentiment ?? row.avg_sentiment) || 0
    case 'social_sentiment': return nullableNumber(row.sentiment_breakdown?.socialSentiment ?? row.social_sentiment) || 0
    case 'filing_sentiment': return nullableNumber(row.sentiment_breakdown?.filingSentiment ?? row.filing_sentiment) || 0
    case 'catalyst_alignment': return nullableNumber(row.prediction_validation?.catalystAlignment ?? row.dashboard_assessment?.components?.catalystAlignment) || 0
    case 'catalyst_power_score': return nullableNumber(row.catalyst_power_score ?? row.catalystScore ?? row.catalyst_score) || 0
    case 'catalyst_window_article_count': return catalystCount
    case 'top_catalyst_power': return Array.isArray(row.catalysts) && row.catalysts.length ? (nullableNumber(row.catalysts[0]?.catalyst_power) || 0) : 0
    case 'has_session_window_catalyst': return catalystCount > 0 ? 1 : 0
    case 'positive_catalyst_gate': return row.prediction_validation?.recognizedNewsCatalyst || row.dashboard_assessment?.components?.positiveCatalystGate ? 1 : 0
    case 'evidence_items': return nullableNumber(row.dashboard_assessment?.evidenceItems) || (Array.isArray(row.reasons) ? row.reasons.length : 0)
    case 'reliability': return nullableNumber(row.dashboard_assessment?.reliability) || ((nullableNumber(row.final_prediction_score ?? row.convictionScore) || 0) / 100)
    case 'is_premarket_signal': return session === 'premarket' || session === 'overnight' ? 1 : 0
    case 'is_regular_signal': return session === 'regular' ? 1 : 0
    case 'is_afterhours_signal': return session === 'afterhours' || session === 'closed_post_afterhours' ? 1 : 0
    case 'is_weekend_carry_signal': return session === 'weekend' ? 1 : 0
    default: return 0
  }
}

function predictionModelProbability(row = {}, modelDoc = null) {
  if (!modelDoc?.live_enabled || !Array.isArray(modelDoc.feature_keys) || !Array.isArray(modelDoc.weights)) return null
  const stats = modelDoc.feature_stats || {}
  let z = Number(modelDoc.intercept || 0)
  modelDoc.feature_keys.forEach((key, index) => {
    const stat = stats[key] || { mean: 0, std: 1 }
    const value = predictionModelFeature(row, key)
    z += Number(modelDoc.weights[index] || 0) * ((Number(value || 0) - Number(stat.mean || 0)) / (Number(stat.std || 1) || 1))
  })
  return Number(sigmoid(z).toFixed(3))
}

function predictionFreshTriggerState(row = {}, validation = {}) {
  const setupStatus = String(row.entry_signal?.setup_status || row.threshold_setup_status || row.threshold_policy?.setupStatus || '')
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const hardBlockers = riskFlags.some(flag => [
    'NO_CATALYST_PRICE_REACTION_OHLC',
    'POST_CATALYST_REACTION_UNVERIFIED',
    'PENDING_OPEN_UNRECOGNIZED_SOURCE',
    'PENDING_OPEN_WEAK_CATALYST_QUALITY',
    'PENDING_OPEN_NEEDS_SECOND_CONFIRMATION',
    'REJECTED_CATALYST_QUALITY',
  ].includes(flag))
  const freshDensityCross = Boolean(row.entry_signal?.status === 'entry_passed' || row.threshold_policy?.passed || row.prediction_threshold_policy?.passed || setupStatus === 'entry_passed')
  const peopleAttention = validation.peopleAttention || predictionPeopleAttention(row)
  const catalystAge = nullableNumber(validation.catalystAgeMinutes)
  const veryFreshNewsCatalyst = Boolean(
    validation.recognizedNewsCatalyst &&
    catalystAge != null &&
    catalystAge <= PREDICTION_ACTIONABLE_CATALYST_MAX_AGE_MINUTES
  )
  const peopleBackedNewsCatalyst = Boolean(
    validation.recognizedNewsCatalyst &&
    peopleAttention.active &&
    catalystAge != null &&
    catalystAge <= PREDICTION_PEOPLE_BACKED_CATALYST_MAX_AGE_MINUTES
  )
  const freshNewsCatalyst = Boolean(veryFreshNewsCatalyst || peopleBackedNewsCatalyst)
  const verifiedSqueeze = Boolean(validation.recognizedSqueezeCatalyst && validation.verifiedShortInterest)
  const peopleMomentum = Boolean(validation.recognizedPeopleAttention && peopleAttention.strong)
  const strongSocialWithNews = Boolean(validation.recognizedSocialCatalyst && (freshNewsCatalyst || peopleMomentum) && peopleAttention.strong)
  const payoffPasses = row.payoff_model_passes == null ? null : Boolean(row.payoff_model_passes)
  const peopleConfirmedTrigger = Boolean(peopleAttention.active && (freshDensityCross || peopleBackedNewsCatalyst || peopleMomentum || verifiedSqueeze))
  const blockedReasons = [
    !freshDensityCross && !freshNewsCatalyst && !verifiedSqueeze && !strongSocialWithNews && !peopleMomentum ? 'NO_FRESH_CONFIRMED_TRIGGER' : '',
    validation.recognizedNewsCatalyst && !freshNewsCatalyst ? 'STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION' : '',
    !peopleAttention.active && !verifiedSqueeze ? 'NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION' : '',
    payoffPasses === false ? 'BELOW_PAYOFF_MODEL_THRESHOLD' : '',
    riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') ? 'LOW_OR_MISSING_SOCIAL_CONFIRMATION' : '',
    riskFlags.includes('STALE_OR_OUT_OF_WINDOW_CATALYST') ? 'STALE_OR_OUT_OF_WINDOW_CATALYST' : '',
    riskFlags.includes('NO_CATALYST_PRICE_REACTION_OHLC') ? 'NO_CATALYST_PRICE_REACTION_OHLC' : '',
    riskFlags.includes('POST_CATALYST_REACTION_UNVERIFIED') ? 'POST_CATALYST_REACTION_UNVERIFIED' : '',
    riskFlags.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE') ? 'PENDING_OPEN_UNRECOGNIZED_SOURCE' : '',
    riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') ? 'PENDING_OPEN_WEAK_CATALYST_QUALITY' : '',
    riskFlags.includes('PENDING_OPEN_NEEDS_SECOND_CONFIRMATION') ? 'PENDING_OPEN_NEEDS_SECOND_CONFIRMATION' : '',
    riskFlags.includes('REJECTED_CATALYST_QUALITY') ? 'REJECTED_CATALYST_QUALITY' : '',
    riskFlags.includes('CATALYST_ALREADY_PRICED_IN') ? 'CATALYST_ALREADY_PRICED_IN' : '',
    riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') && !freshNewsCatalyst && !verifiedSqueeze ? 'NO_FRESH_DENSITY_ENTRY_CROSS' : '',
  ].filter(Boolean)
  return {
    passesRawPrediction: !hardBlockers && (payoffPasses !== false) && (freshDensityCross || freshNewsCatalyst || verifiedSqueeze || strongSocialWithNews || peopleMomentum),
    passesHighConviction: !hardBlockers && payoffPasses === true && peopleConfirmedTrigger && !riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION'),
    freshDensityCross,
    freshNewsCatalyst,
    veryFreshNewsCatalyst,
    peopleBackedNewsCatalyst,
    verifiedSqueeze,
    peopleMomentum,
    strongSocialWithNews,
    peopleAttention,
    payoffPasses,
    blockedReasons: Array.from(new Set(blockedReasons)),
  }
}

function catalystReactionPendingMarketOpen(catalystReaction = {}) {
  const available = Boolean(catalystReaction?.priceReactionAvailable || catalystReaction?.available)
  if (available) return false
  const marketSession = String(catalystReaction?.catalystMarketSession || catalystReaction?.catalyst_market_session || '').toLowerCase()
  const outsideRegular = Boolean(catalystReaction?.catalystOutsideRegularHours || ['postmarket', 'overnight', 'premarket', 'weekend'].includes(marketSession))
  const inWindow = Boolean(catalystReaction?.eventInSessionWindow || catalystReaction?.event_in_session_window)
  return outsideRegular && inWindow
}

function catalystReactionNeedsOhlcBlock(catalystReaction = {}) {
  const available = Boolean(catalystReaction?.priceReactionAvailable || catalystReaction?.available)
  if (available) return false
  return !catalystReactionPendingMarketOpen(catalystReaction)
}

function classifyCatalystReaction(catalystReaction = {}) {
  const available = Boolean(catalystReaction?.priceReactionAvailable || catalystReaction?.available)
  const runup = nullableNumber(catalystReaction?.postCatalystRunupPct ?? catalystReaction?.post_catalyst_runup_pct)
  const latest = nullableNumber(catalystReaction?.postCatalystLatestReturnPct ?? catalystReaction?.post_catalyst_latest_return_pct)
  const giveback = nullableNumber(catalystReaction?.postCatalystGivebackPct ?? catalystReaction?.post_catalyst_giveback_from_high_pct)
  const minutes = nullableNumber(catalystReaction?.post_catalyst_minutes ?? catalystReaction?.postCatalystMinutes)
  const bars = nullableNumber(catalystReaction?.post_catalyst_bar_count ?? catalystReaction?.postCatalystBarCount)
  const rejection = catalystReaction?.rejection || null
  const pendingMarketReaction = catalystReactionPendingMarketOpen(catalystReaction)
  const marketHadChanceToReact = Boolean(
    available &&
    ((bars != null && bars >= PREDICTION_CATALYST_REACTION_MIN_BARS) ||
      (minutes != null && minutes >= PREDICTION_CATALYST_REACTION_MIN_MINUTES))
  )

  if (rejection) {
    return {
      state: 'rejected_already_priced_or_faded',
      label: 'priced/faded',
      tone: 'danger',
      market_had_chance_to_react: marketHadChanceToReact,
      actionable_spillover: false,
      exhaustion_risk: true,
      reason: rejection,
    }
  }
  if (pendingMarketReaction) {
    return {
      state: 'pending_market_open',
      label: 'pending market reaction',
      tone: 'info',
      market_had_chance_to_react: false,
      actionable_spillover: true,
      exhaustion_risk: false,
      reason: 'catalyst arrived outside regular liquidity and no real market reaction bars are available yet',
    }
  }
  if (!available) {
    return {
      state: 'reaction_unavailable',
      label: 'no ohlc reaction',
      tone: 'neutral',
      market_had_chance_to_react: false,
      actionable_spillover: false,
      exhaustion_risk: false,
      reason: catalystReaction?.reason || 'no usable OHLC bars found after catalyst',
    }
  }
  if (!marketHadChanceToReact) {
    return {
      state: 'pending_first_bars',
      label: 'pending first bars',
      tone: 'info',
      market_had_chance_to_react: false,
      actionable_spillover: true,
      exhaustion_risk: false,
      reason: `only ${bars ?? 0} post-catalyst bars / ${minutes ?? 0} minutes are available`,
    }
  }
  if (latest != null && latest <= -3) {
    return {
      state: 'negative_first_reaction',
      label: 'negative reaction',
      tone: 'danger',
      market_had_chance_to_react: true,
      actionable_spillover: false,
      exhaustion_risk: true,
      reason: `latest post-catalyst return is ${latest.toFixed(2)}%`,
    }
  }
  if (runup != null && runup >= PREDICTION_STRONG_REACTION_RUNUP_PCT && giveback != null && giveback >= PREDICTION_POST_CATALYST_FADE_REJECT_PCT) {
    return {
      state: 'spike_then_fade',
      label: 'spike/fade',
      tone: 'danger',
      market_had_chance_to_react: true,
      actionable_spillover: false,
      exhaustion_risk: true,
      reason: `ran ${runup.toFixed(2)}% then gave back ${giveback.toFixed(2)}%`,
    }
  }
  if (runup != null && runup >= PREDICTION_STRONG_REACTION_RUNUP_PCT && latest != null && latest >= PREDICTION_BUILDING_REACTION_MIN_LATEST_PCT) {
    return {
      state: 'already_priced_in_strong_reaction',
      label: 'already priced',
      tone: 'warning',
      market_had_chance_to_react: true,
      actionable_spillover: false,
      exhaustion_risk: true,
      reason: `post-catalyst high already reached ${runup.toFixed(2)}%`,
    }
  }
  if (runup != null && runup <= PREDICTION_MUTED_REACTION_MAX_RUNUP_PCT && latest != null && Math.abs(latest) <= PREDICTION_MUTED_REACTION_MAX_ABS_LATEST_PCT) {
    return {
      state: 'muted_unpriced_reaction',
      label: 'muted/unpriced',
      tone: 'success',
      market_had_chance_to_react: true,
      actionable_spillover: true,
      exhaustion_risk: false,
      reason: `market had bars but only moved ${latest.toFixed(2)}% from catalyst anchor`,
    }
  }
  if (latest != null && latest >= PREDICTION_BUILDING_REACTION_MIN_LATEST_PCT && (giveback == null || giveback <= PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT)) {
    return {
      state: 'building_positive_reaction',
      label: 'building',
      tone: 'success',
      market_had_chance_to_react: true,
      actionable_spillover: true,
      exhaustion_risk: false,
      reason: `post-catalyst return is ${latest.toFixed(2)}% with limited giveback`,
    }
  }
  if (giveback != null && giveback > PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT) {
    return {
      state: 'gave_back_from_high',
      label: 'gave back',
      tone: 'warning',
      market_had_chance_to_react: true,
      actionable_spillover: false,
      exhaustion_risk: true,
      reason: `gave back ${giveback.toFixed(2)}% from post-catalyst high`,
    }
  }
  return {
    state: 'unaffected_or_mixed_reaction',
    label: 'mixed/unaffected',
    tone: 'neutral',
    market_had_chance_to_react: true,
    actionable_spillover: true,
    exhaustion_risk: false,
    reason: 'post-catalyst reaction is present but not decisive',
  }
}

function catalystReactionSummary(catalystReaction = {}) {
  const available = Boolean(catalystReaction?.priceReactionAvailable || catalystReaction?.available)
  const runup = nullableNumber(catalystReaction?.postCatalystRunupPct ?? catalystReaction?.post_catalyst_runup_pct)
  const latest = nullableNumber(catalystReaction?.postCatalystLatestReturnPct ?? catalystReaction?.post_catalyst_latest_return_pct)
  const giveback = nullableNumber(catalystReaction?.postCatalystGivebackPct ?? catalystReaction?.post_catalyst_giveback_from_high_pct)
  const minutes = nullableNumber(catalystReaction?.post_catalyst_minutes ?? catalystReaction?.postCatalystMinutes)
  const eventSec = nullableNumber(catalystReaction?.eventSec ?? catalystReaction?.event_sec)
  const rejection = catalystReaction?.rejection || null
  const state = catalystReaction?.reactionState || catalystReaction?.reaction_state || catalystReaction?.state || (available ? 'reaction_available' : 'reaction_unavailable')
  const classification = classifyCatalystReaction(catalystReaction)
  return {
    available,
    state,
    label: classification.label,
    tone: classification.tone,
    rejection,
    pending_market_reaction: classification.state === 'pending_market_open',
    first_reaction_state: classification.state,
    market_had_chance_to_react: classification.market_had_chance_to_react,
    actionable_spillover: classification.actionable_spillover,
    exhaustion_risk: classification.exhaustion_risk,
    reaction_reason: classification.reason,
    event_sec: eventSec,
    market_session: catalystReaction?.catalystMarketSession || catalystReaction?.catalyst_market_session || null,
    event_in_session_window: Boolean(catalystReaction?.eventInSessionWindow || catalystReaction?.event_in_session_window),
    minutes_since_catalyst: minutes,
    post_catalyst_bar_count: nullableNumber(catalystReaction?.post_catalyst_bar_count),
    post_catalyst_volume: nullableNumber(catalystReaction?.post_catalyst_volume),
    post_catalyst_dollar_volume: nullableNumber(catalystReaction?.post_catalyst_dollar_volume),
    runup_pct: runup,
    latest_return_pct: latest,
    giveback_from_high_pct: giveback,
    anchor_price: nullableNumber(catalystReaction?.anchor_price),
    latest_close: nullableNumber(catalystReaction?.latest_close),
    high_after_catalyst: nullableNumber(catalystReaction?.post_catalyst_high),
    source: catalystReaction?.source || null,
    thresholds: {
      max_unaffected_runup_pct: PREDICTION_UNAFFECTED_MAX_RUNUP_PCT,
      max_unaffected_giveback_pct: PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT,
    },
  }
}

function predictionReadinessState(row = {}, validation = {}, freshTriggerState = {}, catalystReaction = {}) {
  const reaction = catalystReactionSummary(catalystReaction)
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const blockedReasons = Array.from(new Set([
    ...(Array.isArray(freshTriggerState.blockedReasons) ? freshTriggerState.blockedReasons : []),
    ...(reaction.rejection ? [reaction.rejection] : []),
  ].filter(Boolean)))
  const waitingFor = [
    !freshTriggerState.freshDensityCross ? 'fresh_density_cross' : '',
    !validation.recognizedNewsCatalyst && !validation.recognizedSqueezeCatalyst ? 'validated_primary_catalyst' : '',
    freshTriggerState.payoffPasses === false ? 'payoff_model_pass' : '',
    riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') ? 'social_confirmation' : '',
  ].filter(Boolean)

  let level = 'watch_candidate'
  let label = 'Watch'
  let tone = 'neutral'
  if (reaction.rejection) {
    level = 'already_priced_or_faded'
    label = 'Priced/Faded'
    tone = 'danger'
  } else if (riskFlags.includes('REJECTED_CATALYST_QUALITY')) {
    level = 'rejected_catalyst_quality'
    label = 'Bad Catalyst'
    tone = 'danger'
  } else if (riskFlags.includes('NO_CATALYST_PRICE_REACTION_OHLC') || riskFlags.includes('POST_CATALYST_REACTION_UNVERIFIED')) {
    level = 'reaction_unverified'
    label = 'Need OHLC'
    tone = 'warning'
  } else if (riskFlags.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE') || riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') || riskFlags.includes('PENDING_OPEN_NEEDS_SECOND_CONFIRMATION')) {
    level = 'pending_open_needs_confirmation'
    label = 'Needs Confirm'
    tone = 'warning'
  } else if (reaction.pending_market_reaction && freshTriggerState.freshNewsCatalyst) {
    level = 'fresh_catalyst_pending_open'
    label = 'Fresh Pending Open'
    tone = 'info'
  } else if (freshTriggerState.passesHighConviction) {
    level = 'high_conviction_prediction'
    label = 'High Conviction'
    tone = 'success'
  } else if (freshTriggerState.passesRawPrediction && freshTriggerState.freshDensityCross) {
    level = 'trade_ready_prediction'
    label = 'Trade Ready'
    tone = 'success'
  } else if (freshTriggerState.passesRawPrediction && freshTriggerState.freshNewsCatalyst) {
    level = 'fresh_catalyst_candidate'
    label = 'Fresh Catalyst'
    tone = 'info'
  } else if ((validation.recognizedNewsCatalyst || validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest) && !freshTriggerState.freshDensityCross) {
    level = 'waiting_for_density_cross'
    label = 'Wait Density'
    tone = 'warning'
  } else if (freshTriggerState.payoffPasses === false) {
    level = 'blocked_by_payoff_model'
    label = 'Payoff Block'
    tone = 'danger'
  }

  return {
    level,
    label,
    tone,
    trade_ready: level === 'trade_ready_prediction' || level === 'high_conviction_prediction',
    high_conviction_ready: level === 'high_conviction_prediction',
    waiting_for: Array.from(new Set(waitingFor)),
    blocked_reasons: blockedReasons,
    reaction,
  }
}

function predictionDisplayPriority(row = {}) {
  const level = String(row.prediction_readiness_level || row.prediction_readiness?.level || '').toLowerCase()
  const readinessPriority = level === 'high_conviction_prediction'
    ? 7
    : level === 'trade_ready_prediction'
      ? 6
      : level === 'fresh_catalyst_pending_open'
        ? 5
        : level === 'developing_catalyst_momentum'
          ? 4.5
          : level === 'fresh_catalyst_candidate'
          ? 4
          : level === 'waiting_for_density_cross'
            ? 3
            : level === 'developing_market_momentum'
              ? 2.5
              : level === 'pending_open_needs_confirmation'
              ? 2
              : level === 'reaction_unverified'
                ? 1
                : 0
  const payoff = nullableNumber(row.payoff_model_probability) || 0
  const quality = nullableNumber(row.catalyst_quality_score ?? row.catalyst_quality?.score) || 0
  const score = nullableNumber(row.final_prediction_score ?? row.convictionScore ?? row.watchScore) || 0
  return readinessPriority * 1_000_000 + payoff * 100_000 + quality * 500 + score
}

async function loadPredictionPostmortemReport(db) {
  if (!db) return null
  return db.collection('prediction_postmortem_reports').findOne({
    _id: 'latest_prediction_postmortem',
  }, {
    projection: {
      _id: 0,
      generated_at: 1,
      recommendations: 1,
      summary: 1,
      note: 1,
    },
  }).catch(() => null)
}

function nullableFixed(value, decimals = 2) {
  const n = nullableNumber(value)
  return n == null ? null : Number(n.toFixed(decimals))
}

function timestampSeconds(value) {
  if (!value) return null
  if (value instanceof Date) return Math.floor(value.getTime() / 1000)
  const n = Number(value)
  if (Number.isFinite(n) && n > 0) return n > 1_000_000_000_000 ? Math.floor(n / 1000) : Math.floor(n)
  const ms = Date.parse(String(value))
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : null
}

function easternParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date)
  return Object.fromEntries(parts.filter(part => part.type !== 'literal').map(part => [part.type, Number(part.value)]))
}

function etDateKey(date = new Date()) {
  const p = easternParts(date)
  return `${String(p.year).padStart(4, '0')}-${String(p.month).padStart(2, '0')}-${String(p.day).padStart(2, '0')}`
}

function dateKeyAddDays(dateKey, days = 0) {
  const [year, month, day] = String(dateKey || etDateKey()).split('-').map(Number)
  return new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0)).toISOString().slice(0, 10)
}

function dayOfWeekDateKey(dateKey) {
  const [year, month, day] = String(dateKey || etDateKey()).split('-').map(Number)
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0)).getUTCDay()
}

function previousTradingDateKey(dateKey) {
  let days = -1
  for (let guard = 0; guard < 10; guard += 1) {
    const candidate = dateKeyAddDays(dateKey, days)
    const dow = dayOfWeekDateKey(candidate)
    if (dow !== 0 && dow !== 6) return candidate
    days -= 1
  }
  return dateKeyAddDays(dateKey, -1)
}

function utcDateFromEt(dateKey, hour = 0, minute = 0, second = 0) {
  const [year, month, day] = String(dateKey).split('-').map(Number)
  let guess = new Date(Date.UTC(year, month - 1, day, hour, minute, second))
  for (let i = 0; i < 4; i += 1) {
    const p = easternParts(guess)
    const desired = Date.UTC(year, month - 1, day, hour, minute, second)
    const actual = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second || 0)
    const delta = desired - actual
    if (!delta) break
    guess = new Date(guess.getTime() + delta)
  }
  return guess
}

function marketSessionContext(now = new Date()) {
  const p = easternParts(now)
  const dateKey = etDateKey(now)
  const dow = dayOfWeekDateKey(dateKey)
  const minutes = p.hour * 60 + p.minute
  const isWeekend = dow === 0 || dow === 6
  let session = 'overnight'
  if (isWeekend) session = 'weekend'
  else if (minutes >= 4 * 60 && minutes < 9 * 60 + 30) session = 'premarket'
  else if (minutes >= 9 * 60 + 30 && minutes < 16 * 60) session = 'regular'
  else if (minutes >= 16 * 60 && minutes < 20 * 60) session = 'postmarket'

  const windowStartDateKey = (session === 'weekend' || session === 'premarket' || session === 'overnight')
    ? previousTradingDateKey(dateKey)
    : dateKey
  const windowStartHour = (session === 'regular') ? 4 : 16
  const catalystWindowStart = utcDateFromEt(windowStartDateKey, windowStartHour, 0, 0)
  const catalystWindowEnd = now
  const nextSessionDate = session === 'premarket' || session === 'regular' ? dateKey : nextTradingDateIso(dateKey)

  return {
    timezone: 'America/New_York',
    session,
    market_phase: session === 'regular' ? 'during_market' : session === 'premarket' ? 'pre_market' : session === 'postmarket' ? 'post_market' : session,
    date_key: dateKey,
    next_session_date: nextSessionDate,
    catalyst_window_start: catalystWindowStart,
    catalyst_window_end: catalystWindowEnd,
    catalyst_window_start_sec: Math.floor(catalystWindowStart.getTime() / 1000),
    catalyst_window_end_sec: Math.floor(catalystWindowEnd.getTime() / 1000),
    catalyst_window_policy: session === 'weekend' || (dow === 1 && session === 'premarket')
      ? 'friday_postmarket_weekend_monday_premarket'
      : session === 'premarket'
        ? 'prior_postmarket_to_current_premarket'
        : session === 'regular'
          ? 'current_premarket_to_now'
          : 'postmarket_to_next_session',
  }
}

function marketSessionForSec(sec) {
  const n = nullableNumber(sec)
  if (!n) return 'missing'
  const p = easternParts(new Date(n * 1000))
  const dateKey = `${String(p.year).padStart(4, '0')}-${String(p.month).padStart(2, '0')}-${String(p.day).padStart(2, '0')}`
  const dow = dayOfWeekDateKey(dateKey)
  if (dow === 0 || dow === 6) return 'weekend'
  const minutes = p.hour * 60 + p.minute
  if (minutes >= 4 * 60 && minutes < 9 * 60 + 30) return 'premarket'
  if (minutes >= 9 * 60 + 30 && minutes < 16 * 60) return 'regular'
  if (minutes >= 16 * 60 && minutes < 20 * 60) return 'postmarket'
  return 'overnight'
}

function quoteFreshness(value) {
  const sec = timestampSeconds(value)
  if (!sec) return { quote_age_seconds: null, quote_freshness: 'missing' }
  const age = Math.max(0, Math.floor(Date.now() / 1000) - sec)
  return {
    quote_age_seconds: age,
    quote_freshness: age <= 45 * 60 ? 'fresh' : age <= 4 * 3600 ? 'stale' : 'very_stale',
  }
}

function isBroadRoundupCatalyst(item = {}) {
  if (!item) return false
  const tickerCount = Number(item.ticker_count || item.tickerCount || 0)
  const title = String(item.title || item.headline || item.summary || '').toLowerCase()
  const explicitRoundup = Boolean(item.roundup_article || item.is_roundup_article)
  const titleLooksBroad = /stocks moving|here are \d+ stocks|premarket movers|market movers|moving premarket|why .*shares are trading higher by over/.test(title)
  return (explicitRoundup || titleLooksBroad) && tickerCount >= 4
}

function bestTickerSpecificCatalyst(row = {}) {
  const candidates = [
    row.main_catalyst,
    ...(Array.isArray(row.catalysts) ? row.catalysts : []),
  ].filter(Boolean)
  return candidates.find(item => !isBroadRoundupCatalyst(item)) || null
}

function suppressBroadRoundupMainCatalyst(row = {}) {
  if (!row || !isBroadRoundupCatalyst(row.main_catalyst)) return row
  const replacement = bestTickerSpecificCatalyst({ ...row, main_catalyst: null })
  return {
    ...row,
    main_catalyst: replacement,
    broad_roundup_catalyst_suppressed: true,
  }
}

function predictionCatalystReactionState(row = {}, threshold = {}, validation = {}, context = {}) {
  const change = nullableNumber(context.change ?? row.change_pct)
  const nowSec = Math.floor(Date.now() / 1000)
  const setupStatus = String(context.setupStatus || threshold.setupStatus || row.threshold_setup_status || '')
  const freshEntryCross = Boolean(threshold.passed || setupStatus === 'entry_passed')
  const mainCatalyst = row.main_catalyst || {}
  const eventSec = nullableNumber(mainCatalyst.event_sec ?? row.latest_publish_sec)
  const ageMinutes = nullableNumber(mainCatalyst.age_minutes) ?? (
    eventSec != null ? Number(Math.max(0, (nowSec - eventSec) / 60).toFixed(1)) : null
  )
  const sessionContext = row.catalyst_session_context || context.sessionContext || {}
  const sessionStart = nullableNumber(sessionContext.catalyst_window_start_sec)
  const sessionEnd = nullableNumber(sessionContext.catalyst_window_end_sec)
  const catalystMarketSession = marketSessionForSec(eventSec)
  const catalystOutsideRegularHours = ['postmarket', 'overnight', 'premarket', 'weekend'].includes(catalystMarketSession)
  const eventInSessionWindow = Boolean(
    mainCatalyst.in_session_window ||
    (eventSec != null && sessionStart != null && sessionEnd != null && eventSec >= sessionStart && eventSec <= sessionEnd)
  )
  const hasSessionCatalyst = eventInSessionWindow || Number(row.catalyst_window_count || row.catalyst_window_article_count || 0) > 0
  const catalystFreshByAge = ageMinutes != null && ageMinutes <= PREDICTION_FRESH_CATALYST_MAX_AGE_MINUTES
  const catalystFreshForSession = Boolean(
    validation.recognizedNewsCatalyst &&
    (eventInSessionWindow || catalystFreshByAge || (hasSessionCatalyst && ageMinutes != null && ageMinutes <= PREDICTION_SESSION_CATALYST_MAX_AGE_MINUTES))
  )
  const verifiedPrimary = Boolean(
    validation.recognizedNewsCatalyst ||
    validation.recognizedSqueezeCatalyst ||
    validation.verifiedShortInterest ||
    validation.recognizedPeopleAttention ||
    validation.recognizedSocialCatalyst
  )
  const hasStillLivePrimary = Boolean(
    freshEntryCross ||
    validation.verifiedShortInterest ||
    validation.recognizedSqueezeCatalyst ||
    catalystFreshForSession ||
    validation.recognizedPeopleAttention ||
    validation.recognizedSocialCatalyst
  )
  const extendedMove = change != null && change >= PREDICTION_EXTENDED_MOVE_REQUIRES_ENTRY_PCT
  const alreadyBigMove = change != null && change >= PREDICTION_ALREADY_PRICED_IN_MOVE_PCT
  const activeButNotFresh = setupStatus === 'active_setup_already_above_threshold' || setupStatus === 'near_threshold_setup'
  const priceReaction = row.catalyst_price_reaction || context.priceReaction || null
  const postCatalystRunupPct = nullableNumber(priceReaction?.post_catalyst_runup_pct)
  const postCatalystGivebackPct = nullableNumber(priceReaction?.post_catalyst_giveback_from_high_pct)
  const postCatalystLatestReturnPct = nullableNumber(priceReaction?.post_catalyst_latest_return_pct)
  const postCatalystFading = Boolean(
    priceReaction?.available &&
    postCatalystRunupPct != null &&
    postCatalystGivebackPct != null &&
    postCatalystRunupPct >= PREDICTION_POST_CATALYST_MIN_RUNUP_FOR_FADE_PCT &&
    postCatalystGivebackPct >= PREDICTION_POST_CATALYST_FADE_REJECT_PCT
  )
  const postCatalystRunAlreadyDone = Boolean(
    priceReaction?.available &&
    postCatalystRunupPct != null &&
    postCatalystRunupPct >= PREDICTION_POST_CATALYST_MAX_RUNUP_WITHOUT_ENTRY_PCT &&
    !freshEntryCross
  )
  const unaffectedGiveback = Boolean(
    priceReaction?.available &&
    postCatalystGivebackPct != null &&
    postCatalystGivebackPct > PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT
  )
  const unaffectedRunupAlreadyStarted = Boolean(
    priceReaction?.available &&
    postCatalystRunupPct != null &&
    postCatalystRunupPct > PREDICTION_UNAFFECTED_MAX_RUNUP_PCT
  )
  const quoteIssue = row.quote_status && row.quote_status !== 'priced' ? 'QUOTE_NOT_PRICED' : null
  let rejection = quoteIssue
  if (!rejection && PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && !validation.recognizedNewsCatalyst) {
    rejection = 'UNAFFECTED_SETUP_REQUIRES_RECOGNIZED_NEWS_CATALYST'
  }
  if (!rejection && PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && !catalystOutsideRegularHours) {
    rejection = `CATALYST_NOT_AFTER_HOURS_${catalystMarketSession || 'missing'}`
  }
  if (!rejection && PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && !eventInSessionWindow) {
    rejection = 'CATALYST_NOT_IN_CURRENT_AFTER_HOURS_WINDOW'
  }
  if (!rejection && PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && unaffectedGiveback) {
    rejection = `POST_CATALYST_FADED_${postCatalystGivebackPct.toFixed(2)}PCT_UNAFFECTED_REJECT`
  }
  if (!rejection && PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && unaffectedRunupAlreadyStarted) {
    rejection = `POST_CATALYST_ALREADY_MOVED_${postCatalystRunupPct.toFixed(2)}PCT_UNAFFECTED_REJECT`
  }
  if (!rejection && postCatalystFading && !freshEntryCross) {
    rejection = `POST_CATALYST_FADE_${postCatalystGivebackPct.toFixed(1)}PCT_FROM_HIGH`
  }
  if (!rejection && postCatalystRunAlreadyDone) {
    rejection = `POST_CATALYST_RUN_ALREADY_DONE_${postCatalystRunupPct.toFixed(1)}PCT`
  }
  if (!rejection && priceReaction?.available && postCatalystLatestReturnPct != null && postCatalystLatestReturnPct <= -3 && !freshEntryCross) {
    rejection = `POST_CATALYST_REACTION_NEGATIVE_${postCatalystLatestReturnPct.toFixed(1)}PCT`
  }
  if (!rejection && alreadyBigMove && !freshEntryCross) {
    rejection = `ALREADY_PRICED_IN_MOVE_${change.toFixed(1)}PCT_REQUIRES_FRESH_DENSITY_ENTRY`
  }
  if (!rejection && extendedMove && activeButNotFresh && !freshEntryCross) {
    rejection = `EXTENDED_MOVE_${change.toFixed(1)}PCT_SETUP_NOT_NEW`
  }
  if (!rejection && extendedMove && !hasStillLivePrimary) {
    rejection = `EXTENDED_MOVE_${change.toFixed(1)}PCT_REQUIRES_LIVE_PRIMARY_CATALYST`
  }
  if (!rejection && validation.recognizedNewsCatalyst && extendedMove && !catalystFreshForSession && !freshEntryCross && !validation.verifiedShortInterest) {
    rejection = `CATALYST_ALREADY_PRICED_IN_${Math.round(ageMinutes || 0)}M_OLD`
  }
  if (!rejection && !verifiedPrimary && extendedMove && !freshEntryCross) {
    rejection = 'NO_PRIMARY_CATALYST_FOR_EXTENDED_MOVE'
  }

  const reactionClassification = classifyCatalystReaction({
    ...(priceReaction || {}),
    available: Boolean(priceReaction?.available),
    catalystMarketSession,
    catalystOutsideRegularHours,
    eventInSessionWindow,
    rejection,
  })

  const state = rejection
    ? 'rejected_priced_in_or_unverified'
    : freshEntryCross
      ? 'fresh_density_entry_verified'
      : catalystFreshForSession
        ? 'fresh_session_catalyst_verified'
        : validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest
          ? 'verified_squeeze_interest'
          : 'validated_but_watch_closely'

  return {
    state,
    rejection,
    freshEntryCross,
    setupStatus,
    eventSec,
    catalystMarketSession,
    catalystOutsideRegularHours,
    catalystAgeMinutes: ageMinutes,
    eventInSessionWindow,
    hasSessionCatalyst,
    catalystFreshByAge,
    catalystFreshForSession,
    verifiedPrimary,
    hasStillLivePrimary,
    extendedMove,
    alreadyBigMove,
    priceReactionAvailable: Boolean(priceReaction?.available),
    reactionState: reactionClassification.state,
    reactionLabel: reactionClassification.label,
    reactionTone: reactionClassification.tone,
    reactionReason: reactionClassification.reason,
    marketHadChanceToReact: reactionClassification.market_had_chance_to_react,
    actionableSpillover: reactionClassification.actionable_spillover,
    exhaustionRisk: reactionClassification.exhaustion_risk,
    post_catalyst_bar_count: nullableNumber(priceReaction?.post_catalyst_bar_count),
    post_catalyst_volume: nullableNumber(priceReaction?.post_catalyst_volume),
    post_catalyst_dollar_volume: nullableNumber(priceReaction?.post_catalyst_dollar_volume),
    postCatalystRunupPct,
    postCatalystGivebackPct,
    postCatalystLatestReturnPct,
    postCatalystFading,
    postCatalystRunAlreadyDone,
    unaffectedAfterHoursRequired: PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST,
    unaffectedGiveback,
    unaffectedRunupAlreadyStarted,
    unaffectedMaxGivebackPct: PREDICTION_UNAFFECTED_MAX_GIVEBACK_PCT,
    unaffectedMaxRunupPct: PREDICTION_UNAFFECTED_MAX_RUNUP_PCT,
    quoteFreshness: row.quote_freshness || null,
    quoteAgeSeconds: row.quote_age_seconds ?? null,
  }
}

function predictionPricedInRejection(row = {}, threshold = {}, validation = {}, context = {}) {
  return predictionCatalystReactionState(row, threshold, validation, context).rejection
}

function predictionDiscoveryTier(row = {}) {
  const readiness = row.prediction_readiness || {}
  const level = String(row.prediction_readiness_level || readiness.level || '').toLowerCase()
  const reaction = row.catalyst_reaction_summary || readiness.reaction || {}
  const reactionLabel = String(reaction.label || '').toLowerCase()
  const reactionState = String(reaction.first_reaction_state || reaction.state || '').toLowerCase()
  const sourceCode = String(row.prediction_source_code || '').toLowerCase()
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const blocked = Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []
  const score = Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score ?? 0)
  const hasHardBlock = riskFlags.includes('REJECTED_CATALYST_QUALITY') ||
    riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') ||
    blocked.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') ||
    blocked.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE')

  if (hasHardBlock) {
    return {
      tier: 'excluded_or_needs_review',
      label: 'Excluded / Needs Review',
      tone: 'warning',
      rank_order: 50,
      reason: 'hard evidence-quality block is active',
    }
  }
  if (reaction.exhaustion_risk || ['gave_back_from_high', 'spike_then_fade', 'already_priced_in_strong_reaction', 'negative_first_reaction', 'rejected_already_priced_or_faded'].includes(reactionState)) {
    return {
      tier: 'reaction_risk_watch',
      label: 'Reaction Risk',
      tone: 'warning',
      rank_order: 4.5,
      reason: reaction.reaction_reason || 'post-catalyst OHLC reaction shows fade, exhaustion, or a negative first reaction',
    }
  }
  if (level === 'high_conviction_prediction' || row.high_conviction === true) {
    return {
      tier: 'high_conviction',
      label: 'High Conviction',
      tone: 'success',
      rank_order: 1,
      reason: 'passed the strict high-conviction gate',
    }
  }
  if (level === 'trade_ready_prediction' || row.prediction_trade_ready === true) {
    return {
      tier: 'trade_ready',
      label: 'Trade Ready',
      tone: 'success',
      rank_order: 2,
      reason: 'passed the live trade-readiness gate',
    }
  }
  if (level === 'fresh_catalyst_pending_open' || reactionLabel.includes('pending')) {
    return {
      tier: 'unpriced_or_pending_reaction_catalyst',
      label: 'Unpriced / Pending Reaction',
      tone: 'info',
      rank_order: 3,
      reason: 'fresh catalyst exists and the first real market reaction is still pending or incomplete',
    }
  }
  if (level === 'fresh_catalyst_candidate' || sourceCode.includes('evidence')) {
    return {
      tier: 'developing_evidence_candidate',
      label: 'Developing Candidate',
      tone: 'info',
      rank_order: 4,
      reason: 'multi-factor evidence is present but a stricter entry trigger is still developing',
    }
  }
  if (score >= PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE) {
    return {
      tier: 'active_watch_candidate',
      label: 'Active Watch',
      tone: 'warning',
      rank_order: 5,
      reason: 'watch score meets the developing-candidate floor but strict prediction gates did not pass',
    }
  }
  return {
    tier: 'low_evidence_watch',
    label: 'Low Evidence Watch',
    tone: 'muted',
    rank_order: 6,
    reason: 'insufficient independent evidence for a prediction tier',
  }
}

function withDiscoveryTier(row = {}) {
  const discovery = predictionDiscoveryTier(row)
  return withPredictionSetupClassification(withPredictionScorecard({
    ...row,
    discovery_tier: discovery.tier,
    discovery_tier_label: discovery.label,
    discovery_tier_tone: discovery.tone,
    discovery_rank_order: discovery.rank_order,
    discovery_reason: discovery.reason,
  }))
}

function catalystReactionExhausted(row = {}) {
  const reaction = row.catalyst_reaction_summary || row.prediction_readiness?.reaction || {}
  const state = String(reaction.first_reaction_state || reaction.state || '').toLowerCase()
  return Boolean(reaction.exhaustion_risk) ||
    ['gave_back_from_high', 'spike_then_fade', 'already_priced_in_strong_reaction', 'negative_first_reaction', 'rejected_already_priced_or_faded'].includes(state)
}

function normalizedRiskFlags(row = {}) {
  return Array.from(new Set([
    ...(Array.isArray(row.risk_flags) ? row.risk_flags : []),
    ...(Array.isArray(row.riskFlags) ? row.riskFlags : []),
    ...(Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []),
  ].filter(Boolean).map(String)))
}

function hardRejectionReasonsForRow(row = {}) {
  const flags = normalizedRiskFlags(row)
  const reaction = row.catalyst_reaction_summary || row.prediction_readiness?.reaction || {}
  const state = String(reaction.first_reaction_state || reaction.state || row.catalyst_reaction_state || '').toLowerCase()
  const reasons = []
  const add = (condition, reason) => { if (condition) reasons.push(reason) }
  add(flags.some(flag => /ROUTINE|ROUNDUP|WEAK_CATALYST|REJECTED_CATALYST_QUALITY/i.test(flag)), 'routine_news')
  add(flags.some(flag => /INDIRECT|UNRECOGNIZED_SOURCE|NO_PRIMARY_CATALYST/i.test(flag)), 'indirect_ticker_match')
  add(flags.some(flag => /ALREADY_PRICED|RUN_ALREADY_DONE|EXTENDED_MOVE/i.test(flag)) || state.includes('already_priced'), 'already_priced')
  add(flags.some(flag => /STALE_NEWS|STALE_RECAP/i.test(flag)), 'stale_recap')
  add(flags.some(flag => /IMMATERIAL/i.test(flag)), 'immaterial_for_company_size')
  add(flags.some(flag => /LOW_LIQUIDITY|LIQ_LOW|ABNORMAL_SPREAD/i.test(flag)), 'low_liquidity')
  add(flags.some(flag => /DILUTION|OFFERING/i.test(flag)), 'dilution_risk')
  add(flags.some(flag => /REVERSE_SPLIT/i.test(flag)), 'reverse_split_noise')
  add(flags.some(flag => /LOW_OR_MISSING_SOCIAL|NO_SOCIAL|NEEDS_CONFIRM|NO_FRESH_DENSITY/i.test(flag)), 'insufficient_confirmation')
  add(Boolean(reaction.exhaustion_risk) || ['gave_back_from_high', 'spike_then_fade', 'negative_first_reaction', 'rejected_already_priced_or_faded'].includes(state), 'already_priced')
  return Array.from(new Set(reasons))
}

function predictionSetupClassification(row = {}) {
  const validation = row.prediction_validation || {}
  const readiness = row.prediction_readiness || {}
  const reaction = row.catalyst_reaction_summary || readiness.reaction || {}
  const riskFlags = normalizedRiskFlags(row)
  const hardRejections = hardRejectionReasonsForRow(row)
  const sourceCode = String(row.prediction_source_code || row.prediction_status || '').toLowerCase()
  const catalystQuality = row.catalyst_quality || {}
  const catalystTier = String(row.catalyst_quality_tier || catalystQuality.tier || '').toLowerCase()
  const catalystClass = String(catalystQuality.class || row.main_catalyst?.event_type || row.catalyst_type || '').toLowerCase()
  const hasNewsCatalyst = Boolean(validation.recognizedNewsCatalyst || row.main_catalyst || Number(row.catalyst_window_article_count || row.news_article_count || row.article_count || 0) > 0)
  const hasDirectCatalyst = Boolean(validation.recognizedNewsCatalyst || validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest)
  const peopleAttention = row.people_attention || validation.peopleAttention || {}
  const hasPeopleMomentum = Boolean(peopleAttention.active || sourceCode.includes('people_momentum') || Number(row.message_count || 0) > 0 || Number(row.stocktwits_watcher_count || 0) > 0)
  const state = String(reaction.first_reaction_state || reaction.state || row.catalyst_reaction_state || '').toLowerCase()
  const latest = nullableNumber(reaction.latest_return_pct ?? row.prediction_catalyst_reaction?.postCatalystLatestReturnPct)
  const giveback = nullableNumber(reaction.giveback_from_high_pct ?? row.prediction_catalyst_reaction?.postCatalystGivebackPct)
  const arrow = reaction.exhaustion_risk || state.includes('fade') || state.includes('negative') || (giveback != null && giveback > 20)
    ? 'down'
    : reaction.actionable_spillover || state.includes('building') || (latest != null && latest > 0)
      ? 'up'
      : 'neutral'

  let type = 'active_but_unconfirmed'
  let label = 'Active Watch'
  let reason = 'active mover does not yet have enough independent evidence for a stricter setup'

  if (hardRejections.includes('already_priced')) {
    type = 'already_priced_or_faded'
    label = 'Already Priced / Faded'
    reason = reaction.reaction_reason || 'post-catalyst reaction or current extension suggests the move may already be priced in'
  } else if (hardRejections.includes('routine_news') || catalystTier === 'reject') {
    type = 'invalid_or_routine'
    label = 'Routine / Invalid Catalyst'
    reason = 'headline quality is routine, indirect, weak, or not material enough for a prediction'
  } else if (hasDirectCatalyst && (reaction.actionable_spillover || String(row.prediction_readiness_level || '').includes('fresh_catalyst'))) {
    type = 'fresh_direct_catalyst_unpriced'
    label = 'Fresh Catalyst'
    reason = reaction.reaction_reason || 'recognized catalyst appears fresh and not fully priced in'
  } else if (hasDirectCatalyst && String(row.prediction_readiness_level || '').includes('waiting_for_density')) {
    type = 'fresh_catalyst_waiting_density'
    label = 'Fresh Catalyst · Wait Density'
    reason = 'direct catalyst exists but message-density or payoff confirmation is still pending'
  } else if (hasDirectCatalyst) {
    type = 'confirmed_catalyst_monitor'
    label = 'Confirmed Catalyst Monitor'
    reason = validation.reason || 'recognized catalyst exists, but setup confirmation is incomplete'
  } else if (hasPeopleMomentum && hasNewsCatalyst) {
    type = 'people_momentum_with_news'
    label = 'People Momentum + News'
    reason = 'people/message attention is supporting a ticker with news context, but the primary catalyst is not strict enough yet'
  } else if (hasPeopleMomentum) {
    type = 'people_momentum_no_direct_catalyst'
    label = 'People Momentum'
    reason = 'traders are active, but no fresh direct catalyst has been validated'
  }

  return {
    type,
    label,
    arrow,
    reason,
    direct_catalyst: hasDirectCatalyst,
    people_momentum: hasPeopleMomentum,
    reaction_state: state || null,
    hard_rejection_reasons: hardRejections,
    catalyst_class: catalystClass || null,
    catalyst_quality_tier: catalystTier || null,
  }
}

function withPredictionSetupClassification(row = {}) {
  const setup = predictionSetupClassification(row)
  const rawCatalystClass = String(row.main_catalyst?.catalyst_taxonomy || row.main_catalyst?.event_type || row.catalyst_type || '').toLowerCase()
  const catalystLabels = {
    merger_acquisition: 'Merger / Acquisition',
    biotech_regulatory_or_trial: 'FDA / Clinical Catalyst',
    contract_award: 'Contract / Award',
    partnership_commercial: 'Partnership',
    financing_runway: 'Financing / Runway',
    earnings_guidance: 'Earnings / Guidance',
    product_or_ip: 'Product / IP',
    analyst_action: 'Analyst Action',
    insider_activity: 'Insider Activity',
    legal_regulatory: 'Legal / Regulatory',
    ordinary_news: 'General News',
    routine_news: 'Routine News',
  }
  const catalystLabel = catalystLabels[rawCatalystClass] || (rawCatalystClass && rawCatalystClass !== 'general_news' ? rawCatalystClass.replaceAll('_', ' ') : '') || null
  return {
    ...row,
    setup_classification: setup,
    setup_type: setup.type,
    setup_label: setup.label,
    setup_arrow: setup.arrow,
    setup_reason: setup.reason,
    hard_rejection_reasons: setup.hard_rejection_reasons,
    catalyst_label: catalystLabel || (setup.direct_catalyst ? 'Catalyst' : setup.people_momentum ? 'People Momentum' : null),
    catalyst_direction: row.catalyst_direction || setup.arrow,
  }
}

function scorecardTone(score) {
  const n = Number(score)
  if (!Number.isFinite(n)) return 'unknown'
  if (n >= 75) return 'strong'
  if (n >= 55) return 'moderate'
  if (n >= 35) return 'weak'
  return 'poor'
}

function riskLevel(score) {
  const n = Number(score)
  if (!Number.isFinite(n)) return 'unknown'
  if (n >= 70) return 'critical'
  if (n >= 45) return 'high'
  if (n >= 20) return 'moderate'
  return 'low'
}

function boundedScore(value, fallback = 0) {
  const n = Number(value)
  if (!Number.isFinite(n)) return Math.max(0, Math.min(100, fallback))
  return Math.max(0, Math.min(100, n))
}

function confidenceStackComponent(key, label, score, reason, extra = {}) {
  const cleanScore = boundedScore(score)
  return {
    key,
    label,
    score: Number(cleanScore.toFixed(1)),
    tone: key === 'penalty' ? riskLevel(cleanScore) : scorecardTone(cleanScore),
    active: cleanScore >= 45,
    reason,
    ...extra,
  }
}

function buildPredictionConfidenceStack(row = {}, parts = {}) {
  const profile = row.developing_confirmation_profile || {}
  const validation = row.prediction_validation || {}
  const readiness = row.prediction_readiness || {}
  const reaction = row.catalyst_reaction_summary || readiness.reaction || row.prediction_catalyst_reaction || {}
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const blockedReasons = Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []
  const relVolume = nullableNumber(row.rel_volume)
  const change = nullableNumber(row.change_pct)
  const social = nullableNumber(row.message_count ?? row.stocktwits_message_count) || 0
  const news = nullableNumber(row.catalyst_window_article_count ?? row.news_article_count ?? row.article_count) || 0
  const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
  const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
  const floatShort = nullableNumber(row.float_short ?? row.short_interest_pct)
  const squeezeScore = nullableNumber(row.short_squeeze_score ?? row.squeezeScore)
  const correlation = nullableNumber(row.correlation_score ?? row.price_density_correlation)
  const catalystQualityScore = nullableNumber(parts.catalystQualityScore ?? row.catalyst_quality_score ?? row.catalyst_quality?.score)
  const finalScore = nullableNumber(parts.finalScore ?? row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score) || 0
  const confidence = nullableNumber(row.prediction_confidence ?? row.confidence ?? row.prediction?.confidence)
  const probabilityUp = nullableNumber(row.probability_up ?? row.prediction?.probabilityUp)
  const payoffProbability = nullableNumber(row.payoff_model_probability)

  const hasFreshDensity = Boolean(profile.hasFreshDensity || row.entry_signal?.status === 'entry_passed' || row.entry_signal?.passed || row.threshold_policy?.passed)
  const setupStatus = String(row.entry_signal?.setup_status || row.threshold_policy?.setupStatus || row.threshold_setup_status || '').toLowerCase()
  const hasDirectCatalyst = Boolean(profile.hasDirectCatalyst || validation.recognizedNewsCatalyst || validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest || row.main_catalyst || news > 0)
  const hasPeople = Boolean(profile.hasPeople || row.people_attention?.active || social > 0 || watcherCount > 0)
  const hasSqueeze = Boolean(profile.hasSqueeze || validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest || (squeezeScore != null && squeezeScore >= 40) || (floatShort != null && floatShort >= 10))
  const marketConfirmed = Boolean(profile.marketConfirmed || (relVolume != null && relVolume >= 1.25 && change != null && change > 0))

  const marketScore = boundedScore(
    (relVolume == null ? 12 : relVolume >= 6 ? 42 : relVolume >= 3 ? 34 : relVolume >= 1.5 ? 24 : relVolume >= 1 ? 14 : 5) +
    (change == null ? 0 : change >= 15 ? 28 : change >= 6 ? 22 : change > 0 ? 12 : change === 0 ? 3 : -20) +
    (marketConfirmed ? 16 : 0) +
    (reaction.actionable_spillover ? 12 : 0) -
    (riskFlags.includes('QUOTE_NOT_PRICED') ? 25 : 0),
  )
  const catalystScore = boundedScore(
    (catalystQualityScore != null ? catalystQualityScore : hasDirectCatalyst ? 50 : 8) +
    (validation.materialDirectCatalyst ? 12 : 0) +
    (validation.recognizedNewsCatalyst ? 8 : 0) +
    (riskFlags.includes('WEAK_CATALYST_QUALITY') ? -22 : 0) +
    (riskFlags.includes('REJECTED_CATALYST_QUALITY') ? -55 : 0),
  )
  const peopleScore = boundedScore(
    Math.log1p(Math.max(0, social)) * 18 +
    Math.log1p(Math.max(0, watcherCount) / 5000) * 16 +
    (row.people_attention?.active ? 20 : 0) +
    (hasPeople ? 12 : 0) +
    (sentiment != null && sentiment > 0.08 ? 10 : sentiment != null && sentiment < -0.05 ? -16 : 0),
  )
  const densityScore = boundedScore(
    (hasFreshDensity ? 78 : setupStatus === 'near_threshold_setup' ? 48 : setupStatus === 'active_setup_already_above_threshold' ? 42 : 18) +
    (correlation != null ? Math.max(0, Math.min(22, correlation * 38)) : 0) -
    (riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') ? 20 : 0) -
    (riskFlags.includes('LATE_ENTRY_REJECTED') ? 32 : 0),
  )
  const floatScore = boundedScore(
    (hasSqueeze ? 45 : 16) +
    (squeezeScore != null ? Math.min(32, squeezeScore * 0.38) : 0) +
    (floatShort != null ? Math.min(28, floatShort * 1.2) : 0),
  )
  const calibrationScore = boundedScore(
    (payoffProbability != null ? payoffProbability * 100 : probabilityUp != null ? probabilityUp * 92 : confidence != null ? confidence * 88 : finalScore * 0.75) +
    (row.payoff_model_passes === true ? 10 : row.payoff_model_passes === false ? -18 : 0),
  )
  const penaltyScore = boundedScore(
    (reaction.exhaustion_risk ? 30 : 0) +
    (change != null && change >= 45 ? 30 : change != null && change >= 25 ? 18 : 0) +
    (riskFlags.includes('EXTENDED_MOVE_REQUIRES_FRESH_VALIDATION') ? 18 : 0) +
    (riskFlags.includes('NEGATIVE_SENTIMENT_HEADWIND') ? 12 : 0) +
    (riskFlags.includes('BELOW_PAYOFF_MODEL_THRESHOLD') ? 15 : 0) +
    (riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') || riskFlags.includes('NO_SOCIAL_CONFIRMATION') ? 10 : 0) +
    (blockedReasons.length ? Math.min(24, blockedReasons.length * 8) : 0),
  )

  const positiveBlend =
    marketScore * 0.23 +
    catalystScore * 0.2 +
    peopleScore * 0.13 +
    densityScore * 0.14 +
    floatScore * 0.1 +
    calibrationScore * 0.2
  const overall = boundedScore(positiveBlend - penaltyScore * 0.28)
  const stackConfidence = Number((overall / 100).toFixed(3))
  const bars = [
    confidenceStackComponent('market', 'Mkt', marketScore, relVolume != null ? `${relVolume.toFixed(2)}x rel vol, ${change != null ? change.toFixed(2) : 'unknown'}% move` : 'market reaction incomplete'),
    confidenceStackComponent('catalyst', 'Cat', catalystScore, hasDirectCatalyst ? 'validated catalyst evidence present' : 'no strong direct catalyst yet'),
    confidenceStackComponent('people', 'Ppl', peopleScore, hasPeople ? `${social} messages, ${watcherCount} watchers` : 'people/social pressure is thin'),
    confidenceStackComponent('density', 'Den', densityScore, hasFreshDensity ? 'fresh density entry/support detected' : setupStatus || 'density setup not confirmed'),
    confidenceStackComponent('float', 'Flt', floatScore, hasSqueeze ? 'float/squeeze pressure contributes' : 'float/squeeze pressure limited'),
    confidenceStackComponent('calibration', 'Cal', calibrationScore, payoffProbability != null ? `payoff model ${Math.round(payoffProbability * 100)}%` : probabilityUp != null ? `probability up ${Math.round(probabilityUp * 100)}%` : 'model calibration pending'),
    confidenceStackComponent('penalty', 'Risk', penaltyScore, penaltyScore > 0 ? 'overextension or blocker penalty applied' : 'no material overextension penalty', { inverse: true }),
  ]

  return {
    overall: Number(overall.toFixed(1)),
    confidence: confidence ?? stackConfidence,
    stack_confidence: stackConfidence,
    probability_up: probabilityUp,
    payoff_model_probability: payoffProbability,
    bars,
    components: Object.fromEntries(bars.map(bar => [bar.key, bar.score])),
    reasons: bars.filter(bar => bar.key !== 'penalty' && bar.score >= 55).map(bar => `${bar.label}: ${bar.reason}`),
    penalties: penaltyScore > 0 ? bars.filter(bar => bar.key === 'penalty').map(bar => bar.reason) : [],
    model: row.prediction?.model || row.model_mode || row.modelMode || null,
    calibrated: Boolean(payoffProbability != null || probabilityUp != null || confidence != null),
  }
}

function buildAiEvidenceScore(row = {}, parts = {}) {
  const validation = row.prediction_validation || {}
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const relVolume = nullableNumber(row.rel_volume) || 0
  const change = nullableNumber(row.change_pct) || 0
  const news = nullableNumber(row.catalyst_window_article_count ?? row.news_article_count ?? row.article_count) || 0
  const social = nullableNumber(row.message_count ?? row.stocktwits_message_count) || 0
  const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
  const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
  const squeezeScore = nullableNumber(row.short_squeeze_score ?? row.squeezeScore) || 0
  const floatShort = nullableNumber(row.float_short ?? row.short_interest_pct) || 0
  const catalystQualityScore = nullableNumber(row.catalyst_quality_score ?? row.catalyst_quality?.score ?? parts.catalystQualityScore)
  const densityStatus = String(row.entry_signal?.setup_status || row.threshold_policy?.setupStatus || row.threshold_setup_status || '').toLowerCase()
  const correlation = nullableNumber(row.correlation_score ?? row.price_density_correlation)

  const catalyst = boundedScore(
    (catalystQualityScore != null ? catalystQualityScore : news > 0 ? 45 : 8) +
    (validation.recognizedNewsCatalyst ? 10 : 0) +
    (validation.materialDirectCatalyst ? 10 : 0) +
    (row.sec_filing_contributed ? 5 : 0) -
    (riskFlags.includes('WEAK_CATALYST_QUALITY') ? 18 : 0) -
    (riskFlags.includes('REJECTED_CATALYST_QUALITY') ? 55 : 0),
  )
  const marketReaction = boundedScore(
    Math.log1p(Math.max(0, relVolume)) * 24 +
    Math.min(28, Math.max(0, change) * 1.15) +
    (row.catalyst_reaction_summary?.actionable_spillover ? 16 : 0),
  )
  const peoplePressure = boundedScore(
    Math.log1p(Math.max(0, social)) * 18 +
    Math.log1p(Math.max(0, watcherCount) / 3000) * 14 +
    (row.people_attention?.active ? 18 : 0),
  )
  const density = boundedScore(
    (row.entry_signal?.status === 'entry_passed' || row.entry_signal?.passed ? 82 : densityStatus === 'near_threshold_setup' ? 48 : densityStatus === 'active_setup_already_above_threshold' ? 40 : 18) +
    (correlation != null ? Math.max(0, Math.min(18, correlation * 36)) : 0),
  )
  const squeezeFloat = boundedScore(
    (validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest ? 48 : 10) +
    Math.min(30, squeezeScore * 0.38) +
    Math.min(24, floatShort * 1.15),
  )
  const sentimentAlignment = boundedScore(50 + sentiment * 130)
  const penalty = boundedScore(
    (riskFlags.includes('NO_CATALYST_PRICE_REACTION_OHLC') ? 25 : 0) +
    (riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') ? 12 : 0) +
    (riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') || riskFlags.includes('NO_SOCIAL_CONFIRMATION') ? 10 : 0) +
    (riskFlags.includes('NEGATIVE_SENTIMENT_HEADWIND') ? 14 : 0) +
    (riskFlags.includes('EXTENDED_MOVE_REQUIRES_FRESH_VALIDATION') ? 20 : 0) +
    (riskFlags.includes('BELOW_PAYOFF_MODEL_THRESHOLD') ? 16 : 0) +
    (row.catalyst_reaction_summary?.exhaustion_risk ? 28 : 0),
  )
  const score = boundedScore(
    catalyst * 0.24 +
    marketReaction * 0.2 +
    peoplePressure * 0.14 +
    density * 0.14 +
    squeezeFloat * 0.1 +
    sentimentAlignment * 0.08 +
    boundedScore(parts.finalScore ?? row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.evidence_score) * 0.1 -
    penalty * 0.28,
  )
  const components = {
    catalyst: Number(catalyst.toFixed(1)),
    market_reaction: Number(marketReaction.toFixed(1)),
    people_pressure: Number(peoplePressure.toFixed(1)),
    density: Number(density.toFixed(1)),
    float_squeeze: Number(squeezeFloat.toFixed(1)),
    sentiment_alignment: Number(sentimentAlignment.toFixed(1)),
    penalty: Number(penalty.toFixed(1)),
  }
  return {
    score: Number(score.toFixed(1)),
    confidence: Number(Math.max(0.2, Math.min(0.92, score / 100)).toFixed(3)),
    components,
    reasons: Object.entries(components)
      .filter(([key, value]) => key !== 'penalty' && Number(value) >= 60)
      .map(([key, value]) => `${key.replaceAll('_', ' ')} ${Math.round(Number(value))}/100`),
    penalty_reasons: penalty > 0 ? riskFlags.slice(0, 5) : [],
    model: 'ai_evidence_score_v2',
  }
}

function buildPredictionScorecard(row = {}) {
  const finalScore = nullableNumber(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score) || 0
  const probabilityUp = nullableNumber(row.probability_up ?? row.prediction?.probabilityUp)
  const expectedReturn = nullableNumber(row.predicted_return ?? row.predictedReturnPct ?? row.final_predicted_percent)
  const confidence = nullableNumber(row.prediction_confidence ?? row.confidence ?? row.prediction?.confidence)
  const relVolume = nullableNumber(row.rel_volume)
  const volume = nullableNumber(row.volume)
  const price = nullableNumber(row.price)
  const dollarVolume = volume != null && price != null ? volume * price : null
  const change = nullableNumber(row.change_pct)
  const social = nullableNumber(row.message_count)
  const news = nullableNumber(row.news_article_count ?? row.article_count)
  const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
  const catalystQuality = row.catalyst_quality || {}
  const catalystScore = nullableNumber(row.catalyst_quality_score ?? catalystQuality.score)
  const reaction = row.catalyst_reaction_summary || row.prediction_readiness?.reaction || {}
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const missingFields = Array.isArray(row.missing_prediction_fields) ? row.missing_prediction_fields : []
  const hasNews = Number(news || 0) > 0
  const hasSocial = Number(social || 0) > 0
  const hasCatalyst = Boolean(row.main_catalyst || hasNews || row.catalyst_summary)
  const hasOhlcReaction = Boolean(reaction.available || reaction.post_catalyst_bar_count)
  const hasCorrelation = nullableNumber(row.correlation_score ?? row.price_density_correlation) != null
  const hasPayoff = nullableNumber(row.payoff_model_probability) != null

  const catalystQualityScore = catalystScore != null ? Math.max(0, Math.min(100, catalystScore)) : hasCatalyst ? 45 : 10
  const timingQualityScore = Math.max(0, Math.min(100,
    (reaction.actionable_spillover ? 35 : 0) +
    (reaction.market_had_chance_to_react ? 20 : 0) +
    (reaction.first_reaction_state === 'pending_market_open' ? 28 : 0) +
    (reaction.first_reaction_state === 'muted_unpriced_reaction' ? 38 : 0) +
    (reaction.first_reaction_state === 'building_positive_reaction' ? 32 : 0) -
    (reaction.exhaustion_risk ? 45 : 0) -
    (riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') ? 10 : 0) -
    (riskFlags.includes('STALE_OR_OUT_OF_WINDOW_CATALYST') ? 20 : 0)
  ))
  const liquidityRiskScore = Math.max(0, Math.min(100,
    (price != null && price < 1 ? 25 : 0) +
    (relVolume != null && relVolume < 0.75 ? 18 : 0) +
    (dollarVolume != null && dollarVolume < 500_000 ? 35 : dollarVolume != null && dollarVolume < 2_000_000 ? 20 : 0) +
    (volume != null && volume < 100_000 ? 20 : 0) +
    (riskFlags.includes('QUOTE_NOT_PRICED') ? 50 : 0)
  ))
  const reversalRiskScore = Math.max(0, Math.min(100,
    (reaction.exhaustion_risk ? 45 : 0) +
    (Number(reaction.giveback_from_high_pct || 0) >= 2 ? 20 : 0) +
    (change != null && change >= 35 ? 25 : change != null && change >= 15 ? 12 : 0) +
    (riskFlags.includes('EXTENDED_MOVE_REQUIRES_FRESH_VALIDATION') ? 20 : 0) +
    (riskFlags.includes('NEGATIVE_SENTIMENT_HEADWIND') ? 10 : 0) +
    (riskFlags.includes('BELOW_PAYOFF_MODEL_THRESHOLD') ? 12 : 0)
  ))
  const evidenceInputs = [price != null, relVolume != null, hasNews, hasSocial, hasCatalyst, hasOhlcReaction, hasCorrelation, hasPayoff, sentiment != null]
  const evidenceCompletenessScore = Math.max(0, Math.min(100,
    (evidenceInputs.filter(Boolean).length / evidenceInputs.length) * 100 - missingFields.length * 5
  ))
  const signalQualityScore = Math.max(0, Math.min(100,
    finalScore * 0.38 +
    catalystQualityScore * 0.18 +
    timingQualityScore * 0.16 +
    evidenceCompletenessScore * 0.12 +
    Math.max(0, 100 - liquidityRiskScore) * 0.08 +
    Math.max(0, 100 - reversalRiskScore) * 0.08
  ))
  const expectedRangeWidth = expectedReturn == null
    ? null
    : Math.max(0.4, Math.abs(expectedReturn) * (1 - Math.max(0.25, Math.min(0.9, confidence ?? 0.45))) + 0.35 + reversalRiskScore / 120)
  const expectedMoveLow = expectedReturn == null ? null : Number((expectedReturn - expectedRangeWidth).toFixed(2))
  const expectedMoveHigh = expectedReturn == null ? null : Number((expectedReturn + expectedRangeWidth).toFixed(2))
  const primaryReasons = [
    catalystQualityScore >= 70 ? `strong catalyst quality ${Math.round(catalystQualityScore)}/100` : '',
    reaction.actionable_spillover ? `reaction ${reaction.label || reaction.first_reaction_state || 'supports spillover'}` : '',
    relVolume != null && relVolume >= 2 ? `${relVolume.toFixed(2)}x relative volume` : '',
    hasSocial ? `${social} social mention${Number(social) === 1 ? '' : 's'}` : '',
    hasNews ? `${news} news item${Number(news) === 1 ? '' : 's'}` : '',
    probabilityUp != null ? `${Math.round(probabilityUp * 100)}% probability up` : '',
  ].filter(Boolean)
  const primaryCautions = [
    liquidityRiskScore >= 55 ? `liquidity risk ${Math.round(liquidityRiskScore)}/100` : '',
    reversalRiskScore >= 55 ? `reversal risk ${Math.round(reversalRiskScore)}/100` : '',
    reaction.exhaustion_risk ? reaction.reaction_reason || 'reaction exhaustion risk' : '',
    !hasOhlcReaction ? 'OHLC reaction still unavailable' : '',
    riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') ? 'low/missing social confirmation' : '',
    riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') ? 'no fresh density entry cross' : '',
    riskFlags.includes('BELOW_PAYOFF_MODEL_THRESHOLD') ? 'below payoff model threshold' : '',
  ].filter(Boolean)
  const confidenceStack = buildPredictionConfidenceStack(row, {
    finalScore,
    probabilityUp,
    expectedReturn,
    confidence,
    catalystQualityScore,
    timingQualityScore,
    liquidityRiskScore,
    reversalRiskScore,
    evidenceCompletenessScore,
    signalQualityScore,
  })

  return {
    probability_up: probabilityUp,
    expected_move_pct: expectedReturn,
    expected_move_low_pct: expectedMoveLow,
    expected_move_high_pct: expectedMoveHigh,
    confidence,
    signal_quality_score: Number(signalQualityScore.toFixed(1)),
    signal_quality: scorecardTone(signalQualityScore),
    catalyst_quality_score: Number(catalystQualityScore.toFixed(1)),
    catalyst_quality_tier: scorecardTone(catalystQualityScore),
    timing_quality_score: Number(timingQualityScore.toFixed(1)),
    timing_quality: scorecardTone(timingQualityScore),
    liquidity_risk_score: Number(liquidityRiskScore.toFixed(1)),
    liquidity_risk: riskLevel(liquidityRiskScore),
    reversal_risk_score: Number(reversalRiskScore.toFixed(1)),
    reversal_risk: riskLevel(reversalRiskScore),
    evidence_completeness_score: Number(evidenceCompletenessScore.toFixed(1)),
    evidence_completeness: scorecardTone(evidenceCompletenessScore),
    primary_reasons: primaryReasons,
    primary_cautions: primaryCautions,
    confidence_stack: confidenceStack,
    dollar_volume: dollarVolume == null ? null : Number(dollarVolume.toFixed(2)),
    inputs_present: {
      price: price != null,
      relative_volume: relVolume != null,
      news: hasNews,
      social: hasSocial,
      catalyst: hasCatalyst,
      ohlc_reaction: hasOhlcReaction,
      correlation: hasCorrelation,
      payoff_model: hasPayoff,
      sentiment: sentiment != null,
    },
  }
}

function withPredictionScorecard(row = {}) {
  const scorecard = buildPredictionScorecard(row)
  return {
    ...row,
    prediction_scorecard: scorecard,
    signal_quality_score: scorecard.signal_quality_score,
    signal_quality: scorecard.signal_quality,
    timing_quality_score: scorecard.timing_quality_score,
    timing_quality: scorecard.timing_quality,
    liquidity_risk_score: scorecard.liquidity_risk_score,
    liquidity_risk: scorecard.liquidity_risk,
    reversal_risk_score: scorecard.reversal_risk_score,
    reversal_risk: scorecard.reversal_risk,
    evidence_completeness_score: scorecard.evidence_completeness_score,
    evidence_completeness: scorecard.evidence_completeness,
    expected_move_low_pct: scorecard.expected_move_low_pct,
    expected_move_high_pct: scorecard.expected_move_high_pct,
    primary_reasons: scorecard.primary_reasons,
    primary_cautions: scorecard.primary_cautions,
    confidence_stack: scorecard.confidence_stack,
    confidence_stack_score: scorecard.confidence_stack?.overall ?? null,
    confidence_stack_reasons: scorecard.confidence_stack?.reasons ?? [],
    confidence_penalties: scorecard.confidence_stack?.penalties ?? [],
  }
}

function marketCapBucket(marketCap) {
  const cap = Number(marketCap || 0)
  if (cap >= 200e9) return 'Mega'
  if (cap >= 10e9) return 'Large'
  if (cap >= 2e9) return 'Mid'
  if (cap >= 300e6) return 'Small'
  if (cap > 0) return 'Micro'
  return 'Unknown'
}

function predictionMarketCapTier(row = {}) {
  return predictionThresholdPolicy.predictionMarketCapTier(row)
}

function clonePlain(value) {
  return JSON.parse(JSON.stringify(value))
}

function predictionThresholdProfile(row = {}, profileOverride = null) {
  return predictionThresholdPolicy.predictionThresholdProfile(row, profileOverride)
}

function evaluatePredictionEntryThreshold(row = {}, features = {}, profileOverride = null) {
  return predictionThresholdPolicy.evaluatePredictionEntryThreshold(row, features, profileOverride)
}

function rollingWindowMinutes(row) {
  const tier = String(row.market_cap_tier || row.finviz_market_cap_tier || '').toLowerCase()
  const bucket = String(row.market_cap_bucket || '').toLowerCase()
  if (tier === 'nano' || tier === 'micro' || bucket === 'micro') return 5
  if (tier === 'small' || bucket === 'small') return 15
  if (tier === 'mid' || bucket === 'mid') return 30
  if (tier === 'large' || bucket === 'large') return 60
  if (tier === 'mega' || bucket === 'mega') return 120
  return 30
}

function resolvedRollingWindowMinutes(row, override = null) {
  const explicit = Number(override)
  if (Number.isFinite(explicit) && explicit > 0) return Math.max(1, Math.min(4320, explicit))
  return rollingWindowMinutes(row)
}

function normalizeScreenerRow(doc = {}) {
  const ticker = String(doc.ticker || '').toUpperCase()
  const hasStoredPrice = doc.price != null
  const price = nullableFixed(doc.price, 2)
  const change = doc.change_pct ?? doc.change_percent
  const changePct = nullableFixed(change, 2)
  const volume = nullableNumber(doc.volume)
  const avgVolume = nullableNumber(doc.avg_volume)
  const storedRelVolume = nullableNumber(doc.rel_volume ?? doc.relative_volume)
  const relVolume = storedRelVolume != null
    ? Number(storedRelVolume.toFixed(2))
    : volume != null && avgVolume ? Number((volume / Math.max(1, avgVolume)).toFixed(2)) : null
  const marketCap = nullableNumber(doc.market_cap)
  const avgSentiment = Number(doc.avg_sentiment ?? doc.news_sentiment ?? doc.structured_sentiment ?? 0)
  const freshness = quoteFreshness(doc.quote_updated_at || doc.finviz_seen_at || doc.tradingview_seen_at)

  return {
    ticker,
    company: doc.company || '',
    price,
    change_pct: changePct,
    volume,
    avg_volume: avgVolume,
    rel_volume: relVolume,
    market_cap: marketCap,
    market_cap_tier: doc.market_cap_tier || doc.finviz_market_cap_tier || '',
    market_cap_bucket: marketCapBucket(marketCap),
    sector: doc.sector || 'Unclassified',
    industry: doc.industry || 'Unclassified',
    country: doc.country || (US_EXCHANGES.has(normalizeExchange(doc.exchange)) ? 'USA' : ''),
    exchange: normalizeExchange(doc.exchange),
    index: doc.index || '',
    avg_sentiment: avgSentiment,
    social_sentiment: Number(doc.social_sentiment ?? 0),
    structured_sentiment: Number(doc.structured_sentiment ?? doc.news_sentiment ?? avgSentiment),
    message_count: Number(doc.message_count ?? 0),
    news_article_count: Number(doc.news_article_count ?? 0),
    bullish_count: Number(doc.bullish_count ?? 0),
    bearish_count: Number(doc.bearish_count ?? 0),
    neutral_count: Number(doc.neutral_count ?? 0),
    sources: doc.sources || [],
    pe_ratio: nullableNumber(doc.pe_ratio ?? doc.pe),
    forward_pe: nullableNumber(doc.forward_pe),
    peg: nullableNumber(doc.peg),
    ps_ratio: nullableNumber(doc.ps_ratio),
    pb_ratio: nullableNumber(doc.pb_ratio),
    dividend_yield: nullableNumber(doc.dividend_yield),
    eps_growth_this_y: nullableNumber(doc.eps_growth_this_y),
    eps_growth_next_y: nullableNumber(doc.eps_growth_next_y),
    sales_growth: nullableNumber(doc.sales_growth),
    gross_margin: nullableNumber(doc.gross_margin),
    operating_margin: nullableNumber(doc.operating_margin),
    roe: nullableNumber(doc.roe),
    debt_equity: nullableNumber(doc.debt_equity),
    beta: nullableNumber(doc.beta),
    rsi: nullableNumber(doc.rsi),
    sma20: nullableNumber(doc.sma20),
    sma50: nullableNumber(doc.sma50),
    sma200: nullableNumber(doc.sma200),
    perf_week: nullableNumber(doc.perf_week),
    perf_month: nullableNumber(doc.perf_month),
    perf_quarter: nullableNumber(doc.perf_quarter),
    perf_half: nullableNumber(doc.perf_half),
    perf_year: nullableNumber(doc.perf_year),
    perf_ytd: nullableNumber(doc.perf_ytd),
    atr: nullableNumber(doc.atr),
    gap: nullableNumber(doc.gap),
    analyst: doc.analyst || null,
    target_price: nullableFixed(doc.target_price, 2),
    inst_own: nullableNumber(doc.inst_own),
    insider_own: nullableNumber(doc.insider_own),
    float_short: nullableNumber(doc.float_short),
    earnings_date: doc.earnings_date || null,
    price_density_correlation: nullableNumber(doc.price_density_correlation),
    previous_price_density_correlation: nullableNumber(doc.previous_price_density_correlation),
    threshold_pre_return_60m_pct: nullableNumber(doc.threshold_pre_return_60m_pct),
    threshold_trailing_60m_messages: nullableNumber(doc.threshold_trailing_60m_messages),
    threshold_feature_window_minutes: nullableNumber(doc.threshold_feature_window_minutes),
    threshold_feature_status: doc.threshold_feature_status || null,
    threshold_setup_status: doc.threshold_setup_status || null,
    threshold_setup_score: nullableNumber(doc.threshold_setup_score),
    threshold_setup_distance_to_entry: nullableNumber(doc.threshold_setup_distance_to_entry),
    threshold_feature_updated_at: doc.threshold_feature_updated_at || null,
    previous_close: nullableFixed(doc.previous_close, 2),
    quote_source: doc.quote_source || null,
    quote_updated_at: doc.quote_updated_at || null,
    finviz_seen_at: doc.finviz_seen_at || null,
    tradingview_seen_at: doc.tradingview_seen_at || null,
    ...freshness,
    quote_status: doc.quote_status || (hasStoredPrice ? 'priced' : 'missing'),
  }
}

function socialTimeStages() {
  return [
    {
      $addFields: {
        _time_raw: {
          $ifNull: [
            '$created_at',
            { $ifNull: ['$timestamp', { $ifNull: ['$publish_date', { $ifNull: ['$detected_at', '$fetched_at'] }] }] },
          ],
        },
      },
    },
    {
      $addFields: {
        _event_sec: {
          $switch: {
            branches: [
              { case: { $eq: [{ $type: '$_time_raw' }, 'date'] }, then: { $floor: { $divide: [{ $toLong: '$_time_raw' }, 1000] } } },
              { case: { $in: [{ $type: '$_time_raw' }, ['int', 'long', 'double', 'decimal']] }, then: { $toLong: '$_time_raw' } },
              {
                case: { $eq: [{ $type: '$_time_raw' }, 'string'] },
                then: { $floor: { $divide: [{ $toLong: { $dateFromString: { dateString: '$_time_raw', onError: new Date(0) } } }, 1000] } },
              },
            ],
            default: 0,
          },
        },
      },
    },
  ]
}

function catalystTypeWeight(value = '') {
  const text = String(value || '').toLowerCase()
  if (/offering|dilution|reverse[_ -]?split|share[_ -]?consolidation|atm\b|warrant|convertible|bankruptcy|default|delisting|investigation|class action|lawsuit|downgrade|price target cut|cuts? target/.test(text)) return 0.2
  if (/announces? date|announces? schedule|conference call|to report|monthly update|weekly share repurchase|director\/pdmr|shareholding|shareholder approval|annual general|special meeting|maintained at|reiterates? guidance|market report|market update|crude oil surges|top\s+.*gainers|stocks? moving|why shares|roundup/.test(text)) return 0.35
  if (/merger|acquisition|completed acquisition|definitive agreement|business combination|buyout|takeover|tender offer/.test(text)) return 2.35
  if (/private placement|secures?.{0,40}(financing|capital)|financing.{0,40}(pipeline|runway)|fund.{0,40}pipeline|non.?dilutive/.test(text)) return 2.15
  if (/short.?squeeze|short interest|days to cover|short covering|borrow|cost to borrow|watcher|stocktwits|retail interest|social squeeze/.test(text)) return 2.25
  if (/targets?\s+(up to\s+)?(us\$|\$|usd)|incremental annualized ebitda|annualized ebitda|ai data center|ai compute|battery energy storage|bess|preferred tenant|tenant bids|non.?dilutive/.test(text)) return 2.0
  if (/strategic capital|secures? capital|growth capital|support next phase|cooperation agreement|expected to generate.*profit|expected.*profit|media growth|entertainment and media growth/.test(text)) return 1.95
  if (/fda|approval|pdufa|clinical_positive|trial_positive|phase.*(success|positive)|breakthrough/.test(text)) return 2.1
  if (/earnings_beat|guidance_raise|revenue_growth|profit|contract|partnership|acquisition|merger|buyout|strategic/.test(text)) return 1.75
  if (/analyst_upgrade|price_target_raise|initiated.*buy|patent|product_launch|regulatory_clearance/.test(text)) return 1.45
  if (/earnings_miss|guidance_cut|fda_rejection|clinical_negative/.test(text)) return 0.25
  if (/sec_filing|filing|8-k|13d|13g|form 4/.test(text)) return 0.85
  return 1
}

function catalystSourceWeight(value = '') {
  const text = String(value || '').toLowerCase()
  if (/sec|edgar/.test(text)) return 1.25
  if (/pr newswire|globenewswire|business wire|accesswire/.test(text)) return 1.12
  if (/benzinga|dow jones|reuters|associated press|marketwatch/.test(text)) return 1.05
  if (/tradingview|finviz/.test(text)) return 0.95
  return 0.9
}

function isRecognizedCatalystSource(value = '') {
  const text = String(value || '').toLowerCase()
  return /sec|edgar|8-k|pr newswire|globenewswire|business wire|accesswire|access newswire|benzinga|dow jones|reuters|associated press|marketwatch|tradingview news flow|yahoo finance|stocktitan|fintel|seekingalpha/.test(text)
}

function isWeakGenericCatalystText(value = '') {
  const text = String(value || '').toLowerCase()
  if (!text.trim()) return true
  return /these companies just dropped|market report|market update|crude oil surges|profiles?\s+.+\s+other|watchlist|latest news|morning market movers|top\s+.*gainers|stocks? moving|why shares|newsletter|investor alert|law firm|lawsuit|class action|investigating claims|net worth|billionaire|wealth jumps|stock gains momentum|shares? gains? momentum|stock move up|shares? rise|shares? rose|shares? rally|stock rallies|market cap jumps|watch:|rest of the sector|sector is being repriced|announces? date|announces? schedule|conference call|to report|monthly update|weekly share repurchase|director\/pdmr|shareholding|shareholder approval|annual general|special meeting|maintained at|reiterates? guidance/.test(text)
}

function isBearishCatalystText(value = '') {
  const text = String(value || '').toLowerCase()
  if (!text.trim()) return false
  return /bearish|earnings_miss|guidance_cut|fda_rejection|clinical_negative|bankruptcy|default|delisting|investigation|investigating|lawsuit|class action|law firm|offering|dilution|reverse_split|share_consolidation|atm|warrant|convertible|short report|downgrade|price target cut|cuts? target|misses estimates|raises stakes/.test(text)
}

function parseCatalystMoneyAmount(value = '') {
  const text = String(value || '').replace(/,/g, '')
  const patterns = [
    /(?:us\$|usd|\$)\s*([0-9]+(?:\.[0-9]+)?)\s*(billion|million|bn|m|k)?/ig,
    /([0-9]+(?:\.[0-9]+)?)\s*(billion|million|bn|m)\s+(?:usd|us dollars|dollars)/ig,
  ]
  let best = null
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      const raw = Number(match[1])
      if (!Number.isFinite(raw) || raw <= 0) continue
      const unit = String(match[2] || '').toLowerCase()
      const multiplier = /billion|bn/.test(unit) ? 1_000_000_000 : /million|m/.test(unit) ? 1_000_000 : /k/.test(unit) ? 1_000 : 1
      const amount = raw * multiplier
      if (best == null || amount > best) best = amount
    }
  }
  return best
}

function catalystTaxonomyAssessment(row = {}, context = {}) {
  const title = String(context.catalystText || row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || '').trim()
  const typeText = String(row.main_catalyst?.event_type || row.main_catalyst?.type || row.structured_catalyst_type || row.event_type || '').trim()
  const sourceText = [
    row.main_catalyst?.source,
    row.main_catalyst?.publisher,
    row.catalyst_source,
    Array.isArray(row.sources) ? row.sources.join(' ') : row.sources,
    row.news_source,
    row.source,
  ].filter(Boolean).join(' ')
  const text = [title, typeText].join(' ').toLowerCase()
  const source = sourceText.toLowerCase()
  const marketCap = nullableNumber(row.market_cap ?? row.marketCap ?? row.cap_value)
  const amount = parseCatalystMoneyAmount(title)
  const amountToMarketCapPct = amount != null && marketCap != null && marketCap > 0
    ? Number((amount / marketCap * 100).toFixed(2))
    : null
  const tickerSpecific = Boolean(context.tickerMatched || catalystMentionsTickerOrCompany(row, title))
  const recognizedSource = isRecognizedCatalystSource(sourceText)
  const roundup = isBroadRoundupCatalyst({ title, source: sourceText, ticker_count: row.main_catalyst?.ticker_count })
  const weakGeneric = isWeakGenericCatalystText(title)
  const bearishRisk = isBearishCatalystText([title, typeText].join(' '))

  let category = 'ordinary_news'
  let direction = 'neutral'
  let baseScore = 12
  let rejectionReason = null
  const cautions = []

  if (!title) {
    rejectionReason = 'no_direct_catalyst'
  } else if (roundup || /top\s+.*gainers|stocks? moving|why shares|market movers|market update|crude oil surges|roundup/.test(text)) {
    category = 'roundup_or_recap'
    rejectionReason = 'stale_recap'
    baseScore = 3
  } else if (/rest of the sector|sector is being repriced|sector read.?through|peer read.?through/.test(text)) {
    category = 'indirect_sector_readthrough'
    rejectionReason = 'indirect_ticker_match'
    baseScore = 5
  } else if (/reverse[_ -]?split|share[_ -]?consolidation/.test(text)) {
    category = 'capital_structure_risk'
    direction = 'negative'
    rejectionReason = 'reverse_split_noise'
    baseScore = 2
  } else if (/offering|dilution|atm\b|warrant|convertible|registered direct|public offering/.test(text)) {
    category = 'dilution_or_financing_risk'
    direction = /fund.{0,40}pipeline|runway|secures?.{0,40}financing|private placement/.test(text) ? 'mixed' : 'negative'
    rejectionReason = 'dilution_risk'
    baseScore = direction === 'mixed' ? 22 : 2
    cautions.push('dilution risk')
  } else if (/lawsuit|class action|investigation|delisting|bankruptcy|default|downgrade|price target cut|guidance cut|clinical_negative|fda_rejection/.test(text)) {
    category = 'negative_event'
    direction = 'negative'
    rejectionReason = 'bearish_catalyst'
    baseScore = 2
  } else if (/announces? date|announces? schedule|conference call|to report|monthly update|weekly share repurchase|director\/pdmr|shareholding|shareholder approval|annual general|special meeting|maintained at|reiterates? guidance/.test(text)) {
    category = 'routine_news'
    rejectionReason = 'routine_news'
    baseScore = 6
  } else if (/merger|acquisition|completed acquisition|definitive agreement|business combination|buyout|takeover|tender offer/.test(text)) {
    category = 'merger_acquisition'
    direction = 'positive'
    baseScore = 42
  } else if (/fda|pdufa|clearance|approval|breakthrough|orphan drug|fast track|phase\s*(1|2|3)|clinical|trial|endpoint|topline|data readout|nda\b|bla\b/.test(text)) {
    category = 'biotech_regulatory_or_trial'
    direction = 'positive'
    baseScore = 40
  } else if (/contract|award|purchase order|supply agreement|government award|defence|defense|navy|army|air force/.test(text)) {
    category = 'contract_award'
    direction = 'positive'
    baseScore = 32
  } else if (/partnership|collaboration|license agreement|commercial agreement|distribution agreement|strategic alliance/.test(text)) {
    category = 'partnership_commercial'
    direction = 'positive'
    baseScore = 30
  } else if (/private placement|secures?.{0,40}(financing|capital)|financing.{0,40}(pipeline|runway)|fund.{0,40}pipeline|non.?dilutive|grant|credit facility/.test(text)) {
    category = 'financing_runway'
    direction = /offering|warrant|convertible|registered direct/.test(text) ? 'mixed' : 'positive'
    baseScore = direction === 'mixed' ? 26 : 34
    if (direction === 'mixed') cautions.push('financing may dilute holders')
  } else if (/earnings|revenue|eps|guidance|raises? outlook|raises? forecast|beats?|record sales|profitability|ebitda|annualized/.test(text)) {
    category = 'earnings_guidance'
    direction = /miss|cut|below|loss widens/.test(text) ? 'negative' : 'positive'
    baseScore = direction === 'positive' ? 30 : 4
    if (direction !== 'positive') rejectionReason = 'bearish_catalyst'
  } else if (/analyst|upgrade|price target|initiates?|buy rating/.test(text)) {
    category = 'analyst_action'
    direction = /downgrade|cut|lower/.test(text) ? 'negative' : 'positive'
    baseScore = direction === 'positive' ? 18 : 3
    if (direction !== 'positive') rejectionReason = 'bearish_catalyst'
  } else if (/patent|product launch|launches?|regulatory clearance/.test(text)) {
    category = 'product_or_ip'
    direction = 'positive'
    baseScore = 22
  }

  let materialityScore = 10
  if (amountToMarketCapPct != null && ['merger_acquisition', 'biotech_regulatory_or_trial', 'contract_award', 'partnership_commercial', 'financing_runway', 'earnings_guidance'].includes(category)) {
    materialityScore = amountToMarketCapPct >= 50 ? 30
      : amountToMarketCapPct >= 20 ? 25
        : amountToMarketCapPct >= 5 ? 18
          : amountToMarketCapPct >= 2 ? 10
            : 3
    if (['contract_award', 'financing_runway', 'earnings_guidance'].includes(category) && amountToMarketCapPct < 2 && marketCap >= 1_000_000_000) {
      rejectionReason = rejectionReason || 'immaterial_for_company_size'
    }
  } else if (amountToMarketCapPct != null && category === 'ordinary_news') {
    materialityScore = 4
    rejectionReason = rejectionReason || 'indirect_ticker_match'
  } else if (marketCap != null && marketCap >= 10_000_000_000 && ['contract_award', 'analyst_action', 'routine_news', 'ordinary_news'].includes(category)) {
    materialityScore = 4
    if (category === 'contract_award') rejectionReason = rejectionReason || 'immaterial_for_company_size'
  } else if (marketCap != null && marketCap <= 500_000_000 && ['merger_acquisition', 'biotech_regulatory_or_trial', 'financing_runway', 'contract_award', 'partnership_commercial'].includes(category)) {
    materialityScore = 18
  }

  const directnessScore = tickerSpecific ? 18 : 2
  const sourceScore = recognizedSource ? (/pr newswire|globenewswire|business wire|accesswire|company|investor relations|sec|edgar/.test(source) ? 16 : 10) : 3
  const simplicityScore = /merger|acquisition|fda|approval|contract|award|financing|private placement|revenue|guidance|clinical/.test(text) ? 10 : 4
  const routinePenalty = weakGeneric || category === 'routine_news' ? 24 : 0
  const indirectPenalty = tickerSpecific ? 0 : 22
  const bearishPenalty = bearishRisk || direction === 'negative' ? 55 : 0
  if (!tickerSpecific) rejectionReason = rejectionReason || 'indirect_ticker_match'
  if (!recognizedSource) rejectionReason = rejectionReason || 'unrecognized_source'
  if (weakGeneric && !['merger_acquisition', 'biotech_regulatory_or_trial', 'contract_award', 'partnership_commercial', 'financing_runway', 'earnings_guidance'].includes(category)) {
    rejectionReason = rejectionReason || 'routine_news'
  }

  const explosionPotentialScore = Number(Math.max(0, Math.min(100,
    baseScore + materialityScore + directnessScore + sourceScore + simplicityScore - routinePenalty - indirectPenalty - bearishPenalty
  )).toFixed(1))
  return {
    category,
    direction,
    base_score: baseScore,
    materiality_score: materialityScore,
    directness_score: directnessScore,
    source_score: sourceScore,
    simplicity_score: simplicityScore,
    routine_penalty: routinePenalty,
    indirect_penalty: indirectPenalty,
    bearish_penalty: bearishPenalty,
    explosion_potential_score: explosionPotentialScore,
    hard_rejection_reason: rejectionReason,
    cautions,
    ticker_specific: tickerSpecific,
    recognized_source: recognizedSource,
    weak_generic: weakGeneric,
    bearish: bearishRisk || direction === 'negative',
    amount_usd: amount != null ? Number(amount.toFixed(0)) : null,
    amount_to_market_cap_pct: amountToMarketCapPct,
    title,
  }
}

function catalystQualityAssessment(row = {}, validation = {}, context = {}) {
  const title = String(context.catalystText || row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || '').trim()
  const typeText = String(row.main_catalyst?.event_type || row.main_catalyst?.type || row.structured_catalyst_type || row.event_type || '').trim()
  const sourceText = [
    row.main_catalyst?.source,
    row.main_catalyst?.publisher,
    row.catalyst_source,
    Array.isArray(row.sources) ? row.sources.join(' ') : row.sources,
    row.news_source,
    row.source,
  ].filter(Boolean).join(' ')
  const text = [title, typeText].join(' ').toLowerCase()
  const source = sourceText.toLowerCase()
  const catalystPower = nullableNumber(context.catalystPower ?? row.catalyst_power_score) || 0
  const sentiment = nullableNumber(context.sentiment ?? row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
  const articleCount = nullableNumber(context.news ?? row.catalyst_window_article_count ?? row.news_article_count ?? row.article_count) || 0
  const tickerSpecific = validation.tickerSpecificCatalyst ?? catalystMentionsTickerOrCompany(row, title)
  const recognizedSource = validation.recognizedSource ?? isRecognizedCatalystSource(sourceText)
  const weak = validation.weakCatalyst ?? isWeakGenericCatalystText(title)
  const bearish = validation.bearishCatalyst ?? isBearishCatalystText([title, typeText].join(' '))
  const taxonomy = catalystTaxonomyAssessment(row, {
    catalystText: title,
    tickerMatched: Boolean(row.main_catalyst?.ticker_matched || row.main_catalyst?.ticker_match || row.main_catalyst?.matched_ticker),
  })
  const isFiling = Boolean(row.main_catalyst?.isSecFiling || row.sec_filing_contributed || /sec|edgar|8-k|form\s+(8-k|10-q|10-k|6-k)/i.test(sourceText + ' ' + title))

  const className = taxonomy.category === 'ordinary_news' && isFiling ? 'sec_filing' : taxonomy.category
  const classScore = taxonomy.category === 'ordinary_news' && isFiling ? 14 : taxonomy.base_score

  const sourceScore = /pr newswire|globenewswire|business wire|accesswire|access newswire|company|investor relations/.test(source)
    ? 18
    : /sec|edgar|8-k|reuters|dow jones|associated press/.test(source)
      ? 16
      : /benzinga|marketwatch|yahoo finance|stocktitan/.test(source)
        ? 13
        : /tradingview|finviz|seekingalpha/.test(source)
          ? 8
          : 4
  const specificityScore = tickerSpecific ? 18 : 0
  const freshnessScore = validation.freshSessionCatalyst ? 14 : validation.catalystAgeMinutes != null && validation.catalystAgeMinutes <= PREDICTION_SESSION_CATALYST_MAX_AGE_MINUTES ? 8 : 0
  const powerScore = Math.min(10, Math.max(0, catalystPower) * 1.2)
  const sentimentScore = Math.max(-8, Math.min(8, sentiment * 12))
  const articleScore = Math.min(6, Math.log1p(Math.max(0, articleCount)) * 2.5)
  const weakPenalty = weak ? 28 : 0
  const bearishPenalty = bearish ? 60 : 0
  const genericFilingPenalty = isFiling && !/8-k|material agreement|merger|acquisition|contract|approval|clinical|guidance|earnings|financing|strategic/i.test(title + ' ' + typeText) ? 10 : 0
  const materialityBonus = Math.min(14, Math.max(0, Number(taxonomy.materiality_score || 0) * 0.45))
  const taxonomyBonus = Math.min(12, Math.max(0, Number(taxonomy.explosion_potential_score || 0) * 0.12))
  const hardRejectPenalty = taxonomy.hard_rejection_reason ? 22 : 0
  const score = Number(Math.max(0, Math.min(100,
    classScore + sourceScore + specificityScore + freshnessScore + powerScore + sentimentScore + articleScore + materialityBonus + taxonomyBonus - weakPenalty - bearishPenalty - genericFilingPenalty - hardRejectPenalty
  )).toFixed(1))
  const tier = bearish || weak || !tickerSpecific || !recognizedSource || ['routine_news', 'stale_recap', 'indirect_ticker_match', 'immaterial_for_company_size', 'dilution_risk', 'reverse_split_noise', 'bearish_catalyst'].includes(String(taxonomy.hard_rejection_reason || ''))
    ? 'reject'
    : score >= PREDICTION_PENDING_OPEN_STRONG_QUALITY
      ? 'strong'
      : score >= PREDICTION_PENDING_OPEN_MIN_QUALITY
        ? 'moderate'
        : 'weak'
  const reasons = [
    `class ${className}`,
    `quality ${score}/100`,
    tickerSpecific ? 'ticker-specific' : 'not ticker-specific',
    recognizedSource ? 'recognized source' : 'unrecognized source',
    validation.freshSessionCatalyst ? 'fresh window' : 'not fresh window',
    weak ? 'weak/generic text' : '',
    bearish ? 'bearish/risk wording' : '',
    taxonomy.hard_rejection_reason ? `reject ${taxonomy.hard_rejection_reason}` : '',
    taxonomy.amount_to_market_cap_pct != null ? `amount/cap ${taxonomy.amount_to_market_cap_pct}%` : '',
  ].filter(Boolean)
  return {
    score,
    tier,
    class: className,
    taxonomy,
    hard_rejection_reason: taxonomy.hard_rejection_reason,
    company_relative_materiality_score: taxonomy.materiality_score,
    explosion_potential_score: taxonomy.explosion_potential_score,
    source_score: sourceScore,
    specificity_score: specificityScore,
    freshness_score: freshnessScore,
    recognized_source: Boolean(recognizedSource),
    ticker_specific: Boolean(tickerSpecific),
    weak_generic: Boolean(weak),
    bearish: Boolean(bearish),
    is_filing: isFiling,
    reasons,
    title,
    source: sourceText || null,
  }
}

function pendingOpenConfirmationState(row = {}, validation = {}, catalystQuality = {}, catalystReaction = {}, context = {}) {
  const reaction = catalystReactionSummary(catalystReaction)
  const isPendingOpen = Boolean(reaction.pending_market_reaction && validation.recognizedNewsCatalyst && validation.freshSessionCatalyst)
  const payoffProbability = nullableNumber(context.payoffModelProbability ?? row.payoff_model_probability)
  const payoffThreshold = nullableNumber(context.payoffThreshold ?? row.payoff_model_threshold)
  const relVolume = nullableNumber(context.relVolume ?? row.rel_volume) || 0
  const social = nullableNumber(context.social ?? row.message_count) || 0
  const densityScore = nullableNumber(row.message_density_score ?? row.social_message_density) || 0
  const sourceOk = catalystQuality.recognized_source !== false
  const strongQuality = catalystQuality.tier === 'strong' || Number(catalystQuality.score || 0) >= PREDICTION_PENDING_OPEN_STRONG_QUALITY
  const moderateQuality = catalystQuality.tier === 'moderate' || Number(catalystQuality.score || 0) >= PREDICTION_PENDING_OPEN_MIN_QUALITY
  const payoffMarginOk = payoffProbability != null && payoffThreshold != null && payoffProbability >= payoffThreshold + PREDICTION_PENDING_OPEN_PAYOFF_MARGIN
  const socialOk = social >= PREDICTION_PENDING_OPEN_MIN_SOCIAL || densityScore >= 15 || validation.recognizedSocialCatalyst
  const volumeOk = relVolume >= PREDICTION_PENDING_OPEN_MIN_REL_VOLUME
  const supportReasons = [
    strongQuality ? 'strong catalyst quality' : '',
    payoffMarginOk ? `payoff margin ${(payoffProbability - payoffThreshold).toFixed(3)}` : '',
    socialOk ? 'social/density support' : '',
    volumeOk ? 'relative volume support' : '',
    validation.recognizedSqueezeCatalyst || validation.verifiedShortInterest ? 'squeeze/short-interest support' : '',
  ].filter(Boolean)
  const passes = !isPendingOpen
    ? true
    : sourceOk && moderateQuality && supportReasons.length >= 1 && !catalystQuality.bearish && !catalystQuality.weak_generic
  const blockedReasons = [
    isPendingOpen && !sourceOk ? 'PENDING_OPEN_UNRECOGNIZED_SOURCE' : '',
    isPendingOpen && !moderateQuality ? 'PENDING_OPEN_WEAK_CATALYST_QUALITY' : '',
    isPendingOpen && moderateQuality && !supportReasons.length ? 'PENDING_OPEN_NEEDS_SECOND_CONFIRMATION' : '',
  ].filter(Boolean)
  return {
    is_pending_open: isPendingOpen,
    passes,
    support_reasons: supportReasons,
    blocked_reasons: blockedReasons,
    payoff_margin: payoffProbability != null && payoffThreshold != null ? Number((payoffProbability - payoffThreshold).toFixed(3)) : null,
    thresholds: {
      min_quality: PREDICTION_PENDING_OPEN_MIN_QUALITY,
      strong_quality: PREDICTION_PENDING_OPEN_STRONG_QUALITY,
      payoff_margin: PREDICTION_PENDING_OPEN_PAYOFF_MARGIN,
      min_rel_volume: PREDICTION_PENDING_OPEN_MIN_REL_VOLUME,
      min_social: PREDICTION_PENDING_OPEN_MIN_SOCIAL,
    },
  }
}

function companySignalTokens(row = {}) {
  const company = String(row.company || row.name || row.company_name || '').toLowerCase()
  return company
    .replace(/[^a-z0-9 ]+/g, ' ')
    .split(/\s+/)
    .filter(Boolean)
    .filter(token => token.length >= 4)
    .filter(token => ![
      'inc', 'corp', 'corporation', 'company', 'limited', 'ltd', 'holdings', 'holding',
      'group', 'plc', 'class', 'common', 'stock', 'american', 'technologies', 'technology',
      'therapeutics', 'pharmaceuticals', 'biopharma', 'systems', 'international',
    ].includes(token))
    .slice(0, 5)
}

function catalystMentionsTickerOrCompany(row = {}, value = '') {
  const text = String(value || '').toLowerCase()
  if (!text.trim()) return false
  const ticker = String(row.ticker || '').trim().toLowerCase()
  if (ticker && new RegExp(`(^|[^a-z0-9])${ticker}([^a-z0-9]|$)`, 'i').test(text)) return true
  return companySignalTokens(row).some(token => text.includes(token))
}

function likelyLargeCapWithoutMarketCap(row = {}, watcherCount = 0) {
  const tier = predictionMarketCapTier(row)
  return tier === 'Unknown' && Number(watcherCount || 0) >= 100_000
}

function predictionPeopleAttention(row = {}, context = {}) {
  const social = nullableNumber(context.social ?? row.message_count) || 0
  const stocktwits = nullableNumber(row.stocktwits_message_count) || 0
  const watchers = nullableNumber(context.watcherCount ?? row.stocktwits_watcher_count) || 0
  const density = nullableNumber(row.social_message_density ?? row.message_density_now ?? row.message_density_15m ?? row.message_density_60m) || 0
  const densityScore = nullableNumber(row.message_density_score ?? row.message_density_live_score) || 0
  const densityRising = Boolean(row.message_density_rising)
  const densitySupported = Boolean(row.message_density_supported)
  const sentiment = nullableNumber(context.sentiment ?? row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
  const messageCount = Math.max(social, stocktwits)
  const active = Boolean(
    messageCount >= PREDICTION_PEOPLE_MIN_MESSAGES ||
    density >= PREDICTION_PEOPLE_MIN_DENSITY_PER_MIN ||
    densityScore >= PREDICTION_PEOPLE_STRONG_DENSITY_SCORE ||
    (densityRising && messageCount >= 3) ||
    (densitySupported && messageCount >= 3)
  )
  const strong = Boolean(
    messageCount >= PREDICTION_PEOPLE_STRONG_MESSAGES ||
    densityScore >= PREDICTION_PEOPLE_STRONG_DENSITY_SCORE ||
    (density >= PREDICTION_PEOPLE_MIN_DENSITY_PER_MIN && messageCount >= PREDICTION_PEOPLE_MIN_MESSAGES)
  )
  const bullish = sentiment >= 0.08
  const reasons = [
    messageCount >= PREDICTION_PEOPLE_MIN_MESSAGES ? `${messageCount} current social messages` : '',
    density >= PREDICTION_PEOPLE_MIN_DENSITY_PER_MIN ? `${density.toFixed(3)}/m message density` : '',
    densityScore >= PREDICTION_PEOPLE_STRONG_DENSITY_SCORE ? `density score ${densityScore.toFixed(0)}` : '',
    densityRising ? 'message density rising' : '',
    watchers >= SQUEEZE_WATCHER_MIN ? `${watchers.toLocaleString()} watchers` : '',
    bullish ? `bullish sentiment ${sentiment.toFixed(2)}` : '',
  ].filter(Boolean)
  return {
    active,
    strong,
    bullish,
    social,
    stocktwits,
    messageCount,
    watchers,
    density,
    densityScore,
    densityRising,
    densitySupported,
    sentiment,
    reasons,
  }
}

function predictionUpsideProfile(row = {}, watcherCount = 0) {
  const tier = predictionMarketCapTier(row)
  const largeUnknown = likelyLargeCapWithoutMarketCap(row, watcherCount)
  if (tier === 'Mega' || largeUnknown) {
    return {
      tier: largeUnknown ? 'UnknownLargeProfile' : tier,
      highUpsideMaxReturnPct: 2.8,
      highConvictionMinReturnPct: 5,
      requiresExceptionalMomentum: true,
      reason: 'mega/likely-large caps cannot use small-cap squeeze return assumptions',
    }
  }
  if (tier === 'Large') {
    return {
      tier,
      highUpsideMaxReturnPct: 3.8,
      highConvictionMinReturnPct: 5,
      requiresExceptionalMomentum: true,
      reason: 'large caps need exceptional catalyst plus volume to qualify as highest-climber candidates',
    }
  }
  if (tier === 'Mid') return { tier, highUpsideMaxReturnPct: 6.5, highConvictionMinReturnPct: 4.5, requiresExceptionalMomentum: false, reason: 'mid-cap upside cap' }
  if (tier === 'Small') return { tier, highUpsideMaxReturnPct: 10, highConvictionMinReturnPct: 4.5, requiresExceptionalMomentum: false, reason: 'small-cap upside cap' }
  if (tier === 'Nano') return { tier, highUpsideMaxReturnPct: 14, highConvictionMinReturnPct: 4.5, requiresExceptionalMomentum: false, reason: 'micro/nano high-upside cap' }
  return { tier, highUpsideMaxReturnPct: 10, highConvictionMinReturnPct: 4.5, requiresExceptionalMomentum: false, reason: 'unknown-cap conservative high-upside cap' }
}

function predictionClimberGate(row = {}, validation = {}, context = {}) {
  const change = Number(context.change || 0)
  const relVolume = Number(context.relVolume || 0)
  const social = Number(context.social || 0)
  const sentiment = Number(context.sentiment || 0)
  const squeezeScore = Number(context.squeezeScore || 0)
  const watcherCount = Number(context.watcherCount || 0)
  const setupStatus = String(context.setupStatus || '')
  const profile = predictionUpsideProfile(row, watcherCount)
  const hasFreshSetup = ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(setupStatus)
  const hasNews = Boolean(validation.recognizedNewsCatalyst)
  const hasSqueeze = Boolean(validation.recognizedSqueezeCatalyst)
  const hasPeople = Boolean(validation.recognizedPeopleAttention || validation.recognizedSocialCatalyst)
  const hasSocialSentiment = social >= 20 && sentiment >= 0.15
  const hasMomentum = relVolume >= 1.2 || change >= 2 || hasFreshSetup
  const isSmallHighUpside = ['Nano', 'Small', 'Unknown'].includes(profile.tier) && !profile.requiresExceptionalMomentum
  const passes = profile.requiresExceptionalMomentum
    ? (hasNews || hasPeople) && hasMomentum && (hasSocialSentiment || hasFreshSetup || relVolume >= 1.5)
    : (hasNews || hasSqueeze || hasPeople || hasFreshSetup) && (isSmallHighUpside ? relVolume >= 1.2 || social >= 20 || change >= 4 : hasMomentum)
  const riskFlags = [
    profile.requiresExceptionalMomentum && !passes ? 'LARGE_CAP_NOT_HIGH_UPSIDE_CLIMBER' : '',
    !hasMomentum ? 'LOW_MOMENTUM_FOR_NEXT_SESSION_CLIMBER' : '',
  ].filter(Boolean)
  return { ...profile, passes, hasFreshSetup, hasMomentum, hasSocialSentiment, riskFlags }
}

function predictionEvidenceValidation(row = {}, context = {}) {
  const catalystText = String(context.catalystText || '').trim()
  const sourceText = [
    row.main_catalyst?.source,
    row.main_catalyst?.publisher,
    row.catalyst_source,
    Array.isArray(row.sources) ? row.sources.join(' ') : row.sources,
    row.news_source,
    row.source,
  ].filter(Boolean).join(' ')
  const news = Number(context.news || 0)
  const social = Number(context.social || 0)
  const sentiment = Number(context.sentiment || 0)
  const change = Number(context.change ?? row.change_pct ?? 0)
  const relVolume = Number(context.relVolume ?? row.rel_volume ?? 0)
  const catalystPower = Number(context.catalystPower || 0)
  const squeezeScore = Number(context.squeezeScore || 0)
  const watcherCount = Number(context.watcherCount || 0)
  const floatShort = nullableNumber(context.floatShort)
  const shortInterestPct = nullableNumber(row.short_interest_pct ?? row.short_interest_pct_shares_out ?? row.short_interest_pct_float)
  const thresholdMessages = nullableNumber(row.threshold_trailing_60m_messages)
  const setupStatus = String(context.setupStatus || '')
  const tier = predictionMarketCapTier(row)
  const largeUnknownWatcherProfile = likelyLargeCapWithoutMarketCap(row, watcherCount)
  const peopleAttention = predictionPeopleAttention(row, { social, sentiment, watcherCount })
  const verifiedShortInterest = (shortInterestPct != null && shortInterestPct >= 10) || (floatShort != null && floatShort >= 10)
  const weakCatalyst = isWeakGenericCatalystText(catalystText)
  const bearishCatalyst = isBearishCatalystText([
    catalystText,
    row.main_catalyst?.event_type,
    row.main_catalyst?.sentiment,
  ].filter(Boolean).join(' '))
  const tickerMatchedByArticle = Boolean(row.main_catalyst?.ticker_matched || row.main_catalyst?.ticker_match || row.main_catalyst?.matched_ticker)
  const taxonomy = catalystTaxonomyAssessment(row, { catalystText, tickerMatched: tickerMatchedByArticle })
  const tickerSpecificCatalyst = tickerMatchedByArticle || catalystMentionsTickerOrCompany(row, catalystText)
  const tickerSpecificRequired = tier === 'Mega' || tier === 'Large' || largeUnknownWatcherProfile
  const recognizedSource = isRecognizedCatalystSource(sourceText)
  const hasThresholdSupport = ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(setupStatus)
  const nowSec = Math.floor(Date.now() / 1000)
  const eventSec = nullableNumber(row.main_catalyst?.event_sec ?? row.latest_publish_sec)
  const catalystAgeMinutes = nullableNumber(row.main_catalyst?.age_minutes) ?? (
    eventSec != null ? Number(Math.max(0, (nowSec - eventSec) / 60).toFixed(1)) : null
  )
  const sessionContext = row.catalyst_session_context || {}
  const sessionStart = nullableNumber(sessionContext.catalyst_window_start_sec)
  const sessionEnd = nullableNumber(sessionContext.catalyst_window_end_sec)
  const catalystInSessionWindow = Boolean(
    row.main_catalyst?.in_session_window ||
    (eventSec != null && sessionStart != null && sessionEnd != null && eventSec >= sessionStart && eventSec <= sessionEnd)
  )
  const catalystMarketSession = marketSessionForSec(eventSec)
  const catalystOutsideRegularHours = ['postmarket', 'overnight', 'premarket', 'weekend'].includes(catalystMarketSession)
  const freshSessionCatalyst = Boolean(
    catalystInSessionWindow ||
    (catalystAgeMinutes != null && catalystAgeMinutes <= PREDICTION_FRESH_CATALYST_MAX_AGE_MINUTES) ||
    (Number(row.catalyst_window_count || row.catalyst_window_article_count || 0) > 0 && catalystAgeMinutes != null && catalystAgeMinutes <= PREDICTION_SESSION_CATALYST_MAX_AGE_MINUTES)
  )

  const taxonomyHardReject = ['routine_news', 'stale_recap', 'indirect_ticker_match', 'immaterial_for_company_size', 'low_liquidity', 'abnormal_spread', 'dilution_risk', 'reverse_split_noise', 'bearish_catalyst', 'unrecognized_source'].includes(String(taxonomy.hard_rejection_reason || ''))
  const materialDirectCatalyst = !taxonomyHardReject &&
    taxonomy.ticker_specific &&
    taxonomy.recognized_source &&
    ['positive', 'mixed'].includes(taxonomy.direction) &&
    taxonomy.explosion_potential_score >= 55 &&
    ['merger_acquisition', 'biotech_regulatory_or_trial', 'contract_award', 'partnership_commercial', 'financing_runway', 'earnings_guidance', 'product_or_ip'].includes(taxonomy.category)
  const socialConfirmation = peopleAttention.active || social >= PREDICTION_PEOPLE_MIN_MESSAGES
  const earlyMarketConfirmation = change >= 1 || relVolume >= PREDICTION_PENDING_OPEN_MIN_REL_VOLUME || hasThresholdSupport
  const strongMateriality = Number(taxonomy.amount_to_market_cap_pct || 0) >= 5 || Number(taxonomy.explosion_potential_score || 0) >= 78
  const largerCapNewsNeedsConfirmation = ['Mega', 'Large', 'Mid'].includes(tier) || largeUnknownWatcherProfile
  const largeCapConfirmationOk = !largerCapNewsNeedsConfirmation ||
    socialConfirmation ||
    relVolume >= 3 ||
    change >= 3 ||
    Number(taxonomy.amount_to_market_cap_pct || 0) >= 5
  const newsConfirmationOk = materialDirectCatalyst && (socialConfirmation || earlyMarketConfirmation || strongMateriality) && largeCapConfirmationOk

  const recognizedNewsCatalyst = news > 0 && !weakCatalyst && !bearishCatalyst && !taxonomyHardReject && recognizedSource && tickerSpecificCatalyst && freshSessionCatalyst && newsConfirmationOk && (
    catalystPower >= 1 ||
    Number(row.catalyst_window_article_count || 0) > 0 ||
    Math.abs(sentiment) >= 0.08 ||
    materialDirectCatalyst
  )
  const recognizedSqueezeCatalyst = !bearishCatalyst && squeezeScore >= 70 && verifiedShortInterest && social >= 3
  const recognizedDensitySetup = !bearishCatalyst && hasThresholdSupport && (
    social >= predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule.minTrailing60Messages ||
    (thresholdMessages != null && thresholdMessages >= predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule.minTrailing60Messages)
  ) && (recognizedNewsCatalyst || recognizedSqueezeCatalyst || sentiment >= 0.12)
  const recognizedPeopleAttention = !bearishCatalyst && peopleAttention.strong && (
    relVolume >= 1.2 ||
    change >= 2 ||
    hasThresholdSupport
  )
  const recognizedSocialCatalyst = !bearishCatalyst && peopleAttention.active && sentiment >= 0.08 && (
    recognizedNewsCatalyst ||
    recognizedSqueezeCatalyst ||
    recognizedPeopleAttention
  )
  const labels = [
    recognizedNewsCatalyst ? 'recognized news catalyst' : '',
    recognizedSqueezeCatalyst ? 'verified squeeze/social-interest catalyst' : '',
    recognizedDensitySetup ? 'message-density setup confirmation' : '',
    recognizedPeopleAttention ? 'live people/message attention' : '',
    recognizedSocialCatalyst ? 'bullish social sentiment confirmation' : '',
  ].filter(Boolean)
  const riskFlags = [
    weakCatalyst && news > 0 ? 'WEAK_OR_GENERIC_CATALYST_TEXT' : '',
    bearishCatalyst && news > 0 ? 'BEARISH_OR_RISK_CATALYST_NOT_VALID_FOR_UP_PREDICTION' : '',
    news > 0 && !tickerSpecificCatalyst ? 'CATALYST_TITLE_NOT_TICKER_SPECIFIC' : '',
    news > 0 && tickerSpecificCatalyst && recognizedSource && !freshSessionCatalyst ? 'STALE_OR_OUT_OF_WINDOW_CATALYST' : '',
    taxonomy.hard_rejection_reason ? `CATALYST_REJECT_${String(taxonomy.hard_rejection_reason).toUpperCase()}` : '',
    news > 0 && materialDirectCatalyst && !newsConfirmationOk ? 'INSUFFICIENT_EARLY_CONFIRMATION_FOR_CATALYST' : '',
    news > 0 && materialDirectCatalyst && !largeCapConfirmationOk ? 'LARGE_CAP_CATALYST_NEEDS_STRONGER_RETAIL_OR_VOLUME_CONFIRMATION' : '',
    largeUnknownWatcherProfile && watcherCount >= SQUEEZE_WATCHER_MIN && !verifiedShortInterest ? 'LARGE_CAP_WATCHER_ONLY_SQUEEZE_REJECTED' : '',
    news > 0 && !recognizedSource ? 'UNRECOGNIZED_CATALYST_SOURCE' : '',
    !recognizedNewsCatalyst && !recognizedSqueezeCatalyst && !recognizedPeopleAttention ? 'NO_VALIDATED_PRIMARY_CATALYST' : '',
  ].filter(Boolean)

  return {
    valid: Boolean(recognizedNewsCatalyst || recognizedSqueezeCatalyst || recognizedDensitySetup || recognizedPeopleAttention || recognizedSocialCatalyst),
    primary: recognizedSqueezeCatalyst ? 'squeeze' : recognizedNewsCatalyst ? 'news' : recognizedPeopleAttention ? 'people' : recognizedDensitySetup ? 'density' : recognizedSocialCatalyst ? 'social' : 'none',
    labels,
    reason: labels.join(' + '),
    weakCatalyst,
    bearishCatalyst,
    taxonomy,
    materialDirectCatalyst,
    taxonomyHardReject,
    newsConfirmationOk,
    socialConfirmation,
    earlyMarketConfirmation,
    strongMateriality,
    largeCapConfirmationOk,
    hardRejectionReason: taxonomy.hard_rejection_reason,
    recognizedSource,
    tickerSpecificCatalyst,
    tickerSpecificRequired,
    largeUnknownWatcherProfile,
    tier,
    verifiedShortInterest,
    catalystAgeMinutes,
    catalystMarketSession,
    catalystOutsideRegularHours,
    catalystInSessionWindow,
    freshSessionCatalyst,
    recognizedNewsCatalyst,
    recognizedSqueezeCatalyst,
    recognizedDensitySetup,
    recognizedPeopleAttention,
    recognizedSocialCatalyst,
    peopleAttention,
    riskFlags,
  }
}

// Social attention and watchers confirm a direct catalyst; they do not replace one.
// This keeps the strict prediction tabs from turning an already-moving social name
// into a catalyst prediction when no company-specific event was identified.
function hasPrimaryPredictionCatalyst(validation = {}, row = {}) {
  const verifiedSqueeze = Boolean(validation.recognizedSqueezeCatalyst && validation.verifiedShortInterest)
  const directNews = Boolean(validation.recognizedNewsCatalyst && validation.materialDirectCatalyst)
  return directNews || verifiedSqueeze
}

function hasDevelopingCatalystEvidence(row = {}) {
  const validation = row.prediction_validation || {}
  const directNews = Boolean(validation.recognizedNewsCatalyst && validation.materialDirectCatalyst)
  const verifiedSqueeze = Boolean(validation.recognizedSqueezeCatalyst && validation.verifiedShortInterest)
  return Boolean(
    directNews ||
    verifiedSqueeze,
  )
}

function catalystRecencyWeight(eventSec, nowSec) {
  const ageHours = Math.max(0, (Number(nowSec || 0) - Number(eventSec || 0)) / 3600)
  if (ageHours <= 2) return 1.35
  if (ageHours <= 6) return 1.2
  if (ageHours <= 18) return 1.05
  if (ageHours <= 72) return 0.9
  return 0.65
}

function articleTimeStages() {
  return [
    {
      $addFields: {
        _time_raw: {
          $ifNull: [
            '$publish_date',
            { $ifNull: ['$published_at', { $ifNull: ['$detected_at', { $ifNull: ['$fetched_date', { $ifNull: ['$createdAt', '$created_at'] }] }] }] },
          ],
        },
      },
    },
    {
      $addFields: {
        _event_sec: {
          $switch: {
            branches: [
              { case: { $eq: [{ $type: '$_time_raw' }, 'date'] }, then: { $floor: { $divide: [{ $toLong: '$_time_raw' }, 1000] } } },
              { case: { $in: [{ $type: '$_time_raw' }, ['int', 'long', 'double', 'decimal']] }, then: { $toLong: '$_time_raw' } },
              {
                case: { $eq: [{ $type: '$_time_raw' }, 'string'] },
                then: { $floor: { $divide: [{ $toLong: { $dateFromString: { dateString: '$_time_raw', onError: new Date(0) } } }, 1000] } },
              },
            ],
            default: 0,
          },
        },
      },
    },
  ]
}

async function loadArticleStatsForTickers(db, tickers, days = 3, sessionContext = marketSessionContext()) {
  const wanted = Array.from(new Set(tickers.map(t => String(t || '').toUpperCase()).filter(Boolean)))
  if (!wanted.length) return new Map()
  const nowSec = Math.floor(Date.now() / 1000)
  const fallbackSinceSec = nowSec - Math.max(1, Number(days || 3)) * 86_400
  const windowStartSec = Math.max(1, Math.min(Number(sessionContext?.catalyst_window_start_sec || fallbackSinceSec), nowSec))
  const windowEndSec = Math.max(windowStartSec, Number(sessionContext?.catalyst_window_end_sec || nowSec))

  const rows = await db.collection('articles').aggregate([
    ...articleTimeStages(),
    { $match: {
      $and: [
        { _event_sec: { $gte: Math.min(fallbackSinceSec, windowStartSec), $lte: windowEndSec + 300 } },
        { $or: [
          { ticker: { $exists: true, $nin: ['', null] } },
          { tickers: { $type: 'array', $ne: [] } },
        ] },
      ],
    } },
    {
      $addFields: {
        _ticker_parts: {
          $map: {
            input: {
              $setUnion: [
                { $split: [{ $toUpper: { $toString: { $ifNull: ['$ticker', ''] } } }, ','] },
                {
                  $map: {
                    input: { $cond: [{ $isArray: '$tickers' }, '$tickers', []] },
                    as: 'ticker_value',
                    in: { $toUpper: { $toString: '$$ticker_value' } },
                  },
                },
              ],
            },
            as: 'ticker_part',
            in: { $trim: { input: '$$ticker_part' } },
          },
        },
      },
    },
    {
      $addFields: {
        _ticker_count: { $size: { $ifNull: ['$_ticker_parts', []] } },
      },
    },
    { $unwind: '$_ticker_parts' },
    { $match: { _ticker_parts: { $in: wanted } } },
    {
      $addFields: {
        _article_kind: {
          $cond: [
            { $eq: [{ $toLower: { $toString: { $ifNull: ['$article_kind', ''] } } }, 'structured'] },
            'structured',
            {
              $cond: [
                {
                  $or: [
                    { $in: [{ $toLower: { $toString: { $ifNull: ['$article_kind', ''] } } }, ['public', 'unstructured']] },
                    { $in: ['$category', ['unstructured_public_title', 'public_news', 'public_market_news']] },
                    { $eq: ['$collector', 'unstructured_news_title_only_v1'] },
                  ],
                },
                'public',
                'structured',
              ],
            },
          ],
        },
      },
    },
    { $match: { _article_kind: 'structured' } },
    {
      $addFields: {
        _sentiment_direction: {
          $switch: {
            branches: [
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$sentiment', ''] } } }, regex: 'bull|positive' } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$sentiment', ''] } } }, regex: 'bear|negative' } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $addFields: {
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: '$sentiment_score' }, ['int', 'long', 'double', 'decimal']] }, then: { $toDouble: '$sentiment_score' } },
              { case: { $in: [{ $type: '$ml_confidence' }, ['int', 'long', 'double', 'decimal']] }, then: { $multiply: ['$_sentiment_direction', { $toDouble: '$ml_confidence' }] } },
            ],
            default: '$_sentiment_direction',
          },
        },
      },
    },
    {
      $addFields: {
        _source_weight: {
          $switch: {
            branches: [
              {
                case: {
                  $and: [
                    {
                      $or: [
                        { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$source', ''] } } }, regex: 'sec|edgar' } },
                        { $eq: [{ $toLower: { $toString: { $ifNull: ['$event_type', ''] } } }, 'sec_filing'] },
                      ],
                    },
                    { $lte: [{ $abs: '$_score' }, 0.08] },
                  ],
                },
                then: 0.15,
              },
              {
                case: {
                  $in: [
                    { $toLower: { $toString: { $ifNull: ['$event_type', ''] } } },
                    ['earnings_beat', 'earnings_miss', 'guidance_raise', 'guidance_cut', 'fda_approval', 'fda_rejection', 'clinical_positive', 'clinical_negative', 'public_offering', 'bankruptcy_default'],
                  ],
                },
                then: 1.35,
              },
            ],
            default: 1,
          },
        },
        _headline: {
          $trim: {
            input: {
              $toString: {
                $ifNull: ['$title', { $ifNull: ['$headline', { $ifNull: ['$summary', '$description'] }] }],
              },
            },
          },
        },
        _event_type_norm: { $toLower: { $toString: { $ifNull: ['$event_type', { $ifNull: ['$category', '$article_type'] }] } } },
      },
    },
    {
      $addFields: {
        _roundup_article: {
          $regexMatch: {
            input: { $toLower: '$_headline' },
            regex: 'stocks moving|here are [0-9]+ stocks|premarket movers|market movers|moving premarket|why .*shares are trading higher by over',
          },
        },
      },
    },
    {
      $project: {
        ticker: '$_ticker_parts',
        source: '$source',
        title: '$_headline',
        url: '$url',
        event_type: '$_event_type_norm',
        event_sec: '$_event_sec',
        sentiment: '$sentiment',
        score: '$_score',
        source_weight: '$_source_weight',
        ticker_count: '$_ticker_count',
        roundup_article: '$_roundup_article',
        in_session_window: { $and: [{ $gte: ['$_event_sec', windowStartSec] }, { $lte: ['$_event_sec', windowEndSec] }] },
      },
    },
    {
      $group: {
        _id: '$ticker',
        count: { $sum: 1 },
        session_window_count: { $sum: { $cond: ['$in_session_window', 1, 0] } },
        bullish: { $sum: { $cond: [{ $gt: ['$score', 0.08] }, 1, 0] } },
        bearish: { $sum: { $cond: [{ $lt: ['$score', -0.08] }, 1, 0] } },
        neutral: { $sum: { $cond: [{ $lte: [{ $abs: '$score' }, 0.08] }, 1, 0] } },
        score_sum: { $sum: '$score' },
        weighted_score_sum: { $sum: { $multiply: ['$score', '$source_weight'] } },
        weight_sum: { $sum: '$source_weight' },
        sources: { $addToSet: '$source' },
        latest_publish_sec: { $max: '$event_sec' },
        catalysts: {
          $push: {
            title: '$title',
            source: '$source',
            url: '$url',
            event_type: '$event_type',
            event_sec: '$event_sec',
            sentiment: '$sentiment',
            score: '$score',
            source_weight: '$source_weight',
            ticker_count: '$ticker_count',
            roundup_article: '$roundup_article',
            in_session_window: '$in_session_window',
          },
        },
      },
    },
  ]).toArray()

  for (const row of rows) {
    const catalysts = (row.catalysts || [])
      .map(item => {
        const eventTypeWeight = catalystTypeWeight(`${item.event_type || ''} ${item.title || ''}`)
        const sourceWeight = catalystSourceWeight(item.source) * Number(item.source_weight || 1)
        const recencyWeight = catalystRecencyWeight(item.event_sec, nowSec)
        const sessionWeight = item.in_session_window ? 1.35 : 0.8
        const sentimentAbs = Math.max(0.1, Math.min(1, Math.abs(Number(item.score || 0))))
        const roundupPenalty = item.roundup_article && Number(item.ticker_count || 0) >= 4 ? 0.12 : 1
        const taxonomy = catalystTaxonomyAssessment({
          ticker: row._id,
          main_catalyst: item,
        }, { catalystText: item.title, tickerMatched: true })
        const taxonomyBoost = taxonomy.explosion_potential_score >= 70 ? 1.3 : taxonomy.explosion_potential_score >= 55 ? 1.12 : 1
        const taxonomyPenalty = taxonomy.hard_rejection_reason ? 0.25 : 1
        const power = eventTypeWeight * sourceWeight * recencyWeight * sessionWeight * (0.75 + sentimentAbs * 0.5) * roundupPenalty * taxonomyBoost * taxonomyPenalty
        return {
          ...item,
          ticker_matched: true,
          age_minutes: Number(((nowSec - Number(item.event_sec || nowSec)) / 60).toFixed(1)),
          catalyst_taxonomy: taxonomy.category,
          catalyst_direction: taxonomy.direction,
          catalyst_hard_rejection_reason: taxonomy.hard_rejection_reason,
          catalyst_explosion_potential_score: taxonomy.explosion_potential_score,
          catalyst_amount_to_market_cap_pct: taxonomy.amount_to_market_cap_pct,
          catalyst_type_weight: Number(eventTypeWeight.toFixed(3)),
          catalyst_source_weight: Number(sourceWeight.toFixed(3)),
          catalyst_recency_weight: Number(recencyWeight.toFixed(3)),
          catalyst_session_weight: Number(sessionWeight.toFixed(3)),
          catalyst_roundup_penalty: Number(roundupPenalty.toFixed(3)),
          catalyst_power: Number(power.toFixed(3)),
        }
      })
      .filter(item => item.title || item.event_type)
      .sort((a, b) => Number(b.catalyst_power || 0) - Number(a.catalyst_power || 0))
    row.catalysts = catalysts.slice(0, 12)
    row.main_catalyst = row.catalysts.find(item => !isBroadRoundupCatalyst(item)) || null
    row.latest_publish = row.latest_publish_sec ? new Date(Number(row.latest_publish_sec) * 1000) : null
    row.catalyst_power_score = Number(catalysts.reduce((sum, item, index) => sum + Number(item.catalyst_power || 0) * (index < 3 ? 1 : 0.35), 0).toFixed(3))
    row.catalyst_window_count = Number(row.session_window_count || 0)
    row.catalyst_session_context = sessionContext
  }

  return new Map(rows.map(row => [String(row._id || '').toUpperCase(), row]))
}

function catalystEventSec(row = {}) {
  return nullableNumber(row.main_catalyst?.event_sec ?? row.latest_publish_sec)
}

function normalizeOhlcBar(doc = {}) {
  const minute = nullableNumber(doc.minute)
  const open = nullableNumber(doc.open)
  const high = nullableNumber(doc.high)
  const low = nullableNumber(doc.low)
  const close = nullableNumber(doc.close ?? doc.price)
  const volume = nullableNumber(doc.volume)
  if (!minute || ![open, high, low, close].every(value => value != null && value > 0)) return null
  if (high < Math.max(open, close, low) || low > Math.min(open, close, high)) return null
  return {
    minute: Math.floor(minute / 60) * 60,
    open,
    high,
    low,
    close,
    volume: volume != null && volume >= 0 ? volume : null,
    providerIntervalSec: nullableNumber(doc.providerIntervalSec) || null,
    providerInterval: doc.providerInterval || null,
  }
}

function calculateCatalystPriceReaction(row = {}, bars = []) {
  const eventSec = catalystEventSec(row)
  const ticker = String(row.ticker || '').toUpperCase()
  const byMinute = new Map()
  for (const bar of bars.map(normalizeOhlcBar).filter(Boolean)) {
    const existing = byMinute.get(bar.minute)
    if (!existing || Number(existing.providerIntervalSec || Infinity) > Number(bar.providerIntervalSec || Infinity)) {
      byMinute.set(bar.minute, bar)
    }
  }
  const sorted = [...byMinute.values()].sort((a, b) => a.minute - b.minute)
  if (!ticker || !eventSec || !sorted.length) {
    return { available: false, ticker, event_sec: eventSec || null, source: 'mongo_ohlcv_bars', reason: 'missing_catalyst_or_bars' }
  }
  let preBar = null
  for (const bar of sorted) {
    if (bar.minute <= eventSec) preBar = bar
    else break
  }
  const postBars = sorted.filter(bar => bar.minute >= eventSec)
  if (!postBars.length) {
    return { available: false, ticker, event_sec: eventSec, source: 'mongo_ohlcv_bars', reason: 'no_bars_after_catalyst' }
  }
  const firstPost = postBars[0]
  const latest = postBars[postBars.length - 1]
  const anchor = preBar?.close || firstPost.open || firstPost.close
  if (!anchor || anchor <= 0 || !latest?.close) {
    return { available: false, ticker, event_sec: eventSec, source: 'mongo_ohlcv_bars', reason: 'missing_anchor_or_latest_price' }
  }
  const highBar = postBars.reduce((best, bar) => Number(bar.high) > Number(best.high) ? bar : best, firstPost)
  const lowBar = postBars.reduce((best, bar) => Number(bar.low) < Number(best.low) ? bar : best, firstPost)
  const runupPct = pctReturn(anchor, highBar.high)
  const latestReturnPct = pctReturn(anchor, latest.close)
  const lowReturnPct = pctReturn(anchor, lowBar.low)
  const givebackFromHighPct = highBar.high > 0 ? ((highBar.high - latest.close) / highBar.high) * 100 : null
  const minutesSinceCatalyst = Math.max(0, Math.round((latest.minute - eventSec) / 60))
  const postCatalystVolume = postBars.reduce((sum, bar) => sum + Number(bar.volume || 0), 0)
  const postCatalystDollarVolume = postBars.reduce((sum, bar) => sum + Number(bar.volume || 0) * Number(bar.close || 0), 0)
  const baseReaction = {
    available: true,
    ticker,
    source: 'mongo_ohlcv_bars',
    event_sec: eventSec,
    anchor_minute: preBar?.minute || firstPost.minute,
    first_post_minute: firstPost.minute,
    latest_minute: latest.minute,
    post_catalyst_bar_count: postBars.length,
    post_catalyst_minutes: minutesSinceCatalyst,
    anchor_price: Number(anchor.toFixed(4)),
    first_post_close: Number(firstPost.close.toFixed(4)),
    latest_close: Number(latest.close.toFixed(4)),
    post_catalyst_high: Number(highBar.high.toFixed(4)),
    post_catalyst_high_minute: highBar.minute,
    post_catalyst_low: Number(lowBar.low.toFixed(4)),
    post_catalyst_low_minute: lowBar.minute,
    post_catalyst_runup_pct: runupPct == null ? null : Number(runupPct.toFixed(2)),
    post_catalyst_latest_return_pct: latestReturnPct == null ? null : Number(latestReturnPct.toFixed(2)),
    post_catalyst_low_return_pct: lowReturnPct == null ? null : Number(lowReturnPct.toFixed(2)),
    post_catalyst_giveback_from_high_pct: givebackFromHighPct == null ? null : Number(Math.max(0, givebackFromHighPct).toFixed(2)),
    post_catalyst_volume: postCatalystVolume || null,
    post_catalyst_dollar_volume: postCatalystDollarVolume ? Number(postCatalystDollarVolume.toFixed(2)) : null,
    provider_interval_sec: firstPost.providerIntervalSec,
    provider_interval: firstPost.providerInterval,
  }
  const classification = classifyCatalystReaction(baseReaction)
  return {
    ...baseReaction,
    reaction_state: classification.state,
    reaction_label: classification.label,
    reaction_tone: classification.tone,
    reaction_reason: classification.reason,
    market_had_chance_to_react: classification.market_had_chance_to_react,
    actionable_spillover: classification.actionable_spillover,
    exhaustion_risk: classification.exhaustion_risk,
  }
}

function yahooIntervalSeconds(interval = '') {
  const text = String(interval || '').toLowerCase()
  const match = text.match(/^(\d+)(m|h|d)$/)
  if (!match) return null
  const n = Number(match[1])
  if (match[2] === 'm') return n * 60
  if (match[2] === 'h') return n * 3600
  if (match[2] === 'd') return n * 86400
  return null
}

async function fetchYahooReactionCandles(ticker, range = '5d', interval = '1m') {
  const symbol = String(ticker || '').toUpperCase().trim()
  if (!symbol) return []
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), PREDICTION_CATALYST_REACTION_FETCH_TIMEOUT_MS)
  try {
    const url = new URL(`https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`)
    url.searchParams.set('range', range)
    url.searchParams.set('interval', interval)
    url.searchParams.set('includePrePost', 'true')
    url.searchParams.set('events', 'history')
    const resp = await fetch(url, {
      signal: controller.signal,
      headers: { 'User-Agent': 'Mozilla/5.0 FlashFeed/1.0' },
    })
    if (!resp.ok) throw new Error(`chart provider HTTP ${resp.status}`)
    const payload = await resp.json()
    const result = payload?.chart?.result?.[0]
    const timestamps = result?.timestamp || []
    const quote = result?.indicators?.quote?.[0] || {}
    const opens = quote.open || []
    const highs = quote.high || []
    const lows = quote.low || []
    const closes = quote.close || []
    const volumes = quote.volume || []
    const intervalSec = yahooIntervalSeconds(interval)
    const bars = []
    for (let i = 0; i < timestamps.length; i += 1) {
      const minute = nullableNumber(timestamps[i])
      const open = nullableNumber(opens[i])
      const high = nullableNumber(highs[i])
      const low = nullableNumber(lows[i])
      const close = nullableNumber(closes[i])
      if (!minute || ![open, high, low, close].every(value => value != null && value > 0)) continue
      bars.push({
        ticker: symbol,
        minute: Math.floor(minute / 60) * 60,
        open,
        high,
        low,
        close,
        price: close,
        volume: Math.max(0, Number(volumes[i] || 0)),
        source: 'yahoo_chart_ohlcv',
        providerRange: range,
        providerInterval: interval,
        providerIntervalSec: intervalSec,
        fetched_at: new Date(),
      })
    }
    return bars
  } catch (_) {
    return []
  } finally {
    clearTimeout(timeout)
  }
}

async function upsertReactionOhlcBars(collection, bars = []) {
  if (!collection || !bars.length) return 0
  const ops = bars.map(bar => ({
    updateOne: {
      filter: { source: bar.source || 'yahoo_chart_ohlcv', ticker: bar.ticker, minute: bar.minute },
      update: { $set: bar },
      upsert: true,
    },
  }))
  const result = await collection.bulkWrite(ops, { ordered: false }).catch(() => null)
  return Number(result?.upsertedCount || 0) + Number(result?.modifiedCount || 0)
}

async function loadCatalystPriceReactionMap(db, rows = []) {
  if (!db || !rows.length) return new Map()
  const candidates = rows
    .map(row => ({
      row,
      ticker: String(row.ticker || '').toUpperCase(),
      eventSec: catalystEventSec(row),
      priority:
        Math.max(0, Number(row.catalyst_power_score || 0)) * 10 +
        Math.max(0, Number(row.catalyst_window_article_count || 0)) * 10 +
        Math.max(0, Number(row.news_article_count || row.article_count || 0)) * 8 +
        Math.max(0, Number(row.message_count || 0)) * 0.35 +
        Math.max(0, Number(row.change_pct || 0)) * 0.8 +
        Math.log1p(Math.max(0, Number(row.rel_volume || 0))) * 6,
    }))
    .filter(item => item.ticker && item.eventSec)
    .sort((a, b) => b.priority - a.priority)
    .slice(0, PREDICTION_CATALYST_REACTION_MAX_TICKERS)
  if (!candidates.length) return new Map()

  const out = new Map()
  const collection = db.collection('ohlcv_bars')
  let cursor = 0
  let liveFetchUsed = 0
  async function worker() {
    while (cursor < candidates.length) {
      const item = candidates[cursor]
      cursor += 1
      const start = Math.max(1, item.eventSec - 30 * 60)
      const end = Math.floor(Date.now() / 1000) + 10 * 60
      const docs = await collection.find({
        ticker: item.ticker,
        minute: { $gte: start, $lte: end },
      }, {
        projection: { _id: 0, minute: 1, open: 1, high: 1, low: 1, close: 1, price: 1, volume: 1, providerIntervalSec: 1, providerInterval: 1 },
      })
        .sort({ minute: 1, providerIntervalSec: 1 })
        .limit(1500)
        .toArray()
        .catch(() => [])
      const hasPostCatalystBars = docs.some(doc => Number(doc.minute || 0) >= item.eventSec)
      let liveBars = []
      let liveOhlcRefreshed = false
      if (PREDICTION_CATALYST_REACTION_LIVE_OHLC && !hasPostCatalystBars && liveFetchUsed < PREDICTION_CATALYST_REACTION_LIVE_FETCH_MAX) {
        liveFetchUsed += 1
        liveBars = await fetchYahooReactionCandles(item.ticker, '5d', '1m')
        if (!liveBars.length) liveBars = await fetchYahooReactionCandles(item.ticker, '5d', '5m')
        if (liveBars.length) {
          await upsertReactionOhlcBars(collection, liveBars)
          liveOhlcRefreshed = true
        }
      }
      const reaction = calculateCatalystPriceReaction(item.row, liveBars.length ? [...docs, ...liveBars] : docs)
      out.set(item.ticker, {
        ...reaction,
        live_ohlc_refreshed: liveOhlcRefreshed,
        live_ohlc_refresh_enabled: PREDICTION_CATALYST_REACTION_LIVE_OHLC,
      })
    }
  }
  const concurrency = Math.min(8, candidates.length)
  await Promise.all(Array.from({ length: concurrency }, () => worker()))
  return out
}

async function loadThresholdFeatureMap(db, tickers = []) {
  const wanted = Array.from(new Set(tickers.map(ticker => String(ticker || '').toUpperCase()).filter(Boolean)))
  if (!db || !wanted.length) return new Map()
  const docs = await db.collection('screeners').find({
    ticker: { $in: wanted },
  }, {
    projection: {
      _id: 0,
      ticker: 1,
      price_density_correlation: 1,
      previous_price_density_correlation: 1,
      threshold_pre_return_60m_pct: 1,
      threshold_trailing_60m_messages: 1,
      threshold_feature_window_minutes: 1,
      threshold_feature_min_observations: 1,
      threshold_feature_status: 1,
      threshold_setup_status: 1,
      threshold_setup_score: 1,
      threshold_setup_distance_to_entry: 1,
      threshold_feature_policy_version: 1,
      threshold_feature_source: 1,
      threshold_feature_snapshot_sec: 1,
      threshold_feature_updated_at: 1,
    },
  }).toArray().catch(() => [])
  return new Map(docs.map(doc => [String(doc.ticker || '').toUpperCase(), doc]))
}

function socialTickerCandidateStages() {
  const stringSplit = (field) => ({
    $cond: [
      { $eq: [{ $type: field }, 'string'] },
      { $split: [field, ','] },
      [],
    ],
  })
  const arrayOrStringSplit = (field) => ({
    $cond: [
      { $isArray: field },
      field,
      stringSplit(field),
    ],
  })

  return [
    {
      $addFields: {
        _ticker_primary_values_raw: {
          $concatArrays: [
            stringSplit('$ticker'),
            stringSplit('$symbol'),
            stringSplit('$cashtag'),
            arrayOrStringSplit('$tickers_mentioned'),
          ],
        },
        _ticker_text_cashtags: {
          $map: {
            input: {
              $regexFindAll: {
                input: {
                  $concat: [
                    { $toString: { $ifNull: ['$text', ''] } },
                    ' ',
                    { $toString: { $ifNull: ['$content', ''] } },
                    ' ',
                    { $toString: { $ifNull: ['$title', ''] } },
                  ],
                },
                regex: /\$[A-Za-z][A-Za-z0-9.-]{0,5}\b/,
              },
            },
            as: 'tag',
            in: '$$tag.match',
          },
        },
      },
    },
    {
      $addFields: {
        _ticker_values_raw: {
          $cond: [
            {
              $gt: [
                {
                  $size: {
                    $filter: {
                      input: { $ifNull: ['$_ticker_primary_values_raw', []] },
                      as: 'raw',
                      cond: { $ne: [{ $trim: { input: { $toString: '$$raw' } } }, ''] },
                    },
                  },
                },
                0,
              ],
            },
            '$_ticker_primary_values_raw',
            { $ifNull: ['$_ticker_text_cashtags', []] },
          ],
        },
      },
    },
    {
      $addFields: {
        _ticker_candidates: {
          $filter: {
            input: {
              $map: {
                input: '$_ticker_values_raw',
                as: 'raw',
                in: {
                  $trim: {
                    input: {
                      $replaceAll: {
                        input: { $toUpper: { $toString: '$$raw' } },
                        find: { $literal: '$' },
                        replacement: '',
                      },
                    },
                    chars: ' ,;#',
                  },
                },
              },
            },
            as: 'candidate',
            cond: {
              $regexMatch: {
                input: '$$candidate',
                regex: '^[A-Z][A-Z0-9.-]{0,5}$',
              },
            },
          },
        },
      },
    },
  ]
}

function socialMatchForWindow(tickers, windowMinutes) {
  const sinceSec = Math.floor(Date.now() / 1000) - windowMinutes * 60
  return {
    _event_sec: { $gte: sinceSec },
    _ticker_candidates: { $in: tickers },
  }
}

async function loadAdaptiveSocialStatsForRows(db, rows, windowOverride = null) {
  const byWindow = new Map()
  for (const row of rows) {
    const window = resolvedRollingWindowMinutes(row, windowOverride)
    if (!byWindow.has(window)) byWindow.set(window, [])
    byWindow.get(window).push(row.ticker)
  }

  const or = Array.from(byWindow.entries()).map(([window, tickers]) => socialMatchForWindow(tickers, window))
  if (!or.length) return new Map()

  const results = await db.collection('socials').aggregate([
    ...socialTimeStages(),
    ...socialTickerCandidateStages(),
    { $match: { $or: or } },
    { $unwind: '$_ticker_candidates' },
    { $match: { _ticker_candidates: { $in: rows.map(row => row.ticker) } } },
    {
      $addFields: {
        _norm_platform: {
          $switch: {
            branches: [
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$platform', ''] } } }, regex: 'stocktwits' } }, then: 'StockTwits' },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$platform', ''] } } }, regex: 'twitter|x' } }, then: 'Twitter' },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$platform', ''] } } }, regex: 'reddit' } }, then: 'Reddit' },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$platform', ''] } } }, regex: 'bluesky|bsky' } }, then: 'Bluesky' },
            ],
            default: { $ifNull: ['$platform', 'Unknown'] },
          },
        },
        _score: {
          $switch: {
            branches: [
              { case: { $in: [{ $type: '$sentiment_score' }, ['int', 'long', 'double', 'decimal']] }, then: { $toDouble: '$sentiment_score' } },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$sentiment', ''] } } }, regex: 'bull|positive' } }, then: 1 },
              { case: { $regexMatch: { input: { $toLower: { $toString: { $ifNull: ['$sentiment', ''] } } }, regex: 'bear|negative' } }, then: -1 },
            ],
            default: 0,
          },
        },
      },
    },
    {
      $group: {
        _id: '$_ticker_candidates',
        count: { $sum: 1 },
        sentiment: { $avg: '$_score' },
        bullish: { $sum: { $cond: [{ $gt: ['$_score', 0.15] }, 1, 0] } },
        bearish: { $sum: { $cond: [{ $lt: ['$_score', -0.15] }, 1, 0] } },
        platforms: { $addToSet: '$_norm_platform' },
        stocktwits_count: { $sum: { $cond: [{ $eq: ['$_norm_platform', 'StockTwits'] }, 1, 0] } },
        stocktwits_score_sum: { $sum: { $cond: [{ $eq: ['$_norm_platform', 'StockTwits'] }, '$_score', 0] } },
        stocktwits_bullish: { $sum: { $cond: [{ $and: [{ $eq: ['$_norm_platform', 'StockTwits'] }, { $gt: ['$_score', 0.15] }] }, 1, 0] } },
        stocktwits_bearish: { $sum: { $cond: [{ $and: [{ $eq: ['$_norm_platform', 'StockTwits'] }, { $lt: ['$_score', -0.15] }] }, 1, 0] } },
        latest_post: { $max: '$_event_sec' },
      },
    },
  ]).toArray()

  return new Map(results.map(row => [String(row._id || '').toUpperCase(), row]))
}

function enrichScreenerRow(row, articleRow, socialRow, windowOverride = null) {
  const newsScore = articleRow ? sentimentScore(articleRow) : Number(row.structured_sentiment || 0)
  const socialCount = Number(socialRow?.count || 0)
  const socialScore = socialCount ? Number(Number(socialRow.sentiment || 0).toFixed(3)) : Number(row.social_sentiment || 0)
  const rollingWindow = resolvedRollingWindowMinutes(row, windowOverride)
  const stocktwitsCount = Number(socialRow?.stocktwits_count || 0)
  const stocktwitsScore = stocktwitsCount
    ? Number((Number(socialRow?.stocktwits_score_sum || 0) / Math.max(1, stocktwitsCount)).toFixed(3))
    : 0
  const stocktwitsDensity = Number((stocktwitsCount / Math.max(1, rollingWindow)).toFixed(3))
  const socialDensity = Number((socialCount / Math.max(1, rollingWindow)).toFixed(3))
  const messageDensityScore = Number(Math.min(100,
    Math.log1p(Math.max(0, socialCount)) / Math.log1p(120) * 70 +
    Math.max(0, socialScore) * 18 +
    Math.min(12, stocktwitsDensity * 4)
  ).toFixed(1))
  const messageDensityTrend = socialCount <= 0
    ? 'none'
    : socialDensity >= 5
      ? 'surging_social_velocity'
      : socialDensity >= 1
        ? 'high_social_velocity'
        : socialCount >= 3
          ? 'active_social'
          : 'thin_social'
  const articleCount = Number(articleRow?.count || row.news_article_count || 0)
  const catalysts = Array.isArray(articleRow?.catalysts) ? articleRow.catalysts : []
  const mainCatalyst = articleRow?.main_catalyst || catalysts[0] || null
  const catalystPowerScore = nullableNumber(articleRow?.catalyst_power_score) || 0
  const totalWeight = articleCount + socialCount * 0.75
  const avgSentiment = totalWeight
    ? Number(((newsScore * articleCount + socialScore * socialCount * 0.75) / totalWeight).toFixed(3))
    : Number(row.avg_sentiment || 0)

  return {
    ...row,
    country: row.country || 'USA',
    rolling_window_minutes: rollingWindow,
    avg_sentiment: avgSentiment,
    structured_sentiment: newsScore,
    social_sentiment: socialScore,
    social_message_sentiment: socialScore,
    social_message_density: socialDensity,
    stocktwits_message_sentiment: stocktwitsScore,
    stocktwits_message_density: stocktwitsDensity,
    stocktwits_message_count: stocktwitsCount,
    message_count: socialCount,
    message_density_now: socialDensity,
    message_density_5m: socialDensity,
    message_density_15m: socialDensity,
    message_density_30m: socialDensity,
    message_density_60m: socialDensity,
    message_density_prev_window: nullableNumber(row.message_density_now) ?? 0,
    message_density_change: socialDensity - (nullableNumber(row.message_density_now) ?? 0),
    message_density_change_pct: nullableNumber(row.message_density_now) && Number(row.message_density_now) > 0
      ? Number((((socialDensity - Number(row.message_density_now)) / Math.max(0.001, Number(row.message_density_now))) * 100).toFixed(1))
      : socialCount > 0 ? 100 : 0,
    message_density_trend: messageDensityTrend,
    message_density_rising: socialCount > 0 && messageDensityTrend !== 'thin_social',
    message_density_supported: socialCount >= 3,
    message_density_score: messageDensityScore,
    message_density_live_score: messageDensityScore,
    message_density_carry_score: socialCount >= 3 ? Math.min(100, messageDensityScore * 0.75) : 0,
    message_density_session_count: socialCount,
    message_density_session_minutes: rollingWindow,
    message_density_session_density: socialDensity,
    message_density_last_event_age_minutes: socialRow?.latest_post ? Number(Math.max(0, (Date.now() / 1000 - Number(socialRow.latest_post)) / 60).toFixed(1)) : null,
    message_density_active_15m: socialDensity,
    message_density_active_60m: socialDensity,
    news_article_count: articleCount,
    catalyst_window_article_count: Number(articleRow?.catalyst_window_count || 0),
    catalyst_power_score: catalystPowerScore,
    main_catalyst: mainCatalyst,
    catalysts,
    catalyst_summary: mainCatalyst?.title || (articleCount ? `${articleCount} recent news item${articleCount === 1 ? '' : 's'}` : ''),
    structured_catalyst_type: mainCatalyst?.event_type || null,
    catalyst_session_context: articleRow?.catalyst_session_context || null,
    bullish_count: Number(articleRow?.bullish || 0) + Number(socialRow?.bullish || 0),
    bearish_count: Number(articleRow?.bearish || 0) + Number(socialRow?.bearish || 0),
    neutral_count: Number(articleRow?.neutral || 0),
    sources: [...(articleRow?.sources || []), ...(socialRow?.platforms || []), row.quote_source].filter(Boolean).slice(0, 8),
    latest_publish: articleRow?.latest_publish || null,
    latest_social: socialRow?.latest_post || null,
  }
}

function isoDateKey(date = new Date()) {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date)
}

function addDaysIso(dateKey, days = 1) {
  const [year, month, day] = String(dateKey || isoDateKey()).split('-').map(Number)
  const date = new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0))
  return isoDateKey(date)
}

function nextTradingDateIso(dateKey = isoDateKey()) {
  let days = 1
  for (let guard = 0; guard < 10; guard += 1) {
    const candidate = addDaysIso(dateKey, days)
    const [year, month, day] = candidate.split('-').map(Number)
    const dow = new Date(Date.UTC(year, month - 1, day, 12, 0, 0)).getUTCDay()
    if (dow !== 0 && dow !== 6) return candidate
    days += 1
  }
  return addDaysIso(dateKey, 1)
}

function compactReasonList(values = []) {
  return Array.isArray(values) ? values.filter(Boolean).map(String).slice(0, 6) : []
}

function sentimentFromStocktwitsMessage(msg = {}) {
  const basic = String(msg?.entities?.sentiment?.basic || '').toLowerCase()
  if (basic.includes('bull')) return { label: 'bullish', score: 1 }
  if (basic.includes('bear')) return { label: 'bearish', score: -1 }
  return { label: 'neutral', score: 0 }
}

function stocktwitsMessageDoc(ticker, msg = {}) {
  const id = String(msg.id || '')
  const body = String(msg.body || '').trim()
  const createdSec = timestampSeconds(msg.created_at) || Math.floor(Date.now() / 1000)
  const sentiment = sentimentFromStocktwitsMessage(msg)
  if (!ticker || !id || !body) return null
  return {
    _id: `stocktwits:${id}`,
    social_id: `stocktwits:${id}`,
    id,
    platform: 'StockTwits',
    source: 'stocktwits_symbol_stream_api',
    collector: 'stocktwits_squeeze_interest_v1',
    symbol: ticker,
    ticker,
    tickers_mentioned: [ticker],
    title: body.slice(0, 140),
    text: body,
    content: body,
    url: `https://stocktwits.com/symbol/${ticker}`,
    author: msg.user?.username || '',
    sentiment: sentiment.label,
    sentiment_score: sentiment.score,
    score: sentiment.score,
    ml_confidence: sentiment.score === 0 ? 0 : 1,
    keywords: [ticker, 'stocktwits'],
    finance_keywords: [ticker, 'stocktwits'],
    gossip_keywords: /squeeze|short|float|borrow|gamma/i.test(body) ? ['short squeeze'] : [],
    gossip_score: /squeeze|short|float|borrow|gamma/i.test(body) ? 1 : 0,
    created_at: createdSec,
    publish_date: createdSec,
    detected_at: Math.floor(Date.now() / 1000),
    fetched_at: Math.floor(Date.now() / 1000),
    is_real: true,
  }
}

async function fetchStocktwitsSymbol(ticker) {
  if (typeof fetch !== 'function' || !ticker) return null
  const resp = await fetch(`https://api.stocktwits.com/api/2/streams/symbol/${encodeURIComponent(ticker)}.json?limit=30`, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'FeedFlashStockDashboard/0.1',
    },
  })
  if (!resp.ok) throw new Error(`Stocktwits HTTP ${resp.status}`)
  return resp.json()
}

async function captureStocktwitsForTicker(db, ticker) {
  try {
    const payload = await fetchStocktwitsSymbol(ticker)
    const messages = Array.isArray(payload?.messages) ? payload.messages : []
    const docs = messages.map(msg => stocktwitsMessageDoc(ticker, msg)).filter(Boolean)
    const watcherCount = nullableNumber(payload?.symbol?.watchlist_count)
    if (watcherCount != null) {
      await persistWatcherSnapshot(db, {
        ticker,
        watcher_count: watcherCount,
        source: 'stocktwits_symbol_stream_api',
        symbol_id: payload?.symbol?.id || null,
        symbol_title: payload?.symbol?.title || '',
      }, { collector: 'screener_symbol_stream' }).catch(() => null)
    }
    if (docs.length) {
      await db.collection('socials').bulkWrite(
        docs.map(doc => ({
          updateOne: {
            filter: { _id: doc._id },
            update: { $set: doc },
            upsert: true,
          },
        })),
        { ordered: false },
      ).catch(() => null)
    }
    return { watcher_count: watcherCount, messages: docs.length }
  } catch (_) {
    return { watcher_count: null, messages: 0 }
  }
}

async function fetchYahooQuoteSnapshot(ticker) {
  if (typeof fetch !== 'function' || !ticker) return null
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?range=5d&interval=1d&includePrePost=true&events=history`
  const resp = await fetch(url, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'FeedFlashStockDashboard/0.1',
    },
  })
  if (!resp.ok) return null
  const payload = await resp.json()
  const result = payload?.chart?.result?.[0]
  const meta = result?.meta || {}
  const quote = result?.indicators?.quote?.[0] || {}
  const closes = (quote.close || []).map(Number).filter(Number.isFinite)
  const volumes = (quote.volume || []).map(Number).filter(Number.isFinite)
  const price = nullableNumber(meta.regularMarketPrice ?? closes[closes.length - 1])
  const previousClose = nullableNumber(meta.previousClose ?? closes[closes.length - 2])
  const volume = volumes[volumes.length - 1] || null
  const avgVolume = volumes.length > 1
    ? volumes.slice(0, -1).reduce((sum, value) => sum + value, 0) / Math.max(1, volumes.length - 1)
    : null
  const changePct = price != null && previousClose ? Number((((price - previousClose) / previousClose) * 100).toFixed(2)) : null
  const relVolume = volume != null && avgVolume ? Number((volume / Math.max(1, avgVolume)).toFixed(2)) : null
  return {
    price: price == null ? null : Number(price.toFixed(2)),
    previous_close: previousClose == null ? null : Number(previousClose.toFixed(2)),
    change_pct: changePct,
    volume,
    avg_volume: avgVolume == null ? null : Math.round(avgVolume),
    rel_volume: relVolume,
    exchange: normalizeExchange(meta.fullExchangeName || meta.exchangeName || meta.exchange || ''),
    company: meta.longName || meta.shortName || '',
    quote_source: 'yahoo_chart_quote',
    quote_updated_at: Math.floor(Date.now() / 1000),
    quote_status: price == null ? 'missing' : 'priced',
  }
}

async function loadSqueezeInterestRows(db, existingRows = [], sessionContext = marketSessionContext(), days = 3, windowOverride = null) {
  const existing = new Set(existingRows.map(row => String(row.ticker || '').toUpperCase()))
  const nowSec = Math.floor(Date.now() / 1000)
  const sinceSec = nowSec - WATCHER_SNAPSHOT_MAX_AGE_SECONDS
  const candidates = await db.collection('stocktwits_watcher_snapshots').aggregate([
    { $match: { watcher_count: { $gte: SQUEEZE_WATCHER_MIN }, fetched_sec: { $gte: sinceSec, $lte: nowSec } } },
    { $sort: { fetched_sec: -1 } },
    { $group: { _id: '$ticker', watcher_count: { $first: '$watcher_count' }, fetched_sec: { $first: '$fetched_sec' }, source: { $first: '$source' } } },
    { $sort: { watcher_count: -1 } },
    { $limit: SQUEEZE_SUPPLEMENT_LIMIT },
  ]).toArray().catch(() => [])

  const tickers = candidates
    .map(row => String(row._id || '').toUpperCase())
    .filter(ticker => ticker && !existing.has(ticker) && !ticker.includes('.') && !NON_STOCK_TICKERS.has(ticker))
  if (!tickers.length) return []

  await Promise.all(tickers.slice(0, 8).map(ticker => captureStocktwitsForTicker(db, ticker)))
  const [articleMap, socialMap, quoteRows, shortMap] = await Promise.all([
    loadArticleStatsForTickers(db, tickers, Number(days || 3), sessionContext),
    loadAdaptiveSocialStatsForRows(db, tickers.map(ticker => ({ ticker, rolling_window_minutes: windowOverride || 1440 })), windowOverride || 1440),
    Promise.all(tickers.map(async ticker => ({ ticker, quote: await fetchYahooQuoteSnapshot(ticker).catch(() => null) }))),
    loadShortInterestSnapshots(db, tickers),
  ])
  const quoteMap = new Map(quoteRows.map(row => [row.ticker, row.quote]).filter(([, quote]) => quote?.price != null))
  const watcherMap = new Map(candidates.map(row => [String(row._id || '').toUpperCase(), row]))

  return tickers.map(ticker => {
    const quote = quoteMap.get(ticker)
    if (!quote?.price) return null
    const watcherSnapshot = watcherMap.get(ticker) || null
    const watcherCount = Number(watcherSnapshot?.watcher_count || 0)
    const socialRow = socialMap.get(ticker)
    const stocktwitsCount = Number(socialRow?.stocktwits_count || socialRow?.count || 0)
    // The absolute audience is display context only; watcher growth is joined
    // later and can contribute at most two points to an eligible candidate.
    const watcherScore = 0
    const messageScore = Math.min(25, Math.log1p(stocktwitsCount) / Math.log1p(40) * 25)
    const priceScore = Math.min(15, Math.max(0, Number(quote.change_pct || 0)) * 1.5)
    const relVolScore = quote.rel_volume == null ? 0 : Math.min(15, Math.log1p(Math.max(0, quote.rel_volume)) * 5)
    const shortSqueezeScore = Number(Math.min(100, watcherScore + messageScore + priceScore + relVolScore).toFixed(1))
    const base = attachShortInterestEvidence(normalizeScreenerRow({
      ticker,
      company: quote.company,
      exchange: quote.exchange,
      price: quote.price,
      previous_close: quote.previous_close,
      change_pct: quote.change_pct,
      volume: quote.volume,
      avg_volume: quote.avg_volume,
      rel_volume: quote.rel_volume,
      quote_source: 'stocktwits_squeeze_interest+yahoo_chart_quote',
      quote_updated_at: quote.quote_updated_at,
      quote_status: quote.quote_status,
      sector: 'Unclassified',
      industry: 'Unclassified',
    }), shortMap.get(ticker))
    const verifiedShortPct = nullableNumber(base.short_interest_pct)
    const verifiedDaysToCover = nullableNumber(base.days_to_cover)
    const verifiedShortBoost = Math.min(30,
      (verifiedShortPct == null ? 0 : Math.min(20, verifiedShortPct)) +
      (verifiedDaysToCover == null ? 0 : Math.min(10, verifiedDaysToCover * 3))
    )
    return enrichScreenerRow({
      ...base,
      squeeze_interest_candidate: true,
      short_squeeze_score: Math.max(shortSqueezeScore, Number(base.short_squeeze_score || 0), Number(Math.min(100, shortSqueezeScore + verifiedShortBoost).toFixed(1))),
      short_squeeze_available: Boolean(base.short_squeeze_available),
      short_squeeze_reason: [base.short_squeeze_reason, `${watcherCount.toLocaleString()} Stocktwits watchers; ${stocktwitsCount} recent Stocktwits messages`].filter(Boolean).join('; '),
      squeeze_signal: base.squeeze_signal || 'social_interest_watch',
      stocktwits_watcher_count: watcherCount,
      watcher_snapshot_sec: nullableNumber(watcherSnapshot?.fetched_sec),
      watcher_snapshot_age_seconds: watcherSnapshot?.fetched_sec == null ? null : Math.max(0, Math.floor(Date.now() / 1000) - Number(watcherSnapshot.fetched_sec)),
      watcher_snapshot_source: watcherSnapshot?.source || 'stocktwits_watcher_snapshots',
      sources: ['StockTwits', 'Yahoo Chart Quote'],
    }, articleMap.get(ticker), socialRow, windowOverride)
  }).filter(Boolean)
}

async function loadShortInterestSnapshots(db, tickers = []) {
  const wanted = Array.from(new Set(tickers.map(t => String(t || '').toUpperCase()).filter(Boolean)))
  if (!wanted.length) return new Map()
  const rows = await db.collection('short_interest_snapshots').aggregate([
    { $match: { ticker: { $in: wanted } } },
    { $sort: { as_of_date: -1, fetched_sec: -1 } },
    { $group: { _id: '$ticker', doc: { $first: '$$ROOT' } } },
  ]).toArray().catch(() => [])
  return new Map(rows.map(row => [String(row._id || '').toUpperCase(), row.doc || {}]))
}

async function loadStocktwitsWatcherSnapshots(db, tickers = []) {
  const wanted = Array.from(new Set(tickers.map(t => String(t || '').toUpperCase()).filter(Boolean)))
  if (!wanted.length) return new Map()
  return loadWatcherFeatureMap(db, wanted, {
    maxAgeSeconds: WATCHER_SNAPSHOT_MAX_AGE_SECONDS,
    growthWindowSeconds: WATCHER_GROWTH_WINDOW_SECONDS,
  })
}

function attachShortInterestEvidence(row = {}, shortRow = null) {
  if (!shortRow) return row
  const shortInterestShares = nullableNumber(shortRow.short_interest_shares)
  const previousShortInterestShares = nullableNumber(shortRow.previous_short_interest_shares)
  const shortInterestPct = nullableNumber(shortRow.short_interest_pct_shares_out ?? shortRow.short_interest_pct_float ?? shortRow.short_interest_pct)
  const daysToCover = nullableNumber(shortRow.days_to_cover)
  const changePct = shortInterestShares != null && previousShortInterestShares
    ? Number((((shortInterestShares - previousShortInterestShares) / previousShortInterestShares) * 100).toFixed(2))
    : nullableNumber(shortRow.short_interest_change_pct)
  const coveringStarted = changePct != null && changePct < 0
  const squeezeEvidenceScore = Math.min(100,
    (shortInterestPct == null ? 0 : Math.min(55, shortInterestPct * 2.2)) +
    (daysToCover == null ? 0 : Math.min(18, daysToCover * 5)) +
    (coveringStarted ? 12 : 0)
  )
  const shortReason = [
    shortInterestPct != null ? `${shortInterestPct.toFixed(2)}% short interest` : '',
    shortInterestShares != null ? `${(shortInterestShares / 1_000_000).toFixed(2)}M shares short` : '',
    daysToCover != null ? `${daysToCover.toFixed(2)} days to cover` : '',
    changePct != null ? `${changePct < 0 ? 'short interest down' : 'short interest up'} ${Math.abs(changePct).toFixed(1)}% vs prior` : '',
  ].filter(Boolean).join(' · ')
  return {
    ...row,
    short_interest_shares: shortInterestShares,
    previous_short_interest_shares: previousShortInterestShares,
    short_interest_pct: shortInterestPct,
    days_to_cover: daysToCover,
    short_interest_change_pct: changePct,
    short_covering_signal: coveringStarted ? 'covering_started_but_interest_remains_high' : (shortInterestPct != null && shortInterestPct >= 10 ? 'high_short_interest_remaining' : null),
    short_interest_source: shortRow.source || 'short_interest_snapshot',
    short_interest_as_of: shortRow.as_of_date || null,
    short_squeeze_available: true,
    short_squeeze_score: Math.max(Number(row.short_squeeze_score || 0), Number(squeezeEvidenceScore.toFixed(1))),
    short_squeeze_reason: [row.short_squeeze_reason, shortReason].filter(Boolean).join('; '),
    squeeze_signal: squeezeEvidenceScore >= 55 ? 'verified_short_interest_squeeze_setup' : row.squeeze_signal,
  }
}

function attachWatcherSqueezeEvidence(row = {}, watcherRow = null) {
  const watcherAgeSeconds = nullableNumber(watcherRow?.watcher_snapshot_age_seconds ?? row.watcher_snapshot_age_seconds)
  const fallbackWatcherCount = watcherAgeSeconds != null && watcherAgeSeconds <= WATCHER_SNAPSHOT_MAX_AGE_SECONDS
    ? nullableNumber(row.stocktwits_watcher_count)
    : null
  const watcherCount = nullableNumber(watcherRow?.watcher_count) ?? fallbackWatcherCount
  const hasVerifiedShort = Boolean(row.float_or_short_interest_available || row.short_interest_pct != null || row.float_short != null)
  const watcherFeature = watcherRow?.watcher_feature || {
    definition: 'current StockTwits watchlist count; growth unavailable until at least two deduplicated real snapshots exist',
    source: watcherRow?.source || row.watcher_snapshot_source || 'stocktwits_watcher_snapshots',
    snapshot_sec: nullableNumber(watcherRow?.fetched_sec ?? row.watcher_snapshot_sec),
    age_seconds: watcherAgeSeconds,
    status: watcherRow || (watcherAgeSeconds != null && watcherAgeSeconds <= WATCHER_SNAPSHOT_MAX_AGE_SECONDS) ? 'fresh' : 'missing_or_expired',
    normalization: 'positive causal growth only; capped at 2 ranking points',
    max_rank_points: 2,
    score_points: 0,
    missing_behavior: 'zero rank points; never creates or rejects a candidate',
  }
  const watcherScore = watcherRankScore(watcherFeature)

  return {
    ...row,
    stocktwits_watcher_count: watcherCount ?? row.stocktwits_watcher_count ?? null,
    watcher_snapshot_age_seconds: watcherAgeSeconds,
    watcher_feature: watcherFeature,
    watcher_attention_score: watcherScore,
    short_squeeze_available: Boolean(row.short_squeeze_available || hasVerifiedShort),
    float_or_short_interest_available: Boolean(row.float_or_short_interest_available || hasVerifiedShort),
    squeeze_proxy_used: Boolean(row.squeeze_proxy_used),
  }
}

function predictionMissingFieldCounts(rows = []) {
  const counts = {}
  const fields = ['ticker', 'predictedDirection', 'predictedReturnPct', 'confidence', 'convictionScore', 'predictionDate', 'targetDate']
  for (const row of rows) {
    for (const field of fields) {
      if (row?.[field] == null || row?.[field] === '') counts[field] = (counts[field] || 0) + 1
    }
  }
  return counts
}

function predictionPipelineStageCounts({ configuredUniverseRows = 0, loadedRows = [], enteringRows = [], scoringRows = [], strictRows = [], candidateRows = [], finalRows = [], fallbackRows = [], realUpRows = [], realHighConvictionRows = [] } = {}) {
  const uniqueTickers = rows => new Set(rows.map(row => String(row?.ticker || '').toUpperCase()).filter(Boolean)).size
  const hasNews = row => Number(row?.news_article_count ?? row?.article_count ?? 0) > 0
  const hasDirectCatalyst = row => Boolean(
    row?.prediction_validation?.recognizedNewsCatalyst && row?.prediction_validation?.materialDirectCatalyst ||
    row?.main_catalyst?.ticker_matched || row?.main_catalyst?.ticker_match || row?.main_catalyst?.matched_ticker,
  )
  const hasCatalyst = row => hasDirectCatalyst(row) || Boolean(row?.prediction_validation?.recognizedSqueezeCatalyst && row?.prediction_validation?.verifiedShortInterest)
  const hasSocial = row => Number(row?.message_count ?? row?.stocktwits_message_count ?? 0) > 0
  const hasWatcher = row => nullableNumber(row?.watcher_snapshot_age_seconds) != null && nullableNumber(row?.stocktwits_watcher_count) != null
  const hasFreshQuote = row => {
    const age = nullableNumber(row?.quote_age_minutes ?? row?.quote_freshness_minutes)
    return row?.price != null && (age == null || age <= 45)
  }
  const validScore = row => Number.isFinite(Number(row?.final_prediction_score ?? row?.convictionScore ?? row?.watchScore ?? row?.watch_score))
  return {
    configured_universe_rows: Number(configuredUniverseRows || 0),
    loaded_universe_rows: uniqueTickers(loadedRows),
    fresh_quote_rows: loadedRows.filter(hasFreshQuote).length,
    rows_with_news: loadedRows.filter(hasNews).length,
    rows_with_direct_ticker_catalyst: loadedRows.filter(hasDirectCatalyst).length,
    rows_with_validated_primary_catalyst: scoringRows.filter(hasCatalyst).length,
    rows_with_social: loadedRows.filter(hasSocial).length,
    rows_with_current_watchers: loadedRows.filter(hasWatcher).length,
    entering_scoring: uniqueTickers(enteringRows),
    valid_scored_rows: scoringRows.filter(validScore).length,
    strict_up_candidates: uniqueTickers(realUpRows),
    high_conviction_candidates: uniqueTickers(realHighConvictionRows),
    developing_catalyst_candidates: uniqueTickers(candidateRows),
    fallback_watch_rows: uniqueTickers(fallbackRows),
    ranked_rows: uniqueTickers([...realUpRows, ...candidateRows]),
    displayed_rows: uniqueTickers(finalRows),
    top_five_displayed: Math.min(5, uniqueTickers(finalRows)),
    rows_without_validated_primary_catalyst: Math.max(0, scoringRows.filter(row => !hasCatalyst(row)).length),
    rejection_social_or_watcher_only: scoringRows.filter(row => !hasCatalyst(row) && (hasSocial(row) || hasWatcher(row))).length,
  }
}

function positiveCurrentMover(row = {}) {
  const change = nullableNumber(row.currentChangePct ?? row.change_pct)
  return change != null && change > 0
}

function developingConfidenceProfile(row = {}, options = {}) {
  const change = nullableNumber(row.currentChangePct ?? row.change_pct)
  const relVolume = nullableNumber(row.rel_volume) || 0
  const news = Number(row.news_article_count || row.article_count || 0)
  const social = Number(row.message_count || row.stocktwits_message_count || 0)
  const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
  const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
  const squeezeScore = nullableNumber(row.short_squeeze_score) || 0
  const floatShort = nullableNumber(row.float_short)
  const setupStatus = row.entry_signal?.setup_status || row.threshold_setup_status || row.threshold_policy?.setupStatus || ''
  const validation = row.prediction_validation || predictionEvidenceValidation(row, {
    catalystText: row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || '',
    news,
    social,
    sentiment,
    change,
    relVolume,
    catalystPower: nullableNumber(row.catalyst_power_score) || 0,
    squeezeScore,
    watcherCount,
    floatShort,
    setupStatus,
  })
  const peopleAttention = validation.peopleAttention || predictionPeopleAttention(row, { social, sentiment, watcherCount })
  const catalystQuality = row.catalyst_quality || catalystQualityAssessment(row, validation, {
    catalystText: row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || '',
    news,
    sentiment,
    catalystPower: nullableNumber(row.catalyst_power_score) || 0,
  })
  const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
  const blockedReasons = Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []
  const hardBlocks = new Set([
    ...riskFlags,
    ...blockedReasons,
  ])
  const score = Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score ?? 0)
  const predicted = nullableNumber(row.predictedReturnPct ?? row.predicted_return ?? row.final_predicted_percent)
  const payoffProbability = nullableNumber(row.payoff_model_probability)
  const hasFreshDensity = Boolean(
    row.entry_signal?.entry_ready ||
    row.threshold_policy?.passed ||
    row.prediction_threshold_policy?.passed ||
    ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(String(setupStatus))
  )
  const hasDirectCatalyst = Boolean(
    hasDevelopingCatalystEvidence(row) ||
    validation.recognizedNewsCatalyst ||
    validation.materialDirectCatalyst
  )
  const hasPeople = Boolean(
    validation.recognizedPeopleAttention ||
    validation.recognizedSocialCatalyst ||
    peopleAttention.active ||
    social >= 3
  )
  const hasSqueeze = Boolean(validation.verifiedShortInterest || validation.recognizedSqueezeCatalyst || squeezeScore >= 55 || (floatShort != null && floatShort >= 10))
  const hasVolume = relVolume >= PREDICTION_DEVELOPING_MIN_REL_VOLUME || (change != null && change >= 2 && relVolume >= 1)
  const positiveMove = change != null && change >= PREDICTION_DEVELOPING_MIN_CHANGE_PCT
  const overextended = change != null && change >= PREDICTION_DEVELOPING_OVEREXTENDED_PCT
  const severeOverextended = change != null && change >= PREDICTION_DEVELOPING_SEVERE_OVEREXTENDED_PCT
  const independentSignals = [
    hasDirectCatalyst,
    hasPeople,
    hasVolume,
    hasFreshDensity,
    hasSqueeze,
  ].filter(Boolean).length
  const hasStrongPeopleWithoutVolume = Boolean(hasPeople && peopleAttention.strong && relVolume >= 1)
  const hasExceptionalVolumeOnlyConfirmation = Boolean(hasDirectCatalyst && relVolume >= 8 && change != null && change >= 1)
  const marketConfirmed = Boolean(positiveMove && (
    hasVolume ||
    hasFreshDensity ||
    hasStrongPeopleWithoutVolume
  ))
  const marketConfirmationScore =
    Math.min(18, Math.log1p(Math.max(0, relVolume)) * 5.5) +
    Math.min(14, Math.max(0, Number(change || 0)) * 0.35) +
    (hasFreshDensity ? 12 : 0)
  const evidenceConfirmationScore =
    (hasDirectCatalyst ? 15 : 0) +
    (hasPeople ? 12 : 0) +
    (hasSqueeze ? 8 : 0) +
    Math.min(10, Math.max(0, Number(catalystQuality.score || 0)) / 10) +
    Math.max(-6, Math.min(8, sentiment * 10))
  const overextensionPenalty = severeOverextended
    ? 42
    : overextended
      ? 24
      : change != null && change >= 18
        ? 8
        : 0
  const liquidityPenalty = relVolume > 0 && relVolume < 1 ? 8 : 0
  const rawQuality = Math.max(0, Math.min(100, score * 0.45 + marketConfirmationScore + evidenceConfirmationScore - overextensionPenalty - liquidityPenalty))
  const confidence = Number(Math.max(0.25, Math.min(0.82,
    0.33 +
    rawQuality / 220 +
    independentSignals * 0.025 +
    (hasFreshDensity ? 0.04 : 0) +
    (hasDirectCatalyst && hasPeople ? 0.035 : 0) -
    (overextended ? 0.1 : 0) -
    (severeOverextended ? 0.08 : 0) -
    (relVolume > 0 && relVolume < 1 ? 0.04 : 0)
  )).toFixed(3))
  const expectedReturn = Number(Math.max(0.35, Math.min(6.25,
    (predicted != null && predicted > 0 ? predicted : 0.55 + rawQuality / 28 + Math.min(1.2, Math.log1p(Math.max(0, relVolume)) / 3.5)) *
    (severeOverextended ? 0.32 : overextended ? 0.48 : 1)
  )).toFixed(2))
  const probabilityUp = Number(Math.max(0.51, Math.min(0.7,
    0.505 + rawQuality / 700 + Math.max(0, sentiment) / 14 - (overextended ? 0.025 : 0) - (severeOverextended ? 0.025 : 0)
  )).toFixed(3))
  const payoff = Number(Math.max(0.36, Math.min(0.82,
    payoffProbability != null
      ? payoffProbability
      : 0.38 + rawQuality / 260 + (hasFreshDensity ? 0.04 : 0) + (hasDirectCatalyst && hasPeople ? 0.03 : 0) - (overextended ? 0.04 : 0)
  )).toFixed(3))
  const hardBlocked = [
    'REJECTED_CATALYST_QUALITY',
    'PENDING_OPEN_WEAK_CATALYST_QUALITY',
    'PENDING_OPEN_UNRECOGNIZED_SOURCE',
    'BEARISH_OR_RISK_CATALYST_NOT_VALID_FOR_UP_PREDICTION',
    'CATALYST_REJECT_BEARISH_CATALYST',
    'CATALYST_REJECT_DILUTION_RISK',
    'CATALYST_REJECT_REVERSE_SPLIT_NOISE',
  ].some(flag => hardBlocks.has(flag))
  const requiredSignals = severeOverextended ? 4 : overextended ? 3 : 2
  const weakSocialAllowed = Boolean(
    hasPeople ||
    hasFreshDensity ||
    (hasExceptionalVolumeOnlyConfirmation && confidence >= 0.58 && rawQuality >= 60)
  )
  const passes = Boolean(
    !hardBlocked &&
    marketConfirmed &&
    independentSignals >= requiredSignals &&
    rawQuality >= Math.max(PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE, Number(options.minScore || 0)) &&
    confidence >= PREDICTION_DEVELOPING_MIN_CONFIDENCE &&
    weakSocialAllowed &&
    (!severeOverextended || hasFreshDensity || (hasDirectCatalyst && hasPeople && relVolume >= 8))
  )
  const rejectionReasons = [
    hardBlocked ? 'hard_quality_or_bearish_block' : '',
    !positiveMove ? `no_positive_move_min_${PREDICTION_DEVELOPING_MIN_CHANGE_PCT}pct` : '',
    positiveMove && !marketConfirmed ? 'positive_move_without_volume_people_or_density_confirmation' : '',
    !weakSocialAllowed ? 'needs_people_density_or_exceptional_volume_confirmation' : '',
    independentSignals < requiredSignals ? `only_${independentSignals}_of_${requiredSignals}_independent_confirmations` : '',
    rawQuality < PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE ? `developing_quality_${rawQuality.toFixed(1)}_below_${PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE}` : '',
    confidence < PREDICTION_DEVELOPING_MIN_CONFIDENCE ? `confidence_${confidence}_below_${PREDICTION_DEVELOPING_MIN_CONFIDENCE}` : '',
    severeOverextended && !(hasFreshDensity || (hasDirectCatalyst && hasPeople && relVolume >= 8)) ? 'severely_overextended_without_density_or_full_confirmation' : '',
  ].filter(Boolean)
  const rankingScore = Number((
    (passes ? 1000 : 0) +
    rawQuality * 5 +
    confidence * 220 +
    probabilityUp * 90 +
    expectedReturn * 12 +
    Math.min(80, relVolume * 2) +
    (hasFreshDensity ? 120 : 0) +
    (hasDirectCatalyst && hasPeople ? 80 : 0) -
    overextensionPenalty * 14 -
    liquidityPenalty * 6
  ).toFixed(2))
  return {
    passes,
    confidence,
    probabilityUp,
    payoffProbability: payoff,
    expectedReturn,
    rawQuality: Number(rawQuality.toFixed(1)),
    rankingScore,
    marketConfirmed,
    positiveMove,
    hasVolume,
    hasPeople,
    hasDirectCatalyst,
    hasFreshDensity,
    hasSqueeze,
    weakSocialAllowed,
    hasStrongPeopleWithoutVolume,
    hasExceptionalVolumeOnlyConfirmation,
    overextended,
    severeOverextended,
    independentSignals,
    requiredSignals,
    rejectionReasons,
    reason: [
      positiveMove ? `${Number(change).toFixed(2)}% positive move` : '',
      hasVolume ? `${relVolume.toFixed(2)}x rel volume` : '',
      hasDirectCatalyst ? 'direct catalyst' : '',
      hasPeople ? 'people/social attention' : '',
      hasFreshDensity ? 'density setup' : '',
      hasSqueeze ? 'squeeze/float support' : '',
      overextended ? 'overextension penalty applied' : '',
    ].filter(Boolean).join(' + '),
  }
}

function developingCandidateMovementOrCatalystOk(row = {}, riskFlags = [], blockedReasons = []) {
  if (positiveCurrentMover(row)) return true
  const level = String(row.prediction_readiness_level || row.prediction_readiness?.level || '').toLowerCase()
  const reaction = row.catalyst_reaction_summary || row.prediction_readiness?.reaction || {}
  const reactionState = String(reaction.first_reaction_state || '').toLowerCase()
  const reactionLabel = String(reaction.label || '').toLowerCase()
  const pendingOpen = row.pending_open_confirmation || {}
  const freshTrigger = row.fresh_prediction_trigger || {}
  const blocked = new Set([...(riskFlags || []), ...(blockedReasons || [])])
  if (blocked.has('REJECTED_CATALYST_QUALITY') || blocked.has('PENDING_OPEN_WEAK_CATALYST_QUALITY')) return false
  return Boolean(
    pendingOpen.passes ||
    freshTrigger.freshNewsCatalyst ||
    level === 'fresh_catalyst_pending_open' ||
    level === 'fresh_catalyst_candidate' ||
    reactionState === 'pending_market_open' ||
    reactionLabel.includes('pending')
  )
}

function isActionablePredictionRow(row = {}) {
  const direction = String(row.predictedDirection || row.prediction_direction || row.prediction?.predictedDirection || '').toLowerCase()
  const predicted = nullableNumber(row.predictedReturnPct ?? row.predicted_return ?? row.prediction?.predictedReturn)
  const entryReady = Boolean(row.entry_signal?.entry_ready || row.threshold_policy?.passed || row.prediction_threshold_policy?.passed)
  return entryReady && (direction === 'up' || direction === 'down') && predicted != null
}

function isValidatedPredictionRow(row = {}) {
  if (row.validated_prediction || row.prediction_validation_status === 'validated' || row.prediction_validation?.valid) return true
  const news = Number(row.news_article_count || row.article_count || 0)
  const social = Number(row.message_count || 0)
  const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
  const squeezeScore = Number(row.short_squeeze_score || row.squeezeScore || 0)
  const catalystText = row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || ''
  const sourceText = [row.main_catalyst?.source, Array.isArray(row.sources) ? row.sources.join(' ') : row.sources, row.source].filter(Boolean).join(' ')
  const validatedNews = news > 0 &&
    !isWeakGenericCatalystText(catalystText) &&
    !isBearishCatalystText([catalystText, row.main_catalyst?.event_type, row.main_catalyst?.sentiment].filter(Boolean).join(' ')) &&
    isRecognizedCatalystSource(sourceText) &&
    catalystMentionsTickerOrCompany(row, catalystText)
  const shortInterestPct = nullableNumber(row.short_interest_pct ?? row.short_interest_pct_shares_out ?? row.short_interest_pct_float)
  const floatShort = nullableNumber(row.float_short)
  const validatedSqueeze = !isBearishCatalystText([catalystText, row.main_catalyst?.event_type, row.main_catalyst?.sentiment].filter(Boolean).join(' ')) && squeezeScore >= 70 && (
    (shortInterestPct != null && shortInterestPct >= 10) ||
    (floatShort != null && floatShort >= 10)
  ) && social >= 3
  const validatedSetup = Boolean(row.entry_signal?.setup_ready || row.entry_signal?.entry_ready) && social >= 3 && (validatedNews || validatedSqueeze || (sentiment != null && sentiment >= 0.12))
  return Boolean(validatedNews || validatedSqueeze || validatedSetup)
}

function uniquePredictionRows(rows = []) {
  const byTicker = new Map()
  const out = []
  for (const row of rows) {
    const ticker = String(row?.ticker || '').toUpperCase()
    if (!ticker) continue
    if (!byTicker.has(ticker)) {
      byTicker.set(ticker, row)
      out.push(row)
      continue
    }
    const existing = byTicker.get(ticker)
    const existingScore = Number(existing.final_prediction_score ?? existing.convictionScore ?? 0)
    const nextScore = Number(row.final_prediction_score ?? row.convictionScore ?? 0)
    const base = nextScore > existingScore ? row : existing
    const supplement = base === row ? existing : row
    const predictedReturn = firstPresent(base.predictedReturnPct, base.predicted_return, base.final_predicted_percent, base.prediction?.predictedReturn, supplement.predictedReturnPct, supplement.predicted_return, supplement.final_predicted_percent, supplement.prediction?.predictedReturn)
    const predictedDirection = firstPresent(base.predictedDirection, base.prediction_direction, base.prediction?.predictedDirection, supplement.predictedDirection, supplement.prediction_direction, supplement.prediction?.predictedDirection)
    const confidence = firstPresent(base.confidence, base.prediction_confidence, base.prediction?.confidence, supplement.confidence, supplement.prediction_confidence, supplement.prediction?.confidence)
    const evidenceFields = mergePredictionEvidenceFields(base, supplement)
    const merged = {
      ...supplement,
      ...base,
      ...evidenceFields,
      prediction_direction: predictedDirection || null,
      predictedDirection: predictedDirection || null,
      predicted_return: predictedReturn ?? null,
      predictedReturnPct: predictedReturn ?? null,
      final_predicted_percent: predictedReturn ?? null,
      prediction_confidence: confidence ?? null,
      confidence: confidence ?? null,
      probability_up: firstPresent(base.probability_up, base.prediction?.probabilityUp, supplement.probability_up, supplement.prediction?.probabilityUp) ?? null,
      main_catalyst: bestTickerSpecificCatalyst({
        main_catalyst: base.main_catalyst || supplement.main_catalyst || null,
        catalysts: [
          ...(Array.isArray(base.catalysts) ? base.catalysts : []),
          ...(Array.isArray(supplement.catalysts) ? supplement.catalysts : []),
        ],
      }),
      catalysts: (Array.isArray(base.catalysts) && base.catalysts.length ? base.catalysts : supplement.catalysts) || [],
      catalyst_power_score: base.catalyst_power_score ?? supplement.catalyst_power_score ?? null,
      catalyst_window_article_count: base.catalyst_window_article_count ?? supplement.catalyst_window_article_count ?? null,
      catalyst_session_context: base.catalyst_session_context || supplement.catalyst_session_context || null,
      prediction: {
        ...(supplement.prediction || {}),
        ...(base.prediction || {}),
        predictedReturn: predictedReturn ?? base.prediction?.predictedReturn ?? supplement.prediction?.predictedReturn ?? null,
        predictedDirection: predictedDirection || base.prediction?.predictedDirection || supplement.prediction?.predictedDirection || null,
        confidence: confidence ?? base.prediction?.confidence ?? supplement.prediction?.confidence ?? null,
      },
      prediction_debug: evidenceFields.prediction_debug || base.prediction_debug || supplement.prediction_debug || null,
    }
    byTicker.set(ticker, merged)
    const index = out.findIndex(item => String(item?.ticker || '').toUpperCase() === ticker)
    if (index >= 0) out[index] = merged
  }
  return out
}

function isPositivePredictionRow(row = {}) {
  const direction = String(row.predictedDirection || row.prediction_direction || row.prediction?.predictedDirection || '').toLowerCase()
  const predicted = nullableNumber(row.predictedReturnPct ?? row.predicted_return ?? row.prediction?.predictedReturn)
  return direction === 'up' && predicted != null && predicted > 0 && positiveCurrentMover(row)
}

function normalizeStoredPredictionRow(raw = {}, quote = {}, index = 0, meta = {}) {
  const ticker = String(raw.ticker || raw.symbol || quote.ticker || '').toUpperCase()
  if (!ticker) return null
  const prediction = raw.prediction || {}
  const direction = String(raw.predictedDirection || raw.prediction_direction || raw.direction || raw.predicted_direction || prediction.predictedDirection || '').toLowerCase()
  const predictedReturn = nullableNumber(raw.predictedReturnPct ?? raw.predicted_return_pct ?? raw.predicted_return ?? raw.predictedReturn ?? prediction.predictedReturn)
  const confidence = nullableNumber(raw.confidence ?? raw.prediction_confidence ?? prediction.confidence)
  const convictionScore = nullableNumber(raw.convictionScore ?? raw.conviction_score ?? raw.final_prediction_score ?? raw.finalPredictionScore)
  const predictionDate = raw.predictionDate || raw.prediction_date || meta.predictionDate || null
  const targetDate = raw.targetDate || raw.target_date || meta.targetDate || null
  const reasons = compactReasonList(raw.reasons || raw.reason || raw.catalystReason ? [raw.catalystReason || raw.reason] : [])
  const riskFlags = compactReasonList(raw.riskFlags || raw.risk_flags)
  const dataQuality = !direction || predictedReturn == null || confidence == null
    ? 'partial_prediction_record'
    : 'stored_prediction'
  const threshold = raw.threshold_policy || raw.prediction_threshold_policy || raw.prediction_thresholds || raw.entry_signal || evaluatePredictionEntryThreshold({ ...quote, ...raw, ticker })
  const currentChange = nullableNumber(raw.currentChangePct ?? raw.current_change_pct ?? raw.change_pct ?? quote.change_pct)
  const currentRelVolume = nullableNumber(raw.rel_volume ?? raw.relative_volume ?? raw.relativeVolume ?? quote.rel_volume)
  const currentNews = nullableNumber(quote.news_article_count ?? raw.news_article_count ?? raw.article_count)
  const currentSocial = nullableNumber(quote.message_count ?? raw.message_count)
  const currentSentiment = nullableNumber(quote.avg_sentiment ?? quote.structured_sentiment ?? quote.social_sentiment ?? raw.avg_sentiment ?? raw.structured_sentiment ?? raw.social_sentiment)
  const currentSocialDensity = nullableNumber(quote.social_message_density ?? quote.message_density_60m ?? raw.social_message_density ?? raw.message_density_60m)
  const currentTradeScore = nullableNumber(quote.trade_watch?.trade_watch_score ?? raw.trade_watch?.trade_watch_score)
  const currentCatalystPower = nullableNumber(quote.catalyst_power_score ?? raw.catalyst_power_score) || 0
  const storedMoveScore = currentChange == null ? 0 : Math.min(12, Math.max(0, currentChange) * 0.55)
  const storedVolumeScore = currentRelVolume == null ? 0 : Math.min(18, Math.log1p(Math.max(0, currentRelVolume)) * 6.5)
  const storedTradeScore = currentTradeScore == null ? 0 : Math.min(12, Math.max(0, currentTradeScore) * 12)
  const storedNewsScore = currentNews == null ? 0 : Math.min(14, Math.log1p(Math.max(0, currentNews)) * 5)
  const storedCatalystScore = Math.min(22, currentCatalystPower * 3.2)
  const storedSentimentScore = currentSentiment == null ? 0 : Math.max(-12, Math.min(14, currentSentiment * 18))
  const storedDensityScore = currentSocialDensity == null ? 0 : Math.min(8, Math.max(0, currentSocialDensity) * 12)
  const computedMomentumScore = currentChange == null && currentRelVolume == null && currentTradeScore == null
    ? null
    : Number(Math.min(100, ((storedMoveScore + storedVolumeScore + storedTradeScore) / 42) * 100).toFixed(1))
  const computedAiScore = currentNews == null && currentSentiment == null && currentCatalystPower <= 0
    ? null
    : Number(Math.min(100, Math.max(0, ((storedNewsScore + storedCatalystScore + storedSentimentScore) / 50) * 100)).toFixed(1))
  const computedSocialScore = Number((Math.min(16, Math.log1p(Math.max(0, currentSocial || 0)) * 4) + storedDensityScore).toFixed(1))
  const computedEvidenceScore = Number((storedNewsScore + storedCatalystScore + computedSocialScore + Math.max(0, storedSentimentScore)).toFixed(1))
  const storedCorrelationScore = nullableNumber(raw.correlation_score ?? quote.correlation_score ?? threshold.correlation ?? raw.price_density_correlation ?? quote.price_density_correlation)
  const storedSecFilingUsed = Boolean(
    raw.sec_filing_contributed ||
    quote.sec_filing_contributed ||
    Number(raw.filing_used_count || quote.filing_used_count || 0) > 0 ||
    raw.main_catalyst?.isSecFiling ||
    quote.main_catalyst?.isSecFiling
  )

  return {
    ...quote,
    ticker,
    company: raw.company || quote.company || '',
    price: nullableFixed(raw.currentPrice ?? raw.priceAtPrediction ?? raw.price_at_prediction ?? raw.price ?? quote.price, 2),
    change_pct: nullableFixed(raw.currentChangePct ?? raw.current_change_pct ?? raw.change_pct ?? quote.change_pct, 2),
    currentPrice: nullableFixed(raw.currentPrice ?? raw.priceAtPrediction ?? raw.price_at_prediction ?? raw.price ?? quote.price, 2),
    currentChangePct: nullableFixed(raw.currentChangePct ?? raw.current_change_pct ?? raw.change_pct ?? quote.change_pct, 2),
    prediction_status: direction ? 'available' : 'stored_incomplete',
    prediction_direction: direction || null,
    predictedDirection: direction || null,
    predicted_return: predictedReturn,
    predictedReturnPct: predictedReturn,
    final_predicted_percent: predictedReturn,
    prediction_confidence: confidence,
    confidence,
    convictionScore,
    final_prediction_score: convictionScore,
    conviction_score: convictionScore,
    high_conviction_rank: raw.rank ?? raw.high_conviction_rank ?? index + 1,
    rank: raw.rank ?? index + 1,
    model_mode: raw.modelMode || raw.model_mode || meta.modelMode || 'stored_prediction',
    modelMode: raw.modelMode || raw.model_mode || meta.modelMode || 'stored_prediction',
    predictionTimestamp: raw.predictionTimestamp || raw.createdAt || raw.created_at || prediction.generatedAt || meta.createdAt || null,
    predictionDate,
    targetDate,
    catalystReason: raw.catalystReason || raw.catalyst_reason || raw.main_catalyst?.title || reasons[0] || '',
    evidenceScore: nullableNumber(raw.evidenceScore ?? raw.evidence_score ?? quote.evidenceScore ?? quote.evidence_score) ?? computedEvidenceScore,
    evidence_score: nullableNumber(raw.evidence_score ?? raw.evidenceScore ?? quote.evidence_score ?? quote.evidenceScore) ?? computedEvidenceScore,
    catalystScore: nullableNumber(raw.catalystScore ?? raw.catalyst_score ?? quote.catalystScore ?? quote.catalyst_score),
    catalyst_score: nullableNumber(raw.catalyst_score ?? raw.catalystScore ?? quote.catalyst_score ?? quote.catalystScore),
    newsScore: nullableNumber(raw.newsScore ?? raw.news_score ?? quote.newsScore ?? quote.news_score) ?? storedNewsScore,
    news_score: nullableNumber(raw.news_score ?? raw.newsScore ?? quote.news_score ?? quote.newsScore) ?? storedNewsScore,
    socialScore: nullableNumber(raw.socialScore ?? raw.social_score ?? quote.socialScore ?? quote.social_score) ?? computedSocialScore,
    social_score: nullableNumber(raw.social_score ?? raw.socialScore ?? quote.social_score ?? quote.socialScore) ?? computedSocialScore,
    momentumScore: nullableNumber(raw.momentumScore ?? raw.momentum_score ?? quote.momentumScore ?? quote.momentum_score) ?? computedMomentumScore,
    momentum_score: nullableNumber(raw.momentum_score ?? raw.momentumScore ?? quote.momentum_score ?? quote.momentumScore) ?? computedMomentumScore,
    technicalScore: nullableNumber(raw.technicalScore ?? raw.technical_score),
    ai_score: nullableNumber(raw.ai_score ?? raw.aiScore ?? quote.ai_score ?? quote.aiScore) ?? computedAiScore,
    correlation_score: storedCorrelationScore,
    correlation_context: raw.correlation_context || quote.correlation_context || {
      source: threshold.correlation != null ? 'threshold_policy' : quote.price_density_correlation != null ? 'screeners.threshold_features' : 'missing',
      current: storedCorrelationScore,
      previous: nullableNumber(threshold.previousCorrelation ?? raw.previous_price_density_correlation ?? quote.previous_price_density_correlation),
      status: raw.threshold_setup_status || quote.threshold_setup_status || threshold.setupStatus || threshold.status || null,
      updated_at: raw.threshold_feature_updated_at || quote.threshold_feature_updated_at || null,
    },
    sec_filing_contributed: storedSecFilingUsed,
    filing_used_count: nullableNumber(raw.filing_used_count ?? quote.filing_used_count) ?? (storedSecFilingUsed ? 1 : 0),
    filing_sentiment: nullableNumber(raw.filing_sentiment ?? quote.filing_sentiment) ?? 0,
    social_message_sentiment: nullableNumber(quote.social_message_sentiment ?? raw.social_message_sentiment),
    social_message_density: currentSocialDensity,
    message_count: currentSocial,
    message_density_trend: firstPresent(quote.message_density_trend, raw.message_density_trend) || (currentSocial ? 'active_social' : 'none'),
    message_density_now: nullableNumber(quote.message_density_now ?? raw.message_density_now ?? currentSocialDensity),
    message_density_5m: nullableNumber(quote.message_density_5m ?? raw.message_density_5m ?? currentSocialDensity),
    message_density_15m: nullableNumber(quote.message_density_15m ?? raw.message_density_15m ?? currentSocialDensity),
    message_density_30m: nullableNumber(quote.message_density_30m ?? raw.message_density_30m ?? currentSocialDensity),
    message_density_60m: nullableNumber(quote.message_density_60m ?? raw.message_density_60m ?? currentSocialDensity),
    message_density_score: nullableNumber(quote.message_density_score ?? raw.message_density_score),
    message_density_supported: Boolean(quote.message_density_supported || raw.message_density_supported || Number(currentSocial || 0) >= 3),
    message_density_rising: Boolean(quote.message_density_rising || raw.message_density_rising || Number(currentSocial || 0) >= 3),
    stocktwits_message_count: nullableNumber(quote.stocktwits_message_count ?? raw.stocktwits_message_count),
    stocktwits_watcher_count: nullableNumber(quote.stocktwits_watcher_count ?? raw.stocktwits_watcher_count),
    short_squeeze_score: nullableNumber(quote.short_squeeze_score ?? raw.short_squeeze_score),
    short_squeeze_available: Boolean(quote.short_squeeze_available || raw.short_squeeze_available),
    short_squeeze_reason: firstPresent(quote.short_squeeze_reason, raw.short_squeeze_reason) || null,
    squeeze_signal: firstPresent(quote.squeeze_signal, raw.squeeze_signal) || null,
    squeeze_proxy_used: Boolean(quote.squeeze_proxy_used || raw.squeeze_proxy_used),
    news_article_count: currentNews,
    liquidityScore: nullableNumber(raw.liquidityScore ?? raw.liquidity_score),
    reasons,
    reason_included: reasons.join(' · ') || raw.catalystReason || raw.catalyst_reason || raw.main_catalyst?.title || '',
    riskFlags,
    risk_flags: riskFlags,
    dataQuality,
    data_quality: dataQuality,
    isFallback: false,
    is_fallback: false,
    prediction_source_label: 'Real Prediction',
    prediction_source_code: 'stored_daily_prediction',
    prediction_source_tone: 'success',
    prediction_threshold_policy: threshold,
    threshold_policy: threshold,
    entry_signal: raw.entry_signal || {
      policy_version: threshold.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
      tier: threshold.tier,
      status: threshold.status,
      passed: Boolean(threshold.passed),
      entry_ready: Boolean(threshold.passed),
      reason: threshold.reason,
    },
    prediction: {
      horizon: '1d',
      requested_horizon: '1d',
      predictedReturn,
      predictedDirection: direction || null,
      confidence,
      probabilityUp: nullableNumber(raw.probability_up ?? prediction.probabilityUp),
      model: raw.modelMode || raw.model_mode || prediction.model || meta.modelMode || 'stored_prediction',
      generatedAt: raw.predictionTimestamp || raw.createdAt || raw.created_at || prediction.generatedAt || meta.createdAt || null,
    },
  }
}

async function loadStoredNextDayPredictionRows(db, quoteRows = []) {
  const quoteByTicker = new Map(quoteRows.map(row => [row.ticker, row]))
  const rows = []
  const seen = new Set()
  const snapshots = await db.collection('daily_prediction_snapshots')
    .find({})
    .sort({ created_at: -1, createdAt: -1, _id: -1 })
    .limit(8)
    .toArray()
    .catch(() => [])

  for (const snapshot of snapshots) {
    const embeddedRows = [
      ...(Array.isArray(snapshot.rows) ? snapshot.rows : []),
      ...(Array.isArray(snapshot.predictions) ? snapshot.predictions : []),
      ...(Array.isArray(snapshot.high_conviction_rows) ? snapshot.high_conviction_rows : []),
    ]
    const meta = {
      predictionDate: snapshot.predictionDate || snapshot.prediction_date || snapshot.date_key || snapshot.prediction_date_key || String(snapshot._id || '').split(':')[0] || null,
      targetDate: snapshot.targetDate || snapshot.target_date || snapshot.predicted_for_date || snapshot.trading_date_predicted_for || null,
      modelMode: snapshot.modelMode || snapshot.model_mode || snapshot.model || 'stored_daily_prediction',
      createdAt: snapshot.createdAt || snapshot.created_at || snapshot.generated_at || null,
    }
    embeddedRows.forEach((raw, index) => {
      const normalized = normalizeStoredPredictionRow(raw, quoteByTicker.get(String(raw?.ticker || '').toUpperCase()) || {}, index, meta)
      if (!normalized) return
      const key = `${normalized.ticker}|${normalized.predictionDate || ''}|${normalized.targetDate || ''}`
      if (seen.has(key)) return
      seen.add(key)
      rows.push(normalized)
    })
  }

  return rows
}

async function loadLivePredictionSignalRows(db, quoteRows = [], limit = 50) {
  const quoteByTicker = new Map(quoteRows.map(row => [row.ticker, row]))
  const sinceSec = Math.floor(Date.now() / 1000) - 36 * 3600
  const docs = await db.collection('prediction_signals')
    .find({ signal_sec: { $gte: sinceSec } })
    .sort({ signal_sec: -1, rank: 1 })
    .limit(Math.max(10, Math.min(200, Number(limit || 50) * 3)))
    .toArray()
    .catch(() => [])
  const seen = new Set()
  const rows = []
  for (const doc of docs) {
    const ticker = String(doc.ticker || '').toUpperCase()
    if (!ticker || seen.has(ticker)) continue
    seen.add(ticker)
    const quote = quoteByTicker.get(ticker) || {}
    const features = doc.features || {}
    const modelSignal = doc.model_signal || {}
    const thresholdRuleSignal = doc.threshold_rule_signal || {}
    const baselineSignal = doc.baseline_signal || {}
    const modelReturn = nullableNumber(
      modelSignal.predicted_return_60m ??
      modelSignal.predicted_return_15m ??
      modelSignal.predicted_return_5m ??
      modelSignal.predicted_return_intraday_trade,
    )
    const thresholdReturn = nullableNumber(
      thresholdRuleSignal.predicted_return_intraday_trade ??
      thresholdRuleSignal.predicted_return_60m ??
      thresholdRuleSignal.predicted_return_15m ??
      thresholdRuleSignal.predicted_return_5m,
    )
    const modelActionable = Boolean(
      modelSignal.entry_ready &&
      (modelSignal.direction === 'up' || modelSignal.direction === 'down') &&
      modelReturn != null
    )
    const thresholdActionable = Boolean(
      thresholdRuleSignal.entry_ready &&
      (thresholdRuleSignal.direction === 'up' || thresholdRuleSignal.direction === 'down') &&
      thresholdReturn != null
    )
    const signal = modelActionable
      ? modelSignal
      : thresholdActionable
        ? thresholdRuleSignal
        : Object.keys(modelSignal).length ? modelSignal : baselineSignal
    const signalSourceLabel = modelActionable
      ? 'Live ML Signal'
      : thresholdActionable
        ? 'Threshold Rule Prediction'
        : 'Live Signal'
    const signalSourceCode = modelActionable
      ? 'prediction_signals_live_ml'
      : thresholdActionable
        ? 'threshold_rule_backtest_expectancy_v3'
        : 'prediction_signals_live_no_daily_archive'
    const rawDirection = String(signal.raw_direction || signal.direction || baselineSignal.raw_direction || '').toLowerCase()
    const entryReady = Boolean(signal.entry_ready || doc.entry_signal?.entry_ready)
    const displayDirection = entryReady && (rawDirection === 'up' || rawDirection === 'down') ? rawDirection : 'watch'
    const confidence = nullableNumber(signal.confidence ?? baselineSignal.confidence)
    const rawPredictedReturn = nullableNumber(
      signal.predicted_return_60m ??
      signal.predicted_return_15m ??
      signal.predicted_return_5m ??
      signal.predicted_return_intraday_trade ??
      features.predicted_return,
    )
    const predictedReturn = entryReady ? rawPredictedReturn : null
    const tradeWatchScore = nullableNumber(doc.trade_watch?.trade_watch_score)
    const finalScore = nullableNumber(doc.final_prediction_score) ?? (tradeWatchScore == null ? null : Number((tradeWatchScore * 100).toFixed(1)))
    const threshold = doc.threshold_policy || evaluatePredictionEntryThreshold({ ...quote, ...features, ticker })
    rows.push({
      ...quote,
      ticker,
      company: doc.company || quote.company || '',
      exchange: doc.exchange || quote.exchange || '',
      sector: doc.sector || quote.sector || '',
      price: nullableFixed(doc.entry_price ?? features.price ?? quote.price, 2),
      change_pct: nullableFixed(features.change_pct ?? quote.change_pct, 2),
      volume: nullableNumber(features.volume ?? quote.volume),
      avg_volume: nullableNumber(quote.avg_volume),
      rel_volume: nullableNumber(features.rel_volume ?? quote.rel_volume),
      market_cap: nullableNumber(features.market_cap ?? quote.market_cap),
      market_cap_bucket: features.market_cap_bucket || quote.market_cap_bucket || '',
      rolling_window_minutes: nullableNumber(doc.social_window_minutes) || 60,
      news_article_count: nullableNumber(features.article_count ?? quote.news_article_count) || 0,
      message_count: nullableNumber(features.social_count ?? quote.message_count) || 0,
      social_message_sentiment: nullableNumber(features.social_sentiment ?? quote.social_message_sentiment) || 0,
      social_message_density: nullableNumber(features.social_density_per_minute ?? quote.social_message_density) || 0,
      structured_sentiment: nullableNumber(features.structured_sentiment ?? features.article_sentiment ?? quote.structured_sentiment) || 0,
      prediction_status: 'live_signal_no_daily_archive',
      prediction_direction: displayDirection,
      predictedDirection: displayDirection,
      predicted_return: predictedReturn,
      predictedReturnPct: predictedReturn,
      final_predicted_percent: predictedReturn,
      raw_prediction_direction: rawDirection || null,
      raw_predicted_return: rawPredictedReturn,
      prediction_confidence: confidence,
      confidence,
      convictionScore: finalScore,
      final_prediction_score: finalScore,
      conviction_score: finalScore,
      high_conviction_rank: null,
      rank: doc.rank || rows.length + 1,
      model_mode: 'live_prediction_signal_no_daily_archive',
      modelMode: 'live_prediction_signal_no_daily_archive',
      predictionTimestamp: doc.signal_at || doc.created_at || null,
      predictionDate: isoDateKey(),
      targetDate: nextTradingDateIso(isoDateKey()),
      catalystReason: doc.decision || '',
      evidenceScore: nullableNumber(features.evidence_score),
      momentumScore: nullableNumber(features.trade_watch_score),
      reasons: compactReasonList([
        doc.decision,
        `${features.social_count || 0} social messages`,
        `${features.article_count || 0} articles`,
      ]),
      reason_included: `${doc.decision || 'Live signal'} · ${features.social_count || 0} social · ${features.article_count || 0} news`,
      riskFlags: entryReady ? [] : ['ENTRY_THRESHOLD_NOT_MET'],
      risk_flags: entryReady ? [] : ['ENTRY_THRESHOLD_NOT_MET'],
      dataQuality: 'live_prediction_signal_not_daily_archive',
      data_quality: 'live_prediction_signal_not_daily_archive',
      isFallback: false,
      is_fallback: false,
      prediction_source_label: signalSourceLabel,
      prediction_source_code: signalSourceCode,
      prediction_source_tone: 'info',
      threshold_rule_signal: thresholdRuleSignal,
      prediction_threshold_policy: threshold,
      threshold_policy: threshold,
      entry_signal: doc.entry_signal || {
        policy_version: threshold.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
        tier: threshold.tier,
        status: threshold.status,
        passed: Boolean(threshold.passed),
        entry_ready: Boolean(threshold.passed),
        reason: threshold.reason,
      },
      prediction: {
        horizon: signal.horizon || 'intraday_signal',
        requested_horizon: '1d',
        predictedReturn,
        predictedDirection: displayDirection,
        confidence,
        model: signal.model || 'prediction_signals_live',
        generatedAt: doc.signal_at || doc.created_at || null,
        rawDirection: rawDirection || null,
        rawPredictedReturn,
        note: signal.note || 'Live prediction signal shown because no current daily next-day archive exists.',
      },
    })
    if (rows.length >= limit) break
  }
  return rows
}

function buildPredictionWatchRows(rows = [], limit = 50) {
  return [...rows]
    .map((row, index) => {
      const threshold = evaluatePredictionEntryThreshold(row)
      const signedChange = nullableNumber(row.change_pct)
      const change = Math.max(0, Number(signedChange || 0))
      const relVolume = Number(row.rel_volume || 0)
      const news = Number(row.news_article_count || 0)
      const social = Number(row.message_count || 0)
      const sentiment = Number(row.avg_sentiment || row.structured_sentiment || row.social_sentiment || 0)
      const squeezeScore = Number(row.short_squeeze_score || 0)
      const watcherCount = Number(row.stocktwits_watcher_count || 0)
      const catalystText = row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || ''
      const setupStatus = row.threshold_setup_status || threshold.setupStatus || threshold.status
      const validation = predictionEvidenceValidation(row, {
        catalystText,
        news,
        social,
        sentiment,
        change: signedChange,
        relVolume,
        catalystPower: nullableNumber(row.catalyst_power_score) || 0,
        squeezeScore,
        watcherCount,
        floatShort: nullableNumber(row.float_short),
        setupStatus,
      })
      const catalystReaction = predictionCatalystReactionState(row, threshold, validation, {
        change: signedChange,
        setupStatus,
        sessionContext: row.catalyst_session_context || marketSessionContext(),
      })
      if (!validation.valid || catalystReaction.rejection) return null
      const catalystQuality = catalystQualityAssessment(row, validation, {
        catalystText,
        news,
        sentiment,
        catalystPower: nullableNumber(row.catalyst_power_score) || 0,
      })
      const volumeScore = Math.min(35, Math.log1p(Math.max(0, relVolume)) * 12)
      const moveScore = Math.min(16, change * 0.6)
      const evidenceScore = Math.min(25, Math.log1p(news + social) * 5)
      const squeezeBoost = Math.min(20, squeezeScore * 0.2)
      const sentimentScore = Math.max(-8, Math.min(10, sentiment * 10))
      const setupBoost = setupStatus === 'entry_passed'
        ? 45
        : setupStatus === 'active_setup_already_above_threshold'
          ? 0
          : setupStatus === 'near_threshold_setup'
            ? 7
            : 0
      const watchScore = Number(Math.min(100, volumeScore + moveScore + evidenceScore + sentimentScore + setupBoost + squeezeBoost).toFixed(1))
      const momentumCompositeScore = Number(Math.min(100, ((moveScore + volumeScore) / 65) * 100).toFixed(1))
      const correlationScore = nullableNumber(threshold.correlation ?? row.price_density_correlation ?? row.correlation_score)
      const secFilingUsed = Boolean(row.sec_filing_contributed || Number(row.filing_used_count || 0) > 0 || row.main_catalyst?.isSecFiling)
      const sourceLabel = setupStatus === 'entry_passed'
        ? 'Entry Gate Passed'
        : setupStatus === 'active_setup_already_above_threshold'
        ? 'Active Setup'
        : setupStatus === 'near_threshold_setup'
          ? 'Near Threshold'
          : 'Watch Candidate'
      const setupReason = threshold.setupReason || threshold.reason
      const reasons = [
        Number(row.change_pct || 0) ? `current move ${Number(row.change_pct || 0).toFixed(2)}%` : '',
        relVolume ? `${relVolume.toFixed(2)}x relative volume` : '',
        news ? `${news} news item${news === 1 ? '' : 's'}` : '',
        social ? `${social} social mention${social === 1 ? '' : 's'}` : '',
        watcherCount ? `${watcherCount.toLocaleString()} Stocktwits watchers` : '',
        squeezeScore ? `squeeze interest ${squeezeScore.toFixed(0)}/100` : '',
        setupStatus === 'active_setup_already_above_threshold' ? 'active density/price setup, waiting for a fresh entry cross' : '',
        setupStatus === 'near_threshold_setup' ? 'near density/price entry threshold' : '',
      ].filter(Boolean)
      const riskFlags = [
        'NO_STORED_NEXT_DAY_PREDICTION',
        ...(threshold.passed ? ['NO_PREDICTED_RETURN_ATTACHED'] : ['NO_FRESH_ENTRY_SIGNAL']),
        ...(setupStatus === 'active_setup_already_above_threshold' ? ['ALREADY_ABOVE_CORRELATION_THRESHOLD'] : []),
        ...(setupStatus === 'late_setup_rejected' || threshold.status === 'late_entry_rejected' ? ['LATE_ENTRY_REJECTED'] : []),
        ...(news ? [] : ['NO_CATALYST']),
        ...(news && catalystQuality.tier === 'weak' ? ['WEAK_CATALYST_QUALITY'] : []),
        ...(news && catalystQuality.tier === 'reject' ? ['REJECTED_CATALYST_QUALITY'] : []),
        ...(social || watcherCount ? [] : ['NO_SOCIAL_CONFIRMATION']),
        ...(PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && validation.recognizedNewsCatalyst && catalystReactionNeedsOhlcBlock(catalystReaction) ? ['NO_CATALYST_PRICE_REACTION_OHLC'] : []),
        ...(catalystReaction.state === 'validated_but_watch_closely' ? ['CATALYST_REACTION_NEEDS_CLOSE_MONITORING'] : []),
      ]
      const aiEvidence = buildAiEvidenceScore({
        ...row,
        prediction_validation: validation,
        catalyst_quality: catalystQuality,
        catalyst_quality_score: catalystQuality.score,
        catalyst_reaction_summary: catalystReaction,
        risk_flags: riskFlags,
        final_prediction_score: watchScore,
        momentum_score: momentumCompositeScore,
        correlation_score: correlationScore,
      }, { finalScore: watchScore })
      const aiCompositeScore = aiEvidence.score
      let freshTriggerState = predictionFreshTriggerState({
        ...row,
        risk_flags: riskFlags,
        entry_signal: {
          status: threshold.status,
          setup_status: setupStatus,
          passed: Boolean(threshold.passed),
        },
        threshold_policy: threshold,
      }, validation)
      const pendingOpenConfirmation = pendingOpenConfirmationState({
        ...row,
        risk_flags: riskFlags,
      }, validation, catalystQuality, catalystReaction, {
        relVolume,
        social,
      })
      for (const reason of pendingOpenConfirmation.blocked_reasons) {
        if (!riskFlags.includes(reason)) riskFlags.push(reason)
      }
      freshTriggerState = predictionFreshTriggerState({
        ...row,
        risk_flags: riskFlags,
        entry_signal: {
          status: threshold.status,
          setup_status: setupStatus,
          passed: Boolean(threshold.passed),
        },
        threshold_policy: threshold,
      }, validation)
      const readiness = predictionReadinessState({
        ...row,
        risk_flags: riskFlags,
      }, validation, freshTriggerState, catalystReaction)
      const decisionReason = [
        readiness.label ? `tier ${readiness.label}` : '',
        catalystQuality.score != null ? `catalyst ${catalystQuality.tier} ${catalystQuality.score}/100 (${catalystQuality.class})` : '',
        pendingOpenConfirmation.is_pending_open ? `pending open confirmation: ${pendingOpenConfirmation.passes ? pendingOpenConfirmation.support_reasons.join(', ') || 'passed' : pendingOpenConfirmation.blocked_reasons.join(', ')}` : '',
        readiness.reaction?.label ? `reaction ${readiness.reaction.label}` : '',
        riskFlags.length ? `risks ${riskFlags.slice(0, 4).join(', ')}` : '',
      ].filter(Boolean).join(' · ')
      return {
        ...row,
        prediction_status: 'fallback_candidate',
        prediction_direction: null,
        predictedDirection: null,
        predicted_return: null,
        predictedReturnPct: null,
        final_predicted_percent: null,
        prediction_confidence: null,
        confidence: null,
        convictionScore: null,
        final_prediction_score: watchScore,
        watchScore,
        watch_score: watchScore,
        rank: index + 1,
        high_conviction_rank: null,
        model_mode: 'no_stored_next_day_prediction',
        modelMode: 'no_stored_next_day_prediction',
        isFallback: true,
        is_fallback: true,
        fallbackReason: `${sourceLabel}: no stored next-day prediction exists for this row; threshold diagnostics are shown without fabricating a prediction.`,
        fallback_reason: `${sourceLabel}: no stored next-day prediction exists for this row; threshold diagnostics are shown without fabricating a prediction.`,
        fallback_confidence: null,
        fallback_prediction_direction: 'watch',
        prediction_source_label: sourceLabel,
        prediction_source_code: 'screener_watch_candidate_no_stored_prediction',
        prediction_source_tone: setupStatus === 'active_setup_already_above_threshold' ? 'info' : 'warning',
        prediction_validation_status: 'watch_validated',
        prediction_validation: { ...validation, catalystReaction },
        prediction_catalyst_reaction: catalystReaction,
        catalyst_reaction_summary: readiness.reaction,
        catalyst_reaction_state: catalystReaction.state,
        fresh_prediction_trigger: freshTriggerState,
        prediction_trade_ready: readiness.trade_ready,
        prediction_readiness: readiness,
        prediction_readiness_level: readiness.level,
        prediction_readiness_label: readiness.label,
        prediction_readiness_tone: readiness.tone,
        prediction_waiting_for: readiness.waiting_for,
        prediction_blocked_reasons: readiness.blocked_reasons,
        prediction_tier: readiness.level,
        prediction_decision_reason: decisionReason,
        catalyst_quality: catalystQuality,
        catalyst_quality_score: catalystQuality.score,
        catalyst_quality_tier: catalystQuality.tier,
        pending_open_confirmation: pendingOpenConfirmation,
        pending_open_confirmed: pendingOpenConfirmation.is_pending_open ? pendingOpenConfirmation.passes : null,
        evidence_score: Number((evidenceScore + squeezeBoost).toFixed(1)),
        social_score: Number((Math.min(16, Math.log1p(Math.max(0, social)) * 4) + squeezeBoost).toFixed(1)),
        momentum_score: momentumCompositeScore,
        ai_score: aiCompositeScore,
        ai_score_components: aiEvidence.components,
        ai_score_reasons: aiEvidence.reasons,
        ai_context: {
          ai_score: aiCompositeScore,
          ai_confidence: aiEvidence.confidence,
          ai_article_count: news,
          model: aiEvidence.model,
        },
        correlation_score: correlationScore,
        correlation_context: {
          source: threshold.correlation != null ? 'threshold_policy' : row.price_density_correlation != null ? 'screeners.threshold_features' : 'missing',
          current: correlationScore,
          previous: nullableNumber(threshold.previousCorrelation ?? row.previous_price_density_correlation),
          status: setupStatus,
          updated_at: row.threshold_feature_updated_at || null,
        },
        sec_filing_contributed: secFilingUsed,
        filing_used_count: row.filing_used_count ?? (secFilingUsed ? 1 : 0),
        filing_sentiment: row.filing_sentiment ?? 0,
        prediction_threshold_policy: threshold,
        threshold_policy: threshold,
        entry_signal: {
          policy_version: threshold.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
          tier: threshold.tier,
          status: threshold.status,
          setup_status: setupStatus,
          setup_score: row.threshold_setup_score ?? threshold.setupScore ?? null,
          setup_ready: Boolean(threshold.setupReady),
          passed: Boolean(threshold.passed),
          entry_ready: Boolean(threshold.passed),
          reason: threshold.reason,
          setup_reason: setupReason,
        },
        reason_included: reasons.join(' · ') || 'Current screener row; no stored prediction attached.',
        reason_included_detail: decisionReason,
        catalyst_summary: news ? `${news} recent news item${news === 1 ? '' : 's'}` : 'No catalyst attached',
        risk_flags: riskFlags,
        riskFlags,
        dataQuality: 'fallback_watch_candidate',
        data_quality: 'fallback_watch_candidate',
        prediction_debug: {
          watch_score: watchScore,
          fallback_reason: 'No stored next-day prediction exists.',
          screener_row_rank: index + 1,
          evidence_score: evidenceScore,
          momentum_score: momentumCompositeScore,
          volume_score: volumeScore,
          ai_score: aiCompositeScore,
          sentiment_score: sentimentScore,
          correlation_available: correlationScore != null,
          raw_correlation_score: correlationScore,
          correlation_source: threshold.correlation != null ? 'threshold_policy' : row.price_density_correlation != null ? 'screeners.threshold_features' : 'missing',
          message_density_trend: row.message_density_trend || null,
          message_density_score: row.message_density_score ?? null,
          squeeze_score_boost: squeezeBoost,
          short_squeeze_score: squeezeScore || null,
          short_squeeze_available: row.short_squeeze_available ?? null,
          short_squeeze_reason: row.short_squeeze_reason || null,
          squeeze_signal: row.squeeze_signal || null,
          stocktwits_watcher_count: watcherCount || null,
          setup_score_boost: setupBoost,
          setup_status: setupStatus,
          setup_reason: setupReason,
          threshold_status: threshold.status,
          threshold_policy_version: threshold.policyVersion,
          validation,
          catalyst_reaction: catalystReaction,
          catalyst_reaction_summary: readiness.reaction,
          catalyst_quality: catalystQuality,
          pending_open_confirmation: pendingOpenConfirmation,
          prediction_decision_reason: decisionReason,
          fresh_prediction_trigger: freshTriggerState,
          prediction_readiness: readiness,
          prediction_blocked_reasons: readiness.blocked_reasons,
        },
      }
    })
    .filter(Boolean)
    .sort((a, b) => {
      const priority = row => {
        const status = row.entry_signal?.setup_status || row.threshold_setup_status || ''
        if (status === 'entry_passed') return 4
        if (status === 'active_setup_already_above_threshold') return 3
        if (status === 'near_threshold_setup') return 2
        return 1
      }
      const displayDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
      if (displayDiff !== 0) return displayDiff
      return priority(b) - priority(a) || Number(b.watchScore || 0) - Number(a.watchScore || 0)
    })
    .slice(0, Math.max(1, Math.min(200, Number(limit || 50))))
}

function buildPeopleMomentumDevelopingRows(rows = [], limit = 50) {
  return [...rows]
    .map((row, index) => {
      const change = nullableNumber(row.change_pct)
      const relVolume = nullableNumber(row.rel_volume) || 0
      const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
      const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
      const peopleAttention = predictionPeopleAttention(row, {
        social: nullableNumber(row.message_count),
        sentiment,
        watcherCount,
      })
      if (change == null || change <= 0 || !peopleAttention.active) return null
      const catalystText = row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || ''
      if (isBearishCatalystText([catalystText, row.main_catalyst?.event_type, row.main_catalyst?.sentiment].filter(Boolean).join(' '))) return null
      const momentumOk = relVolume >= 1.2 || change >= 2 || peopleAttention.strong
      if (!momentumOk) return null

      const volumeScore = Math.min(24, Math.log1p(Math.max(0, relVolume)) * 7)
      const moveScore = Math.min(22, Math.max(0, change) * 0.5)
      const peopleScore = Math.min(34, Math.log1p(Math.max(0, peopleAttention.messageCount)) * 5 + Math.min(12, peopleAttention.densityScore / 6))
      const sentimentScore = Math.max(-8, Math.min(10, sentiment * 12))
      const watcherScore = watcherRankScore(row)
      const score = Number(Math.max(0, Math.min(100, volumeScore + moveScore + peopleScore + sentimentScore + watcherScore)).toFixed(1))
      if (score < PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE) return null

      const scoreStrength = Math.max(0, Math.min(1, score / 100))
      const relVolumeLift = Math.min(1.2, Math.log1p(Math.max(0, relVolume)) / 4)
      const peopleLift = Math.min(1.1, Math.log1p(Math.max(0, peopleAttention.messageCount)) / 5)
      const sentimentLift = Math.max(-0.35, Math.min(0.45, sentiment * 0.8))
      const extensionMultiplier = change >= 80
        ? 0.35
        : change >= 40
          ? 0.5
          : change >= 20
            ? 0.7
            : 1
      const developingExpectedReturn = Number(Math.max(0.25, Math.min(6.5,
        (0.5 + scoreStrength * 2.2 + relVolumeLift + peopleLift + sentimentLift) * extensionMultiplier
      )).toFixed(2))
      const developingConfidence = Number(Math.max(0.35, Math.min(0.72,
        0.32 + score / 250 + (peopleAttention.strong ? 0.08 : 0) + (row.main_catalyst ? 0.04 : 0) - (change >= 35 ? 0.08 : 0)
      )).toFixed(3))
      const developingProbabilityUp = Number(Math.max(0.51, Math.min(0.68,
        0.5 + score / 600 + (sentiment > 0 ? sentiment / 10 : 0) - (change >= 35 ? 0.04 : 0)
      )).toFixed(3))
      const developingPayoffProbability = Number(Math.max(0.35, Math.min(0.82,
        0.38 + score / 250 + Math.min(relVolume, 30) / 200 + (peopleAttention.strong ? 0.06 : 0) + (row.main_catalyst ? 0.03 : 0) - (change >= 50 ? 0.12 : change >= 30 ? 0.07 : 0)
      )).toFixed(3))
      const riskFlags = [
        'DEVELOPING_PEOPLE_MOMENTUM_NOT_HIGH_CONVICTION',
        'DEVELOPING_ESTIMATE_NOT_STRICT_PAYOFF_MODEL',
        !row.main_catalyst ? 'NO_NEWS_CATALYST_ATTACHED' : '',
        relVolume < 1.2 ? 'LOW_RELATIVE_VOLUME_CONFIRMATION' : '',
      ].filter(Boolean)
      const reason = [
        `${change.toFixed(2)}% current move`,
        `${relVolume.toFixed(2)}x relative volume`,
        peopleAttention.reasons.slice(0, 3).join(', '),
      ].filter(Boolean).join(' · ')

      return withPredictionSetupClassification({
        ...row,
        prediction_status: 'developing_people_momentum',
        prediction_pool_role: 'developing_people_momentum',
        prediction_source_label: 'Developing: People Momentum',
        prediction_source_code: 'developing_people_momentum',
        prediction_source_tone: 'info',
        prediction_direction: 'up',
        predictedDirection: 'up',
        predicted_return: developingExpectedReturn,
        predictedReturnPct: developingExpectedReturn,
        final_predicted_percent: developingExpectedReturn,
        predicted_return_target: 'developing_people_momentum_continuation_estimate',
        prediction_return_basis: 'current_people_momentum_score_not_strict_high_conviction',
        probability_up: developingProbabilityUp,
        payoff_model_probability: developingPayoffProbability,
        payoff_model_threshold: null,
        payoff_model_passes: null,
        confidence: developingConfidence,
        prediction_confidence: developingConfidence,
        final_prediction_score: score,
        convictionScore: score,
        watchScore: score,
        watch_score: score,
        rank: index + 1,
        high_conviction: false,
        high_conviction_rank: null,
        people_attention: peopleAttention,
        prediction_validation: {
          valid: true,
          primary: 'people',
          labels: ['live people/message attention'],
          reason: 'live people/message attention',
          recognizedPeopleAttention: true,
          recognizedSocialCatalyst: peopleAttention.bullish,
          peopleAttention,
        },
        prediction_readiness_level: 'developing_people_momentum',
        prediction_readiness_label: 'People Momentum',
        prediction_readiness_tone: 'info',
        prediction_trade_ready: false,
        reason_included: reason,
        reason_included_detail: `Developing people-backed mover; estimated continuation ${developingExpectedReturn.toFixed(2)}% from live momentum, relative volume, and people attention. Not high conviction until payoff/fresh-density gates confirm. ${reason}`,
        risk_flags: riskFlags,
        riskFlags,
        social_score: Number(peopleScore.toFixed(1)),
        momentum_score: Number((volumeScore + moveScore).toFixed(1)),
        evidence_score: Number((peopleScore + sentimentScore).toFixed(1)),
        ai_score: Number(Math.max(0, Math.min(100, peopleScore + sentimentScore)).toFixed(1)),
        prediction: {
          horizon: 'developing',
          predictionSession: marketSessionContext().session,
          prediction_session: marketSessionContext().session,
          predictionTarget: 'continuation_watch',
          prediction_target: 'continuation_watch',
          predictedReturn: developingExpectedReturn,
          predictedDirection: 'up',
          predictedReturnTarget: 'developing_people_momentum_continuation_estimate',
          returnBasis: 'current_people_momentum_score_not_strict_high_conviction',
          probabilityUp: developingProbabilityUp,
          confidence: developingConfidence,
          model: 'developing_people_momentum_fast_path_v2',
          generatedAt: new Date().toISOString(),
          note: 'Developing estimate only. It is computed from current move, relative volume, people/message attention, sentiment, and at most 2 points of causal watcher-growth confirmation; strict high-conviction still requires payoff/fresh-density confirmation.',
        },
        prediction_debug: {
          people_attention: peopleAttention,
          volume_score: Number(volumeScore.toFixed(1)),
          move_score: Number(moveScore.toFixed(1)),
          people_score: Number(peopleScore.toFixed(1)),
          sentiment_score: Number(sentimentScore.toFixed(1)),
          watcher_score: Number(watcherScore.toFixed(1)),
          developing_expected_return_pct: developingExpectedReturn,
          developing_probability_up: developingProbabilityUp,
          developing_payoff_probability: developingPayoffProbability,
          developing_confidence: developingConfidence,
          note: 'Developing-only people momentum estimate; not a strict high-conviction prediction.',
        },
      })
    })
    .filter(Boolean)
    .sort((a, b) => Number(b.final_prediction_score || 0) - Number(a.final_prediction_score || 0))
    .slice(0, Math.max(1, Math.min(200, Number(limit || 50))))
}

function buildMomentumCatalystDevelopingRows(rows = [], limit = 75) {
  return [...rows]
    .map((row, index) => {
      const change = nullableNumber(row.change_pct)
      if (change == null || change <= 0) return null
      const relVolume = nullableNumber(row.rel_volume) || 0
      const news = Number(row.news_article_count || row.article_count || 0)
      const social = Number(row.message_count || row.stocktwits_message_count || 0)
      const sentiment = nullableNumber(row.avg_sentiment ?? row.sentiment ?? row.structured_sentiment ?? row.social_sentiment) || 0
      const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
      const watcherScore = watcherRankScore(row)
      const peopleAttention = predictionPeopleAttention(row, { social, sentiment, watcherCount })
      const catalystText = row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || ''
      const sourceText = [row.main_catalyst?.source, Array.isArray(row.sources) ? row.sources.join(' ') : row.sources, row.source].filter(Boolean).join(' ')
      const weakCatalyst = catalystText ? isWeakGenericCatalystText(catalystText) : false
      const bearishCatalyst = isBearishCatalystText([catalystText, row.main_catalyst?.event_type, row.main_catalyst?.sentiment].filter(Boolean).join(' '))
      if (bearishCatalyst) return null

      const hasRecognizedNews = news > 0 && !weakCatalyst
      const sourceRecognized = isRecognizedCatalystSource(sourceText) || news > 0
      const hasMarketConfirmation = relVolume >= 2.5 || change >= 8
      const hasPeopleConfirmation = social >= 3 || peopleAttention.active || watcherScore > 0
      const hasEvidence = hasRecognizedNews || hasPeopleConfirmation || hasMarketConfirmation
      if (!hasEvidence) return null

      const catalystQuality = catalystQualityAssessment(row, {
        recognizedNewsCatalyst: hasRecognizedNews,
        materialDirectCatalyst: hasRecognizedNews && sourceRecognized,
        peopleAttention,
      }, {
        catalystText,
        news,
        sentiment,
        catalystPower: nullableNumber(row.catalyst_power_score) || 0,
      })
      const newsScore = hasRecognizedNews ? Math.min(24, 9 + Math.log1p(news) * 5 + Math.max(0, catalystQuality.score || 0) / 10) : 0
      const socialScore = Math.min(24, Math.log1p(Math.max(0, social)) * 4 + Math.min(10, peopleAttention.densityScore / 5) + Math.max(0, watcherScore))
      const volumeScore = Math.min(24, Math.log1p(Math.max(0, relVolume)) * 7)
      const moveScore = Math.min(20, Math.max(0, change) * 0.35)
      const sentimentScore = Math.max(-8, Math.min(8, sentiment * 10))
      const noCatalystPenalty = hasRecognizedNews ? 0 : -10
      const extensionPenalty = change >= 80 ? -16 : change >= 50 ? -12 : change >= 30 ? -7 : 0
      const score = Number(Math.max(0, Math.min(100, newsScore + socialScore + volumeScore + moveScore + sentimentScore + noCatalystPenalty + extensionPenalty)).toFixed(1))
      if (score < Math.max(32, PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE - 10)) return null

      const extendedMove = change >= 30
      const strongEvidence = (hasRecognizedNews && hasMarketConfirmation) || (hasPeopleConfirmation && hasMarketConfirmation)
      const expectedReturn = Number(Math.max(0.25, Math.min(6,
        (0.45 + score / 32 + Math.min(1.2, Math.log1p(relVolume) / 4) + (hasRecognizedNews ? 0.45 : 0) + (hasPeopleConfirmation ? 0.35 : 0)) *
        (extendedMove ? 0.55 : 1)
      )).toFixed(2))
      const confidence = Number(Math.max(0.34, Math.min(0.72,
        0.34 + score / 260 + (hasRecognizedNews ? 0.07 : 0) + (hasPeopleConfirmation ? 0.05 : 0) - (extendedMove ? 0.07 : 0)
      )).toFixed(3))
      const probabilityUp = Number(Math.max(0.51, Math.min(0.69,
        0.51 + score / 650 + (sentiment > 0 ? Math.min(0.03, sentiment / 12) : 0) - (extendedMove ? 0.035 : 0)
      )).toFixed(3))
      const payoffProbability = Number(Math.max(0.36, Math.min(0.8,
        0.37 + score / 250 + Math.min(0.12, relVolume / 160) + (strongEvidence ? 0.05 : 0) - (extendedMove ? 0.08 : 0)
      )).toFixed(3))
      const validation = {
        valid: true,
        primary: hasRecognizedNews ? 'news' : hasPeopleConfirmation ? 'people' : 'market',
        recognizedNewsCatalyst: hasRecognizedNews,
        materialDirectCatalyst: hasRecognizedNews && sourceRecognized,
        recognizedPeopleAttention: hasPeopleConfirmation,
        recognizedSocialCatalyst: hasPeopleConfirmation && social > 0,
        recognizedDensitySetup: false,
        peopleAttention,
        reason: hasRecognizedNews
          ? 'fresh mover with ticker-linked news/catalyst evidence'
          : hasPeopleConfirmation
            ? 'positive mover with live people/social confirmation'
            : 'positive mover with unusual volume confirmation',
        labels: [
          hasRecognizedNews ? 'ticker-linked news/catalyst' : '',
          hasPeopleConfirmation ? 'people/social confirmation' : '',
          hasMarketConfirmation ? 'market/volume confirmation' : '',
        ].filter(Boolean),
      }
      const riskFlags = [
        'DEVELOPING_MOMENTUM_WATCH_NOT_HIGH_CONVICTION',
        'DEVELOPING_ESTIMATE_NOT_STRICT_PAYOFF_MODEL',
        hasRecognizedNews ? '' : 'NO_DIRECT_CATALYST_CONFIRMED',
        hasPeopleConfirmation ? '' : 'LOW_OR_MISSING_SOCIAL_CONFIRMATION',
        extendedMove ? 'EXTENDED_MOVE_CONTINUATION_RISK' : '',
        weakCatalyst ? 'WEAK_CATALYST_QUALITY' : '',
      ].filter(Boolean)
      const reasons = [
        `${change.toFixed(2)}% current move`,
        `${relVolume.toFixed(2)}x relative volume`,
        hasRecognizedNews ? `${news} ticker-linked news item${news === 1 ? '' : 's'}` : '',
        social ? `${social} social mention${social === 1 ? '' : 's'}` : '',
        watcherCount ? `${watcherCount.toLocaleString()} watchers` : '',
      ].filter(Boolean)

      return withPredictionSetupClassification({
        ...row,
        prediction_status: 'developing_momentum_catalyst_watch',
        prediction_pool_role: hasRecognizedNews ? 'developing_catalyst_momentum' : 'developing_market_momentum',
        prediction_source_label: hasRecognizedNews ? 'Developing: Catalyst Momentum' : 'Developing: Momentum Watch',
        prediction_source_code: hasRecognizedNews ? 'developing_catalyst_momentum' : 'developing_market_momentum',
        prediction_source_tone: hasRecognizedNews ? 'info' : 'warning',
        prediction_direction: 'up',
        predictedDirection: 'up',
        predicted_return: expectedReturn,
        predictedReturnPct: expectedReturn,
        final_predicted_percent: expectedReturn,
        predicted_return_target: 'developing_continuation_estimate',
        prediction_return_basis: 'current_price_volume_catalyst_people_watch_not_strict_high_conviction',
        probability_up: probabilityUp,
        payoff_model_probability: payoffProbability,
        payoff_model_threshold: null,
        payoff_model_passes: null,
        confidence,
        prediction_confidence: confidence,
        final_prediction_score: score,
        convictionScore: score,
        watchScore: score,
        watch_score: score,
        rank: index + 1,
        high_conviction: false,
        high_conviction_rank: null,
        people_attention: peopleAttention,
        prediction_validation: validation,
        prediction_readiness_level: hasRecognizedNews ? 'developing_catalyst_momentum' : 'developing_market_momentum',
        prediction_readiness_label: hasRecognizedNews ? 'Catalyst Momentum' : 'Momentum Watch',
        prediction_readiness_tone: hasRecognizedNews ? 'info' : 'warning',
        prediction_trade_ready: false,
        reason_included: reasons.join(' · '),
        reason_included_detail: `Developing watch candidate; estimated continuation ${expectedReturn.toFixed(2)}% from current move, relative volume, ticker-linked catalyst/social evidence, and risk penalties. Not high conviction until payoff/fresh-density gates confirm.`,
        risk_flags: riskFlags,
        riskFlags,
        catalyst_quality: catalystQuality,
        catalyst_quality_score: catalystQuality.score,
        catalyst_quality_tier: catalystQuality.tier,
        evidence_score: Number((newsScore + socialScore).toFixed(1)),
        social_score: Number(socialScore.toFixed(1)),
        momentum_score: Number((volumeScore + moveScore).toFixed(1)),
        ai_score: Number(Math.max(0, Math.min(100, newsScore + socialScore + sentimentScore)).toFixed(1)),
        prediction: {
          horizon: 'developing',
          predictionSession: marketSessionContext().session,
          prediction_session: marketSessionContext().session,
          predictionTarget: 'continuation_watch',
          prediction_target: 'continuation_watch',
          predictedReturn: expectedReturn,
          predictedDirection: 'up',
          predictedReturnTarget: 'developing_continuation_estimate',
          returnBasis: 'current_price_volume_catalyst_people_watch_not_strict_high_conviction',
          probabilityUp,
          confidence,
          model: 'developing_momentum_catalyst_watch_v1',
          generatedAt: new Date().toISOString(),
          note: 'Developing-only watch estimate. Requires strict payoff/fresh-density confirmation before high-conviction promotion.',
        },
        prediction_debug: {
          news_score: Number(newsScore.toFixed(1)),
          social_score: Number(socialScore.toFixed(1)),
          volume_score: Number(volumeScore.toFixed(1)),
          move_score: Number(moveScore.toFixed(1)),
          sentiment_score: Number(sentimentScore.toFixed(1)),
          watcher_score: Number(watcherScore.toFixed(1)),
          extension_penalty: extensionPenalty,
          no_catalyst_penalty: noCatalystPenalty,
          catalyst_quality: catalystQuality,
          validation,
          note: 'Developing-only momentum/catalyst estimate; not a strict high-conviction prediction.',
        },
      })
    })
    .filter(Boolean)
    .sort((a, b) => {
      const sourceDiff = Number(hasDevelopingCatalystEvidence(b)) - Number(hasDevelopingCatalystEvidence(a))
      if (sourceDiff !== 0) return sourceDiff
      const scoreDiff = Number(b.final_prediction_score || 0) - Number(a.final_prediction_score || 0)
      if (scoreDiff !== 0) return scoreDiff
      return Number(b.rel_volume || 0) - Number(a.rel_volume || 0)
    })
    .slice(0, Math.max(1, Math.min(200, Number(limit || 75))))
}

function buildEvidencePredictionRows(rows = [], limit = 50, meta = {}) {
  const backtest = predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY.candidateRule?.backtestSummary || {}
  const baseExpectedReturn = Number(backtest.meanNetReturnPct)
  const baseWinRate = Number(backtest.winRate)
  const modelDoc = meta.modelDoc || null
  const modelMetrics = modelDoc?.metrics?.selected || {}
  const productionAvgReturn = nullableNumber(modelMetrics.avg_labeled_return)
  const productionAccuracy = nullableNumber(modelMetrics.accuracy)
  const productionThreshold = nullableNumber(modelDoc?.selected_threshold ?? modelMetrics.threshold)
  const trainedHighTarget = Boolean(modelDoc?.live_enabled && modelDoc?.status === 'trained_production' && modelDoc?.target === 'high' && productionAvgReturn != null && productionAvgReturn > 0)
  const trainedPayoffTarget = Boolean(modelDoc?.live_enabled && modelDoc?.status === 'trained_production' && String(modelDoc?.target || '').includes('payoff') && productionThreshold != null)
  const predictionDate = meta.predictionDate || isoDateKey()
  const targetDate = meta.targetDate || nextTradingDateIso(predictionDate)

  return [...rows]
    .map((row, index) => {
      const ticker = String(row.ticker || '').toUpperCase()
      const threshold = evaluatePredictionEntryThreshold(row)
      const change = nullableNumber(row.change_pct)
      const price = nullableNumber(row.price)
      const relVolume = nullableNumber(row.rel_volume)
      const news = nullableNumber(row.news_article_count ?? row.article_count)
      const social = nullableNumber(row.message_count)
      const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
      const socialDensity = nullableNumber(row.social_message_density)
      const tradeWatchScore = nullableNumber(row.trade_watch?.trade_watch_score)
      const catalystPower = nullableNumber(row.catalyst_power_score) || 0
      const squeezeScore = nullableNumber(row.short_squeeze_score) || 0
      const watcherCount = nullableNumber(row.stocktwits_watcher_count) || 0
      const floatShort = nullableNumber(row.float_short)
      const shortInterestPct = nullableNumber(row.short_interest_pct ?? row.short_interest_pct_shares_out ?? row.short_interest_pct_float)
      const sessionContext = row.catalyst_session_context || marketSessionContext()
      const catalystText = row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || ''
      const setupStatus = row.threshold_setup_status || threshold.setupStatus || threshold.status
      const missingFields = [
        price == null ? 'price' : '',
        change == null ? 'change_pct' : '',
        relVolume == null ? 'rel_volume' : '',
        news == null ? 'news_article_count' : '',
        social == null ? 'message_count' : '',
        sentiment == null ? 'sentiment' : '',
      ].filter(Boolean)

      if (!ticker || price == null || change == null || relVolume == null || relVolume <= 0) return null

      const validation = predictionEvidenceValidation(row, {
        catalystText,
        news,
        social,
        sentiment,
        change,
        relVolume,
        catalystPower,
        squeezeScore,
        watcherCount,
        floatShort,
        setupStatus,
      })
      if (!validation.valid || !hasPrimaryPredictionCatalyst(validation, row)) return null
      const catalystReaction = predictionCatalystReactionState(row, threshold, validation, { change, setupStatus, sessionContext })
      if (catalystReaction.rejection) return null
      const peopleAttention = validation.peopleAttention || predictionPeopleAttention(row, { social, sentiment, watcherCount })
      const catalystAgeMinutes = nullableNumber(validation.catalystAgeMinutes)
      const staleNewsWithoutPeople = Boolean(
        validation.recognizedNewsCatalyst &&
        catalystAgeMinutes != null &&
        catalystAgeMinutes > PREDICTION_ACTIONABLE_CATALYST_MAX_AGE_MINUTES &&
        !peopleAttention.active
      )
      const catalystQuality = catalystQualityAssessment(row, validation, {
        catalystText,
        news,
        sentiment,
        catalystPower,
      })
      const climberGate = predictionClimberGate(row, validation, {
        change,
        relVolume,
        social,
        sentiment,
        squeezeScore,
        watcherCount,
        setupStatus,
      })
      if (!climberGate.passes) return null

      const hasCatalyst = validation.recognizedNewsCatalyst
      const hasSocialSupport = peopleAttention.active
      const hasPositiveSentiment = sentiment != null && sentiment > 0.05
      const hasThresholdSupport = ['entry_passed', 'active_setup_already_above_threshold', 'near_threshold_setup'].includes(setupStatus)
      const hasSqueezeSupport = squeezeScore >= 55 || (floatShort != null && floatShort >= 10)

      const volumeScore = Math.min(18, Math.log1p(Math.max(0, relVolume)) * 6.5)
      const moveScore = Math.min(12, Math.max(0, change) * 0.55)
      const newsScore = news == null ? 0 : Math.min(14, Math.log1p(Math.max(0, news)) * 5)
      const catalystPowerScore = Math.min(22, catalystPower * 3.2)
      const socialScore = social == null ? 0 : Math.min(16, Math.log1p(Math.max(0, Math.max(social, peopleAttention.messageCount))) * 4)
      const sentimentScore = sentiment == null
        ? 0
        : Math.max(-12, Math.min(14, sentiment * 18))
      const densityScore = socialDensity == null ? 0 : Math.min(8, Math.max(0, socialDensity) * 12)
      const tradeScore = tradeWatchScore == null ? 0 : Math.min(12, Math.max(0, tradeWatchScore) * 12)
      const squeezeScoreComponent = Math.min(14, squeezeScore * 0.14)
      const watcherScore = watcherRankScore(row)
      const setupBoost = setupStatus === 'entry_passed'
        ? 18
        : setupStatus === 'active_setup_already_above_threshold'
          ? 12
          : setupStatus === 'near_threshold_setup'
            ? 7
            : 0
      const sessionCarryBoost = ['weekend', 'premarket'].includes(String(sessionContext.session || '').toLowerCase()) && Number(row.catalyst_window_article_count || 0) > 0 ? 4 : 0
      const catalystBoost = hasCatalyst ? 6 + sessionCarryBoost : 0
      const missingPenalty = Math.min(12, missingFields.length * 2)
      const rawExtensionPenalty = change >= 100
        ? 16
        : change >= 60
          ? 12
          : change >= 35
            ? 8
            : change >= 20
              ? 4
              : 0
      const extensionRelief = validation.recognizedSqueezeCatalyst || validation.recognizedNewsCatalyst || validation.recognizedDensitySetup
        ? Math.min(rawExtensionPenalty, 6)
        : 0
      const extensionPenalty = Math.max(0, rawExtensionPenalty - extensionRelief)
      const staleCatalystPenalty = staleNewsWithoutPeople ? 18 : 0
      const finalScore = Number(Math.max(0, Math.min(100,
        volumeScore + moveScore + newsScore + catalystPowerScore + socialScore + sentimentScore + densityScore + tradeScore + setupBoost + catalystBoost + squeezeScoreComponent + watcherScore - missingPenalty - extensionPenalty - staleCatalystPenalty
      )).toFixed(1))

      if (finalScore < Number(meta.minEvidenceScore || 40)) return null

      const scoreReturnBoost = Math.max(0, (finalScore - 40) / 60)
      const scoreStrength = Math.max(0, Math.min(1, (finalScore - 40) / 60))
      const validationReturnLift = validation.recognizedNewsCatalyst
        ? 0.04
        : validation.recognizedSqueezeCatalyst
          ? 0.06
          : validation.recognizedDensitySetup
            ? 0.035
            : 0
      const catalystReturnLift = Math.min(0.18, catalystPower / 55 + validationReturnLift)
      const moveReturnLift = Math.min(0.10, Math.log1p(Math.max(0, change)) / 28)
      const volumeReturnLift = Math.min(0.11, Math.log1p(Math.max(0, relVolume)) / 36)
      const squeezeReturnLift = Math.min(0.10, squeezeScore / 900 + watcherScore / 180)
      const missingDataPenalty = Math.max(0.65, 1 - missingFields.length * 0.08)
      const uncappedExpectedReturn = trainedHighTarget
        ? Number(Math.max(0.25, Math.min(18, productionAvgReturn * (0.35 + scoreStrength * 0.5 + catalystReturnLift + moveReturnLift + volumeReturnLift + squeezeReturnLift) * missingDataPenalty)).toFixed(3))
        : Number.isFinite(baseExpectedReturn)
          ? Number(Math.max(0.15, Math.min(4.5, baseExpectedReturn * (0.75 + scoreReturnBoost))).toFixed(3))
          : null
      const socialOnlyUnverifiedSqueeze = validation.recognizedSqueezeCatalyst && !validation.verifiedShortInterest && !validation.recognizedNewsCatalyst
      const effectiveMaxReturn = socialOnlyUnverifiedSqueeze
        ? Math.min(climberGate.highUpsideMaxReturnPct, 6.5)
        : climberGate.highUpsideMaxReturnPct
      const expectedReturn = uncappedExpectedReturn == null
        ? null
        : Number(Math.min(uncappedExpectedReturn, effectiveMaxReturn).toFixed(3))
      if (expectedReturn == null) return null

      const momentumCompositeScore = Number(Math.min(100, ((moveScore + volumeScore + tradeScore) / 42) * 100).toFixed(1))
      const correlationScore = nullableNumber(threshold.correlation ?? row.price_density_correlation ?? row.correlation_score)
      const secFilingUsed = Boolean(row.sec_filing_contributed || Number(row.filing_used_count || 0) > 0 || row.main_catalyst?.isSecFiling)
      const probabilityUp = productionAccuracy != null
        ? Number(Math.max(0.51, Math.min(0.82, productionAccuracy - 0.08 + scoreStrength * 0.14)).toFixed(3))
        : Number.isFinite(baseWinRate)
          ? Number(Math.max(0.51, Math.min(0.72, baseWinRate + (finalScore - 50) / 500)).toFixed(3))
          : null
      const confidence = probabilityUp == null
        ? Number(Math.max(0.35, Math.min(0.75, finalScore / 100)).toFixed(3))
        : Number(Math.max(0.35, Math.min(0.82, 0.35 + Math.abs(probabilityUp - 0.5) * 1.8 + finalScore / 300)).toFixed(3))
      const reasons = [
        `${change.toFixed(2)}% current move`,
        `${relVolume.toFixed(2)}x relative volume`,
        news != null && news > 0 ? `${news} recent news item${news === 1 ? '' : 's'}` : '',
        peopleAttention.active ? `people attention: ${peopleAttention.reasons.slice(0, 3).join(', ')}` : '',
        watcherCount ? `${watcherCount.toLocaleString()} Stocktwits watchers` : '',
        squeezeScore ? `squeeze interest ${squeezeScore.toFixed(0)}/100` : '',
        floatShort != null ? `${floatShort.toFixed(1)}% float short` : '',
        sentiment != null && Math.abs(sentiment) > 0.05 ? `${sentiment > 0 ? 'positive' : 'negative'} sentiment ${sentiment.toFixed(2)}` : '',
        hasThresholdSupport ? `density threshold ${setupStatus.replaceAll('_', ' ')}` : '',
        validation.reason ? `validated: ${validation.reason}` : '',
      ].filter(Boolean)
      const riskFlags = [
        ...(!hasCatalyst ? ['NO_NEWS_CATALYST_ATTACHED'] : []),
        ...(hasCatalyst && catalystQuality.tier === 'weak' ? ['WEAK_CATALYST_QUALITY'] : []),
        ...(hasCatalyst && catalystQuality.tier === 'reject' ? ['REJECTED_CATALYST_QUALITY'] : []),
        ...(!hasSocialSupport && !hasSqueezeSupport ? ['LOW_OR_MISSING_SOCIAL_CONFIRMATION'] : []),
        ...(staleNewsWithoutPeople ? ['STALE_NEWS_WITHOUT_CURRENT_PEOPLE_ATTENTION'] : []),
        ...(sentiment != null && sentiment < -0.05 ? ['NEGATIVE_SENTIMENT_HEADWIND'] : []),
        ...(!hasThresholdSupport ? ['NO_FRESH_DENSITY_ENTRY_CROSS'] : []),
        ...(change >= 35 ? ['EXTENDED_MOVE_REQUIRES_FRESH_VALIDATION'] : []),
        ...(PREDICTION_REQUIRE_UNAFFECTED_AFTER_HOURS_CATALYST && validation.recognizedNewsCatalyst && catalystReactionNeedsOhlcBlock(catalystReaction) ? ['NO_CATALYST_PRICE_REACTION_OHLC'] : []),
        ...(catalystReaction.state === 'validated_but_watch_closely' ? ['CATALYST_REACTION_NEEDS_CLOSE_MONITORING'] : []),
        ...climberGate.riskFlags,
        ...(socialOnlyUnverifiedSqueeze ? ['UNVERIFIED_SOCIAL_ONLY_SQUEEZE_CAPPED'] : []),
        ...(uncappedExpectedReturn != null && expectedReturn < uncappedExpectedReturn ? [`TIER_RETURN_CAPPED_${effectiveMaxReturn}%`] : []),
        ...validation.riskFlags,
        ...missingFields.map(field => `MISSING_${field.toUpperCase()}`),
      ]
      const aiEvidence = buildAiEvidenceScore({
        ...row,
        prediction_validation: validation,
        catalyst_quality: catalystQuality,
        catalyst_quality_score: catalystQuality.score,
        catalyst_reaction_summary: catalystReaction,
        risk_flags: riskFlags,
        final_prediction_score: finalScore,
        momentum_score: momentumCompositeScore,
        correlation_score: correlationScore,
        sec_filing_contributed: secFilingUsed,
      }, { finalScore })
      const aiCompositeScore = aiEvidence.score
      const payoffModelRow = {
        ...row,
        final_prediction_score: finalScore,
        convictionScore: finalScore,
        prediction_confidence: confidence,
        confidence,
        probability_up: probabilityUp,
        predicted_return: expectedReturn,
        predictedReturnPct: expectedReturn,
        change_pct: change,
        rel_volume: relVolume,
        ai_score: aiCompositeScore,
        ai_score_components: aiEvidence.components,
        ai_score_reasons: aiEvidence.reasons,
        ai_context: {
          ai_score: aiCompositeScore,
          ai_confidence: aiEvidence.confidence,
          ai_article_count: news ?? 0,
          model: aiEvidence.model,
        },
        momentum_score: momentumCompositeScore,
        correlation_score: correlationScore,
        catalyst_power_score: catalystPower,
        catalyst_window_article_count: row.catalyst_window_article_count ?? news ?? 0,
        risk_flags: riskFlags,
        prediction_validation: validation,
        people_attention: peopleAttention,
        catalyst_session_context: sessionContext,
        prediction_session: sessionContext.session,
        prediction: {
          ...(row.prediction || {}),
          predictionSession: sessionContext.session,
          confidence,
          probabilityUp,
          predictedReturn: expectedReturn,
        },
      }
      const payoffModelProbability = predictionModelProbability(payoffModelRow, modelDoc)
      const payoffModelPasses = trainedPayoffTarget && payoffModelProbability != null && productionThreshold != null
        ? payoffModelProbability >= productionThreshold
        : null
      if (payoffModelPasses === false) riskFlags.push('BELOW_PAYOFF_MODEL_THRESHOLD')
      const pendingOpenConfirmation = pendingOpenConfirmationState({
        ...payoffModelRow,
        risk_flags: riskFlags,
        payoff_model_probability: payoffModelProbability,
        payoff_model_threshold: trainedPayoffTarget ? productionThreshold : null,
      }, validation, catalystQuality, catalystReaction, {
        payoffModelProbability,
        payoffThreshold: trainedPayoffTarget ? productionThreshold : null,
        relVolume,
        social,
      })
      const pendingOpenOverrideSupport = pendingOpenConfirmation.support_reasons.some(reason => reason !== 'strong catalyst quality')
      const pendingOpenPayoffOverride = Boolean(
        payoffModelPasses === false &&
        pendingOpenConfirmation.is_pending_open &&
        pendingOpenConfirmation.passes &&
        catalystQuality.tier === 'strong' &&
        pendingOpenOverrideSupport
      )
      for (const reason of pendingOpenConfirmation.blocked_reasons) {
        if (!riskFlags.includes(reason)) riskFlags.push(reason)
      }
      const freshTriggerState = predictionFreshTriggerState({
        ...payoffModelRow,
        risk_flags: riskFlags,
        payoff_model_passes: pendingOpenPayoffOverride ? null : payoffModelPasses,
        payoff_model_probability: payoffModelProbability,
        payoff_model_threshold: trainedPayoffTarget ? productionThreshold : null,
      }, validation)
      for (const reason of freshTriggerState.blockedReasons) {
        if (!riskFlags.includes(reason)) riskFlags.push(reason)
      }
      const readiness = predictionReadinessState({
        ...payoffModelRow,
        risk_flags: riskFlags,
        payoff_model_passes: pendingOpenPayoffOverride ? null : payoffModelPasses,
      }, validation, freshTriggerState, catalystReaction)
      const decisionReason = [
        readiness.label ? `tier ${readiness.label}` : '',
        catalystQuality.score != null ? `catalyst ${catalystQuality.tier} ${catalystQuality.score}/100 (${catalystQuality.class})` : '',
        pendingOpenConfirmation.is_pending_open ? `pending open confirmation: ${pendingOpenConfirmation.passes ? pendingOpenConfirmation.support_reasons.join(', ') || 'passed' : pendingOpenConfirmation.blocked_reasons.join(', ')}` : '',
        pendingOpenPayoffOverride ? 'payoff override only for pending-open; not high conviction' : '',
        readiness.reaction?.label ? `reaction ${readiness.reaction.label}` : '',
        freshTriggerState.freshDensityCross ? 'fresh density cross' : '',
        payoffModelProbability != null ? `payoff ${Math.round(payoffModelProbability * 100)}%${productionThreshold != null ? ` vs ${Math.round(productionThreshold * 100)}% min` : ''}` : '',
        riskFlags.length ? `risks ${riskFlags.slice(0, 4).join(', ')}` : 'no major blockers',
      ].filter(Boolean).join(' · ')

      return withDiscoveryTier({
        ...row,
        ticker,
        prediction_status: 'evidence_prediction',
        prediction_direction: 'up',
        predictedDirection: 'up',
        predicted_return: expectedReturn,
        predictedReturnPct: expectedReturn,
        final_predicted_percent: expectedReturn,
        uncapped_predicted_return: uncappedExpectedReturn,
        predicted_return_target: trainedHighTarget ? 'next_session_high' : 'message_density_trade_expectancy',
        prediction_return_basis: trainedHighTarget ? 'trained_production_high_target_temporal_holdout' : 'message_density_threshold_backtest',
        probability_up: probabilityUp,
        payoff_model_probability: payoffModelProbability,
        payoff_model_threshold: trainedPayoffTarget ? productionThreshold : null,
        payoff_model_passes: payoffModelPasses,
        pending_open_payoff_override: pendingOpenPayoffOverride,
        fresh_prediction_trigger: freshTriggerState,
        prediction_trade_ready: readiness.trade_ready,
        prediction_readiness: readiness,
        prediction_readiness_level: readiness.level,
        prediction_readiness_label: readiness.label,
        prediction_readiness_tone: readiness.tone,
        prediction_waiting_for: readiness.waiting_for,
        prediction_blocked_reasons: readiness.blocked_reasons,
        prediction_tier: readiness.level,
        prediction_decision_reason: decisionReason,
        catalyst_quality: catalystQuality,
        catalyst_quality_score: catalystQuality.score,
        catalyst_quality_tier: catalystQuality.tier,
        pending_open_confirmation: pendingOpenConfirmation,
        pending_open_confirmed: pendingOpenConfirmation.is_pending_open ? pendingOpenConfirmation.passes : null,
        prediction_confidence: confidence,
        confidence,
        convictionScore: finalScore,
        final_prediction_score: finalScore,
        conviction_score: finalScore,
        high_conviction_rank: null,
        rank: index + 1,
        model_mode: 'validated_evidence_next_session_candidate_v2',
        modelMode: 'validated_evidence_next_session_candidate_v2',
        predictionTimestamp: new Date().toISOString(),
        predictionDate,
        targetDate,
        catalystReason: catalystText || (Number(news || 0) > 0 ? `${news} recent news item${news === 1 ? '' : 's'}` : ''),
        evidenceScore: Number((newsScore + catalystPowerScore + socialScore + sentimentScore + catalystBoost + squeezeScoreComponent + watcherScore).toFixed(1)),
        evidence_score: Number((newsScore + catalystPowerScore + socialScore + sentimentScore + catalystBoost + squeezeScoreComponent + watcherScore).toFixed(1)),
        catalystScore: Number((newsScore + catalystPowerScore + catalystBoost).toFixed(1)),
        newsScore: Number((newsScore + catalystPowerScore + catalystBoost).toFixed(1)),
        news_score: Number((newsScore + catalystPowerScore + catalystBoost).toFixed(1)),
        catalyst_power_score: catalystPower,
        main_catalyst: row.main_catalyst || null,
        catalysts: Array.isArray(row.catalysts) ? row.catalysts : [],
        catalyst_session_context: sessionContext,
        socialScore: Number((socialScore + densityScore + watcherScore).toFixed(1)),
        social_score: Number((socialScore + densityScore + watcherScore).toFixed(1)),
        squeezeScore: squeezeScore || null,
        short_squeeze_score: squeezeScore || null,
        stocktwits_watcher_count: watcherCount || null,
        momentumScore: Number((moveScore + volumeScore + tradeScore).toFixed(1)),
        momentum_score: momentumCompositeScore,
        technicalScore: setupBoost,
        ai_score: aiCompositeScore,
        ai_score_components: aiEvidence.components,
        ai_score_reasons: aiEvidence.reasons,
        ai_context: {
          ai_score: aiCompositeScore,
          ai_confidence: aiEvidence.confidence,
          ai_article_count: news ?? 0,
          model: aiEvidence.model,
        },
        correlation_score: correlationScore,
        correlation_context: {
          source: threshold.correlation != null ? 'threshold_policy' : row.price_density_correlation != null ? 'screeners.threshold_features' : 'missing',
          current: correlationScore,
          previous: nullableNumber(threshold.previousCorrelation ?? row.previous_price_density_correlation),
          status: setupStatus,
          updated_at: row.threshold_feature_updated_at || null,
        },
        sec_filing_contributed: secFilingUsed,
        filing_used_count: row.filing_used_count ?? (secFilingUsed ? 1 : 0),
        filing_sentiment: row.filing_sentiment ?? 0,
        reasons,
        reason_included: reasons.join(' · '),
        reason_included_detail: decisionReason,
        riskFlags,
        risk_flags: riskFlags,
        validated_prediction: true,
        prediction_validation_status: 'validated',
        prediction_validation_reason: validation.reason,
        prediction_validation: { ...validation, catalystReaction },
        prediction_catalyst_reaction: catalystReaction,
        catalyst_reaction_summary: readiness.reaction,
        catalyst_reaction_state: catalystReaction.state,
        prediction_climber_gate: { ...climberGate, effectiveMaxReturnPct: effectiveMaxReturn, socialOnlyUnverifiedSqueeze },
        missing_prediction_fields: missingFields,
        dataQuality: missingFields.length ? 'evidence_prediction_partial_inputs' : 'evidence_prediction_complete_inputs',
        data_quality: missingFields.length ? 'evidence_prediction_partial_inputs' : 'evidence_prediction_complete_inputs',
        isFallback: false,
        is_fallback: false,
        prediction_source_label: 'Validated Evidence Prediction',
        prediction_source_code: 'validated_evidence_next_session_candidate_v2',
        prediction_source_tone: 'success',
        prediction_threshold_policy: threshold,
        threshold_policy: threshold,
        entry_signal: {
          policy_version: threshold.policyVersion || PREDICTION_THRESHOLD_POLICY_VERSION,
          tier: threshold.tier,
          status: threshold.status,
          setup_status: setupStatus,
          setup_score: row.threshold_setup_score ?? threshold.setupScore ?? null,
          setup_ready: Boolean(threshold.setupReady || hasThresholdSupport),
          passed: Boolean(threshold.passed),
          entry_ready: Boolean(threshold.passed || hasThresholdSupport),
          reason: threshold.reason,
          setup_reason: threshold.setupReason || threshold.reason,
        },
        prediction: {
          horizon: 'next_session',
          requested_horizon: '1d',
          predictionSession: sessionContext.session,
          prediction_session: sessionContext.session,
          predictionTarget: sessionContext.session === 'premarket' || sessionContext.session === 'regular' ? 'current_regular_session' : 'next_regular_session',
          prediction_target: sessionContext.session === 'premarket' || sessionContext.session === 'regular' ? 'current_regular_session' : 'next_regular_session',
          evidenceWindowStartSec: sessionContext.catalyst_window_start_sec,
          evidence_window_start_sec: sessionContext.catalyst_window_start_sec,
          evidenceWindowEndSec: sessionContext.catalyst_window_end_sec,
          evidence_window_end_sec: sessionContext.catalyst_window_end_sec,
          catalystWindowPolicy: sessionContext.catalyst_window_policy,
          predictedReturn: expectedReturn,
          predictedDirection: 'up',
          predictedReturnTarget: trainedHighTarget ? 'next_session_high' : 'message_density_trade_expectancy',
          returnBasis: trainedHighTarget ? 'trained_production_high_target_temporal_holdout' : 'message_density_threshold_backtest',
          probabilityUp,
          confidence,
          model: trainedHighTarget ? 'trained_production_high_target_bridge_v1' : 'validated_evidence_next_session_candidate_v2',
          generatedAt: new Date().toISOString(),
          note: trainedHighTarget
            ? 'Validated evidence-ranked next-session candidate sized from the trained production next-session high-target model. A current move alone is not enough; the row must carry a recognized catalyst, squeeze/social-interest evidence, or a confirmed message-density setup.'
            : 'Validated evidence-ranked next-session candidate using live screener momentum, relative volume, social/message density, sentiment, catalysts/news, and the promoted message-density threshold backtest. A current move alone is not enough.',
        },
        prediction_debug: {
          evidence_model: trainedHighTarget ? 'trained_production_high_target_bridge_v1' : 'validated_evidence_next_session_candidate_v2',
          validation,
          catalyst_reaction: catalystReaction,
          score_components: {
            volumeScore,
            moveScore,
            newsScore,
            catalystPowerScore,
            socialScore,
            sentimentScore,
            densityScore,
            tradeScore,
            squeezeScoreComponent,
            watcherScore,
            setupBoost,
            catalystBoost,
            missingPenalty,
            extensionPenalty,
          },
          backtest_expected_return_pct: Number.isFinite(baseExpectedReturn) ? baseExpectedReturn : null,
          backtest_win_rate: Number.isFinite(baseWinRate) ? baseWinRate : null,
          production_avg_labeled_return_pct: trainedHighTarget ? productionAvgReturn : null,
          production_accuracy: productionAccuracy,
          production_selected_threshold: productionThreshold,
          payoff_model_probability: payoffModelProbability,
          payoff_model_threshold: trainedPayoffTarget ? productionThreshold : null,
          payoff_model_passes: payoffModelPasses,
          pending_open_payoff_override: pendingOpenPayoffOverride,
          fresh_prediction_trigger: freshTriggerState,
          prediction_readiness: readiness,
          catalyst_reaction_summary: readiness.reaction,
          catalyst_quality: catalystQuality,
          pending_open_confirmation: pendingOpenConfirmation,
          prediction_decision_reason: decisionReason,
          prediction_blocked_reasons: readiness.blocked_reasons,
          prediction_return_basis: trainedHighTarget ? 'trained_production_high_target_temporal_holdout' : 'message_density_threshold_backtest',
          momentum_score: momentumCompositeScore,
          ai_score: aiCompositeScore,
          ai_score_components: aiEvidence.components,
          ai_score_reasons: aiEvidence.reasons,
          ai_confidence: aiEvidence.confidence,
          ai_article_count: news ?? 0,
          correlation_available: correlationScore != null,
          raw_correlation_score: correlationScore,
          correlation_source: threshold.correlation != null ? 'threshold_policy' : row.price_density_correlation != null ? 'screeners.threshold_features' : 'missing',
          message_density_trend: row.message_density_trend || null,
          message_density_score: row.message_density_score ?? null,
          message_density_5m: row.message_density_5m ?? null,
          message_density_15m: row.message_density_15m ?? null,
          message_density_30m: row.message_density_30m ?? null,
          message_density_60m: row.message_density_60m ?? null,
          message_density_supported: row.message_density_supported ?? null,
          message_density_rising: row.message_density_rising ?? null,
          short_squeeze_score: squeezeScore || null,
          short_squeeze_available: row.short_squeeze_available ?? null,
          short_squeeze_reason: row.short_squeeze_reason || null,
          squeeze_signal: row.squeeze_signal || null,
          missingFields,
        },
      })
    })
    .filter(Boolean)
    .sort((a, b) => {
      const displayDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
      if (displayDiff !== 0) return displayDiff
      const scoreDiff = Number(b.final_prediction_score || 0) - Number(a.final_prediction_score || 0)
      if (scoreDiff !== 0) return scoreDiff
      const confDiff = Number(b.confidence || 0) - Number(a.confidence || 0)
      if (confDiff !== 0) return confDiff
      return Number(b.change_pct || 0) - Number(a.change_pct || 0)
    })
    .slice(0, Math.max(1, Math.min(200, Number(limit || 50))))
}

async function persistPredictionArchiveStatus(db, payload = {}) {
  const predictionDate = isoDateKey()
  const targetDate = nextTradingDateIso(predictionDate)
  const doc = {
    _id: `${predictionDate}:1d:status`,
    created_at: new Date(),
    updated_at: new Date(),
    predictionDate,
    targetDate,
    horizon: '1d',
    archive_status: payload.realRows?.length ? 'stored_predictions_available' : 'no_stored_predictions_watch_candidates_only',
    rowCount: payload.realRows?.length || 0,
    fallbackRows: (payload.fallbackRows || []).slice(0, 50).map(row => ({
      ticker: row.ticker,
      company: row.company,
      currentPrice: row.price,
      currentChangePct: row.change_pct,
      watchScore: row.watchScore,
      fallbackReason: row.fallbackReason,
      reasons: compactReasonList([row.reason_included]),
      riskFlags: row.risk_flags || [],
      entry_signal: row.entry_signal || null,
      threshold_policy: row.threshold_policy || null,
      sourceSnapshotIds: [],
    })),
    metadata: payload.metadata || {},
    note: payload.realRows?.length
      ? 'Stored next-day prediction rows were available.'
      : 'No stored next-day predictions were available; only watch candidates were archived separately.',
  }
  await db.collection('daily_prediction_snapshots').updateOne(
    { _id: doc._id },
    { $set: doc },
    { upsert: true },
  ).catch(() => {})
  return doc
}

function toFiniteNumber(value) {
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function applyMirrorFilters(rows = [], query = {}) {
  let out = [...rows]
  const q = String(query.search || query.q || '').trim().toLowerCase()
  if (q) {
    const exactTicker = /^[a-z][a-z0-9.-]{0,7}$/i.test(q) ? q.toUpperCase() : ''
    out = out.filter(row => {
      if (exactTicker) return String(row.ticker || '').toUpperCase() === exactTicker
      return String(row.ticker || '').toLowerCase().includes(q) ||
        String(row.company || '').toLowerCase().includes(q)
    })
  }

  const eq = (field, value) => {
    const wanted = String(value || '').trim().toLowerCase()
    if (!wanted) return
    out = out.filter(row => String(row[field] || '').trim().toLowerCase() === wanted)
  }
  eq('exchange', query.exchange)
  eq('sector', query.sector)
  eq('industry', query.industry)
  eq('country', query.country)
  eq('market_cap_bucket', query.market_cap_bucket)
  eq('session', query.session)
  eq('quote_source', query.quote_source)

  if (query.source) {
    const wanted = String(query.source || '').trim().toLowerCase()
    out = out.filter(row => {
      const sources = Array.isArray(row.sources) ? row.sources : String(row.sources || '').split(',')
      return sources.some(source => String(source || '').trim().toLowerCase() === wanted)
    })
  }

  if (query.catalyst) {
    const wanted = String(query.catalyst || '').trim().toLowerCase()
    out = out.filter(row => [
      row.main_catalyst,
      row.catalyst,
      row.catalyst_summary,
      row.structured_catalyst_type,
      row.structured_catalyst,
      row.event_type,
    ].some(value => String(value || '').toLowerCase().includes(wanted)))
  }

  if (query.market_cap) {
    const bucket = String(query.market_cap).toLowerCase()
    out = out.filter(row => {
      const cap = Number(row.market_cap)
      if (!Number.isFinite(cap)) return false
      if (bucket === 'micro' || bucket === 'nano') return cap > 0 && cap < 300e6
      if (bucket === 'small') return cap >= 300e6 && cap < 2e9
      if (bucket === 'mid') return cap >= 2e9 && cap < 10e9
      if (bucket === 'large') return cap >= 10e9 && cap < 200e9
      if (bucket === 'mega') return cap >= 200e9
      return true
    })
  }

  const minMax = (field, minKey, maxKey) => {
    const min = toFiniteNumber(query[minKey])
    const max = toFiniteNumber(query[maxKey])
    if (min == null && max == null) return
    out = out.filter(row => {
      const n = toFiniteNumber(row[field])
      if (n == null) return false
      if (min != null && n < min) return false
      if (max != null && n > max) return false
      return true
    })
  }
  minMax('price', 'price_min', 'price_max')
  minMax('change_pct', 'change_min', 'change_max')
  minMax('rel_volume', 'rel_volume_min', 'rel_volume_max')
  minMax('volume', 'volume_min', 'volume_max')
  minMax('avg_volume', 'avg_volume_min', 'avg_volume_max')
  minMax('float', 'float_min', 'float_max')
  minMax('float_short', 'short_float_min', 'short_float_max')
  minMax('market_cap', 'market_cap_min', 'market_cap_max')
  minMax('rsi', 'rsi_min', 'rsi_max')
  minMax('gap', 'gap_min', 'gap_max')
  minMax('dollar_volume', 'dollar_volume_min', 'dollar_volume_max')
  minMax('ai_score', 'ai_score_min', 'ai_score_max')
  minMax('prediction_confidence', 'prediction_confidence_min', 'prediction_confidence_max')
  minMax('predicted_return', 'predicted_return_min', 'predicted_return_max')

  if (query.price_range) {
    const range = String(query.price_range).toLowerCase()
    const ranges = {
      under1: [null, 1], under5: [null, 5], under10: [null, 10], under20: [null, 20],
      one_to_twenty: [1, 20], over5: [5, null], over10: [10, null], over20: [20, null],
      over50: [50, null], over100: [100, null],
    }
    const [min, max] = ranges[range] || []
    if (min != null || max != null) out = out.filter(row => {
      const p = toFiniteNumber(row.price)
      if (p == null) return false
      return (min == null || p >= min) && (max == null || p <= max)
    })
  }

  const signal = String(query.signal || '').toLowerCase()
  const includeStaleQuotes = ['1', 'true', 'yes'].includes(String(query.include_stale || '').toLowerCase())
  const quoteIsCurrentEnough = (row) => {
    if (includeStaleQuotes) return true
    const freshness = String(row.quote_freshness || '').toLowerCase()
    if (freshness === 'very_stale' || freshness === 'missing') return false
    const age = Number(row.quote_age_seconds)
    return !Number.isFinite(age) || age <= 45 * 60
  }
  if (signal === 'top_gainers') out = out.filter(row => quoteIsCurrentEnough(row) && Number(row.change_pct) > 0)
  if (signal === 'top_losers') out = out.filter(row => quoteIsCurrentEnough(row) && Number(row.change_pct) < 0)
  if (signal === 'most_active') out = out.filter(row => quoteIsCurrentEnough(row) && Number(row.volume) > 0)
  if (signal === 'unusual_volume') out = out.filter(row => quoteIsCurrentEnough(row) && Number(row.rel_volume) >= Number(query.unusual_volume_min || 2))
  if (signal === 'most_volatile') out = out.filter(row => quoteIsCurrentEnough(row) && Number.isFinite(Number(row.change_pct)))

  if (query.news_available) {
    const yes = String(query.news_available).toLowerCase() === 'yes'
    out = out.filter(row => (Number(row.news_article_count || 0) > 0) === yes)
  }
  if (query.social_available) {
    const yes = String(query.social_available).toLowerCase() === 'yes'
    out = out.filter(row => (Number(row.message_count || 0) > 0) === yes)
  }
  if (query.sentiment) {
    const sent = String(query.sentiment).toLowerCase()
    out = out.filter(row => {
      const n = Number(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
      if (!Number.isFinite(n)) return false
      if (sent === 'bullish') return n >= 0.2
      if (sent === 'bearish') return n <= -0.2
      if (sent === 'neutral') return n > -0.2 && n < 0.2
      return true
    })
  }
  if (query.prediction_direction) {
    const wanted = String(query.prediction_direction).toLowerCase()
    out = out.filter(row => String(row.prediction_direction || row.predictedDirection || row.fallback_prediction_direction || '').toLowerCase() === wanted)
  }
  if (query.decision_journey) {
    const yes = String(query.decision_journey).toLowerCase() === 'yes'
    out = out.filter(row => Boolean(row.path_points_count || row.ticker_path?.length || row.path_quality) === yes)
  }

  const orderBy = String(query.orderBy || 'change_pct')
  const orderDir = String(query.orderDir || 'desc').toLowerCase() === 'asc' ? 1 : -1
  const signalSort = signal === 'top_losers'
    ? (a, b) => Number(a.change_pct || 0) - Number(b.change_pct || 0)
    : signal === 'most_active'
      ? (a, b) => Number(b.volume || 0) - Number(a.volume || 0)
      : signal === 'most_volatile'
        ? (a, b) => Math.abs(Number(b.change_pct || 0)) - Math.abs(Number(a.change_pct || 0))
        : null
  out.sort(signalSort || ((a, b) => {
    const av = a[orderBy]
    const bv = b[orderBy]
    const an = av == null || av === ''
    const bn = bv == null || bv === ''
    if (an && bn) return 0
    if (an) return 1
    if (bn) return -1
    if (typeof av === 'string') return orderDir * av.localeCompare(String(bv || ''))
    return orderDir * (Number(av || 0) - Number(bv || 0))
  }))

  return out
}

function buildMirrorFilterMetadata(rows = []) {
  const facet = (field, limit = 80) => {
    const map = new Map()
    for (const row of rows) {
      const value = String(row[field] || '').trim()
      if (!value) continue
      map.set(value, (map.get(value) || 0) + 1)
    }
    return Array.from(map.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, limit)
      .map(([value, count]) => ({ value, label: value, count }))
  }
  const listFacet = (field, limit = 80) => {
    const map = new Map()
    for (const row of rows) {
      const values = Array.isArray(row[field]) ? row[field] : String(row[field] || '').split(',')
      for (const raw of values) {
        const value = String(raw || '').trim()
        if (!value) continue
        map.set(value, (map.get(value) || 0) + 1)
      }
    }
    return Array.from(map.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, limit)
      .map(([value, count]) => ({ value, label: value, count }))
  }
  const range = (field) => {
    const values = rows.map(row => Number(row[field])).filter(Number.isFinite)
    if (!values.length) return null
    return { min: Math.min(...values), max: Math.max(...values) }
  }
  const timestampMs = (value) => {
    if (value == null || value === '') return 0
    const numeric = Number(value)
    if (Number.isFinite(numeric)) return numeric > 1_000_000_000_000 ? numeric : numeric * 1000
    const parsed = Date.parse(value)
    return Number.isFinite(parsed) ? parsed : 0
  }
  return {
    supportedSignals: [
      { value: 'top_gainers', label: 'Top Gainers' },
      { value: 'top_losers', label: 'Top Losers' },
      { value: 'most_active', label: 'Most Active' },
      { value: 'unusual_volume', label: 'Unusual Volume' },
      { value: 'most_volatile', label: 'Most Volatile' },
    ],
    facets: {
      exchange: facet('exchange'),
      sector: facet('sector'),
      industry: facet('industry'),
      country: facet('country'),
      session: facet('session'),
      market_cap_bucket: facet('market_cap_bucket'),
      quote_source: facet('quote_source'),
      sources: listFacet('sources'),
      catalyst: facet('structured_catalyst_type'),
    },
    ranges: {
      price: range('price'),
      change_pct: range('change_pct'),
      rel_volume: range('rel_volume'),
      volume: range('volume'),
      avg_volume: range('avg_volume'),
      market_cap: range('market_cap'),
      float: range('float'),
      short_float: range('float_short'),
      rsi: range('rsi'),
      gap: range('gap'),
    },
    sourceFreshness: {
      latest_quote: rows.reduce((max, row) => Math.max(max, timestampMs(row.quote_updated_at || row.finviz_seen_at || row.tradingview_seen_at)), 0) || null,
    },
    capabilities: {
      has_news_counts: rows.some(row => row.news_article_count != null),
      has_social_counts: rows.some(row => row.message_count != null),
      has_decision_journey: rows.some(row => Boolean(row.path_points_count || row.ticker_path?.length || row.path_quality)),
      has_catalyst_text: rows.some(row => Boolean(row.main_catalyst || row.catalyst || row.catalyst_summary || row.structured_catalyst_type || row.structured_catalyst || row.event_type)),
    },
  }
}

function signalQuoteIsCurrentEnough(row = {}, includeStaleQuotes = false) {
  if (includeStaleQuotes) return true
  const freshness = String(row.quote_freshness || '').toLowerCase()
  if (freshness === 'very_stale' || freshness === 'missing') return false
  const age = Number(row.quote_age_seconds)
  return !Number.isFinite(age) || age <= 45 * 60
}

function applySignalFilter(rows = [], signal = '', query = {}) {
  const mode = String(signal || '').toLowerCase()
  if (!mode) return rows
  const includeStaleQuotes = ['1', 'true', 'yes'].includes(String(query.include_stale || '').toLowerCase())
  if (mode === 'top_gainers') return rows.filter(row => signalQuoteIsCurrentEnough(row, includeStaleQuotes) && Number(row.change_pct) > 0)
  if (mode === 'top_losers') return rows.filter(row => signalQuoteIsCurrentEnough(row, includeStaleQuotes) && Number(row.change_pct) < 0)
  if (mode === 'most_active') return rows.filter(row => signalQuoteIsCurrentEnough(row, includeStaleQuotes) && Number(row.volume) > 0)
  if (mode === 'unusual_volume') return rows.filter(row => signalQuoteIsCurrentEnough(row, includeStaleQuotes) && Number(row.rel_volume) >= Number(query.unusual_volume_min || 2))
  if (mode === 'most_volatile') return rows.filter(row => signalQuoteIsCurrentEnough(row, includeStaleQuotes) && Number.isFinite(Number(row.change_pct)))
  return rows
}

function auditIsoTimestamp(value) {
  const sec = timestampSeconds(value)
  return sec ? new Date(sec * 1000).toISOString() : null
}

function auditTickerValues(article = {}) {
  const fields = [
    ['ticker', article.ticker],
    ['tickers', article.tickers],
    ['matched_ticker', article.matched_ticker],
    ['matched_tickers', article.matched_tickers],
    ['matched_mover_tickers', article.matched_mover_tickers],
    ['symbols', article.symbols],
  ]
  const values = []
  for (const [field, raw] of fields) {
    const entries = Array.isArray(raw) ? raw : String(raw || '').split(/[;,|\s]+/)
    for (const entry of entries) {
      const value = String(entry || '').replace(/^\$/, '').trim().toUpperCase()
      if (value) values.push({ field, value })
    }
  }
  return values
}

function auditArticleMatch(article = {}, row = {}) {
  const ticker = String(row.ticker || '').toUpperCase()
  const tickerValues = auditTickerValues(article)
  const exactFields = tickerValues.filter(item => item.value === ticker).map(item => item.field)
  const title = String(article.title || article.headline || article.summary || article.description || '')
  const companyMatch = catalystMentionsTickerOrCompany(row, title)
  const reasons = exactFields.map(field => `exact_${field}_match`)
  if (!exactFields.length && companyMatch) reasons.push('company_name_in_headline_or_summary')
  return {
    matched: reasons.length > 0,
    reasons,
    ticker_fields: exactFields,
    company_name_match: companyMatch,
    match_confidence: exactFields.length ? 'direct' : companyMatch ? 'company_text' : 'none',
  }
}

async function loadCatalystAuditArticles(db, row = {}, days = 7, selectedCatalysts = []) {
  if (!db || !row.ticker) return []
  const ticker = String(row.ticker).toUpperCase()
  const companyTokens = companySignalTokens(row).filter(token => token.length >= 5).slice(0, 2)
  const escapedTicker = ticker.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const or = [
    { ticker: { $regex: `(^|[,;|\\s])\\$?${escapedTicker}([,;|\\s]|$)`, $options: 'i' } },
    { tickers: ticker },
    { matched_ticker: ticker },
    { matched_tickers: ticker },
    { matched_mover_tickers: ticker },
    { symbols: ticker },
  ]
  for (const token of companyTokens) {
    const escaped = token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    or.push({ title: { $regex: escaped, $options: 'i' } })
    or.push({ headline: { $regex: escaped, $options: 'i' } })
  }
  const cutoffSec = Math.floor(Date.now() / 1000) - Math.max(1, Number(days || 7)) * 86_400
  const docs = await db.collection('articles').find({ $or: or }, {
    projection: {
      _id: 1,
      source: 1,
      publisher: 1,
      collector: 1,
      category: 1,
      article_kind: 1,
      event_type: 1,
      title: 1,
      headline: 1,
      summary: 1,
      description: 1,
      url: 1,
      ticker: 1,
      tickers: 1,
      matched_ticker: 1,
      matched_tickers: 1,
      matched_mover_tickers: 1,
      symbols: 1,
      sentiment: 1,
      sentiment_score: 1,
      ml_confidence: 1,
      publish_date: 1,
      published_at: 1,
      detected_at: 1,
      ingestion_time: 1,
      ingested_at: 1,
      fetched_date: 1,
      createdAt: 1,
      created_at: 1,
      updated_at: 1,
      updatedAt: 1,
      duplicate_cluster_id: 1,
      canonical_event_id: 1,
    },
  }).sort({ publish_date: -1, published_at: -1, detected_at: -1 }).limit(150).toArray().catch(() => [])

  const selectedKeys = new Set((Array.isArray(selectedCatalysts) ? selectedCatalysts : []).map(item => [
    String(item.url || ''),
    String(item.title || ''),
    String(item.event_sec || ''),
  ].join('|')))
  return docs.map(article => {
    const publicationValue = article.publish_date ?? article.published_at
    const ingestionValue = article.ingestion_time ?? article.ingested_at ?? article.detected_at ?? article.fetched_date ?? article.createdAt ?? article.created_at
    const publicationSec = timestampSeconds(publicationValue)
    const ingestionSec = timestampSeconds(ingestionValue)
    const updateSec = timestampSeconds(article.updated_at ?? article.updatedAt)
    if (publicationSec != null && publicationSec < cutoffSec) return null
    const match = auditArticleMatch(article, row)
    const title = String(article.title || article.headline || article.summary || article.description || '').trim()
    const eventType = String(article.event_type || article.category || article.article_kind || '').trim().toLowerCase() || null
    const selected = selectedKeys.has([String(article.url || ''), title, String(publicationSec || '')].join('|'))
    return {
      id: article.canonical_event_id || article._id || null,
      canonical_event_id: article.canonical_event_id || null,
      duplicate_cluster_id: article.duplicate_cluster_id || null,
      source: article.source || article.publisher || article.collector || null,
      collector: article.collector || null,
      article_kind: article.article_kind || article.category || null,
      event_type: eventType,
      headline: title || null,
      url: article.url || null,
      publication_timestamp: auditIsoTimestamp(publicationSec),
      ingestion_timestamp: auditIsoTimestamp(ingestionSec),
      update_timestamp: auditIsoTimestamp(updateSec),
      publication_sec: publicationSec,
      ingestion_sec: ingestionSec,
      age_minutes: publicationSec == null ? null : Number(Math.max(0, (Date.now() / 1000 - publicationSec) / 60).toFixed(1)),
      market_session: marketSessionForSec(publicationSec),
      sentiment: article.sentiment || null,
      sentiment_score: nullableNumber(article.sentiment_score ?? article.ml_confidence),
      match,
      used_by_current_catalyst_aggregate: selected,
    }
  }).filter(Boolean)
}

function catalystAuditFeatureInputs(row = {}, validation = {}, threshold = {}, quality = {}, reaction = {}) {
  const price = nullableNumber(row.price)
  const volume = nullableNumber(row.volume)
  const relVolume = nullableNumber(row.rel_volume)
  return {
    quote: {
      price,
      change_pct: nullableNumber(row.change_pct),
      volume,
      dollar_volume: price != null && volume != null ? Number((price * volume).toFixed(2)) : null,
      relative_volume: relVolume,
      quote_source: row.quote_source || null,
      quote_timestamp: auditIsoTimestamp(row.quote_timestamp ?? row.quote_at ?? row.fetched_at ?? row.updated_at),
      quote_age_seconds: nullableNumber(row.quote_age_seconds),
      quote_freshness: row.quote_freshness || null,
      market_cap: nullableNumber(row.market_cap),
      market_cap_bucket: row.market_cap_bucket || row.market_cap_tier || null,
    },
    catalyst: {
      article_count: nullableNumber(row.news_article_count) || 0,
      session_article_count: nullableNumber(row.catalyst_window_article_count) || 0,
      title: row.main_catalyst?.title || null,
      category: quality.class || row.main_catalyst?.event_type || null,
      source: quality.source || row.main_catalyst?.source || null,
      event_timestamp: auditIsoTimestamp(row.main_catalyst?.event_sec ?? row.latest_publish_sec),
      event_session: reaction.market_session || null,
      recognized: Boolean(validation.recognizedNewsCatalyst),
      direct_and_material: Boolean(validation.materialDirectCatalyst),
      materiality_score: nullableNumber(quality.company_relative_materiality_score ?? quality.taxonomy?.materiality_score),
      explosion_potential_score: nullableNumber(quality.explosion_potential_score ?? quality.taxonomy?.explosion_potential_score),
    },
    social: {
      message_count: nullableNumber(row.message_count) || 0,
      message_density: nullableNumber(row.social_message_density),
      density_score: nullableNumber(row.message_density_score),
      density_trend: row.message_density_trend || null,
      sentiment: nullableNumber(row.social_message_sentiment ?? row.social_sentiment),
      watcher_count: nullableNumber(row.stocktwits_watcher_count),
      watcher_age_seconds: nullableNumber(row.watcher_snapshot_age_seconds),
      watcher_source: row.watcher_snapshot_source || null,
    },
    threshold: {
      tier: threshold.tier || null,
      window_minutes: threshold.profile?.windowMinutes ?? threshold.windowMinutes ?? null,
      correlation: threshold.correlation,
      previous_correlation: threshold.previousCorrelation,
      threshold_c: threshold.thresholdC,
      pre_signal_return_60m_pct: threshold.preSignalReturn60mPct,
      trailing_60m_messages: threshold.trailing60Messages,
      setup_status: threshold.setupStatus,
      setup_score: threshold.setupScore,
      entry_passed: Boolean(threshold.passed),
    },
    reaction: {
      state: reaction.first_reaction_state || reaction.reactionState || null,
      label: reaction.label || null,
      latest_return_pct: reaction.latest_return_pct ?? null,
      runup_pct: reaction.runup_pct ?? null,
      giveback_pct: reaction.giveback_from_high_pct ?? null,
      market_had_chance_to_react: Boolean(reaction.market_had_chance_to_react),
      actionable_spillover: Boolean(reaction.actionable_spillover),
      exhaustion_risk: Boolean(reaction.exhaustion_risk),
    },
  }
}

// GET /api/screener/audit/:ticker — source-to-decision trace for one ticker.
// This is intentionally uncached: it is an operator/audit view of current Mongo data.
router.get('/audit/:ticker', async (req, res) => {
  try {
    const ticker = String(req.params.ticker || '').trim().toUpperCase()
    if (!/^[A-Z][A-Z0-9.-]{0,5}$/.test(ticker) || NON_STOCK_TICKERS.has(ticker)) {
      return res.status(400).json({ ok: false, error: 'invalid US stock ticker' })
    }
    const db = mongoose.connection.db
    if (!db) return res.status(503).json({ ok: false, error: 'MongoDB is not connected' })
    const days = Math.max(1, Math.min(14, Number(req.query.days || 7)))
    const sessionContext = marketSessionContext()
    const quoteDoc = await Screener.findOne({ ticker }).lean().catch(() => null)
    if (!quoteDoc) return res.status(404).json({ ok: false, ticker, error: 'ticker not found in screener universe' })
    const baseRow = normalizeScreenerRow(quoteDoc)
    const [articleMap, socialMap, shortMap, watcherMap] = await Promise.all([
      loadArticleStatsForTickers(db, [ticker], days, sessionContext),
      loadAdaptiveSocialStatsForRows(db, [baseRow]),
      loadShortInterestSnapshots(db, [ticker]),
      loadStocktwitsWatcherSnapshots(db, [ticker]),
    ])
    const enriched = attachWatcherSqueezeEvidence(
      attachShortInterestEvidence(
        enrichScreenerRow(baseRow, articleMap.get(ticker), socialMap.get(ticker)),
        shortMap.get(ticker),
      ),
      watcherMap.get(ticker),
    )
    const catalystText = enriched.main_catalyst?.title || enriched.catalyst_summary || ''
    const setupStatus = enriched.threshold_setup_status || evaluatePredictionEntryThreshold(enriched).setupStatus
    const context = {
      catalystText,
      news: Number(enriched.news_article_count || 0),
      social: Number(enriched.message_count || 0),
      sentiment: Number(enriched.avg_sentiment || 0),
      change: nullableNumber(enriched.change_pct),
      relVolume: nullableNumber(enriched.rel_volume),
      catalystPower: nullableNumber(enriched.catalyst_power_score) || 0,
      squeezeScore: nullableNumber(enriched.short_squeeze_score) || 0,
      watcherCount: nullableNumber(enriched.stocktwits_watcher_count) || 0,
      floatShort: nullableNumber(enriched.float_short),
      setupStatus,
    }
    const threshold = evaluatePredictionEntryThreshold(enriched)
    const validation = predictionEvidenceValidation(enriched, context)
    const reactionState = predictionCatalystReactionState(enriched, threshold, validation, {
      change: context.change,
      setupStatus,
      sessionContext,
      priceReaction: enriched.catalyst_price_reaction || null,
    })
    const reaction = catalystReactionSummary(reactionState)
    const quality = catalystQualityAssessment(enriched, validation, context)
    const readiness = predictionReadinessState(enriched, validation, {
      freshDensityCross: Boolean(threshold.passed),
      freshNewsCatalyst: Boolean(validation.recognizedNewsCatalyst),
      passesHighConviction: false,
      passesRawPrediction: false,
      payoffPasses: null,
      blockedReasons: reaction.rejection ? [reaction.rejection] : [],
    }, reactionState)
    const tracedRow = {
      ...enriched,
      prediction_validation: validation,
      catalyst_reaction_summary: reaction,
      catalyst_quality: quality,
      prediction_readiness: readiness,
      prediction_readiness_level: readiness.level,
    }
    const scorecard = buildPredictionScorecard(tracedRow)
    const classification = withPredictionSetupClassification(tracedRow)
    const articles = await loadCatalystAuditArticles(db, enriched, days, enriched.catalysts)
    const rejectionReasons = Array.from(new Set([
      ...hardRejectionReasonsForRow(tracedRow),
      ...(Array.isArray(readiness.blocked_reasons) ? readiness.blocked_reasons : []),
      ...(validation.valid ? [] : ['PREDICTION_EVIDENCE_VALIDATION_FAILED']),
    ].filter(Boolean)))
    const status = hasPrimaryPredictionCatalyst(validation, tracedRow)
      ? reaction.rejection ? 'rejected_priced_or_faded' : 'strict_catalyst_candidate'
      : hasDevelopingCatalystEvidence(tracedRow)
        ? 'developing_catalyst_candidate'
        : 'watch_only_or_rejected'
    return res.json({
      ok: true,
      ticker,
      generated_at: new Date().toISOString(),
      window_days: days,
      session_context: sessionContext,
      selection_status: status,
      rejection_reasons: rejectionReasons,
      trace: {
        quote: {
          ticker,
          company: enriched.company || null,
          exchange: enriched.exchange || null,
          source: enriched.quote_source || null,
          fetched_at: auditIsoTimestamp(enriched.fetched_at ?? enriched.updated_at),
        },
        articles,
        features: catalystAuditFeatureInputs(enriched, validation, threshold, quality, reaction),
        catalyst: {
          main: enriched.main_catalyst || null,
          taxonomy: quality.taxonomy || null,
          quality,
          validation,
          reaction,
        },
        threshold,
        readiness,
        classification: {
          catalyst_label: classification.catalyst_label || null,
          catalyst_direction: classification.catalyst_direction || null,
          setup_type: classification.setup_type || null,
          discovery_tier: classification.discovery_tier || null,
          reason: classification.reason_included || classification.catalystReason || null,
        },
        scorecard,
        score_contributions: {
          catalyst_quality: {
            base_score: quality.taxonomy?.base_score ?? null,
            materiality: quality.taxonomy?.materiality_score ?? null,
            directness: quality.taxonomy?.directness_score ?? null,
            source: quality.taxonomy?.source_score ?? quality.source_score ?? null,
            simplicity: quality.taxonomy?.simplicity_score ?? null,
            explosion_potential: quality.taxonomy?.explosion_potential_score ?? null,
            hard_rejection_reason: quality.hard_rejection_reason || null,
          },
          prediction_scorecard: scorecard,
        },
      },
    })
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err.message || err) })
  }
})

// GET /api/screener
router.get('/tickers', async (req, res) => {
  try {
    const q = String(req.query.q || '').trim().toUpperCase().replace(/[^A-Z0-9.-]/g, '').slice(0, 12)
    const requestedLimit = Math.max(1, Math.min(TICKER_LIST_LIMIT, Number(req.query.limit || TICKER_LIST_LIMIT) || TICKER_LIST_LIMIT))
    const filter = {
      exchange: { $in: Array.from(US_EXCHANGES) },
      ticker: { $not: /\./ },
      price: { $ne: null },
      ...(q ? { ticker: { $regex: `^${escapeRegExp(q)}`, $options: 'i', $not: /\./ } } : {}),
    }
    const rows = (await Screener.find(filter, {
      _id: 0,
      ticker: 1,
      company: 1,
      exchange: 1,
      price: 1,
      quote_status: 1,
      change_pct: 1,
    }).sort({ ticker: 1 }).limit(requestedLimit).lean())
      .map(normalizeScreenerRow)
      .filter(isCleanListedUsRow)

    res.json({
      ok: true,
      total: rows.length,
      limit: requestedLimit,
      universe: 'NASDAQ / NYSE / AMEX listed stocks from numeric screeners',
      tickers: rows.map(row => ({
        ticker: row.ticker,
        company: row.company || '',
        exchange: normalizeExchange(row.exchange),
        price: row.price ?? null,
      })),
    })
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err.message || err), tickers: [] })
  }
})

router.get('/', async (req, res) => {
  try {
    const { sector, signal, orderBy = 'ticker', orderDir = 'asc', limit = 3000, days = 3 } = req.query
    const compact = ['1', 'true', 'yes'].includes(String(req.query.compact || '').toLowerCase())
    const mirrorMode = ['1', 'true', 'yes'].includes(String(req.query.mirror || '').toLowerCase())
    const windowOverride = req.query.window_minutes ? Number(req.query.window_minutes) : null
    const filter = {
      exchange: { $in: Array.from(US_EXCHANGES) },
      ticker: { $not: /\./ },
      price: { $ne: null },
    }
    if (sector && !mirrorMode) filter.sector = sector
    if (signal === 'social_bullish' && !mirrorMode) filter.social_sentiment = { $gte: 0.3 }
    if (signal === 'social_bearish' && !mirrorMode) filter.social_sentiment = { $lte: -0.3 }
    if (signal === 'unusual_volume' && !mirrorMode) filter.volume = { $gte: 30000000 }

    const view = String(req.query.view || 'all').toLowerCase()
    const predictionView = view === 'predicted_increases' || view === 'high_conviction_next_day'
    const sort = { [orderBy]: orderDir === 'asc' ? 1 : -1 }
    const requestedLimit = Math.max(1, Math.min(5000, Number(limit || 3000)))
    const queryLimit = predictionView
      ? Math.max(requestedLimit, PREDICTION_UNIVERSE_LIMIT)
      : mirrorMode
        ? Math.max(requestedLimit, 3000)
        : requestedLimit
    const hasEvidenceSensitiveFilter = Boolean(
      signal ||
      sector ||
      req.query.search ||
      req.query.q ||
      windowOverride ||
      Object.keys(req.query || {}).some(key => [
        'news_available',
        'social_available',
        'sentiment',
        'catalyst',
        'source',
        'prediction_direction',
        'decision_journey',
      ].includes(key))
    )
    const leanFullUniverseOverview = Boolean(
      compact &&
      !predictionView &&
      !mirrorMode &&
      view === 'all' &&
      requestedLimit > 1000 &&
      !hasEvidenceSensitiveFilter &&
      !['1', 'true', 'yes'].includes(String(req.query.enrich || '').toLowerCase())
    )
    
    let data = (await Screener.find(filter)
      .sort(sort)
      .limit(queryLimit)
      .lean())
      .map(normalizeScreenerRow)
      .filter(isCleanListedUsRow)

    const sessionContext = marketSessionContext()

    if (mongoose.connection.db && data.length && !leanFullUniverseOverview) {
      const tickers = data.map(row => row.ticker)
      const [articleMap, socialMap, shortMap, watcherMap] = await Promise.all([
        loadArticleStatsForTickers(mongoose.connection.db, tickers, Number(days || 3), sessionContext),
        loadAdaptiveSocialStatsForRows(mongoose.connection.db, data, windowOverride),
        loadShortInterestSnapshots(mongoose.connection.db, tickers),
        loadStocktwitsWatcherSnapshots(mongoose.connection.db, tickers),
      ])
      data = data.map(row => attachWatcherSqueezeEvidence(
        attachShortInterestEvidence(
          enrichScreenerRow(row, articleMap.get(row.ticker), socialMap.get(row.ticker), windowOverride),
          shortMap.get(row.ticker),
        ),
        watcherMap.get(row.ticker),
      ))
    }

    if (!predictionView && !mirrorMode) {
      data = applySignalFilter(data, signal, req.query)
    }

    const useFastDevelopingPeopleView = view === 'predicted_increases' && ['1', 'true', 'yes'].includes(String(req.query.fastPeopleOnly || req.query.fast_people_only || '').toLowerCase())
    if (useFastDevelopingPeopleView) {
      const peopleRows = buildPeopleMomentumDevelopingRows(data, requestedLimit)
        .map((row, index) => suppressBroadRoundupMainCatalyst({
          ...withDiscoveryTier(row),
          candidate_pool_rank: index + 1,
          rank: index + 1,
        }))
      const activeSocialRows = peopleRows.filter(row => Number(row.message_count || 0) > 0 || Number(row.stocktwits_message_count || 0) > 0)
      // If the fast people-only path is too sparse, continue into the full
      // catalyst-aware pipeline so pending/unreacted news setups are not hidden.
      if (peopleRows.length >= Math.min(5, requestedLimit)) {
      const db = mongoose.connection.db
      const [modelDoc, postmortemReport] = db
        ? await Promise.all([
          db.collection('next_session_prediction_models').findOne({ _id: 'next_session_outcome_calibrator_v1' }).catch(() => null),
          loadPredictionPostmortemReport(db),
        ])
        : [null, null]
      const predictionRiskFlagCounts = peopleRows.reduce((acc, row) => {
        const flags = Array.isArray(row.risk_flags) ? row.risk_flags : []
        for (const flag of flags) acc[flag] = (acc[flag] || 0) + 1
        return acc
      }, {})
      const predictionReadinessCounts = peopleRows.reduce((acc, row) => {
        const level = row.prediction_readiness_level || row.prediction_readiness?.level || 'unknown'
        acc[level] = (acc[level] || 0) + 1
        return acc
      }, {})
      const catalystReactionCounts = peopleRows.reduce((acc, row) => {
        const label = row.catalyst_reaction_summary?.label || row.prediction_readiness?.reaction?.label || 'unknown'
        acc[label] = (acc[label] || 0) + 1
        return acc
      }, {})
      const catalystQualityCounts = peopleRows.reduce((acc, row) => {
        const tier = row.catalyst_quality_tier || row.catalyst_quality?.tier || 'unknown'
        acc[tier] = (acc[tier] || 0) + 1
        return acc
      }, {})
      const pendingOpenConfirmationCounts = peopleRows.reduce((acc, row) => {
        const confirmation = row.pending_open_confirmation
        if (!confirmation?.is_pending_open) return acc
        const key = confirmation.passes ? 'confirmed' : 'needs_confirmation'
        acc[key] = (acc[key] || 0) + 1
        return acc
      }, {})
      const firstReactionStateCounts = peopleRows.reduce((acc, row) => {
        const state = row.catalyst_reaction_summary?.first_reaction_state || row.prediction_readiness?.reaction?.first_reaction_state || 'unknown'
        acc[state] = (acc[state] || 0) + 1
        return acc
      }, {})
      return res.json({
        ok: true,
        tickers: peopleRows,
        rows: compact ? undefined : peopleRows,
        realPredictionRows: peopleRows,
        fallbackRows: [],
        count: peopleRows.length,
        fallbackCount: 0,
        universe: 'NASDAQ / NYSE / AMEX listed stocks from numeric screeners',
        prediction_session_context: sessionContext,
        prediction_debug: {
          ok: true,
          tab: view,
          mode: 'fast_developing_people_momentum',
          sourceUniverseRows: data.length,
          finalRows: peopleRows.length,
          strictRows: 0,
          candidatePoolRows: peopleRows.length,
          developingCandidateMinScore: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
          activePeopleRows: activeSocialRows.length,
          modelMode: 'fast_developing_people_momentum',
          calibratorMode: modelDoc?.status || 'calibrator_shadow_fallback',
          predictionRiskFlagCounts,
          predictionReadinessCounts,
          catalystReactionCounts,
          catalystQualityCounts,
          pendingOpenConfirmationCounts,
          firstReactionStateCounts,
          postmortemReport,
          thresholdPolicyVersion: PREDICTION_THRESHOLD_POLICY_VERSION,
          warnings: peopleRows.length ? ['developing_people_momentum_fast_path'] : ['no_people_backed_positive_movers_found'],
        },
        diagnostics: {
          backend_count: peopleRows.length,
          model_mode: 'fast_developing_people_momentum',
          finalRows: peopleRows.length,
          strictRows: 0,
          candidatePoolRows: peopleRows.length,
          developingCandidateMinScore: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
          sourceUniverseRows: data.length,
          activePeopleRows: activeSocialRows.length,
          predictionRiskFlagCounts,
          predictionReadinessCounts,
          catalystReactionCounts,
          catalystQualityCounts,
          pendingOpenConfirmationCounts,
          firstReactionStateCounts,
          postmortemReport,
          thresholdPolicyVersion: PREDICTION_THRESHOLD_POLICY_VERSION,
        },
        next_session_model: {
          live_enabled: Boolean(modelDoc?.live_enabled),
          evidence_predictions_enabled: peopleRows.length > 0,
          status: modelDoc?.status || 'calibrator_shadow_fallback',
          target: modelDoc?.target || null,
          split_mode: modelDoc?.split_mode || null,
          selected_threshold: modelDoc?.selected_threshold ?? null,
          validation_status: modelDoc?.validation_status || null,
          validation_reason: modelDoc?.validation_reason || null,
          selected_metrics: modelDoc?.metrics?.selected || null,
          baseline_majority_accuracy: modelDoc?.metrics?.baseline_majority_accuracy ?? null,
          samples: modelDoc?.samples || modelDoc?.metrics?.samples || 0,
          min_samples: modelDoc?.min_samples || 0,
        },
        prediction_postmortem: postmortemReport,
        prediction_note: peopleRows.length
          ? 'Showing positive movers with current people/message attention. These are developing opportunities, not strict high-conviction predictions.'
          : 'No positive movers currently have enough live people/message attention for the Developing tab.',
        summary: {
          priced: peopleRows.filter(row => row.price != null).length,
          real_predictions: peopleRows.length,
          strict_predictions: 0,
          candidate_pool_rows: peopleRows.length,
          developing_candidate_min_score: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
          threshold_policy_version: PREDICTION_THRESHOLD_POLICY_VERSION,
        },
        exchanges: Array.from(US_EXCHANGES),
        rolling_windows: {
          selected: windowOverride || 'adaptive',
          nano_micro: 5,
          small: 15,
          mid: 30,
          large: 60,
          mega: 120,
        },
        catalyst_window: {
          session: sessionContext.session,
          market_phase: sessionContext.market_phase,
          policy: sessionContext.catalyst_window_policy,
          start_sec: sessionContext.catalyst_window_start_sec,
          end_sec: sessionContext.catalyst_window_end_sec,
          next_session_date: sessionContext.next_session_date,
        },
        excluded: ['fallback rows are not strict high conviction', 'OTC', 'crypto', 'unpriced/article-only rows', 'non-US exchanges'],
        max_abs_change_pct: MAX_SIGNAL_CHANGE_PCT,
      })
      }
    }

    if (predictionView && mongoose.connection.db) {
      const db = mongoose.connection.db
      const squeezeInterestRows = await loadSqueezeInterestRows(db, data, sessionContext, Number(days || 3), windowOverride)
      if (squeezeInterestRows.length) data = uniquePredictionRows([...data, ...squeezeInterestRows])
      const thresholdFeatureMap = await loadThresholdFeatureMap(db, data.map(row => row.ticker))
      if (thresholdFeatureMap.size) {
        data = data.map(row => {
          const thresholdFeatures = thresholdFeatureMap.get(String(row.ticker || '').toUpperCase())
          return thresholdFeatures ? { ...row, ...thresholdFeatures } : row
        })
      }
      const catalystPriceReactionMap = await loadCatalystPriceReactionMap(db, data)
      if (catalystPriceReactionMap.size) {
        data = data.map(row => {
          const reaction = catalystPriceReactionMap.get(String(row.ticker || '').toUpperCase())
          return reaction ? { ...row, catalyst_price_reaction: reaction } : row
        })
      }
      const predictionDate = isoDateKey()
      const targetDate = nextTradingDateIso(predictionDate)
      const maxPicks = Math.max(1, Math.min(250, Number(req.query.maxPicks || req.query.max_picks || requestedLimit)))
      const [modelDoc, postmortemReport] = await Promise.all([
        db.collection('next_session_prediction_models').findOne({ _id: 'next_session_outcome_calibrator_v1' }).catch(() => null),
        loadPredictionPostmortemReport(db),
      ])
      const allStoredRowsRaw = await loadStoredNextDayPredictionRows(db, data)
      const storedRowsRaw = allStoredRowsRaw.filter(row => row.predictionDate === predictionDate || row.targetDate === targetDate)
      const storedRows = storedRowsRaw.filter(isActionablePredictionRow)
      const liveSignalRows = await loadLivePredictionSignalRows(db, data, requestedLimit)
      const actionableLiveRows = liveSignalRows.filter(isActionablePredictionRow)
      const evidencePredictionRows = buildEvidencePredictionRows(data, Math.max(requestedLimit, maxPicks * 5), { predictionDate, targetDate, modelDoc })
      const predictionRows = uniquePredictionRows([...storedRows, ...actionableLiveRows, ...evidencePredictionRows])
      const predictionSourceMode = storedRows.length && actionableLiveRows.length
        ? 'stored_plus_live_prediction_signals'
          : storedRows.length
            ? 'stored_daily_prediction'
            : actionableLiveRows.length
              ? 'live_prediction_signal_no_daily_archive'
              : evidencePredictionRows.length ? 'validated_evidence_next_session_candidate_v2' : 'no_entry_ready_prediction'
      const realUpRows = predictionRows
        .filter(row => {
          const direction = String(row.predictedDirection || row.prediction_direction || '').toLowerCase()
          const predicted = Number(row.predictedReturnPct ?? row.predicted_return)
          const setupStatus = row.entry_signal?.setup_status || row.threshold_setup_status
          const validation = predictionEvidenceValidation(row, {
            catalystText: row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || '',
            news: nullableNumber(row.news_article_count ?? row.article_count),
            social: nullableNumber(row.message_count),
            sentiment: nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment),
            change: nullableNumber(row.change_pct),
            relVolume: nullableNumber(row.rel_volume),
            catalystPower: nullableNumber(row.catalyst_power_score) || 0,
            squeezeScore: nullableNumber(row.short_squeeze_score) || 0,
            watcherCount: nullableNumber(row.stocktwits_watcher_count) || 0,
            floatShort: nullableNumber(row.float_short),
            setupStatus,
          })
          const climberGate = row.prediction_climber_gate || predictionClimberGate(row, validation, {
            change: row.change_pct,
            relVolume: row.rel_volume,
            social: row.message_count,
            sentiment: row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment,
            squeezeScore: row.short_squeeze_score,
            watcherCount: row.stocktwits_watcher_count,
            setupStatus,
          })
	          const pricedInRejection = predictionPricedInRejection(
	            row,
	            row.threshold_policy || row.prediction_threshold_policy || row.entry_signal || {},
	            validation,
	            { change: row.change_pct, setupStatus },
	          )
	          const freshTriggerState = row.fresh_prediction_trigger || predictionFreshTriggerState(row, validation)
	          return (direction === 'up' || (Number.isFinite(predicted) && predicted > 0)) &&
	            Number.isFinite(predicted) &&
	            predicted > 0 &&
	            positiveCurrentMover(row) &&
	            validation.valid &&
	            hasPrimaryPredictionCatalyst(validation, row) &&
	            climberGate.passes &&
	            !catalystReactionExhausted(row) &&
	            freshTriggerState.passesRawPrediction &&
	            !pricedInRejection
	        })
	        .sort((a, b) => {
	          const displayDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
	          if (displayDiff !== 0) return displayDiff
	          const payoffDiff = Number(b.payoff_model_probability ?? -1) - Number(a.payoff_model_probability ?? -1)
	          if (payoffDiff !== 0) return payoffDiff
	          const returnDiff = Number(b.predictedReturnPct ?? b.predicted_return ?? 0) - Number(a.predictedReturnPct ?? a.predicted_return ?? 0)
	          if (returnDiff !== 0) return returnDiff
	          const scoreDiff = Number(b.convictionScore ?? b.final_prediction_score ?? 0) - Number(a.convictionScore ?? a.final_prediction_score ?? 0)
          if (scoreDiff !== 0) return scoreDiff
          return Number(b.confidence ?? b.prediction_confidence ?? 0) - Number(a.confidence ?? a.prediction_confidence ?? 0)
        })

      const realHighConvictionRows = predictionRows
        .filter(row => {
          const confidence = Number(row.confidence ?? row.prediction_confidence)
          const conviction = Number(row.convictionScore ?? row.final_prediction_score)
          const predicted = Number(row.predictedReturnPct ?? row.predicted_return)
          const setupStatus = row.entry_signal?.setup_status || row.threshold_setup_status
          const validation = predictionEvidenceValidation(row, {
            catalystText: row.main_catalyst?.title || row.catalyst_summary || row.catalyst || row.structured_catalyst || row.event_type || '',
            news: nullableNumber(row.news_article_count ?? row.article_count),
            social: nullableNumber(row.message_count),
            sentiment: nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment),
            change: nullableNumber(row.change_pct),
            relVolume: nullableNumber(row.rel_volume),
            catalystPower: nullableNumber(row.catalyst_power_score) || 0,
            squeezeScore: nullableNumber(row.short_squeeze_score) || 0,
            watcherCount: nullableNumber(row.stocktwits_watcher_count) || 0,
            floatShort: nullableNumber(row.float_short),
            setupStatus,
          })
          const climberGate = row.prediction_climber_gate || predictionClimberGate(row, validation, {
            change: row.change_pct,
            relVolume: row.rel_volume,
            social: row.message_count,
            sentiment: row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment,
            squeezeScore: row.short_squeeze_score,
            watcherCount: row.stocktwits_watcher_count,
            setupStatus,
          })
          const hasEvidence = Number(row.news_article_count || 0) > 0 || Number(row.message_count || 0) >= 3 || Number(row.short_squeeze_score || 0) >= 55 || row.entry_signal?.setup_ready || row.entry_signal?.entry_ready
          const sentiment = nullableNumber(row.avg_sentiment ?? row.structured_sentiment ?? row.social_sentiment)
          const sentimentOk = sentiment == null || sentiment >= -0.05
          const highConvictionEvidence = Boolean(
            validation.recognizedNewsCatalyst ||
            validation.verifiedShortInterest ||
            validation.recognizedPeopleAttention ||
            (validation.recognizedDensitySetup && validation.recognizedSocialCatalyst)
          )
          const newsOrShortRescue = Boolean(
            predicted >= 5 &&
            (validation.recognizedNewsCatalyst || validation.verifiedShortInterest) &&
            Number(row.rel_volume || 0) >= 2
          )
          const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
	          const blockedByPostmortemGate = PREDICTION_HIGH_CONVICTION_REQUIRE_POSTMORTEM_GATES && (
	            riskFlags.includes('NO_FRESH_DENSITY_ENTRY_CROSS') ||
	            riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') ||
	            riskFlags.includes('NO_FRESH_CATALYST') ||
	            riskFlags.includes('CATALYST_ALREADY_PRICED_IN') ||
	            riskFlags.includes('BELOW_PAYOFF_MODEL_THRESHOLD') ||
	            riskFlags.includes('NO_FRESH_CONFIRMED_TRIGGER') ||
	            riskFlags.includes('NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION') ||
	            riskFlags.includes('STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION') ||
	            riskFlags.includes('STALE_NEWS_WITHOUT_CURRENT_PEOPLE_ATTENTION') ||
	            riskFlags.includes('STALE_OR_OUT_OF_WINDOW_CATALYST') ||
	            riskFlags.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE') ||
	            riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') ||
	            riskFlags.includes('PENDING_OPEN_NEEDS_SECOND_CONFIRMATION')
	          )
	          const freshTriggerState = row.fresh_prediction_trigger || predictionFreshTriggerState(row, validation)
	          const pricedInRejection = predictionPricedInRejection(
	            row,
	            row.threshold_policy || row.prediction_threshold_policy || row.entry_signal || {},
            validation,
            { change: row.change_pct, setupStatus },
          )
          return Number.isFinite(confidence) && confidence >= Number(req.query.minConfidence || 0.45) &&
            Number.isFinite(conviction) && (conviction >= Number(req.query.minFinalScore || 52) || newsOrShortRescue) &&
            Number.isFinite(predicted) && predicted >= Number(req.query.minPredictedReturn || climberGate.highConvictionMinReturnPct || 4.5) &&
            isPositivePredictionRow(row) &&
	            validation.valid &&
	            hasPrimaryPredictionCatalyst(validation, row) &&
	            climberGate.passes &&
	            !catalystReactionExhausted(row) &&
	            freshTriggerState.passesHighConviction &&
	            !blockedByPostmortemGate &&
	            !pricedInRejection &&
            highConvictionEvidence &&
            hasEvidence &&
            sentimentOk
	        })
	        .sort((a, b) => {
	          const displayDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
	          if (displayDiff !== 0) return displayDiff
	          const payoffDiff = Number(b.payoff_model_probability ?? -1) - Number(a.payoff_model_probability ?? -1)
	          if (payoffDiff !== 0) return payoffDiff
	          const returnDiff = Number(b.predictedReturnPct ?? b.predicted_return ?? 0) - Number(a.predictedReturnPct ?? a.predicted_return ?? 0)
          if (returnDiff !== 0) return returnDiff
          return Number(b.convictionScore ?? b.final_prediction_score ?? 0) - Number(a.convictionScore ?? a.final_prediction_score ?? 0)
        })
        .map((row, index) => ({ ...row, high_conviction: true, high_conviction_rank: index + 1, rank: index + 1 }))

      const fallbackLimit = Math.min(PREDICTION_DEVELOPING_CANDIDATE_MAX_ROWS, Math.max(requestedLimit, maxPicks * 2))
      const fallbackRowsRaw = buildPredictionWatchRows(data, fallbackLimit)
      const developingMomentumRowsRaw = view === 'predicted_increases'
        ? buildMomentumCatalystDevelopingRows(data, fallbackLimit)
        : []
      const candidatePoolMin = view === 'predicted_increases'
        ? Math.max(0, Math.min(PREDICTION_DEVELOPING_CANDIDATE_MAX_ROWS, Number(req.query.candidatePoolMin || req.query.candidate_pool_min || 0)))
        : 0
      const includeDevelopingCandidates = !['0', 'false', 'no'].includes(String(req.query.includeDevelopingCandidates || req.query.include_developing_candidates || 'true').toLowerCase())
      const includeBestAvailableFallback = ['1', 'true', 'yes'].includes(String(req.query.includeBestAvailable || req.query.include_best_available || 'false').toLowerCase())
      const strictRows = view === 'high_conviction_next_day'
        ? realHighConvictionRows.slice(0, maxPicks)
        : realUpRows.slice(0, requestedLimit)
      const liveDisplayRows = !storedRows.length && actionableLiveRows.length
        ? (view === 'high_conviction_next_day'
          ? actionableLiveRows.slice(0, maxPicks).map((row, index) => ({
            ...row,
            high_conviction: false,
            high_conviction_fallback: true,
            high_conviction_rank: index + 1,
            rank: index + 1,
            reason_included: `${row.reason_included || 'Live signal'} · did not pass stored high-conviction archive gate`,
          }))
          : actionableLiveRows.slice(0, requestedLimit))
        : []
      const baseDisplayRows = strictRows.length ? strictRows : liveDisplayRows
      const baseTickers = new Set(baseDisplayRows.map(row => String(row.ticker || '').toUpperCase()).filter(Boolean))
      const fallbackRows = fallbackRowsRaw.filter(row => !baseTickers.has(String(row.ticker || '').toUpperCase()))
      const developingMomentumRows = developingMomentumRowsRaw.filter(row => !baseTickers.has(String(row.ticker || '').toUpperCase()))
      const developingSourceRows = uniquePredictionRows([...predictionRows, ...developingMomentumRows, ...fallbackRows])
        .filter(row => !baseTickers.has(String(row.ticker || '').toUpperCase()))
      const developingConfidenceDiagnostics = view === 'predicted_increases'
        ? developingSourceRows.reduce((acc, row) => {
          const profile = developingConfidenceProfile(row)
          acc.checked += 1
          if (profile.passes) acc.passed += 1
          else {
            acc.rejected += 1
            for (const reason of profile.rejectionReasons || ['unknown']) {
              acc.rejectionReasons[reason] = (acc.rejectionReasons[reason] || 0) + 1
            }
          }
          return acc
        }, {
          checked: 0,
          passed: 0,
          rejected: 0,
          rejectionReasons: {},
          thresholds: {
            min_score: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
            min_confidence: PREDICTION_DEVELOPING_MIN_CONFIDENCE,
            min_rel_volume: PREDICTION_DEVELOPING_MIN_REL_VOLUME,
            min_change_pct: PREDICTION_DEVELOPING_MIN_CHANGE_PCT,
            overextended_pct: PREDICTION_DEVELOPING_OVEREXTENDED_PCT,
            severe_overextended_pct: PREDICTION_DEVELOPING_SEVERE_OVEREXTENDED_PCT,
          },
        })
        : null
      const candidatePoolCapacity = Math.max(0, requestedLimit - baseDisplayRows.length)
      const candidatePoolTarget = candidatePoolMin > 0
        ? Math.max(0, Math.min(candidatePoolMin - baseDisplayRows.length, candidatePoolCapacity))
        : candidatePoolCapacity
      const candidatePoolRows = view === 'predicted_increases' && includeDevelopingCandidates && candidatePoolTarget > 0
        ? developingSourceRows
          .filter(row => {
            const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
            const blockedReasons = Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []
            const score = Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score ?? 0)
            const confidenceProfile = developingConfidenceProfile(row)
            const movementOrPendingCatalyst = developingCandidateMovementOrCatalystOk(row, riskFlags, blockedReasons)
            const isMomentumDeveloping = String(row.prediction_status || '').includes('developing_momentum')
            const validation = row.prediction_validation || {}
            const relVolume = nullableNumber(row.rel_volume) || 0
            const social = Number(row.message_count || row.stocktwits_message_count || 0)
            const news = Number(row.news_article_count || row.article_count || 0)
            const change = nullableNumber(row.currentChangePct ?? row.change_pct)
            const momentumEvidence = isMomentumDeveloping && positiveCurrentMover(row) && (
              hasDevelopingCatalystEvidence(row) ||
              validation.recognizedPeopleAttention ||
              validation.recognizedSocialCatalyst ||
              news > 0 ||
              social >= 3 ||
              (relVolume >= 8 && change != null && change >= 1.5)
            )
            const catalystEvidence = hasDevelopingCatalystEvidence(row) || momentumEvidence
            const allowPendingCatalystWithoutPeople = movementOrPendingCatalyst && !positiveCurrentMover(row)
            const pendingCatalystScoreFloor = isMomentumDeveloping
              ? Math.max(35, PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE - 10)
              : allowPendingCatalystWithoutPeople ? Math.max(35, PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE - 8) : PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE
            const catalystQualityTier = String(row.catalyst_quality_tier || row.catalyst_quality?.tier || '').toLowerCase()
            const strongPendingCatalyst = allowPendingCatalystWithoutPeople && (catalystQualityTier === 'strong' || row.pending_open_confirmation?.passes === true)
            const notAlreadySellingOff = !allowPendingCatalystWithoutPeople || change == null || change >= -2
            return confidenceProfile.passes &&
              (score >= pendingCatalystScoreFloor || strongPendingCatalyst || confidenceProfile.rawQuality >= pendingCatalystScoreFloor) &&
              notAlreadySellingOff &&
              movementOrPendingCatalyst &&
              catalystEvidence &&
              !catalystReactionExhausted(row) &&
              !riskFlags.includes('REJECTED_CATALYST_QUALITY') &&
              !riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') &&
              (!riskFlags.includes('STALE_NEWS_WITHOUT_CURRENT_PEOPLE_ATTENTION') || allowPendingCatalystWithoutPeople) &&
              (!riskFlags.includes('NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION') || allowPendingCatalystWithoutPeople) &&
              (!riskFlags.includes('STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION') || allowPendingCatalystWithoutPeople) &&
              !blockedReasons.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') &&
              !blockedReasons.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE') &&
              (!blockedReasons.includes('NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION') || allowPendingCatalystWithoutPeople) &&
              (!blockedReasons.includes('STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION') || allowPendingCatalystWithoutPeople)
          })
          .sort((a, b) => {
            const positiveDiff = Number(positiveCurrentMover(b)) - Number(positiveCurrentMover(a))
            if (positiveDiff !== 0) return positiveDiff
            const profileA = developingConfidenceProfile(a)
            const profileB = developingConfidenceProfile(b)
            const overextensionDiff = Number(profileA.overextended) - Number(profileB.overextended)
            if (overextensionDiff !== 0) return overextensionDiff
            const qualityDiff = profileB.rankingScore - profileA.rankingScore
            if (qualityDiff !== 0) return qualityDiff
            const displayDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
            if (displayDiff !== 0) return displayDiff
            const scoreDiff = Number(b.final_prediction_score ?? b.convictionScore ?? b.watchScore ?? b.watch_score ?? b.evidence_score ?? 0) -
              Number(a.final_prediction_score ?? a.convictionScore ?? a.watchScore ?? a.watch_score ?? a.evidence_score ?? 0)
            if (scoreDiff !== 0) return scoreDiff
            return Number(b.rel_volume || 0) - Number(a.rel_volume || 0)
          })
          .slice(0, candidatePoolTarget)
          .map((row, index) => {
            const confidenceProfile = developingConfidenceProfile(row)
            const sourceLabel = row.prediction_source_label || (row.pending_open_confirmation?.passes ? 'Developing: Confirmed Pending Open' : 'Developing: Confirmed Watch')
            const sourceCode = row.prediction_source_code || 'developing_confirmed_watch'
            const sourceTone = row.prediction_source_tone || 'info'
            const predictedReturn = nullableNumber(row.final_predicted_percent ?? row.predictedReturnPct ?? row.predicted_return) ?? confidenceProfile.expectedReturn
            const probabilityUp = nullableNumber(row.probability_up) ?? confidenceProfile.probabilityUp
            const confidence = nullableNumber(row.prediction_confidence ?? row.confidence) ?? confidenceProfile.confidence
            const payoffProbability = nullableNumber(row.payoff_model_probability) ?? confidenceProfile.payoffProbability
            const developingAi = buildAiEvidenceScore({
              ...row,
              developing_confirmation_profile: confidenceProfile,
              prediction_confidence: confidence,
              confidence,
              probability_up: probabilityUp,
              payoff_model_probability: payoffProbability,
              final_prediction_score: Math.max(
                Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? 0),
                confidenceProfile.rawQuality,
              ),
            })
            const enriched = {
              ...withDiscoveryTier(row),
              isFallback: false,
              is_fallback: false,
              prediction_status: String(row.prediction_status || '').includes('evidence') || String(row.prediction_status || '').includes('developing_momentum')
                ? row.prediction_status
                : 'developing_confirmed_watch',
              prediction_pool_role: String(row.prediction_status || '').includes('evidence')
                ? 'developing_evidence_candidate'
                : String(row.prediction_status || '').includes('developing_momentum')
                  ? row.prediction_pool_role
                  : 'developing_confirmed_watch',
              prediction_source_label: sourceLabel,
              prediction_source_code: sourceCode,
              prediction_source_tone: sourceTone,
              prediction_direction: 'up',
              predictedDirection: 'up',
              predicted_return: predictedReturn,
              predictedReturnPct: predictedReturn,
              final_predicted_percent: predictedReturn,
              predicted_return_target: row.predicted_return_target || 'developing_confirmed_continuation_estimate',
              probability_up: probabilityUp,
              payoff_model_probability: payoffProbability,
              confidence,
              prediction_confidence: confidence,
              final_prediction_score: Math.max(
                Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? 0),
                confidenceProfile.rawQuality,
              ),
              ai_score: Math.max(nullableNumber(row.ai_score) || 0, developingAi.score),
              ai_score_components: developingAi.components,
              ai_score_reasons: developingAi.reasons,
              ai_context: {
                ai_score: Math.max(nullableNumber(row.ai_score) || 0, developingAi.score),
                ai_confidence: developingAi.confidence,
                ai_article_count: nullableNumber(row.catalyst_window_article_count ?? row.news_article_count ?? row.article_count) || 0,
                model: developingAi.model,
              },
              developing_confidence: confidenceProfile.confidence,
              developing_quality_score: confidenceProfile.rawQuality,
              developing_ranking_score: confidenceProfile.rankingScore,
              developing_confirmation_profile: confidenceProfile,
              candidate_pool_rank: baseDisplayRows.length + index + 1,
              prediction: row.prediction || {
                horizon: 'developing',
                predictionSession: marketSessionContext().session,
                prediction_session: marketSessionContext().session,
                predictionTarget: 'confirmed_developing_continuation',
                prediction_target: 'confirmed_developing_continuation',
                predictedReturn,
                predictedDirection: 'up',
                predictedReturnTarget: 'developing_confirmed_continuation_estimate',
                probabilityUp,
                confidence,
                model: 'developing_market_confirmation_v1',
                generatedAt: new Date().toISOString(),
              },
              reason_included: `${row.reason_included || 'Confirmed developing watch'} · ${confidenceProfile.reason}; not strict high conviction until payoff/fresh-density gates confirm`,
            }
            return withPredictionScorecard(enriched)
          })
        : []
      const topBestCandidateSourceRows = uniquePredictionRows([...predictionRows, ...fallbackRows])
        .filter(row => {
          const riskFlags = Array.isArray(row.risk_flags) ? row.risk_flags : []
          const blockedReasons = Array.isArray(row.prediction_blocked_reasons) ? row.prediction_blocked_reasons : []
          const isEvidence = String(row.prediction_status || '').includes('evidence')
          const freshTriggerState = row.fresh_prediction_trigger || {}
          const score = Number(row.final_prediction_score ?? row.convictionScore ?? row.watchScore ?? row.watch_score ?? row.evidence_score ?? 0)
          return score >= PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE &&
            freshTriggerState.passesRawPrediction !== false &&
            !catalystReactionExhausted(row) &&
            !riskFlags.includes('REJECTED_CATALYST_QUALITY') &&
            !riskFlags.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') &&
            !riskFlags.includes('LOW_OR_MISSING_SOCIAL_CONFIRMATION') &&
            !riskFlags.includes('STALE_NEWS_WITHOUT_CURRENT_PEOPLE_ATTENTION') &&
            !riskFlags.includes('NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION') &&
            !riskFlags.includes('STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION') &&
            (!isEvidence || row.payoff_model_passes === true || row.pending_open_payoff_override === true) &&
            !blockedReasons.includes('PENDING_OPEN_WEAK_CATALYST_QUALITY') &&
            !blockedReasons.includes('PENDING_OPEN_UNRECOGNIZED_SOURCE') &&
            !blockedReasons.includes('NO_CURRENT_PEOPLE_OR_MESSAGE_ATTENTION') &&
            !blockedReasons.includes('STALE_NEWS_NEEDS_CURRENT_PEOPLE_ATTENTION')
        })
        .sort((a, b) => predictionDisplayPriority(b) - predictionDisplayPriority(a))
      const topBestCandidateRows = view === 'high_conviction_next_day' && !strictRows.length && !liveDisplayRows.length
        && includeBestAvailableFallback
        ? topBestCandidateSourceRows
          .slice(0, maxPicks)
          .map((row, index) => ({
            ...withDiscoveryTier(row),
            high_conviction: false,
            high_conviction_fallback: true,
            high_conviction_rank: index + 1,
            rank: index + 1,
            prediction_status: 'best_available_candidate_not_high_conviction',
            prediction_pool_role: 'best_available_candidate',
            prediction_source_label: 'Best Available Candidate (not strict high conviction)',
            prediction_source_code: 'best_available_candidate_not_strict_high_conviction',
            prediction_source_tone: 'warning',
            reason_included: `${row.reason_included || 'Top candidate'} · best available fallback; strict high-conviction gate did not pass`,
          }))
        : []
      const rankedCandidateRows = view === 'predicted_increases'
        ? [...baseDisplayRows.map(withDiscoveryTier), ...candidatePoolRows]
          .sort((a, b) => {
            const positiveDiff = Number(positiveCurrentMover(b)) - Number(positiveCurrentMover(a))
            if (positiveDiff !== 0) return positiveDiff
            const priorityDiff = predictionDisplayPriority(b) - predictionDisplayPriority(a)
            if (Math.abs(priorityDiff) >= 1_000_000) return priorityDiff
            const profileA = developingConfidenceProfile(a)
            const profileB = developingConfidenceProfile(b)
            const overextensionDiff = Number(profileA.overextended) - Number(profileB.overextended)
            if (overextensionDiff !== 0) return overextensionDiff
            const confirmationDiff = Number(b.developing_ranking_score ?? developingConfidenceProfile(b).rankingScore) -
              Number(a.developing_ranking_score ?? developingConfidenceProfile(a).rankingScore)
            if (confirmationDiff !== 0) return confirmationDiff
            const displayDiff = priorityDiff
            if (displayDiff !== 0) return displayDiff
            return Number(b.final_prediction_score ?? b.convictionScore ?? b.watchScore ?? b.watch_score ?? 0) -
              Number(a.final_prediction_score ?? a.convictionScore ?? a.watchScore ?? a.watch_score ?? 0)
          })
          .slice(0, requestedLimit)
          .map((row, index) => ({ ...row, rank: index + 1 }))
        : []
      let finalRows = view === 'predicted_increases'
        ? rankedCandidateRows
        : (strictRows.length ? strictRows.map(withDiscoveryTier) : (liveDisplayRows.length ? liveDisplayRows.map(withDiscoveryTier) : topBestCandidateRows))
      finalRows = finalRows.map(suppressBroadRoundupMainCatalyst)
      const finalTickers = new Set(finalRows.map(row => String(row.ticker || '').toUpperCase()).filter(Boolean))
      const residualFallbackRows = fallbackRows.filter(row => !finalTickers.has(String(row.ticker || '').toUpperCase()))
      const displayFallbackRows = finalRows.length ? [] : residualFallbackRows
      const fallbackSetupCounts = fallbackRows.reduce((acc, row) => {
        const setupStatus = row.entry_signal?.setup_status || row.threshold_setup_status || 'unknown'
        acc[setupStatus] = (acc[setupStatus] || 0) + 1
        return acc
      }, {})

      const rawPredictionRows = storedRowsRaw.length
      const missingFieldCounts = predictionMissingFieldCounts(predictionRows)
      const predictionRiskFlagCounts = predictionRows.reduce((acc, row) => {
        const flags = Array.isArray(row.risk_flags) ? row.risk_flags : []
        for (const flag of flags) acc[flag] = (acc[flag] || 0) + 1
        return acc
      }, {})
      const pipelineStageCounts = predictionPipelineStageCounts({
        configuredUniverseRows: queryLimit,
        loadedRows: data,
        enteringRows: data,
        scoringRows: predictionRows,
        strictRows,
        candidateRows: candidatePoolRows,
        finalRows,
        fallbackRows,
        realUpRows,
        realHighConvictionRows,
      })
      const removedByFilterCounts = {
        no_up_direction: view === 'predicted_increases' ? Math.max(0, predictionRows.length - realUpRows.length) : 0,
        below_high_conviction_threshold: view === 'high_conviction_next_day' ? Math.max(0, predictionRows.length - realHighConvictionRows.length) : 0,
        postmortem_gate_no_fresh_density_cross: predictionRiskFlagCounts.NO_FRESH_DENSITY_ENTRY_CROSS || 0,
        postmortem_gate_low_social: predictionRiskFlagCounts.LOW_OR_MISSING_SOCIAL_CONFIRMATION || 0,
        postmortem_gate_below_payoff_model: predictionRiskFlagCounts.BELOW_PAYOFF_MODEL_THRESHOLD || 0,
      }
      const predictionReadinessCounts = [...predictionRows, ...fallbackRows].reduce((acc, row) => {
        const level = row.prediction_readiness_level || row.prediction_readiness?.level || 'unknown'
        acc[level] = (acc[level] || 0) + 1
        return acc
      }, {})
      const catalystReactionCounts = [...predictionRows, ...fallbackRows].reduce((acc, row) => {
        const label = row.catalyst_reaction_summary?.label || row.prediction_readiness?.reaction?.label || 'unknown'
        acc[label] = (acc[label] || 0) + 1
        return acc
      }, {})
      const catalystQualityCounts = [...predictionRows, ...fallbackRows].reduce((acc, row) => {
        const tier = row.catalyst_quality_tier || row.catalyst_quality?.tier || 'unknown'
        acc[tier] = (acc[tier] || 0) + 1
        return acc
      }, {})
      const pendingOpenConfirmationCounts = [...predictionRows, ...fallbackRows].reduce((acc, row) => {
        const confirmation = row.pending_open_confirmation
        if (!confirmation?.is_pending_open) return acc
        const key = confirmation.passes ? 'confirmed' : 'needs_confirmation'
        acc[key] = (acc[key] || 0) + 1
        return acc
      }, {})
      const firstReactionStateCounts = [...predictionRows, ...fallbackRows].reduce((acc, row) => {
        const state = row.catalyst_reaction_summary?.first_reaction_state || row.prediction_readiness?.reaction?.first_reaction_state || 'unknown'
        acc[state] = (acc[state] || 0) + 1
        return acc
      }, {})
      const warnings = []
      if (!storedRows.length) warnings.push('no_stored_next_day_predictions_found')
      if (actionableLiveRows.length) warnings.push('showing_live_prediction_signals_no_daily_archive')
      if (evidencePredictionRows.length) warnings.push('showing_evidence_ranked_next_session_predictions')
      if (liveDisplayRows.length && !strictRows.length) warnings.push('live_signals_displayed_because_strict_prediction_gate_returned_zero')
      if (allStoredRowsRaw.length && !storedRowsRaw.length) warnings.push('stored_prediction_archives_are_stale_for_current_target_date')
      if (candidatePoolRows.length) warnings.push('developing_candidate_rows_included_above_evidence_floor')
      if (topBestCandidateRows.length) warnings.push('showing_best_available_candidates_not_strict_high_conviction')
      if (fallbackRows.length && !finalRows.length) warnings.push('showing_screener_watch_candidates_separately')
      if (storedRowsRaw.length && !storedRows.length) warnings.push('stored_prediction_rows_failed_improved_entry_gate')
      if (liveSignalRows.length && !actionableLiveRows.length) warnings.push('live_prediction_signals_failed_improved_entry_gate')
      if (postmortemReport?.recommendations?.length) warnings.push('postmortem_recommendations_active')

      const predictionDebug = {
        ok: true,
        tab: view,
        thresholdPolicyVersion: PREDICTION_THRESHOLD_POLICY_VERSION,
        thresholdPolicy: predictionThresholdPolicy.PREDICTION_THRESHOLD_POLICY,
        discoveryUniverseLimit: PREDICTION_UNIVERSE_LIMIT,
        rawPredictionRows,
        staleStoredPredictionRows: Math.max(0, allStoredRowsRaw.length - storedRowsRaw.length),
        storedPredictionRows: storedRows.length,
        liveSignalRows: liveSignalRows.length,
        actionableLiveRows: actionableLiveRows.length,
        evidencePredictionRows: evidencePredictionRows.length,
        squeezeInterestRows: squeezeInterestRows.length,
        catalystPriceReactionRows: catalystPriceReactionMap.size,
        fallbackRows: displayFallbackRows.length,
        developingMomentumRows: developingMomentumRows.length,
        strictRows: strictRows.length,
        candidatePoolRows: candidatePoolRows.length,
        developingConfidenceDiagnostics,
        developingCandidateMinScore: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
        includeDevelopingCandidates,
        bestAvailableCandidateRows: topBestCandidateRows.length,
        topBestCandidateRows: topBestCandidateRows.length,
        candidatePoolMin,
        suppressedFallbackRows: finalRows.length ? fallbackRows.length : 0,
        fallbackSetupCounts,
        activeSetupRows: fallbackSetupCounts.active_setup_already_above_threshold || 0,
        nearThresholdRows: fallbackSetupCounts.near_threshold_setup || 0,
        finalRows: finalRows.length,
        rankedCandidateRows: rankedCandidateRows.length,
        topFiveTickers: finalRows.slice(0, 5).map(row => String(row.ticker || '').toUpperCase()).filter(Boolean),
        pipelineStageCounts,
        modelMode: predictionSourceMode,
        calibratorMode: modelDoc?.status || 'calibrator_shadow_fallback',
        predictionCacheMode: 'mongo',
        cacheHit: false,
        latestPredictionAt: predictionRows[0]?.predictionTimestamp || null,
        predictionDate: predictionRows[0]?.predictionDate || predictionDate,
        targetDate: predictionRows[0]?.targetDate || targetDate,
        missingFieldCounts,
        predictionRiskFlagCounts,
        predictionReadinessCounts,
        catalystReactionCounts,
        catalystQualityCounts,
        pendingOpenConfirmationCounts,
        firstReactionStateCounts,
        removedByFilterCounts,
        postmortemReport,
        warnings,
      }

      await persistPredictionArchiveStatus(db, {
        realRows: finalRows,
        fallbackRows: displayFallbackRows,
        metadata: predictionDebug,
      })

      return res.json({
        ok: true,
        tickers: finalRows,
        rows: compact ? undefined : finalRows,
        realPredictionRows: finalRows,
        fallbackRows: displayFallbackRows,
        count: finalRows.length,
        fallbackCount: displayFallbackRows.length,
        universe: 'NASDAQ / NYSE / AMEX listed stocks from numeric screeners',
        prediction_session_context: sessionContext,
        prediction_debug: predictionDebug,
        diagnostics: {
          backend_count: finalRows.length,
          model_mode: predictionDebug.modelMode,
          calibrator_mode: predictionDebug.calibratorMode,
          prediction_cache_mode: predictionDebug.predictionCacheMode,
          cache_hit: false,
          storedPredictionRows: storedRows.length,
          liveSignalRows: liveSignalRows.length,
          actionableLiveRows: actionableLiveRows.length,
          evidencePredictionRows: evidencePredictionRows.length,
          catalystPriceReactionRows: predictionDebug.catalystPriceReactionRows,
          rawPredictionRows,
          staleStoredPredictionRows: predictionDebug.staleStoredPredictionRows,
          fallbackRows: displayFallbackRows.length,
          strictRows: predictionDebug.strictRows,
          candidatePoolRows: predictionDebug.candidatePoolRows,
          developingConfidenceDiagnostics: predictionDebug.developingConfidenceDiagnostics,
          developingMomentumRows: predictionDebug.developingMomentumRows,
          developingCandidateMinScore: predictionDebug.developingCandidateMinScore,
          includeDevelopingCandidates: predictionDebug.includeDevelopingCandidates,
          bestAvailableCandidateRows: predictionDebug.bestAvailableCandidateRows,
          topBestCandidateRows: predictionDebug.topBestCandidateRows,
          candidatePoolMin: predictionDebug.candidatePoolMin,
          suppressedFallbackRows: predictionDebug.suppressedFallbackRows,
          fallbackSetupCounts,
          activeSetupRows: predictionDebug.activeSetupRows,
          nearThresholdRows: predictionDebug.nearThresholdRows,
          finalRows: finalRows.length,
          rankedCandidateRows: predictionDebug.rankedCandidateRows,
          topFiveTickers: predictionDebug.topFiveTickers,
          pipelineStageCounts: predictionDebug.pipelineStageCounts,
          latestPredictionAt: predictionDebug.latestPredictionAt,
          predictionDate: predictionDebug.predictionDate,
          targetDate: predictionDebug.targetDate,
          missingFieldCounts,
          predictionRiskFlagCounts,
          predictionReadinessCounts,
          catalystReactionCounts,
          catalystQualityCounts,
          pendingOpenConfirmationCounts,
          firstReactionStateCounts,
          removedByFilterCounts,
          postmortemReport,
          warnings,
          thresholdPolicyVersion: predictionDebug.thresholdPolicyVersion,
          thresholdPolicy: predictionDebug.thresholdPolicy,
          discoveryUniverseLimit: predictionDebug.discoveryUniverseLimit,
        },
        next_session_model: {
          live_enabled: Boolean(modelDoc?.live_enabled),
          evidence_predictions_enabled: evidencePredictionRows.length > 0,
          status: predictionDebug.calibratorMode,
          target: modelDoc?.target || null,
          split_mode: modelDoc?.split_mode || null,
          selected_threshold: modelDoc?.selected_threshold ?? null,
          validation_status: modelDoc?.validation_status || null,
          validation_reason: modelDoc?.validation_reason || null,
          selected_metrics: modelDoc?.metrics?.selected || null,
          baseline_majority_accuracy: modelDoc?.metrics?.baseline_majority_accuracy ?? null,
          samples: modelDoc?.samples || modelDoc?.metrics?.samples || 0,
          min_samples: modelDoc?.min_samples || 0,
        },
        prediction_postmortem: postmortemReport,
        prediction_note: finalRows.length
          ? 'Showing strict predictions first, then developing opportunities that meet the evidence floor. Discovery tiers distinguish trade-ready rows from watch candidates.'
          : storedRows.length
            ? 'Stored next-day prediction rows exist, but none matched the positive-current-mover gate.'
          : actionableLiveRows.length
            ? 'No current stored next-day archive was found. Showing live prediction signals from Mongo; watch candidates remain separate.'
            : evidencePredictionRows.length
              ? 'Evidence-ranked next-session candidates are available, but current filters removed them.'
              : liveSignalRows.length
                ? 'Stored/live prediction rows exist, but none passed the improved entry gate. Showing screener watch candidates separately.'
                : 'No stored next-day predictions found. Showing screener watch candidates separately.',
        summary: {
          priced: finalRows.filter(row => row.price != null).length,
          stored_predictions: storedRows.length,
          live_prediction_signals: liveSignalRows.length,
          evidence_predictions: evidencePredictionRows.length,
          real_predictions: finalRows.length,
          strict_predictions: strictRows.length,
          candidate_pool_rows: candidatePoolRows.length,
          developing_momentum_rows: developingMomentumRows.length,
          developing_candidate_min_score: PREDICTION_DEVELOPING_CANDIDATE_MIN_SCORE,
          best_available_candidate_rows: topBestCandidateRows.length,
          top_5_best_candidate_rows: topBestCandidateRows.length,
          fallback_watch_candidates: fallbackRows.length,
          proxy_predictions: 0,
          threshold_policy_version: PREDICTION_THRESHOLD_POLICY_VERSION,
        },
        exchanges: Array.from(US_EXCHANGES),
        rolling_windows: {
          selected: windowOverride || 'adaptive',
          nano_micro: 5,
          small: 15,
          mid: 30,
          large: 60,
          mega: 120,
        },
        catalyst_window: {
          session: sessionContext.session,
          market_phase: sessionContext.market_phase,
          policy: sessionContext.catalyst_window_policy,
          start_sec: sessionContext.catalyst_window_start_sec,
          end_sec: sessionContext.catalyst_window_end_sec,
          next_session_date: sessionContext.next_session_date,
        },
        excluded: ['fallback rows are not mixed into real prediction rows', 'OTC', 'crypto', 'unpriced/article-only rows', 'non-US exchanges'],
        max_abs_change_pct: MAX_SIGNAL_CHANGE_PCT,
      })
    }

    const filterUniverse = data
    const filteredData = mirrorMode ? applyMirrorFilters(filterUniverse, req.query) : data
    const responseRows = (mirrorMode ? filteredData.slice(0, requestedLimit) : filteredData)
      .map(suppressBroadRoundupMainCatalyst)
      .map(withPredictionSetupClassification)
    const summaryRows = mirrorMode ? filteredData : responseRows
    const activeSocialRows = summaryRows.filter(row => Number(row.message_count || 0) > 0)
    const totalSocialMessages = summaryRows.reduce((sum, row) => sum + Number(row.message_count || 0), 0)
    const totalStocktwitsMessages = summaryRows.reduce((sum, row) => sum + Number(row.stocktwits_message_count || 0), 0)
    const totalSocialDensity = summaryRows.reduce((sum, row) => sum + Number(row.message_count || 0) / Math.max(1, Number(row.rolling_window_minutes || 30)), 0)

    res.json({
      ok: true,
      // `rows` is retained for legacy clients. The current dashboard requests
      // compact mode so the same large array is not serialized twice.
      ...(compact ? {} : { rows: responseRows }),
      tickers: responseRows,
      count: mirrorMode ? filteredData.length : responseRows.length,
      visible_count: responseRows.length,
      result_count: filteredData.length,
      universe_count: filterUniverse.length,
      data_load_mode: leanFullUniverseOverview ? 'lean_full_universe_overview' : 'live_enriched',
      prediction_session_context: sessionContext,
      catalyst_window: {
        session: sessionContext.session,
        market_phase: sessionContext.market_phase,
        policy: sessionContext.catalyst_window_policy,
        start_sec: sessionContext.catalyst_window_start_sec,
        end_sec: sessionContext.catalyst_window_end_sec,
        next_session_date: sessionContext.next_session_date,
      },
      filter_metadata: mirrorMode ? buildMirrorFilterMetadata(filterUniverse) : undefined,
      filter_state: mirrorMode ? {
        mirror: true,
        signal: req.query.signal || 'top_gainers',
        active_filter_count: [
          'signal', 'search', 'q', 'exchange', 'market_cap', 'market_cap_bucket', 'sector', 'industry', 'country',
          'price_range', 'price_min', 'price_max', 'rel_volume_min', 'rel_volume_max', 'volume_min', 'volume_max',
          'avg_volume_min', 'avg_volume_max', 'float_min', 'float_max', 'short_float_min', 'short_float_max',
          'session', 'quote_source', 'source', 'news_available', 'social_available', 'sentiment', 'prediction_direction',
          'decision_journey', 'catalyst',
        ].filter(key => req.query[key] != null && String(req.query[key]).trim() !== '').length,
      } : undefined,
      universe: 'NASDAQ / NYSE / AMEX listed stocks from numeric screeners',
      summary: {
        priced: summaryRows.filter(row => row.price != null).length,
        gainers: summaryRows.filter(row => Number(row.change_pct || 0) > 0).length,
        losers: summaryRows.filter(row => Number(row.change_pct || 0) < 0).length,
        unchanged: summaryRows.filter(row => Number(row.change_pct || 0) === 0).length,
        active_social: activeSocialRows.length,
        total_social_messages: totalSocialMessages,
        total_stocktwits_messages: totalStocktwitsMessages,
        avg_posts_per_active_social: activeSocialRows.length ? Number((totalSocialMessages / activeSocialRows.length).toFixed(2)) : 0,
        avg_social_density_per_ticker: summaryRows.length ? Number((totalSocialDensity / summaryRows.length).toFixed(3)) : 0,
      },
      exchanges: Array.from(US_EXCHANGES),
      rolling_windows: {
        selected: windowOverride || 'adaptive',
        nano_micro: 5,
        small: 15,
        mid: 30,
        large: 60,
        mega: 120,
      },
      excluded: ['OTC', 'crypto', 'unpriced/article-only rows', 'non-US exchanges'],
      max_abs_change_pct: MAX_SIGNAL_CHANGE_PCT,
    })
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// POST /api/screener/upsert  — upsert a single ticker
router.post('/upsert', async (req, res) => {
  try {
    const doc = await Screener.findOneAndUpdate(
      { ticker: req.body.ticker },
      { $set: { ...req.body, updated_at: new Date() } },
      { upsert: true, new: true }
    )
    res.json(doc)
  } catch (err) {
    res.status(500).json({ error: err.message })
  }
})

// Shared with routes/entryScreener.js and routes/exitScreener.js so the entry/
// exit screeners screen the exact same clean listed-US universe and social-
// activity stats as /api/screener.
export {
  normalizeScreenerRow,
  isCleanListedUsRow,
  loadAdaptiveSocialStatsForRows,
  predictionPeopleAttention,
  attachWatcherSqueezeEvidence,
  evaluatePredictionEntryThreshold,
  predictionMarketCapTier,
}

export default router
