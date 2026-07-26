import test from 'node:test'
import assert from 'node:assert/strict'

import {
  evaluatePredictionEntryThreshold,
  PREDICTION_THRESHOLD_POLICY,
  PREDICTION_THRESHOLD_POLICY_VERSION,
  predictionMarketCapTier,
  predictionPolicyCacheNamespace,
} from '../lib/predictionThresholdPolicy.js'

const baseRow = {
  ticker: 'TEST',
  market_cap_bucket: 'Mid',
  market_cap: 3_000_000_000,
  shares_float: 60_000_000,
  change_pct: 5,
  rel_volume: 3,
  article_count: 1,
  message_count: 8,
  social_sentiment: 0.2,
  price_density_correlation: 0.42,
  previous_price_density_correlation: 0.37,
  threshold_pre_return_60m_pct: 2,
  threshold_trailing_60m_messages: 8,
}

test('v11 policy metadata is the single live threshold version', () => {
  assert.equal(PREDICTION_THRESHOLD_POLICY.version, PREDICTION_THRESHOLD_POLICY_VERSION)
  assert.equal(PREDICTION_THRESHOLD_POLICY.candidateRule.windowMinutes, 120)
  assert.equal(PREDICTION_THRESHOLD_POLICY.candidateRule.thresholdC, 0.38)
  assert.equal(predictionPolicyCacheNamespace(), `prediction:${PREDICTION_THRESHOLD_POLICY_VERSION}`)
})

test('valid v11 cross passes with controlled active momentum', () => {
  const result = evaluatePredictionEntryThreshold(baseRow)
  assert.equal(result.policyVersion, PREDICTION_THRESHOLD_POLICY_VERSION)
  assert.equal(result.passed, true)
  assert.equal(result.status, 'entry_passed')
  assert.equal(result.setupStatus, 'entry_passed')
  assert.equal(result.thresholdC, 0.38)
  assert.equal(result.minSignalChangePct, 0)
  assert.equal(result.maxSignalChangePct, 12)
})

test('active momentum band rejects otherwise valid crosses', () => {
  const result = evaluatePredictionEntryThreshold({ ...baseRow, change_pct: 18 })
  assert.equal(result.passed, false)
  assert.equal(result.status, 'active_momentum_band_rejected')
  assert.ok(result.rejectionReasons.some(reason => reason.includes('active_move_18.00pct_gt_12pct')))
})

test('opening volatility guard rejects fragile early regular-session crosses', () => {
  const earlyOpenRow = {
    ...baseRow,
    threshold_feature_snapshot_sec: Date.UTC(2026, 6, 24, 13, 35, 0) / 1000, // 09:35 ET
    article_count: 0,
    message_count: 0,
    social_sentiment: 0,
    threshold_pre_return_60m_pct: 2.2,
    threshold_trailing_60m_messages: 4,
  }
  const result = evaluatePredictionEntryThreshold(earlyOpenRow)
  assert.equal(result.passed, false)
  assert.equal(result.status, 'opening_volatility_rejected')
  assert.equal(result.openingVolatilityGuard.active, true)
  assert.equal(result.openingVolatilityGuard.minutes_since_regular_open, 5)
  assert.ok(result.rejectionReasons.some(reason => reason.includes('opening_volatility_guard_5m_after_open')))
})

test('ultra-low and nano rows require stronger evidence and message count', () => {
  const nanoRow = {
    ...baseRow,
    market_cap_bucket: 'Micro',
    shares_float: 1_000_000,
    article_count: 0,
    message_count: 0,
    social_sentiment: 0,
    threshold_trailing_60m_messages: 8,
  }
  const result = evaluatePredictionEntryThreshold(nanoRow)
  assert.equal(predictionMarketCapTier(nanoRow), 'Nano')
  assert.equal(result.minTrailing60Messages, 12)
  assert.equal(result.passed, false)
  assert.equal(result.status, 'low_message_density_rejected')
})

test('feature object fallback matches row-field evaluation', () => {
  const { price_density_correlation, previous_price_density_correlation, threshold_pre_return_60m_pct, threshold_trailing_60m_messages, ...rowWithoutFeatures } = baseRow
  const result = evaluatePredictionEntryThreshold(rowWithoutFeatures, {
    price_density_correlation,
    previous_price_density_correlation,
    threshold_pre_return_60m_pct,
    threshold_trailing_60m_messages,
  })
  assert.equal(result.passed, true)
  assert.equal(result.status, 'entry_passed')
})

test('explicit threshold profile override changes the evaluated gate', () => {
  const override = {
    label: 'test_override_c060',
    policyVersion: 'test_override_policy',
    windowMinutes: 77,
    smoothingMinutes: 77,
    thresholdC: 0.6,
    setupNearThresholdBand: 0.02,
    maxPreSignalReturn60mPct: 4,
    minTrailing60Messages: 9,
    minSignalChangePct: 0,
    maxSignalChangePct: 12,
  }
  const result = evaluatePredictionEntryThreshold(baseRow, override)
  assert.equal(result.policyVersion, 'test_override_policy')
  assert.equal(result.overrideProfile, 'test_override_c060')
  assert.equal(result.profile.windowMinutes, 77)
  assert.equal(result.thresholdC, 0.6)
  assert.equal(result.minTrailing60Messages, 9)
  assert.equal(result.passed, false)
  assert.equal(result.status, 'entry_not_crossed')
})

test('feature object and threshold override can be supplied together', () => {
  const { price_density_correlation, previous_price_density_correlation, threshold_pre_return_60m_pct, threshold_trailing_60m_messages, ...rowWithoutFeatures } = baseRow
  const result = evaluatePredictionEntryThreshold(rowWithoutFeatures, {
    price_density_correlation,
    previous_price_density_correlation,
    threshold_pre_return_60m_pct,
    threshold_trailing_60m_messages,
  }, {
    label: 'test_override_c040',
    thresholdC: 0.4,
    minTrailing60Messages: 3,
  })
  assert.equal(result.overrideProfile, 'test_override_c040')
  assert.equal(result.thresholdC, 0.4)
  assert.equal(result.passed, true)
  assert.equal(result.status, 'entry_passed')
})
