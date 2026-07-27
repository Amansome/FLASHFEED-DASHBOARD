// NOT REQUIRED ANY MORE — kept as a manual/one-off tool, not a setup step.
//
// Every index defined below is also ensured on boot by ensureRuntimeIndexes()
// in ../index.js, which is awaited before the server starts serving. Running
// this script is therefore optional; forgetting to run it is no longer a way to
// break the app. It used to be: /api/articles/recent-lite hinted
// feed_sort_time_desc, which only existed here, and returned 500 on every
// request against any database where this had not been run.
//
// If you add an index here, add it to ensureRuntimeIndexes() as well, or the
// same class of failure comes back. The two lists were reconciled and verified
// equal (45/45 recreated from an index-free database by startup alone); nothing
// else in the repo references this file except the `indexes` npm script.
import mongoose from 'mongoose'

const MONGO_URI = process.env.MONGODB_URI || process.env.MONGO_URI || 'mongodb://localhost:27017/feedflash'
const DB_NAME = process.env.MONGO_DB || process.env.DB_NAME || 'feedflash'

async function addIndexes() {
  await mongoose.connect(MONGO_URI)
  const db = mongoose.connection.db

  console.log('Adding MongoDB indexes...\n')

  // Articles collection - most important for performance
  const articlesIndexes = [
    { keys: { ticker: 1, publish_date: -1 }, name: 'ticker_publish_date' },
    { keys: { tickers: 1, publish_date: -1 }, name: 'tickers_publish_date' },
    { keys: { symbol: 1, publish_date: -1 }, name: 'symbol_publish_date' },
    { keys: { symbols: 1, publish_date: -1 }, name: 'symbols_publish_date' },
    { keys: { article_kind: 1, publish_date: -1 }, name: 'article_kind_publish_date' },
    { keys: { ticker: 1, sentiment: 1 }, name: 'ticker_sentiment' },
    { keys: { publish_date: -1 }, name: 'publish_date' },
    { keys: { fetched_date: -1 }, name: 'fetched_date_desc' },
    { keys: { detected_at: -1 }, name: 'detected_at_desc' },
    { keys: { source: 1, publish_date: -1 }, name: 'source_publish_date' },
    { keys: { sentiment: 1, publish_date: -1 }, name: 'sentiment_publish_date' },
    { keys: { feed_sort_time: -1 }, name: 'feed_sort_time_desc' },
    { keys: { ticker: 1, event_sec: -1 }, name: 'ticker_event_sec_desc' },
    { keys: { tickers: 1, event_sec: -1 }, name: 'tickers_event_sec_desc' },
    { keys: { market_session: 1, event_sec: -1 }, name: 'market_session_event_sec_desc' },
    { keys: { catalystCategory: 1, event_sec: -1 }, name: 'catalyst_category_event_sec_desc' },
  ]

  for (const idx of articlesIndexes) {
    try {
      await db.collection('articles').createIndex(idx.keys, { name: idx.name, background: true })
      console.log(`✓ Created index: ${idx.name} on articles`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name}:`, err.message)
      }
    }
  }

  // Social posts collection
  const socialIndexes = [
    { keys: { ticker: 1, fetched_at: -1 }, name: 'ticker_fetched_at' },
    { keys: { platform: 1, fetched_at: -1 }, name: 'platform_fetched_at' },
    { keys: { sentiment: 1, fetched_at: -1 }, name: 'sentiment_fetched_at' },
  ]

  for (const idx of socialIndexes) {
    try {
      await db.collection('socials').createIndex(idx.keys, { name: idx.name, background: true })
      console.log(`✓ Created index: ${idx.name} on socials`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name}:`, err.message)
      }
    }
  }

  // Screener collection
  const screenerIndexes = [
    { keys: { ticker: 1 }, options: { unique: true, background: true }, name: 'ticker' },
    { keys: { quote_updated_at: -1, change_pct: -1 }, name: 'quote_updated_change_desc' },
    { keys: { rel_volume: -1, volume: -1 }, name: 'rel_volume_volume_desc' },
    { keys: { threshold_feature_updated_at: -1 }, name: 'threshold_feature_updated_desc' },
    { keys: { threshold_feature_policy_version: 1, threshold_feature_status: 1 }, name: 'threshold_policy_status' },
    { keys: { threshold_feature_policy_version: 1, threshold_feature_window_minutes: 1, threshold_feature_status: 1, threshold_feature_updated_at: -1 }, name: 'threshold_policy_window_status_updated' },
    { keys: { threshold_feature_policy_version: 1, threshold_setup_score: -1, rel_volume: -1 }, name: 'threshold_policy_setup_score_relvol' },
  ]

  for (const idx of screenerIndexes) {
    try {
      await db.collection('screeners').createIndex(idx.keys, { ...(idx.options || { background: true }), name: idx.name })
      console.log(`✓ Created index: ${idx.name} on screeners`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name} on screeners:`, err.message)
      }
    }
  }

  const sourceStatusIndexes = [
    { keys: { source: 1 }, name: 'source' },
    { keys: { type: 1, last_checked_at: -1 }, name: 'type_last_checked_desc' },
    { keys: { status: 1, last_success_at: -1 }, name: 'status_last_success_desc' },
  ]

  for (const idx of sourceStatusIndexes) {
    try {
      await db.collection('source_status').createIndex(idx.keys, { name: idx.name, background: true })
      console.log(`✓ Created index: ${idx.name} on source_status`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name} on source_status:`, err.message)
      }
    }
  }

  const predictionSignalIndexes = [
    { keys: { ticker: 1, signal_sec: -1 }, name: 'ticker_signal_sec_desc' },
    { keys: { label_status: 1, signal_sec: -1 }, name: 'label_status_signal_sec_desc' },
    { keys: { signal_sec: -1, ticker: 1, entry_price: 1 }, name: 'signal_sec_ticker_entry_price' },
    { keys: { 'entry_signal.status': 1, signal_sec: -1 }, name: 'entry_signal_status_signal_sec' },
    { keys: { 'entry_signal.entry_ready': 1, signal_sec: -1 }, name: 'entry_ready_signal_sec' },
    { keys: { threshold_policy_version: 1, signal_sec: -1 }, name: 'threshold_policy_version_signal_sec' },
    { keys: { 'entry_signal.policy_version': 1, signal_sec: -1 }, name: 'entry_policy_version_signal_sec' },
    { keys: { 'labels.return_5m.label_source': 1, signal_sec: -1 }, name: 'return_5m_label_source_signal_sec' },
    { keys: { 'labels.return_15m.label_source': 1, signal_sec: -1 }, name: 'return_15m_label_source_signal_sec' },
    { keys: { 'labels.return_60m.label_source': 1, signal_sec: -1 }, name: 'return_60m_label_source_signal_sec' },
    { keys: { 'labels.return_5m.missing_reason': 1, signal_sec: -1 }, name: 'return_5m_missing_reason_signal_sec' },
  ]

  for (const idx of predictionSignalIndexes) {
    try {
      await db.collection('prediction_signals').createIndex(idx.keys, { name: idx.name, background: true })
      console.log(`✓ Created index: ${idx.name} on prediction_signals`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name} on prediction_signals:`, err.message)
      }
    }
  }

  const activeContextIndexes = [
    { keys: { ticker: 1 }, options: { unique: true, background: true }, name: 'ticker_unique' },
    { keys: { threshold_policy_version: 1, signal_sec: -1 }, name: 'threshold_policy_signal_sec' },
    { keys: { 'entry_signal.status': 1, signal_sec: -1 }, name: 'entry_status_signal_sec' },
    { keys: { 'entry_signal.entry_ready': 1, signal_sec: -1 }, name: 'entry_ready_signal_sec' },
    { keys: { rank: 1, signal_sec: -1 }, name: 'rank_signal_sec' },
  ]

  for (const idx of activeContextIndexes) {
    try {
      await db.collection('active_ticker_context').createIndex(idx.keys, { ...(idx.options || { background: true }), name: idx.name })
      console.log(`✓ Created index: ${idx.name} on active_ticker_context`)
    } catch (err) {
      if (err.codeName === 'IndexOptionsConflict') {
        console.log(`- Index already exists: ${idx.name}`)
      } else {
        console.error(`✗ Failed to create index ${idx.name} on active_ticker_context:`, err.message)
      }
    }
  }

  console.log('\n✓ Index setup complete')
  await mongoose.disconnect()
}

addIndexes().catch(err => {
  console.error('Fatal error:', err)
  process.exit(1)
})
