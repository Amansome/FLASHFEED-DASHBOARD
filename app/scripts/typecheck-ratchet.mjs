#!/usr/bin/env node
/**
 * Typecheck ratchet — blocks the build on NEW type errors without demanding a
 * cleanup of the existing ones.
 *
 * Why this exists: `vite build` does not typecheck. On 2026-07-26 a decision-map
 * change referenced `activeMiniTicker` inside a component where it was never a
 * prop. `tsc` reported it as TS2304 "Cannot find name"; the build passed anyway
 * and the hover tooltip threw a ReferenceError in production. Nothing in the
 * pipeline could have caught it.
 *
 * A plain `tsc --noEmit` gate would fail immediately on the ~78 pre-existing
 * errors, so it would just get disabled. Instead this compares against a
 * committed baseline and fails only on errors beyond it. The baseline is a
 * ceiling that may fall and must never rise.
 *
 *   npm run typecheck            # raw tsc, all errors
 *   npm run typecheck:ratchet    # baseline-compared (what the build runs)
 *   npm run typecheck:accept     # rewrite the baseline (review the diff!)
 *
 * Signatures deliberately EXCLUDE line/column numbers: adding a line to a file
 * would otherwise invalidate every signature below it and force a baseline
 * rewrite on every commit, which is how ratchets rot. A signature is
 * file + TS code + normalised message, counted — so a second identical error in
 * the same file is still caught as new.
 */
import { execFileSync } from 'node:child_process'
import { readFileSync, writeFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const APP_DIR = join(dirname(fileURLToPath(import.meta.url)), '..')
const BASELINE = join(APP_DIR, '.tsc-baseline.json')
const accept = process.argv.includes('--accept')

function runTsc() {
  try {
    execFileSync('npx', ['tsc', '--noEmit'], { cwd: APP_DIR, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
    return ''
  } catch (err) {
    // tsc exits non-zero when it reports errors; that's the normal path here.
    const out = `${err.stdout || ''}${err.stderr || ''}`
    if (!out.trim()) {
      console.error('typecheck-ratchet: tsc produced no output but failed — is typescript installed?')
      process.exit(2)
    }
    return out
  }
}

// "src/pages/Foo.tsx(12,34): error TS2304: Cannot find name 'bar'."
const LINE_RE = /^(.+?)\((\d+),(\d+)\): error (TS\d+): (.*)$/

/**
 * Some tsc messages embed an ABSOLUTE path (TS7016 quotes the resolved module
 * file). That path is the checkout root, so it differs between a dev machine and
 * the Docker build — the same error would hash to two different signatures and
 * the deploy would fail on an error the developer cannot reproduce. Caught
 * exactly that way: identical 78 errors, but `/build/node_modules/three/...` in
 * the container vs `/Users/.../app/node_modules/three/...` locally.
 */
function normaliseMessage(message) {
  return message
    .replace(/\s+/g, ' ')
    // any absolute path down to a node_modules dir -> repo-relative
    .replace(/(['"`])\/[^'"`]*?\/node_modules\//g, '$1node_modules/')
    .trim()
}
// Deliberately NOT also substituting APP_DIR: in the Docker build APP_DIR is
// '/build', which is a substring of the very path being normalised
// ('node_modules/three/build/three.module.js'), so the substitution mangled the
// result and the gate rejected an error already in the baseline. Anything that
// needs the checkout root stripped should get a rule anchored to a path prefix,
// not a bare substring replace.

function parse(output) {
  const counts = new Map()
  const samples = new Map()
  for (const line of output.split('\n')) {
    const m = LINE_RE.exec(line.trim())
    if (!m) continue
    const [, file, ln, , code, message] = m
    // Quoted identifiers stay in the signature — swapping one undefined name for
    // another is exactly the bug class this guards, so it must read as new.
    const sig = `${file}|${code}|${normaliseMessage(message)}`
    counts.set(sig, (counts.get(sig) || 0) + 1)
    if (!samples.has(sig)) samples.set(sig, `${file}:${ln} ${code}: ${message}`)
  }
  return { counts, samples }
}

const output = runTsc()
const { counts, samples } = parse(output)
const total = [...counts.values()].reduce((a, b) => a + b, 0)

if (accept) {
  const record = Object.fromEntries([...counts.entries()].sort(([a], [b]) => a.localeCompare(b)))
  writeFileSync(BASELINE, `${JSON.stringify({ total, signatures: record }, null, 2)}\n`)
  console.log(`typecheck-ratchet: baseline written — ${total} error(s) accepted.`)
  console.log('Review the diff before committing; this file is the ceiling the build enforces.')
  process.exit(0)
}

if (!existsSync(BASELINE)) {
  console.error(`typecheck-ratchet: no baseline at ${BASELINE}. Run: npm run typecheck:accept`)
  process.exit(2)
}

const base = JSON.parse(readFileSync(BASELINE, 'utf8'))
const baseSigs = base.signatures || {}

const introduced = []
for (const [sig, n] of counts) {
  const allowed = baseSigs[sig] || 0
  if (n > allowed) introduced.push({ sig, n, allowed, sample: samples.get(sig) })
}
const fixed = []
for (const [sig, allowed] of Object.entries(baseSigs)) {
  const n = counts.get(sig) || 0
  if (n < allowed) fixed.push({ sig, n, allowed })
}

if (introduced.length) {
  console.error('\n✗ typecheck-ratchet: NEW type errors — build blocked.\n')
  for (const item of introduced) {
    const extra = item.allowed ? ` (${item.n} now, ${item.allowed} in baseline)` : ''
    console.error(`  ${item.sample}${extra}`)
  }
  console.error(`\n  ${total} total vs ${base.total} baseline.`)
  console.error('  Fix the errors above. Only if they are genuinely acceptable:')
  console.error('    npm run typecheck:accept   (raises the ceiling — needs review)\n')
  process.exit(1)
}

console.log(`✓ typecheck-ratchet: no new type errors (${total} total, baseline ${base.total}).`)
if (fixed.length) {
  const gain = fixed.reduce((a, f) => a + (f.allowed - f.n), 0)
  console.log(`  ${gain} baseline error(s) now fixed — tighten with: npm run typecheck:accept`)
}
