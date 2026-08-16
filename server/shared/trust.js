export const TRUST = {
  BASE: 98,
  PENALTIES: {
    LOW_BATTERY: 8,
    TEST_MODE: 10,
    UNKNOWN_SOURCE: 30,
    HISTORY_SPAM: 20,
    PREVIOUS_FALSE_ALARM: 15,
  },
  WINDOW_MS: 10 * 60 * 1000,
  SPAM_THRESHOLD: 3,
  VERIFIED_MIN: 70,
}

export function computeTrust({ score, lowBattery, testMode, source, history = [] }) {
  let s = Math.max(0, Math.min(100, Math.round(score ?? TRUST.BASE)))

  if (lowBattery) s -= TRUST.PENALTIES.LOW_BATTERY
  if (testMode) s -= TRUST.PENALTIES.TEST_MODE
  if (!source || source === 'unknown' || source === 'UNKNOWN') s -= TRUST.PENALTIES.UNKNOWN_SOURCE

  const window = Date.now() - TRUST.WINDOW_MS
  const recent = history.filter((h) => h.ts >= window)
  if (recent.length >= TRUST.SPAM_THRESHOLD) s -= TRUST.PENALTIES.HISTORY_SPAM
  if (recent.some((h) => h.resolved === false)) s -= TRUST.PENALTIES.PREVIOUS_FALSE_ALARM

  return Math.max(0, Math.min(100, s))
}

export function verifiedFromScore(score) {
  return score >= TRUST.VERIFIED_MIN
}

export function trustLabel(score) {
  return score >= TRUST.VERIFIED_MIN ? 'VERIFIED' : 'UNVERIFIED'
}
