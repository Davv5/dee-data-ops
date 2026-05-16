/**
 * Speed-to-Lead visual logic tokens.
 *
 * Resolves the inconsistent-threshold issue documented in chart-critique.md
 * by centralising tone scales and tiny-denominator suppression rules.
 * Visual color tokens live in globals.css (`--od-*`); this file is JS logic only.
 *
 * Reference: 00 Human/30 Projects/Speed-to-Lead Redesign/Speed-to-Lead Spec v2.md
 * Section 7 (Visual & Encoding Conventions) and Section 7.1 (Tokens file).
 */

export type Tone = "ok" | "warning" | "danger" | "stale" | "neutral" | "phase-pending";

export type MetricCategory = "sla" | "coverage" | "conversion";

/**
 * One published tone scale per metric category. No per-component divergence —
 * if a metric doesn't fit one of these scales, add a new category here rather
 * than inventing local thresholds.
 *
 * Values are ratios (0-1). "good" and above → ok. Between "warn" and "good" →
 * warning. Below "warn" → danger.
 */
export const TONE_SCALES: Record<MetricCategory, { good: number; warn: number }> = {
  // SLA target tuned 2026-05-16 — see SLA_TARGET in SpeedToLeadDashboard.tsx.
  sla: { good: 0.5, warn: 0.3 },
  coverage: { good: 0.9, warn: 0.7 },
  conversion: { good: 0.3, warn: 0.15 },
};

/**
 * Minimum N (denominator volume) below which a percentage metric should
 * suppress its delta — the % itself can still render with its raw count,
 * but the delta-vs-prior pill is greyed to avoid ±N-points noise on tiny
 * cohorts (the DeltaPill swinging ±66pt on n=3 problem from chart-critique).
 */
export const MIN_N_FOR_DELTA: Record<MetricCategory | "default", number> = {
  default: 20,
  sla: 30,
  conversion: 15,
  coverage: 20,
};

export function toneForRatio(value: number | null | undefined, category: MetricCategory): Tone {
  if (value === null || value === undefined || !Number.isFinite(value)) return "stale";
  const scale = TONE_SCALES[category];
  if (value >= scale.good) return "ok";
  if (value >= scale.warn) return "warning";
  return "danger";
}

export function shouldSuppressDelta(denominator: number | null | undefined, category: MetricCategory | "default" = "default"): boolean {
  if (denominator === null || denominator === undefined || !Number.isFinite(denominator)) return true;
  return denominator < MIN_N_FOR_DELTA[category];
}

/**
 * Inline-denominator formatter — "73% (8/11)" not "73%".
 * Resolves the buried-denominators issue from chart-critique.
 */
export function formatRatioWithDenominator(
  numerator: number | null | undefined,
  denominator: number | null | undefined,
  options: { digits?: number } = {},
): string {
  if (
    numerator === null || numerator === undefined || !Number.isFinite(numerator) ||
    denominator === null || denominator === undefined || !Number.isFinite(denominator) || denominator === 0
  ) {
    return "—";
  }
  const pct = (numerator / denominator) * 100;
  const digits = options.digits ?? (pct >= 10 ? 0 : 1);
  return `${pct.toFixed(digits)}% (${numerator}/${denominator})`;
}
