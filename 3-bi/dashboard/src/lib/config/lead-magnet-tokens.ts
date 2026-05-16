/**
 * Lead-magnet visual logic tokens.
 *
 * Sibling of speed-to-lead-tokens.ts — shares helper signatures so review
 * patterns transfer 1:1. Visual color tokens live in globals.css (`--od-*`);
 * this file is JS logic only.
 *
 * Reference: 00 Human/30 Projects/Lead Magnet Redesign/Lead Magnet Spec v1.md
 * Section 7.1 (Tokens file).
 */

import {
  type Tone,
  formatRatioWithDenominator,
  shouldSuppressDelta,
  toneForRatio,
} from "./speed-to-lead-tokens";

export {
  type Tone,
  formatRatioWithDenominator,
  shouldSuppressDelta,
  toneForRatio,
};

export type LeadMagnetMetricCategory =
  | "revenue_per_lead"
  | "top_3_concentration"
  | "attribution_coverage"
  | "cold_bench_ratio";

/**
 * Tone scales per lead-magnet metric. Two shapes coexist:
 *   - ratio-style (0-1, higher is better): top_3_concentration, attribution_coverage
 *   - absolute-$ (higher is better): revenue_per_lead
 *   - inverted-ratio (0-1, lower is better): cold_bench_ratio
 *
 * Initial values are starting points — Slice 7 tunes against real BigQuery
 * data and amends this file + the spec change log.
 */
export const TONE_SCALES: Record<LeadMagnetMetricCategory, { good: number; warn: number }> = {
  revenue_per_lead: { good: 60, warn: 20 },           // absolute dollars
  top_3_concentration: { good: 0.7, warn: 0.4 },     // ratio; healthy concentration ≥ 70%
  attribution_coverage: { good: 0.85, warn: 0.65 }, // ratio; 1 - attribution_gap_share
  cold_bench_ratio: { good: 0.2, warn: 0.4 },        // inverted ratio; smaller cold bench = better
};

/**
 * Minimum N (buyers) below which a per-magnet delta or sparkline is suppressed
 * to avoid tiny-cohort noise. Matches MIN_N_FOR_DELTA in speed-to-lead-tokens
 * but tuned for the buyer-grain reality (fewer events than first-touch events).
 */
export const MIN_N_FOR_DELTA = {
  magnet_buyers: 10,
} as const;

/**
 * Window for the "Recently working" operational strip — independent of the
 * date picker. Tune later if observed cadence suggests 14 or 30 is the
 * better operational window. See Spec v1 §5.1.
 */
export const RECENT_BUYER_WINDOW_DAYS = 7;

/**
 * Tone for an absolute value metric (e.g. $/lead). Higher is better.
 */
export function toneForValue(
  value: number | null | undefined,
  scale: { good: number; warn: number },
): Tone {
  if (value === null || value === undefined || !Number.isFinite(value)) return "stale";
  if (value >= scale.good) return "ok";
  if (value >= scale.warn) return "warning";
  return "danger";
}

/**
 * Tone for an inverted-ratio metric where lower is better (e.g. cold-bench share).
 */
export function toneForInvertedRatio(
  value: number | null | undefined,
  scale: { good: number; warn: number },
): Tone {
  if (value === null || value === undefined || !Number.isFinite(value)) return "stale";
  if (value <= scale.good) return "ok";
  if (value <= scale.warn) return "warning";
  return "danger";
}

/**
 * Plain-language label for the dbt `queue_status` enum. Spec v1 §5.3 + §7.2.
 * Used by the leaderboard chip; values must never leak raw to the UI.
 */
export type QueueStatus =
  | "healthy"
  | "repair_candidate"
  | "kill_candidate"
  | "retire_recommended_pending_override"
  | "insufficient_sample";

export const QUEUE_STATUS_LABEL: Record<QueueStatus, string> = {
  healthy: "Healthy",
  repair_candidate: "Repair",
  kill_candidate: "Retire",
  retire_recommended_pending_override: "Retire",
  insufficient_sample: "Needs more data",
};

export function queueStatusLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return QUEUE_STATUS_LABEL[value as QueueStatus] ?? "—";
}

/**
 * Tone keyword for the status chip. Maps each plain-language status to one
 * encoding (no stacking tones — Spec v1 §7.6).
 */
export function queueStatusTone(value: string | null | undefined): Tone {
  if (!value) return "stale";
  switch (value as QueueStatus) {
    case "healthy":
      return "ok";
    case "repair_candidate":
      return "warning";
    case "kill_candidate":
    case "retire_recommended_pending_override":
      return "danger";
    case "insufficient_sample":
      return "neutral";
    default:
      return "stale";
  }
}

/**
 * Compact currency formatter — `$84,200`, `$1.2K`, `$0`.
 * Compact form for hero/strip; full form for table cells.
 */
export function formatCurrencyFull(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (value === 0) return "$0";
  const rounded = Math.round(value);
  return `$${rounded.toLocaleString("en-US")}`;
}

export function formatCurrencyCompact(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  if (Math.abs(value) >= 1_000_000) return `$${(value / 1_000_000).toFixed(1)}M`;
  if (Math.abs(value) >= 10_000) return `$${(value / 1_000).toFixed(0)}K`;
  if (Math.abs(value) >= 1_000) return `$${(value / 1_000).toFixed(1)}K`;
  return formatCurrencyFull(value);
}

/**
 * Shared row-value coercers. Re-implemented locally (rather than imported from
 * SpeedToLeadDashboard) so this module stays in the server-component side of
 * the boundary — SpeedToLeadDashboard.tsx is `"use client"`, which makes its
 * exports unsafe to call from server components.
 */
export function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

export function stringValue(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  return undefined;
}

export function formatNumber(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US").format(value);
}
