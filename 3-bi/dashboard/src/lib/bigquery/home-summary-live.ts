import { runBigQuery } from "@/lib/bigquery/client";
import { buildSpeedToLeadQueries } from "@/lib/bigquery/speed-to-lead-live";
import { buildRevenueFunnelQueries } from "@/lib/bigquery/revenue-funnel-live";
import { buildRetentionQueries } from "@/lib/bigquery/retention-live";
import { buildCustomerActionsQueries } from "@/lib/bigquery/customer-actions-live";
import { buildRecoveryQueries } from "@/lib/bigquery/recovery-live";
import { buildLeadMagnetQueries } from "@/lib/bigquery/lead-magnets-live";
import type { DashboardRow } from "@/types/dashboard-data";

export const HOME_TIME_RANGE_OPTIONS = [
  { value: "7d", label: "7D", description: "Last 7 days." },
  { value: "30d", label: "30D", description: "Last 30 days." },
  { value: "90d", label: "90D", description: "Last 90 days." },
] as const;

export type HomeTimeRange = (typeof HOME_TIME_RANGE_OPTIONS)[number]["value"];

const DEFAULT_HOME_TIME_RANGE: HomeTimeRange = "7d";

export function normalizeHomeTimeRange(value: string | null | undefined): HomeTimeRange {
  const normalized = value?.toLowerCase();
  const option = HOME_TIME_RANGE_OPTIONS.find((candidate) => candidate.value === normalized);
  return option?.value ?? DEFAULT_HOME_TIME_RANGE;
}

export type HomeTileResult<T> = { ok: true; rows: T[] } | { ok: false; error: string };

export type HomeSummary = {
  filters: {
    timeRange: HomeTimeRange;
    timeRangeLabel: string;
    timeRangeOptions: ReadonlyArray<{ value: string; label: string; description: string }>;
  };
  // Range each tile actually queried. Differs from filters.timeRange when a
  // tile's mart does not support that window (e.g. Revenue/Retention floor 7d -> 30d).
  // null = tile ignores time range entirely (Actions).
  tileRanges: {
    speed_to_lead: HomeTimeRange;
    revenue: "30d" | "90d";
    retention: "30d" | "90d";
    actions: null;
    recovery: HomeTimeRange;
    lead_magnets: HomeTimeRange;
  };
  tiles: {
    speed_to_lead: {
      overall: HomeTileResult<DashboardRow>;
      prior: HomeTileResult<DashboardRow>;
    };
    revenue: HomeTileResult<DashboardRow>;
    retention: HomeTileResult<DashboardRow>;
    actions: HomeTileResult<DashboardRow>;
    recovery: HomeTileResult<DashboardRow>;
    lead_magnets: HomeTileResult<DashboardRow>;
  };
  generatedAt: string;
};

type GetHomeSummaryOptions = {
  timeRange?: string | null;
};

// Revenue and Retention marts do not support 7d windows. Floor 7d -> 30d for those tiles.
function flooredTo30d(range: HomeTimeRange): "30d" | "90d" {
  return range === "90d" ? "90d" : "30d";
}

async function settle<T>(promise: Promise<T[]>): Promise<HomeTileResult<T>> {
  try {
    const rows = await promise;
    return { ok: true, rows };
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    return { ok: false, error };
  }
}

type SummaryCacheEntry = { value: HomeSummary; ts: number };
const SUMMARY_CACHE_TTL_MS = 60_000;
const summaryCache = new Map<HomeTimeRange, SummaryCacheEntry>();

export async function getHomeSummary(options: GetHomeSummaryOptions = {}): Promise<HomeSummary> {
  const timeRange = normalizeHomeTimeRange(options.timeRange);
  const cached = summaryCache.get(timeRange);
  if (cached && Date.now() - cached.ts < SUMMARY_CACHE_TTL_MS) {
    return cached.value;
  }

  const revenueRange = flooredTo30d(timeRange);
  const retentionRange = flooredTo30d(timeRange);

  const stl = buildSpeedToLeadQueries(timeRange);
  const rev = buildRevenueFunnelQueries(revenueRange);
  const ret = buildRetentionQueries(retentionRange, "recovery_queue");
  const act = buildCustomerActionsQueries("all", false, 1);
  const rec = buildRecoveryQueries(timeRange);
  const lm = buildLeadMagnetQueries(timeRange);

  const [
    speedOverall,
    speedPrior,
    revenueSummary,
    retentionSummary,
    actionsSummary,
    recoverySummary,
    leadMagnetsSummary,
  ] = await Promise.all([
    settle(runBigQuery(stl.speed_to_lead_overall)),
    settle(runBigQuery(stl.speed_to_lead_overall_prior)),
    settle(runBigQuery(rev.revenue_funnel_summary)),
    settle(runBigQuery(ret.retention_summary)),
    settle(runBigQuery(act.customer_action_summary)),
    settle(runBigQuery(rec.recovery_summary)),
    settle(runBigQuery(lm.lead_magnet_summary)),
  ]);

  const activeOption = HOME_TIME_RANGE_OPTIONS.find((o) => o.value === timeRange);

  const summary: HomeSummary = {
    filters: {
      timeRange,
      timeRangeLabel: activeOption?.label ?? "7D",
      timeRangeOptions: HOME_TIME_RANGE_OPTIONS.map((o) => ({ ...o })),
    },
    tileRanges: {
      speed_to_lead: timeRange,
      revenue: revenueRange,
      retention: retentionRange,
      actions: null,
      recovery: timeRange,
      lead_magnets: timeRange,
    },
    tiles: {
      speed_to_lead: { overall: speedOverall, prior: speedPrior },
      revenue: revenueSummary,
      retention: retentionSummary,
      actions: actionsSummary,
      recovery: recoverySummary,
      lead_magnets: leadMagnetsSummary,
    },
    generatedAt: new Date().toISOString(),
  };

  summaryCache.set(timeRange, { value: summary, ts: Date.now() });
  return summary;
}
