import { runBigQuery } from "@/lib/bigquery/client";
import type { QueryTier } from "@/lib/bigquery/query-tiers";
import type { DashboardRow } from "@/types/dashboard-data";

export type { QueryTier } from "@/lib/bigquery/query-tiers";

export type TieredQuery = {
  name: string;
  sql: string;
  tier: QueryTier;
  params?: Record<string, string | number | boolean | null>;
};

export type TieredQueryResult = {
  rows: Record<string, DashboardRow[]>;
  queryErrors: Record<string, string>;
  criticalError?: string;
  failedTiers: QueryTier[];
};

function getInjectedFailureName(): string | undefined {
  if (process.env.ALLOW_FAIL_INJECTION !== "1") return undefined;
  if (process.env.NODE_ENV === "production") return undefined;
  return process.env.DASHBOARD_FAIL_QUERY?.trim() || undefined;
}

function messageOf(reason: unknown): string {
  if (reason instanceof Error) return reason.message;
  return String(reason);
}

export async function runTieredQueries(
  queries: TieredQuery[],
): Promise<TieredQueryResult> {
  const injectedFailure = getInjectedFailureName();

  const settled = await Promise.allSettled(
    queries.map(async (q) => {
      if (injectedFailure && q.name === injectedFailure) {
        throw new Error(`Injected failure for ${q.name} (DASHBOARD_FAIL_QUERY)`);
      }
      return runBigQuery(q.sql, q.params);
    }),
  );

  const rows: Record<string, DashboardRow[]> = {};
  const queryErrors: Record<string, string> = {};
  const failedTierSet = new Set<QueryTier>();
  let criticalError: string | undefined;

  settled.forEach((result, index) => {
    const q = queries[index];
    if (result.status === "fulfilled") {
      rows[q.name] = result.value;
      return;
    }
    const message = messageOf(result.reason);
    rows[q.name] = [];
    queryErrors[q.name] = message;
    failedTierSet.add(q.tier);
    console.error(`[bq-tier] ${q.tier} ${q.name}: ${message}`);
    if (q.tier === "critical" && !criticalError) {
      criticalError = `${q.name}: ${message}`;
    }
  });

  return {
    rows,
    queryErrors,
    criticalError,
    failedTiers: [...failedTierSet],
  };
}
