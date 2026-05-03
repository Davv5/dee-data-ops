import { BigQuery } from "@google-cloud/bigquery";
import { deeConfig } from "@/lib/config/dee";
import type { DashboardRow } from "@/types/dashboard-data";

let client: BigQuery | null = null;

function getBigQueryClient() {
  client ??= new BigQuery({
    projectId: deeConfig.bigQuery.projectId,
  });

  return client;
}

export async function runBigQuery(sql: string): Promise<DashboardRow[]> {
  const [rows] = await getBigQueryClient().query({
    query: sql,
    location: process.env.BIGQUERY_LOCATION ?? "US",
    useLegacySql: false,
  });

  return JSON.parse(JSON.stringify(rows)) as DashboardRow[];
}
