import { deeConfig } from "@/lib/config/dee";

export type QueryContract = {
  name: string;
  owner: "bq-ingest-report" | "dbt-mart";
  table: string;
  description: string;
  status: "placeholder" | "ready";
};

const table = (name: string) =>
  `${deeConfig.bigQuery.projectId}.${deeConfig.bigQuery.dataset}.${name}`;

export const queryContracts = {
  speed_to_lead_overall: {
    name: "speed_to_lead_overall",
    owner: "bq-ingest-report",
    table: table(deeConfig.bigQuery.tables.speedToLeadOverall),
    description: "Org-wide Speed-to-Lead headline metrics.",
    status: "ready",
  },
  speed_to_lead_daily: {
    name: "speed_to_lead_daily",
    owner: "bq-ingest-report",
    table: table(deeConfig.bigQuery.tables.speedToLeadDaily),
    description: "Daily Speed-to-Lead trend.",
    status: "ready",
  },
  speed_to_lead_by_rep: {
    name: "speed_to_lead_by_rep",
    owner: "bq-ingest-report",
    table: table(deeConfig.bigQuery.tables.repScorecardWeek),
    description: "Rep-week scorecard used for the first rep breakdown.",
    status: "ready",
  },
  speed_to_lead_week: {
    name: "speed_to_lead_week",
    owner: "bq-ingest-report",
    table: table(deeConfig.bigQuery.tables.speedToLeadWeek),
    description: "Weekly Speed-to-Lead rollup.",
    status: "ready",
  },
  freshness: {
    name: "freshness",
    owner: "bq-ingest-report",
    table: table(deeConfig.bigQuery.tables.speedToLeadFact),
    description: "Max mart_refreshed_at across the Speed-to-Lead fact surface.",
    status: "ready",
  },
} satisfies Record<string, QueryContract>;

export type QueryName = keyof typeof queryContracts;
