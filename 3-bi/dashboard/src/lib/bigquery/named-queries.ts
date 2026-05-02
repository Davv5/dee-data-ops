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
  speed_to_lead_quality_summary: {
    name: "speed_to_lead_quality_summary",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_*`,
    description:
      "Plain-English follow-up health: worked, reached, human follow-up, automation, and unworked leads.",
    status: "ready",
  },
  speed_to_lead_follow_up_counts: {
    name: "speed_to_lead_follow_up_counts",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_*`,
    description:
      "Follow-up counts with explicit denominator columns for team-facing dashboard reading.",
    status: "ready",
  },
  speed_to_lead_first_work_by_rep: {
    name: "speed_to_lead_first_work_by_rep",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_* + Core.dim_users`,
    description:
      "Who or what first worked each lead after the Speed-to-Lead trigger.",
    status: "ready",
  },
  speed_to_lead_phone_reach_by_rep: {
    name: "speed_to_lead_phone_reach_by_rep",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_* + Core.dim_users`,
    description:
      "Who or what first reached leads by phone after the Speed-to-Lead trigger.",
    status: "ready",
  },
  speed_to_lead_attribution_confidence: {
    name: "speed_to_lead_attribution_confidence",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_* + Core.dim_users`,
    description:
      "How many reached-by-phone events are tied to named reps versus identities that still need mapping.",
    status: "ready",
  },
  speed_to_lead_not_worked_aging: {
    name: "speed_to_lead_not_worked_aging",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_*`,
    description:
      "Aging buckets for lead events that have no post-trigger outbound follow-up.",
    status: "ready",
  },
  speed_to_lead_reached_examples: {
    name: "speed_to_lead_reached_examples",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_* + Marts.dim_golden_contact`,
    description:
      "Recent lead-level examples for answered or completed outbound calls.",
    status: "ready",
  },
  speed_to_lead_first_attempt_outcomes: {
    name: "speed_to_lead_first_attempt_outcomes",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_*`,
    description:
      "What happened first after a lead raised their hand, grouped into team-readable outcomes.",
    status: "ready",
  },
  speed_to_lead_business_hours: {
    name: "speed_to_lead_business_hours",
    owner: "bq-ingest-report",
    table: `${table(deeConfig.bigQuery.tables.speedToLeadFact)} + Core.fct_ghl_*`,
    description: "Follow-up health for leads that arrived during business hours versus after hours.",
    status: "ready",
  },
} satisfies Record<string, QueryContract>;

export type QueryName = keyof typeof queryContracts;
