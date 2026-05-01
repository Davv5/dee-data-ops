import type { DashboardDefinition } from "@/types/dashboard";

export const speedToLeadDashboard = {
  slug: "speed-to-lead",
  title: "Speed-to-Lead",
  description:
    "Page 1 rebuild of the retired Metabase Speed-to-Lead dashboard, sourced from existing bq-ingest report tables for v1.",
  sourceContract: "bq-ingest-report",
  sections: [
    {
      title: "Overview",
      description: "Headline metric and supporting operating context.",
      tiles: [
        {
          type: "kpi",
          title: "Within 5 minutes",
          query: "speed_to_lead_overall",
          field: "pct_within_5m",
          format: "percent",
          description: "Locked headline metric from the v1 definition.",
        },
        {
          type: "kpi",
          title: "Bookings with outbound touch",
          query: "speed_to_lead_overall",
          field: "bookings_with_outbound_call",
          format: "number",
        },
        {
          type: "line",
          title: "Daily Speed-to-Lead trend",
          query: "speed_to_lead_daily",
          x: "report_date",
          y: "pct_within_5m",
          format: "percent",
        },
      ],
    },
    {
      title: "Rep Breakdown",
      description:
        "First rep-level slice. Current bq-ingest scorecard has known caveats called out in the truth map.",
      tiles: [
        {
          type: "bar",
          title: "Weekly SLA by rep",
          query: "speed_to_lead_by_rep",
          x: "rep_name",
          y: "pct_within_sla",
          format: "percent",
        },
        {
          type: "table",
          title: "Rep scorecard",
          query: "speed_to_lead_by_rep",
          columns: [
            { key: "rep_name", label: "Rep" },
            { key: "rep_role", label: "Role" },
            { key: "total_dials", label: "Dials", format: "number" },
            {
              key: "pct_within_sla",
              label: "Within SLA",
              format: "percent",
            },
          ],
        },
      ],
    },
  ],
} satisfies DashboardDefinition;
