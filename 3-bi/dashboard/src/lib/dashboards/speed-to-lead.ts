import type { DashboardDefinition } from "@/types/dashboard";

export const speedToLeadDashboard = {
  slug: "speed-to-lead",
  title: "Speed-to-Lead",
  description:
    "Page 1 rebuild of the retired Metabase Speed-to-Lead dashboard, sourced from the current BigQuery Marts tables.",
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
            { key: "bookings", label: "Bookings", format: "number" },
            { key: "total_dials", label: "Dials", format: "number" },
            {
              key: "pct_within_sla",
              label: "Within SLA",
              format: "percent",
            },
            {
              key: "avg_speed_to_lead_minutes",
              label: "Avg Speed",
              format: "duration",
            },
          ],
        },
      ],
    },
    {
      title: "Response Quality",
      description:
        "Separates attempt, connection, human response, automation, channel status, and business-hours SLA.",
      tiles: [
        {
          type: "kpi",
          title: "First attempt coverage",
          query: "speed_to_lead_quality_summary",
          field: "first_attempt_rate",
          format: "percent",
          description: "Any outbound call, SMS, email, or conversation attempt after the trigger.",
        },
        {
          type: "kpi",
          title: "Successful connection",
          query: "speed_to_lead_quality_summary",
          field: "successful_connection_rate",
          format: "percent",
          description: "First qualifying call connection after trigger; SMS/email delivery is tracked separately.",
        },
        {
          type: "kpi",
          title: "Meaningful human response",
          query: "speed_to_lead_quality_summary",
          field: "meaningful_human_response_rate",
          format: "percent",
          description: "Non-workflow call connection or delivered/sent SMS/email after trigger.",
        },
        {
          type: "table",
          title: "First attempt outcomes",
          query: "speed_to_lead_first_attempt_outcomes",
          columns: [
            { key: "first_attempt_channel", label: "Channel" },
            { key: "first_attempt_status", label: "Status" },
            { key: "trigger_count", label: "Triggers", format: "number" },
            { key: "share_of_triggers", label: "Share", format: "percent" },
            { key: "workflow_attempts", label: "Workflow", format: "number" },
          ],
        },
        {
          type: "table",
          title: "Business-hours SLA",
          query: "speed_to_lead_business_hours",
          columns: [
            { key: "service_window", label: "Window" },
            { key: "total_triggers", label: "Triggers", format: "number" },
            { key: "first_attempt_rate", label: "Attempted", format: "percent" },
            { key: "first_attempt_within_5m_rate", label: "Attempt <=5m", format: "percent" },
            { key: "meaningful_human_response_rate", label: "Human Response", format: "percent" },
            { key: "meaningful_human_within_5m_rate", label: "Human <=5m", format: "percent" },
          ],
        },
      ],
    },
  ],
} satisfies DashboardDefinition;
