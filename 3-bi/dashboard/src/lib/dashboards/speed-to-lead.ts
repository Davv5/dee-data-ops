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
      title: "Follow-Up Health",
      description:
        "Plain-English read on whether new leads were worked, reached, and handled by a human.",
      tiles: [
        {
          type: "kpi",
          title: "Leads worked",
          query: "speed_to_lead_quality_summary",
          field: "worked_lead_rate",
          format: "percent",
          description: "Had any follow-up attempt after raising their hand.",
        },
        {
          type: "kpi",
          title: "Reached by phone",
          query: "speed_to_lead_quality_summary",
          field: "reached_lead_rate",
          format: "percent",
          description: "Got an answered or completed phone call after the trigger.",
        },
        {
          type: "kpi",
          title: "Human follow-up",
          query: "speed_to_lead_quality_summary",
          field: "human_follow_up_rate",
          format: "percent",
          description: "Non-automated call, text, or email that counts as a real response.",
        },
        {
          type: "kpi",
          title: "Automation touched",
          query: "speed_to_lead_quality_summary",
          field: "automation_touch_rate",
          format: "percent",
          description: "Touched by a workflow after the trigger.",
        },
        {
          type: "kpi",
          title: "Still not worked",
          query: "speed_to_lead_quality_summary",
          field: "unworked_lead_rate",
          format: "percent",
          description: "No follow-up attempt found yet.",
        },
        {
          type: "table",
          title: "What happened first?",
          query: "speed_to_lead_first_attempt_outcomes",
          columns: [
            { key: "outcome_label", label: "First Result" },
            { key: "channel_label", label: "Channel" },
            { key: "trigger_count", label: "Leads", format: "number" },
            { key: "share_of_triggers", label: "Share", format: "percent" },
            { key: "workflow_attempts", label: "Automated", format: "number" },
          ],
        },
        {
          type: "table",
          title: "Business hours vs after hours",
          query: "speed_to_lead_business_hours",
          columns: [
            { key: "service_window_label", label: "When Lead Came In" },
            { key: "total_triggers", label: "Leads", format: "number" },
            { key: "worked_lead_rate", label: "Worked", format: "percent" },
            { key: "five_minute_worked_rate", label: "Worked <=5m", format: "percent" },
            { key: "human_follow_up_rate", label: "Human Follow-Up", format: "percent" },
            { key: "five_minute_human_rate", label: "Human <=5m", format: "percent" },
            { key: "unworked_leads", label: "Not Worked", format: "number" },
          ],
        },
      ],
    },
  ],
} satisfies DashboardDefinition;
