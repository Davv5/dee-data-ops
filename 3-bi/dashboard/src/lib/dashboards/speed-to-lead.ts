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
          title: "Within 45 minutes",
          query: "speed_to_lead_overall",
          field: "pct_within_sla",
          format: "percent",
          description: "Appointment bookings reached within the current SLA window.",
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
          y: "pct_within_sla",
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
      title: "Follow-Up Breakdown",
      description:
        "Counts, denominators, and owner attribution for appointment-booking and lead-magnet trigger events.",
      tiles: [
        {
          type: "table",
          title: "Follow-up counts",
          query: "speed_to_lead_follow_up_counts",
          description:
            "Every percentage is out of all lead events unless the column says it is out of worked leads.",
          columns: [
            { key: "metric", label: "Metric" },
            { key: "lead_count", label: "Lead Events", format: "number" },
            { key: "denominator_count", label: "Out Of Lead Events", format: "number" },
            { key: "share_of_all_leads", label: "% of All Events", format: "percent" },
            { key: "share_of_worked_leads", label: "% of Worked Leads", format: "percent" },
            { key: "plain_english", label: "What It Means" },
          ],
        },
        {
          type: "table",
          title: "Who worked them first?",
          query: "speed_to_lead_first_work_by_rep",
          description:
            "The first human, workflow, deleted GHL user id, or dialer number that touched each lead after the trigger.",
          columns: [
            { key: "worked_by", label: "Worked By" },
            { key: "role", label: "Role" },
            { key: "identity_source", label: "Identity Source" },
            { key: "first_channel_label", label: "First Channel" },
            { key: "leads_worked", label: "Lead Events Worked", format: "number" },
            { key: "share_of_worked_leads", label: "% of Worked", format: "percent" },
            { key: "reached_by_phone", label: "Reached Later", format: "number" },
            { key: "avg_minutes_to_first_attempt", label: "Avg First Touch", format: "duration" },
          ],
        },
        {
          type: "table",
          title: "Who reached leads by phone?",
          query: "speed_to_lead_phone_reach_by_rep",
          description:
            "The first human, deleted GHL user id, or dialer number credited with an answered or completed outbound call.",
          columns: [
            { key: "reached_by", label: "Reached By" },
            { key: "role", label: "Role" },
            { key: "identity_source", label: "Identity Source" },
            { key: "leads_reached", label: "Lead Events Reached", format: "number" },
            { key: "share_of_reached_leads", label: "% of Reached", format: "percent" },
            { key: "share_of_all_leads", label: "% of All Events", format: "percent" },
            { key: "avg_minutes_to_connection", label: "Avg To Connect", format: "duration" },
          ],
        },
        {
          type: "table",
          title: "Reached lead examples",
          query: "speed_to_lead_reached_examples",
          description:
            "Recent leads that had an answered or completed outbound call.",
          columns: [
            { key: "reached_at_et", label: "Reached At" },
            { key: "lead_name", label: "Lead" },
            { key: "lead_email", label: "Email" },
            { key: "source_label", label: "Source" },
            { key: "reached_by", label: "Reached By" },
            { key: "identity_source", label: "Identity Source" },
            { key: "minutes_to_connect", label: "Time To Connect", format: "duration" },
            { key: "phone_status", label: "Status" },
          ],
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
            { key: "sla_worked_rate", label: "Worked <=45m", format: "percent" },
            { key: "human_follow_up_rate", label: "Human Follow-Up", format: "percent" },
            { key: "sla_human_rate", label: "Human <=45m", format: "percent" },
            { key: "unworked_leads", label: "Not Worked", format: "number" },
          ],
        },
      ],
    },
  ],
} satisfies DashboardDefinition;
