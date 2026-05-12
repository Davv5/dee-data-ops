export type QueryTier = "critical" | "section" | "audit";

export const SPEED_TO_LEAD_TIERS: Record<string, QueryTier> = {
  speed_to_lead_overall: "critical",

  speed_to_lead_overall_prior: "section",
  speed_to_lead_lane_summary: "section",
  speed_to_lead_no_touch_examples: "section",
  speed_to_lead_routing_readiness: "section",
  speed_to_lead_quality_summary: "section",
  speed_to_lead_trigger_summary: "section",
  speed_to_lead_business_hours: "section",
  speed_to_lead_by_rep: "section",
  speed_to_lead_critical_exceptions: "section",
  speed_to_lead_attribution_confidence: "section",
  speed_to_lead_first_work_by_rep: "section",
  speed_to_lead_phone_reach_by_rep: "section",
  speed_to_lead_daily: "section",
  speed_to_lead_response_buckets: "section",
  speed_to_lead_follow_up_counts: "section",
  speed_to_lead_source_performance: "section",
  speed_to_lead_not_worked_aging: "section",
  speed_to_lead_first_attempt_outcomes: "section",

  speed_to_lead_unmatched_truth_audit: "audit",
  speed_to_lead_reached_examples: "audit",
  speed_to_lead_typeform_coverage: "audit",
  speed_to_lead_typeform_outbound_opportunities: "audit",
  speed_to_lead_unmatched_calendly_summary: "audit",
  speed_to_lead_unmatched_calendly_invitees: "audit",
  speed_to_lead_ghl_message_coverage: "audit",
  speed_to_lead_ghl_outbound_message_breakdown: "audit",
};

export function tierForSpeedToLeadQuery(name: string): QueryTier {
  return SPEED_TO_LEAD_TIERS[name] ?? "section";
}
