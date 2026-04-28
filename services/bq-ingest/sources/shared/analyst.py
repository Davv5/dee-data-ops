"""
AI Data Analyst — Gemini 1.5 Flash + BigQuery
Accepts a plain-English question, generates SQL against the Marts layer,
executes it, and returns a structured JSON response.

Environment variables required:
  GEMINI_API_KEY   — Google AI Studio free-tier key (aistudio.google.com)
  ANALYST_API_KEY  — (optional) bearer token to gate the /ask endpoint
  GCP_PROJECT      — BigQuery project ID (defaults to prod project)
"""

import json
import logging
import os
import re

from google import genai
from google.cloud import bigquery

logger = logging.getLogger(__name__)

GCP_PROJECT = os.getenv("GCP_PROJECT", "project-41542e21-470f-4589-96d")
DATASET     = "Marts"

# ---------------------------------------------------------------------------
# Mart schema context — fed to Gemini as system instructions
# ---------------------------------------------------------------------------

SCHEMA_CONTEXT = f"""
You are an expert data analyst for Fanbasis, a sales coaching company.
You write BigQuery SQL against the `{GCP_PROJECT}.{DATASET}` dataset.

IMPORTANT RULES:
- Only read from the tables listed below. Never write, update, or delete.
- Always use fully qualified table names: `{GCP_PROJECT}.{DATASET}.table_name`
- `report_week` columns are DATE type (Monday-anchored). Filter with DATE literals.
- `report_month` columns are DATE type (first of month). Filter with DATE literals.
- Return at most 500 rows unless the user asks for more.
- When asked about "this week" use DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)).
- When asked about "last week" subtract 7 days from the current week start.
- show_rate is already a decimal (0.0–1.0). Multiply by 100 for percentages.
- close_rate is already a decimal (0.0–1.0). Multiply by 100 for percentages.

AVAILABLE TABLES:

1. rpt_rep_scorecard_week
   Grain: report_week × rep_name. One row per rep per week — the main team leaderboard.
   Columns:
     report_week DATE, rep_name STRING, rep_role STRING (setter/closer/setter+closer),
     bookings INT64, shows INT64, no_shows INT64,
     host_canceled INT64, invitee_canceled INT64, show_rate FLOAT64,
     total_dials INT64, call_to_booking_rate_14d FLOAT64,
     avg_speed_to_lead_minutes FLOAT64, speed_to_lead_pct_within_sla FLOAT64,
     unbooked_leads_worked INT64, unbooked_total_dials INT64, unbooked_conversion_rate_14d FLOAT64,
     sales_calls_taken INT64, deals_closed INT64, close_rate FLOAT64

2. rpt_appt_funnel_week
   Grain: report_week × setter_name × campaign_reporting. Full booking funnel with cancel attribution.
   Columns:
     report_week DATE, setter_name STRING, campaign_reporting STRING,
     total_bookings INT64, host_canceled INT64, invitee_canceled INT64,
     canceled_unknown INT64, total_canceled INT64, net_appointments INT64,
     shows INT64, no_shows INT64, scheduled_future INT64,
     show_rate FLOAT64, cancel_rate FLOAT64, host_cancel_rate FLOAT64

3. rpt_speed_to_lead_week
   Grain: report_week × setter_name × trigger_type. How fast setters respond to new leads.
   trigger_type is 'lead_magnet' (form submit) or 'appointment_booking' (booking made).
   Columns:
     report_week DATE, setter_name STRING, trigger_type STRING,
     total_triggers INT64, touched INT64, not_yet_touched INT64, touch_rate FLOAT64,
     avg_speed_minutes FLOAT64, median_speed_minutes INT64, p90_speed_minutes INT64,
     within_5m INT64, pct_within_5m FLOAT64,
     within_15m INT64, pct_within_15m FLOAT64,
     within_1h INT64, pct_within_1h FLOAT64,
     sla_breached INT64, sla_breach_rate FLOAT64

4. rpt_closer_close_rate_week
   Grain: report_week × closer_name. Closer performance from Fathom sales calls.
   Columns:
     report_week DATE, closer_name STRING, closer_email STRING,
     sales_calls_taken INT64, calls_with_linked_opp INT64,
     fathom_won_count INT64, close_rate FLOAT64,
     calls_moved_stage_48h INT64, avg_hours_to_win_on_call FLOAT64,
     ghl_won_no_fathom_supplement INT64

5. rpt_closer_revenue_month
   Grain: report_month × closer_name × canonical_offer. Monthly revenue by closer and offer.
   Columns:
     report_month DATE, closer_name STRING, closer_email STRING, canonical_offer STRING,
     close_payment_count INT64, total_payment_count INT64, distinct_clients INT64,
     close_revenue_net FLOAT64, total_revenue_net FLOAT64, avg_deal_value FLOAT64

6. rpt_closer_speed_to_close_week
   Grain: report_week × closer_name. Hours from first sales call to won deal.
   Columns:
     report_week DATE, closer_name STRING,
     won_deals INT64, avg_hours_to_close FLOAT64,
     median_hours_to_close INT64, p90_hours_to_close INT64

7. rpt_funnel_conversion_week
   Grain: report_week × campaign_reporting. End-to-end funnel from new leads to closed deals.
   Columns:
     report_week DATE, campaign_reporting STRING,
     new_leads INT64, total_dials INT64, contacts_reached_any_channel INT64,
     bookings_made INT64, shows_count INT64, deals_closed INT64,
     contact_to_booking_rate FLOAT64, show_rate FLOAT64,
     showed_to_close_rate FLOAT64, lead_to_close_rate FLOAT64

8. rpt_unbooked_lead_quality_by_campaign
   Grain: report_week × campaign_reporting × lead_magnet_name.
   Form submitters who never booked — conversion rates and setter coverage.
   Columns:
     report_week DATE, campaign_reporting STRING, lead_magnet_name STRING,
     form_fills INT64, eventually_booked INT64,
     booked_within_7d INT64, booked_within_14d INT64,
     booking_rate_ever FLOAT64, booking_rate_7d FLOAT64, booking_rate_14d FLOAT64,
     median_days_to_booking INT64,
     setter_touched INT64, never_touched INT64, never_touched_pct FLOAT64

9. rpt_setter_pre_appt_outreach_week
   Grain: report_week × setter_name. Pre-appointment outreach after a lead books.
   Columns:
     report_week DATE, setter_name STRING,
     total_booked INT64, past_appointments INT64, future_appointments INT64,
     reached_before_appt INT64, not_reached_before_appt INT64, pre_appt_reach_rate FLOAT64,
     avg_touches_per_booking FLOAT64, avg_calls_per_booking FLOAT64, avg_sms_per_booking FLOAT64,
     show_rate_overall FLOAT64, show_rate_when_reached FLOAT64, show_rate_when_not_reached FLOAT64

10. rpt_setter_unbooked_conversion_week
    Grain: report_week × setter_name × campaign_reporting. Outbound conversion of unbooked form leads.
    Columns:
      report_week DATE, setter_name STRING, campaign_reporting STRING,
      unbooked_leads_worked INT64, total_dials INT64, total_sms INT64,
      contacts_called INT64, contacts_sms_touched INT64, eventually_booked INT64,
      booked_within_7d INT64, booked_within_14d INT64,
      conversion_rate_all_time FLOAT64, conversion_rate_14d FLOAT64

KEY BUSINESS DEFINITIONS:
- "show rate" = shows / (shows + no_shows). Excludes canceled appointments.
- "host canceled" = your team (triager) canceled the appointment (e.g. DQ'd the lead).
- "invitee canceled" = the lead canceled themselves.
- "speed to lead" = minutes from form submission or booking to first setter outbound touch.
- "SLA" = within 5 minutes for lead_magnet triggers; within 15 minutes for booking triggers.
- "close rate" = deals won / sales calls taken.
- "unbooked conversion" = form submitters who never booked, then converted via outbound.
- "setter" = person who handles inbound leads, books appointments, does pre-call outreach.
- "closer" = person who takes the sales call and closes the deal.
"""

SQL_PROMPT_TEMPLATE = """
{schema}

The user asked: "{question}"

Write a single BigQuery SQL query that answers this question accurately.
Return ONLY a JSON object with this exact format:
{{
  "sql": "<your SQL here>",
  "intent": "<one sentence describing what the query does>"
}}

No markdown, no explanation, no code blocks — just the raw JSON object.
"""

SUMMARIZE_PROMPT_TEMPLATE = """
The user asked: "{question}"

The query returned these results:
{results}

Write a clear, concise plain-English answer (2–5 sentences).
Use specific numbers from the results.
If the results are empty, say so and suggest why.
Do not mention SQL or technical terms.
"""

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def _get_gemini_client() -> genai.Client:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("GEMINI_API_KEY environment variable is not set.")
    return genai.Client(api_key=api_key)


def _generate_sql(client: genai.Client, question: str) -> tuple[str, str]:
    """Ask Gemini to produce a SQL query. Returns (sql, intent)."""
    prompt = SQL_PROMPT_TEMPLATE.format(schema=SCHEMA_CONTEXT, question=question)
    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt,
    )
    raw = response.text.strip()

    # Strip markdown code fences if Gemini wraps the JSON anyway
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    parsed = json.loads(raw)
    return parsed["sql"], parsed.get("intent", "")


def _run_sql(sql: str) -> list[dict]:
    """Execute SQL on BigQuery and return rows as list of dicts."""
    bq = bigquery.Client(project=GCP_PROJECT)
    job = bq.query(sql)
    rows = list(job.result())
    return [dict(row) for row in rows]


def _summarize(client: genai.Client, question: str, results: list[dict]) -> str:
    """Ask Gemini to turn raw rows into a plain-English answer."""
    # Truncate results to avoid token overload
    preview = results[:50]
    results_str = json.dumps(preview, indent=2, default=str)

    prompt = SUMMARIZE_PROMPT_TEMPLATE.format(
        question=question,
        results=results_str,
    )
    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt,
    )
    return response.text.strip()


def ask_analyst(question: str) -> dict:
    """
    Main entry point. Returns:
    {
        "ok": True,
        "answer": "Plain English summary",
        "sql": "The SQL that was executed",
        "intent": "What the query does",
        "data": [...rows...],
        "row_count": N
    }
    """
    try:
        client = _get_gemini_client()

        # Step 1: Generate SQL
        sql, intent = _generate_sql(client, question)
        logger.info("Generated SQL for question=%r intent=%r", question, intent)

        # Step 2: Execute against BigQuery
        data = _run_sql(sql)
        logger.info("Query returned %d rows", len(data))

        # Step 3: Summarize results in plain English
        answer = _summarize(client, question, data)

        return {
            "ok": True,
            "answer": answer,
            "sql": sql,
            "intent": intent,
            "data": data,
            "row_count": len(data),
        }

    except json.JSONDecodeError as exc:
        logger.exception("Gemini returned invalid JSON")
        return {"ok": False, "error": f"SQL generation failed (invalid JSON from model): {exc}"}
    except Exception as exc:
        logger.exception("Analyst error for question=%r", question)
        return {"ok": False, "error": str(exc)}
