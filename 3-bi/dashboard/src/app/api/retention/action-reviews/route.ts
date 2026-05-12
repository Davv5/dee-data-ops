import { NextResponse } from "next/server";
import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const ACTION_BUCKETS = new Set([
  "failed_plan_recovery_needed",
  "active_plan_due_no_payment_yet",
  "one_time_upsell_candidate",
  "completed_plan_paid_off",
  "historical_stripe_product_review",
  "active_plan_not_yet_due",
  "repeat_payment_observed",
  "active_plan_paid_current_month",
  "review_negative_value",
  "no_repeat_expected_yet",
  "manual_collection_stale_review",
  "collection_call_no_payment_review",
  "plan_named_collection_review",
  "repeat_or_upsell_review",
  "manual_collection_recently_collected",
  "contract_terms_review",
]);
const REVIEW_STATUSES = new Set(["open", "reviewed", "fixed", "wont_fix"]);

function retentionDataset() {
  const dataset =
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for retention action reviews: ${dataset}`);
  }

  return dataset;
}

function operatorReviewsTableRef() {
  return `\`${deeConfig.bigQuery.projectId}.${retentionDataset()}.operator_action_reviews\``;
}

function cleanString(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as Record<string, unknown>;
    const contactSk = cleanString(body.contactSk);
    const actionBucket = cleanString(body.actionBucket);
    const reviewStatus = cleanString(body.reviewStatus);
    const reviewNote = cleanString(body.reviewNote);
    const reviewedBy = cleanString(body.reviewedBy) ?? "dashboard";

    if (!contactSk) {
      return NextResponse.json({ error: "contactSk is required" }, { status: 400 });
    }

    if (!actionBucket || !ACTION_BUCKETS.has(actionBucket)) {
      return NextResponse.json({ error: "actionBucket is invalid" }, { status: 400 });
    }

    if (!reviewStatus || !REVIEW_STATUSES.has(reviewStatus)) {
      return NextResponse.json({ error: "reviewStatus is invalid" }, { status: 400 });
    }

    await runBigQuery(
      `
        INSERT INTO ${operatorReviewsTableRef()} (
          area,
          queue_name,
          entity_type,
          entity_id,
          contact_sk,
          action_bucket,
          review_status,
          review_note,
          reviewed_by,
          reviewed_at,
          expires_at
        )
        VALUES (
          'retention',
          'retention_worklist',
          'customer',
          @contactSk,
          @contactSk,
          @actionBucket,
          @reviewStatus,
          @reviewNote,
          @reviewedBy,
          CURRENT_TIMESTAMP(),
          NULL
        )
      `,
      {
        contactSk,
        actionBucket,
        reviewStatus,
        reviewNote,
        reviewedBy,
      },
    );

    return NextResponse.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown retention action review error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
