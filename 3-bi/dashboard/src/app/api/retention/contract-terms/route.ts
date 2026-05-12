import { NextResponse } from "next/server";
import { runBigQuery } from "@/lib/bigquery/client";
import { deeConfig } from "@/lib/config/dee";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const DATASET_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const CONTACT_SK_PATTERN = /^[a-f0-9]{32}$/i;
const REVIEW_CONFIDENCES = new Set(["high", "medium", "low"]);

function retentionDataset() {
  const dataset =
    process.env.BIGQUERY_RETENTION_DATASET ??
    process.env.BIGQUERY_REVENUE_FUNNEL_DATASET ??
    process.env.BIGQUERY_DBT_DATASET ??
    deeConfig.bigQuery.dataset;

  if (!DATASET_NAME_PATTERN.test(dataset)) {
    throw new Error(`Invalid BigQuery dataset name for contract terms reviews: ${dataset}`);
  }

  return dataset;
}

function martTableRef(tableName: string) {
  return `\`${deeConfig.bigQuery.projectId}.${retentionDataset()}.${tableName}\``;
}

function cleanString(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function moneyValue(value: unknown, fieldName: string, required = false) {
  if (value === null || value === undefined || value === "") {
    if (required) throw new Error(`${fieldName} is required`);
    return null;
  }

  const normalized = typeof value === "string" ? value.replace(/[$,]/g, "").trim() : value;
  const amount = Number(normalized);

  if (!Number.isFinite(amount)) {
    throw new Error(`${fieldName} must be a number`);
  }

  if (amount < 0) {
    throw new Error(`${fieldName} cannot be negative`);
  }

  return amount;
}

function formatMoneyForNote(amount: number | null) {
  if (amount === null) return "not set";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(amount);
}

function contractReviewNote({
  promisedContractValue,
  upfrontAgreedAmount,
  balanceExpectedAmount,
  reviewConfidence,
  termsSourceNote,
}: {
  promisedContractValue: number;
  upfrontAgreedAmount: number | null;
  balanceExpectedAmount: number | null;
  reviewConfidence: string;
  termsSourceNote: string | null;
}) {
  const base = [
    `promised=${formatMoneyForNote(promisedContractValue)}`,
    `upfront=${formatMoneyForNote(upfrontAgreedAmount)}`,
    `balance=${formatMoneyForNote(balanceExpectedAmount)}`,
    `confidence=${reviewConfidence}`,
  ].join("; ");

  return termsSourceNote ? `Confirmed contract terms: ${base}. ${termsSourceNote}` : `Confirmed contract terms: ${base}.`;
}

async function ensureContractTermsReviewsTable() {
  await runBigQuery(`
    CREATE TABLE IF NOT EXISTS ${martTableRef("contract_terms_reviews")} (
      contract_terms_review_id STRING,
      contact_sk STRING,
      contact_id STRING,
      review_status STRING,
      promised_contract_value NUMERIC,
      upfront_agreed_amount NUMERIC,
      balance_expected_amount NUMERIC,
      review_confidence STRING,
      terms_source_note STRING,
      reviewed_by STRING,
      reviewed_at TIMESTAMP,
      source_table STRING,
      contract_evidence_status STRING,
      payment_terms_evidence_text STRING,
      mentioned_payment_amounts_text STRING,
      largest_mentioned_payment_amount NUMERIC,
      lifetime_net_revenue_after_refunds NUMERIC,
      upfront_collected_net_revenue NUMERIC,
      post_first_collected_net_revenue NUMERIC,
      top_product_by_net_revenue STRING,
      top_product_family STRING
    )
    PARTITION BY DATE(reviewed_at)
    CLUSTER BY contact_sk, review_status
  `);
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as Record<string, unknown>;
    const contactSk = cleanString(body.contactSk);
    const reviewConfidence = cleanString(body.reviewConfidence)?.toLowerCase();
    const termsSourceNote = cleanString(body.termsSourceNote);
    const reviewedBy = cleanString(body.reviewedBy) ?? "dashboard";
    const promisedContractValue = moneyValue(body.promisedContractValue, "promisedContractValue", true);
    const upfrontAgreedAmount = moneyValue(body.upfrontAgreedAmount, "upfrontAgreedAmount");
    const balanceExpectedAmount = moneyValue(body.balanceExpectedAmount, "balanceExpectedAmount");

    if (!contactSk || !CONTACT_SK_PATTERN.test(contactSk)) {
      return NextResponse.json({ error: "contactSk is invalid" }, { status: 400 });
    }

    if (!reviewConfidence || !REVIEW_CONFIDENCES.has(reviewConfidence)) {
      return NextResponse.json({ error: "reviewConfidence is invalid" }, { status: 400 });
    }

    if (promisedContractValue === null) {
      return NextResponse.json({ error: "promisedContractValue is required" }, { status: 400 });
    }

    await ensureContractTermsReviewsTable();

    const sourceRows = await runBigQuery(
      `
        SELECT COUNT(*) AS source_count
        FROM ${martTableRef("collection_contract_evidence_detail")}
        WHERE contact_sk = @contactSk
      `,
      { contactSk },
    );
    const sourceCount = Number(sourceRows[0]?.source_count ?? 0);

    if (!sourceCount) {
      return NextResponse.json({ error: "No contract evidence row found for this customer" }, { status: 404 });
    }

    await runBigQuery(
      `
        INSERT INTO ${martTableRef("contract_terms_reviews")} (
          contract_terms_review_id,
          contact_sk,
          contact_id,
          review_status,
          promised_contract_value,
          upfront_agreed_amount,
          balance_expected_amount,
          review_confidence,
          terms_source_note,
          reviewed_by,
          reviewed_at,
          source_table,
          contract_evidence_status,
          payment_terms_evidence_text,
          mentioned_payment_amounts_text,
          largest_mentioned_payment_amount,
          lifetime_net_revenue_after_refunds,
          upfront_collected_net_revenue,
          post_first_collected_net_revenue,
          top_product_by_net_revenue,
          top_product_family
        )
        SELECT
          GENERATE_UUID(),
          evidence.contact_sk,
          evidence.contact_id,
          'confirmed',
          CAST(@promisedContractValue AS NUMERIC),
          CAST(@upfrontAgreedAmount AS NUMERIC),
          CAST(@balanceExpectedAmount AS NUMERIC),
          @reviewConfidence,
          @termsSourceNote,
          @reviewedBy,
          CURRENT_TIMESTAMP(),
          'collection_contract_evidence_detail',
          evidence.contract_evidence_status,
          evidence.payment_terms_evidence_text,
          evidence.mentioned_payment_amounts_text,
          evidence.largest_mentioned_payment_amount,
          evidence.lifetime_net_revenue_after_refunds,
          evidence.upfront_collected_net_revenue,
          evidence.post_first_collected_net_revenue,
          evidence.top_product_by_net_revenue,
          evidence.top_product_family
        FROM ${martTableRef("collection_contract_evidence_detail")} AS evidence
        WHERE evidence.contact_sk = @contactSk
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY evidence.contact_sk
          ORDER BY evidence.mart_refreshed_at DESC
        ) = 1
      `,
      {
        contactSk,
        promisedContractValue,
        upfrontAgreedAmount,
        balanceExpectedAmount,
        reviewConfidence,
        termsSourceNote,
        reviewedBy,
      },
    );

    await runBigQuery(
      `
        INSERT INTO ${martTableRef("operator_action_reviews")} (
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
          'contract_terms_review',
          'fixed',
          @reviewNote,
          @reviewedBy,
          CURRENT_TIMESTAMP(),
          NULL
        )
      `,
      {
        contactSk,
        reviewNote: contractReviewNote({
          promisedContractValue,
          upfrontAgreedAmount,
          balanceExpectedAmount,
          reviewConfidence,
          termsSourceNote,
        }),
        reviewedBy,
      },
    );

    return NextResponse.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown contract terms review error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
