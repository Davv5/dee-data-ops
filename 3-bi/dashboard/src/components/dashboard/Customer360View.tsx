import type { ComponentType, ReactNode } from "react";
import {
  AlertTriangle,
  ArrowLeft,
  CalendarCheck,
  CheckCircle2,
  CreditCard,
  DollarSign,
  Mail,
  MessageCircle,
  Phone,
  ReceiptText,
  UserRound,
} from "lucide-react";
import { ContractTermsReviewForm } from "@/components/dashboard/ContractTermsReviewForm";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { OperatorActionReviewButtons } from "@/components/dashboard/OperatorActionReviewButtons";
import type { DashboardData, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

type Customer360ViewProps = {
  data: DashboardData;
  contactSk: string;
  returnHref: string;
  sourceContext?: {
    from?: string;
    reason?: string;
  };
};

type CommandFactTone = "green" | "blue" | "amber";

type CommandFactItem = {
  label: string;
  value: string;
  helper: string;
  tone: CommandFactTone;
};

export function Customer360View({ data, contactSk, returnHref, sourceContext }: Customer360ViewProps) {
  const profile = data.rows.customer_360_profile?.[0];
  const payments = data.rows.customer_360_payments ?? [];
  const refunds = data.rows.customer_360_refunds ?? [];
  const outreach = data.rows.customer_360_outreach ?? [];
  const bookings = data.rows.customer_360_bookings ?? [];
  const magnetTrail = data.rows.customer_360_magnet_trail ?? [];
  const retentionMonths = data.rows.customer_360_retention_months ?? [];
  const relationshipTimeline = data.rows.customer_360_relationship_timeline ?? [];
  const operatorActions = data.rows.customer_360_operator_actions ?? [];

  const customerName = stringValue(profile?.customer_display_name) ?? "Customer 360";
  const contactLine = [stringValue(profile?.email_norm), stringValue(profile?.phone)].filter(Boolean).join(" · ");
  const dueLabel = formatDueStatus(profile);
  const revenueCredit = knownOperatorName(profile?.credited_closer_name) ?? "Unassigned / unknown";
  const revenueCreditHelper = attributionHelper(
    profile?.credited_closer_source,
    profile?.credited_closer_confidence,
    "No revenue credit source",
  );

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <a href={returnHref} className="inline-flex items-center gap-1.5 text-xs font-semibold text-[#0f766e] hover:text-[#115e59]">
            <ArrowLeft className="h-3.5 w-3.5" aria-hidden />
            Back to worklist
          </a>
          <p className="mt-3 text-sm font-medium text-[#0f766e]">customer-360</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">{customerName}</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            {contactLine || `contact_sk ${contactSk}`}
          </p>
        </div>
        <div className="flex flex-col gap-2 md:items-end">
          <FreshnessBadge freshness={data.freshness} />
          <span className="rounded-md border border-[#dedbd2] bg-white px-2.5 py-1.5 text-xs font-semibold text-[#66635f]">
            {shortHash(contactSk)}
          </span>
        </div>
      </header>

      {data.error ? (
        <div className="mt-4 rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <section className="py-4">
        <OperatorCommandPanel
          profile={profile}
          payments={payments}
          refunds={refunds}
          outreach={outreach}
          bookings={bookings}
          magnetTrail={magnetTrail}
          sourceContext={sourceContext}
        />
        <OpenOperatorActionsPanel rows={operatorActions} profile={profile} />
        <OriginContextPanel profile={profile} sourceContext={sourceContext} />
        <RelationshipTimelinePanel rows={relationshipTimeline} />

        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
          <KpiCard title="Lifetime Net" value={formatCurrency(numberValue(profile?.lifetime_net_revenue_after_refunds))} helper={`${formatNumber(numberValue(profile?.lifetime_paid_payments_count))} payments`} icon={DollarSign} tone="green" />
          <KpiCard title="Next Action" value={labelize(stringValue(profile?.retention_operator_next_action))} helper={labelize(stringValue(profile?.collection_health_status) ?? stringValue(profile?.payment_plan_health_status))} icon={CreditCard} tone={booleanValue(profile?.is_expected_payment_due_now) ? "amber" : "blue"} />
          <KpiCard title="Due Status" value={dueLabel} helper={stringValue(profile?.expected_next_payment_label) ?? "No expected date"} icon={CalendarCheck} tone={booleanValue(profile?.is_expected_payment_missed_now) ? "amber" : "green"} />
          <KpiCard title="Revenue Credit" value={revenueCredit} helper={revenueCreditHelper} icon={UserRound} tone={knownOperatorName(profile?.credited_closer_name) ? "blue" : "amber"} />
          <KpiCard title="Bookings" value={formatNumber(bookings.length)} helper={`${formatNumber(outreach.length)} recent touches`} icon={Phone} tone="blue" />
          <KpiCard title="Refunds" value={formatCurrency(numberValue(profile?.lifetime_refunds_amount))} helper={`${formatNumber(numberValue(profile?.lifetime_refunds_count))} refunds`} icon={ReceiptText} tone={numberValue(profile?.lifetime_refunds_count) ? "amber" : "green"} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(24rem,0.85fr)]">
          <ProfilePanel profile={profile} />
          <ActionPanel profile={profile} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(24rem,0.9fr)]">
          <PaymentsPanel payments={payments} refunds={refunds} />
          <TimelinePanel bookings={bookings} outreach={outreach} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(24rem,0.9fr)]">
          <MagnetTrailPanel rows={magnetTrail} />
          <RetentionMonthsPanel rows={retentionMonths} />
        </div>
      </section>

      <AuditDetails data={data} />
    </div>
  );
}

function OperatorCommandPanel({
  profile,
  payments,
  refunds,
  outreach,
  bookings,
  magnetTrail,
  sourceContext,
}: {
  profile: DashboardRow | undefined;
  payments: DashboardRow[];
  refunds: DashboardRow[];
  outreach: DashboardRow[];
  bookings: DashboardRow[];
  magnetTrail: DashboardRow[];
  sourceContext: Customer360ViewProps["sourceContext"];
}) {
  const action = operatorAction(profile, sourceContext);
  const evidenceItems = actionEvidence(profile, payments, refunds, outreach, bookings, magnetTrail);
  const blockerItems = actionBlockers(profile, sourceContext);
  const email = stringValue(profile?.email_norm);
  const phone = stringValue(profile?.phone);
  const phoneHref = phone ? cleanPhoneHref(phone) : null;

  return (
    <section className="mb-3 rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1.25fr)_minmax(20rem,0.75fr)]">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <span className={`inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-xs font-semibold ${priorityClass(action.priorityTone)}`}>
              {action.priorityTone === "red" ? (
                <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
              ) : (
                <CheckCircle2 className="h-3.5 w-3.5" aria-hidden />
              )}
              {action.priority}
            </span>
            <span className="rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1 text-xs font-semibold text-[#66635f]">
              {action.channel}
            </span>
          </div>

          <div className="mt-3 text-[11px] font-semibold uppercase text-[#66635f]">Primary move</div>
          <h2 className="mt-1 text-2xl font-semibold tracking-normal text-[#2d2b28]">{action.title}</h2>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">{action.rationale}</p>

          <div className="mt-4 flex flex-wrap gap-2">
            {phoneHref ? (
              <>
                <a href={`tel:${phoneHref}`} className="inline-flex items-center gap-2 rounded-md bg-[#0f766e] px-3 py-2 text-xs font-semibold text-white hover:bg-[#115e59]">
                  <Phone className="h-3.5 w-3.5" aria-hidden />
                  Call
                </a>
                <a href={`sms:${phoneHref}`} className="inline-flex items-center gap-2 rounded-md border border-[#99f6e4] bg-[#f0fdfa] px-3 py-2 text-xs font-semibold text-[#0f766e] hover:bg-[#ccfbf1]">
                  <MessageCircle className="h-3.5 w-3.5" aria-hidden />
                  Text
                </a>
              </>
            ) : null}
            {email ? (
              <a href={`mailto:${email}`} className="inline-flex items-center gap-2 rounded-md border border-[#bfdbfe] bg-[#eff6ff] px-3 py-2 text-xs font-semibold text-[#1d4ed8] hover:bg-[#dbeafe]">
                <Mail className="h-3.5 w-3.5" aria-hidden />
                Email
              </a>
            ) : null}
            {!phoneHref && !email ? (
              <span className="inline-flex items-center gap-2 rounded-md border border-[#fde68a] bg-[#fffbeb] px-3 py-2 text-xs font-semibold text-[#92400e]">
                <AlertTriangle className="h-3.5 w-3.5" aria-hidden />
                No direct contact route
              </span>
            ) : null}
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
          <CommandFact label="revenue credit" value={action.owner} helper={action.ownerHelper} tone={action.owner === "Unassigned / unknown" ? "amber" : "green"} />
          <CommandFact label="blocker" value={blockerItems[0]?.value ?? "No major blocker"} helper={blockerItems[0]?.helper ?? "Evidence is clean enough to act"} tone={blockerItems.length ? "amber" : "green"} />
          <CommandFact label="money at stake" value={formatCurrency(numberValue(profile?.lifetime_net_revenue_after_refunds))} helper={`${formatNumber(numberValue(profile?.lifetime_paid_payments_count))} payments`} tone="green" />
        </div>
      </div>

      <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {evidenceItems.map((item) => (
          <CommandFact key={item.label} label={item.label} value={item.value} helper={item.helper} tone={item.tone} />
        ))}
      </div>
    </section>
  );
}

function OpenOperatorActionsPanel({ rows, profile }: { rows: DashboardRow[]; profile: DashboardRow | undefined }) {
  const openRows = rows.filter((row) => {
    const status = stringValue(row.review_status) ?? "open";
    return status !== "fixed" && status !== "wont_fix";
  });

  return (
    <section className="mb-3 rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="text-sm font-semibold">Open Operator Actions</h2>
            <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-[11px] font-semibold uppercase text-[#166534]">
              {formatNumber(openRows.length)} open
            </span>
          </div>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
            Revenue and Retention queue reasons for this customer, filtered by the shared operator ledger.
          </p>
        </div>
      </div>

      {openRows.length ? (
        <div className="mt-3 grid gap-2 lg:grid-cols-2">
          {openRows.map((row, index) => {
            const area = stringValue(row.area) ?? "operator";
            const endpoint = area === "retention" ? "/api/retention/action-reviews" : "/api/revenue/action-reviews";
            const label = area === "retention" ? "Retention" : "Revenue";
            const isContractReview = stringValue(row.action_bucket) === "contract_terms_review";

            return (
              <div key={`${area}-${stringValue(row.action_bucket) ?? index}`} className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className={`rounded-md border px-2 py-1 text-[11px] font-semibold ${actionAreaClass(area)}`}>
                        {label}
                      </span>
                      <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-[11px] font-semibold text-[#66635f]">
                        {reviewStatusLabel(stringValue(row.review_status))}
                      </span>
                    </div>
                    <div className="mt-2 truncate text-sm font-semibold text-[#2d2b28]">
                      {stringValue(row.operator_next_action)}
                    </div>
                    <div className="mt-1 text-xs leading-5 text-[#66635f]">
                      {stringValue(row.action_bucket_label)} · {stringValue(row.action_reason)}
                    </div>
                    <div className="mt-1 text-[11px] text-[#66635f]">
                      {stringValue(row.source_table)} · {stringValue(row.source_date_label) ?? "No source date"}
                    </div>
                  </div>
                  <MetricCell label="value" value={formatCurrency(numberValue(row.money_at_stake))} />
                </div>
                {isContractReview ? (
                  <ContractTermsReviewForm
                    contactSk={stringValue(row.contact_sk)}
                    evidenceText={stringValue(profile?.payment_terms_evidence_text)}
                    mentionedAmountsText={stringValue(profile?.mentioned_payment_amounts_text)}
                    largestMentionedAmount={profile?.largest_mentioned_payment_amount}
                  />
                ) : (
                  <OperatorActionReviewButtons
                    endpoint={endpoint}
                    contactSk={stringValue(row.contact_sk)}
                    actionBucket={stringValue(row.action_bucket)}
                    fixedNote={`Closed from Customer 360 ${label} action.`}
                    ignoreNote={`Marked wont_fix from Customer 360 ${label} action.`}
                  />
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <div className="mt-3 rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-3 text-sm font-medium text-[#166534]">
          No open Revenue or Retention actions for this customer.
        </div>
      )}
    </section>
  );
}

function OriginContextPanel({
  profile,
  sourceContext,
}: {
  profile: DashboardRow | undefined;
  sourceContext: Customer360ViewProps["sourceContext"];
}) {
  const from = sourceContext?.from;
  const reason = sourceContext?.reason;
  const context = originContext(from, reason, profile);

  return (
    <section className="mb-3 rounded-lg border border-[#bbf7d0] bg-[#f0fdf4] p-4 shadow-sm">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="text-[11px] font-semibold uppercase text-[#166534]">Why you are here</div>
          <h2 className="mt-1 text-sm font-semibold text-[#2d2b28]">{context.title}</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#3f5f46]">{context.helper}</p>
        </div>
        <span className="w-fit rounded-md border border-[#86efac] bg-white px-2 py-1 text-xs font-semibold text-[#166534]">
          {context.badge}
        </span>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        {context.items.map((item) => (
          <div key={item.label} className="rounded-md border border-[#bbf7d0] bg-white p-2">
            <div className="text-[10px] font-semibold uppercase text-[#66635f]">{item.label}</div>
            <div className="mt-1 truncate text-sm font-semibold text-[#2d2b28]" title={item.value}>{item.value}</div>
            <div className="truncate text-[11px] text-[#66635f]" title={item.helper}>{item.helper}</div>
          </div>
        ))}
      </div>
    </section>
  );
}

function originContext(from: string | undefined, reason: string | undefined, profile: DashboardRow | undefined) {
  if (from === "revenue") {
    return {
      title: "Revenue sent you here to inspect buyer truth.",
      helper: "This is the person-level evidence behind a revenue gap, attribution credit, product cleanup, or source-depth question.",
      badge: "Revenue drilldown",
      items: [
        {
          label: "next check",
          value: revenueReason(profile),
          helper: labelize(stringValue(profile?.revenue_funnel_quality_flag)),
        },
        {
          label: "revenue credit",
          value: knownOperatorName(profile?.credited_closer_name) ?? "Unassigned / unknown",
          helper: attributionHelper(profile?.credited_closer_source, profile?.credited_closer_confidence, "No revenue credit source"),
        },
        {
          label: "product",
          value: stringValue(profile?.top_product_by_net_revenue) ?? "Unknown product",
          helper: stringValue(profile?.top_product_family) ?? "No product family",
        },
        {
          label: "magnet before buy",
          value: stringValue(profile?.latest_prior_lead_magnet_name) ?? "No prior magnet",
          helper: labelize(stringValue(profile?.revenue_purchase_magnet_attribution_flag)),
        },
      ],
    };
  }

  if (from === "retention") {
    return {
      title: "Retention sent you here because this customer needs an operator read.",
      helper: "This is the person-level view behind recovery, renewal, payment-plan, and repeat-payment questions.",
      badge: "Retention drilldown",
      items: [
        {
          label: "next action",
          value: labelize(stringValue(profile?.retention_operator_next_action)),
          helper: labelize(stringValue(profile?.payment_plan_health_status)),
        },
        {
          label: "due timing",
          value: formatDueStatus(profile),
          helper: stringValue(profile?.expected_next_payment_label) ?? "No expected date",
        },
        {
          label: "repeat type",
          value: labelize(stringValue(profile?.repeat_payment_type)),
          helper: labelize(stringValue(profile?.payment_plan_truth_status)),
        },
        {
          label: "collection",
          value: formatCurrency(numberValue(profile?.post_first_collected_net_revenue)),
          helper: labelize(stringValue(profile?.collection_health_status)),
        },
        {
          label: "lifetime net",
          value: formatCurrency(numberValue(profile?.lifetime_net_revenue_after_refunds)),
          helper: `${formatNumber(numberValue(profile?.lifetime_paid_payments_count))} payments`,
        },
      ],
    };
  }

  if (from === "lead-magnets") {
    return {
      title: "Lead Magnets sent you here to inspect magnet-to-money proof.",
      helper: "This buyer row explains which offer showed up before money and whether the attribution is clean enough to trust.",
      badge: "Magnet buyer",
      items: [
        {
          label: "reason",
          value: reasonLabel(reason),
          helper: labelize(stringValue(profile?.purchase_magnet_attribution_flag)),
        },
        {
          label: "latest magnet",
          value: stringValue(profile?.latest_prior_lead_magnet_name) ?? "No prior magnet",
          helper: stringValue(profile?.latest_prior_opportunity_label) ?? "No prior opportunity date",
        },
        {
          label: "offer type",
          value: labelize(stringValue(profile?.latest_prior_lead_magnet_offer_type)),
          helper: stringValue(profile?.latest_prior_lead_magnet_category) ?? "No category",
        },
        {
          label: "first purchase",
          value: stringValue(profile?.first_purchase_label) ?? "No first purchase",
          helper: stringValue(profile?.top_product_by_net_revenue) ?? "Unknown product",
        },
      ],
    };
  }

  if (from === "speed-to-lead") {
    return {
      title: "Speed-to-Lead sent you here to inspect the lead event behind the metric.",
      helper: "This profile connects the lead-event row to contact, pre-sale operator signals, booking, purchase, and outreach evidence.",
      badge: "Speed-to-lead drilldown",
      items: [
        {
          label: "reason",
          value: reasonLabel(reason),
          helper: "From the lead-event audit table",
        },
        {
          label: "pre-sale operator",
          value:
            knownOperatorName(profile?.best_available_operator_name) ??
            knownOperatorName(profile?.credited_setter_name) ??
            knownOperatorName(profile?.credited_closer_name) ??
            "Unassigned / unknown",
          helper: stringValue(profile?.best_available_operator_source) ?? "No pre-sale operator source",
        },
        {
          label: "lead source",
          value: stringValue(profile?.lead_source) ?? stringValue(profile?.utm_campaign) ?? "Unknown source",
          helper: [stringValue(profile?.utm_source), stringValue(profile?.utm_medium)].filter(Boolean).join(" / ") || "No UTM",
        },
        {
          label: "money context",
          value: formatCurrency(numberValue(profile?.lifetime_net_revenue_after_refunds)),
          helper: stringValue(profile?.top_product_by_net_revenue) ?? "No purchase yet",
        },
      ],
    };
  }

  return {
    title: "Full customer profile.",
    helper: "Use this page to connect identity, money, bookings, lead magnets, and follow-up evidence for one person.",
    badge: "Customer 360",
    items: [
      {
        label: "next action",
        value: labelize(stringValue(profile?.retention_operator_next_action)),
        helper: labelize(stringValue(profile?.payment_plan_health_status)),
      },
      {
        label: "lifetime net",
        value: formatCurrency(numberValue(profile?.lifetime_net_revenue_after_refunds)),
        helper: `${formatNumber(numberValue(profile?.lifetime_paid_payments_count))} payments`,
      },
      {
        label: "latest magnet",
        value: stringValue(profile?.latest_prior_lead_magnet_name) ?? "No prior magnet",
        helper: labelize(stringValue(profile?.purchase_magnet_attribution_flag)),
      },
      {
        label: "revenue credit",
        value: knownOperatorName(profile?.credited_closer_name) ?? "Unassigned / unknown",
        helper: attributionHelper(profile?.credited_closer_source, profile?.credited_closer_confidence, "No revenue credit source"),
      },
    ],
  };
}

function revenueReason(profile: DashboardRow | undefined) {
  const qualityFlag = stringValue(profile?.revenue_funnel_quality_flag);
  const closer = knownOperatorName(profile?.credited_closer_name);
  const productFamily = stringValue(profile?.top_product_family);

  if (qualityFlag && qualityFlag !== "clean") return labelize(qualityFlag);
  if (!closer) return "Find Revenue Credit";
  if (productFamily === "Unknown / historical Stripe") return "Repair Product";
  return "Review Buyer";
}

function reasonLabel(reason: string | undefined) {
  if (reason === "magnet_buyer") return "Magnet Buyer";
  if (reason === "reached_by_phone") return "Reached By Phone";
  if (reason === "not_worked") return "Not Worked";
  return "Open Customer";
}

function operatorAction(profile: DashboardRow | undefined, sourceContext: Customer360ViewProps["sourceContext"]) {
  const nextAction = stringValue(profile?.retention_operator_next_action);
  const health = stringValue(profile?.payment_plan_health_status);
  const revenueQuality = stringValue(profile?.revenue_funnel_quality_flag);
  const reason = sourceContext?.reason;
  const from = sourceContext?.from;
  const revenueCredit = knownOperatorName(profile?.credited_closer_name) ?? "Unassigned / unknown";
  const revenueCreditHelper = attributionHelper(
    profile?.credited_closer_source,
    profile?.credited_closer_confidence,
    "No revenue credit source",
  );

  if (reason === "not_worked") {
    return {
      title: "Work this lead now",
      rationale: "This person came from a lead-event row with no follow-up found. The fastest useful move is a direct call, then text/email if they do not answer.",
      priority: "High priority",
      priorityTone: "red",
      channel: "Call first",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (reason === "reached_by_phone") {
    return {
      title: "Review the reached call outcome",
      rationale: "This lead was reached by phone. Check whether the call produced a booking, payment, or next action so the win or gap is credited correctly.",
      priority: "Review",
      priorityTone: "blue",
      channel: "Call proof",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "recover_failed_payment") {
    return {
      title: "Recover the failed payment",
      rationale: "Fanbasis shows a failed subscription/payment-plan state. This is the highest-value retention motion: contact the customer and get the plan current.",
      priority: "High priority",
      priorityTone: "red",
      channel: "Call + text",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "collect_due_payment") {
    return {
      title: "Collect the due payment",
      rationale: "The plan is active and the expected payment date has arrived without a matching paid event. Treat this as a live recovery queue item.",
      priority: "High priority",
      priorityTone: "red",
      channel: "Call + text",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "review_manual_collection") {
    return {
      title: "Review manual collection",
      rationale: "This customer shows payment-plan or collection-call evidence after the first payment. Confirm whether this is remaining balance collection, an upsell, or a source gap before chasing or crediting it.",
      priority: "Collection review",
      priorityTone: "amber",
      channel: "Audit + follow-up",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "confirm_repeat_or_upsell") {
    return {
      title: "Confirm repeat versus upsell",
      rationale: "This buyer paid again, but the product pattern could mean remaining balance collection or a separate upsell. Classify the money before treating it as retention expansion.",
      priority: "Revenue classification",
      priorityTone: "amber",
      channel: "Audit first",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "monitor_manual_collection") {
    return {
      title: "Monitor manual collection",
      rationale: "The customer has post-first-payment cash collected recently. Keep it visible as manual collection activity, separate from the original close.",
      priority: "Monitor",
      priorityTone: "green",
      channel: "No rush",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "upsell_completed_customer" || nextAction === "upsell_one_time_customer") {
    return {
      title: "Pitch the next offer",
      rationale: "This customer has already paid and the current state suggests an expansion or next-step offer instead of basic cleanup.",
      priority: "Revenue opportunity",
      priorityTone: "green",
      channel: "Follow-up",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "review_refund_or_chargeback" || revenueQuality === "negative_net_revenue") {
    return {
      title: "Review refund or chargeback",
      rationale: "The customer has negative or refund-heavy value. Confirm whether this is a true refund, chargeback, duplicate, or source mapping issue before reporting revenue.",
      priority: "Data risk",
      priorityTone: "red",
      channel: "Audit first",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (nextAction === "repair_historical_product" || stringValue(profile?.top_product_family) === "Unknown / historical Stripe") {
    return {
      title: "Repair product history",
      rationale: "The buyer has money attached, but product truth is weak because historical Stripe data is not product-clean yet.",
      priority: "Cleanup",
      priorityTone: "amber",
      channel: "Source repair",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (from === "revenue" && !knownOperatorName(profile?.credited_closer_name)) {
    return {
      title: "Assign revenue credit",
      rationale: "The buyer has revenue, but no trusted operator credit. Check booking, call, and Fathom context before this lands in rep reporting.",
      priority: "Attribution gap",
      priorityTone: "amber",
      channel: "Audit first",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (from === "lead-magnets") {
    return {
      title: "Validate the magnet-to-money path",
      rationale: "This is a buyer-level drilldown from the Lead Magnets tab. Confirm the latest prior magnet, booking path, and purchase timing before treating the offer as revenue-driving.",
      priority: "Attribution read",
      priorityTone: "blue",
      channel: "Proof review",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (revenueQuality && revenueQuality !== "clean") {
    return {
      title: "Repair source truth",
      rationale: "This profile has a revenue quality flag. Resolve the source issue first so the dashboard does not turn bad attribution into confident reporting.",
      priority: "Data risk",
      priorityTone: "amber",
      channel: "Source repair",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  if (health === "active_plan_not_yet_due" || nextAction === "watch_next_due_date") {
    return {
      title: "Watch the next due date",
      rationale: "The customer appears active and not yet due. Keep the profile warm, but no urgent recovery move is showing right now.",
      priority: "Monitor",
      priorityTone: "green",
      channel: "No rush",
      owner: revenueCredit,
      ownerHelper: revenueCreditHelper,
    };
  }

  return {
    title: labelize(nextAction) === "N/A" ? "Review this customer" : labelize(nextAction),
    rationale: "The customer has enough modeled evidence to make a decision. Start with revenue credit, money, latest touch, and lead source before taking action.",
    priority: "Operator read",
    priorityTone: "blue",
    channel: "Review proof",
    owner: revenueCredit,
    ownerHelper: revenueCreditHelper,
  };
}

function actionEvidence(
  profile: DashboardRow | undefined,
  payments: DashboardRow[],
  refunds: DashboardRow[],
  outreach: DashboardRow[],
  bookings: DashboardRow[],
  magnetTrail: DashboardRow[],
): CommandFactItem[] {
  const latestPayment = payments[0];
  const latestRefund = refunds[0];
  const latestTouch = outreach[0];
  const latestBooking = bookings[0];
  const latestMagnet = magnetTrail[0];

  const items: CommandFactItem[] = [
    {
      label: "latest money",
      value: latestPayment
        ? `${formatCurrency(numberValue(latestPayment.net_amount))} ${labelize(stringValue(latestPayment.source_platform))}`
        : "No payment row",
      helper: latestPayment
        ? [stringValue(latestPayment.transaction_label), stringValue(latestPayment.product)].filter(Boolean).join(" · ") || "Payment evidence"
        : "No payment evidence found",
      tone: latestPayment ? "green" : "amber",
    },
    {
      label: "latest touch",
      value: latestTouch
        ? `${labelize(stringValue(latestTouch.channel))} · ${labelize(stringValue(latestTouch.message_status))}`
        : "No recent touch",
      helper: latestTouch
        ? [stringValue(latestTouch.touched_label), stringValue(latestTouch.user_name)].filter(Boolean).join(" · ") || "Outreach evidence"
        : "No outreach evidence found",
      tone: latestTouch ? "green" : "amber",
    },
    {
      label: "booking proof",
      value: latestBooking
        ? labelize(stringValue(latestBooking.event_status))
        : stringValue(profile?.booking_before_purchase_label)
          ? "Booked before buy"
          : "No booking row",
      helper: latestBooking
        ? [stringValue(latestBooking.scheduled_label), stringValue(latestBooking.assigned_user_name)].filter(Boolean).join(" · ") || "Booking evidence"
        : stringValue(profile?.booking_before_purchase_label) ?? "No booking evidence found",
      tone: latestBooking || stringValue(profile?.booking_before_purchase_label) ? "green" : "amber",
    },
    {
      label: "lead source",
      value:
        stringValue(latestMagnet?.lead_magnet_name) ??
        stringValue(profile?.latest_prior_lead_magnet_name) ??
        stringValue(profile?.lead_source) ??
        "Unknown source",
      helper:
        stringValue(latestMagnet?.opportunity_created_label) ??
        stringValue(profile?.latest_prior_opportunity_label) ??
        ([stringValue(profile?.utm_source), stringValue(profile?.utm_campaign)].filter(Boolean).join(" · ") || "No source proof"),
      tone: stringValue(latestMagnet?.lead_magnet_name) || stringValue(profile?.latest_prior_lead_magnet_name) ? "green" : "amber",
    },
    {
      label: "refund pressure",
      value: formatCurrency(numberValue(profile?.lifetime_refunds_amount)),
      helper: latestRefund
        ? `${formatNumber(numberValue(profile?.lifetime_refunds_count))} refunds · latest ${stringValue(latestRefund.refunded_label) ?? "unknown date"}`
        : `${formatNumber(numberValue(profile?.lifetime_refunds_count))} refunds`,
      tone: numberValue(profile?.lifetime_refunds_count) ? "amber" : "green",
    },
  ];

  return items.slice(0, 4);
}

function actionBlockers(profile: DashboardRow | undefined, sourceContext: Customer360ViewProps["sourceContext"]) {
  const blockers: Array<{ value: string; helper: string }> = [];
  const revenueCredit = knownOperatorName(profile?.credited_closer_name);
  const quality = stringValue(profile?.revenue_funnel_quality_flag);

  if (!stringValue(profile?.phone) && !stringValue(profile?.email_norm)) {
    blockers.push({ value: "No direct contact", helper: "Missing phone and email" });
  }

  if (!revenueCredit) {
    blockers.push({ value: "No revenue credit", helper: "Closer credit is not assigned" });
  }

  if (quality && quality !== "clean") {
    blockers.push({ value: labelize(quality), helper: "Revenue quality flag" });
  }

  if (sourceContext?.from === "lead-magnets" && !stringValue(profile?.latest_prior_lead_magnet_name)) {
    blockers.push({ value: "No prior magnet", helper: "Buyer attribution needs context" });
  }

  return blockers;
}

function CommandFact({
  label,
  value,
  helper,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  tone: CommandFactTone;
}) {
  const toneClass = {
    green: "border-[#bbf7d0] bg-[#f0fdf4]",
    blue: "border-[#bfdbfe] bg-[#eff6ff]",
    amber: "border-[#fde68a] bg-[#fffbeb]",
  }[tone];

  return (
    <div className={`rounded-md border p-2 ${toneClass}`}>
      <div className="truncate text-[10px] font-semibold uppercase text-[#66635f]">{label}</div>
      <div className="mt-1 truncate text-sm font-semibold text-[#2d2b28]" title={value}>{value}</div>
      <div className="truncate text-[11px] text-[#66635f]" title={helper}>{helper}</div>
    </div>
  );
}

function priorityClass(tone: string) {
  if (tone === "red") return "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]";
  if (tone === "amber") return "border-[#fde68a] bg-[#fffbeb] text-[#92400e]";
  if (tone === "green") return "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]";
  return "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]";
}

function actionAreaClass(area: string | null) {
  if (area === "retention") return "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]";
  if (area === "revenue") return "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]";
  return "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]";
}

function reviewStatusLabel(status: string | null) {
  if (status === "wont_fix") return "Won't fix";
  if (status === "fixed") return "Fixed";
  if (status === "reviewed") return "Reviewed";
  return "Open";
}

function knownOperatorName(value: DashboardRowValue | undefined) {
  const name = stringValue(value);
  if (!name) return null;

  const normalized = name.toLowerCase();
  if (
    normalized === "unassigned" ||
    normalized === "unassigned / unknown" ||
    normalized === "unknown" ||
    normalized === "n/a"
  ) {
    return null;
  }

  return name;
}

function attributionHelper(
  source: DashboardRowValue | undefined,
  confidence: DashboardRowValue | undefined,
  fallback: string,
) {
  const parts = [stringValue(source), stringValue(confidence)].filter((part) => {
    if (!part) return false;
    return !["unassigned", "unknown", "missing"].includes(part.toLowerCase());
  });

  return parts.join(" · ") || fallback;
}

function cleanPhoneHref(value: string) {
  return value.replace(/[^\d+]/g, "");
}

function ProfilePanel({ profile }: { profile: DashboardRow | undefined }) {
  return (
    <Panel title="Customer Truth" helper="Identity, entry source, purchase source, and source IDs.">
      <div className="grid gap-3 md:grid-cols-2">
        <SourceBlock title="Identity">
          <SourceLine label="GHL contact" value={stringValue(profile?.contact_id)} />
          <SourceLine label="Email" value={stringValue(profile?.email_norm)} />
          <SourceLine label="Phone" value={stringValue(profile?.phone)} />
          <SourceLine label="Created" value={stringValue(profile?.contact_created_label)} />
          <SourceLine label="Location" value={[stringValue(profile?.city), stringValue(profile?.state), stringValue(profile?.country)].filter(Boolean).join(", ")} />
        </SourceBlock>
        <SourceBlock title="Entry Source">
          <SourceLine label="Lead source" value={stringValue(profile?.lead_source)} />
          <SourceLine label="UTM source" value={stringValue(profile?.utm_source)} />
          <SourceLine label="UTM medium" value={stringValue(profile?.utm_medium)} />
          <SourceLine label="UTM campaign" value={stringValue(profile?.utm_campaign)} />
          <SourceLine label="Timezone" value={stringValue(profile?.timezone)} />
        </SourceBlock>
        <SourceBlock title="Purchase">
          <SourceLine label="First paid" value={stringValue(profile?.first_purchase_label)} />
          <SourceLine label="Latest paid" value={stringValue(profile?.latest_purchase_label)} />
          <SourceLine label="Upfront net" value={formatCurrency(numberValue(profile?.upfront_collected_net_revenue))} />
          <SourceLine label="After first" value={formatCurrency(numberValue(profile?.post_first_collected_net_revenue))} />
          <SourceLine label="Contract evidence" value={labelize(stringValue(profile?.contract_evidence_status))} />
          <SourceLine label="Top product" value={stringValue(profile?.top_product_by_net_revenue)} />
          <SourceLine label="Product family" value={stringValue(profile?.top_product_family)} />
          <SourceLine label="Products" value={stringValue(profile?.lifetime_purchased_products)} />
        </SourceBlock>
        <SourceBlock title="Fanbasis">
          <SourceLine label="Lifecycle" value={labelize(stringValue(profile?.customer_lifecycle_status))} />
          <SourceLine label="Sub status" value={stringValue(profile?.latest_fanbasis_subscription_status)} />
          <SourceLine label="Product" value={stringValue(profile?.latest_fanbasis_product_title)} />
          <SourceLine label="Customer IDs" value={stringValue(profile?.fanbasis_customer_ids)} />
          <SourceLine label="Subscriber IDs" value={stringValue(profile?.fanbasis_subscription_ids)} />
        </SourceBlock>
      </div>
    </Panel>
  );
}

function ActionPanel({ profile }: { profile: DashboardRow | undefined }) {
  const missed = booleanValue(profile?.is_expected_payment_missed_now);
  const due = booleanValue(profile?.is_expected_payment_due_now);

  return (
    <Panel title="Operator Read" helper="What this customer needs next, plus the proof behind it.">
      <div className="grid gap-2 sm:grid-cols-2">
        <SignalBox label="next action" value={labelize(stringValue(profile?.retention_operator_next_action))} helper={labelize(stringValue(profile?.payment_plan_health_status))} tone={due || missed ? "amber" : "green"} />
        <SignalBox label="repeat type" value={labelize(stringValue(profile?.repeat_payment_type))} helper={labelize(stringValue(profile?.payment_plan_truth_status))} tone="green" />
        <SignalBox label="collection" value={labelize(stringValue(profile?.collection_health_status))} helper={`${formatCurrency(numberValue(profile?.post_first_collected_net_revenue))} after first pay`} tone={stringValue(profile?.collection_health_status)?.includes("review") ? "amber" : "green"} />
        <SignalBox label="due timing" value={formatDueStatus(profile)} helper={stringValue(profile?.expected_next_payment_label) ?? "No expected date"} tone={due || missed ? "amber" : "green"} />
      </div>

      <div className="mt-3 grid gap-3">
        <SourceBlock title="Collection">
          <SourceLine label="Motion" value={labelize(stringValue(profile?.collection_motion_type))} />
          <SourceLine label="Later payments" value={formatNumber(numberValue(profile?.post_first_paid_payments_count))} />
          <SourceLine label="Latest later pay" value={stringValue(profile?.latest_post_first_payment_label)} />
          <SourceLine label="Collection call" value={stringValue(profile?.latest_collection_booking_name)} />
          <SourceLine label="Call date" value={stringValue(profile?.latest_collection_booking_label)} />
        </SourceBlock>
        <SourceBlock title="Sales Call Terms">
          <SourceLine label="Confirmed promised" value={formatCurrency(numberValue(profile?.confirmed_promised_contract_value))} />
          <SourceLine label="Confirmed upfront" value={formatCurrency(numberValue(profile?.confirmed_upfront_agreed_amount))} />
          <SourceLine label="Confirmed balance" value={formatCurrency(numberValue(profile?.confirmed_balance_expected_amount))} />
          <SourceLine label="Confidence" value={labelize(stringValue(profile?.confirmed_review_confidence))} />
          <SourceLine label="Reviewed" value={stringValue(profile?.confirmed_terms_reviewed_label)} />
          <SourceLine label="Status" value={labelize(stringValue(profile?.contract_evidence_status))} />
          <SourceLine label="Calls checked" value={formatNumber(numberValue(profile?.candidate_sales_calls_count))} />
          <SourceLine label="Snippets" value={formatNumber(numberValue(profile?.payment_terms_snippets_count))} />
          <SourceLine label="Amounts said" value={stringValue(profile?.mentioned_payment_amounts_text)} />
          <SourceLine label="Largest said" value={formatCurrency(numberValue(profile?.largest_mentioned_payment_amount))} />
          <EvidenceText value={stringValue(profile?.payment_terms_evidence_text)} />
        </SourceBlock>
        <SourceBlock title="Attribution">
          <SourceLine label="Revenue credit" value={knownOperatorName(profile?.credited_closer_name) ?? "Unassigned / unknown"} />
          <SourceLine label="Credit source" value={attributionHelper(profile?.credited_closer_source, profile?.credited_closer_confidence, "No revenue credit source")} />
          <SourceLine label="Setter" value={knownOperatorName(profile?.credited_setter_name) ?? "Unassigned / unknown"} />
          <SourceLine label="Setter source" value={stringValue(profile?.credited_setter_source) ?? "No setter source"} />
          <SourceLine label="Current owner" value="Unknown" />
          <SourceLine label="Owner source" value="Not modeled yet" />
          <SourceLine label="Path" value={labelize(stringValue(profile?.pre_purchase_funnel_path))} />
        </SourceBlock>
        <SourceBlock title="Lead Magnet">
          <SourceLine label="Latest prior" value={stringValue(profile?.latest_prior_lead_magnet_name)} />
          <SourceLine label="Category" value={stringValue(profile?.latest_prior_lead_magnet_category)} />
          <SourceLine label="Offer type" value={stringValue(profile?.latest_prior_lead_magnet_offer_type)} />
          <SourceLine label="Opp created" value={stringValue(profile?.latest_prior_opportunity_label)} />
          <SourceLine label="Flag" value={stringValue(profile?.purchase_magnet_attribution_flag)} />
        </SourceBlock>
        <SourceBlock title="Data Honesty">
          <SourceLine label="Retention" value={stringValue(profile?.retention_quality_flag)} />
          <SourceLine label="Revenue" value={stringValue(profile?.revenue_funnel_quality_flag)} />
          <SourceLine label="Revenue magnet" value={stringValue(profile?.revenue_purchase_magnet_attribution_flag)} />
          <SourceLine label="Booking before buy" value={stringValue(profile?.booking_before_purchase_label)} />
        </SourceBlock>
      </div>
    </Panel>
  );
}

function PaymentsPanel({ payments, refunds }: { payments: DashboardRow[]; refunds: DashboardRow[] }) {
  return (
    <Panel title="Money Trail" helper="Paid payments and refunds tied to this contact.">
      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <TableHead>Date</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Product</TableHead>
              <TableHead>Net</TableHead>
              <TableHead>Status</TableHead>
            </tr>
          </thead>
          <tbody>
            {payments.map((row) => {
              const nativePresentment = formatNativePresentment(row);

              return (
                <tr key={stringValue(row.payment_id) ?? "payment"}>
                  <TableCell strong>{stringValue(row.transaction_label)}</TableCell>
                  <TableCell>{labelize(stringValue(row.source_platform))}</TableCell>
                  <TableCell>{stringValue(row.product) ?? "Unknown product"}</TableCell>
                  <TableCell strong>
                    <div>{formatCurrency(numberValue(row.net_amount))}</div>
                    {nativePresentment ? <div className="mt-0.5 text-[10px] font-medium text-[#8c8780]">{nativePresentment}</div> : null}
                  </TableCell>
                  <TableCell>{booleanValue(row.is_refunded) ? "Refunded" : booleanValue(row.is_paid) ? "Paid" : "Not paid"}</TableCell>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {refunds.length ? (
        <div className="mt-3 grid gap-2 sm:grid-cols-2">
          {refunds.map((row) => (
            <SignalBox
              key={stringValue(row.refund_id) ?? "refund"}
              label={stringValue(row.refunded_label) ?? "Refund"}
              value={formatCurrency(numberValue(row.refund_amount))}
              helper={`${labelize(stringValue(row.source_platform))} · parent ${shortHash(stringValue(row.parent_payment_id) ?? "")}`}
              tone="amber"
            />
          ))}
        </div>
      ) : null}
    </Panel>
  );
}

function RelationshipTimelinePanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 16);

  return (
    <section className="mb-3 rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-sm font-semibold">Relationship Timeline</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
            The customer story in order: source, booking or call, money, collection evidence, and current state.
          </p>
        </div>
        <span className="w-fit rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1 text-[11px] font-semibold uppercase text-[#66635f]">
          {formatNumber(rows.length)} events
        </span>
      </div>

      <div className="mt-3 grid gap-2 xl:grid-cols-2">
        {visibleRows.length ? (
          visibleRows.map((row, index) => (
            <div
              key={`${stringValue(row.event_type) ?? "event"}-${stringValue(row.event_id) ?? index}`}
              className="grid gap-3 rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3 sm:grid-cols-[7.5rem_minmax(0,1fr)_7rem] sm:items-start"
            >
              <div className="text-xs">
                <div className="font-semibold text-[#2d2b28]">{stringValue(row.event_date_label) ?? "No date"}</div>
                <div className="mt-1 truncate text-[11px] text-[#66635f]">{shortTime(stringValue(row.event_time_label))}</div>
              </div>
              <div className="min-w-0">
                <div className="flex flex-wrap items-center gap-2">
                  <span className={`rounded-md border px-2 py-1 text-[10px] font-semibold uppercase ${relationshipTone(stringValue(row.event_type))}`}>
                    {stringValue(row.event_label) ?? "Event"}
                  </span>
                  <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-[10px] font-semibold text-[#66635f]">
                    {stringValue(row.event_source)}
                  </span>
                </div>
                <div className="mt-2 truncate text-sm font-semibold text-[#2d2b28]" title={stringValue(row.event_title) ?? undefined}>
                  {labelize(stringValue(row.event_title))}
                </div>
                <div className="mt-1 line-clamp-2 text-[11px] leading-5 text-[#66635f]" title={stringValue(row.event_detail) ?? undefined}>
                  {stringValue(row.event_detail) ?? labelize(stringValue(row.event_quality))}
                </div>
              </div>
              <MetricCell label="value" value={timelineValue(row)} />
            </div>
          ))
        ) : (
          <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3 text-xs text-[#66635f]">
            No timeline events found for this customer.
          </div>
        )}
      </div>
    </section>
  );
}

function TimelinePanel({ bookings, outreach }: { bookings: DashboardRow[]; outreach: DashboardRow[] }) {
  return (
    <Panel title="Calls And Follow-Up" helper="Calendly bookings and recent human call/SMS touches.">
      <div className="grid gap-3">
        <SourceBlock title="Bookings">
          {bookings.length ? (
            bookings.slice(0, 8).map((row) => (
              <TimelineItem
                key={stringValue(row.calendly_event_id) ?? "booking"}
                title={stringValue(row.event_name) ?? "Calendly booking"}
                meta={`${stringValue(row.scheduled_label) ?? stringValue(row.booked_label) ?? "No date"} · ${labelize(stringValue(row.event_status))}`}
                detail={[stringValue(row.assigned_user_name), stringValue(row.stage_name)].filter(Boolean).join(" · ")}
              />
            ))
          ) : (
            <EmptyLine>No bookings found.</EmptyLine>
          )}
        </SourceBlock>
        <SourceBlock title="Recent Outreach">
          {outreach.length ? (
            outreach.slice(0, 10).map((row) => (
              <TimelineItem
                key={stringValue(row.touch_sk) ?? "touch"}
                title={`${labelize(stringValue(row.channel))} · ${labelize(stringValue(row.message_status))}`}
                meta={stringValue(row.touched_label) ?? "No timestamp"}
                detail={[stringValue(row.user_name), stringValue(row.message_source)].filter(Boolean).join(" · ")}
              />
            ))
          ) : (
            <EmptyLine>No human outreach found.</EmptyLine>
          )}
        </SourceBlock>
      </div>
    </Panel>
  );
}

function MagnetTrailPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <Panel title="Lead Magnet Trail" helper="Opportunity windows tied to this contact.">
      <div className="space-y-2">
        {rows.length ? (
          rows.map((row) => (
            <div key={stringValue(row.opportunity_id) ?? "opportunity"} className="rounded-md border border-[#ece9e1] p-3">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#2d2b28]">{stringValue(row.lead_magnet_name) ?? "Unknown magnet"}</div>
                  <div className="mt-1 truncate text-[11px] text-[#66635f]">{stringValue(row.opportunity_created_label)} · {labelize(stringValue(row.opportunity_status))}</div>
                </div>
                <MetricCell label="net" value={formatCurrency(numberValue(row.net_revenue_after_refunds))} />
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                <SignalBox label="touches" value={formatNumber(numberValue(row.touches_count))} helper={`${formatNumber(numberValue(row.call_count))} calls · ${formatNumber(numberValue(row.sms_count))} SMS`} tone="green" />
                <SignalBox label="bookings" value={formatNumber(numberValue(row.window_bookings_count))} helper={`${formatNumber(numberValue(row.canceled_bookings_count))} canceled`} tone={numberValue(row.canceled_bookings_count) ? "amber" : "green"} />
                <SignalBox label="quality" value={labelize(stringValue(row.attribution_quality_flag))} helper={stringValue(row.assigned_user_name) ?? "No opportunity owner"} tone={stringValue(row.attribution_quality_flag) === "clean" ? "green" : "amber"} />
              </div>
            </div>
          ))
        ) : (
          <EmptyLine>No lead magnet opportunities found.</EmptyLine>
        )}
      </div>
    </Panel>
  );
}

function RetentionMonthsPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <Panel title="Retention Timeline" helper="Customer-month payment and refund history.">
      <div className="space-y-2">
        {rows.length ? (
          rows.map((row) => (
            <div key={stringValue(row.activity_month_label) ?? "month"} className="grid gap-2 rounded-md border border-[#ece9e1] p-3 sm:grid-cols-[8rem_minmax(0,1fr)_7rem] sm:items-center">
              <div className="font-semibold text-[#2d2b28]">{stringValue(row.activity_month_label)}</div>
              <div className="min-w-0">
                <div className="truncate text-xs font-medium text-[#0f766e]">{labelize(stringValue(row.retention_state))}</div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.paid_payments_in_month))} payments · {formatNumber(numberValue(row.refunds_count_in_month))} refunds
                </div>
              </div>
              <MetricCell label="net" value={formatCurrency(numberValue(row.net_revenue_after_refunds_in_month))} />
            </div>
          ))
        ) : (
          <EmptyLine>No retention months found.</EmptyLine>
        )}
      </div>
    </Panel>
  );
}

function KpiCard({
  title,
  value,
  helper,
  icon: Icon,
  tone,
}: {
  title: string;
  value: string;
  helper: string;
  icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  tone: "green" | "blue" | "amber";
}) {
  const toneClass = {
    green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
    blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
    amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  }[tone];

  return (
    <article className="rounded-lg border border-[#dedbd2] bg-white p-3 shadow-sm">
      <div className="flex items-start justify-between gap-2">
        <p className="text-[11px] font-semibold uppercase text-[#66635f]">{title}</p>
        <span className={`rounded-md border p-1.5 ${toneClass}`}>
          <Icon className="h-4 w-4" aria-hidden />
        </span>
      </div>
      <div className="mt-3 truncate text-xl font-semibold tracking-normal">{value}</div>
      <div className="mt-1 truncate text-[11px] text-[#66635f]">{helper}</div>
    </article>
  );
}

function Panel({ title, helper, children }: { title: string; helper: string; children: ReactNode }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">{title}</h2>
          <p className="mt-1 text-xs text-[#66635f]">{helper}</p>
        </div>
      </div>
      <div className="mt-3">{children}</div>
    </section>
  );
}

function SourceBlock({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
      <div className="text-[11px] font-semibold uppercase text-[#66635f]">{title}</div>
      <div className="mt-2 space-y-1.5">{children}</div>
    </div>
  );
}

function SourceLine({ label, value }: { label: string; value: string | null }) {
  if (!value) return null;

  return (
    <div className="grid gap-2 text-xs sm:grid-cols-[7.5rem_minmax(0,1fr)]">
      <span className="text-[#66635f]">{label}</span>
      <span className="min-w-0 truncate font-medium text-[#2d2b28]" title={value}>{value}</span>
    </div>
  );
}

function EvidenceText({ value }: { value: string | null }) {
  if (!value) return null;

  return (
    <div className="mt-2 max-h-28 overflow-auto whitespace-pre-wrap rounded-md border border-[#dedbd2] bg-white p-2 text-[11px] leading-5 text-[#3b3936]">
      {value}
    </div>
  );
}

function SignalBox({
  label,
  value,
  helper,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  tone: "green" | "amber";
}) {
  const toneClass = {
    green: "border-[#bbf7d0] bg-[#f0fdf4]",
    amber: "border-[#fde68a] bg-[#fffbeb]",
  }[tone];

  return (
    <div className={`rounded-md border p-2 ${toneClass}`}>
      <div className="truncate text-[10px] font-semibold uppercase text-[#66635f]">{label}</div>
      <div className="mt-1 truncate text-base font-semibold tracking-normal text-[#2d2b28]">{value}</div>
      <div className="truncate text-[11px] text-[#66635f]">{helper}</div>
    </div>
  );
}

function TimelineItem({ title, meta, detail }: { title: string; meta: string; detail: string }) {
  return (
    <div className="border-b border-[#ece9e1] py-2 last:border-b-0">
      <div className="truncate text-xs font-semibold text-[#2d2b28]">{title}</div>
      <div className="mt-1 truncate text-[11px] text-[#66635f]">{meta}</div>
      {detail ? <div className="mt-1 truncate text-[11px] text-[#0f766e]">{detail}</div> : null}
    </div>
  );
}

function EmptyLine({ children }: { children: ReactNode }) {
  return <div className="text-xs text-[#66635f]">{children}</div>;
}

function TableHead({ children }: { children: ReactNode }) {
  return <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">{children}</th>;
}

function TableCell({ children, strong = false }: { children: ReactNode; strong?: boolean }) {
  return (
    <td className={`max-w-48 border-b border-[#ece9e1] px-2 py-2 pl-0 ${strong ? "font-semibold text-[#2d2b28]" : "text-[#66635f]"}`}>
      <span className="block truncate">{children}</span>
    </td>
  );
}

function MetricCell({ label, value }: { label: string; value: string }) {
  return (
    <span className="min-w-0">
      <span className="block text-[10px] font-semibold uppercase text-[#66635f]">{label}</span>
      <span className="block truncate text-sm font-semibold text-[#2d2b28]">{value}</span>
    </span>
  );
}

function AuditDetails({ data }: { data: DashboardData }) {
  return (
    <details className="mt-2 rounded-lg border border-[#dedbd2] bg-white shadow-sm">
      <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">Source Table And Contract</summary>
      <div className="border-t border-[#ece9e1] p-4">
        {data.dataContract ? (
          <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3 text-xs text-[#66635f]">
            <div className="font-semibold text-[#2d2b28]">
              {data.dataContract.projectId}.{data.dataContract.dataset}
            </div>
            <div className="mt-1">{data.dataContract.note}</div>
          </div>
        ) : null}
      </div>
    </details>
  );
}

function formatDueStatus(row: DashboardRow | undefined) {
  const missed = booleanValue(row?.is_expected_payment_missed_now);
  const due = booleanValue(row?.is_expected_payment_due_now);
  const daysPast = numberValue(row?.days_past_expected_payment);
  const daysUntil = numberValue(row?.days_until_expected_next_payment);

  if (missed && daysPast !== null) return `${formatNumber(daysPast)}d past`;
  if (missed) return "Past due";
  if (due) return "Due now";
  if (daysUntil !== null && daysUntil >= 0) return `${formatNumber(daysUntil)}d out`;
  return "No due date";
}

function labelize(value: string | null) {
  if (!value) return "N/A";
  return value
    .replaceAll("_", " ")
    .replaceAll("/", " / ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatNumber(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatCurrency(value: number | null) {
  return formatMoney(value, "USD");
}

function formatMoney(value: number | null, currency: string) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  const hasCents = Math.abs(value % 1) > 0.001;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    minimumFractionDigits: hasCents ? 2 : 0,
    maximumFractionDigits: hasCents ? 2 : 0,
  }).format(value);
}

function formatNativePresentment(row: DashboardRow) {
  const nativeCurrency = stringValue(row.source_presentment_currency)?.toUpperCase();
  const reportingCurrency = stringValue(row.currency)?.toUpperCase();
  const nativeGross = numberValue(row.source_presentment_gross_amount);

  if (!nativeCurrency || !reportingCurrency || nativeCurrency === reportingCurrency || nativeGross === null) {
    return null;
  }

  return `Native charge ${formatMoney(nativeGross, nativeCurrency)}`;
}

function relationshipTone(eventType: string | null) {
  if (eventType === "first_purchase" || eventType === "repeat_payment" || eventType === "current_status") {
    return "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]";
  }

  if (eventType === "refund") {
    return "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]";
  }

  if (eventType === "contract_terms_evidence" || eventType === "contract_terms_confirmed") {
    return "border-[#fde68a] bg-[#fffbeb] text-[#92400e]";
  }

  return "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]";
}

function timelineValue(row: DashboardRow) {
  const value = numberValue(row.event_amount);
  if (value !== null) return formatCurrency(value);
  return labelize(stringValue(row.event_quality));
}

function shortTime(value: string | null) {
  if (!value) return "No time";
  return value.replace(/^[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}\s+/, "");
}

function numberValue(value: DashboardRowValue | undefined) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }
  return null;
}

function booleanValue(value: DashboardRowValue | undefined) {
  return value === true || value === "true" || value === 1 || value === "1";
}

function stringValue(value: DashboardRowValue | undefined) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}

function shortHash(value: string) {
  if (value.length <= 10) return value;
  return `${value.slice(0, 6)}...${value.slice(-4)}`;
}
