import type { ComponentType } from "react";
import { AlertTriangle, CalendarCheck, CreditCard, DollarSign, RefreshCcw, ShieldCheck, Target, UserX } from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { OperatorActionReviewButtons } from "@/components/dashboard/OperatorActionReviewButtons";
import type { DashboardData, DashboardFilters, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

type KpiCard = {
  title: string;
  value: string;
  helper: string;
  tone: "green" | "blue" | "amber" | "red" | "neutral";
  icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
};

const toneClasses = {
  green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
  amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  neutral: "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]",
};

export function RevenueFunnelOperatingView({ data }: { data: DashboardData }) {
  const summary = data.rows.revenue_funnel_summary?.[0];
  const paymentPlans = data.rows.revenue_funnel_payment_plans ?? [];
  const paymentTruth = data.rows.revenue_funnel_payment_truth ?? [];
  const productFamilies = data.rows.revenue_funnel_product_families ?? [];
  const paths = data.rows.revenue_funnel_paths ?? [];
  const magnets = data.rows.revenue_funnel_magnets ?? [];
  const operatorDiagnostic = data.rows.revenue_funnel_operator_diagnostic ?? [];
  const customerWorklist = data.rows.revenue_funnel_customer_worklist ?? [];
  const actionQueueSummary = data.rows.revenue_funnel_action_queue_summary ?? [];
  const topValueGaps = data.rows.revenue_funnel_top_value_gaps ?? [];
  const quality = data.rows.revenue_funnel_quality ?? [];
  const retentionSummary = data.rows.customer_retention_summary?.[0];
  const retentionStates = data.rows.customer_retention_states ?? [];
  const identityHealth = data.rows.revenue_identity_health?.[0];
  const canceledRecoverySummary = data.rows.revenue_canceled_recovery_summary?.[0];
  const canceledRecoveryByActor = data.rows.revenue_canceled_recovery_by_actor ?? [];

  const buyers = numberValue(summary?.buyers);
  const netRevenue = numberValue(summary?.total_net_revenue_after_refunds);
  const paymentPlanBuyers = numberValue(summary?.payment_plan_buyers);
  const autoRenewPayments = numberValue(summary?.fanbasis_auto_renew_payments);
  const bookingRate = numberValue(summary?.booking_before_purchase_rate);
  const magnetCoverage = numberValue(summary?.latest_prior_magnet_buyer_coverage);
  const unassignedRate = numberValue(summary?.unassigned_operator_rate);

  const kpis: KpiCard[] = [
    {
      title: "Collected Net",
      value: formatCurrency(netRevenue),
      helper: `${formatNumber(buyers)} matched paid buyers`,
      tone: "green",
      icon: DollarSign,
    },
    {
      title: "Revenue / Buyer",
      value: formatCurrency(numberValue(summary?.revenue_per_buyer)),
      helper: `${formatDecimal(numberValue(summary?.payments_per_buyer))} payments per buyer`,
      tone: "blue",
      icon: DollarSign,
    },
    {
      title: "Payment-Plan Buyers",
      value: formatPercent(numberValue(summary?.payment_plan_buyer_rate)),
      helper: `${formatNumber(paymentPlanBuyers)} buyers · ${formatNumber(autoRenewPayments)} auto-renew payments`,
      tone: "amber",
      icon: CreditCard,
    },
    {
      title: "Booked Before Buying",
      value: formatPercent(bookingRate),
      helper: `${formatHours(numberValue(summary?.median_hours_booking_to_purchase))} median booking-to-buy`,
      tone: "blue",
      icon: CalendarCheck,
    },
    {
      title: "Magnet Coverage",
      value: formatPercent(magnetCoverage),
      helper: "Latest known magnet before first purchase",
      tone: "green",
      icon: Target,
    },
    {
      title: "Revenue Credit Gap",
      value: formatPercent(unassignedRate),
      helper: `${formatNumber(numberValue(summary?.low_confidence_credit_buyers))} low-confidence, ${formatNumber(numberValue(summary?.true_unassigned_operator_buyers))} truly unknown`,
      tone: unassignedRate && unassignedRate > 0.8 ? "red" : "amber",
      icon: UserX,
    },
  ];

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">dbt-mart</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            Revenue Funnel
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            What turned into money, how it was paid, what came before the purchase, and where attribution is still too thin to flex.
          </p>
        </div>
        <div className="flex flex-col gap-2 md:items-end">
          <FreshnessBadge freshness={data.freshness} />
          <TimeRangeControl filters={data.filters} />
        </div>
      </header>

      {data.error ? (
        <div className="mt-4 rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <section className="py-4">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
          {kpis.map((kpi) => (
            <KpiCard key={kpi.title} kpi={kpi} />
          ))}
        </div>

        <div className="mt-3">
          <IdentityHealthPanel row={identityHealth} />
        </div>

        <div className="mt-3">
          <RevenueCustomerWorklistPanel
            rows={customerWorklist}
            summaryRows={actionQueueSummary}
            topValueRows={topValueGaps}
            filters={data.filters}
          />
        </div>

        <div className="mt-3">
          <CanceledRecoveryPanel summary={canceledRecoverySummary} actorRows={canceledRecoveryByActor} />
        </div>

        <div className="mt-3">
          <RetentionPulsePanel summary={retentionSummary} stateRows={retentionStates} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
          <PaymentPlanPanel rows={paymentPlans} />
          <FanbasisTruthPanel rows={paymentTruth} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
          <ProductFamilyPanel rows={productFamilies} />
          <FunnelPathPanel rows={paths} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
          <TopMagnetsPanel rows={magnets} />
          <QualityPanel rows={quality} />
        </div>

        <div className="mt-3">
          <RevenueCreditPanel rows={operatorDiagnostic} />
        </div>
      </section>

      <AuditDetails data={data} />
    </div>
  );
}

function RetentionPulsePanel({
  summary,
  stateRows,
}: {
  summary: DashboardRow | undefined;
  stateRows: DashboardRow[];
}) {
  const customers = numberValue(summary?.customers);
  const repeatCustomers = numberValue(summary?.repeat_paid_customers);
  const repeatNet = numberValue(summary?.repeat_paid_net_revenue);
  const activeSubscribers = numberValue(summary?.active_fanbasis_subscription_customers);
  const cashNoSubscriber = numberValue(summary?.fanbasis_cash_no_subscriber_customers);
  const qualityGaps = numberValue(summary?.retention_quality_gap_customers);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="grid gap-3 xl:grid-cols-[minmax(16rem,0.72fr)_minmax(0,1.28fr)] xl:items-start">
        <div className="flex items-start gap-3">
          <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-2 text-[#166534]">
            <RefreshCcw className="h-5 w-5" aria-hidden />
          </span>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-sm font-semibold">Retention Pulse</h2>
              <span className="rounded-md border border-[#bbf7d0] px-2 py-1 text-[11px] font-semibold uppercase text-[#166534]">
                customer-month
              </span>
            </div>
            <p className="mt-1 max-w-xl text-xs leading-5 text-[#66635f]">
              Repeat cash, refund months, and current Fanbasis lifecycle evidence without pretending we know unpaid future balances.
            </p>
            <div className="mt-2 text-[11px] leading-5 text-[#66635f]">
              {formatNumber(customers)} customers · {formatCurrency(numberValue(summary?.customer_ltv))} LTV per customer
            </div>
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          <SignalBox
            label="repeat paid"
            value={formatPercent(numberValue(summary?.repeat_paid_customer_rate))}
            helper={`${formatNumber(repeatCustomers)} customers`}
            tone={repeatCustomers ? "green" : "amber"}
          />
          <SignalBox
            label="repeat net"
            value={formatCurrency(repeatNet)}
            helper="Net from repeat paid months"
            tone={repeatNet ? "green" : "amber"}
          />
          <SignalBox
            label="active now"
            value={formatNumber(activeSubscribers)}
            helper="Fanbasis active subscriber evidence"
            tone={activeSubscribers ? "green" : "amber"}
          />
          <SignalBox
            label="source gaps"
            value={formatNumber(qualityGaps)}
            helper={`${formatNumber(cashNoSubscriber)} Fanbasis cash-only buyers`}
            tone={qualityGaps ? "amber" : "green"}
          />
        </div>
      </div>

      <div className="mt-3 grid gap-2 md:grid-cols-3">
        {stateRows.map((row) => (
          <div key={stringValue(row.retention_state) ?? "retention-state"} className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
            <div className="truncate text-sm font-semibold text-[#2d2b28]">
              {stringValue(row.retention_state_label)}
            </div>
            <div className="mt-2 grid grid-cols-3 gap-2">
              <MetricCell label="months" value={formatNumber(numberValue(row.customer_months))} />
              <MetricCell label="buyers" value={formatNumber(numberValue(row.customers))} />
              <MetricCell label="net" value={formatCurrency(numberValue(row.net_revenue_after_refunds))} />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function TimeRangeControl({ filters }: { filters: DashboardFilters }) {
  return (
    <div className="w-full md:w-auto">
      <div className="flex w-full rounded-lg border border-[#dedbd2] bg-white p-1 shadow-sm md:w-auto">
        {filters.timeRangeOptions.map((option) => {
          const isActive = option.value === filters.timeRange;

          return (
            <a
              key={option.value}
              href={`/revenue?range=${option.value}`}
              aria-current={isActive ? "page" : undefined}
              className={`min-w-12 rounded-md px-3 py-1.5 text-center text-xs font-semibold transition ${
                isActive
                  ? "bg-[#0f766e] text-white"
                  : "text-[#66635f] hover:bg-[#f3f1ea] hover:text-[#2d2b28]"
              }`}
            >
              {option.label}
            </a>
          );
        })}
      </div>
      <div className="mt-1 text-right text-[11px] text-[#66635f]">
        {filters.timeRangeDescription}
      </div>
    </div>
  );
}

function KpiCard({ kpi }: { kpi: KpiCard }) {
  const Icon = kpi.icon;

  return (
    <article className="rounded-lg border border-[#dedbd2] bg-white p-3 shadow-sm">
      <div className="flex items-start justify-between gap-2">
        <p className="text-[11px] font-semibold uppercase text-[#66635f]">{kpi.title}</p>
        <span className={`rounded-md border p-1.5 ${toneClasses[kpi.tone]}`}>
          <Icon className="h-4 w-4" aria-hidden />
        </span>
      </div>
      <div className="mt-3 text-xl font-semibold tracking-normal">{kpi.value}</div>
      <div className="mt-1 truncate text-[11px] text-[#66635f]">{kpi.helper}</div>
    </article>
  );
}

function IdentityHealthPanel({ row }: { row: DashboardRow | undefined }) {
  const status = stringValue(row?.identity_health_status);
  const isClean = status === "clean";
  const missingContacts = numberValue(row?.missing_ghl_contacts);
  const unmatchedPaid = numberValue(row?.non_matched_paid_rows);
  const paidContactNullRows = numberValue(row?.paid_contact_null_rows);
  const contactNotMatchedBuyers = numberValue(row?.contact_not_matched_buyers);
  const failedRepairRows = numberValue(row?.latest_contact_sync_failed_rows);
  const createdContacts = numberValue(row?.latest_created_contacts);
  const linkedContacts = numberValue(row?.latest_linked_contacts);
  const historicalStripePaymentOnlyRows = numberValue(row?.historical_stripe_payment_identity_only_rows);
  const historicalStripePaymentOnlyRevenue = numberValue(row?.historical_stripe_payment_identity_only_net_revenue);
  const latestRun = stringValue(row?.latest_contact_sync_run_id);
  const latestSyncAt = stringValue(row?.latest_contact_sync_at);
  const fanbasisRefreshedAt = stringValue(row?.fanbasis_identity_refreshed_at);
  const repairMode = latestRun
    ? row?.latest_contact_sync_was_dry_run === true
      ? "dry run"
      : "live repair"
    : "no repair run";

  return (
    <section className={`rounded-lg border p-4 shadow-sm ${isClean ? "border-[#bbf7d0] bg-[#f0fdf4]" : "border-[#fde68a] bg-[#fffbeb]"}`}>
      <div className="grid gap-3 xl:grid-cols-[minmax(14rem,0.7fr)_minmax(0,1.3fr)] xl:items-center">
        <div className="flex items-start gap-3">
          <span className={`rounded-md border p-2 ${isClean ? "border-[#86efac] bg-white text-[#166534]" : "border-[#fde68a] bg-white text-[#92400e]"}`}>
            {isClean ? <ShieldCheck className="h-5 w-5" aria-hidden /> : <AlertTriangle className="h-5 w-5" aria-hidden />}
          </span>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-sm font-semibold">Identity Health</h2>
              <span className={`rounded-md border px-2 py-1 text-[11px] font-semibold uppercase ${isClean ? "border-[#86efac] text-[#166534]" : "border-[#fcd34d] text-[#92400e]"}`}>
                {isClean ? "clean" : "needs attention"}
              </span>
            </div>
            <p className="mt-1 max-w-xl text-xs leading-5 text-[#66635f]">
              Live Fanbasis paid buyers should exist in GHL, bridge to contacts, and stay out of the unmatched revenue bucket.
            </p>
            <div className="mt-2 text-[11px] leading-5 text-[#66635f]">
              Last repair: {latestSyncAt ? formatDateTime(latestSyncAt) : "N/A"} ·{" "}
              <span className="break-all">{latestRun ?? "no run logged"}</span> · {repairMode}
            </div>
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          <SignalBox
            label="missing ghl"
            value={formatNumber(missingContacts)}
            helper={`${formatNumber(numberValue(row?.contacts_to_create))} create · ${formatNumber(numberValue(row?.contacts_to_repair))} repair`}
            tone={missingContacts ? "amber" : "green"}
          />
          <SignalBox
            label="unmatched paid"
            value={formatNumber(unmatchedPaid)}
            helper={`${formatNumber(numberValue(row?.fanbasis_non_matched_paid_rows))} Fanbasis · ${formatNumber(numberValue(row?.stripe_non_matched_paid_rows))} Stripe`}
            tone={unmatchedPaid ? "amber" : "green"}
          />
          <SignalBox
            label="live contact gaps"
            value={formatNumber(paidContactNullRows)}
            helper={`${formatNumber(contactNotMatchedBuyers)} buyer gaps · ${formatNumber(historicalStripePaymentOnlyRows)} Stripe history (${formatCurrency(historicalStripePaymentOnlyRevenue)})`}
            tone={paidContactNullRows || contactNotMatchedBuyers ? "amber" : "green"}
          />
          <SignalBox
            label="repair result"
            value={`${formatNumber(createdContacts)} / ${formatNumber(linkedContacts)}`}
            helper={`${formatNumber(failedRepairRows)} failed · Fanbasis ${formatDateTime(fanbasisRefreshedAt)}`}
            tone={failedRepairRows ? "amber" : "green"}
          />
        </div>
      </div>
    </section>
  );
}

function CanceledRecoveryPanel({
  summary,
  actorRows,
}: {
  summary: DashboardRow | undefined;
  actorRows: DashboardRow[];
}) {
  const canceledBookings = numberValue(summary?.canceled_bookings);
  const rebookedAfterCancel = numberValue(summary?.rebooked_after_cancel);
  const likelyShowAfterCancel = numberValue(summary?.likely_show_after_cancel);
  const fathomShowEvidence = numberValue(summary?.fathom_show_evidence_after_cancel);
  const buyersAfterCancel = numberValue(summary?.buyers_after_cancel);
  const recoveredRevenue = numberValue(summary?.net_revenue_after_first_cancel);
  const noShowAfterRebook = numberValue(summary?.no_show_after_rebook);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="grid gap-3 xl:grid-cols-[minmax(16rem,0.72fr)_minmax(0,1.28fr)] xl:items-start">
        <div className="flex items-start gap-3">
          <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] p-2 text-[#1d4ed8]">
            <RefreshCcw className="h-5 w-5" aria-hidden />
          </span>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-sm font-semibold">Canceled Booking Recovery</h2>
              <span className="rounded-md border border-[#bfdbfe] px-2 py-1 text-[11px] font-semibold uppercase text-[#1d4ed8]">
                recovery read
              </span>
            </div>
            <p className="mt-1 max-w-xl text-xs leading-5 text-[#66635f]">
              Canceled does not always mean lost. Host cancellations often include triage, reschedules, and cleanup, so this follows what happened next.
            </p>
            <div className="mt-2 text-[11px] leading-5 text-[#66635f]">
              Show signal means the later active booking is due and was not marked no-show; Fathom is the stronger recorded-call signal.
            </div>
          </div>
        </div>

        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          <SignalBox
            label="rebooked"
            value={formatPercent(numberValue(summary?.rebook_rate_after_cancel))}
            helper={`${formatNumber(rebookedAfterCancel)} of ${formatNumber(canceledBookings)}`}
            tone={rebookedAfterCancel ? "green" : "amber"}
          />
          <SignalBox
            label="show signal"
            value={formatNumber(likelyShowAfterCancel)}
            helper={`${formatNumber(fathomShowEvidence)} with Fathom evidence`}
            tone={likelyShowAfterCancel ? "green" : "amber"}
          />
          <SignalBox
            label="buyers after cancel"
            value={formatNumber(buyersAfterCancel)}
            helper={formatCurrency(recoveredRevenue)}
            tone={buyersAfterCancel ? "green" : "amber"}
          />
          <SignalBox
            label="no-show after rebook"
            value={formatNumber(noShowAfterRebook)}
            helper={`${formatHours(numberValue(summary?.median_hours_to_rebook))} median to rebook`}
            tone={noShowAfterRebook ? "amber" : "green"}
          />
        </div>
      </div>

      <div className="mt-3 grid gap-2 md:grid-cols-2">
        {actorRows.map((row) => (
          <div key={stringValue(row.cancel_actor_type) ?? "actor"} className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.cancel_actor_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.canceled_bookings))} canceled bookings
                </div>
              </div>
              <MetricCell label="rebooked" value={formatPercent(numberValue(row.rebook_rate_after_cancel))} />
            </div>
            <div className="mt-3 grid gap-2 sm:grid-cols-3">
              <SignalBox
                label="show signal"
                value={formatNumber(numberValue(row.likely_show_after_cancel))}
                helper={`${formatNumber(numberValue(row.fathom_show_evidence_after_cancel))} Fathom`}
                tone={numberValue(row.likely_show_after_cancel) ? "green" : "amber"}
              />
              <SignalBox
                label="buyers"
                value={formatNumber(numberValue(row.buyers_after_cancel))}
                helper={formatCurrency(numberValue(row.net_revenue_after_first_cancel))}
                tone={numberValue(row.buyers_after_cancel) ? "green" : "amber"}
              />
              <SignalBox
                label="no-show"
                value={formatNumber(numberValue(row.no_show_after_rebook))}
                helper="After rebook"
                tone={numberValue(row.no_show_after_rebook) ? "amber" : "green"}
              />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function RevenueCustomerWorklistPanel({
  rows,
  summaryRows,
  topValueRows,
  filters,
}: {
  rows: DashboardRow[];
  summaryRows: DashboardRow[];
  topValueRows: DashboardRow[];
  filters: DashboardFilters;
}) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Revenue Action Queue"
        helper="Buyer-level gaps prioritized into operator buckets. Fixed or ignored rows are stored in the shared operator ledger and leave the open queue."
        badge={`${formatNumber(rows.length)} open`}
      />

      <div className="mt-3 grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {summaryRows.map((row) => (
          <div key={stringValue(row.action_bucket) ?? "bucket"} className={`rounded-md border p-3 ${actionBucketClass(stringValue(row.action_bucket))}`}>
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.action_bucket_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.buyers))} open · {formatCurrency(numberValue(row.net_revenue_per_buyer))} each
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.closed_buyers))} closed · {formatNumber(numberValue(row.reviewed_buyers))} reviewed
                </div>
              </div>
              <div className="text-right text-sm font-semibold text-[#2d2b28]">
                {formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
              </div>
            </div>
          </div>
        ))}
      </div>

      {topValueRows.length ? (
        <div className="mt-3 rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
          <div className="flex flex-col gap-1 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h3 className="text-sm font-semibold">Top 10 Highest-Value Gaps</h3>
              <p className="mt-1 text-xs text-[#66635f]">Open gaps only, sorted by absolute net value regardless of bucket.</p>
            </div>
            <span className="w-fit rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold text-[#66635f]">
              click through
            </span>
          </div>
          <div className="mt-3 grid gap-2 lg:grid-cols-2">
            {topValueRows.map((row, index) => (
              <a
                key={`${stringValue(row.contact_sk) ?? "top-gap"}-${index}`}
                href={customerHref(row, filters)}
                className="grid gap-2 rounded-md border border-[#ece9e1] bg-white p-2 text-xs hover:border-[#99f6e4] hover:bg-[#f0fdfa] sm:grid-cols-[minmax(0,1fr)_7rem] sm:items-center"
              >
                <span className="min-w-0">
                  <span className="block truncate font-semibold text-[#0f766e]">
                    {stringValue(row.customer_display_name)}
                  </span>
                  <span className="mt-0.5 block truncate text-[11px] text-[#66635f]">
                    {stringValue(row.action_bucket_label)} · {stringValue(row.action_reason)}
                  </span>
                </span>
                <span className="font-semibold text-[#2d2b28] sm:text-right">
                  {formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
                </span>
              </a>
            ))}
          </div>
        </div>
      ) : null}

      <div className="mt-3 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Customer</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Bucket</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Reason</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Status</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Product</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Credit</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Net</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${stringValue(row.contact_sk) ?? "buyer"}-${index}`}>
                <td className="max-w-52 border-b border-[#ece9e1] px-2 py-2 pl-0">
                  <a href={customerHref(row, filters)} className="block truncate font-semibold text-[#0f766e] hover:text-[#115e59]">
                    {stringValue(row.customer_display_name)}
                  </a>
                  <span className="block truncate text-[11px] text-[#66635f]">
                    {[stringValue(row.email_norm), stringValue(row.phone)].filter(Boolean).join(" · ") || stringValue(row.first_purchase_label)}
                  </span>
                </td>
                <td className="max-w-40 border-b border-[#ece9e1] px-2 py-2">
                  <span className={`inline-flex max-w-full rounded-md border px-2 py-1 text-[11px] font-semibold ${actionBucketPillClass(stringValue(row.action_bucket))}`}>
                    <span className="truncate">{stringValue(row.action_bucket_label)}</span>
                  </span>
                  <span className="mt-1 block truncate text-[11px] text-[#66635f]">{stringValue(row.revenue_next_action)}</span>
                </td>
                <td className="max-w-52 border-b border-[#ece9e1] px-2 py-2">
                  <span className="block truncate font-medium text-[#2d2b28]">{stringValue(row.action_reason)}</span>
                  <span className="block truncate text-[11px] text-[#66635f]">{labelize(stringValue(row.revenue_funnel_quality_flag))}</span>
                </td>
                <td className="max-w-36 border-b border-[#ece9e1] px-2 py-2">
                  <span className={`inline-flex max-w-full rounded-md border px-2 py-1 text-[11px] font-semibold ${reviewStatusPillClass(stringValue(row.review_status))}`}>
                    <span className="truncate">{reviewStatusLabel(stringValue(row.review_status))}</span>
                  </span>
                  <span className="mt-1 block truncate text-[11px] text-[#66635f]">
                    {stringValue(row.reviewed_label) ?? "No review row"}
                  </span>
                  <OperatorActionReviewButtons
                    endpoint="/api/revenue/action-reviews"
                    contactSk={stringValue(row.contact_sk)}
                    actionBucket={stringValue(row.action_bucket)}
                    fixedNote="Closed from Revenue Action Queue."
                    ignoreNote="Marked wont_fix from Revenue Action Queue."
                  />
                </td>
                <td className="max-w-44 border-b border-[#ece9e1] px-2 py-2">
                  <span className="block truncate">{stringValue(row.top_product_family)}</span>
                  <span className="block truncate text-[11px] text-[#66635f]">{stringValue(row.top_product_by_net_revenue)}</span>
                </td>
                <td className="max-w-40 border-b border-[#ece9e1] px-2 py-2">
                  <span className="block truncate">{stringValue(row.credited_closer_name)}</span>
                  <span className="block truncate text-[11px] text-[#66635f]">{stringValue(row.credited_closer_confidence)}</span>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 font-semibold">
                  {formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length ? null : (
        <div className="mt-3 rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-3 text-xs font-medium text-[#166534]">
          No revenue worklist rows in this time range.
        </div>
      )}
    </section>
  );
}

function PaymentPlanPanel({ rows }: { rows: DashboardRow[] }) {
  const maxRevenue = Math.max(...rows.map((row) => numberValue(row.total_net_revenue_after_refunds) ?? 0), 1);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Payment Plan Reality"
        helper="Inferred from multiple payments, Fanbasis auto-renew, and plan-like product names."
        badge="cash shape"
      />
      <div className="mt-4 space-y-3">
        {rows.map((row) => {
          const revenue = numberValue(row.total_net_revenue_after_refunds);
          const width = Math.max(4, ((revenue ?? 0) / maxRevenue) * 100);

          return (
            <div key={stringValue(row.payment_plan_status) ?? "plan"} className="grid gap-2 md:grid-cols-[minmax(10rem,0.9fr)_minmax(10rem,1fr)_6rem_6rem] md:items-center">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.payment_plan_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.buyers))} buyers
                </div>
              </div>
              <div>
                <div className="h-2 rounded-sm bg-[#ece9e1]">
                  <div className="h-2 rounded-sm bg-[#0f766e]" style={{ width: `${width}%` }} />
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatCurrency(revenue)} collected net
                </div>
              </div>
              <MetricCell label="per buyer" value={formatCurrency(numberValue(row.revenue_per_buyer))} />
              <MetricCell label="booked" value={formatPercent(numberValue(row.booking_before_purchase_rate))} />
            </div>
          );
        })}
      </div>
    </section>
  );
}

function FanbasisTruthPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Payment Source Truth"
        helper="This separates collected cash from subscription schedule truth that is not landed yet."
        badge="source gap"
      />
      <div className="mt-3 space-y-2">
        {rows.map((row) => {
          const status = stringValue(row.payment_plan_truth_status);
          const isGap = status === "fanbasis_auto_renew_cash_only" || status === "name_inferred_plan_cash_only";

          return (
            <div key={status ?? "truth"} className="rounded-md border border-[#ece9e1] p-3">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#2d2b28]">
                    {stringValue(row.truth_label)}
                  </div>
                  <div className="mt-1 text-[11px] text-[#66635f]">
                    {isGap ? "Cash is real; receivables schedule is not landed" : "Cash evidence is transaction-grain"}
                  </div>
                </div>
                <MetricCell label="buyers" value={formatNumber(numberValue(row.buyers))} />
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                <SignalBox
                  label="net"
                  value={formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
                  helper="Collected net"
                  tone="green"
                />
                <SignalBox
                  label="auto-renew"
                  value={formatNumber(numberValue(row.fanbasis_auto_renew_payments))}
                  helper="Payment rows"
                  tone={isGap ? "amber" : "green"}
                />
                <SignalBox
                  label="unreleased"
                  value={formatNumber(numberValue(row.fanbasis_unreleased_payments))}
                  helper="Payout flag false"
                  tone={numberValue(row.fanbasis_unreleased_payments) ? "amber" : "green"}
                />
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function ProductFamilyPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 7);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="What People Bought"
        helper="Invoice-backed Stripe products are recovered; no-invoice direct charges stay grouped."
        badge="offer mix"
      />
      <div className="mt-3 space-y-2">
        {visibleRows.map((row, index) => (
          <details key={`${stringValue(row.top_product_family)}-${index}`} className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] open:bg-white">
            <summary className="grid cursor-pointer gap-2 px-3 py-2 text-sm md:grid-cols-[minmax(10rem,1fr)_7rem_7rem_5rem] md:items-center">
              <span className="min-w-0">
                <span className="block truncate font-semibold text-[#2d2b28]">
                  {stringValue(row.top_product_family)}
                </span>
                <span className="block truncate text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.paid_payments))} paid payments
                </span>
              </span>
              <MetricCell label="buyers" value={formatNumber(numberValue(row.buyers))} />
              <MetricCell label="net" value={formatCurrency(numberValue(row.total_net_revenue_after_refunds))} />
              <MetricCell label="plans" value={formatPercent(numberValue(row.payment_plan_buyer_rate))} />
            </summary>
            <div className="grid gap-2 border-t border-[#ece9e1] px-3 py-3 text-xs sm:grid-cols-3">
              <SignalBox
                label="per buyer"
                value={formatCurrency(numberValue(row.revenue_per_buyer))}
                helper="Collected net / buyers"
                tone="green"
              />
              <SignalBox
                label="stripe-first"
                value={formatNumber(numberValue(row.first_purchase_stripe_buyers))}
                helper="First purchase was Stripe"
                tone="amber"
              />
              <SignalBox
                label="fanbasis seen"
                value={formatNumber(numberValue(row.has_fanbasis_buyers))}
                helper="Buyer has Fanbasis payment"
                tone="green"
              />
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}

function FunnelPathPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Path Before Purchase"
        helper="Highest-known pre-purchase signal for each buyer."
        badge="buyer path"
      />
      <div className="mt-3 space-y-2">
        {rows.map((row) => (
          <div key={stringValue(row.pre_purchase_funnel_path) ?? "path"} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.path_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.buyers))} buyers · {formatPercent(numberValue(row.buyer_share))}
                </div>
              </div>
              <div className="text-right">
                <div className="text-sm font-semibold">{formatCurrency(numberValue(row.total_net_revenue_after_refunds))}</div>
                <div className="text-[11px] text-[#66635f]">{formatHours(numberValue(row.median_hours_booking_to_purchase))}</div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function TopMagnetsPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 8);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Latest Magnet Before Money"
        helper="This shares the attribution language with the Lead Magnets chapter."
        badge="money path"
      />
      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Magnet</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Buyers</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Net</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Booked</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => (
              <tr key={`${stringValue(row.lead_magnet_name)}-${index}`}>
                <td className="max-w-44 border-b border-[#ece9e1] px-2 py-2 pl-0">
                  <span className="block truncate font-medium text-[#2d2b28]">{stringValue(row.lead_magnet_name)}</span>
                  <span className="block truncate text-[11px] text-[#66635f]">{stringValue(row.offer_type)}</span>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.buyers))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2 font-semibold">{formatCurrency(numberValue(row.total_net_revenue_after_refunds))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatPercent(numberValue(row.booking_before_purchase_rate))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function RevenueCreditPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Revenue Credit</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Best-known person or account tied to each buyer before money came in. Low-confidence and unassigned rows still need source cleanup.
          </p>
        </div>
        <DollarSign className="h-4 w-4 shrink-0 text-[#0f766e]" aria-hidden />
      </div>
      <div className="mt-3 space-y-2">
        {rows.map((row, index) => (
          <div key={`${stringValue(row.revenue_credit_source)}-${index}`} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.revenue_credit_name)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {stringValue(row.revenue_credit_source_label)} · {stringValue(row.revenue_credit_confidence)}
                </div>
                {stringValue(row.revenue_credit_review_note) ? (
                  <div className="mt-1 text-[11px] text-[#8a6a20]">
                    {stringValue(row.revenue_credit_review_note)}
                  </div>
                ) : null}
              </div>
              <div className="flex shrink-0 gap-4">
                <MetricCell label="buyers" value={formatNumber(numberValue(row.buyers))} />
                <MetricCell label="net" value={formatCurrency(numberValue(row.total_net_revenue_after_refunds))} />
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function QualityPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Data Honesty"
        helper="Quality buckets keep the uncomfortable rows visible."
        badge="audit"
      />
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        {rows.map((row) => (
          <SignalBox
            key={stringValue(row.revenue_funnel_quality_flag) ?? "quality"}
            label={stringValue(row.quality_label) ?? "Quality"}
            value={formatNumber(numberValue(row.buyers))}
            helper={formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
            tone={stringValue(row.revenue_funnel_quality_flag) === "clean" ? "green" : "amber"}
          />
        ))}
      </div>
    </section>
  );
}

function AuditDetails({ data }: { data: DashboardData }) {
  return (
    <details className="mt-2 rounded-lg border border-[#dedbd2] bg-white shadow-sm">
      <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">
        Source Table And Contract
      </summary>
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

function PanelHeader({ title, helper, badge }: { title: string; helper: string; badge: string }) {
  return (
    <div className="flex items-start justify-between gap-3">
      <div>
        <h2 className="text-sm font-semibold">{title}</h2>
        <p className="mt-1 text-xs text-[#66635f]">{helper}</p>
      </div>
      <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-xs font-semibold text-[#166534]">
        {badge}
      </span>
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
      <div className="mt-1 text-base font-semibold tracking-normal text-[#2d2b28]">{value}</div>
      <div className="truncate text-[11px] text-[#66635f]">{helper}</div>
    </div>
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

function customerHref(row: DashboardRow, filters: DashboardFilters) {
  const contactSk = stringValue(row.contact_sk);
  if (!contactSk) return `/revenue?range=${filters.timeRange}`;

  const params = new URLSearchParams({
    from: "revenue",
    range: filters.timeRange,
  });

  return `/customers/${contactSk}?${params.toString()}`;
}

function actionBucketClass(bucket: string | null) {
  if (bucket === "data_risk") return "border-[#fecaca] bg-[#fef2f2]";
  if (bucket === "product_cleanup") return "border-[#fde68a] bg-[#fffbeb]";
  if (bucket === "attribution_gap") return "border-[#bfdbfe] bg-[#eff6ff]";
  if (bucket === "payment_plan_review") return "border-[#bbf7d0] bg-[#f0fdf4]";
  return "border-[#dedbd2] bg-[#fbfaf7]";
}

function actionBucketPillClass(bucket: string | null) {
  if (bucket === "data_risk") return "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]";
  if (bucket === "product_cleanup") return "border-[#fde68a] bg-[#fffbeb] text-[#92400e]";
  if (bucket === "attribution_gap") return "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]";
  if (bucket === "payment_plan_review") return "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]";
  return "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]";
}

function reviewStatusPillClass(status: string | null) {
  if (status === "fixed") return "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]";
  if (status === "wont_fix") return "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]";
  if (status === "reviewed") return "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]";
  return "border-[#fde68a] bg-[#fffbeb] text-[#92400e]";
}

function reviewStatusLabel(status: string | null) {
  if (status === "wont_fix") return "Won't fix";
  if (status === "fixed") return "Fixed";
  if (status === "reviewed") return "Reviewed";
  return "Open";
}

function labelize(value: string | null) {
  if (!value) return "N/A";
  return value
    .replaceAll("_", " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatNumber(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatCurrency(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(value);
}

function formatDecimal(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  }).format(value);
}

function formatHours(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  if (Math.abs(value) === 1) return "1 hour";
  if (Math.abs(value) < 24) return `${formatNumber(value)} hours`;
  const days = value / 24;
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(days)} days`;
}

function formatDateTime(value: string | null) {
  if (!value) return "N/A";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function numberValue(value: DashboardRowValue | undefined) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }
  return null;
}

function stringValue(value: DashboardRowValue | undefined) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  return null;
}
