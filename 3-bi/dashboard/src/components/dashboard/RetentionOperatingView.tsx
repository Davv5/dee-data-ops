import type { ComponentType, ReactNode } from "react";
import { AlertTriangle, ChevronRight, CreditCard, ListChecks, Repeat2, ShieldCheck, TrendingUp, Users } from "lucide-react";
import { ContractTermsReviewForm } from "@/components/dashboard/ContractTermsReviewForm";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { OperatorActionReviewButtons } from "@/components/dashboard/OperatorActionReviewButtons";
import type { DashboardData, DashboardFilters, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

type KpiCard = {
  title: string;
  value: string;
  helper: string;
  tone: "green" | "blue" | "amber" | "neutral";
  icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
  href?: string;
  ariaLabel?: string;
};

const toneClasses = {
  green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
  amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  neutral: "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]",
};

export function RetentionOperatingView({ data }: { data: DashboardData }) {
  const summary = data.rows.retention_summary?.[0];
  const states = data.rows.retention_states ?? [];
  const paymentHealth = data.rows.retention_payment_health ?? [];
  const repeatTypes = data.rows.retention_repeat_payment_types ?? [];
  const collections = data.rows.retention_collections ?? [];
  const contractEvidence = data.rows.retention_contract_evidence ?? [];
  const worklist = data.rows.retention_worklist ?? [];
  const cohorts = data.rows.retention_cohorts ?? [];
  const products = data.rows.retention_product_families ?? [];
  const magnets = data.rows.retention_magnets ?? [];
  const operators = data.rows.retention_operators ?? [];
  const lifecycle = data.rows.retention_lifecycle ?? [];
  const quality = data.rows.retention_quality ?? [];

  const customers = numberValue(summary?.customers);
  const repeatCustomers = numberValue(summary?.repeat_paid_customers);
  const repeatRate = numberValue(summary?.repeat_paid_customer_rate);
  const repeatNet = numberValue(summary?.repeat_paid_net_revenue);
  const eligibleCustomers = numberValue(summary?.repeat_payment_eligible_customers);
  const expectedDue = numberValue(summary?.expected_payment_due_customers);
  const expectedMissed = numberValue(summary?.expected_payment_missed_customers);
  const failedPlans = numberValue(summary?.failed_plan_recovery_customers);
  const activeDue = numberValue(summary?.active_plan_due_customers);
  const postFirstCollected = numberValue(summary?.post_first_collected_net_revenue);
  const manualCollectionCustomers = numberValue(summary?.manual_collection_customers);
  const manualCollectionReview = numberValue(summary?.manual_collection_review_customers);
  const recoveryQueue = (failedPlans ?? 0) + (activeDue ?? 0);

  const kpis: KpiCard[] = [
    {
      title: "Customers",
      value: formatNumber(customers),
      helper: `${formatCurrency(numberValue(summary?.customer_ltv))} LTV per customer`,
      tone: "blue",
      icon: Users,
    },
    {
      title: "Repeat Paid",
      value: formatPercent(repeatRate),
      helper: `${formatNumber(repeatCustomers)} customers paid again`,
      tone: "green",
      icon: Repeat2,
      href: retentionHref(data.filters, "repeat_payment_observed"),
      ariaLabel: "Open repeat paid customer worklist",
    },
    {
      title: "After First Pay",
      value: formatCurrency(postFirstCollected),
      helper: `${formatNumber(manualCollectionCustomers)} manual collection customers`,
      tone: "green",
      icon: TrendingUp,
    },
    {
      title: "Eligible Now",
      value: formatNumber(eligibleCustomers),
      helper: `${formatNumber(expectedDue)} due · ${formatNumber(expectedMissed)} missed`,
      tone: "blue",
      icon: CreditCard,
      href: retentionHref(data.filters, "active_plan_due_no_payment_yet"),
      ariaLabel: "Open active plan due worklist",
    },
    {
      title: "Recovery Queue",
      value: formatNumber(recoveryQueue),
      helper: `${formatNumber(failedPlans)} failed · ${formatNumber(activeDue)} due/no pay`,
      tone: recoveryQueue ? "amber" : "green",
      icon: AlertTriangle,
      href: retentionHref(data.filters, "recovery_queue"),
      ariaLabel: "Open recovery queue worklist",
    },
    {
      title: "Collection Review",
      value: formatNumber(manualCollectionReview),
      helper: `${formatCurrency(repeatNet)} repeat net collected`,
      tone: manualCollectionReview ? "amber" : "green",
      icon: ShieldCheck,
      href: retentionHref(data.filters, "manual_collections"),
      ariaLabel: "Open manual collections worklist",
    },
  ];

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">dbt-mart</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">Retention</h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            Who kept paying, what brought them in, what they bought, and where Fanbasis source truth still needs a cleaner receipt.
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
          <WorklistPanel rows={worklist} filters={data.filters} />
        </div>

        <div className="mt-3">
          <CollectionsPanel rows={collections} filters={data.filters} />
        </div>

        <div className="mt-3">
          <ContractEvidencePanel rows={contractEvidence} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.15fr)_minmax(22rem,0.85fr)]">
          <PaymentHealthPanel rows={paymentHealth} filters={data.filters} />
          <RepeatTypePanel rows={repeatTypes} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.15fr)_minmax(22rem,0.85fr)]">
          <CohortPanel rows={cohorts} />
          <StatePanel rows={states} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
          <ProductPanel rows={products} />
          <LifecyclePanel rows={lifecycle} />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.05fr)_minmax(22rem,0.95fr)]">
          <MagnetPanel rows={magnets} />
          <OperatorPanel rows={operators} />
        </div>

        <div className="mt-3">
          <QualityPanel rows={quality} />
        </div>
      </section>

      <AuditDetails data={data} />
    </div>
  );
}

function WorklistPanel({ rows, filters }: { rows: DashboardRow[]; filters: DashboardFilters }) {
  const activeWorklist = filters.worklist ?? "recovery_queue";
  const activeLabel = filters.worklistLabel ?? "Recovery";
  const activeDescription =
    filters.worklistDescription ?? "Failed plans plus active plans that are due with no observed payment.";

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="flex items-start gap-3">
          <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] p-2 text-[#1d4ed8]">
            <ListChecks className="h-5 w-5" aria-hidden />
          </span>
          <div>
            <div className="flex flex-wrap items-center gap-2">
              <h2 className="text-sm font-semibold">Retention Worklist</h2>
              <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-[11px] font-semibold uppercase text-[#166534]">
                {formatNumber(rows.length)} open
              </span>
            </div>
            <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
              {activeLabel}: {activeDescription}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap gap-1.5 lg:max-w-xl lg:justify-end">
          {(filters.worklistOptions ?? []).map((option) => {
            const isActive = option.value === activeWorklist;

            return (
              <a
                key={option.value}
                href={retentionHref(filters, option.value)}
                aria-current={isActive ? "page" : undefined}
                className={`rounded-md border px-2.5 py-1.5 text-[11px] font-semibold transition ${
                  isActive
                    ? "border-[#0f766e] bg-[#0f766e] text-white"
                    : "border-[#dedbd2] text-[#66635f] hover:bg-[#f3f1ea] hover:text-[#2d2b28]"
                }`}
              >
                {option.label}
              </a>
            );
          })}
        </div>
      </div>

      <div className="mt-3 space-y-2">
        {rows.length ? (
          rows.map((row, index) => (
            <CustomerWorklistRow key={`${stringValue(row.contact_sk) ?? "customer"}-${index}`} row={row} filters={filters} />
          ))
        ) : (
          <div className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-4 text-sm font-medium text-[#166534]">
            No customers currently match this worklist in the selected time range.
          </div>
        )}
      </div>
    </section>
  );
}

function CustomerWorklistRow({ row, filters }: { row: DashboardRow; filters: DashboardFilters }) {
  const missed = booleanValue(row.is_expected_payment_missed_now);
  const due = booleanValue(row.is_expected_payment_due_now);
  const isContractReview = stringValue(row.action_bucket) === "contract_terms_review";
  const contactLine = [stringValue(row.email_norm), stringValue(row.phone)].filter(Boolean).join(" · ");
  const dueLabel = formatDueStatus(row);
  const revenueCredit = knownOperatorName(row.credited_closer_name) ?? "Unassigned / unknown";
  const revenueCreditHelper = attributionHelper(row.credited_closer_source, row.credited_closer_confidence, "No revenue credit source");
  const href = customerHref(row, filters);

  return (
    <details className="group rounded-md border border-[#ece9e1] bg-[#fbfaf7]">
      <summary className="cursor-pointer list-none p-3 [&::-webkit-details-marker]:hidden">
        <div className="grid gap-3 lg:grid-cols-[minmax(14rem,1fr)_minmax(10rem,0.75fr)_7rem_8rem_1.5rem] lg:items-center">
          <div className="min-w-0">
            <a href={href} className="block truncate text-sm font-semibold text-[#0f766e] hover:text-[#115e59]">
              {stringValue(row.customer_display_name)}
            </a>
            <div className="mt-1 truncate text-[11px] text-[#66635f]">{contactLine || "No email or phone on row"}</div>
          </div>
          <div className="min-w-0">
            <div className="truncate text-xs font-semibold text-[#0f766e]">{stringValue(row.next_action_label)}</div>
            <div className="mt-1 truncate text-[11px] text-[#66635f]">{stringValue(row.health_label)}</div>
          </div>
          <MetricCell label="due" value={dueLabel} />
          <MetricCell label="net" value={formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))} />
          <ChevronRight className="h-4 w-4 text-[#66635f] transition group-open:rotate-90" aria-hidden />
        </div>
      </summary>
      <div className="border-t border-[#ece9e1] p-3">
        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
          <SignalBox
            label={isContractReview ? "largest said" : missed ? "past due" : due ? "due now" : "next due"}
            value={isContractReview ? formatCurrency(numberValue(row.largest_mentioned_payment_amount)) : dueLabel}
            helper={
              isContractReview
                ? stringValue(row.mentioned_payment_amounts_text) ?? "No amount extracted"
                : stringValue(row.expected_next_payment_label) ?? "No expected payment date"
            }
            tone={missed || due ? "amber" : "green"}
          />
          <SignalBox
            label="payments"
            value={formatNumber(numberValue(row.lifetime_paid_payments_count))}
            helper={`${formatNumber(numberValue(row.lifetime_fanbasis_payments_count))} Fanbasis · ${formatNumber(numberValue(row.lifetime_stripe_payments_count))} Stripe`}
            tone="green"
          />
          <SignalBox
            label="latest"
            value={stringValue(row.latest_purchase_label) ?? "N/A"}
            helper={stringValue(row.latest_purchase_product) ?? "No latest product"}
            tone="green"
          />
          <SignalBox
            label="revenue credit"
            value={revenueCredit}
            helper={revenueCreditHelper}
            tone={knownOperatorName(row.credited_closer_name) ? "green" : "amber"}
          />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-3">
          <SourceBlock title="Purchase Source">
            <SourceLine label="First purchase" value={stringValue(row.first_purchase_label)} />
            <SourceLine label="Top product" value={stringValue(row.top_product_by_net_revenue)} />
            <SourceLine label="Product family" value={stringValue(row.top_product_family)} />
            <SourceLine label="Lead magnet" value={stringValue(row.latest_prior_lead_magnet_name)} />
          </SourceBlock>
          <SourceBlock title="Fanbasis Evidence">
            <SourceLine label="Lifecycle" value={stringValue(row.lifecycle_label)} />
            <SourceLine label="Subscription" value={stringValue(row.latest_fanbasis_subscription_status)} />
            <SourceLine label="Frequency" value={formatDays(numberValue(row.latest_fanbasis_payment_frequency_days))} />
            <SourceLine label="Subscriber IDs" value={stringValue(row.fanbasis_subscription_ids)} />
          </SourceBlock>
          <SourceBlock title="Collection Evidence">
            <SourceLine label="Status" value={stringValue(row.collection_health_label)} />
            <SourceLine label="After first pay" value={formatCurrency(numberValue(row.post_first_collected_net_revenue))} />
            <SourceLine label="Later payments" value={formatNumber(numberValue(row.post_first_paid_payments_count))} />
            <SourceLine label="Latest collection" value={stringValue(row.latest_collection_booking_name)} />
          </SourceBlock>
          <SourceBlock title="Contract Terms">
            <SourceLine label="Status" value={labelize(stringValue(row.contract_evidence_status))} />
            <SourceLine label="Confirmed promised" value={formatCurrency(numberValue(row.confirmed_promised_contract_value))} />
            <SourceLine label="Confirmed balance" value={formatCurrency(numberValue(row.confirmed_balance_expected_amount))} />
            <SourceLine label="Confidence" value={labelize(stringValue(row.confirmed_review_confidence))} />
            <SourceLine label="Calls checked" value={formatNumber(numberValue(row.candidate_sales_calls_count))} />
            <SourceLine label="Snippets" value={formatNumber(numberValue(row.payment_terms_snippets_count))} />
            <SourceLine label="Amounts said" value={stringValue(row.mentioned_payment_amounts_text)} />
            <EvidenceText value={stringValue(row.payment_terms_evidence_text)} />
          </SourceBlock>
          <SourceBlock title="Attribution And Quality">
            <SourceLine label="Revenue credit" value={knownOperatorName(row.credited_closer_name) ?? "Unassigned / unknown"} />
            <SourceLine label="Credit source" value={attributionHelper(row.credited_closer_source, row.credited_closer_confidence, "No revenue credit source")} />
            <SourceLine label="Setter" value={knownOperatorName(row.credited_setter_name) ?? "Unassigned / unknown"} />
            <SourceLine label="Setter source" value={stringValue(row.credited_setter_source) ?? "No setter source"} />
            <SourceLine label="Current owner" value="Unknown" />
            <SourceLine label="Owner source" value="Not modeled yet" />
            <SourceLine label="Retention quality" value={stringValue(row.retention_quality_flag)} />
            <SourceLine label="Revenue quality" value={stringValue(row.revenue_funnel_quality_flag)} />
            <SourceLine label="Review status" value={reviewStatusLabel(stringValue(row.review_status))} />
          </SourceBlock>
        </div>
        <div className="mt-3 flex flex-wrap items-center gap-2">
          <a
            href={href}
            className="inline-flex rounded-md border border-[#0f766e] px-3 py-1.5 text-xs font-semibold text-[#0f766e] hover:bg-[#f0fdfa]"
          >
            Open Customer 360
          </a>
          {isContractReview ? (
            <ContractTermsReviewForm
              contactSk={stringValue(row.contact_sk)}
              evidenceText={stringValue(row.payment_terms_evidence_text)}
              mentionedAmountsText={stringValue(row.mentioned_payment_amounts_text)}
              largestMentionedAmount={row.largest_mentioned_payment_amount}
            />
          ) : (
            <OperatorActionReviewButtons
              endpoint="/api/retention/action-reviews"
              contactSk={stringValue(row.contact_sk)}
              actionBucket={stringValue(row.action_bucket) ?? stringValue(row.payment_plan_health_status)}
              fixedNote="Closed from Retention Worklist."
              ignoreNote="Marked wont_fix from Retention Worklist."
            />
          )}
        </div>
      </div>
    </details>
  );
}

function CollectionsPanel({ rows, filters }: { rows: DashboardRow[]; filters: DashboardFilters }) {
  const visibleRows = rows.filter((row) => stringValue(row.collection_health_status) !== "no_collection_signal");

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Manual Collections"
        helper="Separates original close from the follow-up motion that collects the rest of a manual plan."
        badge="collections"
      />
      <div className="mt-3 grid gap-2 lg:grid-cols-3">
        {visibleRows.slice(0, 6).map((row) => {
          const status = stringValue(row.collection_health_status);
          const href = status && [
            "manual_collection_stale_review",
            "collection_call_no_payment_review",
          ].includes(status)
            ? retentionHref(filters, status)
            : retentionHref(filters, "manual_collections");
          const attention = [
            "manual_collection_stale_review",
            "collection_call_no_payment_review",
            "plan_named_collection_review",
            "repeat_or_upsell_review",
          ].includes(status ?? "");

          return (
            <a
              key={`${status}-${stringValue(row.collection_motion_type)}`}
              href={href}
              className="block rounded-md border border-[#ece9e1] p-3 transition hover:border-[#99f6e4] hover:bg-[#f8fffd]"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#2d2b28]">
                    {stringValue(row.collection_health_label)}
                  </div>
                  <div className="mt-1 truncate text-[11px] text-[#66635f]">
                    {stringValue(row.collection_motion_label)}
                  </div>
                </div>
                <MetricCell label="customers" value={formatNumber(numberValue(row.customers))} />
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-2">
                <SignalBox
                  label="after first"
                  value={formatCurrency(numberValue(row.post_first_collected_net_revenue))}
                  helper={`${formatPercent(numberValue(row.post_first_collected_net_revenue_share))} of net`}
                  tone={attention ? "amber" : "green"}
                />
                <SignalBox
                  label="calls"
                  value={formatNumber(numberValue(row.collection_booking_customers))}
                  helper={`${formatNumber(numberValue(row.post_first_paid_customers))} paid again`}
                  tone={numberValue(row.collection_booking_customers) ? "green" : "amber"}
                />
              </div>
            </a>
          );
        })}
      </div>
    </section>
  );
}

function ContractEvidencePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Collected Value By Product"
        helper="Payment rows show cash collected. Transcript evidence shows where the call mentioned payment terms; it is not balance-owed truth."
        badge="cash + call"
      />
      <div className="mt-3 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[10px] uppercase text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2">Product</th>
              <th className="border-b border-[#dedbd2] px-2 py-2">Status</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right">Customers</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right">Collected</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right">After First</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right">Transcript</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right">Confirmed</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${stringValue(row.top_product_by_net_revenue)}-${stringValue(row.collection_health_status)}`}>
                <td className="max-w-[22rem] border-b border-[#ece9e1] px-2 py-2">
                  <div className="truncate font-semibold text-[#2d2b28]" title={stringValue(row.top_product_by_net_revenue) ?? undefined}>
                    {stringValue(row.top_product_by_net_revenue)}
                  </div>
                  <div className="truncate text-[11px] text-[#66635f]">
                    {stringValue(row.top_product_family)}
                  </div>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-[#66635f]">
                  {stringValue(row.collection_health_label)}
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-right font-semibold">
                  {formatNumber(numberValue(row.customers))}
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-right font-semibold">
                  {formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))}
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-right">
                  <div className="font-semibold text-[#2d2b28]">
                    {formatCurrency(numberValue(row.post_first_collected_net_revenue))}
                  </div>
                  <div className="text-[11px] text-[#66635f]">
                    {formatPercent(numberValue(row.post_first_collected_net_revenue_share))}
                  </div>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-right">
                  <div className="font-semibold text-[#2d2b28]">
                    {formatNumber(numberValue(row.transcript_evidence_customers))}
                  </div>
                  <div className="text-[11px] text-[#66635f]">
                    {formatNumber(numberValue(row.sales_call_no_terms_customers))} no terms
                  </div>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2 text-right">
                  <div className="font-semibold text-[#2d2b28]">
                    {formatNumber(numberValue(row.confirmed_terms_customers))}
                  </div>
                  <div className="text-[11px] text-[#66635f]">
                    {formatCurrency(numberValue(row.confirmed_balance_expected_amount))} balance
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function PaymentHealthPanel({ rows, filters }: { rows: DashboardRow[]; filters: DashboardFilters }) {
  const actionableRows = rows.filter((row) => {
    const status = stringValue(row.payment_plan_health_status);
    return status !== "no_repeat_expected_yet";
  });
  const activeWorklist = filters.worklist ?? "recovery_queue";

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Payment Plan Health"
        helper="This is the operator queue: recover, collect, watch, upsell, repair, or monitor."
        badge="next action"
      />
      <div className="mt-3 space-y-2">
        {actionableRows.map((row) => {
          const status = stringValue(row.payment_plan_health_status);
          const href = retentionHref(filters, status ?? "recovery_queue");
          const isSelected = isHealthSelected(status, activeWorklist);
          const isAttention = [
            "failed_plan_recovery_needed",
            "active_plan_due_no_payment_yet",
            "historical_stripe_product_review",
            "review_negative_value",
          ].includes(status ?? "");

          return (
            <a
              key={status ?? "health"}
              href={href}
              aria-current={isSelected ? "page" : undefined}
              className={`block rounded-md border p-3 transition ${
                isSelected
                  ? "border-[#0f766e] bg-[#f0fdfa]"
                  : "border-[#ece9e1] hover:border-[#99f6e4] hover:bg-[#f8fffd]"
              }`}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#2d2b28]">
                    {stringValue(row.health_label)}
                  </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {stringValue(row.next_action_label)}
                </div>
                {numberValue(row.closed_customers) ? (
                  <div className="mt-1 text-[11px] text-[#66635f]">
                    {formatNumber(numberValue(row.closed_customers))} closed in operator ledger
                  </div>
                ) : null}
              </div>
                <MetricCell label="customers" value={formatNumber(numberValue(row.customers))} />
              </div>
              <div className="mt-3 grid gap-2 sm:grid-cols-3">
                <SignalBox
                  label="net"
                  value={formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))}
                  helper="Lifetime net"
                  tone={isAttention ? "amber" : "green"}
                />
                <SignalBox
                  label="due"
                  value={formatNumber(numberValue(row.expected_payment_due_customers))}
                  helper={`${formatNumber(numberValue(row.expected_payment_missed_customers))} missed`}
                  tone={numberValue(row.expected_payment_due_customers) ? "amber" : "green"}
                />
                <SignalBox
                  label="ltv"
                  value={formatCurrency(numberValue(row.customer_ltv))}
                  helper="Per customer"
                  tone="green"
                />
              </div>
            </a>
          );
        })}
      </div>
    </section>
  );
}

function RepeatTypePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Repeat Payment Type"
        helper="Splits true repeat/upsell from installment and auto-renew cash."
        badge="definition"
      />
      <div className="mt-3 space-y-2">
        {rows.slice(0, 7).map((row) => (
          <div key={stringValue(row.repeat_payment_type) ?? "repeat-type"} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.repeat_type_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.repeat_payment_eligible_customers))} eligible · {formatNumber(numberValue(row.expected_payment_due_customers))} due
                </div>
              </div>
              <MetricCell label="customers" value={formatNumber(numberValue(row.customers))} />
            </div>
            <div className="mt-2 text-[11px] text-[#66635f]">
              {formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))} lifetime net · {formatCurrency(numberValue(row.customer_ltv))} LTV
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
              href={retentionHref(filters, filters.worklist ?? "recovery_queue", option.value)}
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

  const card = (
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

  if (!kpi.href) return card;

  return (
    <a href={kpi.href} aria-label={kpi.ariaLabel ?? kpi.title} className="block transition hover:-translate-y-0.5">
      {card}
    </a>
  );
}

function CohortPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Recent Cohorts" helper="Each cohort is grouped by first purchase month." badge="cohort" />
      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Cohort</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Customers</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Repeat</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">LTV</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Active</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={stringValue(row.cohort_label) ?? "cohort"}>
                <td className="border-b border-[#ece9e1] px-2 py-2 pl-0 font-semibold text-[#2d2b28]">{stringValue(row.cohort_label)}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.customers))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatPercent(numberValue(row.repeat_paid_customer_rate))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatCurrency(numberValue(row.customer_ltv))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.active_fanbasis_subscription_customers))}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function StatePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Customer-Month States" helper="Month-specific activity labels, not vague churn claims." badge="state" />
      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        {rows.map((row) => (
          <SignalBox
            key={stringValue(row.retention_state) ?? "state"}
            label={stringValue(row.retention_state_label) ?? "State"}
            value={formatNumber(numberValue(row.customer_months))}
            helper={`${formatNumber(numberValue(row.customers))} customers · ${formatCurrency(numberValue(row.net_revenue_after_refunds))}`}
            tone={stringValue(row.retention_state) === "repeat_paid_month" ? "green" : "amber"}
          />
        ))}
      </div>
    </section>
  );
}

function ProductPanel({ rows }: { rows: DashboardRow[] }) {
  const maxRevenue = Math.max(...rows.map((row) => numberValue(row.lifetime_net_revenue_after_refunds) ?? 0), 1);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Retention By Product" helper="Which products created repeat cash and higher customer value." badge="product" />
      <div className="mt-4 space-y-3">
        {rows.map((row) => {
          const revenue = numberValue(row.lifetime_net_revenue_after_refunds);
          const width = Math.max(4, ((revenue ?? 0) / maxRevenue) * 100);

          return (
            <div key={stringValue(row.top_product_family) ?? "product"} className="grid gap-2 md:grid-cols-[minmax(10rem,0.9fr)_minmax(10rem,1fr)_6rem_6rem] md:items-center">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">{stringValue(row.top_product_family)}</div>
                <div className="mt-1 text-[11px] text-[#66635f]">{formatNumber(numberValue(row.customers))} customers</div>
              </div>
              <div>
                <div className="h-2 rounded-sm bg-[#ece9e1]">
                  <div className="h-2 rounded-sm bg-[#0f766e]" style={{ width: `${width}%` }} />
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">{formatCurrency(revenue)} lifetime net</div>
              </div>
              <MetricCell label="repeat" value={formatPercent(numberValue(row.repeat_paid_customer_rate))} />
              <MetricCell label="ltv" value={formatCurrency(numberValue(row.customer_ltv))} />
            </div>
          );
        })}
      </div>
    </section>
  );
}

function LifecyclePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Fanbasis Lifecycle" helper="Subscriber evidence next to collected-cash reality." badge="source truth" />
      <div className="mt-3 space-y-2">
        {rows.map((row) => (
          <div key={stringValue(row.customer_lifecycle_status) ?? "lifecycle"} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">{stringValue(row.lifecycle_label)}</div>
                <div className="mt-1 text-[11px] text-[#66635f]">{formatNumber(numberValue(row.customers))} customers</div>
              </div>
              <MetricCell label="repeat" value={formatPercent(numberValue(row.repeat_paid_customer_rate))} />
            </div>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              <SignalBox label="lifetime net" value={formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))} helper="Collected net" tone="green" />
              <SignalBox label="ltv" value={formatCurrency(numberValue(row.customer_ltv))} helper="Per customer" tone="green" />
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function MagnetPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Retention By Magnet" helper="Latest known magnet before first purchase, then customer value after." badge="magnet" />
      <RetentionTable
        rows={rows.slice(0, 10)}
        nameKey="lead_magnet_name"
        subKey="offer_type"
      />
    </section>
  );
}

function OperatorPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Retention By Revenue Credit" helper="Not commission logic; this shows which credited paths produced better customers." badge="operator" />
      <RetentionTable
        rows={rows.slice(0, 10)}
        nameKey="operator_name"
        subKey="operator_confidence"
      />
    </section>
  );
}

function RetentionTable({
  rows,
  nameKey,
  subKey,
}: {
  rows: DashboardRow[];
  nameKey: string;
  subKey: string;
}) {
  return (
    <div className="mt-4 overflow-x-auto">
      <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
        <thead>
          <tr className="text-[#66635f]">
            <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Name</th>
            <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Customers</th>
            <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Repeat</th>
            <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">LTV</th>
            <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Net</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${stringValue(row[nameKey])}-${index}`}>
              <td className="max-w-48 border-b border-[#ece9e1] px-2 py-2 pl-0">
                <span className="block truncate font-medium text-[#2d2b28]">{stringValue(row[nameKey])}</span>
                <span className="block truncate text-[11px] text-[#66635f]">{stringValue(row[subKey])}</span>
              </td>
              <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.customers))}</td>
              <td className="border-b border-[#ece9e1] px-2 py-2">{formatPercent(numberValue(row.repeat_paid_customer_rate))}</td>
              <td className="border-b border-[#ece9e1] px-2 py-2">{formatCurrency(numberValue(row.customer_ltv))}</td>
              <td className="border-b border-[#ece9e1] px-2 py-2 font-semibold">{formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function QualityPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title="Data Honesty" helper="Rows that should shape the next source-layer fixes." badge="audit" />
      <div className="mt-3 grid gap-2 sm:grid-cols-2 xl:grid-cols-4">
        {rows.map((row) => (
          <SignalBox
            key={stringValue(row.retention_quality_flag) ?? "quality"}
            label={stringValue(row.quality_label) ?? "Quality"}
            value={formatNumber(numberValue(row.customers))}
            helper={formatCurrency(numberValue(row.lifetime_net_revenue_after_refunds))}
            tone={stringValue(row.retention_quality_flag) === "clean" ? "green" : "amber"}
          />
        ))}
      </div>
    </section>
  );
}

function SourceBlock({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="rounded-md border border-[#ece9e1] bg-white p-3">
      <div className="text-[11px] font-semibold uppercase text-[#66635f]">{title}</div>
      <div className="mt-2 space-y-1.5">{children}</div>
    </div>
  );
}

function SourceLine({ label, value }: { label: string; value: string | null }) {
  if (!value) return null;

  return (
    <div className="grid gap-2 text-xs sm:grid-cols-[7rem_minmax(0,1fr)]">
      <span className="text-[#66635f]">{label}</span>
      <span className="min-w-0 truncate font-medium text-[#2d2b28]" title={value}>
        {value}
      </span>
    </div>
  );
}

function EvidenceText({ value }: { value: string | null }) {
  if (!value) return null;

  return (
    <div className="mt-2 max-h-24 overflow-auto whitespace-pre-wrap rounded-md border border-[#dedbd2] bg-[#fbfaf7] p-2 text-[11px] leading-5 text-[#3b3936]">
      {value}
    </div>
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
      <div className="mt-1 truncate text-base font-semibold tracking-normal text-[#2d2b28]">{value}</div>
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

function retentionHref(filters: DashboardFilters, worklist: string, timeRange = filters.timeRange) {
  const params = new URLSearchParams({
    range: timeRange,
    worklist,
  });

  return `/retention?${params.toString()}`;
}

function customerHref(row: DashboardRow, filters: DashboardFilters) {
  const contactSk = stringValue(row.contact_sk);
  if (!contactSk) return retentionHref(filters, filters.worklist ?? "recovery_queue");

  const params = new URLSearchParams({
    from: "retention",
    range: filters.timeRange,
    worklist: filters.worklist ?? "recovery_queue",
  });

  return `/customers/${contactSk}?${params.toString()}`;
}

function isHealthSelected(status: string | null, activeWorklist: string) {
  if (!status) return false;
  if (activeWorklist === status) return true;

  return (
    activeWorklist === "recovery_queue" &&
    ["failed_plan_recovery_needed", "active_plan_due_no_payment_yet"].includes(status)
  );
}

function formatDueStatus(row: DashboardRow) {
  const missed = booleanValue(row.is_expected_payment_missed_now);
  const due = booleanValue(row.is_expected_payment_due_now);
  const daysPast = numberValue(row.days_past_expected_payment);
  const daysUntil = numberValue(row.days_until_expected_next_payment);

  if (missed && daysPast !== null) return `${formatNumber(daysPast)}d past`;
  if (missed) return "Past due";
  if (due) return "Due now";
  if (daysUntil !== null && daysUntil >= 0) return `${formatNumber(daysUntil)}d out`;
  return "No due date";
}

function formatDays(value: number | null) {
  if (value === null) return null;
  return `${formatNumber(value)} days`;
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

function labelize(value: string | null) {
  if (!value) return null;
  return value
    .replace(/_/g, " ")
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
