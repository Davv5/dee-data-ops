import type { ComponentType } from "react";
import { AlertTriangle, CalendarCheck, CreditCard, DollarSign, Layers, Target } from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
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

const ACTION_QUEUE_STATUSES = new Set([
  "repair_candidate",
  "kill_candidate",
  "retire_recommended_pending_override",
]);

export function LeadMagnetsOperatingView({ data }: { data: DashboardData }) {
  const summary = data.rows.lead_magnet_summary?.[0];
  const topMagnets = data.rows.lead_magnet_top_magnets ?? [];
  const offerTypes = data.rows.lead_magnet_offer_types ?? [];
  const attributionFlags = data.rows.lead_magnet_attribution_flags ?? [];
  const opportunityOfferTypes = data.rows.lead_magnet_opportunity_offer_types ?? [];
  const pipelineActivity = data.rows.lead_magnet_pipeline_activity ?? [];
  const queueDistribution = data.rows.lead_magnet_queue_distribution ?? [];
  const reviewQueue = data.rows.lead_magnet_review_queue ?? [];
  const performanceRows = data.rows.lead_magnet_performance_rows ?? [];
  const decisionSummary = buildDecisionSummary(performanceRows);
  const segmentRows = buildSegmentRows(performanceRows);

  const buyers = numberValue(summary?.buyers);
  const firstPurchaseRevenue = numberValue(summary?.first_purchase_net_revenue);
  const totalNetRevenue = numberValue(summary?.total_net_revenue_after_refunds);
  const attributionCoverage = numberValue(summary?.latest_prior_magnet_buyer_coverage);
  const revenueCoverage = numberValue(summary?.latest_prior_magnet_revenue_coverage);
  const noPriorBuyers =
    (numberValue(summary?.buyers_purchase_before_first_magnet) ?? 0) +
    (numberValue(summary?.buyers_no_known_magnet) ?? 0);

  const kpis: KpiCard[] = [
    {
      title: "Matched Buyers",
      value: formatNumber(buyers),
      helper: `${formatNumber(numberValue(summary?.paid_payments))} paid payments`,
      tone: "green",
      icon: Target,
    },
    {
      title: "First Purchase",
      value: formatCurrency(firstPurchaseRevenue),
      helper: `${formatCurrency(numberValue(summary?.avg_first_purchase_revenue))} avg first buy`,
      tone: "blue",
      icon: DollarSign,
    },
    {
      title: "Collected Net",
      value: formatCurrency(totalNetRevenue),
      helper: `${formatCurrency(numberValue(summary?.revenue_per_buyer))} per buyer`,
      tone: "green",
      icon: DollarSign,
    },
    {
      title: "Payment-Plan Buyers",
      value: formatPercent(numberValue(summary?.multi_payment_buyer_rate)),
      helper: `${formatNumber(numberValue(summary?.multi_payment_buyers))} buyers paid more than once`,
      tone: "amber",
      icon: CreditCard,
    },
    {
      title: "Booked Before Buying",
      value: formatPercent(numberValue(summary?.booking_before_purchase_rate)),
      helper: `${formatNumber(numberValue(summary?.buyers_with_booking_before_purchase))} buyers had a booking first`,
      tone: "blue",
      icon: CalendarCheck,
    },
    {
      title: "Attribution Coverage",
      value: formatPercent(attributionCoverage),
      helper: `${formatPercent(revenueCoverage)} of collected net has prior magnet context`,
      tone: "neutral",
      icon: Layers,
    },
  ];

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">dbt-mart</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            Lead Magnets
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            Which raised hands became buyers, which offers show up before money, and where the data is asking us to be honest.
          </p>
        </div>
        <div className="flex flex-col gap-2 md:items-end">
          <span className="rounded-md border border-[#99d4cb] bg-[#f0fdfa] px-2.5 py-1 text-xs font-semibold text-[#115e59]">
            Lead Magnet v2 live
          </span>
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
        <DecisionSummaryPanel summary={decisionSummary} />

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,0.75fr)_minmax(0,1.25fr)]">
          <QueueDistributionStrip rows={queueDistribution} />
          <SegmentLensPanel rows={segmentRows} />
        </div>

        <div className="mt-3">
          <ReviewQueuePanel rows={reviewQueue} />
        </div>

        <div className="mt-3">
          <OperatingLeaderboardPanel rows={performanceRows} />
        </div>

        <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
          {kpis.map((kpi) => (
            <KpiCard key={kpi.title} kpi={kpi} />
          ))}
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.15fr)_minmax(22rem,0.85fr)]">
          <OfferTypePanel rows={offerTypes} />
          <AttributionPanel
            rows={attributionFlags}
            noPriorBuyers={noPriorBuyers}
            buyers={buyers}
            revenueCoverage={revenueCoverage}
          />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.15fr)_minmax(22rem,0.85fr)]">
          <TopMagnetsPanel rows={topMagnets} />
          <ActivityPanel rows={opportunityOfferTypes} />
        </div>
      </section>

      <AuditDetails
        data={data}
        pipelineActivityRows={pipelineActivity}
      />
    </div>
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
              href={`/lead-magnets?range=${option.value}`}
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

function OfferTypePanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 7);
  const maxRevenue = Math.max(...visibleRows.map((row) => numberValue(row.total_net_revenue_after_refunds) ?? 0), 1);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Offer Types That Become Buyers</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Latest known magnet before first purchase, grouped by offer format.
          </p>
        </div>
        <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] px-2 py-1 text-xs font-semibold text-[#1d4ed8]">
          buyer truth
        </span>
      </div>

      <div className="mt-4 space-y-3">
        {visibleRows.map((row) => {
          const revenue = numberValue(row.total_net_revenue_after_refunds);
          const buyers = numberValue(row.buyers);
          const width = Math.max(4, ((revenue ?? 0) / maxRevenue) * 100);

          return (
            <div key={stringValue(row.offer_type) ?? "offer-type"} className="grid gap-2 md:grid-cols-[minmax(9rem,0.9fr)_minmax(10rem,1fr)_6rem_6rem] md:items-center">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.offer_type_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(buyers)} buyers
                </div>
              </div>
              <div>
                <div className="h-2 rounded-sm bg-[#ece9e1]">
                  <div
                    className="h-2 rounded-sm bg-[#0f766e]"
                    style={{ width: `${width}%` }}
                  />
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatCurrency(revenue)} collected net
                </div>
              </div>
              <MetricCell label="per buyer" value={formatCurrency(numberValue(row.revenue_per_buyer))} />
              <MetricCell label="paid plan" value={formatPercent(numberValue(row.multi_payment_buyer_rate))} />
            </div>
          );
        })}
      </div>
    </section>
  );
}

function AttributionPanel({
  rows,
  noPriorBuyers,
  buyers,
  revenueCoverage,
}: {
  rows: DashboardRow[];
  noPriorBuyers: number;
  buyers: number | null;
  revenueCoverage: number | null;
}) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Attribution Honesty</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            We use latest known magnet before first purchase, not magic credit.
          </p>
        </div>
        <span className="rounded-md border border-[#fde68a] bg-[#fffbeb] px-2 py-1 text-xs font-semibold text-[#92400e]">
          source-aware
        </span>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        <SignalBox
          label="covered revenue"
          value={formatPercent(revenueCoverage)}
          helper="Collected net with a prior magnet"
          tone="green"
        />
        <SignalBox
          label="no prior magnet"
          value={formatNumber(noPriorBuyers)}
          helper={`${formatPercent(safeDivide(noPriorBuyers, buyers))} of buyers`}
          tone={noPriorBuyers === 0 ? "green" : "amber"}
        />
      </div>

      <div className="mt-4 space-y-2">
        {rows.map((row) => (
          <div key={stringValue(row.attribution_flag) ?? "flag"} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.attribution_label)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(numberValue(row.buyers))} buyers · {formatPercent(numberValue(row.buyer_share))}
                </div>
              </div>
              <div className="text-right text-sm font-semibold">
                {formatCurrency(numberValue(row.total_net_revenue_after_refunds))}
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
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Top Latest Magnets Before Purchase</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Click a row for the deeper money and booking read.
          </p>
        </div>
        <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-xs font-semibold text-[#166534]">
          clickable
        </span>
      </div>

      <div className="mt-3 space-y-2">
        {visibleRows.map((row, index) => (
          <details
            key={`${stringValue(row.lead_magnet_name)}-${index}`}
            className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] open:bg-white"
          >
            <summary className="grid cursor-pointer gap-2 px-3 py-2 text-sm md:grid-cols-[minmax(10rem,1fr)_7rem_7rem_5rem] md:items-center">
              <span className="min-w-0">
                <span className="block truncate font-semibold text-[#2d2b28]">
                  {stringValue(row.lead_magnet_name)}
                </span>
                <span className="block truncate text-[11px] text-[#66635f]">
                  {stringValue(row.offer_type_label)}
                </span>
              </span>
              <MetricCell label="buyers" value={formatNumber(numberValue(row.buyers))} />
              <MetricCell label="net" value={formatCurrency(numberValue(row.total_net_revenue_after_refunds))} />
              <MetricCell label="booked" value={formatPercent(numberValue(row.booking_before_purchase_rate))} />
            </summary>
            <div className="grid gap-2 border-t border-[#ece9e1] px-3 py-3 text-xs sm:grid-cols-4">
              <SignalBox
                label="first purchase"
                value={formatCurrency(numberValue(row.first_purchase_net_revenue))}
                helper="First paid event only"
                tone="green"
              />
              <SignalBox
                label="per buyer"
                value={formatCurrency(numberValue(row.revenue_per_buyer))}
                helper="Collected net / buyers"
                tone="green"
              />
              <SignalBox
                label="payments"
                value={formatNumber(numberValue(row.paid_payments))}
                helper={`${formatDecimal(numberValue(row.payments_per_buyer))} per buyer`}
                tone="amber"
              />
              <SignalBox
                label="median lag"
                value={formatDays(numberValue(row.median_days_latest_magnet_to_purchase))}
                helper="Magnet to first buy"
                tone="amber"
              />
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}

function ActivityPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 6);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Activity vs Money</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Opportunity activity is window-attributed, so it is directional by design.
          </p>
        </div>
        <AlertTriangle className="h-4 w-4 shrink-0 text-[#92400e]" aria-hidden />
      </div>

      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Offer</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Opps</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Bookings</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Window Net</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row) => (
              <tr key={stringValue(row.offer_type) ?? "activity"}>
                <td className="max-w-40 border-b border-[#ece9e1] px-2 py-2 pl-0">
                  <span className="block truncate font-medium text-[#2d2b28]">
                    {stringValue(row.offer_type_label)}
                  </span>
                </td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.opportunities))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2">{formatNumber(numberValue(row.direct_bookings))}</td>
                <td className="border-b border-[#ece9e1] px-2 py-2 font-semibold">
                  {formatCurrency(numberValue(row.window_attributed_net_revenue))}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

type QueueStatusBadge = {
  label: string;
  short: string;
  tone: keyof typeof toneClasses;
};

function queueStatusBadgeProps(status: string | null): QueueStatusBadge {
  switch (status) {
    case "retire_recommended_pending_override":
      return { label: "Strategic review", short: "Review", tone: "red" };
    case "kill_candidate":
      return { label: "Needs proof", short: "Proof", tone: "red" };
    case "repair_candidate":
      return { label: "Needs repair", short: "Repair", tone: "amber" };
    case "healthy":
      return { label: "Healthy", short: "Healthy", tone: "green" };
    case "insufficient_sample":
      return { label: "Insufficient sample", short: "Low N", tone: "neutral" };
    default:
      return { label: status ?? "Unknown", short: status ?? "Unknown", tone: "neutral" };
  }
}

function flagSummary(row: DashboardRow): string {
  const fired: string[] = [];
  if (row.kill_candidate === true) fired.push("needs proof");
  if (row.repair_candidate === true) fired.push("needs repair");
  if (row.retire_recommended_pending_override === true) fired.push("strategic review");
  return fired.length > 0 ? `Signals: ${fired.join(", ")}` : "";
}

type DecisionSummary = {
  reviewCount: number;
  healthyCount: number;
  insufficientCount: number;
  bestMagnetName: string;
  bestRevenuePerLead: number | null;
  bestMagnetStatus: string;
};

type SegmentSummary = {
  category: string;
  label: string;
  magnetCount: number;
  reviewCount: number;
  healthyCount: number;
  insufficientCount: number;
  bestRevenuePerLead: number | null;
  bestMagnetName: string;
};

function isActionQueueStatus(status: string | null) {
  return status !== null && ACTION_QUEUE_STATUSES.has(status);
}

function categoryLabel(value: DashboardRowValue | undefined) {
  const raw = stringValue(value);
  switch (raw) {
    case "true_lead_magnet":
      return "True lead magnets";
    case "sales_operating_pipeline":
      return "Sales operating pipelines";
    case "launch_event":
      return "Launch / event funnels";
    case "waitlist":
      return "Waitlists";
    case "uncategorized":
      return "Uncategorized";
    default:
      return raw ? raw.replace(/_/g, " ") : "Unknown";
  }
}

function buildDecisionSummary(rows: DashboardRow[]): DecisionSummary {
  const best = rows.reduce<DashboardRow | null>((current, row) => {
    const currentRpl = numberValue(current?.revenue_per_lead);
    const rowRpl = numberValue(row.revenue_per_lead);
    if (rowRpl === null) return current;
    if (currentRpl === null || rowRpl > currentRpl) return row;
    return current;
  }, null);

  return {
    reviewCount: rows.filter((row) => isActionQueueStatus(stringValue(row.queue_status))).length,
    healthyCount: rows.filter((row) => stringValue(row.queue_status) === "healthy").length,
    insufficientCount: rows.filter((row) => stringValue(row.queue_status) === "insufficient_sample").length,
    bestMagnetName: stringValue(best?.lead_magnet_reporting_name) ?? "N/A",
    bestRevenuePerLead: numberValue(best?.revenue_per_lead),
    bestMagnetStatus: stringValue(best?.queue_status) ?? "unknown",
  };
}

function buildSegmentRows(rows: DashboardRow[]): SegmentSummary[] {
  const segments = new Map<string, SegmentSummary>();

  rows.forEach((row) => {
    const category = stringValue(row.lead_magnet_category) ?? "unknown";
    const status = stringValue(row.queue_status);
    const revenuePerLead = numberValue(row.revenue_per_lead);
    const existing = segments.get(category) ?? {
      category,
      label: categoryLabel(row.lead_magnet_category),
      magnetCount: 0,
      reviewCount: 0,
      healthyCount: 0,
      insufficientCount: 0,
      bestRevenuePerLead: null,
      bestMagnetName: "N/A",
    };

    existing.magnetCount += 1;
    if (isActionQueueStatus(status)) existing.reviewCount += 1;
    if (status === "healthy") existing.healthyCount += 1;
    if (status === "insufficient_sample") existing.insufficientCount += 1;
    if (revenuePerLead !== null && (existing.bestRevenuePerLead === null || revenuePerLead > existing.bestRevenuePerLead)) {
      existing.bestRevenuePerLead = revenuePerLead;
      existing.bestMagnetName = stringValue(row.lead_magnet_reporting_name) ?? "N/A";
    }

    segments.set(category, existing);
  });

  return Array.from(segments.values()).sort((a, b) => {
    if (b.reviewCount !== a.reviewCount) return b.reviewCount - a.reviewCount;
    return b.magnetCount - a.magnetCount;
  });
}

function DecisionSummaryPanel({ summary }: { summary: DecisionSummary }) {
  const bestBadge = queueStatusBadgeProps(summary.bestMagnetStatus);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-sm font-semibold">Operator Decision Snapshot</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
            Start here: review the active queue, separate low-sample tests from real decisions, then compare magnets inside their category.
          </p>
        </div>
        <span className="rounded-md border border-[#dedbd2] bg-[#f7f7f4] px-2 py-1 text-xs font-semibold text-[#3b3936]">
          RevOps readout
        </span>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <SignalBox
          label="review now"
          value={formatNumber(summary.reviewCount)}
          helper="Proof / repair / strategic-review rows"
          tone={summary.reviewCount > 0 ? "amber" : "green"}
        />
        <SignalBox
          label="double-down"
          value={formatNumber(summary.healthyCount)}
          helper="Sufficient-sample healthy rows"
          tone="green"
        />
        <SignalBox
          label="testing backlog"
          value={formatNumber(summary.insufficientCount)}
          helper="Visible, not queue-eligible yet"
          tone="amber"
        />
        <div className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-2">
          <div className="text-[10px] font-semibold uppercase text-[#66635f]">best current RPL</div>
          <div className="mt-1 text-base font-semibold tracking-normal text-[#2d2b28]">
            {formatCurrency(summary.bestRevenuePerLead)}
          </div>
          <div className="truncate text-[11px] text-[#66635f]">{summary.bestMagnetName}</div>
          <span className={`mt-2 inline-block rounded-md border px-2 py-0.5 text-[10px] font-semibold ${toneClasses[bestBadge.tone]}`}>
            {bestBadge.short}
          </span>
        </div>
      </div>
    </section>
  );
}

function QueueDistributionStrip({ rows }: { rows: DashboardRow[] }) {
  const total = rows.reduce((sum, row) => sum + (numberValue(row.magnet_count) ?? 0), 0);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Queue Status Distribution</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Magnet counts by precomputed queue status from the mart. The dashboard reads these fields directly and never recomputes thresholds.
          </p>
        </div>
        <span className="rounded-md border border-[#dedbd2] bg-[#f7f7f4] px-2 py-1 text-xs font-semibold text-[#3b3936]">
          {formatNumber(total)} magnets total
        </span>
      </div>

      {rows.length === 0 ? (
        <p className="mt-4 text-xs text-[#66635f]">
          No queue distribution rows returned from the mart.
        </p>
      ) : (
        <div className="mt-3 flex flex-wrap gap-2">
          {rows.map((row) => {
            const status = stringValue(row.queue_status);
            const props = queueStatusBadgeProps(status);
            const count = numberValue(row.magnet_count);
            const share = safeDivide(count, total);
            return (
              <div
                key={status ?? "unknown"}
                className={`min-w-32 rounded-md border px-3 py-2 ${toneClasses[props.tone]}`}
              >
                <div className="text-[10px] font-semibold uppercase">{props.label}</div>
                <div className="mt-1 text-xl font-semibold tracking-normal">
                  {formatNumber(count)}
                </div>
                <div className="text-[10px]">{formatPercent(share)} of magnets</div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function SegmentLensPanel({ rows }: { rows: SegmentSummary[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Segment Lens</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Use category context before comparing magnets. A booked-call funnel and a guide are not the same operating motion.
          </p>
        </div>
        <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] px-2 py-1 text-xs font-semibold text-[#1d4ed8]">
          compare fairly
        </span>
      </div>

      {rows.length === 0 ? (
        <p className="mt-4 text-xs text-[#66635f]">No segment rows returned.</p>
      ) : (
        <div className="mt-3 overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
            <thead>
              <tr className="text-[#66635f]">
                <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Segment</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Magnets</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Review</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Low N</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Best RPL</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.category}>
                  <td className="max-w-64 border-b border-[#ece9e1] px-2 py-2 pl-0">
                    <span className="block truncate font-semibold text-[#2d2b28]">{row.label}</span>
                    <span className="block truncate text-[10px] text-[#66635f]">Best: {row.bestMagnetName}</span>
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatNumber(row.magnetCount)}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatNumber(row.reviewCount)}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatNumber(row.insufficientCount)}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right font-semibold tabular-nums">
                    {formatCurrency(row.bestRevenuePerLead)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function ReviewQueuePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Action Queue</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Mart-classified candidates only. The operator decides whether to watch, improve, or escalate — the dashboard does not auto-act. Low-sample magnets are excluded by the mart.
          </p>
        </div>
        <span className="rounded-md border border-[#fde68a] bg-[#fffbeb] px-2 py-1 text-xs font-semibold text-[#92400e]">
          decision support
        </span>
      </div>

      {rows.length === 0 ? (
        <p className="mt-4 text-xs text-[#66635f]">
          No magnets currently meet the queue criteria. queue_status determines inclusion.
        </p>
      ) : (
        <div className="mt-4 overflow-x-auto">
          <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
            <thead>
              <tr className="text-[#66635f]">
                <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Magnet</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Queue status</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">90d leads</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Rev / lead</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Purchase</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Wk-4 engage</th>
                <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Repeat buyer</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                const status = stringValue(row.queue_status);
                const props = queueStatusBadgeProps(status);
                const isStrategicReview = status === "retire_recommended_pending_override";
                const flags = flagSummary(row);
                return (
                  <tr key={`${stringValue(row.lead_magnet_id) ?? "row"}-${index}`}>
                    <td className="max-w-60 border-b border-[#ece9e1] px-2 py-2 pl-0">
                      <span className="block truncate font-semibold text-[#2d2b28]">
                        {stringValue(row.lead_magnet_reporting_name) ?? "—"}
                      </span>
                      <span className="block truncate text-[10px] text-[#66635f]">
                        {stringValue(row.offer_type_label) ?? ""} · {categoryLabel(row.lead_magnet_category)}
                      </span>
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 align-top">
                      <span
                        className={`inline-block rounded-md border px-2 py-0.5 text-[10px] font-semibold ${toneClasses[props.tone]}`}
                      >
                        {props.label}
                      </span>
                      {isStrategicReview ? (
                        <div className="mt-1 max-w-44 text-[10px] italic text-[#66635f]">
                          Decision support — needs operator approval before any strategic change.
                        </div>
                      ) : null}
                      {flags ? (
                        <div className="mt-1 max-w-44 text-[10px] text-[#66635f]">{flags}</div>
                      ) : null}
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                      {formatNumber(numberValue(row.leads_in_90d))}
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                      {formatCurrency(numberValue(row.revenue_per_lead))}
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                      {formatPercent(numberValue(row.purchase_rate))}
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                      {formatPercent(numberValue(row.engagement_rate_week_4))}
                    </td>
                    <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                      {formatPercent(numberValue(row.repeat_buyer_rate))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function OperatingLeaderboardPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Unit-Economics Leaderboard</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            All magnets including insufficient_sample (rendered greyed). Sorted by revenue per lead descending. Click a row for funnel and engagement-decay drill-down.
          </p>
        </div>
        <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] px-2 py-1 text-xs font-semibold text-[#1d4ed8]">
          mart truth
        </span>
      </div>

      {rows.length === 0 ? (
        <p className="mt-4 text-xs text-[#66635f]">
          No magnets returned from lead_magnet_performance_summary.
        </p>
      ) : (
        <div className="mt-3 space-y-2">
          {rows.map((row, index) => {
            const status = stringValue(row.queue_status);
            const isInsufficient = status === "insufficient_sample";
            const badge = queueStatusBadgeProps(status);

            return (
              <details
                key={`${stringValue(row.lead_magnet_id) ?? "magnet"}-${index}`}
                className={`rounded-md border ${
                  isInsufficient
                    ? "border-[#ece9e1] bg-[#f7f7f4] opacity-70"
                    : "border-[#ece9e1] bg-[#fbfaf7]"
                } open:bg-white`}
              >
                <summary className="grid cursor-pointer gap-2 px-3 py-2 text-sm md:grid-cols-[minmax(10rem,1.4fr)_6rem_5rem_5rem_5rem_5rem] md:items-center">
                  <span className="min-w-0">
                    <span className="block truncate font-semibold text-[#2d2b28]">
                      {stringValue(row.lead_magnet_reporting_name) ?? "—"}
                    </span>
                    <span className="block truncate text-[11px] text-[#66635f]">
                      {stringValue(row.offer_type_label) ?? ""} · {categoryLabel(row.lead_magnet_category)}
                    </span>
                  </span>
                  <span
                    className={`inline-block w-fit rounded-md border px-2 py-0.5 text-[10px] font-semibold ${toneClasses[badge.tone]}`}
                  >
                    {badge.short}
                  </span>
                  <MetricCell label="90d leads" value={formatNumber(numberValue(row.leads_in_90d))} />
                  <MetricCell label="rev/lead" value={formatCurrency(numberValue(row.revenue_per_lead))} />
                  <MetricCell label="time-to-buy" value={formatDays(numberValue(row.time_to_buyer_days))} />
                  <MetricCell label="repeat" value={formatPercent(numberValue(row.repeat_buyer_rate))} />
                </summary>
                <div className="grid gap-3 border-t border-[#ece9e1] px-3 py-3 text-xs sm:grid-cols-2">
                  <FunnelDrilldown row={row} />
                  <DecayDrilldown row={row} />
                </div>
              </details>
            );
          })}
        </div>
      )}
    </section>
  );
}

function FunnelDrilldown({ row }: { row: DashboardRow }) {
  const activationLabel =
    stringValue(row.intermediate_activation_milestone)?.replace(/_/g, " ") ?? "Activation";
  const optInCount = numberValue(row.opt_in_lead_count);
  const steps: Array<{ label: string; count: number | null; rate: number | null }> = [
    { label: "Opt-in", count: optInCount, rate: optInCount && optInCount > 0 ? 1 : null },
    {
      label: activationLabel,
      count: numberValue(row.intermediate_activation_lead_count),
      rate: numberValue(row.intermediate_activation_rate),
    },
    {
      label: "Booking",
      count: numberValue(row.booking_lead_count),
      rate: numberValue(row.booking_rate),
    },
    {
      label: "Purchase",
      count: numberValue(row.purchase_lead_count),
      rate: numberValue(row.purchase_rate),
    },
  ];

  return (
    <div className="rounded-md border border-[#ece9e1] p-3">
      <div className="text-[10px] font-semibold uppercase text-[#66635f]">Funnel (per magnet)</div>
      <div className="mt-2 space-y-1.5">
        {steps.map((step) => (
          <div
            key={step.label}
            className="grid grid-cols-[minmax(0,1fr)_4.5rem_4.5rem] items-center gap-2"
          >
            <span className="truncate text-[11px] text-[#2d2b28]">{step.label}</span>
            <span className="text-right text-[11px] font-semibold tabular-nums">
              {formatNumber(step.count)}
            </span>
            <span className="text-right text-[11px] tabular-nums text-[#66635f]">
              {formatPercent(step.rate)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function DecayDrilldown({ row }: { row: DashboardRow }) {
  const weeks: Array<{ week: number; rate: number | null }> = [
    { week: 0, rate: numberValue(row.engagement_rate_week_0) },
    { week: 1, rate: numberValue(row.engagement_rate_week_1) },
    { week: 2, rate: numberValue(row.engagement_rate_week_2) },
    { week: 4, rate: numberValue(row.engagement_rate_week_4) },
    { week: 8, rate: numberValue(row.engagement_rate_week_8) },
  ];
  const decay = numberValue(row.engagement_decay_rate_week_0_to_4);
  const maxRate = Math.max(...weeks.map((w) => w.rate ?? 0), 0.0001);

  return (
    <div className="rounded-md border border-[#ece9e1] p-3">
      <div className="flex items-baseline justify-between gap-2">
        <div className="text-[10px] font-semibold uppercase text-[#66635f]">
          Engagement decay
        </div>
        <div className="text-[10px] text-[#66635f]">
          Week 0 → 4:{" "}
          <span className="font-semibold text-[#2d2b28]">{formatPercent(decay)}</span>
        </div>
      </div>
      <div className="mt-2 grid grid-cols-5 gap-1">
        {weeks.map((w) => {
          const pct = w.rate ?? 0;
          const heightPx = Math.max(4, Math.round((pct / maxRate) * 40));
          return (
            <div key={w.week} className="flex flex-col items-center">
              <div className="flex h-10 w-full items-end">
                <div
                  className="w-full rounded-sm bg-[#0f766e]"
                  style={{ height: `${heightPx}px` }}
                />
              </div>
              <div className="mt-1 text-[9px] text-[#66635f]">w{w.week}</div>
              <div className="text-[9px] font-semibold tabular-nums">
                {formatPercent(w.rate)}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function AuditDetails({
  data,
  pipelineActivityRows,
}: {
  data: DashboardData;
  pipelineActivityRows: DashboardRow[];
}) {
  return (
    <details className="mt-2 rounded-lg border border-[#dedbd2] bg-white shadow-sm">
      <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">
        Source Tables And Audit
      </summary>
      <div className="border-t border-[#ece9e1] p-4">
        {data.dataContract ? (
          <div className="mb-4 rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3 text-xs text-[#66635f]">
            <div className="font-semibold text-[#2d2b28]">
              {data.dataContract.projectId}.{data.dataContract.dataset}
            </div>
            <div className="mt-1">{data.dataContract.note}</div>
          </div>
        ) : null}
        <div className="grid gap-3 xl:grid-cols-2">
          <TablePanel
            title="Offer type buyer truth"
            rows={data.rows.lead_magnet_offer_types ?? []}
            columns={[
              { key: "offer_type_label", label: "Offer Type" },
              { key: "buyers", label: "Buyers", format: "number" },
              { key: "total_net_revenue_after_refunds", label: "Collected Net", format: "currency" },
              { key: "revenue_per_buyer", label: "Per Buyer", format: "currency" },
              { key: "multi_payment_buyer_rate", label: "Paid Plan", format: "percent" },
            ]}
          />
          <TablePanel
            title="Pipeline activity truth"
            rows={pipelineActivityRows}
            columns={[
              { key: "lead_magnet_name", label: "Pipeline" },
              { key: "offer_type_label", label: "Type" },
              { key: "opportunities", label: "Opps", format: "number" },
              { key: "direct_bookings", label: "Bookings", format: "number" },
              { key: "window_attributed_net_revenue", label: "Window Net", format: "currency" },
            ]}
          />
        </div>
      </div>
    </details>
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
      <div className="text-[10px] font-semibold uppercase text-[#66635f]">{label}</div>
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

function TablePanel({
  title,
  rows,
  columns,
}: {
  title: string;
  rows: DashboardRow[];
  columns: Array<{ key: string; label: string; format?: "number" | "percent" | "currency" }>;
}) {
  return (
    <section className="min-w-0 rounded-md border border-[#ece9e1] p-3">
      <h3 className="text-sm font-semibold">{title}</h3>
      <div className="mt-3 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              {columns.map((column) => (
                <th key={column.key} className="border-b border-[#dedbd2] px-2 py-2 font-semibold first:pl-0">
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, index) => (
              <tr key={`${title}-${index}`}>
                {columns.map((column) => (
                  <td key={column.key} className="max-w-48 border-b border-[#ece9e1] px-2 py-2 first:pl-0">
                    <span className="block truncate">
                      {formatValue(row[column.key], column.format)}
                    </span>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function formatValue(value: DashboardRowValue | undefined, format?: "number" | "percent" | "currency") {
  if (format === "number") return formatNumber(numberValue(value));
  if (format === "percent") return formatPercent(numberValue(value));
  if (format === "currency") return formatCurrency(numberValue(value));
  return stringValue(value) ?? "N/A";
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

function formatDays(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  if (Math.abs(value) === 1) return `${value} day`;
  return `${formatNumber(value)} days`;
}

function safeDivide(numerator: number | null, denominator: number | null) {
  if (numerator === null || denominator === null || denominator === 0) return null;
  return numerator / denominator;
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
