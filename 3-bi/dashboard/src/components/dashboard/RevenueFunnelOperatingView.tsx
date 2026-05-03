import type { ComponentType } from "react";
import { AlertTriangle, CalendarCheck, CreditCard, DollarSign, Target, UserX } from "lucide-react";
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

export function RevenueFunnelOperatingView({ data }: { data: DashboardData }) {
  const summary = data.rows.revenue_funnel_summary?.[0];
  const paymentPlans = data.rows.revenue_funnel_payment_plans ?? [];
  const paymentTruth = data.rows.revenue_funnel_payment_truth ?? [];
  const productFamilies = data.rows.revenue_funnel_product_families ?? [];
  const paths = data.rows.revenue_funnel_paths ?? [];
  const magnets = data.rows.revenue_funnel_magnets ?? [];
  const operatorDiagnostic = data.rows.revenue_funnel_operator_diagnostic ?? [];
  const quality = data.rows.revenue_funnel_quality ?? [];

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
      title: "Rep Attribution Gap",
      value: formatPercent(unassignedRate),
      helper: "Operator source is diagnostic only",
      tone: "red",
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
          <OperatorDiagnosticPanel rows={operatorDiagnostic} />
        </div>
      </section>

      <AuditDetails data={data} />
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
        helper="Historical Stripe rows stay honestly grouped when product detail is unavailable."
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

function OperatorDiagnosticPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Rep Attribution Gap</h2>
          <p className="mt-1 text-xs text-[#66635f]">
            Useful as a source-quality read, not a revenue leaderboard.
          </p>
        </div>
        <AlertTriangle className="h-4 w-4 shrink-0 text-[#92400e]" aria-hidden />
      </div>
      <div className="mt-3 space-y-2">
        {rows.map((row, index) => (
          <div key={`${stringValue(row.best_available_operator_source)}-${index}`} className="rounded-md border border-[#ece9e1] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.best_available_operator_name)}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {stringValue(row.operator_source_label)}
                </div>
              </div>
              <MetricCell label="buyers" value={formatNumber(numberValue(row.buyers))} />
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
