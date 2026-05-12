import { AlertTriangle, CheckCircle2, Clock3, DollarSign, Target, Users } from "lucide-react";
import type { DashboardData, DashboardFilters, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

const actionStatuses = new Set([
  "repair_candidate",
  "kill_candidate",
  "retire_recommended_pending_override",
]);

type StatusTone = "green" | "amber" | "red" | "neutral";

type StatusLabel = {
  label: string;
  tone: StatusTone;
};

type LeadMagnetSourceGroup = "lead-magnets" | "sales-pipelines" | "waitlists" | "launches" | "all";

type SourceGroupCopy = {
  eyebrow: string;
  title: string;
  subtitle: string;
  summaryNoun: string;
  tableTitle: string;
  emptyWorkingText: string;
  emptyNeedsWorkText: string;
  emptyEarlyText: string;
};

type LeadMagnetSummary = {
  totalMagnets: number;
  enoughData: number;
  working: number;
  needsWork: number;
  tooEarly: number;
  reviewCarefully: number;
  bestName: string;
  bestRevenuePerLead: number | null;
  totalLeads: number | null;
  totalBookings: number | null;
  totalBuyers: number | null;
  totalRepeatBuyers: number | null;
  bookingRate: number | null;
  purchaseRate: number | null;
  repeatBuyerRate: number | null;
  avgRevenuePerLead: number | null;
  visibleRevenue: number | null;
  attributionCoverage: number | null;
};

const toneClasses: Record<StatusTone, string> = {
  green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  neutral: "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]",
};

const dotClasses: Record<StatusTone, string> = {
  green: "bg-[#16a34a]",
  amber: "bg-[#d97706]",
  red: "bg-[#dc2626]",
  neutral: "bg-[#9ca3af]",
};

const sourceGroupOptions: Array<{ value: LeadMagnetSourceGroup; label: string }> = [
  { value: "lead-magnets", label: "Lead magnets" },
  { value: "sales-pipelines", label: "Sales pipelines" },
  { value: "waitlists", label: "Waitlists" },
  { value: "launches", label: "Launches" },
  { value: "all", label: "All sources" },
];

export function LeadMagnetsWireframeView({
  data,
  sourceGroup,
}: {
  data: DashboardData;
  sourceGroup?: string | null;
}) {
  const activeSourceGroup = normalizeSourceGroup(sourceGroup);
  const copy = sourceGroupCopy(activeSourceGroup);
  const summaryRow = data.rows.lead_magnet_summary?.[0];
  const allPerformanceRows = data.rows.lead_magnet_performance_rows ?? [];
  const performanceRows = allPerformanceRows.filter((row) => rowMatchesSourceGroup(row, activeSourceGroup));
  const attributionRows = data.rows.lead_magnet_attribution_flags ?? [];
  const offerTypeRows = data.rows.lead_magnet_offer_types ?? [];
  const summary = buildLeadMagnetSummary(performanceRows, summaryRow);
  const bestRows = performanceRows
    .filter((row) => stringValue(row.queue_status) !== "insufficient_sample")
    .slice(0, 3);
  const needsWorkRows = performanceRows.filter((row) => actionStatuses.has(stringValue(row.queue_status) ?? "")).slice(0, 4);
  const earlyRows = performanceRows.filter((row) => stringValue(row.queue_status) === "insufficient_sample").slice(0, 4);

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">{copy.eyebrow}</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            {copy.title}
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            {copy.subtitle}
          </p>
        </div>
        <div className="flex flex-col gap-2 lg:items-end">
          <FreshnessLine data={data} />
          <TimeRangeControl filters={data.filters} sourceGroup={activeSourceGroup} />
        </div>
      </header>

      <SourceGroupControl
        activeGroup={activeSourceGroup}
        rows={allPerformanceRows}
        range={data.filters.timeRange}
      />

      {data.error ? (
        <div className="mt-4 rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <section className="grid gap-4 py-4 xl:grid-cols-[minmax(21rem,0.8fr)_minmax(0,1.2fr)]">
        <CurrentRead summary={summary} copy={copy} />
        <LeadMagnetMap rows={performanceRows} title={`${copy.summaryNoun} Map`} />
      </section>

      <section className="grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(20rem,0.55fr)]">
        <LeadMagnetTable rows={performanceRows} title={copy.tableTitle} />
        <aside className="grid gap-4 self-start">
          <ShortListPanel title="Best so far" rows={bestRows} emptyText={copy.emptyWorkingText} />
          <ShortListPanel title="Needs work" rows={needsWorkRows} emptyText={copy.emptyNeedsWorkText} />
          <ShortListPanel title="Too early" rows={earlyRows} emptyText={copy.emptyEarlyText} />
          <AttributionNotes rows={attributionRows} offerTypes={offerTypeRows} summary={summary} />
        </aside>
      </section>

      <details className="mt-4 rounded-lg border border-[#dedbd2] bg-white shadow-sm">
        <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">
          Source Data
        </summary>
        <div className="border-t border-[#ece9e1] p-4 text-xs leading-5 text-[#66635f]">
          {data.dataContract ? (
            <>
              <div className="font-semibold text-[#2d2b28]">
                {data.dataContract.projectId}.{data.dataContract.dataset}
              </div>
              <div className="mt-1">{data.dataContract.tables.join(", ")}</div>
              <div className="mt-2">{data.dataContract.note}</div>
            </>
          ) : (
            "No source metadata returned."
          )}
        </div>
      </details>
    </div>
  );
}

function CurrentRead({ summary, copy }: { summary: LeadMagnetSummary; copy: SourceGroupCopy }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-semibold uppercase text-[#66635f]">Current read</p>
          <h2 className="mt-2 max-w-2xl text-xl font-semibold leading-tight tracking-normal text-[#2d2b28] md:text-2xl">
            {formatNumber(summary.totalLeads)} leads became {formatNumber(summary.totalBuyers)} buyers.
          </h2>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-[#66635f]">
            This view starts with the chain we can prove today: lead captured, booked call, purchase,
            repeat buyer, and revenue. Traffic, landing-page conversion, CPL, and MQL scoring are not
            in this mart yet.
          </p>
        </div>
        <span className="hidden rounded-md border border-[#dedbd2] bg-[#f7f7f4] px-2 py-1 text-xs font-semibold text-[#3b3936] sm:inline-block">
          Draft
        </span>
      </div>

      <div className="mt-4 grid grid-cols-2 gap-2 xl:grid-cols-4">
        <BigMetric icon={Target} label="Leads" value={formatNumber(summary.totalLeads)} helper={`${formatNumber(summary.totalMagnets)} ${copy.summaryNoun.toLowerCase()}`} tone="neutral" />
        <BigMetric icon={CheckCircle2} label="Booked" value={formatNumber(summary.totalBookings)} helper={`${formatPercent(summary.bookingRate)} booking rate`} tone="green" />
        <BigMetric icon={Users} label="Buyers" value={formatNumber(summary.totalBuyers)} helper={`${formatPercent(summary.purchaseRate)} purchase rate`} tone="green" />
        <BigMetric icon={DollarSign} label="Revenue / lead" value={formatCurrency(summary.avgRevenuePerLead)} helper={`${formatCurrency(summary.visibleRevenue)} total revenue`} tone="green" />
        <BigMetric icon={Clock3} label="Too early" value={formatNumber(summary.tooEarly)} helper={`${formatNumber(summary.enoughData)} with enough data`} tone="neutral" />
        <BigMetric icon={AlertTriangle} label="Need work" value={formatNumber(summary.needsWork)} helper={`${formatNumber(summary.reviewCarefully)} need careful review`} tone={summary.needsWork > 0 ? "amber" : "green"} />
        <BigMetric icon={CheckCircle2} label="Working" value={formatNumber(summary.working)} helper={`Best: ${summary.bestName}`} tone="green" />
        <BigMetric icon={Users} label="Repeat buyers" value={formatNumber(summary.totalRepeatBuyers)} helper={`${formatPercent(summary.repeatBuyerRate)} of buyers`} tone="neutral" />
      </div>
    </section>
  );
}

function BigMetric({
  icon: Icon,
  label,
  value,
  helper,
  tone,
}: {
  icon: typeof Target;
  label: string;
  value: string;
  helper: string;
  tone: StatusTone;
}) {
  return (
    <div className={`rounded-md border p-2.5 ${toneClasses[tone]}`}>
      <div className="flex items-center justify-between gap-3">
        <span className="text-[11px] font-semibold uppercase">{label}</span>
        <Icon className="h-4 w-4" aria-hidden />
      </div>
      <div className="mt-2 text-lg font-semibold tracking-normal text-[#2d2b28]">{value}</div>
      <div className="mt-1 truncate text-[11px]">{helper}</div>
    </div>
  );
}

function LeadMagnetMap({ rows, title }: { rows: DashboardRow[]; title: string }) {
  const plotted = rows
    .map((row) => ({
      row,
      leads: numberValue(row.leads_in_90d) ?? 0,
      purchaseRate: numberValue(row.purchase_rate) ?? 0,
      revenuePerLead: numberValue(row.revenue_per_lead) ?? 0,
      label: stringValue(row.lead_magnet_reporting_name) ?? "Unknown",
      status: statusLabel(row),
    }))
    .filter((point) => point.leads > 0 || point.purchaseRate > 0);

  const maxLeads = Math.max(...plotted.map((point) => point.leads), 1);
  const maxPurchaseRate = Math.max(...plotted.map((point) => point.purchaseRate), 0.001);
  const labeled = new Set(
    [...plotted]
      .sort((a, b) => b.revenuePerLead - a.revenuePerLead)
      .slice(0, 4)
      .map((point) => point.label),
  );

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-sm font-semibold">{title}</h2>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-[#66635f]">
            Lead volume runs left to right. Buyer conversion rises bottom to top. Dot size reflects revenue per lead.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-[11px]">
          {(["Working", "Needs work", "Low performer", "Too early"] as const).map((label) => {
            const tone = label === "Working" ? "green" : label === "Too early" ? "neutral" : label === "Low performer" ? "red" : "amber";
            return (
              <span key={label} className="flex items-center gap-1.5 rounded-md border border-[#ece9e1] bg-[#fbfaf7] px-2 py-1 text-[#3b3936]">
                <span className={`h-2 w-2 rounded-full ${dotClasses[tone]}`} />
                {label}
              </span>
            );
          })}
        </div>
      </div>

      <div className="mt-4 overflow-hidden rounded-md border border-[#ece9e1] bg-[#fbfaf7]">
        <svg viewBox="0 0 760 330" role="img" aria-label="Lead magnet conversion map" className="h-[22rem] w-full">
          <rect x="0" y="0" width="760" height="330" fill="#fbfaf7" />
          <line x1="58" x2="724" y1="276" y2="276" stroke="#d8d4ca" />
          <line x1="58" x2="58" y1="32" y2="276" stroke="#d8d4ca" />
          <text x="58" y="306" fill="#66635f" fontSize="12">fewer leads</text>
          <text x="650" y="306" fill="#66635f" fontSize="12">more leads</text>
          <text x="14" y="44" fill="#66635f" fontSize="12" transform="rotate(-90 14 44)">higher purchase rate</text>
          {[0.25, 0.5, 0.75].map((tick) => (
            <line
              key={tick}
              x1="58"
              x2="724"
              y1={276 - tick * 244}
              y2={276 - tick * 244}
              stroke="#ece9e1"
            />
          ))}
          {plotted.map((point, index) => {
            const x = 58 + (Math.log1p(point.leads) / Math.log1p(maxLeads)) * 666;
            const y = 276 - (point.purchaseRate / maxPurchaseRate) * 244;
            const radius = Math.max(5, Math.min(18, 5 + Math.sqrt(point.revenuePerLead)));
            const shouldLabel = labeled.has(point.label);

            return (
              <g key={`${point.label}-${index}`}>
                <circle
                  cx={x}
                  cy={y}
                  r={radius}
                  fill={dotColor(point.status.tone)}
                  fillOpacity={point.status.label === "Too early" ? 0.35 : 0.78}
                  stroke="#ffffff"
                  strokeWidth="2"
                />
                {shouldLabel ? (
                  <text x={Math.min(x + radius + 6, 620)} y={Math.max(y - 4, 20)} fill="#2d2b28" fontSize="11" fontWeight="600">
                    {truncateLabel(point.label)}
                  </text>
                ) : null}
              </g>
            );
          })}
        </svg>
      </div>
    </section>
  );
}

function LeadMagnetTable({ rows, title }: { rows: DashboardRow[]; title: string }) {
  const visibleRows = rows.slice(0, 18);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-sm font-semibold">{title}</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
            Ranked by revenue per lead. The status labels come from the mart; the page does not recalculate thresholds.
          </p>
        </div>
        <span className="rounded-md border border-[#dedbd2] bg-[#f7f7f4] px-2 py-1 text-xs font-semibold text-[#3b3936]">
          {formatNumber(rows.length)} magnets
        </span>
      </div>

      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] px-2 py-2 pl-0 font-semibold">Lead magnet</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 font-semibold">Type</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Leads</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Buyers</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Purchase</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Rev / lead</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Booked</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Wk 4</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Repeat</th>
              <th className="border-b border-[#dedbd2] px-2 py-2 text-right font-semibold">Status</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.map((row, index) => {
              const status = statusLabel(row);
              return (
                <tr key={`${stringValue(row.lead_magnet_id) ?? "row"}-${index}`} className={status.label === "Too early" ? "text-[#74706a]" : undefined}>
                  <td className="max-w-64 border-b border-[#ece9e1] px-2 py-2 pl-0">
                    <span className="block truncate font-semibold text-[#2d2b28]">
                      {stringValue(row.lead_magnet_reporting_name) ?? "Unknown"}
                    </span>
                    <span className="block truncate text-[10px] text-[#66635f]">
                      {categoryLabel(row.lead_magnet_category)}
                    </span>
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2">
                    <span className="block max-w-32 truncate">{stringValue(row.offer_type_label) ?? "Unknown"}</span>
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatNumber(numberValue(row.leads_in_90d))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatNumber(numberValue(row.buyer_count))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatPercent(numberValue(row.purchase_rate))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right font-semibold tabular-nums text-[#2d2b28]">
                    {formatCurrency(numberValue(row.revenue_per_lead))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatPercent(numberValue(row.booking_rate))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatPercent(numberValue(row.engagement_rate_week_4))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right tabular-nums">
                    {formatPercent(numberValue(row.repeat_buyer_rate))}
                  </td>
                  <td className="border-b border-[#ece9e1] px-2 py-2 text-right">
                    <span className={`inline-flex rounded-md border px-2 py-0.5 text-[10px] font-semibold ${toneClasses[status.tone]}`}>
                      {status.label}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function ShortListPanel({
  title,
  rows,
  emptyText,
}: {
  title: string;
  rows: DashboardRow[];
  emptyText: string;
}) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <h2 className="text-sm font-semibold">{title}</h2>
      {rows.length === 0 ? (
        <p className="mt-3 text-xs text-[#66635f]">{emptyText}</p>
      ) : (
        <div className="mt-3 space-y-3">
          {rows.map((row, index) => {
            const status = statusLabel(row);
            return (
              <div key={`${title}-${stringValue(row.lead_magnet_id) ?? index}`} className="border-b border-[#ece9e1] pb-3 last:border-b-0 last:pb-0">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-[#2d2b28]">
                      {stringValue(row.lead_magnet_reporting_name) ?? "Unknown"}
                    </div>
                    <div className="mt-1 truncate text-[11px] text-[#66635f]">
                      {stringValue(row.offer_type_label) ?? "Unknown"} · {formatNumber(numberValue(row.leads_in_90d))} leads
                    </div>
                  </div>
                  <span className={`shrink-0 rounded-md border px-2 py-0.5 text-[10px] font-semibold ${toneClasses[status.tone]}`}>
                    {status.label}
                  </span>
                </div>
                <div className="mt-2 grid grid-cols-3 gap-2 text-[11px]">
                  <MiniMetric label="Rev / lead" value={formatCurrency(numberValue(row.revenue_per_lead))} />
                  <MiniMetric label="Purchase" value={formatPercent(numberValue(row.purchase_rate))} />
                  <MiniMetric label="Booked" value={formatPercent(numberValue(row.booking_rate))} />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}

function AttributionNotes({
  rows,
  offerTypes,
  summary,
}: {
  rows: DashboardRow[];
  offerTypes: DashboardRow[];
  summary: LeadMagnetSummary;
}) {
  const priorMagnet = rows.find((row) => stringValue(row.attribution_flag) === "latest_prior_magnet");
  const purchaseBefore = rows.find((row) => stringValue(row.attribution_flag) === "purchase_before_first_magnet");
  const topOfferType = offerTypes[0];

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <h2 className="text-sm font-semibold">Attribution Notes</h2>
      <div className="mt-3 space-y-3 text-xs leading-5 text-[#66635f]">
        <FactLine
          label="Prior magnet coverage"
          value={formatPercent(numberValue(priorMagnet?.buyer_share) ?? summary.attributionCoverage)}
        />
        <FactLine
          label="Bought before any known magnet"
          value={formatNumber(numberValue(purchaseBefore?.buyers))}
        />
        <FactLine
          label="Largest buyer source"
          value={stringValue(topOfferType?.offer_type_label) ?? "N/A"}
        />
        <div className="rounded-md border border-[#fde68a] bg-[#fffbeb] p-3 text-[#92400e]">
          <div className="font-semibold text-[#2d2b28]">Not in this mart yet</div>
          <div className="mt-1">
            Traffic, landing-page conversion, form starts, CPL, paid spend, MQL score, SQL rate, and pipeline dollars.
          </div>
        </div>
        <p>
          This view gives latest-known prior magnet credit. It is useful for direction, but it is not proof that the
          asset alone caused the purchase.
        </p>
      </div>
    </section>
  );
}

function MiniMetric({ label, value }: { label: string; value: string }) {
  return (
    <span>
      <span className="block text-[10px] font-semibold uppercase text-[#66635f]">{label}</span>
      <span className="block truncate font-semibold text-[#2d2b28]">{value}</span>
    </span>
  );
}

function FactLine({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between gap-3 border-b border-[#ece9e1] pb-2 last:border-b-0">
      <span>{label}</span>
      <span className="font-semibold text-[#2d2b28]">{value}</span>
    </div>
  );
}

function TimeRangeControl({
  filters,
  sourceGroup,
}: {
  filters: DashboardFilters;
  sourceGroup: LeadMagnetSourceGroup;
}) {
  return (
    <div className="w-fit max-w-full">
      <div className="flex w-fit max-w-full rounded-lg border border-[#dedbd2] bg-white p-1 shadow-sm">
        {filters.timeRangeOptions.map((option) => {
          const isActive = option.value === filters.timeRange;

          return (
            <a
              key={option.value}
              href={wireframeHref(option.value, sourceGroup)}
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

function SourceGroupControl({
  activeGroup,
  rows,
  range,
}: {
  activeGroup: LeadMagnetSourceGroup;
  rows: DashboardRow[];
  range: string;
}) {
  return (
    <div className="border-b border-[#dedbd2] py-3">
      <div className="flex flex-wrap gap-2">
        {sourceGroupOptions.map((option) => {
          const isActive = option.value === activeGroup;
          const count = rows.filter((row) => rowMatchesSourceGroup(row, option.value)).length;

          return (
            <a
              key={option.value}
              href={wireframeHref(range, option.value)}
              aria-current={isActive ? "page" : undefined}
              className={`rounded-md border px-3 py-2 text-xs font-semibold transition ${
                isActive
                  ? "border-[#0f766e] bg-[#0f766e] text-white"
                  : "border-[#dedbd2] bg-white text-[#3b3936] hover:bg-[#f3f1ea]"
              }`}
            >
              {option.label}
              <span className={isActive ? "ml-2 text-white/80" : "ml-2 text-[#66635f]"}>
                {formatNumber(count)}
              </span>
            </a>
          );
        })}
      </div>
      <p className="mt-2 max-w-3xl text-xs leading-5 text-[#66635f]">
        Default view is true lead magnets only. Sales pipelines, waitlists, and launches stay available as separate source groups.
      </p>
    </div>
  );
}

function FreshnessLine({ data }: { data: DashboardData }) {
  const tone =
    data.freshness.status === "live"
      ? "text-[#0f766e]"
      : data.freshness.status === "stale"
        ? "text-[#92400e]"
        : "text-[#991b1b]";

  return (
    <div className={`text-xs font-medium ${tone}`}>
      {data.freshness.label} · {data.freshness.detail}
    </div>
  );
}

function buildLeadMagnetSummary(rows: DashboardRow[], summaryRow: DashboardRow | undefined): LeadMagnetSummary {
  const best = rows.reduce<DashboardRow | null>((current, row) => {
    const currentRpl = numberValue(current?.revenue_per_lead);
    const rowRpl = numberValue(row.revenue_per_lead);
    if (rowRpl === null) return current;
    if (currentRpl === null || rowRpl > currentRpl) return row;
    return current;
  }, null);
  const totalLeads = sumMetric(rows, "leads_in_90d");
  const totalBookings = sumMetric(rows, "booking_lead_count");
  const totalBuyers = sumMetric(rows, "buyer_count");
  const totalRepeatBuyers = sumMetric(rows, "repeat_buyer_count");
  const visibleRevenue = sumMetric(rows, "net_revenue_after_refunds");

  return {
    totalMagnets: rows.length,
    enoughData: rows.filter((row) => stringValue(row.queue_status) !== "insufficient_sample").length,
    working: rows.filter((row) => stringValue(row.queue_status) === "healthy").length,
    needsWork: rows.filter((row) => actionStatuses.has(stringValue(row.queue_status) ?? "")).length,
    tooEarly: rows.filter((row) => stringValue(row.queue_status) === "insufficient_sample").length,
    reviewCarefully: rows.filter((row) => stringValue(row.queue_status) === "retire_recommended_pending_override").length,
    bestName: stringValue(best?.lead_magnet_reporting_name) ?? "N/A",
    bestRevenuePerLead: numberValue(best?.revenue_per_lead),
    totalLeads,
    totalBookings,
    totalBuyers,
    totalRepeatBuyers,
    bookingRate: safeDivide(totalBookings, totalLeads),
    purchaseRate: safeDivide(totalBuyers, totalLeads),
    repeatBuyerRate: safeDivide(totalRepeatBuyers, totalBuyers),
    avgRevenuePerLead: safeDivide(visibleRevenue, totalLeads),
    visibleRevenue,
    attributionCoverage: numberValue(summaryRow?.latest_prior_magnet_buyer_coverage),
  };
}

function normalizeSourceGroup(value: string | null | undefined): LeadMagnetSourceGroup {
  const normalized = value?.toLowerCase();
  return sourceGroupOptions.some((option) => option.value === normalized)
    ? (normalized as LeadMagnetSourceGroup)
    : "lead-magnets";
}

function rowMatchesSourceGroup(row: DashboardRow, sourceGroup: LeadMagnetSourceGroup) {
  if (sourceGroup === "all") return true;
  if (sourceGroup === "lead-magnets") return row.is_true_lead_magnet === true;
  if (sourceGroup === "sales-pipelines") {
    return row.is_sales_pipeline === true || stringValue(row.lead_magnet_category) === "sales_operating_pipeline";
  }
  if (sourceGroup === "waitlists") {
    return row.is_waitlist === true || stringValue(row.lead_magnet_category) === "waitlist";
  }
  return row.is_launch === true || stringValue(row.lead_magnet_category) === "launch_event";
}

function sourceGroupCopy(sourceGroup: LeadMagnetSourceGroup): SourceGroupCopy {
  switch (sourceGroup) {
    case "sales-pipelines":
      return {
        eyebrow: "Sales pipeline analytics",
        title: "Sales Pipelines",
        subtitle: "Which booked-call and sales pipeline sources are creating buyers, revenue, and repeat customers?",
        summaryNoun: "Sales Pipelines",
        tableTitle: "Sales Pipeline Performance",
        emptyWorkingText: "No sales pipelines have enough positive signal yet.",
        emptyNeedsWorkText: "No sales pipelines currently need work.",
        emptyEarlyText: "No early sales pipelines returned.",
      };
    case "waitlists":
      return {
        eyebrow: "Waitlist analytics",
        title: "Waitlists",
        subtitle: "Which waitlists are collecting demand, and which ones have enough data to judge?",
        summaryNoun: "Waitlists",
        tableTitle: "Waitlist Performance",
        emptyWorkingText: "No waitlists have enough positive signal yet.",
        emptyNeedsWorkText: "No waitlists currently need work.",
        emptyEarlyText: "No early waitlists returned.",
      };
    case "launches":
      return {
        eyebrow: "Launch analytics",
        title: "Launches",
        subtitle: "Which launch and event funnels are creating buyers, revenue, and repeat customers?",
        summaryNoun: "Launches",
        tableTitle: "Launch Performance",
        emptyWorkingText: "No launches have enough positive signal yet.",
        emptyNeedsWorkText: "No launches currently need work.",
        emptyEarlyText: "No early launches returned.",
      };
    case "all":
      return {
        eyebrow: "Source analytics",
        title: "All Lead Sources",
        subtitle: "A combined view of lead magnets, sales pipelines, waitlists, and launches.",
        summaryNoun: "Sources",
        tableTitle: "Source Performance",
        emptyWorkingText: "No sources have enough positive signal yet.",
        emptyNeedsWorkText: "No sources currently need work.",
        emptyEarlyText: "No early sources returned.",
      };
    default:
      return {
        eyebrow: "Lead magnet analytics",
        title: "Lead Magnets",
        subtitle: "Are the assets that collect leads turning into buyers, revenue, and repeat customers?",
        summaryNoun: "Lead Magnets",
        tableTitle: "Lead Magnet Performance",
        emptyWorkingText: "No lead magnets have enough positive signal yet.",
        emptyNeedsWorkText: "No lead magnets currently need work.",
        emptyEarlyText: "No early lead magnets returned.",
      };
  }
}

function wireframeHref(range: string, sourceGroup: LeadMagnetSourceGroup) {
  const params = new URLSearchParams();
  params.set("range", range);
  if (sourceGroup !== "lead-magnets") params.set("group", sourceGroup);
  return `/lead-magnets-wireframe?${params.toString()}`;
}

function statusLabel(row: DashboardRow): StatusLabel {
  const status = stringValue(row.queue_status);

  switch (status) {
    case "healthy":
      return { label: "Working", tone: "green" };
    case "repair_candidate":
      return { label: "Needs work", tone: "amber" };
    case "kill_candidate":
      return { label: "Low performer", tone: "red" };
    case "retire_recommended_pending_override":
      return { label: "Review carefully", tone: "red" };
    case "insufficient_sample":
      return { label: "Too early", tone: "neutral" };
    default:
      return { label: status ?? "Unknown", tone: "neutral" };
  }
}

function categoryLabel(value: DashboardRowValue | undefined) {
  const raw = stringValue(value);
  switch (raw) {
    case "true_lead_magnet":
      return "Lead magnet";
    case "sales_operating_pipeline":
      return "Sales pipeline";
    case "launch_event":
      return "Launch";
    case "waitlist":
      return "Waitlist";
    case "uncategorized":
      return "Uncategorized";
    default:
      return raw ? raw.replace(/_/g, " ") : "Unknown";
  }
}

function dotColor(tone: StatusTone) {
  if (tone === "green") return "#16a34a";
  if (tone === "amber") return "#d97706";
  if (tone === "red") return "#dc2626";
  return "#9ca3af";
}

function truncateLabel(value: string) {
  return value.length > 24 ? `${value.slice(0, 22)}...` : value;
}

function sumMetric(rows: DashboardRow[], key: string) {
  const values = rows.map((row) => numberValue(row[key])).filter((value): value is number => value !== null);
  if (values.length === 0) return null;
  return values.reduce((sum, value) => sum + value, 0);
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
