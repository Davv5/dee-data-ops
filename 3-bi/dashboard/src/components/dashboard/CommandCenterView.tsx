import Link from "next/link";
import {
  AlertTriangle,
  ArrowUpRight,
  DollarSign,
  Gauge,
  ListChecks,
  Repeat2,
  RotateCcw,
  ShieldCheck,
  Target,
  UserRound,
} from "lucide-react";
import { CustomerSearch } from "@/components/layout/CustomerSearch";
import type { HomeSummary, HomeTileResult } from "@/lib/bigquery/home-summary-live";
import type { DashboardRow } from "@/types/dashboard-data";

const toneClasses = {
  green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
  amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  neutral: "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]",
} as const;

type Tone = keyof typeof toneClasses;

export function CommandCenterView({ data }: { data: HomeSummary }) {
  const chips = buildAttentionChips(data);

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">command center</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            D-DEE Command Center
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            D-DEE today, area by area. One click to drill.
          </p>
        </div>
        <TimeRangeControl
          timeRange={data.filters.timeRange}
          options={data.filters.timeRangeOptions}
        />
      </header>

      {chips.length ? <NeedsReviewStrip chips={chips} /> : null}

      <section className="py-4">
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
          <SpeedToLeadTile result={data.tiles.speed_to_lead} />
          <RevenueTile
            result={data.tiles.revenue}
            homeRange={data.filters.timeRange}
            tileRange={data.tileRanges.revenue}
          />
          <RetentionTile
            result={data.tiles.retention}
            homeRange={data.filters.timeRange}
            tileRange={data.tileRanges.retention}
          />
          <ActionsTile result={data.tiles.actions} />
          <RecoveryTile result={data.tiles.recovery} />
          <LeadMagnetsTile result={data.tiles.lead_magnets} />
          <CustomerSearchTile />
          <DataHealthTile data={data} />
        </div>
      </section>
    </div>
  );
}

// ----------------------------------------------------------------------
// Time range control (server-component-friendly: link-based)
// ----------------------------------------------------------------------

function TimeRangeControl({
  timeRange,
  options,
}: {
  timeRange: string;
  options: HomeSummary["filters"]["timeRangeOptions"];
}) {
  return (
    <div className="flex items-center gap-1 rounded-md border border-[#dedbd2] bg-white p-1 text-xs">
      {options.map((option) => {
        const isActive = option.value === timeRange;
        return (
          <Link
            key={option.value}
            href={`/?range=${option.value}`}
            aria-current={isActive ? "page" : undefined}
            className={`rounded px-2 py-1 font-semibold transition ${
              isActive ? "bg-[#0f766e] text-white" : "text-[#3b3936] hover:bg-[#f3f1ea]"
            }`}
            title={option.description}
          >
            {option.label}
          </Link>
        );
      })}
    </div>
  );
}

// ----------------------------------------------------------------------
// Needs Review strip
// ----------------------------------------------------------------------

type AttentionChip = {
  tone: "red" | "amber";
  label: string;
};

function buildAttentionChips(data: HomeSummary): AttentionChip[] {
  const chips: AttentionChip[] = [];

  const erroredCount = countErrored(data);
  if (erroredCount > 0) {
    chips.push({ tone: "red", label: `${erroredCount} ${erroredCount === 1 ? "source" : "sources"} errored` });
  }

  const staleCount = countStale(data);
  if (staleCount > 0) {
    chips.push({ tone: "amber", label: `${staleCount} ${staleCount === 1 ? "source" : "sources"} stale` });
  }

  const actions = firstRow(data.tiles.actions);
  const highPriority = numberValue(actions?.high_priority_open_actions);
  if (highPriority && highPriority > 0) {
    chips.push({ tone: "red", label: `${formatNumber(highPriority)} high-priority actions` });
  }

  const recovery = firstRow(data.tiles.recovery);
  const recoveryCustomers = numberValue(recovery?.payment_recovery_customers);
  const recoveryValue = numberValue(recovery?.payment_recovery_lifetime_value);
  if (recoveryCustomers && recoveryCustomers > 0 && recoveryValue && recoveryValue > 0) {
    chips.push({ tone: "amber", label: `${formatCurrency(recoveryValue)} in payment recovery` });
  }

  const stlOverall = firstRow(data.tiles.speed_to_lead.overall);
  const sla = numberValue(stlOverall?.pct_within_sla);
  if (sla !== null && sla < 0.5) {
    chips.push({ tone: "red", label: `SLA below 50% (${formatPercent(sla)})` });
  }

  return chips;
}

function NeedsReviewStrip({ chips }: { chips: AttentionChip[] }) {
  const visible = chips.slice(0, 3);
  const overflow = chips.length - visible.length;

  return (
    <section className="mt-4 flex flex-wrap items-center gap-2 rounded-lg border border-[#fde68a] bg-[#fffbeb] px-3 py-2 text-xs text-[#92400e]">
      <span className="inline-flex items-center gap-1 font-semibold uppercase tracking-wide">
        <AlertTriangle className="h-3.5 w-3.5" aria-hidden /> Needs Review
      </span>
      {visible.map((chip) => (
        <span
          key={chip.label}
          className={`inline-flex items-center rounded-md border px-2 py-1 font-semibold ${toneClasses[chip.tone]}`}
        >
          {chip.label}
        </span>
      ))}
      {overflow > 0 ? (
        <span className="inline-flex items-center rounded-md border border-[#dedbd2] bg-white px-2 py-1 font-semibold text-[#3b3936]">
          +{overflow} more
        </span>
      ) : null}
    </section>
  );
}

// ----------------------------------------------------------------------
// Tiles
// ----------------------------------------------------------------------

function SpeedToLeadTile({
  result,
}: {
  result: HomeSummary["tiles"]["speed_to_lead"];
}) {
  if (!result.overall.ok) {
    return <TileError title="Speed-to-Lead" icon={Gauge} href="/speed-to-lead" error={result.overall.error} />;
  }

  const row = result.overall.rows[0];
  const prior = result.prior.ok ? result.prior.rows[0] : undefined;

  const sla = numberValue(row?.pct_within_sla);
  const coverage = numberValue(row?.pct_triggers_with_outbound_touch);
  const totalEvents = numberValue(row?.total_triggers_all);
  const slaPrior = numberValue(prior?.pct_within_sla);
  const coveragePrior = numberValue(prior?.pct_triggers_with_outbound_touch);

  return (
    <TileShell title="Speed-to-Lead" icon={Gauge} href="/speed-to-lead" eyebrow="lead routing">
      <Kpi label="Within 45m SLA" value={formatPercent(sla)} tone={slaTone(sla)} delta={pointsDelta(sla, slaPrior)} />
      <Kpi label="Coverage" value={formatPercent(coverage)} tone={coverageTone(coverage)} delta={pointsDelta(coverage, coveragePrior)} />
      <Kpi label="Lead events" value={formatNumber(totalEvents)} tone="neutral" />
    </TileShell>
  );
}

function RevenueTile({
  result,
  homeRange,
  tileRange,
}: {
  result: HomeTileResult<DashboardRow>;
  homeRange: string;
  tileRange: string;
}) {
  if (!result.ok) return <TileError title="Revenue" icon={DollarSign} href="/revenue" error={result.error} />;
  const row = result.rows[0];
  const note = tileRange !== homeRange ? `Showing ${tileRange.toUpperCase()} — mart minimum window` : undefined;
  return (
    <TileShell title="Revenue" icon={DollarSign} href="/revenue" eyebrow="dbt-mart" rangeNote={note}>
      <Kpi label="Collected net" value={formatCurrency(numberValue(row?.total_net_revenue_after_refunds))} tone="green" />
      <Kpi label="Revenue / buyer" value={formatCurrency(numberValue(row?.revenue_per_buyer))} tone="blue" />
      <Kpi
        label="Magnet coverage"
        value={formatPercent(numberValue(row?.latest_prior_magnet_buyer_coverage))}
        tone={coverageTone(numberValue(row?.latest_prior_magnet_buyer_coverage))}
      />
    </TileShell>
  );
}

function RetentionTile({
  result,
  homeRange,
  tileRange,
}: {
  result: HomeTileResult<DashboardRow>;
  homeRange: string;
  tileRange: string;
}) {
  if (!result.ok) return <TileError title="Retention" icon={Repeat2} href="/retention" error={result.error} />;
  const row = result.rows[0];
  const failed = numberValue(row?.failed_plan_recovery_customers) ?? 0;
  const due = numberValue(row?.active_plan_due_customers) ?? 0;
  const note = tileRange !== homeRange ? `Showing ${tileRange.toUpperCase()} — mart minimum window` : undefined;
  return (
    <TileShell title="Retention" icon={Repeat2} href="/retention" eyebrow="dbt-mart" rangeNote={note}>
      <Kpi label="Customers" value={formatNumber(numberValue(row?.customers))} tone="blue" />
      <Kpi
        label="Repeat-paid"
        value={formatPercent(numberValue(row?.repeat_paid_customer_rate))}
        tone={retentionTone(numberValue(row?.repeat_paid_customer_rate))}
      />
      <Kpi label="Recovery queue" value={formatNumber(failed + due)} tone={failed + due ? "amber" : "green"} />
    </TileShell>
  );
}

function ActionsTile({ result }: { result: HomeTileResult<DashboardRow> }) {
  if (!result.ok) return <TileError title="Actions" icon={ListChecks} href="/actions" error={result.error} />;
  const row = result.rows[0];
  const high = numberValue(row?.high_priority_open_actions) ?? 0;
  return (
    <TileShell
      title="Actions"
      icon={ListChecks}
      href="/actions"
      eyebrow="agent-ops"
      rangeNote="Showing all open actions — not time-windowed"
    >
      <Kpi label="Open actions" value={formatNumber(numberValue(row?.open_actions))} tone="blue" />
      <Kpi label="Money at stake" value={formatCurrency(numberValue(row?.open_money_at_stake))} tone="green" />
      <Kpi label="High priority" value={formatNumber(high)} tone={high ? "red" : "green"} />
    </TileShell>
  );
}

function RecoveryTile({ result }: { result: HomeTileResult<DashboardRow> }) {
  if (!result.ok) return <TileError title="Recovery" icon={RotateCcw} href="/recovery" error={result.error} />;
  const row = result.rows[0];
  const payRec = numberValue(row?.payment_recovery_customers) ?? 0;
  const notRec = numberValue(row?.canceled_not_recovered) ?? 0;
  const review = numberValue(row?.manual_collection_review_customers) ?? 0;
  return (
    <TileShell title="Recovery" icon={RotateCcw} href="/recovery" eyebrow="cross-source">
      <Kpi label="Payment recovery" value={formatNumber(payRec)} tone={payRec ? "amber" : "green"} />
      <Kpi label="Canceled not recovered" value={formatNumber(notRec)} tone={notRec ? "amber" : "green"} />
      <Kpi label="Evidence to review" value={formatNumber(review)} tone={review ? "amber" : "green"} />
    </TileShell>
  );
}

function LeadMagnetsTile({ result }: { result: HomeTileResult<DashboardRow> }) {
  if (!result.ok) return <TileError title="Lead Magnets" icon={Target} href="/lead-magnets" error={result.error} />;
  const row = result.rows[0];
  return (
    <TileShell title="Lead Magnets" icon={Target} href="/lead-magnets" eyebrow="attribution">
      <Kpi label="Matched buyers" value={formatNumber(numberValue(row?.buyers))} tone="blue" />
      <Kpi
        label="Magnet → Buyer"
        value={formatPercent(numberValue(row?.latest_prior_magnet_buyer_coverage))}
        tone={coverageTone(numberValue(row?.latest_prior_magnet_buyer_coverage))}
      />
      <Kpi label="Net revenue" value={formatCurrency(numberValue(row?.total_net_revenue_after_refunds))} tone="green" />
    </TileShell>
  );
}

function CustomerSearchTile() {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2">
          <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] p-2 text-[#1d4ed8]">
            <UserRound className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <p className="text-[11px] font-semibold uppercase text-[#66635f]">customer</p>
            <h2 className="text-sm font-semibold">Customer 360</h2>
          </div>
        </div>
      </div>
      <div className="mt-3">
        <CustomerSearch />
      </div>
      <p className="mt-2 text-xs text-[#66635f]">Search by name, email, or phone to open a customer profile.</p>
    </section>
  );
}

function DataHealthTile({ data }: { data: HomeSummary }) {
  const tiles = [
    { name: "Speed-to-Lead", result: data.tiles.speed_to_lead.overall },
    { name: "Revenue", result: data.tiles.revenue },
    { name: "Retention", result: data.tiles.retention },
    { name: "Actions", result: data.tiles.actions },
    { name: "Recovery", result: data.tiles.recovery },
    { name: "Lead Magnets", result: data.tiles.lead_magnets },
  ];

  const errored = tiles.filter((t) => !t.result.ok);
  const okTiles = tiles.filter((t) => t.result.ok);
  const oldestRefresh = oldestRefreshedAt(data);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start gap-2">
        <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-2 text-[#166534]">
          <ShieldCheck className="h-4 w-4" aria-hidden />
        </span>
        <div>
          <p className="text-[11px] font-semibold uppercase text-[#66635f]">freshness</p>
          <h2 className="text-sm font-semibold">Data Health</h2>
        </div>
      </div>

      <div className="mt-3 grid grid-cols-2 gap-2">
        <Kpi label="Sources OK" value={formatNumber(okTiles.length)} tone={errored.length ? "amber" : "green"} />
        <Kpi label="Sources errored" value={formatNumber(errored.length)} tone={errored.length ? "red" : "green"} />
      </div>

      <p className="mt-3 text-xs text-[#66635f]">
        {oldestRefresh ? `Oldest mart refresh: ${formatTimestamp(oldestRefresh)}` : "Refresh timestamps unavailable."}
      </p>

      {errored.length ? (
        <ul className="mt-2 space-y-1 text-xs text-[#991b1b]">
          {errored.map((t) => (
            <li key={t.name}>
              <span className="font-semibold">{t.name}:</span>{" "}
              <span title={t.result.ok ? "" : t.result.error}>
                {t.result.ok ? "" : truncate(t.result.error, 80)}
              </span>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}

// ----------------------------------------------------------------------
// Tile primitives
// ----------------------------------------------------------------------

type IconComponent = React.ComponentType<{ className?: string; "aria-hidden"?: boolean }>;

function TileShell({
  title,
  eyebrow,
  icon: Icon,
  href,
  rangeNote,
  children,
}: {
  title: string;
  eyebrow: string;
  icon: IconComponent;
  href: string;
  rangeNote?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="flex flex-col rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2">
          <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] p-2 text-[#1d4ed8]">
            <Icon className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <p className="text-[11px] font-semibold uppercase text-[#66635f]">{eyebrow}</p>
            <h2 className="text-sm font-semibold">{title}</h2>
          </div>
        </div>
        <Link
          href={href}
          className="inline-flex items-center gap-1 rounded-md border border-[#dedbd2] px-2 py-1 text-[11px] font-semibold text-[#3b3936] hover:bg-[#f3f1ea]"
        >
          Open <ArrowUpRight className="h-3 w-3" aria-hidden />
        </Link>
      </div>
      <div className="mt-3 grid grid-cols-3 gap-2">{children}</div>
      {rangeNote ? (
        <p className="mt-2 text-[11px] text-[#92400e]" title="This mart does not support the home-page time range.">
          {rangeNote}
        </p>
      ) : null}
    </section>
  );
}

function TileError({
  title,
  icon: Icon,
  href,
  error,
}: {
  title: string;
  icon: IconComponent;
  href: string;
  error: string;
}) {
  return (
    <section className="flex flex-col rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2">
          <span className="rounded-md border border-[#fecaca] bg-white p-2 text-[#991b1b]">
            <Icon className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <p className="text-[11px] font-semibold uppercase text-[#991b1b]">unavailable</p>
            <h2 className="text-sm font-semibold">{title}</h2>
          </div>
        </div>
        <Link
          href={href}
          className="inline-flex items-center gap-1 rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-[11px] font-semibold text-[#3b3936] hover:bg-[#fff5f5]"
        >
          Open <ArrowUpRight className="h-3 w-3" aria-hidden />
        </Link>
      </div>
      <p className="mt-3 text-xs text-[#991b1b]" title={error}>
        {truncate(error, 140)}
      </p>
    </section>
  );
}

function Kpi({
  label,
  value,
  tone,
  delta,
}: {
  label: string;
  value: string;
  tone: Tone;
  delta?: Delta;
}) {
  return (
    <div className={`rounded-md border px-2 py-2 ${toneClasses[tone]}`}>
      <div className="text-[10px] font-semibold uppercase tracking-wide opacity-80">{label}</div>
      <div className="mt-1 flex items-baseline gap-2">
        <span className="text-base font-semibold">{value}</span>
        {delta?.available ? <DeltaPill points={delta.points} /> : null}
      </div>
    </div>
  );
}

type Delta = { available: true; points: number } | { available: false };

function DeltaPill({ points }: { points: number }) {
  const rounded = Math.round(points * 10) / 10;
  if (Math.abs(rounded) < 0.1) {
    return <span className="rounded-sm bg-white/70 px-1 text-[10px] font-semibold opacity-80">flat</span>;
  }
  const sign = rounded > 0 ? "+" : "";
  return (
    <span className="rounded-sm bg-white/70 px-1 text-[10px] font-semibold">
      {sign}
      {rounded.toFixed(1)}pt
    </span>
  );
}

function pointsDelta(current: number | null, prior: number | null): Delta {
  if (current === null || prior === null) return { available: false };
  return { available: true, points: (current - prior) * 100 };
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

function firstRow<T>(result: HomeTileResult<T>): T | undefined {
  if (!result.ok) return undefined;
  return result.rows[0];
}

function countErrored(data: HomeSummary): number {
  const t = data.tiles;
  return [t.speed_to_lead.overall, t.revenue, t.retention, t.actions, t.recovery, t.lead_magnets].filter(
    (r) => !r.ok,
  ).length;
}

function countStale(data: HomeSummary): number {
  // V1: stale detection is left to per-source freshness in the future. Today we only surface errors.
  // Hook reserved here so the Needs Review rule order doesn't need to change later.
  void data;
  return 0;
}

function oldestRefreshedAt(data: HomeSummary): string | null {
  const candidates: Array<string | null> = [];
  const r1 = firstRow(data.tiles.recovery);
  candidates.push(stringValue(r1?.oldest_dependency_refreshed_at));
  const r2 = firstRow(data.tiles.lead_magnets);
  candidates.push(stringValue(r2?.refreshed_at));
  const r3 = firstRow(data.tiles.retention);
  candidates.push(stringValue(r3?.retention_refreshed_at));
  const r4 = firstRow(data.tiles.revenue);
  candidates.push(stringValue(r4?.refreshed_at));

  const valid = candidates.filter((v): v is string => Boolean(v));
  if (!valid.length) return null;
  return valid.sort()[0];
}

function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (value && typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    return numberValue((value as { value: unknown }).value);
  }
  return null;
}

function stringValue(value: unknown): string | null {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (value && typeof value === "object" && "value" in (value as Record<string, unknown>)) {
    const inner = (value as { value: unknown }).value;
    if (typeof inner === "string" && inner.trim() !== "") return inner;
  }
  return null;
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US").format(value);
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
}

function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    if (!Number.isFinite(d.getTime())) return iso;
    return d.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function truncate(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 1)}…`;
}

function slaTone(value: number | null): Tone {
  if (value === null) return "neutral";
  if (value >= 0.8) return "green";
  if (value >= 0.5) return "amber";
  return "red";
}

function coverageTone(value: number | null): Tone {
  if (value === null) return "neutral";
  if (value >= 0.9) return "green";
  if (value >= 0.7) return "amber";
  return "red";
}

function retentionTone(value: number | null): Tone {
  if (value === null) return "neutral";
  if (value >= 0.3) return "green";
  if (value >= 0.15) return "amber";
  return "red";
}
