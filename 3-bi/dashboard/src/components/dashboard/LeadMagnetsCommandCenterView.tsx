"use client";

import { Fragment, useMemo, useState, type ReactNode } from "react";
import {
  ArrowDownUp,
  BadgeCheck,
  BarChart3,
  BookOpen,
  CalendarCheck,
  ChevronRight,
  CircleDollarSign,
  ClipboardList,
  Filter,
  HelpCircle,
  Layers,
  Search,
  Target,
  Users,
  X,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type Column,
  type ColumnDef,
  type Row as TableRow,
  type SortingState,
} from "@tanstack/react-table";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { LeadMagnetsGuideWindow } from "@/components/dashboard/LeadMagnetsGuide";
import type {
  DashboardData,
  DashboardFilters,
  DashboardRow,
  DashboardRowValue,
} from "@/types/dashboard-data";

type Lens = "all" | "calls" | "buyers" | "sales" | "followup" | "proof";
type Tone = "green" | "blue" | "amber" | "red" | "neutral";
type DecisionKey = "scale" | "update" | "adjacent" | "tracking" | "low";
type CommandView = "priority" | "bench" | "audit";
type BenchKey = "watchlist" | "proven" | "dormant" | "tracking";
type GravityZoneKey = "proven" | "callRich" | "quietClosers" | "lowPull";

type LensOption = {
  value: Lens;
  label: string;
  purpose: string;
};

type TypeGroup = {
  key: string;
  label: string;
  rows: DashboardRow[];
  magnets: number;
  leads: number;
  booked: number;
  buyers: number;
  direct: number;
  sales: number;
  net: number;
  best: DashboardRow | null;
};

type Totals = {
  magnets: number;
  leads: number;
  booked: number;
  buyers: number;
  direct: number;
  sales: number;
  net: number;
};

type DecisionDefinition = {
  key: DecisionKey;
  label: string;
  helper: string;
  tone: Tone;
};

type DecisionPick = {
  title: string;
  label: string;
  helper: string;
  row: DashboardRow | null;
  tone: Tone;
};

type BenchDefinition = {
  key: BenchKey;
  label: string;
  helper: string;
  tone: Tone;
};

type GravityZoneDefinition = {
  key: GravityZoneKey;
  label: string;
  helper: string;
  tone: Tone;
  color: string;
};

type GravityPoint = {
  id: string;
  name: string;
  type: string;
  leads: number;
  calls: number;
  buyers: number;
  net: number;
  size: number;
  zone: GravityZoneKey;
  row: DashboardRow;
};

const DECISION_GROUPS: DecisionDefinition[] = [
  {
    key: "scale",
    label: "Scale / Repeat",
    helper: "Already producing buyer proof or a clean call-to-buyer path.",
    tone: "green",
  },
  {
    key: "update",
    label: "Update Existing",
    helper: "Demand is present, but the next step needs a clearer bridge.",
    tone: "blue",
  },
  {
    key: "adjacent",
    label: "Create Adjacent",
    helper: "Buyer proof is real enough to test a sibling angle.",
    tone: "green",
  },
  {
    key: "tracking",
    label: "Investigate Tracking",
    helper: "Volume exists, but buyer proof is missing or thin.",
    tone: "amber",
  },
  {
    key: "low",
    label: "Low Proof Right Now",
    helper: "Not enough signal to overwork this yet.",
    tone: "neutral",
  },
];

const DECISION_BY_KEY = DECISION_GROUPS.reduce(
  (lookup, group) => ({ ...lookup, [group.key]: group }),
  {} as Record<DecisionKey, DecisionDefinition>,
);

const BENCH_GROUPS: BenchDefinition[] = [
  {
    key: "watchlist",
    label: "Watchlist",
    helper: "Lead or call signal exists, but it is not a priority action yet.",
    tone: "blue",
  },
  {
    key: "proven",
    label: "Proven but Stable",
    helper: "Buyer proof exists, but the row did not make the current priority set.",
    tone: "green",
  },
  {
    key: "tracking",
    label: "Tracking Check",
    helper: "There is useful demand signal with thin buyer attribution.",
    tone: "amber",
  },
  {
    key: "dormant",
    label: "Dormant Signal",
    helper: "Low current signal, but still kept for audit and history.",
    tone: "neutral",
  },
];

const GRAVITY_ZONES: GravityZoneDefinition[] = [
  {
    key: "proven",
    label: "Proven pull",
    helper: "Calls and buyer proof are both above the current portfolio midpoint.",
    tone: "green",
    color: "#16a34a",
  },
  {
    key: "callRich",
    label: "Call-rich, buyer-thin",
    helper: "The magnet creates booked-call intent before buyer proof fully shows up.",
    tone: "blue",
    color: "#2563eb",
  },
  {
    key: "quietClosers",
    label: "Quiet closers",
    helper: "Buyer proof exists without the same call volume. Worth inspecting for hidden leverage.",
    tone: "amber",
    color: "#d97706",
  },
  {
    key: "lowPull",
    label: "Low current pull",
    helper: "Current call and buyer signal are below the portfolio midpoint.",
    tone: "neutral",
    color: "#8a857d",
  },
];

const GRAVITY_ZONE_BY_KEY = GRAVITY_ZONES.reduce(
  (lookup, zone) => ({ ...lookup, [zone.key]: zone }),
  {} as Record<GravityZoneKey, GravityZoneDefinition>,
);

const LENS_OPTIONS: LensOption[] = [
  {
    value: "all",
    label: "All Magnets",
    purpose: "Full operating board grouped by the next decision.",
  },
  {
    value: "calls",
    label: "Creating Calls",
    purpose: "Which magnets turn attention into booked-call intent?",
  },
  {
    value: "buyers",
    label: "Creating Buyers",
    purpose: "Which magnets already have buyer proof?",
  },
  {
    value: "sales",
    label: "Helping Sales",
    purpose: "Which magnets warm buyers before sales captures final credit?",
  },
  {
    value: "followup",
    label: "Needs Follow-Up",
    purpose: "Where demand or calls exist, but the next step needs tightening.",
  },
  {
    value: "proof",
    label: "Needs Proof",
    purpose: "Which magnets have demand but no buyer proof in this view.",
  },
];

const tonePill: Record<Tone, string> = {
  green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
  amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  neutral: "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]",
};

const toneDot: Record<Tone, string> = {
  green: "bg-[#16a34a]",
  blue: "bg-[#2563eb]",
  amber: "bg-[#d97706]",
  red: "bg-[#dc2626]",
  neutral: "bg-[#8a857d]",
};

const toneBand: Record<Tone, string> = {
  green: "border-l-[#16a34a] bg-[#f0fdf4]",
  blue: "border-l-[#2563eb] bg-[#eff6ff]",
  amber: "border-l-[#d97706] bg-[#fffbeb]",
  red: "border-l-[#dc2626] bg-[#fef2f2]",
  neutral: "border-l-[#8a857d] bg-[#f7f7f4]",
};

const toneLeftBorder: Record<Tone, string> = {
  green: "border-l-[#16a34a]",
  blue: "border-l-[#2563eb]",
  amber: "border-l-[#d97706]",
  red: "border-l-[#dc2626]",
  neutral: "border-l-[#8a857d]",
};

const nextMoveSurface: Record<Tone, string> = {
  green: "border-[#bbf7d0] bg-[#f0fdf4]",
  blue: "border-[#bfdbfe] bg-[#eff6ff]",
  amber: "border-[#fde68a] bg-[#fffbeb]",
  red: "border-[#fecaca] bg-[#fef2f2]",
  neutral: "border-[#dedbd2] bg-[#fbfaf7]",
};

export function LeadMagnetsCommandCenterView({ data }: { data: DashboardData }) {
  const [lens, setLens] = useState<Lens>("all");
  const [search, setSearch] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const [commandView, setCommandView] = useState<CommandView>("priority");
  const [selectedRow, setSelectedRow] = useState<DashboardRow | null>(null);
  const [guideOpen, setGuideOpen] = useState(false);

  const performanceRows = useMemo(
    () => data.rows.lead_magnet_performance_rows ?? [],
    [data.rows.lead_magnet_performance_rows],
  );
  const leadRows = useMemo(
    () =>
      performanceRows.filter(
        (row) => stringValue(row.lead_magnet_category) !== "sales_operating_pipeline",
      ),
    [performanceRows],
  );
  const salesRows = useMemo(
    () =>
      performanceRows.filter(
        (row) => stringValue(row.lead_magnet_category) === "sales_operating_pipeline",
      ),
    [performanceRows],
  );
  const influenceSummary = data.rows.lead_magnet_influence_summary?.[0];
  const creditSplitRows = data.rows.lead_magnet_influence_credit_split ?? [];
  const typeGroups = useMemo(() => buildTypeGroups(leadRows), [leadRows]);
  const selectedGroup = typeGroups.find((group) => group.key === typeFilter);
  const scopedRows = useMemo(
    () =>
      typeFilter === "all"
        ? leadRows
        : leadRows.filter((row) => typeKey(row) === typeFilter),
    [leadRows, typeFilter],
  );
  const commandRows = useMemo(
    () => rankRowsForLens(filterRows(scopedRows, lens, search), lens),
    [scopedRows, lens, search],
  );
  const priorityRows = useMemo(() => commandRows.slice(0, 10), [commandRows]);
  const benchRows = useMemo(() => commandRows.slice(10), [commandRows]);
  const visibleCommandRows = useMemo(() => {
    if (commandView === "priority") return priorityRows;
    if (commandView === "bench") return benchRows;
    return commandRows;
  }, [benchRows, commandRows, commandView, priorityRows]);
  const totals = useMemo(() => buildTotals(leadRows), [leadRows]);
  const decisionPicks = useMemo(() => buildDecisionPicks(leadRows), [leadRows]);
  const activeLens = LENS_OPTIONS.find((option) => option.value === lens) ?? LENS_OPTIONS[0];

  return (
    <div className="pb-8">
      <header className="flex flex-col gap-4 border-b border-[#dedbd2] pb-5 lg:flex-row lg:items-start lg:justify-between">
        <div className="max-w-4xl">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-xs font-semibold text-[#166534]">
              Lead magnet command center
            </span>
            <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold text-[#3b3936]">
              Influence separated from final credit
            </span>
          </div>
          <h1 className="mt-3 text-4xl font-semibold tracking-normal text-[#171717] lg:text-5xl">
            Lead Magnets
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            Decide which magnets to update, repeat, create adjacent versions of, or investigate by looking at leads, booked calls, buyer influence, direct credit, and sales handoff together.
          </p>
        </div>

        <div className="flex flex-col gap-2 lg:items-end">
          <FreshnessBadge freshness={data.freshness} />
          <TimeRangeControl filters={data.filters} />
          <button
            type="button"
            className="inline-flex items-center justify-center gap-2 rounded-lg border border-[#dedbd2] bg-white px-3 py-2 text-xs font-semibold text-[#3b3936] shadow-sm transition hover:bg-[#f3f1ea] focus:outline-none focus:ring-2 focus:ring-[#0f766e]"
            onClick={() => setGuideOpen(true)}
          >
            <BookOpen className="h-3.5 w-3.5" aria-hidden />
            How this works
          </button>
        </div>
      </header>

      {data.error ? (
        <div className="mt-4 rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <main className="mt-4 space-y-4">
        <DecisionBrief picks={decisionPicks} onSelect={setSelectedRow} />

        <CommandTable
          rows={visibleCommandRows}
          priorityRows={priorityRows}
          benchRows={benchRows}
          auditRows={commandRows}
          allRows={scopedRows}
          typeGroups={typeGroups}
          lens={lens}
          activeLens={activeLens}
          commandView={commandView}
          search={search}
          selectedTypeKey={typeFilter}
          selectedGroup={selectedGroup}
          onCommandViewChange={(view) => {
            setCommandView(view);
            setSelectedRow(null);
          }}
          onLensChange={(value) => {
            setLens(value);
            setSelectedRow(null);
            setCommandView("priority");
          }}
          onTypeChange={(key) => {
            setTypeFilter(key);
            setSelectedRow(null);
            setCommandView("priority");
          }}
          onSearchChange={(value) => {
            setSearch(value);
            setCommandView(value.trim() ? "audit" : "priority");
          }}
          onClearSearch={() => {
            setSearch("");
            setCommandView("priority");
          }}
          onResetView={() => {
            setLens("all");
            setSearch("");
            setTypeFilter("all");
            setCommandView("priority");
            setSelectedRow(null);
          }}
          onSelectRow={setSelectedRow}
        />

        <BuyerGravityPanel
          rows={scopedRows}
          scopeLabel={selectedGroup?.label ?? "All types"}
          onSelectRow={setSelectedRow}
        />

        <ContextSection>
          <SnapshotSection
            summary={influenceSummary}
            totals={totals}
            filters={data.filters}
          />

          <TypeMixChart groups={typeGroups} />

          <CreditSplitPanel rows={creditSplitRows} summary={influenceSummary} />

          <section className="grid gap-4 xl:grid-cols-[minmax(0,0.78fr)_minmax(0,1.22fr)]">
            <SalesHandoffPanel rows={salesRows} />
            <MetricInspector data={data} />
          </section>
        </ContextSection>
      </main>

      <MagnetJourneyDrawer
        row={selectedRow}
        filters={data.filters}
        freshnessDetail={data.freshness.detail}
        onClose={() => setSelectedRow(null)}
      />
      <LeadMagnetsGuideWindow open={guideOpen} onClose={() => setGuideOpen(false)} />
    </div>
  );
}

function TimeRangeControl({ filters }: { filters: DashboardFilters }) {
  return (
    <div className="w-full lg:w-auto">
      <div className="flex w-full flex-wrap rounded-lg border border-[#dedbd2] bg-white p-1 shadow-sm lg:w-auto lg:flex-nowrap">
        {filters.timeRangeOptions.map((option) => {
          const isActive = option.value === filters.timeRange;
          const href = `/lead-magnets?range=${option.value}`;

          return (
            <a
              key={option.value}
              href={href}
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
      <div className="mt-1 text-left text-[11px] leading-4 text-[#66635f] lg:text-right">
        {filters.timeRangeDescription}
      </div>
    </div>
  );
}

function ContextSection({ children }: { children: ReactNode }) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <details
      className="rounded-xl border border-[#dedbd2] bg-white shadow-sm"
      onToggle={(event) => setIsOpen(event.currentTarget.open)}
    >
      <summary className="cursor-pointer list-none px-4 py-3">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="text-sm font-semibold text-[#2d2b28]">Context, definitions, and portfolio view</div>
            <div className="mt-1 text-xs text-[#66635f]">
              Open when you want the supporting analytics behind the command table.
            </div>
          </div>
          <span className="rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1 text-xs font-semibold text-[#3b3936]">
            optional context
          </span>
        </div>
      </summary>
      {isOpen ? <div className="space-y-4 border-t border-[#ece9e1] p-4">{children}</div> : null}
    </details>
  );
}

function SnapshotSection({
  summary,
  totals,
  filters,
}: {
  summary: DashboardRow | undefined;
  totals: Totals;
  filters: DashboardFilters;
}) {
  const influencedBuyers =
    numberValue(summary?.expanded_touch_buyers) ??
    numberValue(summary?.any_lead_magnet_touch_buyers);
  const influencedRevenue =
    numberValue(summary?.expanded_touch_buyer_revenue) ??
    numberValue(summary?.any_lead_magnet_touch_buyer_revenue);
  const directCredit =
    numberValue(summary?.latest_touch_true_buyers) ??
    totals.direct;
  const salesHandoff =
    numberValue(summary?.expanded_buyers_later_credited_to_sales_pipeline) ??
    totals.sales;

  const cards = [
    {
      label: "Form Leads",
      value: formatNumber(numberValue(summary?.unique_form_leads)),
      helper: `${formatNumber(numberValue(summary?.form_events))} events across ${formatNumber(numberValue(summary?.distinct_form_magnets))} forms`,
      icon: Users,
      tone: "green" as const,
    },
    {
      label: "Influenced Buyers",
      value: formatNumber(influencedBuyers),
      helper: `${formatNumber(numberValue(summary?.expanded_form_touch_buyers))} form-touch, ${formatNumber(numberValue(summary?.expanded_opportunity_only_buyers))} opportunity-only`,
      icon: Target,
      tone: "green" as const,
    },
    {
      label: "Booked Calls",
      value: formatNumber(totals.booked),
      helper: "Calls tied to non-sales magnet paths",
      icon: CalendarCheck,
      tone: "blue" as const,
    },
    {
      label: "Influenced Net",
      value: formatCurrency(influencedRevenue),
      helper: "Nonexclusive collected buyer net",
      icon: CircleDollarSign,
      tone: "green" as const,
    },
    {
      label: "Direct Credit",
      value: formatNumber(directCredit),
      helper: "Still credited to true magnets",
      icon: BadgeCheck,
      tone: "neutral" as const,
    },
    {
      label: "Sales Handoff",
      value: formatNumber(salesHandoff),
      helper: "Influenced buyers credited downstream",
      icon: Layers,
      tone: "amber" as const,
    },
  ];

  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div>
          <h2 className="text-sm font-semibold text-[#2d2b28]">Snapshot</h2>
          <p className="mt-1 max-w-3xl text-xs leading-5 text-[#66635f]">
            {formatNumber(influencedBuyers)} buyers had a lead-magnet signal before purchase. Direct credit and sales handoff are intentionally separated.
          </p>
        </div>
        <span className="w-fit rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1 text-xs font-semibold text-[#3b3936]">
          {filters.timeRangeLabel} buyer window
        </span>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6">
        {cards.map((card) => {
          const Icon = card.icon;

          return (
            <article key={card.label} className="rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="text-[11px] font-semibold uppercase text-[#66635f]">
                    {card.label}
                  </div>
                  <div className="mt-2 text-2xl font-semibold tracking-normal text-[#171717]">
                    {card.value}
                  </div>
                </div>
                <span className={`rounded-md border p-1.5 ${tonePill[card.tone]}`}>
                  <Icon className="h-4 w-4" aria-hidden />
                </span>
              </div>
              <p className="mt-2 line-clamp-2 text-[11px] leading-4 text-[#66635f]">
                {card.helper}
              </p>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function DecisionBrief({
  picks,
  onSelect,
}: {
  picks: DecisionPick[];
  onSelect: (row: DashboardRow) => void;
}) {
  return (
    <section className="overflow-hidden rounded-xl bg-[#2d2b28] shadow-sm">
      <div className="grid lg:grid-cols-[17rem_minmax(0,1fr)]">
        <div className="border-b border-white/10 px-5 py-5 text-white lg:border-b-0 lg:border-r">
          <div className="flex items-center gap-3">
            <span className="h-2.5 w-2.5 rounded-full bg-[#c6f6d5]" />
            <span className="text-[10px] font-semibold uppercase tracking-[0.18em] text-[#c6f6d5]">
              Decision first
            </span>
          </div>
          <h2 className="mt-4 text-2xl font-semibold tracking-normal">What should we do next?</h2>
          <p className="mt-3 max-w-[14rem] text-sm font-medium leading-6 text-white/70">
            Three distinct magnets to act on now. Click any card to open its brief.
          </p>
        </div>

        <div className="grid divide-y divide-white/10 md:grid-cols-3 md:divide-x md:divide-y-0">
          {picks.map((pick) => {
            const interactive = pick.row !== null;
            const Tag = interactive ? "button" : "div";
            const interactiveProps = interactive
              ? {
                  type: "button" as const,
                  onClick: () => pick.row && onSelect(pick.row),
                  "aria-label": `${pick.title}: ${pick.label} — open brief`,
                }
              : {};

            return (
              <Tag
                key={pick.title}
                {...interactiveProps}
                className={`group block w-full bg-white/[0.035] px-5 py-5 text-left text-white transition focus:outline-none focus-visible:ring-2 focus-visible:ring-[#c6f6d5] ${
                  interactive ? "cursor-pointer hover:bg-white/[0.08]" : "cursor-default"
                }`}
              >
                <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.14em] text-white/55">
                  <span className={`h-2.5 w-2.5 rounded-full ${toneDot[pick.tone]}`} />
                  {pick.title}
                </div>
                <h3 className="mt-5 text-2xl font-semibold tracking-normal">{pick.label}</h3>
                <p className="mt-3 text-sm font-medium leading-6 text-white/65">{pick.helper}</p>
                {interactive ? (
                  <span className="mt-4 inline-flex items-center gap-1 text-xs font-semibold text-[#c6f6d5] transition group-hover:gap-2">
                    Open brief
                    <ChevronRight className="h-3.5 w-3.5" aria-hidden />
                  </span>
                ) : null}
              </Tag>
            );
          })}
        </div>
      </div>
    </section>
  );
}

function CreditSplitPanel({
  rows,
  summary,
}: {
  rows: DashboardRow[];
  summary: DashboardRow | undefined;
}) {
  const totalBuyers =
    numberValue(summary?.expanded_touch_buyers) ??
    rows.reduce((sum, row) => sum + (numberValue(row.buyers) ?? 0), 0);

  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-[#2d2b28]">Where Influence Ends</h2>
          <p className="mt-1 text-xs leading-5 text-[#66635f]">
            Lead magnets may start intent even when a later sales or launch motion wins final credit.
          </p>
        </div>
        <HelpCircle className="h-4 w-4 shrink-0 text-[#66635f]" aria-hidden />
      </div>

      <div className="mt-4 space-y-3">
        {rows.map((row) => {
          const label = stringValue(row.latest_credit_label) ?? "Unknown";
          const buyers = numberValue(row.buyers);
          const share = numberValue(row.buyer_share) ?? safeDivide(buyers, totalBuyers) ?? 0;

          return (
            <div key={stringValue(row.latest_credit_category) ?? label}>
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#2d2b28]">{label}</div>
                  <div className="mt-0.5 text-[11px] text-[#66635f]">
                    {formatNumber(buyers)} buyers · {formatCurrency(numberValue(row.buyer_net_revenue))}
                  </div>
                </div>
                <div className="text-sm font-semibold tabular-nums text-[#2d2b28]">
                  {formatPercent(share)}
                </div>
              </div>
              <div className="mt-2 h-2 rounded-sm bg-[#ece9e1]">
                <div
                  className="h-2 rounded-sm bg-[#0f766e]"
                  style={{ width: `${Math.max(2, share * 100)}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function TypeFilterStrip({
  groups,
  selectedKey,
  onSelect,
}: {
  groups: TypeGroup[];
  selectedKey: string;
  onSelect: (key: string) => void;
}) {
  const totals = groups.reduce(
    (current, group) => ({
      magnets: current.magnets + group.magnets,
      leads: current.leads + group.leads,
      booked: current.booked + group.booked,
      buyers: current.buyers + group.buyers,
      direct: current.direct + group.direct,
      sales: current.sales + group.sales,
      net: current.net + group.net,
    }),
    { magnets: 0, leads: 0, booked: 0, buyers: 0, direct: 0, sales: 0, net: 0 },
  );

  return (
    <div className="mx-5 mt-3 border-t border-[#ece9e1] pt-3">
      <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex w-fit items-center gap-2 rounded-lg border border-[#ece9e1] bg-[#fbfaf7] px-3 py-2">
          <span className="h-2 w-2 rounded-full bg-[#0f766e]" />
          <div className="text-[10px] font-semibold uppercase tracking-normal text-[#8a857d]">Type focus</div>
          <span className="hidden text-[11px] text-[#66635f] sm:inline">Narrow by family</span>
        </div>
        <span className="w-fit rounded-md border border-[#dedbd2] bg-white px-3 py-1.5 text-[11px] font-semibold tabular-nums text-[#3b3936]">
          {selectedKey === "all" ? `${formatNumber(totals.magnets)} total magnets` : "Filtered table"}
        </span>
      </div>

      <div className="-mx-1 mt-2 flex gap-1.5 overflow-x-auto px-1 pb-1">
        <TypeFilterButton
          label="All types"
          metric={`${formatNumber(totals.magnets)} magnets`}
          helper={`${formatNumber(totals.buyers)} buyers · ${formatNumber(totals.booked)} calls`}
          active={selectedKey === "all"}
          onClick={() => onSelect("all")}
        />
        {groups.map((group) => (
          <TypeFilterButton
            key={group.key}
            label={group.label}
            metric={`${formatNumber(group.magnets)} magnets`}
            helper={`${formatNumber(group.buyers)} buyers · ${formatNumber(group.booked)} calls`}
            active={selectedKey === group.key}
            onClick={() => onSelect(selectedKey === group.key ? "all" : group.key)}
          />
        ))}
      </div>
    </div>
  );
}

function TypeFilterButton({
  label,
  metric,
  helper,
  active,
  onClick,
}: {
  label: string;
  metric: string;
  helper: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      title={helper}
      className={`inline-flex min-w-fit shrink-0 items-center gap-2 rounded-md border px-3 py-2 text-left transition focus:outline-none focus:ring-2 focus:ring-[#0f766e] ${
        active
          ? "border-[#0f766e] bg-white shadow-sm"
          : "border-[#ece9e1] bg-white/70 hover:border-[#c9c5bc] hover:bg-white"
      }`}
      onClick={onClick}
    >
      <span className="max-w-36 truncate text-xs font-semibold text-[#2d2b28]">{label}</span>
      <span className="rounded-md bg-[#f0fdfa] px-2 py-0.5 text-[11px] font-semibold tabular-nums text-[#0f766e]">
        {metric.replace(" magnets", "")}
      </span>
      <span className="sr-only">{helper}</span>
    </button>
  );
}

function CommonMoveBanner({
  insight,
}: {
  insight: { move: ReturnType<typeof nextMove>; count: number; total: number };
}) {
  const { move, count, total } = insight;

  return (
    <div className={`mx-5 mt-4 rounded-lg border border-l-4 px-4 py-3 ${nextMoveSurface[move.tone]} ${toneLeftBorder[move.tone]}`}>
      <div className="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.14em] text-[#66635f]">
            <span className={`h-2.5 w-2.5 rounded-full ${toneDot[move.tone]}`} />
            Common gap in this view
          </div>
          <div className="mt-1 text-sm font-semibold text-[#2d2b28]">
            {move.label} appears on {formatNumber(count)} of {formatNumber(total)} shown magnets.
          </div>
        </div>
        <p className="max-w-2xl text-xs leading-5 text-[#3b3936]">{move.helper}</p>
      </div>
    </div>
  );
}

function TypeMixChart({ groups }: { groups: TypeGroup[] }) {
  const chartData = groups.slice(0, 8).map((group) => ({
    type: group.label,
    calls: group.booked,
    buyers: group.buyers,
    sales: group.sales,
  }));

  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-[#2d2b28]">Calls, Buyers, Sales Assist By Type</h2>
          <p className="mt-1 text-xs leading-5 text-[#66635f]">
            Horizontal comparison only. The table keeps the exact operating read.
          </p>
        </div>
        <BarChart3 className="h-4 w-4 shrink-0 text-[#66635f]" aria-hidden />
      </div>

      <div className="mt-4 h-72">
        {chartData.length === 0 ? (
          <EmptyState title="No type data" body="No lead magnet type groups returned for this range." />
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} layout="vertical" margin={{ left: 18, right: 16, top: 8, bottom: 8 }}>
              <CartesianGrid stroke="#ece9e1" horizontal={false} />
              <XAxis type="number" tick={{ fill: "#66635f", fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis
                dataKey="type"
                type="category"
                width={112}
                tick={{ fill: "#66635f", fontSize: 11 }}
                axisLine={false}
                tickLine={false}
              />
              <Tooltip
                cursor={{ fill: "#f3f1ea" }}
                contentStyle={{
                  border: "1px solid #dedbd2",
                  borderRadius: 8,
                  color: "#2d2b28",
                  fontSize: 12,
                }}
              />
              <Bar dataKey="calls" fill="#2563eb" radius={[0, 4, 4, 0]} />
              <Bar dataKey="buyers" fill="#16a34a" radius={[0, 4, 4, 0]} />
              <Bar dataKey="sales" fill="#d97706" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </section>
  );
}

function CommandTable({
  rows,
  priorityRows,
  benchRows,
  auditRows,
  allRows,
  typeGroups,
  lens,
  activeLens,
  commandView,
  search,
  selectedTypeKey,
  selectedGroup,
  onCommandViewChange,
  onLensChange,
  onTypeChange,
  onSearchChange,
  onClearSearch,
  onResetView,
  onSelectRow,
}: {
  rows: DashboardRow[];
  priorityRows: DashboardRow[];
  benchRows: DashboardRow[];
  auditRows: DashboardRow[];
  allRows: DashboardRow[];
  typeGroups: TypeGroup[];
  lens: Lens;
  activeLens: LensOption;
  commandView: CommandView;
  search: string;
  selectedTypeKey: string;
  selectedGroup: TypeGroup | undefined;
  onCommandViewChange: (view: CommandView) => void;
  onLensChange: (lens: Lens) => void;
  onTypeChange: (key: string) => void;
  onSearchChange: (value: string) => void;
  onClearSearch: () => void;
  onResetView: () => void;
  onSelectRow: (row: DashboardRow) => void;
}) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const columns = useMemo<ColumnDef<DashboardRow>[]>(
    () => [
      {
        id: "magnet",
        accessorFn: (row) => stringValue(row.lead_magnet_reporting_name) ?? "",
        header: ({ column }) => <SortHeader column={column} label="Magnet" />,
        cell: ({ row }) => <MagnetCell row={row.original} />,
      },
      {
        id: "path",
        accessorFn: (row) => pathScore(row),
        header: ({ column }) => <SortHeader column={column} label="Path" />,
        cell: ({ row }) => <PathCell row={row.original} />,
      },
      {
        id: "credit",
        accessorFn: (row) => (directBuyers(row) ?? 0) + (salesBuyers(row) ?? 0),
        header: ({ column }) => <SortHeader column={column} label="Credit" />,
        cell: ({ row }) => <CreditCell row={row.original} />,
      },
      {
        id: "value",
        accessorFn: (row) => numberValue(row.influence_nonexclusive_buyer_revenue) ?? 0,
        header: ({ column }) => <SortHeader column={column} label="Value" align="right" />,
        cell: ({ row }) => <ValueCell row={row.original} />,
      },
      {
        id: "proof",
        accessorFn: (row) => confidenceRank(row),
        header: ({ column }) => <SortHeader column={column} label="Proof" />,
        cell: ({ row }) => {
          const confidence = confidenceRead(row.original);
          return (
            <Badge
              label={confidence.label}
              helper={confidence.helper}
              tone={confidence.tone}
            />
          );
        },
      },
      {
        id: "next",
        accessorFn: (row) => nextMove(row).label,
        header: "Next move",
        cell: ({ row }) => <NextMoveCell row={row.original} />,
      },
    ],
    [],
  );

  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });
  const visibleTotals = useMemo(() => buildTotals(rows), [rows]);
  const commonMoveInsight = useMemo(() => buildCommonMoveInsight(rows), [rows]);
  const groupedTableRows = groupTableRows(table.getRowModel().rows);
  const decisionCounts = DECISION_GROUPS.map((definition) => ({
    definition,
    count: rows.filter((row) => decisionForRow(row).key === definition.key).length,
  }));
  const activeTypeLabel =
    selectedTypeKey === "all"
      ? "All types"
      : selectedGroup?.label ?? "Selected type";
  const hasModifiedView =
    commandView !== "priority" || lens !== "all" || selectedTypeKey !== "all" || search.trim() !== "";
  const viewOptions: Array<{ value: CommandView; label: string; helper: string; count: number }> = [
    {
      value: "priority",
      label: "Priority 10",
      helper: "Act on these first.",
      count: priorityRows.length,
    },
    {
      value: "bench",
      label: "Bench",
      helper: "Useful signals, lower urgency.",
      count: benchRows.length,
    },
    {
      value: "audit",
      label: `Audit All ${formatNumber(auditRows.length)}`,
      helper: "Full inspection mode.",
      count: auditRows.length,
    },
  ];

  return (
    <section className="overflow-hidden rounded-xl border border-[#c9c5bc] bg-white shadow-sm">
      <div className="flex flex-col gap-4 border-b border-[#ece9e1] px-5 py-4 xl:flex-row xl:items-start xl:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="flex h-9 w-9 items-center justify-center rounded-full bg-[#0f766e] text-white">
              <ClipboardList className="h-4 w-4" aria-hidden />
            </span>
            <div>
              <div className="text-[11px] font-semibold uppercase text-[#0f766e]">
                Main operating surface
              </div>
              <h2 className="text-2xl font-semibold tracking-normal text-[#2d2b28]">
                What should we do with each magnet?
              </h2>
            </div>
          </div>
          <p className="mt-2 max-w-4xl text-xs leading-5 text-[#66635f]">
            One row per non-sales lead magnet path. Click a row to inspect source, scope, caveats, and the journey proof behind the recommendation.
          </p>
        </div>
        <div className="rounded-lg border border-[#ece9e1] bg-[#fbfaf7] px-4 py-3 text-xs leading-5 text-[#66635f] xl:max-w-md xl:text-right">
          <span className="font-semibold text-[#2d2b28]">{activeLens.label}:</span>{" "}
          {activeLens.purpose} Showing {formatNumber(rows.length)} of {formatNumber(allRows.length)} rows.
          <span className="mt-1 block tabular-nums text-[#3b3936]">
            {formatNumber(visibleTotals.leads)} leads · {formatNumber(visibleTotals.booked)} calls · {formatNumber(visibleTotals.buyers)} buyers · {formatCurrency(visibleTotals.net)}
          </span>
        </div>
      </div>

      {commonMoveInsight ? <CommonMoveBanner insight={commonMoveInsight} /> : null}

      <DecisionGroupSummary items={decisionCounts} />

      <div className="mx-5 mt-4 flex flex-col gap-2 rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-1 lg:flex-row">
        {viewOptions.map((option) => {
          const active = commandView === option.value;
          return (
            <button
              key={option.value}
              type="button"
              aria-pressed={active}
              className={`flex flex-1 items-center justify-between gap-3 rounded-md border px-4 py-2 text-left transition focus:outline-none focus:ring-2 focus:ring-[#0f766e] ${
                active
                  ? "border-[#0f766e] bg-white shadow-sm"
                  : "border-transparent bg-transparent hover:bg-white"
              }`}
              onClick={() => onCommandViewChange(option.value)}
            >
              <div className="min-w-0">
                <span className="text-sm font-semibold text-[#2d2b28]">{option.label}</span>
                <span className="ml-2 hidden truncate text-[11px] text-[#66635f] xl:inline">{option.helper}</span>
              </div>
              <span className="rounded-full border border-[#dedbd2] bg-white px-2 py-0.5 text-xs font-semibold tabular-nums text-[#3b3936]">
                {formatNumber(option.count)}
              </span>
            </button>
          );
        })}
      </div>

      <div className="mx-5 mt-4 grid gap-3 xl:grid-cols-[minmax(0,1fr)_20rem]">
        <div className="flex flex-wrap gap-2">
          {LENS_OPTIONS.map((option) => {
            const active = lens === option.value;
            return (
              <button
                key={option.value}
                type="button"
                aria-pressed={active}
                className={`rounded-full border px-3 py-2 text-xs font-semibold transition focus:outline-none focus:ring-2 focus:ring-[#0f766e] ${
                  active
                    ? "border-[#2d2b28] bg-[#2d2b28] text-white"
                    : "border-[#dedbd2] bg-[#fbfaf7] text-[#66635f] hover:bg-white hover:text-[#2d2b28]"
                }`}
                onClick={() => onLensChange(option.value)}
              >
                {option.label}
              </button>
            );
          })}
        </div>
        <label className="relative block">
          <span className="sr-only">Search lead magnets</span>
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#8a857d]" aria-hidden />
          <input
            value={search}
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder="Search magnets..."
            className="h-10 w-full rounded-lg border border-[#dedbd2] bg-white pl-9 pr-9 text-sm outline-none transition placeholder:text-[#aaa69f] focus:border-[#0f766e] focus:ring-2 focus:ring-[#0f766e]/20"
          />
          {search ? (
            <button
              type="button"
              aria-label="Clear search"
              className="absolute right-2 top-1/2 -translate-y-1/2 rounded-md p-1 text-[#66635f] hover:bg-[#f3f1ea] hover:text-[#2d2b28]"
              onClick={onClearSearch}
            >
              <X className="h-4 w-4" aria-hidden />
            </button>
          ) : null}
        </label>
      </div>

      <TypeFilterStrip groups={typeGroups} selectedKey={selectedTypeKey} onSelect={onTypeChange} />

      <div className="mx-5 mt-3 flex flex-wrap items-center gap-2 border-t border-[#ece9e1] pt-3 text-[11px] text-[#66635f]">
        <span className="font-semibold uppercase text-[#8a857d]">Active view</span>
        <ActiveChip label="Mode" value={commandViewLabel(commandView)} />
        <ActiveChip label="Lens" value={activeLens.label} />
        <ActiveChip label="Type" value={activeTypeLabel} />
        {search.trim() ? <ActiveChip label="Search" value={search.trim()} onClear={onClearSearch} /> : null}
        {hasModifiedView ? (
          <button
            type="button"
            className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 font-semibold text-[#3b3936] transition hover:bg-[#f3f1ea] focus:outline-none focus:ring-2 focus:ring-[#0f766e]"
            onClick={onResetView}
          >
            Reset
          </button>
        ) : null}
      </div>

      {rows.length === 0 ? (
        <div className="mx-5 mt-4">
          <EmptyState
            title="No magnets match this view"
            body="Clear search, change the type, or switch the lens. This is a filtered-empty state, not proof that the magnets have no data."
          />
        </div>
      ) : commandView === "bench" ? (
        <BenchBoard rows={rows} onSelectRow={onSelectRow} />
      ) : (
        <div className="mt-4 overflow-x-auto border-t border-[#ece9e1]">
          <table className="min-w-[1040px] border-separate border-spacing-0 text-left text-xs">
            <thead className="bg-[#fbfaf7]">
              {table.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <th
                      key={header.id}
                      className={`border-b border-[#dedbd2] px-3 py-2 font-semibold text-[#66635f] ${headerClass(header.column.id)}`}
                    >
                      {header.isPlaceholder
                        ? null
                        : flexRender(header.column.columnDef.header, header.getContext())}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {groupedTableRows.map((group) => (
                <Fragment key={group.definition.key}>
                  <tr>
                    <td
                      colSpan={columns.length}
                      className={`border-b border-l-4 border-[#ece9e1] px-3 py-3 ${toneBand[group.definition.tone]}`}
                    >
                      <div className="flex flex-wrap items-center justify-between gap-3 text-xs">
                        <div className="flex min-w-0 items-center gap-2">
                          <span className={`h-2.5 w-2.5 rounded-full ${toneDot[group.definition.tone]}`} />
                          <span className="text-sm font-semibold text-[#2d2b28]">
                            {group.definition.label}
                          </span>
                          <span className="text-[#66635f]">{group.definition.helper}</span>
                        </div>
                        <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-0.5 font-semibold tabular-nums text-[#3b3936]">
                          {formatNumber(group.rows.length)} magnets
                        </span>
                      </div>
                    </td>
                  </tr>
                  {group.rows.map((tableRow) => (
                    <tr
                      key={rowKey(tableRow.original)}
                      tabIndex={0}
                      className="group cursor-pointer outline-none transition hover:bg-[#fbfaf7] focus:bg-[#f0fdf4] focus:ring-2 focus:ring-inset focus:ring-[#0f766e]"
                      onClick={() => onSelectRow(tableRow.original)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          onSelectRow(tableRow.original);
                        }
                      }}
                    >
                      {tableRow.getVisibleCells().map((cell) => (
                        <td
                          key={cell.id}
                          className={`border-b border-[#ece9e1] px-3 py-4 align-middle first:border-l-2 first:border-l-transparent group-hover:first:border-l-[#0f766e] ${cellClass(cell.column.id)}`}
                        >
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </td>
                      ))}
                    </tr>
                  ))}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function BuyerGravityPanel({
  rows,
  scopeLabel,
  onSelectRow,
}: {
  rows: DashboardRow[];
  scopeLabel: string;
  onSelectRow: (row: DashboardRow) => void;
}) {
  const { points, thresholds } = useMemo(() => buildGravityPoints(rows), [rows]);
  const zoneCards = GRAVITY_ZONES.map((definition) => {
    const zonePoints = points
      .filter((point) => point.zone === definition.key)
      .sort(
        (a, b) =>
          b.buyers - a.buyers ||
          b.calls - a.calls ||
          b.net - a.net ||
          a.name.localeCompare(b.name),
      );

    return {
      definition,
      points: zonePoints,
      topPoint: zonePoints[0] ?? null,
    };
  });
  const topPoint = [...points].sort(
    (a, b) =>
      b.buyers - a.buyers ||
      b.calls - a.calls ||
      b.net - a.net ||
      a.name.localeCompare(b.name),
  )[0];
  const xMax = Math.max(...points.map((point) => point.buyers), thresholds.buyers * 1.35, 1);
  const yMax = Math.max(...points.map((point) => point.calls), thresholds.calls * 1.35, 1);

  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-5 shadow-sm">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
        <div className="max-w-3xl">
          <div className="flex items-center gap-2">
            <span className="flex h-8 w-8 items-center justify-center rounded-lg bg-[#2d2b28] text-white">
              <CircleDollarSign className="h-4 w-4" aria-hidden />
            </span>
            <div>
              <div className="text-[11px] font-semibold uppercase text-[#0f766e]">
                Portfolio physics
              </div>
              <h2 className="text-xl font-semibold tracking-normal text-[#2d2b28]">Buyer Gravity</h2>
            </div>
          </div>
          <p className="mt-2 text-xs leading-5 text-[#66635f]">
            A portfolio read of where magnets pull attention into calls and buyer proof. Calls run vertical, influenced buyers run horizontal, and larger dots carry more influenced net.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-[11px] text-[#66635f] xl:justify-end">
          <SummaryChip label="Scope" value={scopeLabel} />
          <SummaryChip label="Magnets" value={formatNumber(points.length)} />
          <SummaryChip label="Midpoint" value={`${formatNumber(thresholds.calls)} calls / ${formatNumber(thresholds.buyers)} buyers`} />
        </div>
      </div>

      {points.length === 0 ? (
        <div className="mt-4">
          <EmptyState title="No magnets to map" body="There are no lead magnets in this type focus." />
        </div>
      ) : (
        <div className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1.45fr)_minmax(22rem,0.55fr)]">
          <div className="rounded-xl border border-[#cbd5e1] bg-[#f8fbff] p-3">
            <div className="relative h-[22rem] overflow-hidden rounded-lg bg-white">
              <div className="pointer-events-none absolute left-3 top-3 z-10 rounded-md bg-white/80 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-[#64748b] shadow-sm">
                Call-rich, buyer-thin
              </div>
              <div className="pointer-events-none absolute right-3 top-3 z-10 rounded-md bg-white/80 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-[#0f766e] shadow-sm">
                Proven pull
              </div>
              <div className="pointer-events-none absolute bottom-3 left-3 z-10 rounded-md bg-white/80 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-[#8a857d] shadow-sm">
                Low current pull
              </div>
              <div className="pointer-events-none absolute bottom-3 right-3 z-10 rounded-md bg-white/80 px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.12em] text-[#64748b] shadow-sm">
                Quiet closers
              </div>
              <ResponsiveContainer width="100%" height="100%">
                <ScatterChart margin={{ left: 8, right: 20, top: 18, bottom: 18 }}>
                  <CartesianGrid stroke="#dbeafe" strokeDasharray="3 3" />
                  <XAxis
                    type="number"
                    dataKey="buyers"
                    name="Influenced buyers"
                    domain={[0, xMax]}
                    allowDecimals={false}
                    tick={{ fill: "#66635f", fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                    label={{
                      value: "Influenced buyers",
                      position: "insideBottom",
                      offset: -10,
                      fill: "#66635f",
                      fontSize: 11,
                    }}
                  />
                  <YAxis
                    type="number"
                    dataKey="calls"
                    name="Booked calls"
                    domain={[0, yMax]}
                    allowDecimals={false}
                    tick={{ fill: "#66635f", fontSize: 11 }}
                    axisLine={false}
                    tickLine={false}
                    label={{
                      value: "Booked calls",
                      angle: -90,
                      position: "insideLeft",
                      fill: "#66635f",
                      fontSize: 11,
                    }}
                  />
                  <ZAxis type="number" dataKey="size" range={[64, 520]} />
                  <ReferenceLine x={thresholds.buyers} stroke="#94a3b8" strokeDasharray="4 4" />
                  <ReferenceLine y={thresholds.calls} stroke="#94a3b8" strokeDasharray="4 4" />
                  <Tooltip content={<BuyerGravityTooltip />} cursor={{ stroke: "#aaa69f", strokeDasharray: "3 3" }} />
                  <Scatter
                    data={points}
                    onClick={(event: unknown) => {
                      const point = (event as { payload?: GravityPoint })?.payload;
                      if (point) onSelectRow(point.row);
                    }}
                  >
                    {points.map((point) => (
                      <Cell
                        key={point.id}
                        fill={GRAVITY_ZONE_BY_KEY[point.zone].color}
                        stroke="#ffffff"
                        strokeWidth={1.5}
                      />
                    ))}
                  </Scatter>
                </ScatterChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-2 flex flex-wrap items-center justify-between gap-2 border-t border-[#ece9e1] pt-3 text-[11px] leading-4 text-[#66635f]">
              <span>Dashed lines mark the positive-signal midpoint for this scope.</span>
              {topPoint ? (
                <button
                  type="button"
                  className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 font-semibold text-[#3b3936] transition hover:bg-[#f3f1ea] focus:outline-none focus:ring-2 focus:ring-[#0f766e]"
                  onClick={() => onSelectRow(topPoint.row)}
                >
                  Inspect strongest pull
                </button>
              ) : null}
            </div>
          </div>

          <div className="grid gap-2">
            {zoneCards.map(({ definition, points: zonePoints, topPoint: zoneTopPoint }) => (
              <button
                key={definition.key}
                type="button"
                disabled={!zoneTopPoint}
                className={`rounded-lg border border-l-4 bg-[#fbfaf7] p-3 text-left transition focus:outline-none focus:ring-2 focus:ring-[#0f766e] ${
                  zoneTopPoint ? "hover:border-[#c9c5bc] hover:bg-white" : "cursor-default opacity-70"
                } ${toneBand[definition.tone]}`}
                onClick={() => {
                  if (zoneTopPoint) onSelectRow(zoneTopPoint.row);
                }}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className={`h-2.5 w-2.5 rounded-full ${toneDot[definition.tone]}`} />
                      <h3 className="truncate text-sm font-semibold text-[#2d2b28]">{definition.label}</h3>
                    </div>
                    <p className="mt-1 text-[11px] leading-4 text-[#66635f]">{definition.helper}</p>
                  </div>
                  <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold tabular-nums text-[#3b3936]">
                    {formatNumber(zonePoints.length)}
                  </span>
                </div>
                <div className="mt-3 rounded-md border border-[#ece9e1] bg-white px-3 py-2">
                  <div className="truncate text-xs font-semibold text-[#2d2b28]">
                    {zoneTopPoint?.name ?? "No magnets in this zone"}
                  </div>
                  {zoneTopPoint ? (
                    <div className="mt-1 text-[11px] tabular-nums text-[#66635f]">
                      {formatNumber(zoneTopPoint.calls)} calls · {formatNumber(zoneTopPoint.buyers)} buyers · {formatCurrency(zoneTopPoint.net)}
                    </div>
                  ) : (
                    <div className="mt-1 text-[11px] text-[#66635f]">Nothing to inspect in this scope.</div>
                  )}
                </div>
              </button>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}

function BuyerGravityTooltip({
  active,
  payload,
}: {
  active?: boolean;
  payload?: Array<{ payload: GravityPoint }>;
}) {
  const point = payload?.[0]?.payload;
  if (!active || !point) return null;
  const zone = GRAVITY_ZONE_BY_KEY[point.zone];

  return (
    <div className="max-w-64 rounded-lg border border-[#dedbd2] bg-white p-3 text-xs shadow-lg">
      <div className="truncate font-semibold text-[#2d2b28]">{point.name}</div>
      <div className="mt-1 text-[11px] text-[#66635f]">{point.type}</div>
      <div className={`mt-2 inline-flex rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[zone.tone]}`}>
        {zone.label}
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2 tabular-nums text-[#66635f]">
        <div>
          <div className="font-semibold text-[#2d2b28]">{formatNumber(point.leads)}</div>
          <div className="text-[10px]">leads</div>
        </div>
        <div>
          <div className="font-semibold text-[#2d2b28]">{formatNumber(point.calls)}</div>
          <div className="text-[10px]">calls</div>
        </div>
        <div>
          <div className="font-semibold text-[#2d2b28]">{formatNumber(point.buyers)}</div>
          <div className="text-[10px]">buyers</div>
        </div>
        <div>
          <div className="font-semibold text-[#2d2b28]">{formatCurrency(point.net)}</div>
          <div className="text-[10px]">influence net</div>
        </div>
      </div>
    </div>
  );
}

function SortHeader({
  column,
  label,
  align = "left",
}: {
  column: Column<DashboardRow, unknown>;
  label: string;
  align?: "left" | "right";
}) {
  const sorted = column.getIsSorted();

  return (
    <button
      type="button"
      className={`inline-flex items-center gap-1 rounded-md text-xs font-semibold text-[#66635f] hover:text-[#2d2b28] focus:outline-none focus:ring-2 focus:ring-[#0f766e] ${
        align === "right" ? "justify-end" : "justify-start"
      }`}
      onClick={() => column.toggleSorting(sorted === "asc")}
    >
      <span>{label}</span>
      <ArrowDownUp className={`h-3.5 w-3.5 ${sorted ? "text-[#0f766e]" : "text-[#aaa69f]"}`} aria-hidden />
    </button>
  );
}

function DecisionGroupSummary({
  items,
}: {
  items: Array<{ definition: DecisionDefinition; count: number }>;
}) {
  return (
    <div className="mx-5 mt-4 grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
      {items.map(({ definition, count }) => (
        <div
          key={definition.key}
          className="flex items-center justify-between gap-2 rounded-lg border border-[#ece9e1] bg-[#fbfaf7] px-3 py-2"
        >
          <div className="flex min-w-0 items-center gap-2">
            <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${toneDot[definition.tone]}`} />
            <span className="truncate text-[11px] font-semibold uppercase text-[#66635f]">{definition.label}</span>
          </div>
          <span className="rounded-md bg-white px-2 py-0.5 text-xs font-semibold tabular-nums text-[#2d2b28]">
            {formatNumber(count)}
          </span>
        </div>
      ))}
    </div>
  );
}

function ActiveChip({
  label,
  value,
  onClear,
}: {
  label: string;
  value: string;
  onClear?: () => void;
}) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1">
      <span className="font-semibold text-[#2d2b28]">{label}:</span>
      <span className="max-w-40 truncate">{value}</span>
      {onClear ? (
        <button
          type="button"
          aria-label={`Clear ${label}`}
          className="ml-1 rounded-sm text-[#66635f] hover:bg-[#ece9e1] hover:text-[#2d2b28] focus:outline-none focus:ring-2 focus:ring-[#0f766e]"
          onClick={onClear}
        >
          <X className="h-3 w-3" aria-hidden />
        </button>
      ) : null}
    </span>
  );
}

function BenchBoard({
  rows,
  onSelectRow,
}: {
  rows: DashboardRow[];
  onSelectRow: (row: DashboardRow) => void;
}) {
  const groups = groupBenchRows(rows);

  return (
    <div className="mx-5 mt-4 grid gap-3 lg:grid-cols-2">
      {groups.map((group) => {
        const totals = buildTotals(group.rows);

        return (
          <section
            key={group.definition.key}
            className={`rounded-xl border border-l-4 border-[#ece9e1] bg-[#fbfaf7] p-3 ${toneBand[group.definition.tone]}`}
          >
            <div className="flex items-start justify-between gap-3">
              <div>
                <div className="flex items-center gap-2">
                  <span className={`h-2.5 w-2.5 rounded-full ${toneDot[group.definition.tone]}`} />
                  <h3 className="text-sm font-semibold text-[#2d2b28]">
                    {group.definition.label}
                  </h3>
                </div>
                <p className="mt-1 text-[11px] leading-4 text-[#66635f]">
                  {group.definition.helper}
                </p>
              </div>
              <span className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold tabular-nums text-[#3b3936]">
                {formatNumber(group.rows.length)}
              </span>
            </div>

            <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-[#66635f]">
              <SummaryChip label="Leads" value={formatNumber(totals.leads)} />
              <SummaryChip label="Calls" value={formatNumber(totals.booked)} />
              <SummaryChip label="Buyers" value={formatNumber(totals.buyers)} />
              <SummaryChip label="Net" value={formatCurrency(totals.net)} />
            </div>

            <div className="mt-3 space-y-2">
              {group.rows.map((row) => (
                <button
                  key={rowKey(row)}
                  type="button"
                  className="w-full rounded-lg border border-[#ece9e1] bg-white p-3 text-left transition hover:border-[#c9c5bc] hover:bg-[#fbfaf7] focus:outline-none focus:ring-2 focus:ring-[#0f766e]"
                  onClick={() => onSelectRow(row)}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="truncate text-sm font-semibold text-[#2d2b28]">
                        {rowName(row)}
                      </div>
                      <div className="mt-1 truncate text-[11px] text-[#66635f]">
                        {priorityReason(row)} · {stringValue(row.offer_type_label) ?? "Unknown type"}
                      </div>
                    </div>
                    <div className="text-right text-[11px] tabular-nums text-[#66635f]">
                      <div className="font-semibold text-[#2d2b28]">
                        {formatNumber(influencedBuyers(row))} buyers
                      </div>
                      <div>{formatNumber(bookedCalls(row))} calls</div>
                    </div>
                  </div>
                </button>
              ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}

function MagnetCell({ row }: { row: DashboardRow }) {
  const name = stringValue(row.lead_magnet_reporting_name) ?? "Unknown magnet";
  const decision = decisionForRow(row);

  return (
    <div className="min-w-0">
      <div className="flex items-center gap-2">
        <span className="truncate text-sm font-semibold text-[#2d2b28]">{name}</span>
        <ChevronRight className="h-3.5 w-3.5 shrink-0 text-[#8a857d]" aria-hidden />
      </div>
      <div className="mt-1 flex min-w-0 flex-wrap items-center gap-1.5 text-[11px] text-[#66635f]">
        <span className="truncate">{stringValue(row.offer_type_label) ?? "Unknown type"}</span>
        <span className="text-[#aaa69f]">·</span>
        <span className="truncate">{categoryLabel(row.lead_magnet_category)}</span>
        <span className={`rounded-md border px-1.5 py-0.5 text-[10px] font-semibold ${tonePill[decision.tone]}`}>
          {decision.label}
        </span>
        <span className="rounded-md border border-[#dedbd2] bg-white px-1.5 py-0.5 text-[10px] font-semibold text-[#3b3936]">
          {priorityReason(row)}
        </span>
      </div>
    </div>
  );
}

function PathCell({ row }: { row: DashboardRow }) {
  const leads = leadCount(row);
  const calls = bookedCalls(row);
  const buyers = influencedBuyers(row);
  const max = Math.max(leads ?? 0, calls ?? 0, buyers ?? 0, 1);

  return (
    <div className="min-w-56">
      <div className="flex items-center gap-2 text-[11px] font-semibold tabular-nums text-[#2d2b28]">
        <span>{formatNumber(leads)} leads</span>
        <span className="text-[#aaa69f]">/</span>
        <span>{formatNumber(calls)} calls</span>
        <span className="text-[#aaa69f]">/</span>
        <span>{formatNumber(buyers)} buyers</span>
      </div>
      <div className="mt-2 grid grid-cols-3 gap-1">
        <MiniBar value={leads ?? 0} max={max} tone="neutral" />
        <MiniBar value={calls ?? 0} max={max} tone="blue" />
        <MiniBar value={buyers ?? 0} max={max} tone="green" />
      </div>
      <div className="mt-1 text-[10px] text-[#66635f]">
        {leadSourceLabel(row)} · {formatPercent(safeDivide(calls, leads))} lead-to-call
      </div>
    </div>
  );
}

function CreditCell({ row }: { row: DashboardRow }) {
  const direct = directBuyers(row);
  const sales = salesBuyers(row);
  const buyers = influencedBuyers(row);

  return (
    <div className="min-w-40">
      <div className="flex items-center gap-2 text-[11px] tabular-nums">
        <span className="font-semibold text-[#2d2b28]">{formatNumber(direct)}</span>
        <span className="text-[#66635f]">direct</span>
        <span className="text-[#aaa69f]">/</span>
        <span className="font-semibold text-[#2d2b28]">{formatNumber(sales)}</span>
        <span className="text-[#66635f]">sales</span>
      </div>
      <div className="mt-2 grid grid-cols-2 gap-1">
        <MiniBar value={direct ?? 0} max={Math.max(buyers ?? 0, 1)} tone="green" />
        <MiniBar value={sales ?? 0} max={Math.max(buyers ?? 0, 1)} tone="amber" />
      </div>
      <div className="mt-1 text-[10px] text-[#66635f]">
        {formatPercent(safeDivide(sales, buyers))} handoff rate
      </div>
    </div>
  );
}

function ValueCell({ row }: { row: DashboardRow }) {
  const net = numberValue(row.influence_nonexclusive_buyer_revenue);
  const rpl = safeDivide(net, leadCount(row)) ?? numberValue(row.revenue_per_lead);

  return (
    <div className="text-right tabular-nums">
      <div className="text-sm font-semibold text-[#2d2b28]">{formatCurrency(net)}</div>
      <div className="mt-1 text-[11px] text-[#66635f]">{formatCurrency(rpl)} / lead</div>
    </div>
  );
}

function MiniBar({ value, max, tone }: { value: number; max: number; tone: Tone }) {
  const width = max <= 0 || value <= 0 ? "0%" : `${Math.max(4, (value / max) * 100)}%`;

  return (
    <div className="h-1.5 rounded-sm bg-[#ece9e1]">
      <div className={`h-1.5 rounded-sm ${toneDot[tone]}`} style={{ width }} />
    </div>
  );
}

function Badge({ label, helper, tone }: { label: string; helper: string; tone: Tone }) {
  return (
    <div className="min-w-36">
      <span className={`inline-flex rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[tone]}`}>
        {label}
      </span>
      <div className="mt-1 text-[11px] leading-4 text-[#66635f]">{helper}</div>
    </div>
  );
}

function NextMoveCell({ row }: { row: DashboardRow }) {
  const move = nextMove(row);

  return (
    <div className={`min-w-64 border-l-2 pl-3 ${toneLeftBorder[move.tone]}`}>
      <div className="flex items-center gap-2">
        <span className={`h-2.5 w-2.5 shrink-0 rounded-full ${toneDot[move.tone]}`} />
        <span className="text-[10px] font-semibold uppercase text-[#66635f]">{move.signal}</span>
      </div>
      <div className="mt-1 text-sm font-semibold text-[#2d2b28]">{move.label}</div>
      <div className="mt-1 text-[11px] leading-4 text-[#66635f]">{move.helper}</div>
    </div>
  );
}

function SummaryChip({ label, value }: { label: string; value: string }) {
  return (
    <span className="rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1">
      <span className="font-semibold text-[#2d2b28]">{label}:</span>{" "}
      <span className="tabular-nums">{value}</span>
    </span>
  );
}

function SalesHandoffPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = [...rows]
    .sort((a, b) => (directBuyers(b) ?? 0) - (directBuyers(a) ?? 0))
    .slice(0, 4);

  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-[#2d2b28]">Sales Motions Kept Separate</h2>
          <p className="mt-1 text-xs leading-5 text-[#66635f]">
            These rows can win final credit after a magnet warms the buyer. They stay out of the command table.
          </p>
        </div>
        <Filter className="h-4 w-4 shrink-0 text-[#66635f]" aria-hidden />
      </div>

      <div className="mt-4 space-y-2">
        {visibleRows.map((row) => (
          <div key={rowKey(row)} className="rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="truncate text-sm font-semibold text-[#2d2b28]">
                  {stringValue(row.lead_magnet_reporting_name) ?? "Sales motion"}
                </div>
                <div className="mt-1 text-[11px] text-[#66635f]">
                  {formatNumber(directBuyers(row))} buyers · {formatCurrency(numberValue(row.direct_credit_net_revenue))}
                </div>
              </div>
              <span className={`rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[statusTone(row)]}`}>
                {statusLabel(row)}
              </span>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function MetricInspector({ data }: { data: DashboardData }) {
  return (
    <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold text-[#2d2b28]">Metric Inspector</h2>
          <p className="mt-1 text-xs leading-5 text-[#66635f]">
            Source, scope, and caveats for the main dashboard. This keeps definitions visible without crowding the table.
          </p>
        </div>
        <HelpCircle className="h-4 w-4 shrink-0 text-[#66635f]" aria-hidden />
      </div>

      <div className="mt-4 grid gap-2 md:grid-cols-2">
        <DefinitionCard
          title="Influenced Buyers"
          body="Buyers with a lead-magnet signal before purchase. This is nonexclusive influence, not final credit."
          source="lead_magnet_influence_summary.expanded_touch_buyers"
        />
        <DefinitionCard
          title="Direct Credit"
          body="Buyers where the true lead magnet kept latest-touch final credit."
          source="latest_touch_true_buyers / direct_credit_buyers"
        />
        <DefinitionCard
          title="Sales Handoff"
          body="Buyers influenced by a magnet but later credited to a sales pipeline motion."
          source="expanded_buyers_later_credited_to_sales_pipeline"
        />
        <DefinitionCard
          title="Booked Calls"
          body="Call bookings tied to the lead-magnet path in the selected buyer window."
          source="range_booked_calls"
        />
      </div>

      {data.dataContract ? (
        <details className="mt-3 rounded-lg border border-[#ece9e1] bg-[#fbfaf7]">
          <summary className="cursor-pointer px-3 py-2 text-xs font-semibold text-[#0f766e]">
            Source tables and caveat
          </summary>
          <div className="border-t border-[#ece9e1] px-3 py-3 text-xs leading-5 text-[#66635f]">
            <div className="font-semibold text-[#2d2b28]">
              {data.dataContract.projectId}.{data.dataContract.dataset}
            </div>
            <div className="mt-1">{data.dataContract.tables.join(", ")}</div>
            <div className="mt-2">{data.dataContract.note}</div>
          </div>
        </details>
      ) : null}
    </section>
  );
}

function DefinitionCard({ title, body, source }: { title: string; body: string; source: string }) {
  return (
    <article className="rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3">
      <div className="text-sm font-semibold text-[#2d2b28]">{title}</div>
      <p className="mt-1 text-[11px] leading-4 text-[#66635f]">{body}</p>
      <div className="mt-2 rounded-md bg-white px-2 py-1 font-mono text-[10px] text-[#66635f]">
        {source}
      </div>
    </article>
  );
}

function MagnetJourneyDrawer({
  row,
  filters,
  freshnessDetail,
  onClose,
}: {
  row: DashboardRow | null;
  filters: DashboardFilters;
  freshnessDetail: string;
  onClose: () => void;
}) {
  if (!row) return null;

  const name = stringValue(row.lead_magnet_reporting_name) ?? "Lead magnet";
  const read = rowRead(row);
  const confidence = confidenceRead(row);
  const decision = decisionForRow(row);
  const move = nextMove(row);
  const buyers = influencedBuyers(row);
  const direct = directBuyers(row);
  const sales = salesBuyers(row);
  const otherCredit = Math.max((buyers ?? 0) - (direct ?? 0) - (sales ?? 0), 0);
  const net = numberValue(row.influence_nonexclusive_buyer_revenue);
  const firstSeen = dateValue(row.first_seen_at);
  const latestSeen = dateValue(row.latest_seen_at);

  return (
    <div className="fixed inset-0 z-50">
      <button
        type="button"
        aria-label="Close magnet journey"
        className="absolute inset-0 bg-[#2d2b28]/30"
        onClick={onClose}
      />
      <aside className="absolute right-0 top-0 flex h-full w-full max-w-xl flex-col overflow-y-auto border-l border-[#dedbd2] bg-[#fbfaf7] shadow-2xl">
        <div className="sticky top-0 z-10 border-b border-[#dedbd2] bg-white p-4">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs font-semibold uppercase text-[#0f766e]">Magnet journey</p>
              <h2 className="mt-1 truncate text-2xl font-semibold tracking-normal text-[#171717]">{name}</h2>
              <p className="mt-1 text-xs text-[#66635f]">
                {stringValue(row.offer_type_label) ?? "Unknown type"} · {categoryLabel(row.lead_magnet_category)}
              </p>
            </div>
            <button
              type="button"
              aria-label="Close"
              className="rounded-md border border-[#dedbd2] bg-white p-2 text-[#66635f] hover:bg-[#f3f1ea] hover:text-[#2d2b28]"
              onClick={onClose}
            >
              <X className="h-4 w-4" aria-hidden />
            </button>
          </div>
        </div>

        <div className="space-y-3 p-4">
          <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
            <div className="flex flex-wrap items-center gap-2">
              <span className={`rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[decision.tone]}`}>
                {decision.label}
              </span>
              <span className={`rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[confidence.tone]}`}>
                {confidence.label}
              </span>
              <span className={`rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[read.tone]}`}>
                {read.label}
              </span>
              <span className={`rounded-md border px-2 py-0.5 text-[10px] font-semibold ${tonePill[statusTone(row)]}`}>
                {statusLabel(row)}
              </span>
            </div>
            <p className="mt-3 text-sm leading-6 text-[#66635f]">
              {journeyNarrative(row)}
            </p>
            <div className={`mt-3 rounded-xl border border-l-4 p-3 ${nextMoveSurface[move.tone]} ${toneLeftBorder[move.tone]}`}>
              <div className="flex items-center gap-2">
                <span className={`h-2.5 w-2.5 rounded-full ${toneDot[move.tone]}`} />
                <div className="text-[10px] font-semibold uppercase text-[#66635f]">
                  {move.signal}
                </div>
              </div>
              <div className="mt-1 text-base font-semibold text-[#2d2b28]">{move.label}</div>
              <p className="mt-1 text-xs leading-5 text-[#3b3936]">{move.helper}</p>
            </div>
            <div className="mt-4 rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3">
              <div className="text-[11px] font-semibold uppercase text-[#66635f]">
                Simple journey
              </div>
              <div className="mt-3 space-y-3">
                <JourneyStep
                  label="Grabbed magnet"
                  value={formatNumber(leadCount(row))}
                  helper={leadSourceLabel(row)}
                  tone="neutral"
                />
                <JourneyStep
                  label="Booked call"
                  value={formatNumber(bookedCalls(row))}
                  helper={`${formatPercent(safeDivide(bookedCalls(row), leadCount(row)))} lead-to-call`}
                  tone="blue"
                />
                <JourneyStep
                  label="Sales handoff"
                  value={formatNumber(sales)}
                  helper={`${formatPercent(safeDivide(sales, buyers))} of influenced buyers`}
                  tone="amber"
                />
                <JourneyStep
                  label="Buyer influence"
                  value={formatNumber(buyers)}
                  helper={formatCurrency(net)}
                  tone="green"
                />
                <JourneyStep
                  label="Direct credit"
                  value={formatNumber(direct)}
                  helper={`${formatCurrency(numberValue(row.direct_credit_net_revenue))} latest-touch net`}
                  tone="neutral"
                />
              </div>
            </div>
          </section>

          <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
            <div className="text-[11px] font-semibold uppercase text-[#66635f]">Credit landing</div>
            <div className="mt-3 space-y-3">
              <DrawerPathRow label="Kept direct credit" value={direct} total={buyers} tone="green" />
              <DrawerPathRow label="Moved to sales handoff" value={sales} total={buyers} tone="amber" />
              {otherCredit > 0 ? (
                <DrawerPathRow label="Other final credit" value={otherCredit} total={buyers} tone="neutral" />
              ) : null}
            </div>
          </section>

          <section className="rounded-xl border border-[#dedbd2] bg-white p-4 shadow-sm">
            <div className="text-[11px] font-semibold uppercase text-[#66635f]">Source proof</div>
            <div className={`mt-3 rounded-lg border p-3 ${tonePill[confidence.tone]}`}>
              <div className="text-[10px] font-semibold uppercase">Proof confidence</div>
              <div className="mt-1 text-sm font-semibold">{confidence.label}</div>
              <p className="mt-1 text-xs leading-5">{confidence.helper}</p>
            </div>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              <DrawerFact label="Form-touch buyers" value={formatNumber(numberValue(row.influence_form_touch_buyers))} />
              <DrawerFact label="Opportunity buyers" value={formatNumber(numberValue(row.influence_opportunity_touch_buyers))} />
              <DrawerFact label="Source-only buyers" value={formatNumber(numberValue(row.influence_source_only_buyers))} />
              <DrawerFact label="Direct-credit revenue" value={formatCurrency(numberValue(row.direct_credit_net_revenue))} />
              <DrawerFact label="First seen" value={formatDate(firstSeen)} />
              <DrawerFact label="Latest seen" value={formatDate(latestSeen)} />
            </div>
            <div className="mt-3 rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3 text-xs leading-5 text-[#66635f]">
              <div><span className="font-semibold text-[#2d2b28]">Range:</span> {filters.timeRangeLabel}</div>
              <div><span className="font-semibold text-[#2d2b28]">Freshness:</span> {freshnessDetail}</div>
              <div><span className="font-semibold text-[#2d2b28]">Caveat:</span> Influence is nonexclusive. Direct credit is latest-touch and exclusive.</div>
            </div>
          </section>
        </div>
      </aside>
    </div>
  );
}

function DrawerFact({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-[#ece9e1] bg-[#fbfaf7] p-3">
      <div className="text-[10px] font-semibold uppercase text-[#66635f]">{label}</div>
      <div className="mt-1 text-base font-semibold text-[#2d2b28]">{value}</div>
    </div>
  );
}

function JourneyStep({
  label,
  value,
  helper,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  tone: Tone;
}) {
  return (
    <div className="grid grid-cols-[1rem_minmax(0,1fr)_auto] items-start gap-3">
      <span className={`mt-1 h-2.5 w-2.5 rounded-full ${toneDot[tone]}`} />
      <div className="min-w-0">
        <div className="text-sm font-semibold text-[#2d2b28]">{label}</div>
        <div className="mt-0.5 truncate text-[11px] text-[#66635f]">{helper}</div>
      </div>
      <div className="text-sm font-semibold tabular-nums text-[#2d2b28]">{value}</div>
    </div>
  );
}

function DrawerPathRow({
  label,
  value,
  total,
  tone,
}: {
  label: string;
  value: number | null;
  total: number | null;
  tone: Tone;
}) {
  const share = safeDivide(value, total);

  return (
    <div>
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-sm font-semibold text-[#2d2b28]">{label}</div>
          <div className="mt-0.5 text-[11px] text-[#66635f]">{formatPercent(share)} of influenced buyers</div>
        </div>
        <div className="text-sm font-semibold tabular-nums text-[#2d2b28]">{formatNumber(value)}</div>
      </div>
      <div className="mt-2 h-2 rounded-sm bg-[#ece9e1]">
        <div className={`h-2 rounded-sm ${toneDot[tone]}`} style={{ width: `${share ? Math.max(3, share * 100) : 0}%` }} />
      </div>
    </div>
  );
}

function EmptyState({ title, body }: { title: string; body: string }) {
  return (
    <div className="rounded-lg border border-dashed border-[#dedbd2] bg-[#fbfaf7] p-6 text-center">
      <div className="text-sm font-semibold text-[#2d2b28]">{title}</div>
      <p className="mx-auto mt-1 max-w-md text-xs leading-5 text-[#66635f]">{body}</p>
    </div>
  );
}

function buildTotals(rows: DashboardRow[]): Totals {
  return {
    magnets: rows.length,
    leads: sumRows(rows, leadCount),
    booked: sumRows(rows, bookedCalls),
    buyers: sumRows(rows, influencedBuyers),
    direct: sumRows(rows, directBuyers),
    sales: sumRows(rows, salesBuyers),
    net: sumRows(rows, (row) => numberValue(row.influence_nonexclusive_buyer_revenue)),
  };
}

function buildCommonMoveInsight(rows: DashboardRow[]): { move: ReturnType<typeof nextMove>; count: number; total: number } | null {
  if (rows.length < 2) return null;

  const moves = new Map<string, { move: ReturnType<typeof nextMove>; count: number }>();
  rows.forEach((row) => {
    const move = nextMove(row);
    const current = moves.get(move.label);
    if (current) {
      current.count += 1;
      return;
    }
    moves.set(move.label, { move, count: 1 });
  });

  const topMove = Array.from(moves.values()).sort((a, b) => b.count - a.count)[0];
  const threshold = Math.max(2, Math.ceil(rows.length * 0.35));

  if (!topMove || topMove.count < threshold) return null;

  return {
    move: topMove.move,
    count: topMove.count,
    total: rows.length,
  };
}

function buildGravityPoints(rows: DashboardRow[]) {
  const thresholds = {
    calls: Math.max(medianPositive(rows.map((row) => bookedCalls(row) ?? 0)) ?? 1, 1),
    buyers: Math.max(medianPositive(rows.map((row) => influencedBuyers(row) ?? 0)) ?? 1, 1),
  };
  const maxNet = Math.max(...rows.map((row) => numberValue(row.influence_nonexclusive_buyer_revenue) ?? 0), 1);
  const points = rows.map((row) => {
    const leads = leadCount(row) ?? 0;
    const calls = bookedCalls(row) ?? 0;
    const buyers = influencedBuyers(row) ?? 0;
    const net = numberValue(row.influence_nonexclusive_buyer_revenue) ?? 0;

    return {
      id: rowKey(row),
      name: rowName(row),
      type: stringValue(row.offer_type_label) ?? categoryLabel(row.lead_magnet_category),
      leads,
      calls,
      buyers,
      net,
      size: Math.max(net / maxNet, 0.08),
      zone: gravityZone(calls, buyers, thresholds),
      row,
    };
  });

  return { points, thresholds };
}

function gravityZone(
  calls: number,
  buyers: number,
  thresholds: { calls: number; buyers: number },
): GravityZoneKey {
  const strongCalls = calls >= thresholds.calls;
  const strongBuyers = buyers >= thresholds.buyers;

  if (strongCalls && strongBuyers) return "proven";
  if (strongCalls) return "callRich";
  if (strongBuyers) return "quietClosers";
  return "lowPull";
}

function buildTypeGroups(rows: DashboardRow[]): TypeGroup[] {
  const groups = new Map<string, DashboardRow[]>();

  rows.forEach((row) => {
    const key = typeKey(row);
    groups.set(key, [...(groups.get(key) ?? []), row]);
  });

  return Array.from(groups.entries())
    .map(([key, groupRows]) => {
      const totals = buildTotals(groupRows);
      const best = rankRowsForLens(groupRows, "buyers")[0] ?? null;
      return {
        key,
        label: typeLabel(groupRows[0]),
        rows: groupRows,
        ...totals,
        best,
      };
    })
    .sort((a, b) => b.buyers - a.buyers || b.booked - a.booked || b.net - a.net);
}

function buildDecisionPicks(rows: DashboardRow[]): DecisionPick[] {
  // Pick distinct magnets across the three slots. A single magnet that wins
  // all three metrics should not occupy all three cards — fall back to the
  // next-best candidate so each pick says something different.
  const taken = new Set<string>();
  const pickBest = (
    pool: DashboardRow[],
    score: (row: DashboardRow) => number,
  ): DashboardRow | null => {
    const scored = pool
      .map((row) => ({ row, value: score(row), key: rowKey(row) }))
      .filter((entry) => entry.value > 0)
      .sort((a, b) => b.value - a.value);
    const next = scored.find((entry) => !taken.has(entry.key));
    if (next) taken.add(next.key);
    return next?.row ?? null;
  };

  const updateExisting = pickBest(rows, (row) => followupScore(row));
  const bestCall = pickBest(rows, (row) => bookedCalls(row) ?? 0);
  const bestBuyer = pickBest(rows, (row) => influencedBuyers(row) ?? 0);

  return [
    {
      title: "Top fix",
      label: updateExisting ? rowName(updateExisting) : "Nothing flagged",
      helper: updateExisting
        ? nextMove(updateExisting).helper
        : "No update candidate in this range.",
      row: updateExisting,
      tone: "blue" as const,
    },
    {
      title: "Top call driver",
      label: bestCall ? rowName(bestCall) : "No call signal",
      helper: bestCall
        ? `${formatNumber(bookedCalls(bestCall))} booked calls · ${formatPercent(safeDivide(bookedCalls(bestCall), leadCount(bestCall)))} lead-to-call`
        : "No booked-call signal in this range.",
      row: bestCall,
      tone: "blue" as const,
    },
    {
      title: "Top buyer driver",
      label: bestBuyer ? rowName(bestBuyer) : "No buyer proof yet",
      helper: bestBuyer
        ? `${formatNumber(influencedBuyers(bestBuyer))} influenced buyers · ${formatCurrency(numberValue(bestBuyer?.influence_nonexclusive_buyer_revenue))}`
        : "No buyer proof in this range.",
      row: bestBuyer,
      tone: "green" as const,
    },
  ];
}

function groupTableRows(rows: TableRow<DashboardRow>[]) {
  return DECISION_GROUPS.map((definition) => ({
    definition,
    rows: rows.filter((row) => decisionForRow(row.original).key === definition.key),
  })).filter((group) => group.rows.length > 0);
}

function groupBenchRows(rows: DashboardRow[]) {
  return BENCH_GROUPS.map((definition) => ({
    definition,
    rows: rows.filter((row) => benchForRow(row).key === definition.key),
  })).filter((group) => group.rows.length > 0);
}

function benchForRow(row: DashboardRow): BenchDefinition {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;

  if (buyers === 0 && (calls > 0 || leads >= 50)) {
    return BENCH_GROUPS.find((group) => group.key === "tracking") ?? BENCH_GROUPS[0];
  }

  if (buyers > 0) {
    return BENCH_GROUPS.find((group) => group.key === "proven") ?? BENCH_GROUPS[0];
  }

  if (leads > 0 || calls > 0) {
    return BENCH_GROUPS.find((group) => group.key === "watchlist") ?? BENCH_GROUPS[0];
  }

  return BENCH_GROUPS.find((group) => group.key === "dormant") ?? BENCH_GROUPS[0];
}

function priorityReason(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const sales = salesBuyers(row) ?? 0;
  const direct = directBuyers(row) ?? 0;
  const net = numberValue(row.influence_nonexclusive_buyer_revenue) ?? 0;

  if (buyers > 0 && net > 0) return "Buyer proof";
  if (calls > buyers && calls > 0) return "Calls ahead";
  if (sales > direct && sales > 0) return "Sales assist";
  if (leads >= 50 && buyers === 0) return "Tracking gap";
  if (leads > 0) return "Demand signal";
  return "Low signal";
}

function commandViewLabel(view: CommandView) {
  switch (view) {
    case "priority":
      return "Priority 10";
    case "bench":
      return "Bench";
    case "audit":
      return "Audit All";
  }
}

function decisionForRow(row: DashboardRow): DecisionDefinition {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const direct = directBuyers(row) ?? 0;
  const sales = salesBuyers(row) ?? 0;

  if (buyers >= 25 && sales <= direct) return DECISION_BY_KEY.adjacent;
  if (buyers > 0 && calls > 0 && sales <= direct) return DECISION_BY_KEY.scale;
  if (buyers >= 25 && sales > direct) return DECISION_BY_KEY.update;
  if (calls > buyers && calls > 0) return DECISION_BY_KEY.update;
  if (leads >= 50 && calls === 0) return DECISION_BY_KEY.update;
  if (buyers === 0 && (leads > 0 || calls > 0)) return DECISION_BY_KEY.tracking;
  if (buyers > 0) return DECISION_BY_KEY.scale;
  return DECISION_BY_KEY.low;
}

function filterRows(rows: DashboardRow[], lens: Lens, search: string) {
  const normalizedSearch = search.trim().toLowerCase();

  return rows.filter((row) => {
    const textMatches =
      normalizedSearch.length === 0 ||
      [row.lead_magnet_reporting_name, row.offer_type_label, row.lead_magnet_category]
        .map((value) => stringValue(value)?.toLowerCase() ?? "")
        .some((value) => value.includes(normalizedSearch));

    if (!textMatches) return false;

    const leads = leadCount(row) ?? 0;
    const calls = bookedCalls(row) ?? 0;
    const buyers = influencedBuyers(row) ?? 0;
    const sales = salesBuyers(row) ?? 0;

    switch (lens) {
      case "calls":
        return calls > 0;
      case "buyers":
        return buyers > 0;
      case "sales":
        return sales > 0;
      case "followup":
        return (calls > buyers && calls > 0) || (leads >= 50 && calls === 0) || sales > 0;
      case "proof":
        return buyers === 0 && (leads > 0 || calls > 0);
      case "all":
      default:
        return true;
    }
  });
}

function rankRowsForLens(rows: DashboardRow[], lens: Lens) {
  return [...rows].sort((a, b) => {
    const comparisons =
      lens === "calls"
        ? [
            (bookedCalls(b) ?? 0) - (bookedCalls(a) ?? 0),
            (safeDivide(bookedCalls(b), leadCount(b)) ?? 0) - (safeDivide(bookedCalls(a), leadCount(a)) ?? 0),
            (influencedBuyers(b) ?? 0) - (influencedBuyers(a) ?? 0),
          ]
        : lens === "buyers"
          ? [
              (influencedBuyers(b) ?? 0) - (influencedBuyers(a) ?? 0),
              (numberValue(b.influence_nonexclusive_buyer_revenue) ?? 0) -
                (numberValue(a.influence_nonexclusive_buyer_revenue) ?? 0),
              (directBuyers(b) ?? 0) - (directBuyers(a) ?? 0),
            ]
          : lens === "sales"
            ? [
                (salesBuyers(b) ?? 0) - (salesBuyers(a) ?? 0),
                (safeDivide(salesBuyers(b), influencedBuyers(b)) ?? 0) -
                  (safeDivide(salesBuyers(a), influencedBuyers(a)) ?? 0),
                (influencedBuyers(b) ?? 0) - (influencedBuyers(a) ?? 0),
              ]
            : lens === "followup"
              ? [followupScore(b) - followupScore(a), (leadCount(b) ?? 0) - (leadCount(a) ?? 0)]
              : lens === "proof"
                ? [proofScore(b) - proofScore(a), (leadCount(b) ?? 0) - (leadCount(a) ?? 0)]
                : [
                    (influencedBuyers(b) ?? 0) - (influencedBuyers(a) ?? 0),
                    (bookedCalls(b) ?? 0) - (bookedCalls(a) ?? 0),
                    (numberValue(b.influence_nonexclusive_buyer_revenue) ?? 0) -
                      (numberValue(a.influence_nonexclusive_buyer_revenue) ?? 0),
                  ];

    return comparisons.find((comparison) => comparison !== 0) ?? rowName(a).localeCompare(rowName(b));
  });
}

function rowRead(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const direct = directBuyers(row) ?? 0;
  const sales = salesBuyers(row) ?? 0;

  if (calls > buyers && calls > 0) {
    return {
      label: "Creating calls",
      helper: `${formatNumber(calls - buyers)} calls ahead of buyer proof`,
      tone: "blue" as const,
    };
  }

  if (buyers > 0 && sales > direct) {
    return {
      label: "Helping sales",
      helper: `${formatPercent(safeDivide(sales, buyers))} sales handoff`,
      tone: "amber" as const,
    };
  }

  if (buyers > 0) {
    return {
      label: "Creating buyers",
      helper: `${formatNumber(buyers)} influenced buyers`,
      tone: "green" as const,
    };
  }

  if (leads > 0) {
    return {
      label: "Needs proof",
      helper: "Demand exists, buyer proof missing",
      tone: "neutral" as const,
    };
  }

  return {
    label: "Too early",
    helper: "Not enough signal",
    tone: "neutral" as const,
  };
}

function confidenceRead(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const formTouchBuyers = numberValue(row.influence_form_touch_buyers) ?? 0;
  const opportunityBuyers = numberValue(row.influence_opportunity_touch_buyers) ?? 0;
  const sourceOnlyBuyers = numberValue(row.influence_source_only_buyers) ?? 0;

  if (buyers > 0 && (formTouchBuyers > 0 || opportunityBuyers > 0)) {
    return {
      label: "Strong proof",
      helper: `${formatNumber(formTouchBuyers + opportunityBuyers)} buyers from form/opportunity evidence`,
      tone: "green" as const,
    };
  }

  if (buyers > 0) {
    return {
      label: "Partial proof",
      helper:
        sourceOnlyBuyers > 0
          ? `${formatNumber(sourceOnlyBuyers)} source-only buyers`
          : `${formatNumber(buyers)} influenced buyers`,
      tone: "blue" as const,
    };
  }

  if (leads > 0 || calls > 0) {
    return {
      label: "Needs source check",
      helper: `${formatNumber(leads)} leads and ${formatNumber(calls)} calls with no buyer proof`,
      tone: "amber" as const,
    };
  }

  return {
    label: "No proof yet",
    helper: "No lead, call, or buyer signal in this view",
    tone: "neutral" as const,
  };
}

function confidenceRank(row: DashboardRow) {
  const confidence = confidenceRead(row).label;
  if (confidence === "Strong proof") return 4000 + (influencedBuyers(row) ?? 0);
  if (confidence === "Partial proof") return 3000 + (influencedBuyers(row) ?? 0);
  if (confidence === "Needs source check") return 2000 + (leadCount(row) ?? 0);
  return leadCount(row) ?? 0;
}

function nextMove(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const direct = directBuyers(row) ?? 0;
  const sales = salesBuyers(row) ?? 0;
  const leadToCall = safeDivide(calls, leads) ?? 0;

  if (buyers === 0 && leads >= 50 && calls === 0) {
    return {
      label: "Add call bridge",
      helper: "Demand is visible, but the next step is not producing calls.",
      tone: "blue" as const,
      signal: "Demand",
    };
  }

  if (buyers === 0 && calls > 0) {
    return {
      label: "Verify buyer proof",
      helper: "Demand exists, but buyer attribution is thin. Check source joins before judging the magnet.",
      tone: "amber" as const,
      signal: "Tracking",
    };
  }

  if (buyers >= 25 && sales > direct) {
    return {
      label: "Make the sales bridge explicit",
      helper: "This magnet warms people up. Make the opt-in to sales path visible and measurable.",
      tone: "amber" as const,
      signal: "Sales assist",
    };
  }

  if (calls >= 25 && calls > buyers * 2) {
    return {
      label: "Tighten call-to-buyer",
      helper: "Call pull is strong. Improve show, follow-up, offer fit, or handoff quality.",
      tone: "blue" as const,
      signal: "Call pull",
    };
  }

  if (buyers >= 25) {
    return {
      label: "Create adjacent",
      helper: "Buyer proof is real. Test a sibling angle instead of rebuilding from scratch.",
      tone: "green" as const,
      signal: "Buyer proof",
    };
  }

  if (buyers > 0 && calls === 0) {
    return {
      label: "Lift distribution",
      helper: "Buyer proof exists without much call volume. Give it more traffic or a clearer CTA.",
      tone: "green" as const,
      signal: "Quiet closer",
    };
  }

  if (buyers > 0 && sales > direct) {
    return {
      label: "Name the handoff",
      helper: "Sales is capturing credit after influence. Make that bridge explicit in follow-up.",
      tone: "amber" as const,
      signal: "Sales assist",
    };
  }

  if (buyers > 0) {
    return {
      label: leadToCall >= 0.12 ? "Refine the path" : "Protect and test",
      helper:
        leadToCall >= 0.12
          ? "Buyer proof and call intent both exist. Test one path improvement at a time."
          : "There is buyer proof. Improve around the path without burying what works.",
      tone: "green" as const,
      signal: "Proof",
    };
  }

  if (leads > 0) {
    return {
      label: "Keep nurturing",
      helper: "Let more signal build while tightening nurture and proof collection.",
      tone: "neutral" as const,
      signal: "Early signal",
    };
  }

  return {
    label: "Keep watching",
    helper: "Low signal. Do not overwork this yet.",
    tone: "neutral" as const,
    signal: "Low signal",
  };
}

function journeyNarrative(row: DashboardRow) {
  const read = rowRead(row);
  const leads = leadCount(row);
  const calls = bookedCalls(row);
  const buyers = influencedBuyers(row);
  const sales = salesBuyers(row);
  const direct = directBuyers(row);

  return `${read.label}: ${formatNumber(leads)} leads, ${formatNumber(calls)} booked calls, ${formatNumber(buyers)} influenced buyers, ${formatNumber(direct)} direct-credit buyers, and ${formatNumber(sales)} sales handoff buyers.`;
}

function statusLabel(row: DashboardRow) {
  switch (stringValue(row.queue_status)) {
    case "healthy":
      return "Healthy";
    case "repair_candidate":
      return "Optimize";
    case "kill_candidate":
      return "Review low direct";
    case "retire_recommended_pending_override":
      return "Deep review";
    case "insufficient_sample":
      return "Still gathering";
    default:
      return "Unclassified";
  }
}

function statusTone(row: DashboardRow): Tone {
  switch (stringValue(row.queue_status)) {
    case "healthy":
      return "green";
    case "repair_candidate":
      return "amber";
    case "kill_candidate":
      return "amber";
    case "retire_recommended_pending_override":
      return "red";
    default:
      return "neutral";
  }
}

function categoryLabel(value: DashboardRowValue | undefined) {
  const raw = stringValue(value);
  switch (raw) {
    case "true_lead_magnet":
      return "True lead magnet";
    case "launch_event":
      return "Launch / event";
    case "waitlist":
      return "Waitlist";
    case "uncategorized":
      return "Uncategorized";
    default:
      return raw ? raw.replace(/_/g, " ") : "Unknown";
  }
}

function typeKey(row: DashboardRow) {
  return (stringValue(row.offer_type_label) ?? categoryLabel(row.lead_magnet_category)).toLowerCase();
}

function typeLabel(row: DashboardRow | undefined) {
  if (!row) return "Unknown";
  return stringValue(row.offer_type_label) ?? categoryLabel(row.lead_magnet_category);
}

function rowKey(row: DashboardRow | null | undefined) {
  if (!row) return "";
  return stringValue(row.lead_magnet_id) ?? rowName(row);
}

function rowName(row: DashboardRow | null | undefined) {
  return stringValue(row?.lead_magnet_reporting_name) ?? "N/A";
}

function leadSourceLabel(row: DashboardRow) {
  const formLeads = numberValue(row.influence_form_leads);
  return formLeads !== null && formLeads > 0 ? "form ledger" : "mart opt-ins";
}

function leadCount(row: DashboardRow | null | undefined) {
  if (!row) return null;
  const formLeads = numberValue(row.influence_form_leads);
  if (formLeads !== null && formLeads > 0) return formLeads;
  return numberValue(row.opt_in_lead_count);
}

function bookedCalls(row: DashboardRow | null | undefined) {
  if (!row) return null;
  return numberValue(row.range_booked_calls) ?? numberValue(row.booking_lead_count);
}

function influencedBuyers(row: DashboardRow | null | undefined) {
  if (!row) return null;
  return numberValue(row.influence_buyers_before_purchase);
}

function directBuyers(row: DashboardRow | null | undefined) {
  if (!row) return null;
  return numberValue(row.direct_credit_buyers) ?? numberValue(row.influence_direct_credit_buyers);
}

function salesBuyers(row: DashboardRow | null | undefined) {
  if (!row) return null;
  return numberValue(row.influence_assisted_to_sales_pipeline_buyers);
}

function pathScore(row: DashboardRow) {
  return (leadCount(row) ?? 0) + (bookedCalls(row) ?? 0) * 5 + (influencedBuyers(row) ?? 0) * 20;
}

function followupScore(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  const sales = salesBuyers(row) ?? 0;
  if (calls > buyers && calls > 0) return 100000 + calls - buyers;
  if (leads >= 50 && calls === 0) return 50000 + leads;
  if (sales > 0) return 10000 + sales;
  return 0;
}

function proofScore(row: DashboardRow) {
  const leads = leadCount(row) ?? 0;
  const calls = bookedCalls(row) ?? 0;
  const buyers = influencedBuyers(row) ?? 0;
  if (buyers > 0) return 0;
  return leads + calls * 5;
}

function sumRows(rows: DashboardRow[], value: (row: DashboardRow) => number | null) {
  return rows.reduce((sum, row) => sum + (value(row) ?? 0), 0);
}

function maxBy(rows: DashboardRow[], value: (row: DashboardRow) => number) {
  return rows.reduce<DashboardRow | null>((best, row) => {
    if (!best) return row;
    return value(row) > value(best) ? row : best;
  }, null);
}

function medianPositive(values: number[]) {
  const positive = values
    .filter((value) => Number.isFinite(value) && value > 0)
    .sort((a, b) => a - b);

  if (positive.length === 0) return null;

  const middle = Math.floor(positive.length / 2);
  if (positive.length % 2 === 1) return positive[middle];

  return (positive[middle - 1] + positive[middle]) / 2;
}

function headerClass(columnId: string) {
  if (columnId === "value") return "text-right";
  if (columnId === "next") return "min-w-64";
  if (columnId === "magnet") return "min-w-72";
  if (columnId === "proof") return "min-w-44";
  return "";
}

function cellClass(columnId: string) {
  if (columnId === "value") return "text-right";
  if (columnId === "magnet") return "min-w-72";
  if (columnId === "next") return "min-w-64";
  if (columnId === "proof") return "min-w-44";
  return "";
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

function formatDate(value: Date | null) {
  if (!value) return "N/A";
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
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

function dateValue(value: DashboardRowValue | undefined) {
  const raw = stringValue(value);
  if (!raw) return null;
  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}
