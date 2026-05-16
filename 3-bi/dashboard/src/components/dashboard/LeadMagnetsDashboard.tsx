import Link from "next/link";

import {
  RECENT_BUYER_WINDOW_DAYS,
  TONE_SCALES,
  formatCurrencyCompact,
  formatCurrencyFull,
  formatNumber,
  numberValue,
  stringValue,
  toneForValue,
} from "@/lib/config/lead-magnet-tokens";
import type { DashboardData, DashboardRow } from "@/types/dashboard-data";

/**
 * Lead Magnets dashboard — front page.
 *
 * Editorial surface anchored on the question "Where did the buyer revenue
 * come from this period?" Mirrors the Speed-to-Lead anatomy 1:1 — same outer
 * container scope (.stl-page stl-od-page), same hero shape (od-kpi.hero-sla),
 * same operational strip (.stl-waiting-strip), same table density
 * (.od-panel rep-summary-table). The strip-hard pass (Slice 9, 2026-05-16)
 * cut the 4-line hero to one number + verdict, cut 5 leaderboard columns to 3,
 * cut 3 strip pills to 2, and deleted the bespoke .lm-page theme.
 *
 * Reference spec: 00 Human/30 Projects/Lead Magnet Redesign/Lead Magnet Spec v1.md
 */
export function LeadMagnetsDashboard({ data }: { data: DashboardData }) {
  const summary = (data.rows.lead_magnet_summary ?? [])[0];
  const concentration = (data.rows.lead_magnet_revenue_concentration ?? [])[0];
  const recentActivity = (data.rows.lead_magnet_recent_activity ?? [])[0];
  const attributionBreakdown = (data.rows.lead_magnet_attribution_breakdown ?? [])[0];
  const performanceRows = data.rows.lead_magnet_performance_rows ?? [];

  return (
    <div className="stl-page stl-od-page lm-page space-y-5 pb-12">
      <div className="od-page-head">
        <div className="od-title">
          <h1>Lead Magnets</h1>
          <p className="od-blurb">
            {data.filters.timeRangeLabel} · where the buyer revenue came from
          </p>
        </div>
        <LeadMagnetRangePicker current={data.filters.timeRange} />
      </div>

      {data.error ? (
        <div className="od-callout od-callout-danger">{data.error}</div>
      ) : null}

      <RecentlyWorkingStrip activity={recentActivity} />

      <MagnetRevenueHero
        summary={summary}
        concentration={concentration}
        rangeLabel={data.filters.timeRangeLabel}
      />

      <MagnetLeaderboard
        rows={performanceRows}
        attributionBreakdown={attributionBreakdown}
      />
    </div>
  );
}

// ------------------------------------------------------------------
// RecentlyWorkingStrip — operational pulse, fixed 7-day window.
// Reuses Speed-to-Lead's .stl-waiting-strip / .stl-waiting-pill classes
// directly so the strip is visually identical across the two pages.
//
// Reference spec §5.1.
// ------------------------------------------------------------------

function RecentlyWorkingStrip({ activity }: { activity: DashboardRow | undefined }) {
  const newBuyers = numberValue(activity?.new_buyers) ?? 0;
  const attributedBuyers = numberValue(activity?.attributed_buyers) ?? 0;
  const unattributedBuyers = numberValue(activity?.unattributed_buyers) ?? 0;

  if (newBuyers === 0) {
    return (
      <div className="stl-waiting-strip is-empty">
        <span className="stl-waiting-headline">
          <span className="stl-waiting-bolt">⚡</span>
          No new buyers in the last {RECENT_BUYER_WINDOW_DAYS} days
        </span>
        <span className="stl-waiting-window-tag">last {RECENT_BUYER_WINDOW_DAYS}d</span>
      </div>
    );
  }

  return (
    <div className="stl-waiting-strip">
      <span className="stl-waiting-headline">
        <span className="stl-waiting-bolt">⚡</span>
        Last {RECENT_BUYER_WINDOW_DAYS} days · <strong>{formatNumber(newBuyers)}</strong> new
        buyers
      </span>
      <div className="stl-waiting-pills">
        <span className="stl-waiting-pill in-window">
          <span className="pill-count">{formatNumber(attributedBuyers)}</span>
          <span className="pill-label">attributed</span>
        </span>
        <span className="stl-waiting-pill past-one-hour">
          <span className="pill-count">{formatNumber(unattributedBuyers)}</span>
          <span className="pill-label">came from nowhere</span>
        </span>
      </div>
      <span className="stl-waiting-window-tag">last {RECENT_BUYER_WINDOW_DAYS}d</span>
    </div>
  );
}

// ------------------------------------------------------------------
// MagnetRevenueHero — one number + caption + tone-coded verdict.
//
// Mirrors HeroSLACard's anatomy:
//   - .label          "Buyer revenue"
//   - .value          "$51K"   (big metric)
//   - .hero-sub       "187 buyers · last 90 days"
//   - .hero-target    pareto window label
//   - .hero-verdict   "Healthy concentration · top 3 earned 92%"
//
// The cold-bench + attribution-honesty facts moved OUT to the leaderboard
// header subtitle (where they belong as anchors to the table that follows).
//
// Reference spec §5.2.
// ------------------------------------------------------------------

function MagnetRevenueHero({
  summary,
  concentration,
  rangeLabel,
}: {
  summary: DashboardRow | undefined;
  concentration: DashboardRow | undefined;
  rangeLabel: string;
}) {
  const totalRevenue = numberValue(summary?.total_net_revenue_after_refunds);
  const totalBuyers = numberValue(summary?.buyers);
  const totalAttributedRevenue = numberValue(concentration?.total_attributed_revenue);
  const top3Revenue = numberValue(concentration?.top_3_revenue);

  const top3Share =
    totalAttributedRevenue != null && totalAttributedRevenue > 0 && top3Revenue != null
      ? top3Revenue / totalAttributedRevenue
      : null;

  const verdictTone =
    top3Share == null
      ? "neutral"
      : top3Share >= TONE_SCALES.top_3_concentration.good
        ? "ok"
        : top3Share >= TONE_SCALES.top_3_concentration.warn
          ? "warning"
          : "danger";

  const verdictLabel = (() => {
    if (top3Share == null) return "Concentration unavailable";
    const sharePct = formatSharePercent(top3Share);
    if (verdictTone === "ok") return `Healthy concentration · top 3 earned ${sharePct}`;
    if (verdictTone === "warning") return `Flat distribution · top 3 earned ${sharePct}`;
    return `Spread thin · top 3 earned ${sharePct}`;
  })();

  const unattributedRevenue =
    totalRevenue != null && totalAttributedRevenue != null
      ? Math.max(0, totalRevenue - totalAttributedRevenue)
      : null;
  const unattributedShare =
    unattributedRevenue != null && totalRevenue != null && totalRevenue > 0
      ? unattributedRevenue / totalRevenue
      : null;
  const showAttributionGap =
    unattributedRevenue != null && unattributedRevenue > 0 && unattributedShare != null;

  return (
    <div className="od-kpi hero-sla">
      <span className="label">Buyer revenue</span>
      <div className="hero-row">
        <div className="value">{formatCurrencyCompact(totalRevenue)}</div>
      </div>
      <div className="hero-sub">
        {totalBuyers != null && totalBuyers > 0
          ? `${formatNumber(totalBuyers)} buyers · ${rangeLabel.toLowerCase()}`
          : "no buyers in this period"}
      </div>
      <div className="hero-row hero-footer">
        <span className="hero-target">Pareto check</span>
        <span className={`hero-verdict tone-${verdictTone}`}>{verdictLabel}</span>
      </div>
      {showAttributionGap ? (
        <div className="hero-attribution-gap">
          {formatCurrencyCompact(unattributedRevenue)} ({formatSharePercent(unattributedShare)}) of
          revenue had no prior magnet
        </div>
      ) : null}
    </div>
  );
}

function formatSharePercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  const pct = value * 100;
  if (pct >= 10) return `${Math.round(pct)}%`;
  return `${pct.toFixed(1)}%`;
}

// ------------------------------------------------------------------
// MagnetLeaderboard — 3 columns (Magnet · Revenue · $/lead).
//
// Reuses Speed-to-Lead's `.od-panel rep-summary-table` shell so the table
// reads at the same density across pages. Status encodes as a tiny chip in
// the magnet-name cell (subtitle), not as a separate column. Insufficient-
// sample rows pinned to the bottom under a separator.
//
// Reference spec §5.3.
// ------------------------------------------------------------------

type LeaderRow = {
  id: string;
  name: string;
  category: string | undefined;
  leadsLifetime: number;
  buyersLifetime: number;
  revenueLifetime: number;
  revenuePerLeadLifetime: number | null;
  timeToBuyerDays: number | null;
  lastSeenLabel: string;
  lastActivityRecent: boolean;
};

const DAY_MS = 24 * 60 * 60 * 1000;
const NINETY_DAYS_MS = 90 * DAY_MS;

const MONTH_NAMES = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

function formatLastSeen(raw: string | null | undefined): {
  label: string;
  recent: boolean;
} {
  if (!raw) return { label: "—", recent: false };
  const ts = Date.parse(raw);
  if (!Number.isFinite(ts)) return { label: "—", recent: false };
  const ageMs = Date.now() - ts;
  const days = Math.floor(ageMs / DAY_MS);
  const recent = ageMs <= NINETY_DAYS_MS;
  if (days <= 0) return { label: "today", recent };
  if (days === 1) return { label: "yesterday", recent };
  if (days <= 30) return { label: `${days}d ago`, recent };
  const date = new Date(ts);
  const month = MONTH_NAMES[date.getUTCMonth()] ?? "—";
  const year = date.getUTCFullYear();
  return { label: `${month} ${year}`, recent };
}

function formatConversionPercent(buyers: number, leads: number): string {
  if (leads <= 0 || buyers <= 0) return "—";
  const pct = (buyers / leads) * 100;
  if (pct >= 10) return `${Math.round(pct)}%`;
  if (pct >= 1) return `${pct.toFixed(1)}%`;
  return `${pct.toFixed(2)}%`;
}

function formatDaysToBuy(days: number | null): string {
  if (days == null || !Number.isFinite(days)) return "—";
  const rounded = Math.round(days);
  if (rounded <= 0) return "<1d";
  if (rounded >= 90) return "90d+";
  return `${rounded}d`;
}

function buildLeaderRows(rows: DashboardRow[]): LeaderRow[] {
  const mapped: LeaderRow[] = rows
    .map((row) => {
      const id = stringValue(row.lead_magnet_id);
      const name = stringValue(row.lead_magnet_reporting_name);
      if (!id || !name) return null;
      const { label, recent } = formatLastSeen(stringValue(row.last_activity_at));
      return {
        id,
        name,
        category: stringValue(row.lead_magnet_category),
        leadsLifetime: numberValue(row.leads_lifetime) ?? 0,
        buyersLifetime: numberValue(row.buyers_lifetime) ?? 0,
        revenueLifetime: numberValue(row.revenue_lifetime) ?? 0,
        revenuePerLeadLifetime: numberValue(row.revenue_per_lead_lifetime),
        timeToBuyerDays: numberValue(row.time_to_buyer_days),
        lastSeenLabel: label,
        lastActivityRecent: recent,
      } satisfies LeaderRow;
    })
    .filter((row): row is LeaderRow => row !== null);

  return mapped.sort((a, b) => {
    if (b.revenueLifetime !== a.revenueLifetime) return b.revenueLifetime - a.revenueLifetime;
    return b.leadsLifetime - a.leadsLifetime;
  });
}

function MagnetLeaderboard({
  rows,
  attributionBreakdown,
}: {
  rows: DashboardRow[];
  attributionBreakdown: DashboardRow | undefined;
}) {
  const leaderRows = buildLeaderRows(rows);
  const totalMagnets = leaderRows.length;
  const activeMagnets = leaderRows.filter((r) => r.lastActivityRecent).length;
  const sections = groupRowsByCategory(leaderRows);

  const totalBuyers = numberValue(attributionBreakdown?.total_buyers) ?? 0;
  const multiMagnetBuyers = numberValue(attributionBreakdown?.multi_magnet_buyers) ?? 0;

  const metaLine2Parts: string[] = [];
  if (totalBuyers > 0 && multiMagnetBuyers > 0) {
    metaLine2Parts.push(
      `${formatNumber(multiMagnetBuyers)} of ${formatNumber(totalBuyers)} buyers touched 2+ magnets first`,
    );
  }
  if (totalMagnets > 0) {
    metaLine2Parts.push(
      `${formatNumber(activeMagnets)} of ${formatNumber(totalMagnets)} magnets active in last 90 days`,
    );
  }

  if (leaderRows.length === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title">
            <span className="dot" /> Magnets, ranked by revenue
          </span>
        </div>
        <div className="od-panel-body">
          <div className="od-empty-state m-3">No magnets in the curated dashboard set yet.</div>
        </div>
      </div>
    );
  }

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Magnets, ranked by revenue
          <span className="lm-window-chip">Lifetime</span>
        </span>
        <span className="od-panel-meta lm-attribution-meta">
          <span className="lm-attribution-rule">
            Revenue is credited to the last magnet a buyer touched before their first purchase.
          </span>
          {metaLine2Parts.length > 0 ? (
            <span className="lm-attribution-counts">{metaLine2Parts.join(" · ")}</span>
          ) : null}
        </span>
      </div>
      <div className="od-panel-body flush">
        <table className="od-table compact rep-summary-table lm-magnet-table">
          <thead>
            <tr>
              <th className="col-magnet">Magnet</th>
              <th className="num col-leads">Leads</th>
              <th className="num col-buyers">Buyers</th>
              <th className="num col-convert">Convert</th>
              <th className="num col-days">Days to buy</th>
              <th className="col-lastseen">Last seen</th>
              <th className="num col-revenue">Revenue</th>
              <th className="num col-rpl">$ / lead</th>
            </tr>
          </thead>
          <tbody>
            {sections.map((section) => (
              <MagnetCategorySection
                key={section.key}
                label={section.label}
                count={section.rows.length}
                totalRevenue={section.totalRevenue}
                rows={section.rows}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

type CategorySection = {
  key: string;
  label: string;
  rows: LeaderRow[];
  totalRevenue: number;
};

function groupRowsByCategory(rows: LeaderRow[]): CategorySection[] {
  const byKey = new Map<string, CategorySection>();
  for (const row of rows) {
    const key = row.category ?? "uncategorized";
    let section = byKey.get(key);
    if (!section) {
      section = {
        key,
        label: formatCategoryHeaderLabel(key),
        rows: [],
        totalRevenue: 0,
      };
      byKey.set(key, section);
    }
    section.rows.push(row);
    section.totalRevenue += row.revenueLifetime;
  }
  return Array.from(byKey.values()).sort((a, b) => b.totalRevenue - a.totalRevenue);
}

function formatCategoryHeaderLabel(category: string): string {
  switch (category) {
    case "true_lead_magnet":
      return "True lead magnets";
    case "launch_event":
      return "Launch events";
    case "waitlist":
      return "Waitlists";
    case "sales_operating_pipeline":
      return "Sales pipelines";
    case "uncategorized":
      return "Uncategorized";
    default:
      return category.replace(/_/g, " ");
  }
}

function MagnetCategorySection({
  label,
  count,
  totalRevenue,
  rows,
}: {
  label: string;
  count: number;
  totalRevenue: number;
  rows: LeaderRow[];
}) {
  return (
    <>
      <tr className="lm-category-section" aria-label={label}>
        <td colSpan={8}>
          <span className="lm-category-label">{label}</span>
          <span className="lm-category-meta">
            {formatNumber(count)} {count === 1 ? "magnet" : "magnets"} · {formatCurrencyFull(totalRevenue)} total
          </span>
        </td>
      </tr>
      {rows.map((row) => (
        <LeaderRowGroup key={row.id} row={row} />
      ))}
    </>
  );
}

function LeaderRowGroup({ row }: { row: LeaderRow }) {
  const rplTone = toneForValue(row.revenuePerLeadLifetime, TONE_SCALES.revenue_per_lead);
  const convertLabel = formatConversionPercent(row.buyersLifetime, row.leadsLifetime);

  return (
    <>
      <tr className="rep-row lm-magnet-row">
        <td className="col-magnet">
          <Link
            href={`/lead-magnets/${encodeURIComponent(row.id)}`}
            prefetch={false}
            className="rep-row-link"
            title={row.name}
          >
            <span className="lm-leader-name">{row.name}</span>
          </Link>
        </td>
        <td className="num col-leads">{row.leadsLifetime > 0 ? formatNumber(row.leadsLifetime) : "—"}</td>
        <td className="num col-buyers">{row.buyersLifetime > 0 ? formatNumber(row.buyersLifetime) : "—"}</td>
        <td className="num col-convert">{convertLabel}</td>
        <td className="num col-days">{formatDaysToBuy(row.timeToBuyerDays)}</td>
        <td className="col-lastseen">{row.lastSeenLabel}</td>
        <td className="num col-revenue">{formatCurrencyFull(row.revenueLifetime)}</td>
        <td className={`num rep-pct-cell lm-rpl-cell col-rpl tone-${rplTone}`}>
          {row.revenuePerLeadLifetime == null ? "—" : formatCurrencyCompact(row.revenuePerLeadLifetime)}
        </td>
      </tr>
    </>
  );
}

// ------------------------------------------------------------------
// LeadMagnetRangePicker — segmented control. Reuses the global
// .stl-range-picker / .stl-range-btn styles from globals.css.
// ------------------------------------------------------------------

const RANGE_OPTIONS: { id: string; label: string }[] = [
  { id: "30d", label: "30D" },
  { id: "90d", label: "90D" },
  { id: "180d", label: "180D" },
  { id: "all", label: "All" },
];

function LeadMagnetRangePicker({ current }: { current: string }) {
  return (
    <div className="stl-range-picker" role="tablist" aria-label="Time range">
      {RANGE_OPTIONS.map(({ id, label }) => {
        const isActive = current === id;
        return (
          <Link
            key={id}
            href={`/lead-magnets?range=${id}`}
            prefetch={false}
            role="tab"
            aria-selected={isActive}
            className={`stl-range-btn${isActive ? " active" : ""}`}
          >
            {label}
          </Link>
        );
      })}
    </div>
  );
}
