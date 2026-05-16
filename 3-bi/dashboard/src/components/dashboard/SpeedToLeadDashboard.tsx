"use client";

import { useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  Clock,
  Gauge,
  PhoneCall,
  Phone,
  Radar,
  Radio,
  Users,
  Zap,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type {
  DashboardData,
  DashboardFilters,
  DashboardFreshness,
  DashboardRow,
} from "@/types/dashboard-data";
import Link from "next/link";
import { SectionErrorChip } from "@/components/dashboard/SectionErrorChip";
import { DataHealthDisclosure } from "@/components/dashboard/DataHealthDisclosure";
import { tierForSpeedToLeadQuery } from "@/lib/bigquery/query-tiers";
import { shouldSuppressDelta } from "@/lib/config/speed-to-lead-tokens";

const BOOKING_SLA_MIN = 45;
const STRICT_SLA_MIN = 5;

const BAND_QUERIES = {
  header: ["speed_to_lead_overall_prior"],
  now: [
    "speed_to_lead_lane_summary",
    "speed_to_lead_no_touch_examples",
    "speed_to_lead_routing_readiness",
  ],
  hour: [
    "speed_to_lead_quality_summary",
    "speed_to_lead_trigger_summary",
    "speed_to_lead_business_hours",
  ],
  today: [
    "speed_to_lead_by_rep",
    "speed_to_lead_critical_exceptions",
    "speed_to_lead_attribution_confidence",
    "speed_to_lead_first_work_by_rep",
    "speed_to_lead_phone_reach_by_rep",
  ],
  trend: [
    "speed_to_lead_daily",
    "speed_to_lead_response_buckets",
    "speed_to_lead_follow_up_counts",
    "speed_to_lead_source_performance",
    "speed_to_lead_not_worked_aging",
    "speed_to_lead_first_attempt_outcomes",
  ],
} as const;

const BAND_LABEL: Record<keyof typeof BAND_QUERIES, string> = {
  header: "Header comparison",
  now: "Live queue",
  hour: "Hour pulse",
  today: "Today",
  trend: "Trend & audit",
};

function pickErrors(
  errors: Partial<Record<string, string>> | undefined,
  names: readonly string[],
): { name: string; detail: string }[] {
  if (!errors) return [];
  return names
    .map((name) => ({ name, detail: errors[name] }))
    .filter((entry): entry is { name: string; detail: string } => Boolean(entry.detail));
}

function hasError(
  errors: Partial<Record<string, string>> | undefined,
  name: string,
): boolean {
  return Boolean(errors?.[name]);
}

function BandErrors({
  bandKey,
  errors,
}: {
  bandKey: keyof typeof BAND_QUERIES;
  errors: Partial<Record<string, string>> | undefined;
}) {
  const owned = pickErrors(errors, BAND_QUERIES[bandKey]);
  if (owned.length === 0) return null;
  return (
    <div className="mb-3 space-y-1.5">
      {owned.map(({ name, detail }) => (
        <SectionErrorChip
          key={name}
          title={`${BAND_LABEL[bandKey]} · ${name}`}
          detail={detail}
          queryName={name}
        />
      ))}
    </div>
  );
}

type LaneFilter = "all" | "inbound_bookings" | "lead_magnets" | "outbound_existing_bookings";

const LANE_LABEL: Record<string, string> = {
  inbound_bookings: "Inbound bookings",
  lead_magnets: "Lead magnets",
  outbound_existing_bookings: "Outbound / existing",
  other: "Other",
};

export function SpeedToLeadDashboard({ data }: { data: DashboardData }) {
  const overall = (data.rows.speed_to_lead_overall ?? [])[0];
  const overallPrior = (data.rows.speed_to_lead_overall_prior ?? [])[0];
  const reps = data.rows.speed_to_lead_by_rep ?? [];
  const daily = data.rows.speed_to_lead_daily ?? [];
  const activeQueue = (data.rows.speed_to_lead_active_queue ?? [])[0];

  const queryErrors = data.queryErrors;

  return (
    <div className="stl-page stl-od-page space-y-5 pb-12">
      <div className="od-page-head">
        <div className="od-title">
          <h1>Speed-to-Lead</h1>
          <p className="od-blurb">{data.filters.timeRangeLabel} · how fast SDRs touch leads</p>
        </div>
        <RangePicker current={data.filters.timeRange} basePath="/speed-to-lead" />
      </div>

      {data.error ? (
        <div className="od-callout od-callout-danger">{data.error}</div>
      ) : null}

      <LeadsWaitingStrip queue={activeQueue} />

      <HeroSLACard
        slaPct={numberValue(overall?.pct_within_sla)}
        slaPctPrior={numberValue(overallPrior?.pct_within_sla)}
        totalLeads={numberValue(overall?.total_triggers_all)}
        daily={daily}
      />

      <RepSummaryTable rows={reps} currentRange={data.filters.timeRange} />

      {queryErrors && Object.keys(queryErrors).length > 0 ? (
        <DataHealthDisclosure errors={queryErrors} tierFor={tierForSpeedToLeadQuery} />
      ) : null}
    </div>
  );
}

// ------------------------------------------------------------------
// LeadsWaitingStrip — operational "right now" signal at the top of the page.
// Independent of the date-range picker (always last 72h via ACTIVE_QUEUE_HOURS
// in speed-to-lead-live.ts). Three age-bucket counts + a positive empty state
// when the queue is clear.
// ------------------------------------------------------------------

const ACTIVE_QUEUE_LABEL_HOURS = 72;

function LeadsWaitingStrip({ queue }: { queue: DashboardRow | undefined }) {
  const inWindow = numberValue(queue?.in_window) ?? 0;
  const pastBar = numberValue(queue?.past_bar) ?? 0;
  const pastOneHour = numberValue(queue?.past_one_hour) ?? 0;
  const total = numberValue(queue?.total) ?? 0;

  const isEmpty = total === 0;

  if (isEmpty) {
    return (
      <div className="stl-waiting-strip is-empty">
        <span className="stl-waiting-headline">
          <span className="stl-waiting-bolt">⚡</span>
          All clear · queue empty
        </span>
        <span className="stl-waiting-window-tag">last {ACTIVE_QUEUE_LABEL_HOURS}h</span>
      </div>
    );
  }

  return (
    <div className="stl-waiting-strip">
      <span className="stl-waiting-headline">
        <span className="stl-waiting-bolt">⚡</span>
        Right now · <strong>{formatNumber(total)}</strong> unworked
      </span>
      <div className="stl-waiting-pills">
        <span className="stl-waiting-pill in-window">
          <span className="pill-count">{formatNumber(inWindow)}</span>
          <span className="pill-label">in window</span>
        </span>
        <span className="stl-waiting-pill past-bar">
          <span className="pill-count">{formatNumber(pastBar)}</span>
          <span className="pill-label">past 15m</span>
        </span>
        <span className="stl-waiting-pill past-one-hour">
          <span className="pill-count">{formatNumber(pastOneHour)}</span>
          <span className="pill-label">past 1 hour</span>
        </span>
      </div>
      <span className="stl-waiting-window-tag">last {ACTIVE_QUEUE_LABEL_HOURS}h</span>
    </div>
  );
}

// ------------------------------------------------------------------
// OpenDesign ported view
// ------------------------------------------------------------------

function OpenDesignPageHead({
  filters,
  freshness,
  lanes,
  queue,
  attribution,
  queryErrors,
}: {
  filters: DashboardFilters;
  freshness: DashboardFreshness;
  overall: DashboardRow | undefined;
  overallPrior: DashboardRow | undefined;
  lanes: DashboardRow[];
  queue: DashboardRow[];
  attribution: DashboardRow | undefined;
  queryErrors?: Partial<Record<string, string>>;
}) {
  const stats = buildOpenQueueStats(queue, lanes);
  const named = numberValue(attribution?.named_rep_reached);
  const reached = numberValue(attribution?.reached_leads);
  const namedRate = named != null && reached ? named / reached : null;
  const errorCount = queryErrors ? Object.keys(queryErrors).length : 0;

  return (
    <div className="od-page-head">
      <div className="od-title">
        <h1>Speed-to-Lead</h1>
        <p className="od-blurb">
          Every minute a lead waits is a percentage point of conversion. Queue, SLA, owners, and reasons we miss.
        </p>
      </div>
      <div className="od-head-actions">
        <div className="od-ctxchips">
          <span className={`od-badge ${stats.breached > 0 ? "danger" : "success"}`}>
            {formatNumber(stats.breached)} breached
          </span>
          <span className={`od-badge ${stats.live > 0 ? "warn" : "success"}`}>
            {formatNumber(stats.live)} in queue
          </span>
          {stats.ownerless > 0 ? (
            <span className="od-badge unknown">{formatNumber(stats.ownerless)} ownerless</span>
          ) : null}
          {namedRate != null ? (
            <span className={`od-freshness ${namedRate >= 0.7 ? "fresh" : namedRate >= 0.4 ? "stale" : "broken"}`}>
              <span className="dot" />
              Rep proof · {formatPercent(namedRate)}
            </span>
          ) : null}
          <span className={`od-freshness ${freshness.status === "live" ? "fresh" : freshness.status === "stale" ? "stale" : "broken"}`}>
            <span className="dot" />
            {freshness.label}
          </span>
          {errorCount > 0 ? <span className="od-badge danger">{errorCount} query gap{errorCount === 1 ? "" : "s"}</span> : null}
        </div>
        <TimeRange filters={filters} />
      </div>
    </div>
  );
}

// OpenDesignKpiStrip removed 2026-05-16 — replaced by MoneyStrip (below)
// per Speed-to-Lead Spec v2 Section 5.1 block 2.

type DisplayDelta = { label: string; tone: "up" | "down" | "flat" } | null;

function OpenDesignKpi({
  label,
  value,
  delta,
  note,
  featured = false,
  denominator,
}: {
  label: string;
  value: string;
  delta: DisplayDelta;
  note: string;
  featured?: boolean;
  denominator?: number | null;
}) {
  // Sparklines and phase-pending state intentionally dropped from this primitive
  // post 2026-05-16 Slice 1 review (asymmetric/half-broken optics). Phase-pending
  // metrics now render as one consolidated placeholder tile (MoneyStripPlaceholder).
  const className = `od-kpi${featured ? " featured" : ""}`;
  return (
    <div className={className}>
      <span className="label">{label}</span>
      <div className="value">
        {value}
        {denominator !== undefined && denominator !== null ? (
          <span className="denominator"> / {formatNumber(denominator)}</span>
        ) : null}
      </div>
      <div className="row">
        {delta ? (
          <span className={`od-delta delta-${delta.tone}`}>{delta.label}</span>
        ) : (
          <span className="od-delta delta-flat">—</span>
        )}
        <span className="gap-note">{note}</span>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// MoneyStrip — headline strip per Speed-to-Lead Spec v2 Section 5.1 block 2.
// Replaces OpenDesignKpiStrip for the manager headline. Metrics that depend
// on Phase 3 (outbound cadence mart) or Phase 5 (closer / cash marts) render
// in a `phase-pending` greyed state so the spec layout is visible immediately
// without misleading numbers.
// ------------------------------------------------------------------

function MoneyStrip({
  filters,
  overall,
  overallPrior,
  quality,
  daily,
}: {
  filters: DashboardFilters;
  overall: DashboardRow | undefined;
  overallPrior: DashboardRow | undefined;
  quality: DashboardRow | undefined;
  daily: DashboardRow[];
}) {
  const totalLeads = numberValue(overall?.total_triggers_all);
  const totalLeadsPrior = numberValue(overallPrior?.total_triggers_all);
  const attemptedRate = numberValue(overall?.pct_triggers_with_outbound_touch);
  const attemptedRatePrior = numberValue(overallPrior?.pct_triggers_with_outbound_touch);
  const attempted = totalLeads !== null && attemptedRate !== null ? Math.round(totalLeads * attemptedRate) : null;
  const attemptedPrior = totalLeadsPrior !== null && attemptedRatePrior !== null ? Math.round(totalLeadsPrior * attemptedRatePrior) : null;
  const connected = numberValue(quality?.successful_connections) ?? numberValue(quality?.reached_leads);
  const slaPct = numberValue(overall?.pct_within_sla);
  const slaPctPrior = numberValue(overallPrior?.pct_within_sla);

  return (
    <div className="od-kpi-strip money-strip">
      <OpenDesignKpi
        label="Leads in"
        value={formatNumber(totalLeads)}
        delta={countDelta(totalLeads, totalLeadsPrior)}
        note={`${filters.timeRangeLabel}`}
      />
      <OpenDesignKpi
        label="Attempted"
        value={formatNumber(attempted)}
        delta={countDelta(attempted, attemptedPrior)}
        note="human-initiated touches"
        denominator={totalLeads}
      />
      <OpenDesignKpi
        label="Connected"
        value={formatNumber(connected)}
        delta={null}
        note="call ≥30s OR SMS reply"
        denominator={totalLeads}
      />
      <HeroSLACard
        slaPct={slaPct}
        slaPctPrior={slaPctPrior}
        totalLeads={totalLeads}
        daily={daily}
      />
    </div>
  );
}

// Target tuned to team reality (2026-05-16): named-rep median sits around 50%,
// and 80% against a team currently at 18% reads as "we're failing." 50% is
// median-performer territory — reachable and morale-preserving.
export const SLA_TARGET = 0.5;

export function HeroSLACard({
  slaPct,
  slaPctPrior,
  totalLeads,
  daily,
  totalLeadsLabel = "total leads",
}: {
  slaPct: number | null;
  slaPctPrior: number | null;
  totalLeads: number | null;
  daily: DashboardRow[];
  totalLeadsLabel?: string;
}) {
  const dailyValues = dailyMetricValues(daily, "pct_within_sla");
  const withinSlaCount =
    slaPct !== null && totalLeads !== null ? Math.round(slaPct * totalLeads) : null;

  const meetsTarget = slaPct !== null && slaPct >= SLA_TARGET;
  const delta = slaPct !== null && slaPctPrior !== null ? slaPct - slaPctPrior : null;
  const direction =
    delta === null
      ? "unknown"
      : Math.abs(delta) < 0.005
        ? "stable"
        : delta > 0
          ? "improving"
          : "worsening";

  const statusText = slaPct === null ? "—" : meetsTarget ? "On goal" : "Below goal";
  const directionText =
    direction === "unknown"
      ? ""
      : direction === "stable"
        ? "holding"
        : direction === "improving"
          ? `improving ${formatPctDelta(delta)}`
          : `worsening ${formatPctDelta(delta)}`;
  const verdictLabel = directionText ? `${statusText} · ${directionText}` : statusText;

  const verdictTone =
    slaPct === null
      ? "neutral"
      : meetsTarget
        ? "ok"
        : direction === "improving"
          ? "warning"
          : "danger";

  return (
    <div className="od-kpi hero-sla">
      <span className="label">Responded within 15 minutes</span>
      <div className="hero-row">
        <div className="value">{formatPercent(slaPct)}</div>
        {dailyValues.length > 1 ? <MiniSparkline values={dailyValues} tone="up" /> : null}
      </div>
      <div className="hero-sub">
        {withinSlaCount !== null && totalLeads !== null && totalLeads > 0
          ? `${formatNumber(withinSlaCount)} touched in 15 min · ${formatNumber(totalLeads)} ${totalLeadsLabel}`
          : "no data this period"}
      </div>
      <div className="hero-row hero-footer">
        <span className="hero-target">Goal: {Math.round(SLA_TARGET * 100)}%</span>
        <span className={`hero-verdict tone-${verdictTone}`}>{verdictLabel}</span>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// SetterStrip — side-by-side per-rep cards per Spec v2 Sections 4.4 & 5.1 block 6.
// Two-half card: claim speed (top, wall-clock caveat) + post-claim conversion (bottom).
// Replaces OpenDesignOwnerPerformance table.
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// RangePicker — segmented control for switching time windows.
// Reusable across surfaces; basePath lets it sit on /speed-to-lead, the
// per-rep drill page, and any future analytical view.
// ------------------------------------------------------------------

const RANGE_OPTIONS: { id: string; label: string }[] = [
  { id: "today", label: "Today" },
  { id: "7d", label: "7D" },
  { id: "30d", label: "30D" },
  { id: "90d", label: "90D" },
];

export function RangePicker({ current, basePath }: { current: string; basePath: string }) {
  return (
    <div className="stl-range-picker" role="tablist" aria-label="Time range">
      {RANGE_OPTIONS.map(({ id, label }) => {
        const isActive = current === id;
        return (
          <Link
            key={id}
            href={`${basePath}?range=${id}`}
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

// ------------------------------------------------------------------
// RepSummaryTable — compact ranked list of rep performance. Replaces the
// individual SetterStrip cards per the strip-hard pass: one row per real
// rep (dialer phone-fingerprints filtered out), columns are SLA · leads
// worked · vs team median. Sorted by SLA desc.
// ------------------------------------------------------------------

export function isRealRep(row: DashboardRow): boolean {
  const role = stringValue(row.rep_role);
  if (role === "automation") return false;
  const name = stringValue(row.rep_name) ?? "";
  if (name.startsWith("WAVV ·")) return false;
  if (name.startsWith("Phone ·")) return false;
  if (name.startsWith("Unmapped GHL user")) return false;
  if (name.startsWith("Deleted GHL user")) return false;
  if (name === "Workflow automation") return false;
  if (name === "GHL event with no rep supplied") return false;
  if (name === "Unknown rep") return false;
  if ((numberValue(row.leads_worked) ?? 0) <= 0) return false;
  return true;
}

export function computeMedian(values: number[]): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

function RepSummaryTable({ rows, currentRange }: { rows: DashboardRow[]; currentRange: string }) {
  const reps = rows.filter(isRealRep);
  const ghostCount = rows.filter((r) => !isRealRep(r) && (numberValue(r.leads_worked) ?? 0) > 0).length;
  const ghostAttempts = rows
    .filter((r) => !isRealRep(r))
    .reduce((sum, r) => sum + (numberValue(r.leads_worked) ?? 0), 0);

  if (reps.length === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title">
            <span className="dot" /> Rep performance
          </span>
        </div>
        <div className="od-panel-body">
          <div className="od-empty-state m-3">No rep activity in this window.</div>
        </div>
      </div>
    );
  }

  const slaPcts = reps
    .map((r) => numberValue(r.pct_within_sla))
    .filter((v): v is number => v !== null && Number.isFinite(v));
  const teamAvgSla = computeMedian(slaPcts); // median is the right "typical" — still surfaced as "team avg" in copy

  const sortedReps = [...reps].sort((a, b) => {
    const aPct = numberValue(a.pct_within_sla) ?? -1;
    const bPct = numberValue(b.pct_within_sla) ?? -1;
    return bPct - aPct;
  });

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Rep performance
        </span>
        <span className="od-panel-meta">
          {reps.length} {reps.length === 1 ? "rep" : "reps"} ·{" "}
          {teamAvgSla !== null
            ? `team avg ${formatPercent(teamAvgSla)} within 15 min`
            : "no team average yet"}
        </span>
      </div>
      <div className="od-panel-body flush">
        <table className="od-table compact rep-summary-table">
          <thead>
            <tr>
              <th>Rep</th>
              <th className="num">Within 15m</th>
              <th className="num">Leads worked</th>
            </tr>
          </thead>
          <tbody>
            {sortedReps.map((rep) => {
              const name = stringValue(rep.rep_name) ?? "Unknown";
              const sla = numberValue(rep.pct_within_sla);
              const leadsWorked = numberValue(rep.leads_worked);
              const diff = sla !== null && teamAvgSla !== null ? sla - teamAvgSla : null;
              const tone =
                diff === null ? "flat" : diff > 0.02 ? "up" : diff < -0.02 ? "down" : "flat";
              const href = `/speed-to-lead/rep/${encodeURIComponent(name)}?range=${currentRange}`;
              return (
                <tr key={name} className="rep-row">
                  <td>
                    <Link href={href} prefetch={false} className="rep-row-link">
                      {name}
                    </Link>
                  </td>
                  <td className={`num rep-pct-cell tone-${tone}`}>{formatPercent(sla)}</td>
                  <td className="num">{formatNumber(leadsWorked)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {ghostCount > 0 || ghostAttempts > 0 ? (
        <div className="od-panel-footer">
          <span className="od-ev gap">
            {formatNumber(ghostAttempts)} attempts from unmapped dialer lines / workflow automation — fix at /audit to attribute them.
          </span>
        </div>
      ) : null}
    </div>
  );
}

function SetterStrip({ rows }: { rows: DashboardRow[] }) {
  const setters = rows.filter((row) => {
    if (stringValue(row.rep_role) === "automation") return false;
    if (numberValue(row.leads_worked) === null || (numberValue(row.leads_worked) ?? 0) <= 0) return false;
    return true;
  });

  if (setters.length === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title"><span className="dot" /> Setter scorecard</span>
        </div>
        <div className="od-empty-state m-4">No rep activity logged in this window.</div>
      </div>
    );
  }

  return (
    <div className="od-setter-strip">
      {setters.map((rep, idx) => (
        <SetterCard key={`${stringValue(rep.rep_name)}-${idx}`} rep={rep} />
      ))}
    </div>
  );
}

function SetterCard({ rep }: { rep: DashboardRow }) {
  const name = stringValue(rep.rep_name) ?? "Unknown rep";
  const roleRaw = stringValue(rep.rep_role) ?? "unknown";
  const role = roleRaw === "setter" || roleRaw === "closer" ? roleRaw : "unknown";
  const leadsWorked = numberValue(rep.leads_worked);
  const leadsReached = numberValue(rep.leads_reached);
  const bookingsWorked = numberValue(rep.bookings_worked);
  const avgSpeed = numberValue(rep.avg_speed_to_lead_minutes);
  const slaPct = numberValue(rep.pct_within_sla);

  return (
    <div className="od-setter-card">
      <div className="od-setter-card-header">
        <span className="rep-name">{name}</span>
        <span className={`role-badge role-${role}`}>{role}</span>
      </div>

      <div className="od-setter-card-half claim-speed">
        <div className="half-label">Claim speed</div>
        <div className="caveat-banner">
          Wall-clock measure — shift-time signals pending (Phase 0 stream b)
        </div>
        <div className="metrics">
          <SetterMetric label="Avg first touch" value={formatAge(avgSpeed)} />
          <SetterMetric
            label="Within 45m SLA"
            value={formatPercent(slaPct)}
            denominator={leadsWorked}
          />
        </div>
      </div>

      <div className="od-setter-card-half post-claim">
        <div className="half-label">Post-claim conversion</div>
        <div className="metrics">
          <SetterMetric label="Leads worked" value={formatNumber(leadsWorked)} />
          <SetterMetric label="Connected" value={formatNumber(leadsReached)} denominator={leadsWorked} />
          <SetterMetric label="Bookings worked" value={formatNumber(bookingsWorked)} denominator={leadsWorked} />
        </div>
      </div>
    </div>
  );
}

function SetterMetric({
  label,
  value,
  denominator,
}: {
  label: string;
  value: string;
  denominator?: number | null;
}) {
  return (
    <div className="od-setter-metric">
      <div className="metric-value">
        {value}
        {denominator !== undefined && denominator !== null && denominator > 0 ? (
          <span className="metric-denominator"> / {formatNumber(denominator)}</span>
        ) : null}
      </div>
      <div className="metric-label">{label}</div>
    </div>
  );
}

// ------------------------------------------------------------------
// SpeedFunnel — 3-stage funnel (Lead → Attempted → Connected) with leak attribution
// per Spec v2 Section 4.7 + 5.1 block 5. Option A narrow scope: closer-side stages
// (Set/Shown/Pitched/Closed) live on /revenue/closers, not here.
// ------------------------------------------------------------------

type FunnelStageData = {
  name: string;
  volume: number | null;
  convRate: number | null; // conversion from previous stage (0-1)
  convDelta: number | null; // current convRate - prior convRate (in 0-1 ratio space)
};

function SpeedFunnel({
  filters,
  overall,
  overallPrior,
  quality,
}: {
  filters: DashboardFilters;
  overall: DashboardRow | undefined;
  overallPrior: DashboardRow | undefined;
  quality: DashboardRow | undefined;
}) {
  const leadIn = numberValue(overall?.total_triggers_all);
  const leadInPrior = numberValue(overallPrior?.total_triggers_all);
  const attemptedRate = numberValue(overall?.pct_triggers_with_outbound_touch);
  const attemptedRatePrior = numberValue(overallPrior?.pct_triggers_with_outbound_touch);
  const attempted =
    leadIn !== null && attemptedRate !== null ? Math.round(leadIn * attemptedRate) : null;
  const connected =
    numberValue(quality?.successful_connections) ?? numberValue(quality?.reached_leads);

  const conv1 = attemptedRate; // Lead → Attempted (mart already provides this as the rate)
  const conv1Delta =
    conv1 !== null && attemptedRatePrior !== null ? conv1 - attemptedRatePrior : null;
  const conv2 =
    attempted !== null && attempted > 0 && connected !== null ? connected / attempted : null;
  // No prior-period quality_summary in current data layer — Connected delta stays null for Phase 1.

  const stages: FunnelStageData[] = [
    { name: "Lead in", volume: leadIn, convRate: null, convDelta: null },
    { name: "Attempted", volume: attempted, convRate: conv1, convDelta: conv1Delta },
    { name: "Connected", volume: connected, convRate: conv2, convDelta: null },
  ];

  // Worst leak: largest negative convDelta (Phase 1: only conv1 has a delta)
  const leakIndex = stages.reduce<number | null>((worstIdx, stage, idx) => {
    if (stage.convDelta === null || stage.convDelta >= 0) return worstIdx;
    if (worstIdx === null) return idx;
    const worstDelta = stages[worstIdx].convDelta;
    if (worstDelta !== null && stage.convDelta < worstDelta) return idx;
    return worstIdx;
  }, null);

  const maxVolume = stages.reduce<number>(
    (max, stage) => (stage.volume !== null && stage.volume > max ? stage.volume : max),
    0,
  );

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Speed funnel
        </span>
        <span className="od-panel-meta">{filters.timeRangeLabel}</span>
      </div>
      <div className="od-panel-body">
        <div className="od-speed-funnel">
          {stages.map((stage, idx) => (
            <FunnelStageRow
              key={stage.name}
              stage={stage}
              maxVolume={maxVolume}
              isLeak={leakIndex === idx}
              showConvArrow={idx > 0}
            />
          ))}
        </div>
      </div>
      <div className="od-panel-footer">
        {leakIndex !== null ? (
          <span className="od-ev gap">
            Worst leak: {stages[leakIndex].name} —{" "}
            {formatPctDelta(stages[leakIndex].convDelta)} vs prior period
          </span>
        ) : (
          <span className="od-ev">Connect-stage delta unavailable — prior-period quality data not yet wired.</span>
        )}
      </div>
    </div>
  );
}

function FunnelStageRow({
  stage,
  maxVolume,
  isLeak,
  showConvArrow,
}: {
  stage: FunnelStageData;
  maxVolume: number;
  isLeak: boolean;
  showConvArrow: boolean;
}) {
  const widthPct =
    maxVolume > 0 && stage.volume !== null ? Math.max(2, (stage.volume / maxVolume) * 100) : 0;
  return (
    <>
      {showConvArrow ? (
        <div className="funnel-conv-arrow">
          <span className="conv-arrow-glyph">↓</span>
          {stage.convRate !== null ? (
            <span className="conv-rate">{formatPercent(stage.convRate)}</span>
          ) : (
            <span className="conv-rate muted">—</span>
          )}
          {stage.convDelta !== null ? (
            <span className={`conv-delta ${stage.convDelta < 0 ? "down" : "up"}`}>
              {formatPctDelta(stage.convDelta)}
            </span>
          ) : null}
        </div>
      ) : null}
      <div className={`funnel-stage${isLeak ? " leak" : ""}`}>
        <div className="stage-row">
          <span className="stage-name">{stage.name}</span>
          <span className="stage-volume">{formatNumber(stage.volume)}</span>
        </div>
        <div className="stage-bar-wrap">
          <div className="stage-bar" style={{ width: `${widthPct}%` }} />
        </div>
        {isLeak ? <span className="stage-leak-tag">leak</span> : null}
      </div>
    </>
  );
}

// ------------------------------------------------------------------
// LeakAlerts — ranked compact list of "what's getting worse" per Spec v2
// Section 4.1 (Top 3 leak alerts) + Section 5.1 block 3. Option A narrow
// scope: only speed-funnel + SLA candidates. Connected-stage delta is
// skipped until prior-period quality data is wired.
// ------------------------------------------------------------------

type LeakAlert = {
  id: string;
  title: string;
  current: number; // ratio 0-1
  prior: number; // ratio 0-1
  delta: number; // current - prior (in ratio space; negative = leak)
  severity: number; // abs(delta) — larger = worse
  note: string;
};

const LEAK_THRESHOLD = 0.005; // 0.5 percentage points — below this, ignore as noise

function LeakAlerts({
  overall,
  overallPrior,
}: {
  overall: DashboardRow | undefined;
  overallPrior: DashboardRow | undefined;
}) {
  const alerts = computeLeakAlerts(overall, overallPrior);

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Top leak alerts
        </span>
        <span className="od-panel-meta">
          {alerts.length} {alerts.length === 1 ? "signal" : "signals"}
        </span>
      </div>
      <div className="od-panel-body">
        {alerts.length === 0 ? (
          <div className="od-empty-state m-3">
            All speed signals stable vs prior period.
          </div>
        ) : (
          <div className="od-leak-alerts-list">
            {alerts.map((alert) => (
              <LeakAlertRow key={alert.id} alert={alert} />
            ))}
          </div>
        )}
      </div>
      <div className="od-panel-footer">
        <span className="od-ev">
          Phase 1 surface: Lead → Attempted + Within-45m SLA. Connected-stage delta wires when prior-period quality data lands.
        </span>
      </div>
    </div>
  );
}

function computeLeakAlerts(
  overall: DashboardRow | undefined,
  overallPrior: DashboardRow | undefined,
): LeakAlert[] {
  const leadsPrior = numberValue(overallPrior?.total_triggers_all);
  const alerts: LeakAlert[] = [];

  // Lead → Attempted conversion
  const attemptedRate = numberValue(overall?.pct_triggers_with_outbound_touch);
  const attemptedRatePrior = numberValue(overallPrior?.pct_triggers_with_outbound_touch);
  if (
    attemptedRate !== null &&
    attemptedRatePrior !== null &&
    !shouldSuppressDelta(leadsPrior, "conversion")
  ) {
    const delta = attemptedRate - attemptedRatePrior;
    if (delta < -LEAK_THRESHOLD) {
      alerts.push({
        id: "lead-to-attempted",
        title: "Lead → Attempted",
        current: attemptedRate,
        prior: attemptedRatePrior,
        delta,
        severity: Math.abs(delta),
        note: "share of leads that got a human-initiated touch",
      });
    }
  }

  // Within 45m SLA
  const slaPct = numberValue(overall?.pct_within_sla);
  const slaPctPrior = numberValue(overallPrior?.pct_within_sla);
  if (
    slaPct !== null &&
    slaPctPrior !== null &&
    !shouldSuppressDelta(leadsPrior, "sla")
  ) {
    const delta = slaPct - slaPctPrior;
    if (delta < -LEAK_THRESHOLD) {
      alerts.push({
        id: "sla-45m",
        title: "Within 45m SLA",
        current: slaPct,
        prior: slaPctPrior,
        delta,
        severity: Math.abs(delta),
        note: "share of bookings answered within the 45-minute window",
      });
    }
  }

  return alerts.sort((a, b) => b.severity - a.severity);
}

// SLATrendChart removed 2026-05-16 — folded into HeroSLACard's inline sparkline
// per the strip-hard pass. Trend lives inside the hero card now.

function LeakAlertRow({ alert }: { alert: LeakAlert }) {
  return (
    <div className="od-leak-alert-row">
      <div className="leak-alert-head">
        <span className="leak-alert-title">{alert.title}</span>
        <span className="leak-alert-delta">{formatPctDelta(alert.delta)}</span>
      </div>
      <div className="leak-alert-rates">
        <span className="leak-rate-current">{formatPercent(alert.current)}</span>
        <span className="leak-rate-arrow">↓</span>
        <span className="leak-rate-prior">from {formatPercent(alert.prior)}</span>
      </div>
      <div className="leak-alert-note">{alert.note}</div>
    </div>
  );
}

export function formatPctDelta(delta: number | null): string {
  if (delta === null || !Number.isFinite(delta)) return "—";
  const points = delta * 100;
  if (Math.abs(points) < 0.05) return "flat";
  const sign = points > 0 ? "+" : "−";
  return `${sign}${Math.abs(points).toFixed(1)}pp`;
}

function countDelta(current: number | null, prior: number | null): DisplayDelta {
  if (current === null || prior === null) return null;
  if (shouldSuppressDelta(prior)) {
    return { label: "—", tone: "flat" };
  }
  const diff = current - prior;
  if (diff === 0) return { label: "flat", tone: "flat" };
  const sign = diff > 0 ? "+" : "−";
  const magnitude = Math.abs(diff);
  return {
    label: `${sign}${formatNumber(magnitude)}`,
    tone: diff > 0 ? "up" : "down",
  };
}

function pctDelta(current: number | null, prior: number | null): DisplayDelta {
  if (current === null || prior === null) return null;
  const points = (current - prior) * 100;
  if (Math.abs(points) < 0.5) return { label: "flat", tone: "flat" };
  const sign = points > 0 ? "+" : "−";
  return {
    label: `${sign}${Math.abs(points).toFixed(1)}pp`,
    tone: points > 0 ? "up" : "down",
  };
}

function OpenDesignAgeDistribution({ rows, lanes }: { rows: DashboardRow[]; lanes: DashboardRow[] }) {
  const liveRows = liveQueueRows(rows);
  const stats = buildOpenQueueStats(rows, lanes);
  const buckets = [
    { label: "0-5m", count: liveRows.filter((row) => withinAge(row, 0, STRICT_SLA_MIN)).length, tone: "success" },
    { label: "5-15m", count: liveRows.filter((row) => withinAge(row, STRICT_SLA_MIN, 15)).length, tone: "success" },
    { label: "15-60m", count: liveRows.filter((row) => withinAge(row, 15, 60)).length, tone: "warn" },
    { label: "1-4h", count: liveRows.filter((row) => withinAge(row, 60, 240)).length, tone: "warn" },
    { label: ">4h", count: liveRows.filter((row) => (numberValue(row.age_minutes) ?? 0) > 240).length, tone: "danger" },
  ] as const;
  const max = Math.max(1, ...buckets.map((bucket) => bucket.count));

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title"><span className="dot" /> Queue · age distribution</span>
        <span className="od-panel-meta">{formatNumber(stats.live)} active · {formatNumber(stats.breached)} in breach</span>
      </div>
      <div className="od-panel-body">
        <div className="od-age-distribution">
          {buckets.map((bucket) => (
            <div className="od-age-bucket" key={bucket.label}>
              <span className="b-l">{bucket.label}</span>
              <span className={`b-n ${bucket.tone}`}>{formatNumber(bucket.count)}</span>
              <div className="b-bar">
                <div className={`b-fill ${bucket.tone}`} style={{ width: `${(bucket.count / max) * 100}%` }} />
              </div>
            </div>
          ))}
        </div>
        <div className="od-chip-row">
          <span className="od-chip queue">Ownerless · {formatNumber(stats.ownerless)}</span>
          <span className="od-chip">After-hours · {formatNumber(stats.afterHours)}</span>
          <span className="od-chip gap">No source · {formatNumber(stats.noSource)}</span>
          <span className="od-chip">Inbound · {formatNumber(stats.inbound)}</span>
          <span className="od-chip">Lead magnets · {formatNumber(stats.leadMagnets)}</span>
        </div>
      </div>
      <div className="od-panel-footer">
        <span className="od-ev">SLA target: {STRICT_SLA_MIN}m strict · {BOOKING_SLA_MIN}m booking window</span>
      </div>
    </div>
  );
}

function OpenDesignResponseTrend({ rows }: { rows: DashboardRow[] }) {
  const medianValues = dailyMetricValues(rows, "median_speed_to_lead_minutes");
  const p90Values = dailyMetricValues(rows, "p90_speed_to_lead_minutes");
  const p50Delta = firstLastDelta(medianValues);
  const p90Delta = firstLastDelta(p90Values);

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title"><span className="dot" /> Response time trend · selected range</span>
        <span className="od-panel-meta">
          p50 {formatTrendDelta(p50Delta)} · p90 {formatTrendDelta(p90Delta)}
        </span>
      </div>
      <div className="od-panel-body">
        {medianValues.length >= 2 || p90Values.length >= 2 ? (
          <TrendLinesSvg medianValues={medianValues} p90Values={p90Values} />
        ) : (
          <div className="od-empty-state">Widen the range to plot a response trend.</div>
        )}
      </div>
      <div className="od-panel-footer">
        <span className="od-ev">Trend uses the same daily Speed-to-Lead rows as the diagnostic chart below.</span>
      </div>
    </div>
  );
}

function OpenDesignQueueTable({
  rows,
  lanes,
  timeRange,
}: {
  rows: DashboardRow[];
  lanes: DashboardRow[];
  timeRange: string;
}) {
  const liveRows = sortQueueForOpenDesign(liveQueueRows(rows)).slice(0, 10);
  const stats = buildOpenQueueStats(rows, lanes);

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title danger"><span className="dot" /> Open queue · {formatNumber(stats.live)} leads</span>
        <span className="od-panel-meta">{formatNumber(stats.breached)} in breach</span>
        <div className="od-panel-actions">
          <span className="od-mini-btn accent">Breached</span>
          <span className="od-mini-btn">In-window</span>
          <span className="od-mini-btn">Ownerless</span>
        </div>
      </div>
      <div className="od-panel-body flush">
        {liveRows.length === 0 ? (
          <div className="od-empty-state m-4">Live queue is clear. No missing post-trigger touch proof right now.</div>
        ) : (
          <div className="od-table-wrap">
            <table className="od-table">
              <thead>
                <tr>
                  <th>Lead</th>
                  <th>Source</th>
                  <th>Trigger</th>
                  <th>Age</th>
                  <th>SLA</th>
                  <th>Owner</th>
                  <th>Proof</th>
                </tr>
              </thead>
              <tbody>
                {liveRows.map((row, index) => (
                  <OpenDesignQueueTableRow
                    key={`${stringValue(row.trigger_event_id) ?? stringValue(row.lead_email) ?? "lead"}-${index}`}
                    row={row}
                    timeRange={timeRange}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
      <div className="od-panel-footer">
        <span className={`od-ev ${stats.ownerless > 0 ? "gap" : ""}`}>
          {formatNumber(stats.ownerless)} ownerless · {formatNumber(stats.noSource)} no source · {formatNumber(stats.afterHours)} after-hours
        </span>
        {stats.live > liveRows.length ? <span className="od-ev od-footer-right">Showing {formatNumber(liveRows.length)} of {formatNumber(stats.live)}</span> : null}
      </div>
    </div>
  );
}

function OpenDesignQueueTableRow({ row, timeRange }: { row: DashboardRow; timeRange: string }) {
  const ageMin = numberValue(row.age_minutes);
  const tone = ageToTone(ageMin);
  const leadName = stringValue(row.lead_name) ?? "Unknown lead";
  const email = stringValue(row.lead_email);
  const contactSk = stringValue(row.contact_sk);
  const href = contactSk ? `/customers/${contactSk}?from=speed-to-lead&reason=not_worked&range=${encodeURIComponent(timeRange)}` : null;

  return (
    <tr>
      <td className="primary">
        <div className="od-lead-cell">
          <span>{leadName}</span>
          <span>{email && email !== "No email" ? email : stringValue(row.lead_phone) ?? "No contact path"}</span>
        </div>
      </td>
      <td><span className={`od-badge ${sourceBadgeTone(row)}`}>{stringValue(row.source_label) ?? "Unknown"}</span></td>
      <td className="dim">{formatTriggerType(stringValue(row.trigger_type) ?? stringValue(row.lane_id) ?? "Lead")}</td>
      <td><span className={`od-timer ${queueTimerTone(tone)}`}>{formatAge(ageMin)}</span></td>
      <td><SlaBar minutes={ageMin} /></td>
      <td><OwnerPill name={stringValue(row.assigned_rep) ?? "Unassigned"} /></td>
      <td>
        {href ? (
          <Link className="od-ev" href={href} prefetch={false}>open profile →</Link>
        ) : (
          <span className="od-ev gap">no profile</span>
        )}
      </td>
    </tr>
  );
}

// OpenDesignOwnerPerformance removed 2026-05-16 — replaced by SetterStrip
// per Speed-to-Lead Spec v2 Sections 4.4 + 5.1 block 6.

function OpenDesignExceptionDrivers({ rows }: { rows: DashboardRow[] }) {
  const driverRows = rows
    .map((row) => ({
      label: stringValue(row.exception_label) ?? "Exception",
      helper: stringValue(row.manager_action) ?? "",
      count: numberValue(row.open_unmatched_leads) ?? numberValue(row.unworked_leads) ?? numberValue(row.lead_events) ?? 0,
      events: numberValue(row.lead_events) ?? 0,
    }))
    .filter((row) => row.count > 0 || row.events > 0)
    .sort((a, b) => b.count - a.count || b.events - a.events)
    .slice(0, 6);
  const total = driverRows.reduce((sum, row) => sum + Math.max(row.count, row.events), 0);
  const max = Math.max(1, ...driverRows.map((row) => Math.max(row.count, row.events)));

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title"><span className="dot" /> Exception drivers · selected range</span>
        <span className="od-panel-meta">{formatNumber(total)} events</span>
      </div>
      <div className="od-panel-body">
        {driverRows.length === 0 ? (
          <div className="od-empty-state">No exception rows returned for this range.</div>
        ) : (
          <div>
            {driverRows.map((row) => {
              const value = Math.max(row.count, row.events);
              return (
                <div className="od-reason-row" key={row.label}>
                  <div>
                    <div className="reason-title">{row.label}</div>
                    <div className="reason-subtitle">{row.helper}</div>
                  </div>
                  <div className="b"><div className="f" style={{ width: `${(value / max) * 100}%` }} /></div>
                  <span className="n">{formatNumber(value)}</span>
                  <span className="p">{formatPercent(total > 0 ? value / total : null)}</span>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

export function MiniSparkline({ values, tone }: { values: number[]; tone?: "up" | "dn" }) {
  if (values.length < 2) return <span className="od-sparkline-empty" />;
  const width = 80;
  const height = 24;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const points = values
    .map((value, index) => {
      const x = (index / Math.max(1, values.length - 1)) * width;
      const y = height - ((value - min) / span) * (height - 4) - 2;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <svg className="od-sparkline" viewBox={`0 0 ${width} ${height}`} aria-hidden>
      <polyline className={`line ${tone ?? ""}`} points={points} />
    </svg>
  );
}

function TrendLinesSvg({ medianValues, p90Values }: { medianValues: number[]; p90Values: number[] }) {
  const all = [...medianValues, ...p90Values];
  const min = Math.min(...all);
  const max = Math.max(...all);
  const span = max - min || 1;
  const pathFor = (values: number[]) => {
    if (values.length < 2) return "";
    return values
      .map((value, index) => {
        const x = (index / Math.max(1, values.length - 1)) * 600;
        const y = 180 - ((value - min) / span) * 150;
        return `${index === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(" ");
  };

  return (
    <svg viewBox="0 0 600 200" preserveAspectRatio="none" className="od-trend-svg" aria-label="Response time trend">
      <line x1="0" y1="20" x2="600" y2="20" />
      <line x1="0" y1="80" x2="600" y2="80" />
      <line x1="0" y1="140" x2="600" y2="140" />
      <line className="target" x1="0" y1="180" x2="600" y2="180" />
      {p90Values.length >= 2 ? <path className="p90" d={pathFor(p90Values)} /> : null}
      {medianValues.length >= 2 ? <path className="p50" d={pathFor(medianValues)} /> : null}
      <text x="8" y="18">slow</text>
      <text x="8" y="184">fast</text>
      <text x="560" y="42">p90</text>
      <text x="560" y="170">p50</text>
    </svg>
  );
}

function SlaBar({ minutes }: { minutes: number | null }) {
  const pct = minutes == null ? 0 : Math.min(100, Math.max(4, (minutes / BOOKING_SLA_MIN) * 100));
  const tone = ageToTone(minutes);
  return (
    <div className="od-sla-bar" title={`SLA timer: ${formatAge(minutes)} of ${BOOKING_SLA_MIN}m`}>
      <div className={`fill ${queueTimerTone(tone)}`} style={{ width: `${pct}%` }} />
    </div>
  );
}

function OwnerPill({ name }: { name: string }) {
  const isUnknown = !name || name === "Unassigned" || name === "Unknown" || name === "Unknown rep";
  return (
    <span className={`od-owner ${isUnknown ? "unknown" : ""}`}>
      <span className="av">{isUnknown ? "?" : ownerInitials(name)}</span>
      <span className="name">{isUnknown ? "unassigned" : name}</span>
    </span>
  );
}

type OpenQueueStats = {
  live: number;
  breached: number;
  ownerless: number;
  afterHours: number;
  noSource: number;
  inbound: number;
  leadMagnets: number;
  stale: number;
};

function buildOpenQueueStats(rows: DashboardRow[], lanes: DashboardRow[]): OpenQueueStats {
  const liveRows = liveQueueRows(rows);
  const laneStats = buildNoTouchLaneStats(lanes);
  const live = Math.max(laneStats.all.fresh, liveRows.length);
  const staleRows = rows.filter((row) => ageToTone(numberValue(row.age_minutes)) === "stale");

  return {
    live,
    breached: liveRows.filter((row) => (numberValue(row.age_minutes) ?? 0) > BOOKING_SLA_MIN).length,
    ownerless: liveRows.filter(isOwnerlessQueueRow).length,
    afterHours: liveRows.filter((row) => stringValue(row.service_window) === "after_hours").length,
    noSource: liveRows.filter((row) => {
      const source = stringValue(row.source_label);
      return !source || source === "Unknown" || source === "N/A";
    }).length,
    inbound: laneStats.byLane.inbound_bookings?.fresh ?? liveRows.filter((row) => stringValue(row.lane_id) === "inbound_bookings").length,
    leadMagnets: laneStats.byLane.lead_magnets?.fresh ?? liveRows.filter((row) => stringValue(row.lane_id) === "lead_magnets").length,
    stale: Math.max(laneStats.all.stale, staleRows.length),
  };
}

function liveQueueRows(rows: DashboardRow[]) {
  return rows.filter((row) => ageToTone(numberValue(row.age_minutes)) !== "stale");
}

function sortQueueForOpenDesign(rows: DashboardRow[]) {
  const priority: Record<Tone, number> = {
    danger: 0,
    warning: 1,
    ok: 2,
    neutral: 3,
    stale: 4,
  };
  return [...rows].sort((a, b) => {
    const aAge = numberValue(a.age_minutes);
    const bAge = numberValue(b.age_minutes);
    const toneDiff = priority[ageToTone(aAge)] - priority[ageToTone(bAge)];
    if (toneDiff !== 0) return toneDiff;
    return (bAge ?? 0) - (aAge ?? 0);
  });
}

function withinAge(row: DashboardRow, minExclusive: number, maxInclusive: number) {
  const minutes = numberValue(row.age_minutes);
  return minutes != null && minutes > minExclusive && minutes <= maxInclusive;
}

export function dailyMetricValues(rows: DashboardRow[], key: string) {
  return rows.map((row) => numberValue(row[key])).filter((value): value is number => value != null);
}

// minutesDelta and pointsDeltaDisplay removed 2026-05-16 — they were only used
// by the deleted OpenDesignKpiStrip. MoneyStrip uses countDelta (defined
// alongside it). Reintroduce a centralized delta helper if other components
// need percentage-point or duration deltas later.

function firstLastDelta(values: number[]) {
  if (values.length < 2) return null;
  return values[values.length - 1] - values[0];
}

function formatTrendDelta(delta: number | null) {
  if (delta == null) return "n/a";
  if (Math.abs(delta) < 0.5) return "flat";
  return delta < 0 ? `↓ ${formatAge(Math.abs(delta))}` : `↑ ${formatAge(delta)}`;
}

function sourceBadgeTone(row: DashboardRow) {
  const source = stringValue(row.source_label);
  if (!source || source === "Unknown" || source === "N/A") return "unknown";
  if (stringValue(row.lane_id) === "lead_magnets") return "accent";
  return "muted";
}

function queueTimerTone(tone: Tone) {
  if (tone === "ok") return "ok";
  if (tone === "warning") return "warn";
  if (tone === "danger") return "breach";
  return "muted";
}

function ownerInitials(name: string) {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("") || "?";
}

function TimeRange({ filters }: { filters: DashboardFilters }) {
  return (
    <div
      className="stl-range-control flex w-full items-center gap-1 rounded-md border p-1 md:w-auto"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card)" }}
    >
      {filters.timeRangeOptions.map((option) => {
        const isActive = option.value === filters.timeRange;
        return (
          <a
            key={option.value}
            href={`/speed-to-lead?range=${option.value}`}
            aria-current={isActive ? "page" : undefined}
            title={option.description}
            className={`stl-range-link min-w-10 rounded px-2.5 py-1 text-center text-xs font-semibold transition ${
              isActive ? "text-white" : "hover:opacity-90"
            }`}
            style={
              isActive
                ? { background: "var(--stl-accent)" }
                : { color: "var(--stl-text)" }
            }
          >
            {option.label}
          </a>
        );
      })}
    </div>
  );
}

// ------------------------------------------------------------------
// Band shell
// ------------------------------------------------------------------

function Band({
  title,
  caption,
  icon,
  rightSlot,
  defaultOpen = true,
  collapsible = false,
  children,
}: {
  title: string;
  caption?: string;
  icon: React.ReactNode;
  rightSlot?: React.ReactNode;
  defaultOpen?: boolean;
  collapsible?: boolean;
  children: React.ReactNode;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <section
      className="overflow-hidden rounded-xl border"
      style={{
        borderColor: "var(--stl-border)",
        background: "var(--stl-card)",
        boxShadow: "var(--stl-shadow-soft)",
      }}
    >
      <button
        type="button"
        onClick={() => collapsible && setOpen((v) => !v)}
        disabled={!collapsible}
        aria-expanded={open}
        className={`flex w-full items-center gap-3 px-4 py-2.5 text-left ${
          collapsible ? "cursor-pointer hover:opacity-90" : "cursor-default"
        }`}
      >
        <span
          className="flex h-7 w-7 shrink-0 items-center justify-center rounded-md"
          style={{ background: "var(--stl-accent-soft)", color: "var(--stl-accent)" }}
        >
          {icon}
        </span>
        <div className="min-w-0 flex-1">
          <div
            className="text-[11px] font-semibold uppercase tracking-wider"
            style={{ color: "var(--stl-muted)" }}
          >
            {title}
          </div>
          {caption ? (
            <div
              className="truncate text-sm font-medium"
              style={{ color: "var(--stl-text)" }}
            >
              {caption}
            </div>
          ) : null}
        </div>
        {rightSlot}
        {collapsible ? (
          open ? (
            <ChevronDown className="h-4 w-4" style={{ color: "var(--stl-muted)" }} aria-hidden />
          ) : (
            <ChevronRight className="h-4 w-4" style={{ color: "var(--stl-muted)" }} aria-hidden />
          )
        ) : null}
      </button>
      {open ? (
        <div className="border-t px-4 py-4 md:px-5" style={{ borderColor: "var(--stl-border)" }}>
          {children}
        </div>
      ) : null}
    </section>
  );
}

// ------------------------------------------------------------------
// Band 1: NOW — live queue
// ------------------------------------------------------------------

const LANE_TOOLTIP: Record<string, string> = {
  all: "Every live lead event with no post-trigger touch found",
  inbound_bookings: "New booking made through a public scheduling page",
  lead_magnets: "Opt-in from a downloadable guide or content offer",
  outbound_existing_bookings:
    "Booking that came from an outbound dial or an existing customer rebook",
};

function NowBand({
  queue,
  lanes,
  timeRange,
  queryErrors,
}: {
  queue: DashboardRow[];
  lanes: DashboardRow[];
  timeRange: string;
  queryErrors?: Partial<Record<string, string>>;
}) {
  const [activeLane, setActiveLane] = useState<LaneFilter>("all");
  // Sort by action priority: actionable leads (warning > ok > danger) first,
  // then within the same tier put the freshest at the top.
  const sortedQueue = useMemo(() => {
    const tonePriority: Record<Tone, number> = {
      warning: 0,
      ok: 1,
      danger: 2,
      stale: 3,
      neutral: 4,
    };
    return [...queue].sort((a, b) => {
      const aTone = ageToTone(numberValue(a.age_minutes));
      const bTone = ageToTone(numberValue(b.age_minutes));
      const tonal = tonePriority[aTone] - tonePriority[bTone];
      if (tonal !== 0) return tonal;
      return (numberValue(a.age_minutes) ?? 0) - (numberValue(b.age_minutes) ?? 0);
    });
  }, [queue]);
  const liveQueue = useMemo(
    () => sortedQueue.filter((r) => ageToTone(numberValue(r.age_minutes)) !== "stale"),
    [sortedQueue],
  );
  const filtered =
    activeLane === "all"
      ? liveQueue
      : liveQueue.filter((r) => stringValue(r.lane_id) === activeLane);
  const visible = filtered.slice(0, 12);
  const laneStats = buildNoTouchLaneStats(lanes);
  const activeStats = activeLane === "all" ? laneStats.all : laneStats.byLane[activeLane];
  const activeLiveCount = activeStats?.fresh ?? filtered.length;
  const activeHistoricalCount = activeStats?.stale ?? 0;
  const hiddenCount = Math.max(0, activeLiveCount - visible.length);

  const laneCounts = liveQueue.reduce<Record<string, number>>((acc, row) => {
    const id = stringValue(row.lane_id);
    if (id) acc[id] = (acc[id] ?? 0) + 1;
    return acc;
  }, {});

  const liveCount = activeLiveCount;
  const staleCount = activeHistoricalCount;

  return (
    <Band
      title="Live queue"
      icon={<Activity className="h-4 w-4" aria-hidden />}
      caption={
        liveCount === 0
          ? staleCount > 0
            ? `Live queue clear · ${formatNumber(staleCount)} historical unmatched in audit`
            : "Live queue clear · no missing post-trigger touch proof"
          : `${formatNumber(liveCount)} live · freshest first`
      }
      rightSlot={
        liveCount > 0 ? (
          <span className={`shrink-0 rounded-md px-2 py-1 text-xs font-semibold ${toneBadgeClass("danger")}`}>
            {formatNumber(liveCount)} need attention
          </span>
        ) : staleCount > 0 ? (
          <span className={`shrink-0 rounded-md px-2 py-1 text-xs font-semibold ${toneBadgeClass("stale")}`}>
            No live miss
          </span>
        ) : null
      }
    >
      <BandErrors bandKey="now" errors={queryErrors} />
      <div className="mb-3 flex flex-wrap items-center gap-1">
        <LaneChip
          label="All"
          count={laneStats.all.fresh || liveQueue.length}
          active={activeLane === "all"}
          tooltip={LANE_TOOLTIP.all}
          onClick={() => setActiveLane("all")}
        />
        {(["inbound_bookings", "lead_magnets", "outbound_existing_bookings"] as LaneFilter[]).map(
          (lane) => (
            <LaneChip
              key={lane}
              label={LANE_LABEL[lane] ?? lane}
              count={laneStats.byLane[lane]?.fresh ?? laneCounts[lane] ?? 0}
              active={activeLane === lane}
              tooltip={LANE_TOOLTIP[lane]}
              onClick={() => setActiveLane(lane)}
            />
          ),
        )}
      </div>

      {filtered.length > 0 ? <QueueSnapshot rows={filtered} /> : null}

      {visible.length === 0 ? (
        <EmptyRow message="Live queue is clear. No leads need attention right now." icon="ok" />
      ) : (
        <div className="space-y-1.5">
          {visible.map((row, index) => (
            <QueueRow
              key={`${stringValue(row.trigger_event_id) ?? stringValue(row.lead_email) ?? "lead"}-${index}`}
              row={row}
              timeRange={timeRange}
            />
          ))}
          {hiddenCount > 0 ? (
            <div className="pt-2 text-xs" style={{ color: "var(--stl-muted)" }}>
              + {formatNumber(hiddenCount)} more in this lane
            </div>
          ) : null}
        </div>
      )}
    </Band>
  );
}

function QueueSnapshot({ rows }: { rows: DashboardRow[] }) {
  const buckets = buildQueueAgeBuckets(rows);
  const focus = buildQueueFocusStats(rows);
  const total = rows.length;

  if (total === 0) return null;

  return (
    <div className="mb-4 grid gap-3 lg:grid-cols-[minmax(0,1.3fr)_minmax(18rem,0.7fr)]">
      <div
        className="rounded-lg border px-3 py-3"
        style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
      >
        <div className="flex flex-wrap items-start justify-between gap-2">
          <div>
            <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
              Open queue age
            </div>
            <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
              Returned no-touch rows in this lane, bucketed by age.
            </div>
          </div>
          <span className="rounded-md px-2 py-1 text-[11px] font-semibold" style={{
            background: "var(--stl-neutral-chip)",
            color: "var(--stl-muted)",
          }}>
            {formatNumber(total)} returned
          </span>
        </div>
        <div className="mt-3 space-y-2">
          {buckets.map((bucket) => (
            <div
              key={bucket.label}
              className="grid grid-cols-[4.5rem_minmax(0,1fr)_3rem] items-center gap-2 text-xs"
            >
              <span className="font-medium" style={{ color: "var(--stl-muted)" }}>
                {bucket.label}
              </span>
              <div
                className="h-2.5 overflow-hidden rounded-full"
                style={{ background: "var(--stl-neutral-chip)" }}
                aria-label={`${bucket.label}: ${formatNumber(bucket.count)} rows`}
              >
                <div
                  className="h-full rounded-full"
                  style={{
                    width: `${total > 0 ? Math.max(4, (bucket.count / total) * 100) : 0}%`,
                    opacity: bucket.count > 0 ? 1 : 0,
                    background: queueBucketColor(bucket.tone),
                  }}
                />
              </div>
              <span
                className={`text-right font-semibold ${toneTextClass(bucket.tone)}`}
                style={{ fontFamily: "var(--font-metric)" }}
              >
                {formatNumber(bucket.count)}
              </span>
            </div>
          ))}
        </div>
      </div>

      <div
        className="rounded-lg border px-3 py-3"
        style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
      >
        <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
          Recommended focus
        </div>
        <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
          Counts are derived from the returned no-touch rows.
        </div>
        <div className="mt-3 grid grid-cols-2 gap-2">
          {focus.map((item) => (
            <div
              key={item.label}
              className="rounded-md border px-3 py-2"
              style={{ borderColor: "var(--stl-border)", background: "var(--stl-card)" }}
              title={item.title}
            >
              <div className="text-[10px] font-semibold uppercase tracking-wide" style={{ color: "var(--stl-muted)" }}>
                {item.label}
              </div>
              <div
                className={`mt-1 text-xl font-semibold leading-none ${toneTextClass(item.tone)}`}
                style={{ fontFamily: "var(--font-metric)" }}
              >
                {formatNumber(item.count)}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function buildQueueAgeBuckets(rows: DashboardRow[]) {
  const buckets: { label: string; tone: Tone; test: (minutes: number | null) => boolean }[] = [
    { label: "0-5m", tone: "ok", test: (minutes) => minutes != null && minutes <= STRICT_SLA_MIN },
    { label: "5-15m", tone: "warning", test: (minutes) => minutes != null && minutes > STRICT_SLA_MIN && minutes <= 15 },
    { label: "15-45m", tone: "warning", test: (minutes) => minutes != null && minutes > 15 && minutes <= BOOKING_SLA_MIN },
    { label: "45m-4h", tone: "danger", test: (minutes) => minutes != null && minutes > BOOKING_SLA_MIN && minutes <= 240 },
    { label: "4-24h", tone: "danger", test: (minutes) => minutes != null && minutes > 240 && minutes <= 1440 },
  ];

  return buckets.map((bucket) => ({
    label: bucket.label,
    tone: bucket.tone,
    count: rows.filter((row) => bucket.test(numberValue(row.age_minutes))).length,
  }));
}

function buildQueueFocusStats(rows: DashboardRow[]) {
  const overSla = rows.filter((row) => (numberValue(row.age_minutes) ?? 0) > BOOKING_SLA_MIN).length;
  const ownerless = rows.filter(isOwnerlessQueueRow).length;
  const afterHours = rows.filter((row) => stringValue(row.service_window) === "after_hours").length;
  const noSource = rows.filter((row) => {
    const source = stringValue(row.source_label);
    return !source || source === "Unknown" || source === "N/A";
  }).length;

  return [
    {
      label: `Over ${BOOKING_SLA_MIN}m`,
      count: overSla,
      tone: overSla > 0 ? "danger" : "ok",
      title: "Returned open rows beyond the 45-minute booking SLA.",
    },
    {
      label: "Ownerless",
      count: ownerless,
      tone: ownerless > 0 ? "warning" : "ok",
      title: "Returned open rows where the assigned rep is missing or unassigned.",
    },
    {
      label: "After hours",
      count: afterHours,
      tone: afterHours > 0 ? "neutral" : "ok",
      title: "Returned open rows triggered outside the configured business-hours window.",
    },
    {
      label: "No source",
      count: noSource,
      tone: noSource > 0 ? "warning" : "ok",
      title: "Returned open rows where source labeling is missing or generic.",
    },
  ] satisfies { label: string; count: number; tone: Tone; title: string }[];
}

function isOwnerlessQueueRow(row: DashboardRow): boolean {
  const rep = stringValue(row.assigned_rep);
  return !rep || rep === "Unassigned" || rep === "Unknown";
}

function queueBucketColor(tone: Tone) {
  switch (tone) {
    case "ok":
      return "var(--stl-accent)";
    case "warning":
      return "var(--stl-warning)";
    case "danger":
      return "var(--stl-danger)";
    default:
      return "var(--stl-muted)";
  }
}

function buildNoTouchLaneStats(lanes: DashboardRow[]) {
  const byLane = lanes.reduce<Record<string, { noTouch: number; fresh: number; stale: number }>>(
    (acc, row) => {
      const lane = stringValue(row.lane_id);
      if (!lane) return acc;
      acc[lane] = {
        noTouch: numberValue(row.no_touch) ?? 0,
        fresh: numberValue(row.fresh_no_touch) ?? 0,
        stale: numberValue(row.stale_no_touch) ?? 0,
      };
      return acc;
    },
    {},
  );
  const all = Object.values(byLane).reduce(
    (sum, lane) => ({
      noTouch: sum.noTouch + lane.noTouch,
      fresh: sum.fresh + lane.fresh,
      stale: sum.stale + lane.stale,
    }),
    { noTouch: 0, fresh: 0, stale: 0 },
  );

  return { all, byLane };
}

function LaneChip({
  label,
  count,
  active,
  tooltip,
  onClick,
}: {
  label: string;
  count: number;
  active: boolean;
  tooltip?: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      title={tooltip}
      className="flex items-center gap-1.5 rounded-md border px-2.5 py-1 text-xs font-medium transition hover:opacity-90"
      style={
        active
          ? {
              borderColor: "var(--stl-accent)",
              background: "var(--stl-accent)",
              color: "white",
            }
          : {
              borderColor: "var(--stl-border)",
              background: "var(--stl-card-strong)",
              color: "var(--stl-text)",
            }
      }
    >
      <span>{label}</span>
      <span
        className="rounded px-1.5 py-0.5 text-[10px] font-semibold"
        style={
          active
            ? { background: "rgba(255,255,255,0.22)" }
            : { background: "var(--stl-neutral-chip)", color: "var(--stl-muted)" }
        }
      >
        {formatNumber(count)}
      </span>
    </button>
  );
}

function QueueRow({ row, timeRange }: { row: DashboardRow; timeRange: string }) {
  const ageMin = numberValue(row.age_minutes);
  const tone = ageToTone(ageMin);
  const lane = stringValue(row.lane_id);
  const laneLabel = lane ? LANE_LABEL[lane] ?? lane : "—";
  const serviceWindow = stringValue(row.service_window) === "after_hours" ? "After hours" : "Business hours";
  const rep = stringValue(row.assigned_rep) ?? "Unassigned";
  const ringColor = toneRingColor(tone);
  const email = stringValue(row.lead_email);
  const phone = stringValue(row.lead_phone);
  const isStale = tone === "stale";
  const leadName = stringValue(row.lead_name) ?? "Unknown";
  const contactSk = stringValue(row.contact_sk);
  const profileHref = contactSk
    ? `/customers/${contactSk}?from=speed-to-lead&reason=not_worked&range=${encodeURIComponent(timeRange)}`
    : null;

  return (
    <div
      className={`relative grid grid-cols-[3rem_minmax(0,1.5fr)_minmax(0,1fr)_minmax(0,0.85fr)_auto] items-center gap-3 rounded-md border px-3 py-2 transition${
        profileHref ? " hover:border-[var(--stl-accent)]" : ""
      }`}
      style={{
        borderColor: "var(--stl-border)",
        background: isStale ? "var(--stl-card)" : "var(--stl-card-strong)",
        opacity: isStale ? 0.62 : 1,
      }}
    >
      {profileHref ? (
        <Link
          href={profileHref}
          prefetch={false}
          aria-label={`Open profile for ${leadName}`}
          className="absolute inset-0 z-0 rounded-md focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--stl-accent)]"
        >
          <span className="sr-only">Open profile</span>
        </Link>
      ) : null}
      <SlaRing minutes={ageMin} color={ringColor} />
      <div className="min-w-0">
        <div className="truncate text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
          {leadName}
        </div>
        <div
          className="relative z-10 flex min-w-0 items-center gap-2 truncate text-[11px]"
          style={{ color: "var(--stl-muted)" }}
        >
          {email && email !== "No email" ? (
            <a
              href={`mailto:${email}`}
              className="truncate underline-offset-2 hover:underline"
              title={`Email ${email}`}
            >
              {email}
            </a>
          ) : (
            <span className="truncate">{email ?? "—"}</span>
          )}
          <span aria-hidden>·</span>
          {phone && phone !== "No phone" ? (
            <a
              href={`tel:${phone.replace(/[^+\d]/g, "")}`}
              className="shrink-0 underline-offset-2 hover:underline"
              title={`Call ${phone}`}
            >
              {phone}
            </a>
          ) : (
            <span className="shrink-0">{phone ?? "—"}</span>
          )}
        </div>
      </div>
      <div className="min-w-0">
        <div className="truncate text-xs font-medium" style={{ color: "var(--stl-text)" }}>
          {laneLabel}
        </div>
        <div className="truncate text-[11px]" style={{ color: "var(--stl-muted)" }}>
          {stringValue(row.source_label) ?? "—"} · {serviceWindow}
        </div>
      </div>
      <div className="min-w-0 text-right">
        <div className={`text-xs font-semibold ${toneTextClass(tone)}`}>
          {ageMin != null ? formatAge(ageMin) : "—"}
        </div>
        <div className="truncate text-[11px]" style={{ color: "var(--stl-muted)" }}>
          {rep}
        </div>
      </div>
      <span
        className={`relative z-10 shrink-0 rounded-md px-2 py-1 text-[10px] font-semibold uppercase tracking-wide ${toneBadgeClass(tone)}`}
        title={
          tone === "ok"
            ? "Less than 5 minutes old"
            : tone === "warning"
              ? "5–45 minutes old — act now to hit SLA"
              : tone === "danger"
                ? "Over the 45-minute SLA but under 24 hours"
                : tone === "stale"
                  ? "Over 24 hours — likely cleanup, not a live save"
                  : ""
        }
      >
        {toneBadgeLabel(tone)}
      </span>
    </div>
  );
}

function SlaRing({ minutes, color }: { minutes: number | null; color: string }) {
  const pct =
    minutes == null
      ? 0
      : Math.min(1, Math.max(0, minutes / BOOKING_SLA_MIN));
  const stroke = 3;
  const r = 16;
  const c = 2 * Math.PI * r;
  const dash = c * pct;
  return (
    <div className="relative h-10 w-10">
      <svg className="h-10 w-10 -rotate-90" viewBox="0 0 40 40">
        <circle
          cx="20"
          cy="20"
          r={r}
          fill="none"
          stroke="var(--stl-border)"
          strokeWidth={stroke}
        />
        <circle
          cx="20"
          cy="20"
          r={r}
          fill="none"
          stroke={color}
          strokeWidth={stroke}
          strokeDasharray={`${dash} ${c - dash}`}
          strokeLinecap="round"
        />
      </svg>
      <div
        className="absolute inset-0 flex items-center justify-center text-[9px] font-semibold"
        style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}
      >
        {minutes == null ? "—" : minutes < 60 ? `${Math.round(minutes)}m` : `${Math.round(minutes / 60)}h`}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// Band 2: This hour — pulse strip
// ------------------------------------------------------------------

function HourPulseBand({
  overall,
  triggers,
  quality,
  queryErrors,
}: {
  overall: DashboardRow | undefined;
  triggers: DashboardRow[];
  quality: DashboardRow | undefined;
  queryErrors?: Partial<Record<string, string>>;
}) {
  const within45 = numberValue(overall?.pct_within_sla);
  const within5 = numberValue(overall?.pct_within_5m);
  const median = numberValue(overall?.median_speed_to_lead_minutes);
  const total = numberValue(overall?.total_triggers_all);
  const reachRate = numberValue(quality?.reached_lead_rate);
  const reachedLeads = numberValue(quality?.reached_leads);
  const totalForReach = numberValue(quality?.total_triggers);

  const triggerSplit = triggers.reduce<Record<string, number>>((acc, row) => {
    const t = stringValue(row.trigger_type);
    if (t) acc[t] = numberValue(row.total_triggers) ?? 0;
    return acc;
  }, {});

  return (
    <Band
      title="Pulse"
      caption="How the team is pacing in the selected range"
      icon={<Zap className="h-4 w-4" aria-hidden />}
    >
      <BandErrors bandKey="hour" errors={queryErrors} />
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
        <PulseCard
          label={`Within ${BOOKING_SLA_MIN}m`}
          value={formatPercent(within45)}
          target="SLA window for first attempt"
          tone={slaTonePct(within45)}
          icon={<Gauge className="h-4 w-4" aria-hidden />}
        />
        <PulseCard
          label={`Within ${STRICT_SLA_MIN}m`}
          value={formatPercent(within5)}
          target="Bridge / first-touch window"
          tone={strictTonePct(within5)}
          icon={<Zap className="h-4 w-4" aria-hidden />}
        />
        <PulseCard
          label="Reached"
          value={formatPercent(reachRate)}
          target={
            reachedLeads != null && totalForReach != null
              ? `${formatNumber(reachedLeads)} of ${formatNumber(totalForReach)} answered or completed`
              : "Successful phone connections"
          }
          tone={reachTonePct(reachRate)}
          icon={<PhoneCall className="h-4 w-4" aria-hidden />}
        />
        <PulseCard
          label="Median speed"
          value={median == null ? "—" : formatAge(median)}
          target={`p90 ${formatAge(numberValue(overall?.p90_speed_to_lead_minutes))}`}
          tone="neutral"
          icon={<Clock className="h-4 w-4" aria-hidden />}
        />
        <PulseCard
          label="Lead events"
          value={formatNumber(total)}
          target={
            triggers.length === 0
              ? "—"
              : Object.entries(triggerSplit)
                  .map(([k, v]) => `${formatTriggerType(k)}: ${formatNumber(v)}`)
                  .join(" · ")
          }
          tone="neutral"
          icon={<Users className="h-4 w-4" aria-hidden />}
        />
      </div>
    </Band>
  );
}

function reachTonePct(value: number | null | undefined): Tone {
  if (value == null) return "neutral";
  if (value >= 0.6) return "ok";
  if (value >= 0.3) return "warning";
  return "danger";
}

function PulseCard({
  label,
  value,
  target,
  tone,
  icon,
}: {
  label: string;
  value: string;
  target: string;
  tone: Tone;
  icon: React.ReactNode;
}) {
  return (
    <div
      className="rounded-lg border px-3 py-3"
      style={{
        borderColor: "var(--stl-border)",
        background: "var(--stl-card-strong)",
      }}
    >
      <div
        className="flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wide"
        style={{ color: "var(--stl-muted)" }}
      >
        <span style={{ color: "var(--stl-accent)" }}>{icon}</span>
        {label}
      </div>
      <div
        className={`mt-1 text-2xl font-semibold leading-tight ${toneTextClass(tone)}`}
        style={{ fontFamily: "var(--font-metric)" }}
      >
        {value}
      </div>
      <div className="mt-0.5 text-[11px]" style={{ color: "var(--stl-muted)" }}>
        {target}
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// Band 3: TODAY — rep scorecard + critical exceptions
// ------------------------------------------------------------------

function TodayBand({
  reps,
  exceptions,
  filters,
  attribution,
  queryErrors,
}: {
  reps: DashboardRow[];
  exceptions: DashboardRow[];
  filters: DashboardFilters;
  attribution: DashboardRow | undefined;
  queryErrors?: Partial<Record<string, string>>;
}) {
  const activeExceptions = exceptions.filter(
    (row) =>
      (numberValue(row.open_unmatched_leads) ?? 0) > 0 &&
      stringValue(row.exception_key) !== "stale_no_touch",
  );
  // The rep query is now built from trigger_rollup (time-range-aware) so every
  // returned row already has at least one worked lead. Just pass them through.
  const activeReps = reps;
  const hasReps = activeReps.length > 0;

  return (
    <Band
      title="Reps & exceptions"
      caption={`Per-rep speed-to-lead activity in this window (${filters.timeRangeLabel}) and routing health`}
      icon={<Users className="h-4 w-4" aria-hidden />}
      rightSlot={
        <div className="flex items-center gap-2">
          <AttributionPill attribution={attribution} />
          {activeExceptions.length > 0 ? (
            <span className="shrink-0 rounded-md px-2 py-1 text-xs font-semibold" style={{
              background: "var(--stl-danger-soft)",
              color: "var(--stl-danger)",
            }}>
              {activeExceptions.length} flag{activeExceptions.length === 1 ? "" : "s"}
            </span>
          ) : (
            <span className="shrink-0 rounded-md px-2 py-1 text-xs font-semibold" style={{
              background: "var(--stl-accent-soft)",
              color: "var(--stl-accent)",
            }}>
              All green
            </span>
          )}
        </div>
      }
    >
      <BandErrors bandKey="today" errors={queryErrors} />
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        {hasReps ? (
          <RepScorecard rows={activeReps} />
        ) : (
          <EmptyRow message="No rep activity logged in this window. Either no leads landed, or no outbound touches were attributed to a named rep." />
        )}
        <CriticalExceptions rows={activeExceptions} />
      </div>
    </Band>
  );
}

function RepScorecard({ rows }: { rows: DashboardRow[] }) {
  const headers: { label: string; align: "left" | "right"; tooltip?: string }[] = [
    { label: "Rep", align: "left" },
    { label: "Role", align: "left" },
    { label: "Worked", align: "right", tooltip: "Lead events where this rep made the first outbound attempt" },
    { label: "Reached", align: "right", tooltip: "Of those, how many ended in an answered or completed call" },
    { label: "Bookings", align: "right", tooltip: "Of worked leads, how many were appointment bookings" },
    { label: "SLA", align: "right", tooltip: "% of worked leads where the first attempt landed within 45 minutes" },
    { label: "Avg speed", align: "right", tooltip: "Average minutes from trigger to first attempt" },
  ];
  return (
    <div
      className="overflow-hidden rounded-lg border"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <table className="w-full text-sm">
        <thead>
          <tr style={{ background: "var(--stl-soft)" }}>
            {headers.map((h) => (
              <th
                key={h.label}
                title={h.tooltip}
                className={`px-3 py-2 text-[11px] font-semibold uppercase tracking-wide ${
                  h.align === "right" ? "text-right" : "text-left"
                }`}
                style={{ color: "var(--stl-muted)" }}
              >
                {h.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => {
            const slaPct = numberValue(row.pct_within_sla);
            const tone = slaTonePct(slaPct);
            const isAutomation = row.is_automation === true || stringValue(row.rep_role) === "automation";
            return (
              <tr
                key={`${stringValue(row.rep_name)}-${idx}`}
                className="border-t"
                style={{ borderColor: "var(--stl-border)" }}
              >
                <td className="px-3 py-2 font-medium" style={{ color: "var(--stl-text)" }}>
                  <div className="flex items-center gap-2">
                    <span>{stringValue(row.rep_name) ?? "—"}</span>
                    {isAutomation ? (
                      <span
                        className="rounded-sm px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide"
                        style={{
                          background: "var(--stl-accent-soft)",
                          color: "var(--stl-accent)",
                        }}
                        title="Workflow automation, not a human rep"
                      >
                        Auto
                      </span>
                    ) : null}
                  </div>
                </td>
                <td className="px-3 py-2 text-xs" style={{ color: "var(--stl-muted)" }}>
                  {stringValue(row.rep_role) ?? "—"}
                </td>
                <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                  {formatNumber(numberValue(row.leads_worked))}
                </td>
                <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                  {formatNumber(numberValue(row.leads_reached))}
                </td>
                <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                  {formatNumber(numberValue(row.bookings_worked))}
                </td>
                <td className={`px-3 py-2 text-right font-semibold ${toneTextClass(tone)}`} style={{ fontFamily: "var(--font-metric)" }}>
                  {formatPercent(slaPct)}
                </td>
                <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                  {formatAge(numberValue(row.avg_speed_to_lead_minutes))}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function AttributionPill({ attribution }: { attribution: DashboardRow | undefined }) {
  const reached = numberValue(attribution?.reached_leads);
  const named = numberValue(attribution?.named_rep_reached);
  const needsMapping = numberValue(attribution?.needs_mapping);
  if (reached == null || reached === 0) return null;
  const namedRate = named != null ? named / reached : null;
  // Healthy when most reached calls are credited to named GHL users.
  const tone: Tone =
    namedRate == null
      ? "neutral"
      : namedRate >= 0.7
        ? "ok"
        : namedRate >= 0.4
          ? "warning"
          : "danger";
  return (
    <span
      className={`shrink-0 rounded-md px-2 py-1 text-xs font-semibold ${toneBadgeClass(tone)}`}
      title={`Of ${formatNumber(reached)} reached calls in this window: ${formatNumber(named)} credited to named GHL users (${formatPercent(namedRate)}), ${formatNumber(needsMapping)} on dialer lines or unmapped users.`}
    >
      <Radar className="mr-1 inline h-3 w-3" aria-hidden />
      {formatNumber(named)}/{formatNumber(reached)} named
    </span>
  );
}

function CriticalExceptions({ rows }: { rows: DashboardRow[] }) {
  if (rows.length === 0) {
    return (
      <div
        className="flex h-full items-center gap-3 rounded-lg border px-4 py-6"
        style={{
          borderColor: "var(--stl-border)",
          background: "var(--stl-accent-soft)",
        }}
      >
        <span
          className="flex h-8 w-8 items-center justify-center rounded-full"
          style={{ background: "var(--stl-accent)", color: "white" }}
        >
          <Activity className="h-4 w-4" aria-hidden />
        </span>
        <div>
          <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
            No exceptions in scope.
          </div>
          <div className="text-xs" style={{ color: "var(--stl-muted)" }}>
            Routing, attribution, and after-hours all clear.
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {rows.slice(0, 6).map((row, index) => {
        const openUnmatched = numberValue(row.open_unmatched_leads) ?? 0;
        const historicalUnmatched = numberValue(row.historical_unmatched_events) ?? 0;
        const oldestOpen = numberValue(row.oldest_open_age_hours);
        return (
          <div
            key={`${stringValue(row.exception_key) ?? stringValue(row.exception_label) ?? "exception"}-${index}`}
            className="rounded-lg border px-3 py-2"
            style={{
              borderColor: "var(--stl-border)",
              background: "var(--stl-card-strong)",
            }}
          >
            <div className="flex items-start gap-2">
              <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" style={{ color: "var(--stl-danger)" }} aria-hidden />
              <div className="min-w-0 flex-1">
                <div className="flex items-baseline justify-between gap-2">
                  <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
                    {stringValue(row.exception_label) ?? "Exception"}
                  </div>
                  <div className="shrink-0 text-xs" style={{ color: "var(--stl-muted)", fontFamily: "var(--font-metric)" }}>
                    {formatNumber(openUnmatched)} live unmatched
                    {oldestOpen != null && oldestOpen > 0 ? ` · oldest ${formatAge(oldestOpen * 60)}` : ""}
                  </div>
                </div>
                <div className="mt-1 text-xs leading-5" style={{ color: "var(--stl-muted)" }}>
                  {stringValue(row.manager_action) ?? ""}
                  {historicalUnmatched > 0 ? (
                    <span>
                      {" "}
                      Historical overlap lives in the audit below.
                    </span>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ------------------------------------------------------------------
// Band 4: TREND — collapsed by default
// ------------------------------------------------------------------

function TrendBand({
  daily,
  buckets,
  followUp,
  sources,
  unmatchedTruthAudit,
  typeformCoverage,
  typeformOutboundOpportunities,
  unmatchedCalendlySummary,
  unmatchedCalendlyInvitees,
  ghlMessageCoverage,
  ghlOutboundMessageBreakdown,
  queryErrors,
}: {
  daily: DashboardRow[];
  buckets: DashboardRow[];
  followUp: DashboardRow[];
  sources: DashboardRow[];
  unmatchedTruthAudit: DashboardRow[];
  typeformCoverage: DashboardRow[];
  typeformOutboundOpportunities: DashboardRow[];
  unmatchedCalendlySummary: DashboardRow[];
  unmatchedCalendlyInvitees: DashboardRow[];
  ghlMessageCoverage: DashboardRow[];
  ghlOutboundMessageBreakdown: DashboardRow[];
  queryErrors?: Partial<Record<string, string>>;
}) {
  const hasTrend = daily.length >= 2;

  const truthAuditFailed = hasError(queryErrors, "speed_to_lead_unmatched_truth_audit");
  const dataGapKeys = [
    "speed_to_lead_typeform_coverage",
    "speed_to_lead_typeform_outbound_opportunities",
    "speed_to_lead_unmatched_calendly_summary",
    "speed_to_lead_unmatched_calendly_invitees",
    "speed_to_lead_ghl_message_coverage",
    "speed_to_lead_ghl_outbound_message_breakdown",
  ] as const;
  const dataGapAnyFailed = dataGapKeys.some((k) => hasError(queryErrors, k));

  return (
    <Band
      title="Trend & audit"
      caption="Daily SLA, response distribution, source breakdown, follow-up reconciliation"
      icon={<Radio className="h-4 w-4" aria-hidden />}
      collapsible
      defaultOpen={hasTrend}
    >
      <BandErrors bandKey="trend" errors={queryErrors} />
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <DailyTrendChart rows={daily} />
        <ResponseBucketsChart rows={buckets} />
      </div>
      {sources.length > 0 ? (
        <div className="mt-4">
          <SourcePerformance rows={sources} />
        </div>
      ) : null}
      {dataGapAnyFailed ? null : (
        <div className="mt-4">
          <SpeedToLeadDataGapAudit
            typeformCoverage={typeformCoverage}
            typeformOutboundOpportunities={typeformOutboundOpportunities}
            unmatchedCalendlySummary={unmatchedCalendlySummary}
            unmatchedCalendlyInvitees={unmatchedCalendlyInvitees}
            ghlMessageCoverage={ghlMessageCoverage}
            ghlOutboundMessageBreakdown={ghlOutboundMessageBreakdown}
          />
        </div>
      )}
      {truthAuditFailed ? null : (
        <div className="mt-4">
          <UnmatchedTruthAudit rows={unmatchedTruthAudit} />
        </div>
      )}
      <div className="mt-4">
        <FollowUpAudit rows={followUp} />
      </div>
    </Band>
  );
}


function SpeedToLeadDataGapAudit({
  typeformCoverage,
  typeformOutboundOpportunities,
  unmatchedCalendlySummary,
  unmatchedCalendlyInvitees,
  ghlMessageCoverage,
  ghlOutboundMessageBreakdown,
}: {
  typeformCoverage: DashboardRow[];
  typeformOutboundOpportunities: DashboardRow[];
  unmatchedCalendlySummary: DashboardRow[];
  unmatchedCalendlyInvitees: DashboardRow[];
  ghlMessageCoverage: DashboardRow[];
  ghlOutboundMessageBreakdown: DashboardRow[];
}) {
  const typeform = typeformCoverage[0];
  const calendly = unmatchedCalendlySummary[0];
  const ghl = ghlMessageCoverage[0];
  const hasRows = Boolean(typeform || calendly || ghl || typeformOutboundOpportunities.length || unmatchedCalendlyInvitees.length || ghlOutboundMessageBreakdown.length);

  if (!hasRows) return null;

  return (
    <div
      className="overflow-hidden rounded-lg border"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="px-3 py-2" style={{ background: "var(--stl-soft)" }}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
              Source coverage gaps
            </div>
            <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
              Typeform no-book opportunities, Calendly invitees without contact matches, and GHL message-history ingestion coverage.
            </div>
          </div>
          <span
            className="rounded-md px-2 py-1 text-[11px] font-semibold"
            style={{ background: "var(--stl-danger-soft)", color: "var(--stl-danger)" }}
          >
            Data QA
          </span>
        </div>
      </div>

      <div className="grid gap-3 border-t p-3 md:grid-cols-3" style={{ borderColor: "var(--stl-border)" }}>
        <GapStat
          label="Typeform SDR opps"
          value={formatNumber(numberValue(typeform?.outbound_opportunities))}
          sublabel={`${formatNumber(numberValue(typeform?.responses))} fills, ${formatNumber(numberValue(typeform?.booked_after_typeform))} booked after`}
        />
        <GapStat
          label="Unmatched Calendly"
          value={formatNumber(numberValue(calendly?.unmatched_invitees))}
          sublabel={`${formatPercent(numberValue(calendly?.unmatched_rate))} of invitees missing GHL contact match`}
        />
        <GapStat
          label="Core outbound SMS"
          value={formatNumber(numberValue(ghl?.core_outbound_sms_rows))}
          sublabel={`${formatNumber(numberValue(ghl?.raw_message_rows))} raw message rows captured`}
        />
      </div>

      <div className="grid gap-4 border-t p-3 xl:grid-cols-3" style={{ borderColor: "var(--stl-border)" }}>
        <CompactAuditTable
          title="Typeform SDR opportunities"
          rows={typeformOutboundOpportunities.slice(0, 8)}
          empty="No matched Typeform fills without a later booking in this window."
          columns={[
            { key: "submitted_at_et", label: "Submitted" },
            { key: "lead_name", label: "Lead" },
            { key: "lead_email", label: "Email" },
            { key: "hours_since_submit", label: "Age", format: "number" },
          ]}
        />
        <CompactAuditTable
          title="Unmatched Calendly invitees"
          rows={unmatchedCalendlyInvitees.slice(0, 8)}
          empty="No unmatched Calendly invitees in this window."
          columns={[
            { key: "booked_at_et", label: "Booked" },
            { key: "invitee_name", label: "Invitee" },
            { key: "invitee_email", label: "Email" },
            { key: "event_name", label: "Event" },
          ]}
        />
        <CompactAuditTable
          title="GHL conversation breakdown"
          rows={ghlOutboundMessageBreakdown.slice(0, 8)}
          empty="No GHL conversation rows in this window."
          columns={[
            { key: "channel", label: "Channel" },
            { key: "direction", label: "Direction" },
            { key: "message_type", label: "Type" },
            { key: "row_count", label: "Rows", format: "number" },
          ]}
        />
      </div>
    </div>
  );
}

function GapStat({ label, value, sublabel }: { label: string; value: string; sublabel: string }) {
  return (
    <div className="rounded-md border px-3 py-2" style={{ borderColor: "var(--stl-border)", background: "var(--stl-card)" }}>
      <div className="text-[10px] font-semibold uppercase tracking-wide" style={{ color: "var(--stl-muted)" }}>
        {label}
      </div>
      <div className="mt-1 text-xl font-semibold" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
        {value}
      </div>
      <div className="mt-0.5 text-[11px]" style={{ color: "var(--stl-muted)" }}>
        {sublabel}
      </div>
    </div>
  );
}

type CompactAuditColumn = {
  key: string;
  label: string;
  format?: "number" | "percent";
};

function CompactAuditTable({
  title,
  rows,
  columns,
  empty,
}: {
  title: string;
  rows: DashboardRow[];
  columns: CompactAuditColumn[];
  empty: string;
}) {
  return (
    <div className="min-w-0 overflow-hidden rounded-md border" style={{ borderColor: "var(--stl-border)", background: "var(--stl-card)" }}>
      <div className="px-3 py-2 text-sm font-semibold" style={{ color: "var(--stl-text)", background: "var(--stl-soft)" }}>
        {title}
      </div>
      {rows.length === 0 ? (
        <div className="px-3 py-4 text-xs" style={{ color: "var(--stl-muted)" }}>
          {empty}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr style={{ background: "var(--stl-soft)" }}>
                {columns.map((column) => (
                  <th key={column.key} className="px-3 py-2 text-left font-semibold uppercase tracking-wide" style={{ color: "var(--stl-muted)" }}>
                    {column.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${title}-${index}`} className="border-t" style={{ borderColor: "var(--stl-border)" }}>
                  {columns.map((column) => (
                    <td key={column.key} className="max-w-40 px-3 py-2" style={{ color: "var(--stl-text)" }}>
                      <span className="block truncate">{formatAuditCell(row[column.key], column.format)}</span>
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function formatAuditCell(value: unknown, format?: "number" | "percent") {
  if (format === "number") return formatNumber(numberValue(value));
  if (format === "percent") return formatPercent(numberValue(value));
  if (typeof value === "number") return formatNumber(value);
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return stringValue(value) ?? "N/A";
}

function SourcePerformance({ rows }: { rows: DashboardRow[] }) {
  // Roll up trigger_type variants for the same source so the table is one row per source.
  const grouped = rows.reduce<Map<string, { triggers: number; touched: number; w5: number; w45: number; medians: number[] }>>(
    (acc, r) => {
      const key = stringValue(r.source_label) ?? "Unknown";
      const triggers = numberValue(r.total_triggers) ?? 0;
      const touched = numberValue(r.touched) ?? 0;
      const w5 = numberValue(r.within_5m) ?? 0;
      const w45 = numberValue(r.within_sla) ?? 0;
      const med = numberValue(r.median_minutes);
      const cur = acc.get(key) ?? { triggers: 0, touched: 0, w5: 0, w45: 0, medians: [] };
      cur.triggers += triggers;
      cur.touched += touched;
      cur.w5 += w5;
      cur.w45 += w45;
      if (med != null) cur.medians.push(med);
      acc.set(key, cur);
      return acc;
    },
    new Map(),
  );

  const sourceRows = Array.from(grouped.entries())
    .map(([source, agg]) => ({
      source,
      triggers: agg.triggers,
      touched: agg.touched,
      touch_rate: agg.triggers > 0 ? agg.touched / agg.triggers : null,
      within_5m_rate: agg.triggers > 0 ? agg.w5 / agg.triggers : null,
      within_sla_rate: agg.triggers > 0 ? agg.w45 / agg.triggers : null,
      median: agg.medians.length > 0 ? agg.medians.reduce((s, x) => s + x, 0) / agg.medians.length : null,
    }))
    .sort((a, b) => b.triggers - a.triggers)
    .slice(0, 8);

  return (
    <div
      className="overflow-hidden rounded-lg border"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="px-3 py-2" style={{ background: "var(--stl-soft)" }}>
        <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
          Source performance
        </div>
        <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
          Top {sourceRows.length} sources by volume in this window. Spot which channels we respond fastest to and which leak.
        </div>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr style={{ background: "var(--stl-soft)" }}>
            {[
              { label: "Source", align: "left" },
              { label: "Triggers", align: "right" },
              { label: "Touch %", align: "right" },
              { label: `Within ${STRICT_SLA_MIN}m`, align: "right" },
              { label: `Within ${BOOKING_SLA_MIN}m`, align: "right" },
              { label: "Median", align: "right" },
            ].map((h) => (
              <th
                key={h.label}
                className={`px-3 py-2 text-[11px] font-semibold uppercase tracking-wide ${
                  h.align === "right" ? "text-right" : "text-left"
                }`}
                style={{ color: "var(--stl-muted)" }}
              >
                {h.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sourceRows.map((row) => (
            <tr key={row.source} className="border-t" style={{ borderColor: "var(--stl-border)" }}>
              <td className="px-3 py-2 font-medium" style={{ color: "var(--stl-text)" }}>
                {row.source}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(row.triggers)}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatPercent(row.touch_rate)}
              </td>
              <td className={`px-3 py-2 text-right font-semibold ${toneTextClass(strictTonePct(row.within_5m_rate))}`} style={{ fontFamily: "var(--font-metric)" }}>
                {formatPercent(row.within_5m_rate)}
              </td>
              <td className={`px-3 py-2 text-right font-semibold ${toneTextClass(slaTonePct(row.within_sla_rate))}`} style={{ fontFamily: "var(--font-metric)" }}>
                {formatPercent(row.within_sla_rate)}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatAge(row.median)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DailyTrendChart({ rows }: { rows: DashboardRow[] }) {
  const data = rows
    .map((r) => ({
      date: stringValue(r.report_date) ?? "",
      sla: Math.round(((numberValue(r.pct_within_sla) ?? 0) * 1000)) / 10,
      touched: Math.round(((numberValue(r.pct_triggers_with_outbound_touch) ?? 0) * 1000)) / 10,
    }))
    .filter((d) => d.date);

  if (data.length < 2) {
    return (
      <div
        className="rounded-lg border px-4 py-6 text-sm"
        style={{
          borderColor: "var(--stl-border)",
          background: "var(--stl-card-strong)",
          color: "var(--stl-muted)",
        }}
      >
        Not enough daily rows to plot a trend yet. Widen the time range.
      </div>
    );
  }

  return (
    <div
      className="rounded-lg border px-3 py-3"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="mb-2 flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
            Daily SLA
          </div>
          <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
            % of bookings reached within {BOOKING_SLA_MIN} minutes
          </div>
        </div>
      </div>
      <div className="h-48">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 6, right: 8, bottom: 0, left: -12 }}>
            <CartesianGrid stroke="var(--stl-border)" strokeDasharray="2 4" vertical={false} />
            <XAxis
              dataKey="date"
              tickFormatter={(v: string) => v.slice(5)}
              tick={{ fontSize: 10, fill: "var(--stl-muted)" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              domain={[0, 100]}
              tick={{ fontSize: 10, fill: "var(--stl-muted)" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: number) => `${v}%`}
            />
            <Tooltip
              contentStyle={{
                background: "var(--stl-card-strong)",
                border: "1px solid var(--stl-border)",
                borderRadius: 6,
                fontSize: 12,
              }}
              formatter={(value, name) => [`${value}%`, name === "sla" ? "Within 45m" : "Touched"]}
            />
            <Line
              type="monotone"
              dataKey="sla"
              stroke="var(--stl-accent)"
              strokeWidth={2}
              dot={{ r: 2, fill: "var(--stl-accent)" }}
            />
            <Line
              type="monotone"
              dataKey="touched"
              stroke="var(--stl-champagne)"
              strokeWidth={1.5}
              strokeDasharray="3 3"
              dot={false}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function ResponseBucketsChart({ rows }: { rows: DashboardRow[] }) {
  const order = ["<=1m", "1-5m", "5-15m", "15-60m", "1-24h", ">24h", "no touch"];
  const totals = order.map((bucket) => ({
    bucket,
    triggers: rows
      .filter((r) => stringValue(r.response_bucket) === bucket)
      .reduce((sum, r) => sum + (numberValue(r.triggers) ?? 0), 0),
  }));
  const total = totals.reduce((s, b) => s + b.triggers, 0);
  if (total === 0) {
    return (
      <div
        className="rounded-lg border px-4 py-6 text-sm"
        style={{
          borderColor: "var(--stl-border)",
          background: "var(--stl-card-strong)",
          color: "var(--stl-muted)",
        }}
      >
        No response distribution yet.
      </div>
    );
  }
  return (
    <div
      className="rounded-lg border px-3 py-3"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="mb-2 flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
            Response distribution
          </div>
          <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
            How long lead events sit before the first attempt
          </div>
        </div>
      </div>
      <div className="h-48">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={totals} margin={{ top: 6, right: 8, bottom: 0, left: -16 }}>
            <CartesianGrid stroke="var(--stl-border)" strokeDasharray="2 4" vertical={false} />
            <XAxis
              dataKey="bucket"
              tick={{ fontSize: 10, fill: "var(--stl-muted)" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 10, fill: "var(--stl-muted)" }}
              axisLine={false}
              tickLine={false}
              allowDecimals={false}
            />
            <Tooltip
              contentStyle={{
                background: "var(--stl-card-strong)",
                border: "1px solid var(--stl-border)",
                borderRadius: 6,
                fontSize: 12,
              }}
              formatter={(value) => [`${value}`, "Lead events"]}
            />
            <Bar dataKey="triggers" fill="var(--stl-accent)" radius={[3, 3, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function UnmatchedTruthAudit({ rows }: { rows: DashboardRow[] }) {
  if (rows.length === 0) {
    return null;
  }

  const all = rows.find((row) => stringValue(row.lane_id) === "all") ?? rows[0];
  const unmatched = numberValue(all?.unmatched_events);
  const crmProgress = numberValue(all?.any_crm_progress_evidence);
  const strongProof = numberValue(all?.strong_work_or_progress_evidence);
  const noProof = numberValue(all?.no_external_progress_evidence);
  const progressRate = numberValue(all?.crm_progress_evidence_rate);
  const firstDate = stringValue(all?.first_trigger_date);
  const lastDate = stringValue(all?.last_trigger_date);

  return (
    <div
      className="overflow-hidden rounded-lg border"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="px-3 py-2" style={{ background: "var(--stl-soft)" }}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
              Unmatched truth check
            </div>
            <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
              These are trigger events with no qualifying post-trigger outbound touch found. They are not automatically &quot;never worked.&quot;
            </div>
          </div>
          <span
            className="rounded-md px-2 py-1 text-[11px] font-semibold"
            style={{ background: "var(--stl-warning-soft)", color: "var(--stl-warning)" }}
          >
            Attribution audit
          </span>
        </div>
      </div>

      <div className="grid gap-3 border-t p-3 sm:grid-cols-2 lg:grid-cols-4" style={{ borderColor: "var(--stl-border)" }}>
        <TruthStat
          label="Unmatched events"
          value={formatNumber(unmatched)}
          sublabel={firstDate && lastDate ? `${firstDate} to ${lastDate}` : "No post-trigger touch proof"}
        />
        <TruthStat
          label="CRM progress evidence"
          value={formatNumber(crmProgress)}
          sublabel={`${formatPercent(progressRate)} have meetings, opps, payments, or owners`}
        />
        <TruthStat
          label="Strong proof"
          value={formatNumber(strongProof)}
          sublabel="Showed meeting, payment, or owner exists"
        />
        <TruthStat
          label="Needs real audit"
          value={formatNumber(noProof)}
          sublabel="No CRM progress proof found either"
        />
      </div>

      <table className="w-full text-sm">
        <thead>
          <tr style={{ background: "var(--stl-soft)" }}>
            {[
              { label: "Slice", align: "left" },
              { label: "Events", align: "right" },
              { label: "Contacts", align: "right" },
              { label: "CRM proof", align: "right" },
              { label: "Strong proof", align: "right" },
              { label: "Captured ever", align: "right" },
              { label: "Captured after", align: "right" },
              { label: "No proof", align: "right" },
            ].map((h) => (
              <th
                key={h.label}
                className={`px-3 py-2 text-[11px] font-semibold uppercase tracking-wide ${
                  h.align === "right" ? "text-right" : "text-left"
                }`}
                style={{ color: "var(--stl-muted)" }}
              >
                {h.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr
              key={stringValue(row.lane_id) ?? stringValue(row.truth_slice) ?? "truth-row"}
              className="border-t"
              style={{ borderColor: "var(--stl-border)" }}
            >
              <td className="px-3 py-2 font-medium" style={{ color: "var(--stl-text)" }}>
                {stringValue(row.truth_slice) ?? "—"}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.unmatched_events))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.distinct_contacts))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-accent)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.any_crm_progress_evidence))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.strong_work_or_progress_evidence))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-muted)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.had_captured_call_or_message_ever))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-muted)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.had_captured_call_or_message_after_trigger))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-danger)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.no_external_progress_evidence))}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function TruthStat({ label, value, sublabel }: { label: string; value: string; sublabel: string }) {
  return (
    <div className="rounded-md border px-3 py-2" style={{ borderColor: "var(--stl-border)", background: "var(--stl-card)" }}>
      <div className="text-[10px] font-semibold uppercase tracking-wide" style={{ color: "var(--stl-muted)" }}>
        {label}
      </div>
      <div className="mt-1 text-xl font-semibold" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
        {value}
      </div>
      <div className="mt-0.5 text-[11px]" style={{ color: "var(--stl-muted)" }}>
        {sublabel}
      </div>
    </div>
  );
}

function FollowUpAudit({ rows }: { rows: DashboardRow[] }) {
  if (rows.length === 0) {
    return null;
  }
  return (
    <div
      className="overflow-hidden rounded-lg border"
      style={{ borderColor: "var(--stl-border)", background: "var(--stl-card-strong)" }}
    >
      <div className="px-3 py-2" style={{ background: "var(--stl-soft)" }}>
        <div className="text-sm font-semibold" style={{ color: "var(--stl-text)" }}>
          Follow-up reconciliation
        </div>
        <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
          Single source of truth for every percentage on this page. If two panels disagree, this table is canonical.
        </div>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr style={{ background: "var(--stl-soft)" }}>
            {["Metric", "Lead events", "Out of", "% of all", "% of worked", "Plain English"].map((h, i) => (
              <th
                key={h}
                className={`px-3 py-2 text-[11px] font-semibold uppercase tracking-wide ${
                  i === 0 || i === 5 ? "text-left" : "text-right"
                }`}
                style={{ color: "var(--stl-muted)" }}
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx} className="border-t" style={{ borderColor: "var(--stl-border)" }}>
              <td className="px-3 py-2 font-medium" style={{ color: "var(--stl-text)" }}>
                {stringValue(row.metric) ?? "—"}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.lead_count))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-muted)", fontFamily: "var(--font-metric)" }}>
                {formatNumber(numberValue(row.denominator_count))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatPercent(numberValue(row.share_of_all_leads))}
              </td>
              <td className="px-3 py-2 text-right" style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}>
                {formatPercent(numberValue(row.share_of_worked_leads))}
              </td>
              <td className="px-3 py-2 text-xs" style={{ color: "var(--stl-muted)" }}>
                {stringValue(row.plain_english) ?? ""}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ------------------------------------------------------------------
// Shared
// ------------------------------------------------------------------

function EmptyRow({ message, icon = "info" }: { message: string; icon?: "ok" | "info" }) {
  return (
    <div
      className="flex items-center gap-3 rounded-md border px-4 py-6 text-sm"
      style={{
        borderColor: "var(--stl-border)",
        background: "var(--stl-card-strong)",
        color: "var(--stl-muted)",
      }}
    >
      <span
        className="flex h-7 w-7 items-center justify-center rounded-full"
        style={{
          background: icon === "ok" ? "var(--stl-accent-soft)" : "var(--stl-neutral-chip)",
          color: icon === "ok" ? "var(--stl-accent)" : "var(--stl-muted)",
        }}
      >
        {icon === "ok" ? <Activity className="h-3.5 w-3.5" aria-hidden /> : <Phone className="h-3.5 w-3.5" aria-hidden />}
      </span>
      <span>{message}</span>
    </div>
  );
}

// ------------------------------------------------------------------
// Tone + formatting helpers
// ------------------------------------------------------------------

type Tone = "ok" | "warning" | "danger" | "stale" | "neutral";

function slaTonePct(value: number | null | undefined): Tone {
  if (value == null) return "neutral";
  if (value >= 0.8) return "ok";
  if (value >= 0.5) return "warning";
  return "danger";
}

function strictTonePct(value: number | null | undefined): Tone {
  if (value == null) return "neutral";
  if (value >= 0.5) return "ok";
  if (value >= 0.2) return "warning";
  return "danger";
}

function toneTextClass(tone: Tone) {
  switch (tone) {
    case "ok":
      return "text-[var(--stl-accent)]";
    case "warning":
      return "text-[var(--stl-warning)]";
    case "danger":
      return "text-[var(--stl-danger)]";
    case "stale":
      return "text-[var(--stl-muted)]";
    default:
      return "text-[var(--stl-text)]";
  }
}

function toneBadgeClass(tone: Tone) {
  switch (tone) {
    case "ok":
      return "bg-[var(--stl-accent-soft)] text-[var(--stl-accent)]";
    case "warning":
      return "bg-[var(--stl-warning-soft)] text-[var(--stl-warning)]";
    case "danger":
      return "bg-[var(--stl-danger-soft)] text-[var(--stl-danger)]";
    case "stale":
      return "bg-[var(--stl-neutral-chip)] text-[var(--stl-muted)]";
    default:
      return "bg-[var(--stl-neutral-chip)] text-[var(--stl-muted)]";
  }
}

function toneRingColor(tone: Tone) {
  switch (tone) {
    case "ok":
      return "var(--stl-accent)";
    case "warning":
      return "var(--stl-warning)";
    case "danger":
      return "var(--stl-danger)";
    case "stale":
      return "var(--stl-muted)";
    default:
      return "var(--stl-muted)";
  }
}

function toneBadgeLabel(tone: Tone): string {
  switch (tone) {
    case "ok":
      return "Fresh";
    case "warning":
      return "Past 5m";
    case "danger":
      return "Over SLA";
    case "stale":
      return "Aged";
    default:
      return "—";
  }
}

function ageToTone(ageMinutes: number | null): Tone {
  if (ageMinutes == null) return "neutral";
  if (ageMinutes > 60 * 24) return "stale";
  if (ageMinutes > BOOKING_SLA_MIN) return "danger";
  if (ageMinutes > STRICT_SLA_MIN) return "warning";
  return "ok";
}

export function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

export function stringValue(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  return undefined;
}

export function formatNumber(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(value >= 0.1 ? 0 : 1)}%`;
}

export function formatAge(minutes: number | null | undefined): string {
  if (minutes == null || !Number.isFinite(minutes)) return "—";
  const m = Math.max(0, minutes);
  if (m < 1) return "<1m";
  if (m < 60) return `${Math.round(m)}m`;
  if (m < 60 * 24) return `${Math.round(m / 60)}h`;
  return `${Math.round(m / 60 / 24)}d`;
}

function formatTriggerType(value: string): string {
  if (value === "appointment_booking") return "Bookings";
  if (value === "lead_magnet") return "Magnets";
  return value
    .split(/[_\s]+/)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}
