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
  ShieldCheck,
  TrendingDown,
  TrendingUp,
  Users,
  Zap,
} from "lucide-react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
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

const BOOKING_SLA_MIN = 45;
const STRICT_SLA_MIN = 5;

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
  const quality = (data.rows.speed_to_lead_quality_summary ?? [])[0];
  const attribution = (data.rows.speed_to_lead_attribution_confidence ?? [])[0];
  const lanes = data.rows.speed_to_lead_lane_summary ?? [];
  const queue = data.rows.speed_to_lead_no_touch_examples ?? [];
  const reps = data.rows.speed_to_lead_by_rep ?? [];
  const exceptions = data.rows.speed_to_lead_critical_exceptions ?? [];
  const daily = data.rows.speed_to_lead_daily ?? [];
  const buckets = data.rows.speed_to_lead_response_buckets ?? [];
  const followUp = data.rows.speed_to_lead_follow_up_counts ?? [];
  const unmatchedTruthAudit = data.rows.speed_to_lead_unmatched_truth_audit ?? [];
  const triggers = data.rows.speed_to_lead_trigger_summary ?? [];
  const sources = data.rows.speed_to_lead_source_performance ?? [];
  const typeformCoverage = data.rows.speed_to_lead_typeform_coverage ?? [];
  const typeformOutboundOpportunities = data.rows.speed_to_lead_typeform_outbound_opportunities ?? [];
  const unmatchedCalendlySummary = data.rows.speed_to_lead_unmatched_calendly_summary ?? [];
  const unmatchedCalendlyInvitees = data.rows.speed_to_lead_unmatched_calendly_invitees ?? [];
  const ghlMessageCoverage = data.rows.speed_to_lead_ghl_message_coverage ?? [];
  const ghlOutboundMessageBreakdown = data.rows.speed_to_lead_ghl_outbound_message_breakdown ?? [];

  return (
    <div className="space-y-4 pb-12">
      <Header
        filters={data.filters}
        freshness={data.freshness}
        overall={overall}
        overallPrior={overallPrior}
      />

      {data.error ? (
        <div className="rounded-lg border border-[var(--stl-danger)] bg-[var(--stl-danger-soft)] px-4 py-3 text-sm text-[var(--stl-text)]">
          {data.error}
        </div>
      ) : null}

      <NowBand queue={queue} lanes={lanes} timeRange={data.filters.timeRange} />
      <HourPulseBand overall={overall} triggers={triggers} quality={quality} />
      <TodayBand
        reps={reps}
        exceptions={exceptions}
        filters={data.filters}
        attribution={attribution}
      />
      <TrendBand
        daily={daily}
        buckets={buckets}
        followUp={followUp}
        sources={sources}
        unmatchedTruthAudit={unmatchedTruthAudit}
        typeformCoverage={typeformCoverage}
        typeformOutboundOpportunities={typeformOutboundOpportunities}
        unmatchedCalendlySummary={unmatchedCalendlySummary}
        unmatchedCalendlyInvitees={unmatchedCalendlyInvitees}
        ghlMessageCoverage={ghlMessageCoverage}
        ghlOutboundMessageBreakdown={ghlOutboundMessageBreakdown}
      />
    </div>
  );
}

// ------------------------------------------------------------------
// Header
// ------------------------------------------------------------------

function Header({
  filters,
  freshness,
  overall,
  overallPrior,
}: {
  filters: DashboardFilters;
  freshness: DashboardFreshness;
  overall: DashboardRow | undefined;
  overallPrior: DashboardRow | undefined;
}) {
  const slaPct = numberValue(overall?.pct_within_sla);
  const slaPctPrior = numberValue(overallPrior?.pct_within_sla);
  const coveragePct = numberValue(overall?.pct_triggers_with_outbound_touch);
  const coveragePctPrior = numberValue(overallPrior?.pct_triggers_with_outbound_touch);
  const totalEvents = numberValue(overall?.total_triggers_all);

  return (
    <header
      className="rounded-xl border px-4 py-3 shadow-sm md:px-5"
      style={{
        background: "var(--stl-card-strong)",
        borderColor: "var(--stl-border)",
        boxShadow: "var(--stl-shadow-soft)",
      }}
    >
      <div className="grid gap-3 md:grid-cols-[auto_1fr_auto] md:items-center">
        <div className="flex items-center gap-3">
          <span
            className="flex h-9 w-9 items-center justify-center rounded-md text-white"
            style={{ background: "var(--stl-accent)" }}
          >
            <Gauge className="h-4 w-4" aria-hidden />
          </span>
          <div>
            <h1
              className="text-xl font-semibold tracking-tight md:text-2xl"
              style={{ color: "var(--stl-text)", fontFamily: "var(--font-display)" }}
            >
              Speed-to-Lead
            </h1>
            <FreshnessLine freshness={freshness} />
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-start gap-x-6 gap-y-2 md:justify-center">
          <HeroStat
            label="Coverage"
            value={formatPercent(coveragePct)}
            tone={coverageTone(coveragePct)}
            sublabel={`${filters.timeRangeLabel} · ${formatNumber(totalEvents)} lead events`}
            delta={pointsDelta(coveragePct, coveragePctPrior)}
            icon={<ShieldCheck className="h-3.5 w-3.5" aria-hidden />}
          />
          <span
            className="hidden h-10 w-px md:inline-block"
            style={{ background: "var(--stl-border)" }}
            aria-hidden
          />
          <HeroStat
            label={`Within ${BOOKING_SLA_MIN}m SLA`}
            value={formatPercent(slaPct)}
            tone={slaTonePct(slaPct)}
            sublabel={`${formatNumber(numberValue(overall?.total_bookings_matched_to_contact))} bookings in window`}
            delta={pointsDelta(slaPct, slaPctPrior)}
            icon={<Gauge className="h-3.5 w-3.5" aria-hidden />}
          />
        </div>

        <TimeRange filters={filters} />
      </div>
    </header>
  );
}

type Delta = { points: number; available: true } | { available: false };

function pointsDelta(current: number | null, prior: number | null): Delta {
  if (current == null || prior == null) return { available: false };
  return { points: (current - prior) * 100, available: true };
}

function HeroStat({
  label,
  value,
  tone,
  sublabel,
  delta,
  icon,
}: {
  label: string;
  value: string;
  tone: Tone;
  sublabel: string;
  delta?: Delta;
  icon?: React.ReactNode;
}) {
  return (
    <div className="flex items-center gap-3">
      <span
        className="flex h-7 w-7 items-center justify-center rounded-md"
        style={{ background: "var(--stl-accent-soft)", color: "var(--stl-accent)" }}
        aria-hidden
      >
        {icon ?? <span className={`h-2 w-2 rounded-full ${toneDotClass(tone)}`} />}
      </span>
      <div>
        <div
          className="text-[10px] font-semibold uppercase tracking-wide"
          style={{ color: "var(--stl-muted)" }}
        >
          {label}
        </div>
        <div className="flex items-baseline gap-2">
          <div
            className={`text-2xl font-semibold leading-tight ${toneTextClass(tone)}`}
            style={{ fontFamily: "var(--font-metric)" }}
          >
            {value}
          </div>
          {delta?.available ? <DeltaPill points={delta.points} /> : null}
        </div>
        <div className="text-[11px]" style={{ color: "var(--stl-muted)" }}>
          {sublabel}
        </div>
      </div>
    </div>
  );
}

function DeltaPill({ points }: { points: number }) {
  const rounded = Math.round(points * 10) / 10;
  if (Math.abs(rounded) < 0.1) {
    return (
      <span
        className="inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[10px] font-semibold"
        style={{ background: "var(--stl-neutral-chip)", color: "var(--stl-muted)" }}
        title="No change vs the prior window of the same length"
      >
        flat
      </span>
    );
  }
  const isUp = rounded > 0;
  return (
    <span
      className="inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 text-[10px] font-semibold"
      style={{
        background: isUp ? "var(--stl-accent-soft)" : "var(--stl-danger-soft)",
        color: isUp ? "var(--stl-accent)" : "var(--stl-danger)",
      }}
      title={`${isUp ? "Up" : "Down"} ${Math.abs(rounded)} points vs the prior window of the same length`}
    >
      {isUp ? (
        <TrendingUp className="h-3 w-3" aria-hidden />
      ) : (
        <TrendingDown className="h-3 w-3" aria-hidden />
      )}
      {isUp ? "+" : ""}
      {rounded.toFixed(1)}pt
    </span>
  );
}

function coverageTone(value: number | null | undefined): Tone {
  if (value == null) return "neutral";
  if (value >= 0.7) return "ok";
  if (value >= 0.4) return "warning";
  return "danger";
}

function FreshnessLine({ freshness }: { freshness: DashboardFreshness }) {
  const isLive = freshness.status === "live";
  return (
    <div className="mt-1 flex items-center gap-1.5 text-[11px]" style={{ color: "var(--stl-muted)" }}>
      <span
        className={`h-1.5 w-1.5 rounded-full ${isLive ? "bg-[var(--stl-accent)]" : "bg-[var(--warning)]"}`}
        aria-hidden
      />
      <span>{freshness.detail}</span>
    </div>
  );
}

function TimeRange({ filters }: { filters: DashboardFilters }) {
  return (
    <div
      className="flex w-full items-center gap-1 rounded-md border p-1 md:w-auto"
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
            className={`min-w-10 rounded px-2.5 py-1 text-center text-xs font-semibold transition ${
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
}: {
  queue: DashboardRow[];
  lanes: DashboardRow[];
  timeRange: string;
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
}: {
  overall: DashboardRow | undefined;
  triggers: DashboardRow[];
  quality: DashboardRow | undefined;
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
}: {
  reps: DashboardRow[];
  exceptions: DashboardRow[];
  filters: DashboardFilters;
  attribution: DashboardRow | undefined;
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
}) {
  const hasTrend = daily.length >= 2;
  return (
    <Band
      title="Trend & audit"
      caption="Daily SLA, response distribution, source breakdown, follow-up reconciliation"
      icon={<Radio className="h-4 w-4" aria-hidden />}
      collapsible
      defaultOpen={hasTrend}
    >
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.4fr)_minmax(0,1fr)]">
        <DailyTrendChart rows={daily} />
        <ResponseBucketsChart rows={buckets} />
      </div>
      {sources.length > 0 ? (
        <div className="mt-4">
          <SourcePerformance rows={sources} />
        </div>
      ) : null}
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
      <div className="mt-4">
        <UnmatchedTruthAudit rows={unmatchedTruthAudit} />
      </div>
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
      return "text-[var(--warning)]";
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
      return "bg-[var(--stl-champagne-soft)] text-[var(--warning)]";
    case "danger":
      return "bg-[var(--stl-danger-soft)] text-[var(--stl-danger)]";
    case "stale":
      return "bg-[var(--stl-neutral-chip)] text-[var(--stl-muted)]";
    default:
      return "bg-[var(--stl-neutral-chip)] text-[var(--stl-muted)]";
  }
}

function toneDotClass(tone: Tone) {
  switch (tone) {
    case "ok":
      return "bg-[var(--stl-accent)]";
    case "warning":
      return "bg-[var(--warning)]";
    case "danger":
      return "bg-[var(--stl-danger)]";
    case "stale":
      return "bg-[var(--stl-muted)]";
    default:
      return "bg-[var(--stl-muted)]";
  }
}

function toneRingColor(tone: Tone) {
  switch (tone) {
    case "ok":
      return "var(--stl-accent)";
    case "warning":
      return "var(--warning)";
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

function numberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function stringValue(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  return undefined;
}

function formatNumber(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US").format(value);
}

function formatPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(value >= 0.1 ? 0 : 1)}%`;
}

function formatAge(minutes: number | null | undefined): string {
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
