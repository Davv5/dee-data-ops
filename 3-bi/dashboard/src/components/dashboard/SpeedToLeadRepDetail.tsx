"use client";

import Link from "next/link";
import type { DashboardData, DashboardRow } from "@/types/dashboard-data";
import {
  HeroSLACard,
  RangePicker,
  SLA_TARGET,
  computeMedian,
  dailyMetricValues,
  formatAge,
  formatNumber,
  formatPctDelta,
  formatPercent,
  isRealRep,
  numberValue,
  stringValue,
} from "@/components/dashboard/SpeedToLeadDashboard";

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const _retainImports = { dailyMetricValues, SLA_TARGET };

export function SpeedToLeadRepDetail({
  data,
  repName,
}: {
  data: DashboardData;
  repName: string;
}) {
  const allRepRows = data.rows.speed_to_lead_by_rep ?? [];
  const daily = data.rows.speed_to_lead_daily ?? [];
  const allLeadRows = data.rows.speed_to_lead_leads_by_rep ?? [];

  const realReps = allRepRows.filter(isRealRep);
  const repRow = realReps.find((row) => stringValue(row.rep_name) === repName) ?? null;
  const repLeads = allLeadRows.filter((r) => stringValue(r.rep_name) === repName);

  const backHref = `/speed-to-lead?range=${data.filters.timeRange}`;
  const selfBasePath = `/speed-to-lead/rep/${encodeURIComponent(repName)}`;

  return (
    <div className="stl-page stl-od-page space-y-5 pb-12">
      <div className="od-page-head">
        <div className="od-title">
          <Link href={backHref} prefetch={false} className="stl-back-link">
            ← Back to team
          </Link>
          <h1>{repName}</h1>
          <p className="od-blurb">{data.filters.timeRangeLabel} · individual SDR performance</p>
        </div>
        <RangePicker current={data.filters.timeRange} basePath={selfBasePath} />
      </div>

      {repRow === null ? (
        <div className="od-callout od-callout-danger">
          No activity found for &quot;{repName}&quot; in this window. Try a wider date range.
        </div>
      ) : (
        <>
          <HeroSLACard
            slaPct={numberValue(repRow.pct_within_sla)}
            slaPctPrior={null}
            totalLeads={numberValue(repRow.leads_worked)}
            daily={daily}
            totalLeadsLabel="leads worked"
          />
          <RepMetricsStrip repRow={repRow} />
          <RepTeamComparison repRow={repRow} allReps={realReps} />
          <RepActivityByHour leads={repLeads} />
          <RepChannelMix leads={repLeads} />
          <RepLeadsTable leads={repLeads} />
        </>
      )}
    </div>
  );
}

function RepMetricsStrip({ repRow }: { repRow: DashboardRow }) {
  const leadsWorked = numberValue(repRow.leads_worked);
  const leadsReached = numberValue(repRow.leads_reached);
  const bookingsWorked = numberValue(repRow.bookings_worked);
  const avgSpeed = numberValue(repRow.avg_speed_to_lead_minutes);

  return (
    <div className="od-kpi-strip rep-metrics-strip">
      <RepKpi label="Leads worked" value={formatNumber(leadsWorked)} note="taken from queue" />
      <RepKpi
        label="Reached"
        value={formatNumber(leadsReached)}
        denominator={leadsWorked}
        note="call ≥30s or SMS reply"
      />
      <RepKpi
        label="Bookings worked"
        value={formatNumber(bookingsWorked)}
        denominator={leadsWorked}
        note="inbound bookings handled"
      />
      <RepKpi label="Avg first-touch" value={formatAge(avgSpeed)} note="seconds-to-minutes" />
    </div>
  );
}

function RepKpi({
  label,
  value,
  note,
  denominator,
}: {
  label: string;
  value: string;
  note: string;
  denominator?: number | null;
}) {
  return (
    <div className="od-kpi">
      <span className="label">{label}</span>
      <div className="value">
        {value}
        {denominator !== undefined && denominator !== null && denominator > 0 ? (
          <span className="denominator"> / {formatNumber(denominator)}</span>
        ) : null}
      </div>
      <div className="gap-note">{note}</div>
    </div>
  );
}

// ------------------------------------------------------------------
// RepActivityByHour — 24 hourly buckets showing when the rep was working.
// Distinguishes business-hours (9a–6p local) from after-hours. Derived
// client-side from speed_to_lead_leads_by_rep rows.
// ------------------------------------------------------------------

const BUSINESS_HOURS_START = 9; // inclusive
const BUSINESS_HOURS_END = 18; // exclusive

function RepActivityByHour({ leads }: { leads: DashboardRow[] }) {
  const buckets = Array.from({ length: 24 }, () => 0);
  for (const lead of leads) {
    const hour = numberValue(lead.trigger_hour_local);
    if (hour !== null && hour >= 0 && hour < 24) {
      buckets[Math.floor(hour)] += 1;
    }
  }
  const max = Math.max(1, ...buckets);
  const bhCount = buckets
    .slice(BUSINESS_HOURS_START, BUSINESS_HOURS_END)
    .reduce((a, b) => a + b, 0);
  const ohCount = leads.length - bhCount;

  if (leads.length === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title">
            <span className="dot" /> Activity by hour
          </span>
        </div>
        <div className="od-empty-state m-3">No lead-level activity in this window.</div>
      </div>
    );
  }

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Activity by hour
        </span>
        <span className="od-panel-meta">
          {formatNumber(bhCount)} BH · {formatNumber(ohCount)} OOH
        </span>
      </div>
      <div className="od-panel-body">
        <div className="rep-activity-strip">
          {buckets.map((count, hour) => {
            const heightPct = max > 0 ? (count / max) * 100 : 0;
            const isBh = hour >= BUSINESS_HOURS_START && hour < BUSINESS_HOURS_END;
            return (
              <div key={hour} className="rep-activity-col">
                <div className="rep-activity-bar-track">
                  <div
                    className={`rep-activity-bar${isBh ? " bh" : " ooh"}`}
                    style={{ height: `${heightPct}%` }}
                    title={`${count} lead${count === 1 ? "" : "s"} at ${hour}:00`}
                  />
                </div>
                <div className="rep-activity-hour-label">{hour % 6 === 0 ? hour : ""}</div>
              </div>
            );
          })}
        </div>
      </div>
      <div className="od-panel-footer">
        <span className="od-ev">
          Bars show first-touch counts by hour of day (America/Chicago). Solid bars are business hours
          (9a–6p); muted bars are after-hours. Front-loaded patterns can indicate queue-clearing; flat
          patterns indicate steady work.
        </span>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// RepChannelMix — breakdown by lead source. Reveals cherry-picking
// (one channel hammered, another ignored) or per-source response gaps.
// ------------------------------------------------------------------

const BOOKING_SLA_SECONDS = 15 * 60;

function RepChannelMix({ leads }: { leads: DashboardRow[] }) {
  // Group by trigger_source_label (more specific than trigger_type)
  const byChannel = new Map<string, { count: number; withinSla: number }>();
  for (const lead of leads) {
    const channel = stringValue(lead.trigger_source_label) ?? "Unknown";
    const secs = numberValue(lead.seconds_to_first_attempt);
    const hit = secs !== null && secs <= BOOKING_SLA_SECONDS;
    const existing = byChannel.get(channel) ?? { count: 0, withinSla: 0 };
    existing.count += 1;
    if (hit) existing.withinSla += 1;
    byChannel.set(channel, existing);
  }

  if (byChannel.size === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title">
            <span className="dot" /> Channel mix
          </span>
        </div>
        <div className="od-empty-state m-3">No leads in this window.</div>
      </div>
    );
  }

  const rows = Array.from(byChannel.entries())
    .map(([channel, stats]) => ({
      channel,
      count: stats.count,
      withinSla: stats.withinSla,
      slaRate: stats.count > 0 ? stats.withinSla / stats.count : null,
    }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 8);

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Channel mix
        </span>
        <span className="od-panel-meta">
          {byChannel.size} {byChannel.size === 1 ? "source" : "sources"} touched
        </span>
      </div>
      <div className="od-panel-body flush">
        <table className="od-table compact rep-channel-table">
          <thead>
            <tr>
              <th>Source</th>
              <th className="num">Leads</th>
              <th className="num">Within 15m</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const tone =
                row.slaRate === null
                  ? "flat"
                  : row.slaRate >= SLA_TARGET
                    ? "up"
                    : row.slaRate >= SLA_TARGET / 2
                      ? "flat"
                      : "down";
              return (
                <tr key={row.channel}>
                  <td>{row.channel}</td>
                  <td className="num">{formatNumber(row.count)}</td>
                  <td className="num">
                    <span className={`rep-diff diff-${tone}`}>{formatPercent(row.slaRate)}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// RepLeadsTable — the smoking gun. One row per touched lead, sorted
// by worst breach first. Flags suspicious dispositions (no-answer in
// <10s = possible queue clearing).
// ------------------------------------------------------------------

const QUICK_NO_ANSWER_SECONDS = 10;
const MAX_VISIBLE_LEADS = 50;

function RepLeadsTable({ leads }: { leads: DashboardRow[] }) {
  if (leads.length === 0) {
    return (
      <div className="od-panel">
        <div className="od-panel-header">
          <span className="od-panel-title">
            <span className="dot" /> Leads worked
          </span>
        </div>
        <div className="od-empty-state m-3">No lead-level rows in this window.</div>
      </div>
    );
  }

  const sorted = [...leads].sort((a, b) => {
    const aSecs = numberValue(a.seconds_to_first_attempt) ?? -1;
    const bSecs = numberValue(b.seconds_to_first_attempt) ?? -1;
    return bSecs - aSecs;
  });
  const visible = sorted.slice(0, MAX_VISIBLE_LEADS);
  const hidden = sorted.length - visible.length;

  return (
    <div className="od-panel">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Leads worked
        </span>
        <span className="od-panel-meta">
          {formatNumber(leads.length)} total · worst breach first
        </span>
      </div>
      <div className="od-panel-body flush">
        <div className="od-table-wrap">
          <table className="od-table compact rep-leads-table">
            <thead>
              <tr>
                <th>Lead</th>
                <th>Source</th>
                <th className="num">Age at first touch</th>
                <th>Outcome</th>
                <th>Window</th>
              </tr>
            </thead>
            <tbody>
              {visible.map((lead) => {
                const triggerTs = stringValue(lead.trigger_ts);
                const leadName = stringValue(lead.lead_name) ?? "Unknown lead";
                const source = stringValue(lead.trigger_source_label) ?? "Unknown";
                const secs = numberValue(lead.seconds_to_first_attempt);
                const ageDisplay = secs !== null ? formatAge(secs / 60) : "—";
                const ageTone =
                  secs === null
                    ? "flat"
                    : secs <= BOOKING_SLA_SECONDS
                      ? "up"
                      : secs <= 3600
                        ? "down"
                        : "danger";
                const status = stringValue(lead.first_touch_status) ?? "—";
                const channel = stringValue(lead.first_touch_channel) ?? "—";
                const duration = numberValue(lead.first_touch_duration_seconds);
                const isQuickNoAnswer =
                  status === "no-answer" &&
                  duration !== null &&
                  duration < QUICK_NO_ANSWER_SECONDS;
                const window = stringValue(lead.service_window) ?? "—";
                const isOoh = window === "after_hours";
                return (
                  <tr key={String(lead.trigger_event_id)}>
                    <td>
                      <div className="rep-lead-cell">
                        <span className="rep-lead-name">{leadName}</span>
                        {triggerTs ? (
                          <span className="rep-lead-time">{formatTriggerTs(triggerTs)}</span>
                        ) : null}
                      </div>
                    </td>
                    <td>
                      <span className="rep-source-chip">{source}</span>
                    </td>
                    <td className={`num age-${ageTone}`}>{ageDisplay}</td>
                    <td>
                      <div className="rep-outcome-cell">
                        <span className="rep-channel">{channel}</span>
                        <span className="rep-status">{status}</span>
                        {duration !== null ? (
                          <span className="rep-duration">{formatNumber(duration)}s</span>
                        ) : null}
                        {isQuickNoAnswer ? (
                          <span className="rep-flag-chip">queue-clear?</span>
                        ) : null}
                      </div>
                    </td>
                    <td>
                      <span className={`rep-window-chip ${isOoh ? "ooh" : "bh"}`}>
                        {isOoh ? "OOH" : "BH"}
                      </span>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
      {hidden > 0 ? (
        <div className="od-panel-footer">
          <span className="od-ev">
            Showing top {MAX_VISIBLE_LEADS} of {formatNumber(leads.length)} leads · {formatNumber(hidden)} more not shown
          </span>
        </div>
      ) : null}
    </div>
  );
}

function formatTriggerTs(ts: string): string {
  try {
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return ts;
    return d.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    });
  } catch {
    return ts;
  }
}

function RepTeamComparison({
  repRow,
  allReps,
}: {
  repRow: DashboardRow;
  allReps: DashboardRow[];
}) {
  const repSla = numberValue(repRow.pct_within_sla);
  const slaPcts = allReps
    .map((r) => numberValue(r.pct_within_sla))
    .filter((v): v is number => v !== null && Number.isFinite(v));
  const teamMedian = computeMedian(slaPcts);
  const delta = repSla !== null && teamMedian !== null ? repSla - teamMedian : null;

  // Rank by SLA descending
  const sortedReps = [...allReps].sort(
    (a, b) => (numberValue(b.pct_within_sla) ?? -1) - (numberValue(a.pct_within_sla) ?? -1),
  );
  const rank = sortedReps.findIndex((r) => stringValue(r.rep_name) === stringValue(repRow.rep_name)) + 1;
  const total = allReps.length;

  const tone =
    delta === null
      ? "neutral"
      : delta > 0.02
        ? "ok"
        : delta < -0.02
          ? "warning"
          : "neutral";

  return (
    <div className="od-panel rep-team-comparison">
      <div className="od-panel-header">
        <span className="od-panel-title">
          <span className="dot" /> Team comparison
        </span>
        <span className="od-panel-meta">
          {rank > 0 ? `#${rank} of ${total}` : "—"}
        </span>
      </div>
      <div className="od-panel-body comparison-body">
        <div className="comparison-line">
          <span className="comparison-label">Team average response rate</span>
          <span className="comparison-value">{formatPercent(teamMedian)}</span>
        </div>
        <div className="comparison-line">
          <span className="comparison-label">This rep</span>
          <span className="comparison-value">{formatPercent(repSla)}</span>
        </div>
        <div className="comparison-line comparison-delta">
          <span className="comparison-label">vs median</span>
          <span className={`comparison-value tone-${tone}`}>{formatPctDelta(delta)}</span>
        </div>
      </div>
    </div>
  );
}
