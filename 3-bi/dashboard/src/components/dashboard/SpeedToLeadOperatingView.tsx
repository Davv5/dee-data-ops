"use client";

import { useState } from "react";
import {
  ArrowUpRight,
  ClipboardCheck,
  Clock,
  Mail,
  Phone,
  PhoneCall,
  Radio,
  ShieldAlert,
  UserCheck,
  Zap,
} from "lucide-react";
import type {
  DashboardData,
  DashboardFilters,
  DashboardFreshness,
  DashboardRow,
  DashboardRowValue,
} from "@/types/dashboard-data";

const BOOKING_SLA_LABEL = "45m";

type LaneId = "fresh_inbound" | "fresh_magnets" | "outbound_existing" | "stale_backlog";

type ScoreCard = {
  label: string;
  value: string;
  helper: string;
  tone: "green" | "blue" | "amber" | "red";
};

type LeadActionRow = {
  id: string;
  laneId: string;
  leadName: string;
  leadEmail: string;
  leadPhone: string;
  contactId: string;
  assignedRep: string;
  sourceLabel: string;
  triggerType: string;
  triggerTs: string;
  triggerTsEt: string;
  triggerDate: string;
  utmSource: string;
  utmCampaign: string;
  serviceWindow: string;
  ageMinutes: number | null;
  ageHours: number | null;
  priorityLabel: string;
  priorityTone: "red" | "amber" | "blue";
  nextMove: string;
  managerAction: string;
  exceptionFlags: LeadException[];
};

type LaneCard = {
  id: LaneId;
  label: string;
  count: number;
  helper: string;
  metric: string;
  tone: "green" | "blue" | "amber" | "red";
};

type LeadException = {
  label: string;
  helper: string;
  tone: "red" | "amber" | "blue";
};

const sourcePillClass: Record<string, string> = {
  "GHL user": "stl-chip-green",
  "Dialer number": "stl-chip-amber",
  "Phone number": "stl-chip-blue",
  "Deleted GHL user ID": "stl-chip-red",
  Workflow: "stl-chip-blue",
};

export function SpeedToLeadOperatingView({ data }: { data: DashboardData }) {
  const counts = data.rows.speed_to_lead_follow_up_counts ?? [];
  const triggerRows = data.rows.speed_to_lead_trigger_summary ?? [];
  const businessHourRows = data.rows.speed_to_lead_business_hours ?? [];
  const actionRows = buildLeadActionRows(data.rows.speed_to_lead_no_touch_examples ?? []);
  const laneSummary = data.rows.speed_to_lead_lane_summary ?? [];
  const laneCards = buildLaneCards(actionRows, laneSummary);
  const [activeLane, setActiveLane] = useState<LaneId>(() => defaultLane(laneCards));
  const [selectedLeadId, setSelectedLeadId] = useState<string | null>(null);
  const activeRows = filterActionRows(actionRows, activeLane);
  const activeLaneCard = laneCards.find((lane) => lane.id === activeLane) ?? laneCards[0];
  const selectedLead = activeRows.find((row) => row.id === selectedLeadId) ?? activeRows[0] ?? actionRows[0] ?? null;
  const allLeadEvents = metricRow(counts, "All lead events");
  const appointmentBooking = triggerRows.find(
    (row) => row.trigger_type === "appointment_booking",
  );
  const leadMagnet = triggerRows.find((row) => row.trigger_type === "lead_magnet");
  const businessHours = businessHourRows.find((row) => row.service_window === "business_hours");
  const afterHours = businessHourRows.find((row) => row.service_window === "after_hours");

  const totalEvents = numberValue(allLeadEvents?.lead_count);

  return (
    <div className="stl-page space-y-4">
      <QueueHeader
        filters={data.filters}
        freshness={data.freshness}
        activeLane={activeLaneCard}
        activeLaneId={activeLane}
        laneCards={laneCards}
        onLaneChange={setActiveLane}
      />

      {data.error ? (
        <div className="rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <section className="grid items-start gap-3 xl:grid-cols-[minmax(0,1.5fr)_minmax(22rem,0.62fr)]">
        <div className="space-y-3">
          <ActionQueuePanel
            rows={activeRows}
            activeLane={activeLane}
            laneCard={activeLaneCard}
            selectedLeadId={selectedLead?.id ?? null}
            onLeadSelect={setSelectedLeadId}
          />
          <RoutingReadinessPanel rows={data.rows.speed_to_lead_routing_readiness ?? []} />
        </div>
        <div className="space-y-3">
          <LeadProofPanel lead={selectedLead} />
          <CriticalExceptionsPanel rows={data.rows.speed_to_lead_critical_exceptions ?? []} />
        </div>
      </section>

      <details className="stl-card rounded-lg border border-[#dedbd2] bg-white shadow-sm">
        <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">
          Evidence and diagnostics
        </summary>
        <div className="space-y-3 border-t border-[#ece9e1] p-4">
          <section className="grid gap-3 xl:grid-cols-3">
            <LeadLaneEvidenceCard rows={laneSummary} />
            <LeadTypeEvidenceCard bookingRow={appointmentBooking} leadMagnetRow={leadMagnet} />
            <ServiceWindowEvidenceCard businessHours={businessHours} afterHours={afterHours} />
          </section>

          <section className="grid gap-3 xl:grid-cols-[minmax(0,1.2fr)_minmax(22rem,0.8fr)]">
            <DailyTrendPanel rows={data.rows.speed_to_lead_daily ?? []} />
            <FollowUpMixPanel
              humanFollowUp={metricRow(counts, "Human follow-up")}
              automationTouched={metricRow(counts, "Automation touched")}
              totalEvents={totalEvents}
            />
          </section>

          <section className="grid gap-3 xl:grid-cols-[minmax(0,1.35fr)_minmax(22rem,0.85fr)]">
            <FirstWorkPanel rows={data.rows.speed_to_lead_first_work_by_rep ?? []} />
            <ReachedByPanel
              rows={data.rows.speed_to_lead_phone_reach_by_rep ?? []}
              confidenceRows={data.rows.speed_to_lead_attribution_confidence ?? []}
            />
          </section>

          <section className="grid gap-3 xl:grid-cols-[minmax(0,0.75fr)_minmax(0,1.25fr)]">
            <ResponseBucketsPanel rows={data.rows.speed_to_lead_response_buckets ?? []} />
            <NoTouchPanel rows={data.rows.speed_to_lead_no_touch_examples ?? []} />
          </section>
        </div>
      </details>

      <AuditDetails data={data} />
    </div>
  );
}

function QueueHeader({
  filters,
  freshness,
  activeLane,
  activeLaneId,
  laneCards,
  onLaneChange,
}: {
  filters: DashboardFilters;
  freshness: DashboardFreshness;
  activeLane?: LaneCard;
  activeLaneId: LaneId;
  laneCards: LaneCard[];
  onLaneChange: (lane: LaneId) => void;
}) {
  return (
    <header className="stl-hero overflow-hidden rounded-xl border border-[#26231f] bg-[#191714] text-white shadow-sm">
      <div className="grid gap-4 p-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-center">
        <div className="flex min-w-0 items-center gap-3">
          <div className="relative flex h-12 w-12 shrink-0 items-center justify-center rounded-md border border-[#437f74]/30 bg-[#437f74] text-white shadow-[0_0_0_4px_rgba(67,127,116,0.12)]">
            <Zap className="h-6 w-6" aria-hidden />
            <span className="absolute -right-1 -top-1 h-3 w-3 rounded-full border-2 border-[#191714] bg-[#34d399]" />
          </div>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <span className="rounded-sm border border-white/15 bg-white/10 px-2 py-1 text-[11px] font-semibold uppercase text-[#99f6e4]">
                Live SDR queue
              </span>
              <FreshnessPill freshness={freshness} />
            </div>
            <h1 className="mt-2 truncate text-2xl font-semibold tracking-normal md:text-3xl">
              Speed-to-Lead Queue
            </h1>
          </div>
        </div>

        <TimeRangeControl filters={filters} />
      </div>

      <div className="stl-hero-footer grid border-t border-white/10 bg-[#211f1b] md:grid-cols-[minmax(0,1fr)_auto]">
        <div className="grid gap-2 p-3 sm:grid-cols-2 xl:grid-cols-4">
          {laneCards.map((lane) => {
            const isActive = lane.id === activeLaneId;
            return (
              <button
                key={lane.id}
                type="button"
                onClick={() => onLaneChange(lane.id)}
                aria-pressed={isActive}
                className={`stl-lane-button rounded-lg border px-3 py-2 text-left transition ${
                  isActive
                    ? "border-[#437f74] bg-[#437f74]/15 text-[#171d22]"
                    : "border-white/10 bg-white/[0.06] text-white hover:border-[#437f74]/50"
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <span className={`truncate text-[11px] font-semibold uppercase ${isActive ? "text-[#3b3936]" : "text-[#d6d3cb]"}`}>
                    {lane.label}
                  </span>
                  <span className={`h-2 w-2 rounded-sm ${isActive ? "bg-[#191714]" : scoreDotClass(lane.tone)}`} />
                </div>
                <div className="mt-1 flex items-baseline gap-2">
                  <span className="text-xl font-semibold tracking-normal">{formatNumber(lane.count)}</span>
                  <span className={`text-xs font-semibold ${isActive ? "text-[#4b463f]" : "text-[#a7f3d0]"}`}>
                    {lane.metric}
                  </span>
                </div>
              </button>
            );
          })}
        </div>
        <div className="flex min-w-72 items-center gap-3 border-t border-white/10 p-3 md:border-l md:border-t-0">
          <Radio className="h-4 w-4 shrink-0 text-[#437f74]" aria-hidden />
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase text-[#a7f3d0]">On deck</div>
            <div className="truncate text-sm font-semibold text-white">
              {activeLane?.label ?? "Queue"} · {formatNumber(activeLane?.count ?? null)} in lane
            </div>
          </div>
        </div>
      </div>
    </header>
  );
}

function FreshnessPill({ freshness }: { freshness: DashboardFreshness }) {
  const isLive = freshness.status === "live";
  return (
    <span className="inline-flex items-center gap-1.5 rounded-sm border border-white/15 bg-white/10 px-2 py-1 text-[11px] font-medium text-[#e7e1d6]">
      <span className={`h-1.5 w-1.5 rounded-full ${isLive ? "bg-[#34d399]" : "bg-[#f59e0b]"}`} />
      {freshness.detail}
    </span>
  );
}

function LeadProofPanel({ lead }: { lead: LeadActionRow | null }) {
  if (!lead) {
    return (
      <aside className="stl-proof-card rounded-xl border border-[#2f2a24] bg-[#171614] p-4 text-white shadow-sm">
        <PanelChromeLabel label="Lead proof" />
        <div className="mt-4 rounded-md border border-white/10 bg-white/[0.06] p-4">
          <EmptyDarkState message="Select a lead to see owner proof, timeline, exceptions, and the manager decision." />
        </div>
      </aside>
    );
  }

  return (
    <aside className="stl-proof-card overflow-hidden rounded-xl border border-[#2f2a24] bg-[#171614] text-white shadow-sm">
      <div className="flex items-center justify-between border-b border-white/10 bg-[#24211d] px-4 py-2">
        <div className="flex items-center gap-1.5">
          <span className="h-2.5 w-2.5 rounded-full bg-[#d84a3a]" />
          <span className="h-2.5 w-2.5 rounded-full bg-[#437f74]" />
          <span className="h-2.5 w-2.5 rounded-full bg-[#2f7d73]" />
        </div>
        <div className="font-mono text-[10px] font-semibold uppercase text-[#d6d3cb]">
          LEAD_PROOF
        </div>
      </div>

      <div className="p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex items-center gap-2 font-mono text-xs font-semibold uppercase text-[#437f74]">
              <ClipboardCheck className="h-4 w-4" aria-hidden />
              Why this lead is here
            </div>
            <h2 className="mt-2 truncate text-xl font-semibold tracking-normal">{lead.leadName}</h2>
            <div className="mt-1 truncate text-xs text-[#d6d3cb]">{lead.contactId}</div>
          </div>
          <span className={`shrink-0 rounded-md border px-2 py-1 font-mono text-xs font-semibold ${darkPriorityClass(lead.priorityTone)}`}>
            {lead.priorityLabel}
          </span>
        </div>

        <div className="mt-4 grid grid-cols-2 gap-2">
          <ProofMetric label="SLA timer" value={slaTimerLabel(lead.ageMinutes)} tone="urgent" />
          <ProofMetric label="Owner" value={lead.assignedRep} tone={lead.assignedRep === "Unassigned" ? "urgent" : "calm"} />
        </div>

        <div className="mt-3 rounded-md border border-white/10 bg-white/[0.06] p-3">
          <div className="font-mono text-xs font-semibold uppercase text-[#a7f3d0]">Manager decision</div>
          <p className="mt-2 text-sm leading-6 text-white">{lead.managerAction}</p>
        </div>

        <div className="mt-3 rounded-md border border-white/10 bg-white/[0.06] p-3">
          <div className="font-mono text-xs font-semibold uppercase text-[#a7f3d0]">Proof timeline</div>
          <div className="mt-3 space-y-3 text-xs leading-5">
            <TimelineFact label="Lead triggered" value={lead.triggerTsEt || lead.triggerDate} />
            <TimelineFact label="Assigned owner" value={lead.assignedRep} />
            <TimelineFact label="First human touch" value="None found after trigger" danger />
            <TimelineFact label="Service window" value={formatServiceWindow(lead.serviceWindow)} />
          </div>
        </div>

        <div className="mt-3 rounded-md border border-[#2f7d73]/60 bg-[#203d37] p-3">
          <div className="font-mono text-xs font-semibold uppercase text-[#a7f3d0]">Contact path</div>
          <div className="mt-2 space-y-1.5 text-xs leading-5 text-[#e6fffb]">
            <div className="flex items-center gap-2">
              <Phone className="h-3.5 w-3.5 shrink-0" aria-hidden />
              <span>{lead.leadPhone}</span>
            </div>
            <div className="flex items-center gap-2">
              <Mail className="h-3.5 w-3.5 shrink-0" aria-hidden />
              <span className="truncate">{lead.leadEmail}</span>
            </div>
            <div>{lead.sourceLabel} / {formatTriggerType(lead.triggerType)}</div>
          </div>
        </div>

        <div className="mt-3">
          <div className="font-mono text-xs font-semibold uppercase text-[#d6d3cb]">Critical exceptions</div>
          <div className="mt-2 flex flex-wrap gap-2">
            {lead.exceptionFlags.map((exception) => (
              <span key={exception.label} className={`rounded-md border px-2 py-1 text-xs font-semibold ${darkExceptionClass(exception.tone)}`}>
                {exception.label}
              </span>
            ))}
          </div>
          <p className="mt-2 text-xs leading-5 text-[#d6d3cb]">
            Assignment timestamp and completed reassignment actions are not in the current mart, so this panel separates proof from policy.
          </p>
        </div>
      </div>
    </aside>
  );
}

function RoutingReadinessPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 8);

  return (
    <section className="stl-panel rounded-xl border border-[#d7d1c2] bg-white shadow-sm">
      <div className="border-b border-[#ece9e1] p-4">
        <div className="flex items-start justify-between gap-3">
          <PanelHeader
            title="SDR Routing Readiness"
            helper="Who should keep receiving new leads, who needs watching, and who should be paused or reassigned."
          />
          <UserCheck className="mt-0.5 h-5 w-5 shrink-0 text-[#0f766e]" aria-hidden />
        </div>
      </div>

      <div className="overflow-x-auto p-4 pt-0">
        {visibleRows.length > 0 ? (
          <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
            <thead>
              <tr className="text-[#66635f]">
                <th className="border-b border-[#dedbd2] py-3 pr-3 font-semibold">SDR</th>
                <th className="border-b border-[#dedbd2] px-3 py-3 font-semibold">Routing status</th>
                <th className="border-b border-[#dedbd2] px-3 py-3 text-right font-semibold">Owned</th>
                <th className="border-b border-[#dedbd2] px-3 py-3 text-right font-semibold">Fresh open</th>
                <th className="border-b border-[#dedbd2] px-3 py-3 text-right font-semibold">{"<=5m"}</th>
                <th className="border-b border-[#dedbd2] py-3 pl-3 text-right font-semibold">Avg first touch</th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => {
                const decision = routingDecision(row);
                const assignedRep = stringValue(row.assigned_rep) ?? "Unassigned";

                return (
                  <tr key={assignedRep}>
                    <td className="min-w-56 border-b border-[#ece9e1] py-3 pr-3">
                      <div className="font-semibold text-[#2d2b28]">{assignedRep}</div>
                      <div className="mt-1 text-[11px] text-[#66635f]">
                        Last activity: {stringValue(row.last_activity_et) ?? "None found"}
                      </div>
                    </td>
                    <td className="min-w-56 border-b border-[#ece9e1] px-3 py-3">
                      <span className={`inline-flex rounded-md border px-2 py-1 font-semibold ${routingStatusClass(decision.tone)}`}>
                        {decision.label}
                      </span>
                      <div className="mt-1 text-[11px] leading-4 text-[#66635f]">{decision.helper}</div>
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3 text-right font-semibold tabular-nums">
                      {formatNumber(numberValue(row.owned_leads))}
                      <div className="font-normal text-[#66635f]">{formatNumber(numberValue(row.unworked_leads))} untouched</div>
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3 text-right font-semibold tabular-nums text-[#991b1b]">
                      {formatNumber(numberValue(row.fresh_unworked_leads))}
                      <div className="font-normal text-[#66635f]">{formatNumber(numberValue(row.unworked_bookings))} bookings</div>
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3 text-right font-semibold tabular-nums">
                      {formatPercent(numberValue(row.first_attempt_within_5m_rate))}
                      <div className="font-normal text-[#66635f]">{formatPercent(numberValue(row.first_attempt_within_sla_rate))} {"<=45m"}</div>
                    </td>
                    <td className="border-b border-[#ece9e1] py-3 pl-3 text-right tabular-nums">
                      {formatDuration(numberValue(row.avg_minutes_to_first_attempt))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <div className="pt-4">
            <EmptyState message="No SDR readiness rows returned for this range." />
          </div>
        )}
      </div>
    </section>
  );
}

function CriticalExceptionsPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 6);

  return (
    <section className="stl-warm-panel rounded-xl border border-[#d7d1c2] bg-[#fffaf1] p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <PanelHeader
          title="Critical Exceptions"
          helper="These explain when the board should fix routing or data quality before blaming SDR speed."
        />
        <ShieldAlert className="mt-0.5 h-5 w-5 shrink-0 text-[#b45309]" aria-hidden />
      </div>

      <div className="mt-4 space-y-3">
        {visibleRows.length > 0 ? (
          visibleRows.map((row) => {
            const tone = exceptionTone(stringValue(row.exception_key));

            return (
              <div key={stringValue(row.exception_key) ?? stringValue(row.exception_label)} className="rounded-md border border-[#e3d6c2] bg-white p-3">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-[#2d2b28]">{stringValue(row.exception_label) ?? "Exception"}</div>
                    <div className="mt-1 text-xs leading-5 text-[#66635f]">{stringValue(row.manager_action) ?? "Review before reassignment."}</div>
                  </div>
                  <span className={`rounded-md border px-2 py-1 font-mono text-xs font-semibold ${routingStatusClass(tone)}`}>
                    {formatNumber(numberValue(row.unworked_leads))}
                  </span>
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
                  <SignalMini label="Events" value={formatNumber(numberValue(row.lead_events))} />
                  <SignalMini label="Reps" value={formatNumber(numberValue(row.affected_reps))} />
                  <SignalMini label="Oldest" value={formatAgeHours(numberValue(row.oldest_age_hours))} />
                </div>
              </div>
            );
          })
        ) : (
          <EmptyState message="No critical exception rows returned for this range." />
        )}
      </div>
    </section>
  );
}

function PanelChromeLabel({ label }: { label: string }) {
  return (
    <div className="flex items-center justify-between border-b border-white/10 bg-[#24211d] px-4 py-2">
      <div className="flex items-center gap-1.5">
        <span className="h-2.5 w-2.5 rounded-full bg-[#d84a3a]" />
        <span className="h-2.5 w-2.5 rounded-full bg-[#437f74]" />
        <span className="h-2.5 w-2.5 rounded-full bg-[#2f7d73]" />
      </div>
      <div className="font-mono text-[10px] font-semibold uppercase text-[#d6d3cb]">{label}</div>
    </div>
  );
}

function ProofMetric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "urgent" | "calm";
}) {
  return (
    <div className={`rounded-md border p-3 ${tone === "urgent" ? "border-[#d84a3a]/60 bg-[#3a211d]" : "border-[#2f7d73]/60 bg-[#203d37]"}`}>
      <div className="font-mono text-[10px] font-semibold uppercase text-[#d6d3cb]">{label}</div>
      <div className="mt-1 truncate text-base font-semibold tracking-normal text-white">{value}</div>
    </div>
  );
}

function TimelineFact({
  label,
  value,
  danger,
}: {
  label: string;
  value: string;
  danger?: boolean;
}) {
  return (
    <div className="grid grid-cols-[7rem_minmax(0,1fr)] gap-3">
      <div className="font-mono text-[10px] font-semibold uppercase text-[#a7f3d0]">{label}</div>
      <div className={danger ? "font-semibold text-[#fecaca]" : "text-[#f7f1e7]"}>{value}</div>
    </div>
  );
}

function SignalMini({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-[#ece5d8] bg-[#fbf7ef] p-2">
      <div className="text-[10px] font-semibold uppercase text-[#8a857d]">{label}</div>
      <div className="mt-1 font-semibold text-[#2d2b28]">{value}</div>
    </div>
  );
}

function EmptyDarkState({ message }: { message: string }) {
  return <div className="text-sm leading-6 text-[#d6d3cb]">{message}</div>;
}

function ActionQueuePanel({
  rows,
  activeLane,
  laneCard,
  selectedLeadId,
  onLeadSelect,
}: {
  rows: LeadActionRow[];
  activeLane: LaneId;
  laneCard?: LaneCard;
  selectedLeadId: string | null;
  onLeadSelect: (leadId: string) => void;
}) {
  const visibleRows = rows.slice(0, 10);
  const description = queueDescription(activeLane);

  return (
    <section className="stl-panel rounded-xl border border-[#d7d1c2] bg-white shadow-sm">
      <div className="border-b border-[#ece9e1] p-4">
        <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
          <div>
            <div className="flex items-center gap-2 text-xs font-semibold uppercase text-[#0f766e]">
              <PhoneCall className="h-4 w-4" aria-hidden />
              Work these leads now
            </div>
            <h2 className="mt-1 text-lg font-semibold tracking-normal text-[#191714]">
              {laneCard?.label ?? "SDR Action Queue"}
            </h2>
            <p className="mt-1 max-w-2xl text-sm leading-6 text-[#66635f]">
              {description}
            </p>
          </div>
          <div className="rounded-md border border-[#fecaca] bg-[#fef2f2] px-3 py-2 text-sm font-semibold text-[#991b1b]">
            {formatNumber(laneCard?.count ?? rows.length)} in lane
          </div>
        </div>
      </div>

      <div className="overflow-x-auto p-4 pt-0">
        {visibleRows.length > 0 ? (
          <table className="w-full table-fixed border-separate border-spacing-0 text-left text-xs">
            <thead>
              <tr className="text-[#66635f]">
                <th className="w-[8.5rem] border-b border-[#dedbd2] py-3 pr-3 font-semibold">SLA</th>
                <th className="w-[16rem] border-b border-[#dedbd2] px-3 py-3 font-semibold">Lead</th>
                <th className="w-[8.5rem] border-b border-[#dedbd2] px-3 py-3 font-semibold">Owner</th>
                <th className="w-[10rem] border-b border-[#dedbd2] px-3 py-3 font-semibold">Why flagged</th>
                <th className="border-b border-[#dedbd2] py-3 pl-3 font-semibold">Next action</th>
              </tr>
            </thead>
            <tbody>
              {visibleRows.map((row) => {
                const isSelected = row.id === selectedLeadId;
                return (
                <tr
                  key={row.id}
                  onClick={() => onLeadSelect(row.id)}
                  className={`cursor-pointer transition ${isSelected ? "stl-selected-row bg-[#fff7d6]" : "hover:bg-[#fbfaf7]"}`}
                >
                  <td className="border-b border-[#ece9e1] py-3 pr-3 align-top">
                    <span className={`inline-flex rounded-md border px-2 py-1 font-semibold ${actionToneClass(row.priorityTone)}`}>
                      {row.priorityLabel}
                    </span>
                    <div className="mt-1 font-mono text-[11px] font-semibold text-[#991b1b]">
                      {slaTimerLabel(row.ageMinutes)}
                    </div>
                    <div className="mt-1 text-[11px] text-[#66635f]">{formatServiceWindow(row.serviceWindow)}</div>
                  </td>
                  <td className="border-b border-[#ece9e1] px-3 py-3 align-top">
                    <div className="truncate font-medium text-[#2d2b28]">{row.leadName}</div>
                    <div className="mt-1 truncate text-[11px] font-medium text-[#3b3936]">
                      {row.sourceLabel} / {formatTriggerType(row.triggerType)}
                    </div>
                    <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-[#66635f]">
                      <span className="inline-flex max-w-44 items-center gap-1 truncate">
                        <Mail className="h-3 w-3 shrink-0" aria-hidden />
                        <span className="truncate">{row.leadEmail}</span>
                      </span>
                      <span className="inline-flex max-w-36 items-center gap-1 truncate">
                        <Phone className="h-3 w-3 shrink-0" aria-hidden />
                        <span className="truncate">{row.leadPhone}</span>
                      </span>
                    </div>
                    <div className="mt-1 truncate text-[11px] text-[#8a857d]">{row.contactId}</div>
                  </td>
                  <td className="border-b border-[#ece9e1] px-3 py-3 align-top">
                    <div className={`truncate font-semibold ${row.assignedRep === "Unassigned" ? "text-[#991b1b]" : "text-[#2d2b28]"}`}>
                      {row.assignedRep}
                    </div>
                    <div className="mt-1 text-[11px] text-[#66635f]">No first touch found</div>
                  </td>
                  <td className="border-b border-[#ece9e1] px-3 py-3 align-top">
                    <div className="flex flex-wrap gap-1.5">
                      {row.exceptionFlags.slice(0, 2).map((exception) => (
                        <span key={exception.label} className={`rounded-md border px-2 py-0.5 text-[11px] font-semibold ${exceptionClass(exception.tone)}`}>
                          {exception.label}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="border-b border-[#ece9e1] py-3 pl-3 align-top text-sm leading-5 text-[#2d2b28]">
                    <div className="line-clamp-2">{row.nextMove}</div>
                    <button
                      type="button"
                      onClick={(event) => {
                        event.stopPropagation();
                        onLeadSelect(row.id);
                      }}
                      className="mt-2 inline-flex items-center gap-1 rounded-md border border-[#d9d0c2] bg-white px-2 py-1 text-xs font-semibold text-[#0f766e] hover:border-[#0f766e]"
                    >
                      Open proof
                      <ArrowUpRight className="h-3.5 w-3.5" aria-hidden />
                    </button>
                  </td>
                </tr>
                );
              })}
            </tbody>
          </table>
        ) : (
          <div className="pt-4">
            <EmptyState message="No unworked lead rows match this lane for the selected range." />
          </div>
        )}
      </div>
    </section>
  );
}

function LeadTypeEvidenceCard({
  bookingRow,
  leadMagnetRow,
}: {
  bookingRow?: DashboardRow;
  leadMagnetRow?: DashboardRow;
}) {
  return (
    <EvidenceCard
      title="Lead type play"
      helper="Bookings and lead magnets need different response plays."
      rows={[
        {
          label: "Bookings <=5m",
          value: formatPercent(numberValue(bookingRow?.within_5m_rate)),
          helper: `${formatNumber(numberValue(bookingRow?.within_5m))} of ${formatNumber(numberValue(bookingRow?.total_triggers))}`,
        },
        {
          label: `Bookings <=${BOOKING_SLA_LABEL}`,
          value: formatPercent(numberValue(bookingRow?.within_sla_rate)),
          helper: "Human call speed target",
        },
        {
          label: "Magnets <=5m",
          value: formatPercent(numberValue(leadMagnetRow?.within_5m_rate)),
          helper: "Use automation bridge before SDR pickup",
        },
      ]}
    />
  );
}

function ServiceWindowEvidenceCard({
  businessHours,
  afterHours,
}: {
  businessHours?: DashboardRow;
  afterHours?: DashboardRow;
}) {
  return (
    <EvidenceCard
      title="Coverage window"
      helper="After-hours volume is large enough to deserve its own play."
      rows={[
        {
          label: "Business hours",
          value: formatPercent(numberValue(businessHours?.first_attempt_within_sla_rate)),
          helper: `${formatNumber(numberValue(businessHours?.total_triggers))} lead events`,
        },
        {
          label: "After-hours/weekend",
          value: formatPercent(numberValue(afterHours?.first_attempt_within_sla_rate)),
          helper: `${formatNumber(numberValue(afterHours?.total_triggers))} lead events`,
        },
        {
          label: "After-hours not worked",
          value: formatNumber(numberValue(afterHours?.unworked_leads)),
          helper: `${formatPercent(numberValue(afterHours?.unworked_lead_rate))} unworked rate`,
        },
      ]}
    />
  );
}

function LeadLaneEvidenceCard({ rows }: { rows: DashboardRow[] }) {
  return (
    <EvidenceCard
      title="Lane split"
      helper="Bookings are split so inbound and outbound/existing work are not judged as one SLA."
      rows={[
        laneEvidenceRow(rows, "inbound_bookings", "Inbound bookings"),
        laneEvidenceRow(rows, "lead_magnets", "Lead magnets"),
        laneEvidenceRow(rows, "outbound_existing_bookings", "Outbound/existing"),
      ]}
    />
  );
}

function EvidenceCard({
  title,
  helper,
  rows,
}: {
  title: string;
  helper: string;
  rows: Array<{ label: string; value: string; helper: string }>;
}) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader title={title} helper={helper} />
      <div className="mt-4 space-y-3">
        {rows.map((row) => (
          <div key={row.label} className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
            <div className="text-xs font-semibold uppercase text-[#66635f]">{row.label}</div>
            <div className="mt-1 text-xl font-semibold tracking-normal text-[#191714]">{row.value}</div>
            <div className="mt-1 text-xs leading-5 text-[#66635f]">{row.helper}</div>
          </div>
        ))}
      </div>
    </section>
  );
}

function TimeRangeControl({ filters }: { filters: DashboardFilters }) {
  return (
    <div className="w-full lg:w-auto">
      <div className="stl-range-control flex w-full rounded-lg border border-white/15 bg-white/10 p-1 shadow-sm lg:w-auto">
        {filters.timeRangeOptions.map((option) => {
          const isActive = option.value === filters.timeRange;
          return (
            <a
              key={option.value}
              href={`/speed-to-lead?range=${option.value}`}
              aria-current={isActive ? "page" : undefined}
              title={option.description}
              className={`stl-range-link min-w-12 rounded-md px-3 py-1.5 text-center text-xs font-semibold transition ${
                isActive
                  ? "bg-[#437f74]/15 text-[#171d22]"
                  : "text-[#e7e1d6] hover:bg-white/10 hover:text-white"
              }`}
            >
              {option.label}
            </a>
          );
        })}
      </div>
    </div>
  );
}

function ReachedByPanel({
  rows,
  confidenceRows,
}: {
  rows: DashboardRow[];
  confidenceRows: DashboardRow[];
}) {
  const visibleRows = rows.slice(0, 6);
  const maxReached = Math.max(...visibleRows.map((row) => numberValue(row.leads_reached) ?? 0), 1);
  const confidence = confidenceRows[0];
  const totalReached = numberValue(confidence?.reached_leads);
  const namedRepReached = numberValue(confidence?.named_rep_reached);
  const needsMapping = numberValue(confidence?.needs_mapping);
  const noRepEvents =
    numberValue(confidence?.no_rep_supplied) ??
    rows
      .filter((row) => stringValue(row.identity_source) === "No rep supplied")
      .reduce((sum, row) => sum + (numberValue(row.leads_reached) ?? 0), 0);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Reached-By Identity</h2>
          <p className="mt-1 text-xs text-[#66635f]">Top identities credited with answered or completed calls.</p>
        </div>
        <span className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-xs font-semibold text-[#166534]">
          {noRepEvents === 0 ? "0 Unknown" : `${formatNumber(noRepEvents)} No Rep`}
        </span>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2">
        <SignalBox
          label="Named reps"
          value={`${formatNumber(namedRepReached)} / ${formatNumber(totalReached)}`}
          helper={`${formatPercent(numberValue(confidence?.named_rep_rate))} of reached calls`}
          tone="green"
        />
        <SignalBox
          label="Needs mapping"
          value={formatNumber(needsMapping)}
          helper="Dialer lines or deleted users"
          tone={needsMapping === 0 ? "green" : "amber"}
        />
      </div>

      <div className="mt-4 space-y-2.5">
        {visibleRows.map((row) => {
          const reached = numberValue(row.leads_reached) ?? 0;
          const identitySource = stringValue(row.identity_source) ?? "No rep supplied";
          return (
            <div key={`${row.reached_by}-${identitySource}`} className="grid grid-cols-[minmax(10rem,1fr)_minmax(9rem,0.7fr)_4.5rem] items-center gap-3 text-sm">
              <div className="min-w-0">
                <div className="truncate font-medium text-[#2d2b28]">{stringValue(row.reached_by)}</div>
                <div className="mt-1 h-1.5 rounded-sm bg-[#ece9e1]">
                  <div
                    className="h-1.5 rounded-sm bg-[#0f766e]"
                    style={{ width: `${Math.max(4, (reached / maxReached) * 100)}%` }}
                  />
                </div>
              </div>
              <span className={`truncate rounded-md border px-2 py-1 text-xs font-medium ${pillClass(identitySource)}`}>
                {identitySource}
              </span>
              <div className="text-right">
                <div className="font-semibold">{formatNumber(reached)}</div>
                <div className="text-xs text-[#66635f]">{formatPercent(numberValue(row.share_of_reached_leads))}</div>
              </div>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function DailyTrendPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(-18);
  const latest = visibleRows[visibleRows.length - 1];
  const slaPath = sparklinePath(visibleRows, "pct_within_sla");
  const touchPath = sparklinePath(visibleRows, "pct_triggers_with_outbound_touch");

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Daily Response Trend"
        helper="Latest daily SLA and outbound-touch rates from the selected range."
      />
      <div className="mt-4 grid gap-3 sm:grid-cols-[12rem_1fr]">
        <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
          <div className="text-[11px] font-semibold uppercase text-[#66635f]">
            Latest day
          </div>
          <div className="mt-2 text-xl font-semibold tracking-normal">
            {formatPercent(numberValue(latest?.pct_within_sla))}
          </div>
          <div className="mt-1 text-xs text-[#66635f]">
            {stringValue(latest?.report_date) ?? "No daily rows"} - {formatNumber(numberValue(latest?.total_triggers_all))} lead events
          </div>
        </div>
        <div className="min-h-36 rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
          {visibleRows.length > 1 ? (
            <>
              <div className="flex flex-wrap gap-3 text-[11px] font-medium text-[#66635f]">
                <span className="flex items-center gap-1.5">
                  <span className="stl-bucket-fast h-2 w-4 rounded-sm" />
                  Bookings &lt;= {BOOKING_SLA_LABEL}
                </span>
                <span className="flex items-center gap-1.5">
                  <span className="stl-bucket-neutral h-2 w-4 rounded-sm" />
                  Any outbound touch
                </span>
              </div>
              <svg viewBox="0 0 100 42" className="mt-3 h-24 w-full" preserveAspectRatio="none">
                <path d="M0 40H100" stroke="var(--stl-border-strong)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
                <path d="M0 21H100" stroke="var(--stl-border)" strokeWidth="1" vectorEffect="non-scaling-stroke" />
                <path d={touchPath} fill="none" stroke="var(--stl-muted)" strokeWidth="2" vectorEffect="non-scaling-stroke" />
                <path d={slaPath} fill="none" stroke="var(--stl-accent)" strokeWidth="2.5" vectorEffect="non-scaling-stroke" />
              </svg>
              <div className="mt-2 flex justify-between text-[11px] text-[#66635f]">
                <span>{stringValue(visibleRows[0]?.report_date)}</span>
                <span>{stringValue(latest?.report_date)}</span>
              </div>
            </>
          ) : (
            <EmptyState message="Not enough daily rows for a trend." />
          )}
        </div>
      </div>
    </section>
  );
}

function FollowUpMixPanel({
  humanFollowUp,
  automationTouched,
  totalEvents,
}: {
  humanFollowUp?: DashboardRow;
  automationTouched?: DashboardRow;
  totalEvents: number | null;
}) {
  const rows = [
    {
      label: "Human follow-up",
      count: numberValue(humanFollowUp?.lead_count),
      rate: numberValue(humanFollowUp?.share_of_all_leads),
      helper: "Non-workflow call, text, or email.",
      color: "stl-bucket-fast",
    },
    {
      label: "Automation touched",
      count: numberValue(automationTouched?.lead_count),
      rate: numberValue(automationTouched?.share_of_all_leads),
      helper: "Workflow-generated follow-up.",
      color: "stl-bucket-neutral",
    },
  ];

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Follow-Up Mix"
        helper="Human and automation touches can overlap on the same lead event."
      />
      <div className="mt-4 space-y-4">
        {rows.map((row) => (
          <div key={row.label}>
            <div className="flex items-baseline justify-between gap-3 text-sm">
              <div>
                <div className="font-medium text-[#2d2b28]">{row.label}</div>
                <div className="mt-0.5 text-xs text-[#66635f]">{row.helper}</div>
              </div>
              <div className="text-right">
                <div className="font-semibold">{formatNumber(row.count)}</div>
                <div className="text-xs text-[#66635f]">of {formatNumber(totalEvents)}</div>
              </div>
            </div>
            <div className="mt-2 grid grid-cols-[1fr_4.5rem] items-center gap-3">
              <div className="h-2 rounded-sm bg-[#ece9e1]">
                <div className={`h-2 rounded-sm ${row.color}`} style={{ width: percentWidth(row.rate) }} />
              </div>
              <div className="text-right text-xs font-semibold text-[#3b3936]">
                {formatPercent(row.rate)}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

function FirstWorkPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 10);
  const maxWorked = Math.max(...visibleRows.map((row) => numberValue(row.leads_worked) ?? 0), 1);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <PanelHeader
          title="First-Work Leaderboard"
          helper="Who or what first worked each lead event after the trigger."
        />
        <ArrowUpRight className="mt-0.5 h-4 w-4 shrink-0 text-[#0f766e]" aria-hidden />
      </div>
      <div className="mt-4 overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-xs">
          <thead>
            <tr className="text-[#66635f]">
              <th className="border-b border-[#dedbd2] py-2 pr-3 font-semibold">Worked by</th>
              <th className="border-b border-[#dedbd2] px-3 py-2 font-semibold">Channel</th>
              <th className="border-b border-[#dedbd2] px-3 py-2 text-right font-semibold">Lead events</th>
              <th className="border-b border-[#dedbd2] px-3 py-2 text-right font-semibold">Reached later</th>
              <th className="border-b border-[#dedbd2] py-2 pl-3 text-right font-semibold">Avg first touch</th>
            </tr>
          </thead>
          <tbody>
            {visibleRows.length > 0 ? (
              visibleRows.map((row) => {
                const worked = numberValue(row.leads_worked) ?? 0;
                const identitySource = stringValue(row.identity_source) ?? "No rep supplied";
                return (
                  <tr key={`${row.worked_by}-${row.first_channel_label}-${identitySource}`}>
                    <td className="min-w-56 border-b border-[#ece9e1] py-3 pr-3">
                      <div className="font-medium text-[#2d2b28]">{stringValue(row.worked_by) ?? "Unknown"}</div>
                      <div className="mt-1 flex items-center gap-2">
                        <span className={`max-w-40 truncate rounded-md border px-2 py-0.5 font-medium ${pillClass(identitySource)}`}>
                          {identitySource}
                        </span>
                        <span className="truncate text-[#66635f]">{stringValue(row.role) ?? "Unknown role"}</span>
                      </div>
                      <div className="mt-2 h-1.5 rounded-sm bg-[#ece9e1]">
                        <div className="h-1.5 rounded-sm bg-[#0f766e]" style={{ width: `${Math.max(3, (worked / maxWorked) * 100)}%` }} />
                      </div>
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3">
                      {stringValue(row.first_channel_label) ?? "N/A"}
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3 text-right font-semibold tabular-nums">
                      {formatNumber(worked)}
                      <div className="font-normal text-[#66635f]">{formatPercent(numberValue(row.share_of_worked_leads))}</div>
                    </td>
                    <td className="border-b border-[#ece9e1] px-3 py-3 text-right tabular-nums">
                      {formatNumber(numberValue(row.reached_by_phone))}
                    </td>
                    <td className="border-b border-[#ece9e1] py-3 pl-3 text-right tabular-nums">
                      {formatDuration(numberValue(row.avg_minutes_to_first_attempt))}
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={5} className="py-6">
                  <EmptyState message="No first-work rows returned from BigQuery." />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function NoTouchPanel({ rows }: { rows: DashboardRow[] }) {
  const visibleRows = rows.slice(0, 8);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <PanelHeader
          title="No-Touch Proof Rows"
          helper="Recent lead events where no post-trigger follow-up was found."
        />
        <Clock className="mt-0.5 h-4 w-4 shrink-0 text-[#991b1b]" aria-hidden />
      </div>
      <div className="mt-4 space-y-3">
        {visibleRows.length > 0 ? (
          visibleRows.map((row, index) => (
            <ProofRow key={`${row.trigger_ts}-${index}`} row={row} />
          ))
        ) : (
          <EmptyState message="No no-touch examples returned for this range." />
        )}
      </div>
    </section>
  );
}

function ProofRow({ row }: { row: DashboardRow }) {
  return (
    <div className="border-b border-[#ece9e1] pb-3 last:border-b-0 last:pb-0">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="truncate text-sm font-medium text-[#2d2b28]">
            {stringValue(row.source_label) ?? "Unknown source"}
          </div>
          <div className="mt-1 truncate text-xs text-[#66635f]">
            {formatTriggerType(row.trigger_type)} - {stringValue(row.utm_campaign) ?? "No campaign"}
          </div>
        </div>
        <span className="shrink-0 rounded-md border border-[#fecaca] bg-[#fef2f2] px-2 py-1 text-xs font-semibold text-[#991b1b]">
          {formatAgeHours(numberValue(row.age_hours))}
        </span>
      </div>
      <div className="mt-2 text-[11px] text-[#66635f]">{stringValue(row.trigger_date)}</div>
    </div>
  );
}

function ResponseBucketsPanel({ rows }: { rows: DashboardRow[] }) {
  const groups = groupRowsByValue(rows, "trigger_type").slice(0, 3);

  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <PanelHeader
        title="Response Buckets"
        helper="How quickly lead events move from trigger to first touch."
      />
      <div className="mt-4 space-y-4">
        {groups.length > 0 ? (
          groups.map((group) => (
            <div key={group.label}>
              <div className="flex items-center justify-between gap-3 text-sm">
                <span className="font-medium text-[#2d2b28]">{formatTriggerType(group.label)}</span>
                <span className="text-xs text-[#66635f]">{formatNumber(sumRows(group.rows, "triggers"))} events</span>
              </div>
              <div className="mt-2 flex h-3 overflow-hidden rounded-sm bg-[#ece9e1]">
                {sortResponseBuckets(group.rows).map((row) => {
                  const bucket = stringValue(row.response_bucket) ?? "unknown";
                  return (
                    <div
                      key={`${group.label}-${bucket}`}
                      className={bucketColor(bucket)}
                      style={{ width: percentWidth(numberValue(row.share_of_type), false) }}
                    />
                  );
                })}
              </div>
              <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-[#66635f]">
                {sortResponseBuckets(group.rows).map((row) => {
                  const bucket = stringValue(row.response_bucket) ?? "unknown";
                  return (
                    <div key={`${group.label}-${bucket}-label`} className="flex items-center justify-between gap-2">
                      <span className="truncate">{bucket}</span>
                      <span className="font-medium text-[#3b3936]">{formatPercent(numberValue(row.share_of_type))}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          ))
        ) : (
          <EmptyState message="No response bucket rows returned from BigQuery." />
        )}
      </div>
    </section>
  );
}

function AuditDetails({ data }: { data: DashboardData }) {
  return (
    <details className="mt-2 rounded-lg border border-[#dedbd2] bg-white shadow-sm">
      <summary className="cursor-pointer px-4 py-3 text-sm font-semibold text-[#0f766e]">
        Audit Tables
      </summary>
      <div className="grid gap-3 border-t border-[#ece9e1] p-4 xl:grid-cols-2">
        <TablePanel
          title="Attribution confidence"
          rows={data.rows.speed_to_lead_attribution_confidence ?? []}
          columns={[
            { key: "reached_leads", label: "Reached", format: "number" },
            { key: "named_rep_reached", label: "Named Reps", format: "number" },
            { key: "named_rep_rate", label: "Named Rate", format: "percent" },
            { key: "needs_mapping", label: "Needs Mapping", format: "number" },
            { key: "no_rep_supplied", label: "No Rep", format: "number" },
          ]}
        />
        <TablePanel
          title="Not worked aging"
          rows={data.rows.speed_to_lead_not_worked_aging ?? []}
          columns={[
            { key: "age_bucket", label: "Age" },
            { key: "lead_events", label: "Lead Events", format: "number" },
            { key: "share_of_not_worked", label: "Share", format: "percent" },
            { key: "oldest_age_hours", label: "Oldest Hrs", format: "number" },
          ]}
        />
        <TablePanel
          title="Follow-up counts"
          rows={data.rows.speed_to_lead_follow_up_counts ?? []}
          columns={[
            { key: "metric", label: "Metric" },
            { key: "lead_count", label: "Lead Events", format: "number" },
            { key: "denominator_count", label: "Out Of", format: "number" },
            { key: "share_of_all_leads", label: "% All", format: "percent" },
            { key: "share_of_worked_leads", label: "% Worked", format: "percent" },
          ]}
        />
        <TablePanel
          title="First attempt outcomes"
          rows={data.rows.speed_to_lead_first_attempt_outcomes ?? []}
          columns={[
            { key: "outcome_label", label: "First Result" },
            { key: "channel_label", label: "Channel" },
            { key: "trigger_count", label: "Events", format: "number" },
            { key: "share_of_triggers", label: "Share", format: "percent" },
          ]}
        />
        <TablePanel
          title="Business hours"
          rows={data.rows.speed_to_lead_business_hours ?? []}
          columns={[
            { key: "service_window_label", label: "Window" },
            { key: "total_triggers", label: "Events", format: "number" },
            { key: "worked_lead_rate", label: "Worked", format: "percent" },
            { key: "sla_worked_rate", label: `<=${BOOKING_SLA_LABEL}`, format: "percent" },
            { key: "unworked_leads", label: "Not Worked", format: "number" },
          ]}
        />
        <TablePanel
          title="Reached examples"
          rows={(data.rows.speed_to_lead_reached_examples ?? []).slice(0, 8)}
          columns={[
            { key: "reached_at_et", label: "Reached At" },
            { key: "lead_name", label: "Lead" },
            { key: "source_label", label: "Source" },
            { key: "reached_by", label: "Reached By" },
            { key: "identity_source", label: "Source" },
          ]}
        />
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
      <div className="text-[11px] font-semibold uppercase text-[#66635f]">{label}</div>
      <div className="mt-1 text-lg font-semibold tracking-normal text-[#2d2b28]">{value}</div>
      <div className="truncate text-[11px] text-[#66635f]">{helper}</div>
    </div>
  );
}

function TablePanel({
  title,
  rows,
  columns,
}: {
  title: string;
  rows: DashboardRow[];
  columns: Array<{ key: string; label: string; format?: "number" | "percent" | "duration" }>;
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
                  <td key={column.key} className="max-w-44 border-b border-[#ece9e1] px-2 py-2 first:pl-0">
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

function PanelHeader({ title, helper }: { title: string; helper: string }) {
  return (
    <div>
      <h2 className="text-sm font-semibold">{title}</h2>
      <p className="mt-1 text-xs leading-5 text-[#66635f]">{helper}</p>
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-md border border-dashed border-[#dedbd2] bg-[#fbfaf7] px-3 py-4 text-sm text-[#66635f]">
      {message}
    </div>
  );
}

function buildLeadActionRows(rows: DashboardRow[]): LeadActionRow[] {
  return rows
    .map((row, index) => {
      const laneId = stringValue(row.lane_id) ?? "other";
      const triggerType = stringValue(row.trigger_type) ?? "unknown";
      const ageHours = numberValue(row.age_hours);
      const ageMinutes = numberValue(row.age_minutes);
      const leadPhone = stringValue(row.lead_phone) ?? "No phone";
      const assignedRep = stringValue(row.assigned_rep) ?? "Unassigned";
      const priority = actionPriority(laneId, triggerType, ageHours);
      const exceptionFlags = leadExceptions({
        laneId,
        triggerType,
        leadPhone,
        assignedRep,
        serviceWindow: stringValue(row.service_window) ?? "unknown",
        ageMinutes,
        contactId: stringValue(row.ghl_contact_id) ?? "No contact id",
      });

      return {
        id: stringValue(row.trigger_event_id)
          ?? `${stringValue(row.lead_email) ?? "lead"}-${stringValue(row.trigger_ts) ?? index}`,
        laneId,
        leadName: stringValue(row.lead_name) ?? "Unknown lead",
        leadEmail: stringValue(row.lead_email) ?? "No email",
        leadPhone,
        contactId: stringValue(row.ghl_contact_id) ?? "No contact id",
        assignedRep,
        sourceLabel: stringValue(row.source_label) ?? "Unknown source",
        triggerType,
        triggerTs: stringValue(row.trigger_ts) ?? "",
        triggerTsEt: stringValue(row.trigger_ts_et) ?? "",
        triggerDate: stringValue(row.trigger_date) ?? "Unknown date",
        utmSource: stringValue(row.utm_source) ?? "N/A",
        utmCampaign: stringValue(row.utm_campaign) ?? "N/A",
        serviceWindow: stringValue(row.service_window) ?? "unknown",
        ageMinutes,
        ageHours,
        priorityLabel: priority.label,
        priorityTone: priority.tone,
        nextMove: nextMoveForLead(laneId, triggerType, ageHours),
        managerAction: managerActionForLead(laneId, triggerType, assignedRep, leadPhone, ageMinutes, stringValue(row.service_window) ?? "unknown"),
        exceptionFlags,
      };
    })
    .sort(leadActionSort);
}

function buildLaneCards(actionRows: LeadActionRow[], summaryRows: DashboardRow[]): LaneCard[] {
  const inbound = summaryRow(summaryRows, "inbound_bookings");
  const magnets = summaryRow(summaryRows, "lead_magnets");
  const outbound = summaryRow(summaryRows, "outbound_existing_bookings");
  const freshInbound = actionRows.filter((row) => row.laneId === "inbound_bookings" && isFresh(row));
  const freshMagnets = actionRows.filter((row) => row.laneId === "lead_magnets" && isFresh(row));
  const outboundRows = actionRows.filter((row) => row.laneId === "outbound_existing_bookings" && isFresh(row));
  const staleRows = actionRows.filter((row) => !isFresh(row));

  return [
    {
      id: "fresh_inbound",
      label: "Fresh inbound",
      count: freshInbound.length,
      helper: "Inbound booking rows under 24h with no touch.",
      metric: `${formatPercent(numberValue(inbound?.within_5m_rate))} <=5m`,
      tone: freshInbound.length > 0 ? "red" : scoreTone(numberValue(inbound?.within_5m_rate), 0.25, 0.5),
    },
    {
      id: "fresh_magnets",
      label: "Fresh magnets",
      count: freshMagnets.length,
      helper: "Lead magnet rows under 24h that need bridge or pickup.",
      metric: `${formatPercent(numberValue(magnets?.within_5m_rate))} <=5m`,
      tone: freshMagnets.length > 0 ? "amber" : scoreTone(numberValue(magnets?.within_5m_rate), 0.1, 0.25),
    },
    {
      id: "outbound_existing",
      label: "Outbound/existing",
      count: outboundRows.length,
      helper: "Bookings that should be reviewed separately from inbound SLA.",
      metric: `${formatPercent(numberValue(outbound?.within_sla_rate))} <=${BOOKING_SLA_LABEL}`,
      tone: outboundRows.length > 0 ? "amber" : "blue",
    },
    {
      id: "stale_backlog",
      label: "Stale backlog",
      count: staleRows.length,
      helper: "Older than 24h; cleanup and process evidence.",
      metric: `${formatNumber(totalLaneStaleNoTouch(summaryRows))} total stale`,
      tone: staleRows.length > 0 ? "blue" : "green",
    },
  ];
}

function defaultLane(cards: LaneCard[]): LaneId {
  return cards.find((card) => card.id === "fresh_inbound" && card.count > 0)?.id
    ?? cards.find((card) => card.id === "fresh_magnets" && card.count > 0)?.id
    ?? cards.find((card) => card.id === "outbound_existing" && card.count > 0)?.id
    ?? "stale_backlog";
}

function filterActionRows(rows: LeadActionRow[], lane: LaneId) {
  return rows.filter((row) => {
    if (lane === "fresh_inbound") return row.laneId === "inbound_bookings" && isFresh(row);
    if (lane === "fresh_magnets") return row.laneId === "lead_magnets" && isFresh(row);
    if (lane === "outbound_existing") return row.laneId === "outbound_existing_bookings" && isFresh(row);
    return !isFresh(row);
  });
}

function leadActionSort(left: LeadActionRow, right: LeadActionRow) {
  const leftIntent = laneSortRank(left);
  const rightIntent = laneSortRank(right);
  if (leftIntent !== rightIntent) return leftIntent - rightIntent;

  const leftFresh = (left.ageHours ?? Number.POSITIVE_INFINITY) < 24 ? 0 : 1;
  const rightFresh = (right.ageHours ?? Number.POSITIVE_INFINITY) < 24 ? 0 : 1;
  if (leftFresh !== rightFresh) return leftFresh - rightFresh;

  return (left.ageHours ?? Number.POSITIVE_INFINITY) - (right.ageHours ?? Number.POSITIVE_INFINITY);
}

function actionPriority(laneId: string, triggerType: string, ageHours: number | null): {
  label: string;
  tone: "red" | "amber" | "blue";
} {
  if (laneId === "inbound_bookings" && (ageHours ?? 999) < 24) {
    return { label: "Call now", tone: "red" };
  }

  if (laneId === "lead_magnets" && (ageHours ?? 999) < 24) {
    return { label: "Bridge", tone: "amber" };
  }

  if (laneId === "outbound_existing_bookings" && (ageHours ?? 999) < 24) {
    return { label: "Rescue", tone: "amber" };
  }

  if (triggerType === "appointment_booking") return { label: "Rescue", tone: "amber" };

  return { label: "Backlog", tone: "blue" };
}

function nextMoveForLead(laneId: string, triggerType: string, ageHours: number | null) {
  if (laneId === "inbound_bookings" && (ageHours ?? 999) < 24) {
    return "Call now, then send a direct text if there is no answer.";
  }

  if (laneId === "lead_magnets" && (ageHours ?? 999) < 24) {
    return "Send the bridge text/email; call only if qualified or engaged.";
  }

  if (laneId === "outbound_existing_bookings" && (ageHours ?? 999) < 24) {
    return "Rescue or disposition separately from inbound booking SLA.";
  }

  if (triggerType === "appointment_booking") {
    return "Run booking rescue: call once, text once, then mark the outcome.";
  }

  if ((ageHours ?? 999) < 24) {
    return "Send the bridge text, then call if the profile looks qualified.";
  }

  return "Use as backlog proof; clean up only after fresh leads are clear.";
}

function managerActionForLead(
  laneId: string,
  triggerType: string,
  assignedRep: string,
  leadPhone: string,
  ageMinutes: number | null,
  serviceWindow: string,
) {
  if (assignedRep === "Unassigned") {
    return "Assign an SDR before this counts against rep speed. If no one is available, route to the fastest eligible owner.";
  }

  if (leadPhone === "No phone") {
    return "Do not treat this as a dial-speed miss. Fix the contact path or use email/SMS follow-up if available.";
  }

  if (serviceWindow === "after_hours") {
    return "Queue for the next business block unless the team has an after-hours coverage policy.";
  }

  if ((ageMinutes ?? 0) >= 24 * 60) {
    return "This is no longer a speed save. Reassign or disposition it, then inspect why the owner missed the lane.";
  }

  if (laneId === "inbound_bookings") {
    return "This is the highest-priority live save. If the owner is not actively dialing, reassign this lead now.";
  }

  if (laneId === "lead_magnets") {
    return "Send the bridge follow-up first, then call only if fit or engagement justifies taking SDR time.";
  }

  if (laneId === "outbound_existing_bookings" || triggerType === "appointment_booking") {
    return "Rescue or disposition, but keep the read separate from fresh inbound routing accountability.";
  }

  return "Work only after fresh booking demand is clear.";
}

function leadExceptions({
  laneId,
  triggerType,
  leadPhone,
  assignedRep,
  serviceWindow,
  ageMinutes,
  contactId,
}: {
  laneId: string;
  triggerType: string;
  leadPhone: string;
  assignedRep: string;
  serviceWindow: string;
  ageMinutes: number | null;
  contactId: string;
}): LeadException[] {
  const exceptions: LeadException[] = [];

  if (assignedRep === "Unassigned") {
    exceptions.push({
      label: "Unassigned",
      helper: "Routing must be fixed before SDR accountability.",
      tone: "red",
    });
  }

  if (leadPhone === "No phone") {
    exceptions.push({
      label: "No phone",
      helper: "Cannot be judged as a dial miss.",
      tone: "amber",
    });
  }

  if (contactId === "No contact id") {
    exceptions.push({
      label: "No contact ID",
      helper: "Needs CRM identity QA.",
      tone: "amber",
    });
  }

  if (serviceWindow === "after_hours") {
    exceptions.push({
      label: "After hours",
      helper: "Needs coverage policy before blame.",
      tone: "blue",
    });
  }

  if ((ageMinutes ?? 0) >= 24 * 60) {
    exceptions.push({
      label: "Stale",
      helper: "Cleanup/reassignment evidence.",
      tone: "blue",
    });
  }

  if (laneId === "outbound_existing_bookings") {
    exceptions.push({
      label: "Outbound/existing",
      helper: "Separate from fresh inbound SLA.",
      tone: "amber",
    });
  }

  if (exceptions.length === 0) {
    exceptions.push({
      label: triggerType === "appointment_booking" ? "True SDR miss" : "No touch",
      helper: "No exception found in current mart.",
      tone: triggerType === "appointment_booking" ? "red" : "amber",
    });
  }

  return exceptions;
}

function queueDescription(lane: LaneId) {
  if (lane === "fresh_inbound") {
    return "Inbound booking rows under 24h. This is the highest-value lane because SDR action can still rescue speed.";
  }
  if (lane === "fresh_magnets") {
    return "Lead magnet rows under 24h. Bridge quickly, then call only the best-fit prospects after bookings are clear.";
  }
  if (lane === "outbound_existing") {
    return "Outbound or existing-booking rows under 24h. Work them, but keep them separate from fresh inbound SLA.";
  }
  return "Older no-touch rows. Treat these as cleanup and process evidence, not the main live SDR priority.";
}

function laneEvidenceRow(rows: DashboardRow[], laneId: string, label: string) {
  const row = summaryRow(rows, laneId);
  return {
    label,
    value: formatNumber(numberValue(row?.lead_events)),
    helper: `${formatPercent(numberValue(row?.within_5m_rate))} <=5m, ${formatNumber(numberValue(row?.no_touch))} no-touch`,
  };
}

function summaryRow(rows: DashboardRow[], laneId: string) {
  return rows.find((row) => stringValue(row.lane_id) === laneId);
}

function totalLaneStaleNoTouch(rows: DashboardRow[]) {
  return rows.reduce((sum, row) => sum + (numberValue(row.stale_no_touch) ?? 0), 0);
}

function isFresh(row: LeadActionRow) {
  return (row.ageHours ?? Number.POSITIVE_INFINITY) < 24;
}

function laneSortRank(row: LeadActionRow) {
  if (row.laneId === "inbound_bookings" && isFresh(row)) return 0;
  if (row.laneId === "lead_magnets" && isFresh(row)) return 1;
  if (row.laneId === "outbound_existing_bookings" && isFresh(row)) return 2;
  return 3;
}

function scoreTone(value: number | null, amberThreshold: number, greenThreshold: number): ScoreCard["tone"] {
  if (value === null) return "blue";
  if (value >= greenThreshold) return "green";
  if (value >= amberThreshold) return "amber";
  return "red";
}

function scoreDotClass(tone: ScoreCard["tone"]) {
  const classes = {
    green: "stl-dot-green",
    blue: "stl-dot-blue",
    amber: "stl-dot-amber",
    red: "stl-dot-red",
  };

  return classes[tone];
}

function actionToneClass(tone: LeadActionRow["priorityTone"]) {
  const classes = {
    red: "stl-chip-red",
    amber: "stl-chip-amber",
    blue: "stl-chip-blue",
  };

  return classes[tone];
}

function exceptionClass(tone: LeadException["tone"]) {
  const classes = {
    red: "stl-chip-red",
    amber: "stl-chip-amber",
    blue: "stl-chip-blue",
  };

  return classes[tone];
}

function darkExceptionClass(tone: LeadException["tone"]) {
  const classes = {
    red: "stl-dark-chip-red",
    amber: "stl-dark-chip-amber",
    blue: "stl-dark-chip-blue",
  };

  return classes[tone];
}

function darkPriorityClass(tone: LeadActionRow["priorityTone"]) {
  const classes = {
    red: "stl-dark-chip-red",
    amber: "stl-dark-chip-amber",
    blue: "stl-dark-chip-blue",
  };

  return classes[tone];
}

function routingStatusClass(tone: "green" | "amber" | "red" | "blue") {
  const classes = {
    green: "stl-chip-green",
    amber: "stl-chip-amber",
    red: "stl-chip-red",
    blue: "stl-chip-blue",
  };

  return classes[tone];
}

function routingDecision(row: DashboardRow): {
  label: string;
  helper: string;
  tone: "green" | "amber" | "red" | "blue";
} {
  const assignedRep = stringValue(row.assigned_rep) ?? "Unassigned";
  const ownedLeads = numberValue(row.owned_leads) ?? 0;
  const freshUnworked = numberValue(row.fresh_unworked_leads) ?? 0;
  const unworkedBookings = numberValue(row.unworked_bookings) ?? 0;
  const within5m = numberValue(row.first_attempt_within_5m_rate);

  if (assignedRep === "Unassigned") {
    return {
      label: "Fix routing",
      helper: "Leads have no accountable SDR owner.",
      tone: "red",
    };
  }

  if (unworkedBookings > 0 || freshUnworked >= 3) {
    return {
      label: "Pause / reassign",
      helper: "Fresh demand is open under this owner.",
      tone: "red",
    };
  }

  if (freshUnworked > 0 || (ownedLeads >= 5 && within5m !== null && within5m < 0.25)) {
    return {
      label: "Watch",
      helper: "Keep visible before routing more volume.",
      tone: "amber",
    };
  }

  if (ownedLeads === 0) {
    return {
      label: "No volume",
      helper: "Not enough current signal to judge routing.",
      tone: "blue",
    };
  }

  return {
    label: "Keep routing",
    helper: "No fresh ownership blockers in this range.",
    tone: "green",
  };
}

function exceptionTone(key: string | null): "green" | "amber" | "red" | "blue" {
  if (key === "unassigned" || key === "stale_no_touch") return "red";
  if (key === "no_phone" || key === "outbound_existing") return "amber";
  return "blue";
}

function metricRow(rows: DashboardRow[], metric: string) {
  return rows.find((row) => row.metric === metric);
}

function pillClass(source: string) {
  return sourcePillClass[source] ?? "stl-chip-blue";
}

function formatValue(value: DashboardRowValue | undefined, format?: "number" | "percent" | "duration") {
  if (format === "number") return formatNumber(numberValue(value));
  if (format === "percent") return formatPercent(numberValue(value));
  if (format === "duration") return formatDuration(numberValue(value));
  return stringValue(value) ?? "N/A";
}

function formatNumber(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function formatPercent(value: number | null) {
  if (value === null || !Number.isFinite(value)) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "percent",
    minimumFractionDigits: value > 0 && value < 0.1 ? 1 : 0,
    maximumFractionDigits: value > 0 && value < 0.1 ? 1 : 0,
  }).format(value);
}

function formatDuration(minutes: number | null) {
  if (minutes === null || !Number.isFinite(minutes)) return "N/A";
  if (minutes >= 1440) return `${(minutes / 1440).toFixed(1)}d`;
  if (minutes >= 60) return `${(minutes / 60).toFixed(1)}h`;
  return `${minutes.toFixed(1)}m`;
}

function formatAgeHours(hours: number | null) {
  if (hours === null || !Number.isFinite(hours)) return "N/A";
  if (hours >= 48) return `${Math.round(hours / 24)}d`;
  return `${formatNumber(hours)}h`;
}

function formatAgeMinutes(minutes: number | null) {
  if (minutes === null || !Number.isFinite(minutes)) return "N/A";
  if (minutes >= 1440) return `${Math.round(minutes / 1440)}d`;
  if (minutes >= 60) return `${Math.floor(minutes / 60)}h ${Math.round(minutes % 60)}m`;
  return `${Math.max(0, Math.round(minutes))}m`;
}

function slaTimerLabel(ageMinutes: number | null) {
  if (ageMinutes === null || !Number.isFinite(ageMinutes)) return "No age";
  const remaining = 45 - ageMinutes;
  if (remaining > 0) return `${formatAgeMinutes(remaining)} left`;
  return `Past SLA ${formatAgeMinutes(Math.abs(remaining))}`;
}

function formatServiceWindow(value: string) {
  if (value === "business_hours") return "Business hours";
  if (value === "after_hours") return "After hours";
  return "Unknown window";
}

function formatTriggerType(value: DashboardRowValue | undefined) {
  const label = stringValue(value);
  if (!label) return "N/A";
  return label
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function percentWidth(value: number | null, enforceMinimum = true) {
  if (value === null || !Number.isFinite(value) || value <= 0) return "0%";
  const width = Math.min(100, Math.max(enforceMinimum ? 3 : 0, value * 100));
  return `${width}%`;
}

function sparklinePath(rows: DashboardRow[], key: string) {
  const points = rows
    .map((row) => ({
      value: numberValue(row[key]),
    }))
    .filter((point) => point.value !== null);

  if (points.length === 0) return "";

  const width = 100;
  const height = 42;
  return points
    .map((point, pointIndex) => {
      const x = points.length === 1 ? width : (pointIndex / (points.length - 1)) * width;
      const y = height - Math.max(0, Math.min(1, point.value ?? 0)) * (height - 4) - 2;
      return `${pointIndex === 0 ? "M" : "L"}${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
}

function groupRowsByValue(rows: DashboardRow[], key: string) {
  const groups = new Map<string, DashboardRow[]>();
  rows.forEach((row) => {
    const label = stringValue(row[key]) ?? "unknown";
    groups.set(label, [...(groups.get(label) ?? []), row]);
  });

  return Array.from(groups, ([label, groupRows]) => ({
    label,
    rows: groupRows,
    total: sumRows(groupRows, "triggers"),
  })).sort((left, right) => right.total - left.total);
}

function sumRows(rows: DashboardRow[], key: string) {
  return rows.reduce((sum, row) => sum + (numberValue(row[key]) ?? 0), 0);
}

const responseBucketOrder = ["<=1m", "1-5m", "5-15m", "15-60m", "1-24h", ">24h", "no touch"];

function sortResponseBuckets(rows: DashboardRow[]) {
  return [...rows].sort((left, right) => {
    const leftBucket = stringValue(left.response_bucket) ?? "";
    const rightBucket = stringValue(right.response_bucket) ?? "";
    return responseBucketIndex(leftBucket) - responseBucketIndex(rightBucket);
  });
}

function responseBucketIndex(bucket: string) {
  const index = responseBucketOrder.indexOf(bucket);
  return index === -1 ? responseBucketOrder.length : index;
}

function bucketColor(bucket: string) {
  const colors: Record<string, string> = {
    "<=1m": "stl-bucket-fast",
    "1-5m": "stl-bucket-fast",
    "5-15m": "stl-bucket-ok",
    "15-60m": "stl-bucket-watch",
    "1-24h": "stl-bucket-late",
    ">24h": "stl-bucket-late",
    "no touch": "stl-bucket-late",
  };

  return colors[bucket] ?? "stl-bucket-neutral";
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
