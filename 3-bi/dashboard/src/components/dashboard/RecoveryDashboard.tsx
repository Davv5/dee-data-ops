"use client";

import type { ComponentType, ReactNode } from "react";
import {
  CalendarClock,
  CalendarX,
  CreditCard,
  DatabaseZap,
  FileWarning,
  Flame,
  Mail,
  Phone,
  RefreshCcw,
  RotateCcw,
  ShieldAlert,
  Wrench,
} from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import type {
  DashboardData,
  DashboardFilters,
  DashboardFreshness,
  DashboardRow,
} from "@/types/dashboard-data";

type Tone = "green" | "blue" | "amber" | "red" | "neutral";
type Icon = ComponentType<{ className?: string; "aria-hidden"?: boolean }>;

const toneStyles: Record<Tone, { bg: string; border: string; fg: string; soft: string }> = {
  green: {
    bg: "#f0fdf4",
    border: "#bbf7d0",
    fg: "#166534",
    soft: "rgba(22, 101, 52, 0.08)",
  },
  blue: {
    bg: "#eff6ff",
    border: "#bfdbfe",
    fg: "#1d4ed8",
    soft: "rgba(29, 78, 216, 0.08)",
  },
  amber: {
    bg: "#fffbeb",
    border: "#fde68a",
    fg: "#92400e",
    soft: "rgba(146, 64, 14, 0.09)",
  },
  red: {
    bg: "#fef2f2",
    border: "#fecaca",
    fg: "#991b1b",
    soft: "rgba(153, 27, 27, 0.08)",
  },
  neutral: {
    bg: "#f7f7f4",
    border: "#dedbd2",
    fg: "#3b3936",
    soft: "rgba(59, 57, 54, 0.06)",
  },
};

export function RecoveryDashboard({ data }: { data: DashboardData }) {
  const summary = data.rows.recovery_summary?.[0];
  const queue = data.rows.recovery_action_queue ?? [];
  const paymentHealth = data.rows.recovery_payment_health ?? [];
  const collectionHealth = data.rows.recovery_collection_health ?? [];
  const canceledOutcomes = data.rows.recovery_canceled_outcomes ?? [];
  const sources = data.rows.recovery_source_breakdown ?? [];
  const missedMeetings = data.rows.recovery_missed_meetings ?? [];
  const gaps = data.rows.recovery_data_gaps ?? [];
  const freshness = data.rows.recovery_source_freshness ?? [];

  return (
    <div className="space-y-4 pb-12">
      <Header
        filters={data.filters}
        freshness={data.freshness}
        summary={summary}
      />

      {data.error ? (
        <section className="rounded-lg border border-[#fecaca] bg-[#fef2f2] px-4 py-3 text-sm text-[#991b1b]">
          {data.error}
        </section>
      ) : null}

      <KpiStrip summary={summary} />

      <section className="grid gap-3 xl:grid-cols-[minmax(0,1.42fr)_minmax(23rem,0.58fr)]">
        <RecoveryQueue rows={queue} filters={data.filters} />
        <GapPanel rows={gaps} />
      </section>

      <section className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <PaymentRecoveryPanel rows={paymentHealth} />
        <CanceledOutcomePanel rows={canceledOutcomes} />
      </section>

      <section className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <CollectionPanel rows={collectionHealth} />
        <SourceBreakdown rows={sources} />
      </section>

      <section className="grid gap-3 xl:grid-cols-[minmax(0,1.25fr)_minmax(22rem,0.75fr)]">
        <MissedMeetingsPanel rows={missedMeetings} />
        <SourceFreshnessPanel rows={freshness} generatedAt={data.generatedAt} />
      </section>

      <AuditDetails data={data} />
    </div>
  );
}

function Header({
  filters,
  freshness,
  summary,
}: {
  filters: DashboardFilters;
  freshness: DashboardFreshness;
  summary: DashboardRow | undefined;
}) {
  const openSurface = numberValue(summary?.open_recovery_surface_count);
  const money = numberValue(summary?.open_money_at_stake);

  return (
    <header
      className="rounded-xl border px-4 py-4 shadow-sm md:px-5"
      style={{
        background: "var(--stl-card-strong)",
        borderColor: "var(--stl-border)",
        boxShadow: "var(--stl-shadow-soft)",
      }}
    >
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
        <div className="flex items-start gap-3">
          <span
            className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md text-white"
            style={{ background: "var(--warning)" }}
          >
            <RotateCcw className="h-5 w-5" aria-hidden />
          </span>
          <div className="min-w-0">
            <p className="text-sm font-medium text-[#0f766e]">Recovery Command</p>
            <h1
              className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl"
              style={{ color: "var(--stl-text)", fontFamily: "var(--font-display)" }}
            >
              Money Still On The Table
            </h1>
            <p className="mt-2 max-w-4xl text-sm leading-6" style={{ color: "var(--stl-muted)" }}>
              Payment recovery, canceled-booking rescue, manual collections, contract-term evidence, and source gaps in one work surface.
            </p>
          </div>
        </div>

        <div className="flex flex-col gap-2 lg:items-end">
          <FreshnessBadge freshness={freshness} />
          <TimeRange filters={filters} />
        </div>
      </div>

      <div className="mt-4 grid gap-2 md:grid-cols-3">
        <HeroMetric
          label="Open recovery surface"
          value={formatNumber(openSurface)}
          helper={`${filters.timeRangeLabel} evidence window`}
          icon={ShieldAlert}
          tone="amber"
        />
        <HeroMetric
          label="Action queue money"
          value={formatCurrency(money)}
          helper={`${formatNumber(numberValue(summary?.open_recovery_actions))} open customer actions`}
          icon={CreditCard}
          tone="green"
        />
        <HeroMetric
          label="Known data gaps"
          value={formatNumber(numberValue(summary?.known_gap_count))}
          helper="Visible before action, not hidden"
          icon={FileWarning}
          tone={numberValue(summary?.known_gap_count) ? "red" : "green"}
        />
      </div>
    </header>
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
            href={`/recovery?range=${option.value}`}
            aria-current={isActive ? "page" : undefined}
            title={option.description}
            className={`min-w-10 rounded px-2.5 py-1 text-center text-xs font-semibold transition ${
              isActive ? "text-white" : "hover:opacity-90"
            }`}
            style={
              isActive
                ? { background: "var(--warning)" }
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

function HeroMetric({
  label,
  value,
  helper,
  icon: IconComponent,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  icon: Icon;
  tone: Tone;
}) {
  const colors = toneStyles[tone];

  return (
    <div
      className="flex items-center gap-3 rounded-lg border px-3 py-2.5"
      style={{
        borderColor: colors.border,
        background: colors.bg,
        color: colors.fg,
      }}
    >
      <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-white/60">
        <IconComponent className="h-4 w-4" aria-hidden />
      </span>
      <div className="min-w-0">
        <div className="text-[10px] font-semibold uppercase tracking-wide">{label}</div>
        <div className="truncate text-2xl font-semibold leading-tight" style={{ fontFamily: "var(--font-metric)" }}>
          {value}
        </div>
        <div className="truncate text-[11px] opacity-80">{helper}</div>
      </div>
    </div>
  );
}

function KpiStrip({ summary }: { summary: DashboardRow | undefined }) {
  const paymentRecovery = numberValue(summary?.payment_recovery_customers);
  const failedPlans = numberValue(summary?.failed_plan_customers);
  const dueNow = numberValue(summary?.active_due_customers);
  const manualReview = numberValue(summary?.manual_collection_review_customers);
  const canceledNotRecovered = numberValue(summary?.canceled_not_recovered);
  const boughtAfterCancel = numberValue(summary?.bought_after_cancel_bookings);
  const transcriptTerms = numberValue(summary?.transcript_terms_customers);
  const bookedNeverAttended = numberValue(summary?.booked_never_attended_leads);

  return (
    <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
      <KpiCard
        title="Payment Recovery"
        value={formatNumber(paymentRecovery)}
        helper={`${formatNumber(failedPlans)} failed · ${formatNumber(dueNow)} due/no pay`}
        icon={CreditCard}
        tone={paymentRecovery ? "amber" : "green"}
      />
      <KpiCard
        title="Manual Collections"
        value={formatNumber(manualReview)}
        helper={formatCurrency(numberValue(summary?.manual_collection_lifetime_value))}
        icon={RefreshCcw}
        tone={manualReview ? "amber" : "green"}
      />
      <KpiCard
        title="Canceled Not Recovered"
        value={formatNumber(canceledNotRecovered)}
        helper={`${formatNumber(boughtAfterCancel)} bought after cancel`}
        icon={CalendarX}
        tone={canceledNotRecovered ? "red" : "green"}
      />
      <KpiCard
        title="Evidence To Review"
        value={formatNumber((transcriptTerms ?? 0) + (bookedNeverAttended ?? 0))}
        helper={`${formatNumber(transcriptTerms)} contract terms · ${formatNumber(bookedNeverAttended)} no-shows`}
        icon={DatabaseZap}
        tone="blue"
      />
    </section>
  );
}

function KpiCard({
  title,
  value,
  helper,
  icon: IconComponent,
  tone,
}: {
  title: string;
  value: string;
  helper: string;
  icon: Icon;
  tone: Tone;
}) {
  const colors = toneStyles[tone];

  return (
    <div
      className="rounded-lg border px-4 py-3"
      style={{ borderColor: colors.border, background: "var(--stl-card-strong)" }}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div
            className="flex items-center gap-1.5 text-[11px] font-semibold uppercase tracking-wide"
            style={{ color: colors.fg }}
          >
            <IconComponent className="h-4 w-4" aria-hidden />
            {title}
          </div>
          <div
            className="mt-1 text-3xl font-semibold leading-tight"
            style={{ color: "var(--stl-text)", fontFamily: "var(--font-metric)" }}
          >
            {value}
          </div>
          <div className="mt-1 text-xs" style={{ color: "var(--stl-muted)" }}>
            {helper}
          </div>
        </div>
      </div>
    </div>
  );
}

function RecoveryQueue({ rows, filters }: { rows: DashboardRow[]; filters: DashboardFilters }) {
  return (
    <Panel
      title="Recovery Work Queue"
      eyebrow={`${formatNumber(rows.length)} rows`}
      icon={ShieldAlert}
      action={<span className="text-xs text-[#66635f]">{filters.timeRangeLabel} window</span>}
    >
      {rows.length === 0 ? (
        <EmptyState>No open recovery rows in this window.</EmptyState>
      ) : (
        <div className="overflow-hidden rounded-md border border-[#dedbd2]">
          <div className="grid grid-cols-[minmax(14rem,1.2fr)_minmax(10rem,0.75fr)_minmax(10rem,0.72fr)_minmax(7rem,0.42fr)] gap-3 border-b border-[#dedbd2] bg-[#f7f7f4] px-3 py-2 text-[11px] font-semibold uppercase tracking-wide text-[#66635f] max-lg:hidden">
            <div>Customer / action</div>
            <div>Evidence</div>
            <div>Route</div>
            <div className="text-right">Value</div>
          </div>
          <div className="divide-y divide-[#dedbd2]">
            {rows.map((row) => (
              <QueueRow key={stringValue(row.source_record_id) ?? `${row.contact_sk}-${row.action_bucket}`} row={row} />
            ))}
          </div>
        </div>
      )}
    </Panel>
  );
}

function QueueRow({ row }: { row: DashboardRow }) {
  const area = stringValue(row.action_area) ?? "recovery";
  const tone = area === "booking_recovery" ? "red" : area === "contract_terms" ? "blue" : "amber";
  const colors = toneStyles[tone];
  const contactSk = stringValue(row.contact_sk);
  const email = stringValue(row.email_norm);
  const phone = stringValue(row.phone);

  return (
    <div className="grid gap-3 px-3 py-3 lg:grid-cols-[minmax(14rem,1.2fr)_minmax(10rem,0.75fr)_minmax(10rem,0.72fr)_minmax(7rem,0.42fr)] lg:items-center">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span
            className="rounded-md border px-2 py-1 text-[10px] font-semibold uppercase tracking-wide"
            style={{ borderColor: colors.border, background: colors.bg, color: colors.fg }}
          >
            {labelize(area)}
          </span>
          <span className="text-[11px] text-[#66635f]">
            Priority {formatNumber(numberValue(row.priority_rank))}
          </span>
        </div>
        <div className="mt-1 truncate text-sm font-semibold text-[#171d22]">
          {contactSk ? (
            <a href={customerHref(contactSk)} className="underline-offset-2 hover:underline">
              {stringValue(row.customer_display_name) ?? "Unknown customer"}
            </a>
          ) : (
            stringValue(row.customer_display_name) ?? "Unknown customer"
          )}
        </div>
        <div className="mt-1 line-clamp-2 text-xs leading-5 text-[#66635f]">
          {stringValue(row.action_label)} · {stringValue(row.action_reason)}
        </div>
      </div>

      <div className="min-w-0 text-xs leading-5 text-[#66635f]">
        <div className="truncate font-medium text-[#2d2b28]">
          {stringValue(row.top_product_by_net_revenue) ?? stringValue(row.cancel_reason) ?? stringValue(row.source_table) ?? "Evidence"}
        </div>
        <div className="truncate">
          {stringValue(row.latest_prior_lead_magnet_name) ??
            stringValue(row.cancelled_by_type) ??
            stringValue(row.source_event_label) ??
            "No secondary evidence"}
        </div>
      </div>

      <div className="min-w-0">
        <div className="flex flex-wrap gap-1.5">
          {phone ? <ContactPill href={`tel:${cleanPhone(phone)}`} icon={Phone} label="Call" /> : null}
          {email ? <ContactPill href={`mailto:${email}`} icon={Mail} label="Email" /> : null}
        </div>
        <div className="mt-1 truncate text-[11px] text-[#66635f]">
          {stringValue(row.recommended_channel_label) ?? "Audit first"}
        </div>
      </div>

      <div className="text-left lg:text-right">
        <div className="text-sm font-semibold text-[#171d22]" style={{ fontFamily: "var(--font-metric)" }}>
          {formatCurrency(numberValue(row.money_at_stake))}
        </div>
        <div className="text-[11px] text-[#66635f]">
          {stringValue(row.source_event_label) ?? "No date"}
        </div>
      </div>
    </div>
  );
}

function ContactPill({ href, icon: IconComponent, label }: { href: string; icon: Icon; label: string }) {
  return (
    <a
      href={href}
      className="inline-flex items-center gap-1 rounded-md border border-[#dedbd2] px-2 py-1 text-[11px] font-semibold text-[#3b3936] hover:bg-[#f3f1ea]"
    >
      <IconComponent className="h-3.5 w-3.5" aria-hidden />
      {label}
    </a>
  );
}

function GapPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <Panel title="Main Gaps" eyebrow="source truth" icon={Wrench}>
      {rows.length === 0 ? (
        <EmptyState>No gap rows returned.</EmptyState>
      ) : (
        <div className="space-y-2">
          {rows.map((row) => {
            const affected = numberValue(row.affected_count) ?? 0;
            const tone: Tone = affected > 500 ? "red" : affected > 0 ? "amber" : "green";
            const colors = toneStyles[tone];
            return (
              <div
                key={stringValue(row.gap_key) ?? stringValue(row.gap_label)}
                className="rounded-md border px-3 py-2"
                style={{ borderColor: colors.border, background: colors.bg }}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-sm font-semibold" style={{ color: colors.fg }}>
                      {stringValue(row.gap_label)}
                    </div>
                    <div className="mt-1 text-xs leading-5 text-[#66635f]">
                      {stringValue(row.gap_detail)}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-lg font-semibold" style={{ color: colors.fg, fontFamily: "var(--font-metric)" }}>
                      {formatNumber(affected)}
                    </div>
                    <div className="text-[10px] uppercase tracking-wide text-[#66635f]">affected</div>
                  </div>
                </div>
                <div className="mt-2 text-[11px] leading-5 text-[#3b3936]">
                  {stringValue(row.recommended_fix)}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Panel>
  );
}

function PaymentRecoveryPanel({ rows }: { rows: DashboardRow[] }) {
  const max = maxNumber(rows, "customers");

  return (
    <Panel title="Payment Recovery" eyebrow="Fanbasis + Stripe truth" icon={CreditCard}>
      <div className="space-y-2">
        {rows.map((row) => {
          const customers = numberValue(row.customers) ?? 0;
          const status = stringValue(row.payment_plan_health_status) ?? "";
          const tone: Tone =
            status === "failed_plan_recovery_needed" || status === "active_plan_due_no_payment_yet"
              ? "red"
              : status === "one_time_upsell_candidate" || status === "completed_plan_paid_off"
                ? "amber"
                : "neutral";
          return (
            <BarRow
              key={status}
              label={stringValue(row.health_label) ?? labelize(status)}
              value={formatNumber(customers)}
              helper={`${formatCurrency(numberValue(row.lifetime_net_revenue))} lifetime · ${formatNumber(numberValue(row.missed_now_customers))} missed`}
              share={max > 0 ? customers / max : 0}
              tone={tone}
            />
          );
        })}
      </div>
    </Panel>
  );
}

function CanceledOutcomePanel({ rows }: { rows: DashboardRow[] }) {
  const max = maxNumber(rows, "canceled_bookings");

  return (
    <Panel title="Canceled Booking Recovery" eyebrow="Calendly → Fathom → Revenue" icon={CalendarX}>
      <div className="space-y-2">
        {rows.map((row) => {
          const outcome = stringValue(row.recovery_outcome) ?? "";
          const count = numberValue(row.canceled_bookings) ?? 0;
          const tone: Tone =
            outcome === "not_recovered_yet" || outcome === "rebooked_no_show"
              ? "red"
              : outcome === "bought_after_cancel" || outcome === "fathom_show_after_cancel"
                ? "green"
                : outcome === "contact_not_matched"
                  ? "amber"
                  : "blue";
          return (
            <BarRow
              key={outcome}
              label={stringValue(row.outcome_label) ?? labelize(outcome)}
              value={formatNumber(count)}
              helper={`${formatNumber(numberValue(row.contacts))} contacts · ${formatCurrency(numberValue(row.credited_net_revenue_after_first_cancel))} credited`}
              share={max > 0 ? count / max : 0}
              tone={tone}
            />
          );
        })}
      </div>
    </Panel>
  );
}

function CollectionPanel({ rows }: { rows: DashboardRow[] }) {
  const reviewRows = rows.filter((row) => {
    const status = stringValue(row.collection_health_status) ?? "";
    return status !== "no_collection_signal";
  });
  const max = maxNumber(reviewRows, "customers");

  return (
    <Panel title="Manual Collection Signals" eyebrow="post-first-payment evidence" icon={RefreshCcw}>
      <div className="space-y-2">
        {reviewRows.map((row) => {
          const status = stringValue(row.collection_health_status) ?? "";
          const customers = numberValue(row.customers) ?? 0;
          const tone: Tone =
            status.includes("stale") || status.includes("no_payment")
              ? "red"
              : status.includes("recent")
                ? "green"
                : "amber";
          return (
            <BarRow
              key={`${status}-${stringValue(row.collection_motion_type)}`}
              label={stringValue(row.collection_health_label) ?? labelize(status)}
              value={formatNumber(customers)}
              helper={`${formatCurrency(numberValue(row.post_first_collected_net_revenue))} after first pay · ${formatNumber(numberValue(row.collection_booking_customers))} collection calls`}
              share={max > 0 ? customers / max : 0}
              tone={tone}
            />
          );
        })}
      </div>
    </Panel>
  );
}

function SourceBreakdown({ rows }: { rows: DashboardRow[] }) {
  const max = maxNumber(rows, "recoverable_count");

  return (
    <Panel title="No-Show Sources" eyebrow="booked, never attended" icon={Flame}>
      <div className="space-y-2">
        {rows.map((row) => {
          const total = numberValue(row.recoverable_count) ?? 0;
          return (
            <BarRow
              key={stringValue(row.source_label) ?? "Unknown"}
              label={stringValue(row.source_label) ?? "Unknown"}
              value={formatNumber(total)}
              helper={`${formatNumber(numberValue(row.hot_count))} hot · ${formatNumber(numberValue(row.warm_count))} warm · avg ${formatDays(numberValue(row.avg_age_days))}`}
              share={max > 0 ? total / max : 0}
              tone={numberValue(row.hot_count) ? "red" : "neutral"}
            />
          );
        })}
      </div>
    </Panel>
  );
}

function MissedMeetingsPanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <Panel title="Fresh No-Show Rescue" eyebrow={`${formatNumber(rows.length)} shown`} icon={CalendarClock}>
      {rows.length === 0 ? (
        <EmptyState>No booked-never-attended leads in this window.</EmptyState>
      ) : (
        <div className="divide-y divide-[#dedbd2] rounded-md border border-[#dedbd2]">
          {rows.map((row) => {
            const days = numberValue(row.days_since_missed);
            const tone: Tone = days == null ? "neutral" : days <= 7 ? "red" : days <= 30 ? "amber" : "neutral";
            const colors = toneStyles[tone];
            const email = stringValue(row.lead_email);
            const phone = stringValue(row.lead_phone);
            return (
              <div key={stringValue(row.golden_contact_key)} className="grid gap-2 px-3 py-2.5 md:grid-cols-[minmax(12rem,1fr)_auto] md:items-center">
                <div className="min-w-0">
                  <div className="truncate text-sm font-semibold text-[#171d22]">
                    {stringValue(row.lead_name) ?? "Unknown lead"}
                  </div>
                  <div className="mt-1 flex min-w-0 flex-wrap items-center gap-2 text-[11px] text-[#66635f]">
                    {email && email !== "No email" ? <a href={`mailto:${email}`} className="underline-offset-2 hover:underline">{email}</a> : null}
                    {phone && phone !== "No phone" ? <a href={`tel:${cleanPhone(phone)}`} className="underline-offset-2 hover:underline">{phone}</a> : null}
                    <span>{stringValue(row.source_label) ?? "Unknown source"}</span>
                  </div>
                </div>
                <div className="text-left md:text-right">
                  <div className="text-xs font-semibold" style={{ color: colors.fg }}>
                    {days != null ? `${days}d ago` : "No date"}
                  </div>
                  <div className="text-[11px] text-[#66635f]">
                    {stringValue(row.last_meeting_et)}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Panel>
  );
}

function SourceFreshnessPanel({ rows, generatedAt }: { rows: DashboardRow[]; generatedAt: string }) {
  const visibleRows = rows.slice(0, 12);
  const generatedAtMs = new Date(generatedAt).getTime();

  return (
    <Panel title="Source Capture" eyebrow="raw feed freshness" icon={DatabaseZap}>
      {visibleRows.length === 0 ? (
        <EmptyState>Source freshness checks did not return rows.</EmptyState>
      ) : (
        <div className="space-y-2">
          {visibleRows.map((row) => {
            const ingestedAt = stringValue(row.max_ingested_iso);
            const ingestedAtMs = ingestedAt ? new Date(ingestedAt).getTime() : NaN;
            const ageMinutes =
              Number.isFinite(generatedAtMs) && Number.isFinite(ingestedAtMs)
                ? Math.max(0, Math.round((generatedAtMs - ingestedAtMs) / 60000))
                : null;
            const tone: Tone = ageMinutes == null ? "neutral" : ageMinutes <= 360 ? "green" : ageMinutes <= 1440 ? "amber" : "red";
            const colors = toneStyles[tone];
            return (
              <div
                key={stringValue(row.source_table) ?? stringValue(row.source_label)}
                className="rounded-md border px-3 py-2"
                style={{ borderColor: colors.border, background: colors.soft }}
              >
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-sm font-semibold text-[#171d22]">
                      {stringValue(row.source_label)}
                    </div>
                    <div className="truncate text-[11px] text-[#66635f]">
                      {stringValue(row.coverage_note)}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-xs font-semibold" style={{ color: colors.fg }}>
                      {ageMinutes == null ? "Unknown" : formatAge(ageMinutes)}
                    </div>
                    <div className="text-[10px] uppercase tracking-wide text-[#66635f]">
                      {formatNumber(numberValue(row.row_count))} rows
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </Panel>
  );
}

function BarRow({
  label,
  value,
  helper,
  share,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  share: number;
  tone: Tone;
}) {
  const colors = toneStyles[tone];
  const width = `${Math.max(3, Math.min(100, share * 100))}%`;

  return (
    <div className="rounded-md border border-[#dedbd2] bg-white/70 px-3 py-2">
      <div className="flex items-baseline justify-between gap-3">
        <div className="min-w-0 truncate text-sm font-medium text-[#171d22]">{label}</div>
        <div className="shrink-0 text-sm font-semibold text-[#171d22]" style={{ fontFamily: "var(--font-metric)" }}>
          {value}
        </div>
      </div>
      <div className="mt-2 h-1.5 overflow-hidden rounded-sm bg-[#dedbd2]">
        <div className="h-full rounded-sm" style={{ width, background: colors.fg }} />
      </div>
      <div className="mt-1.5 text-[11px] leading-5 text-[#66635f]">{helper}</div>
    </div>
  );
}

function Panel({
  title,
  eyebrow,
  icon: IconComponent,
  action,
  children,
}: {
  title: string;
  eyebrow: string;
  icon: Icon;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section
      className="overflow-hidden rounded-xl border"
      style={{
        borderColor: "var(--stl-border)",
        background: "var(--stl-card)",
        boxShadow: "var(--stl-shadow-soft)",
      }}
    >
      <header className="flex items-start justify-between gap-3 border-b px-4 py-3" style={{ borderColor: "var(--stl-border)" }}>
        <div className="flex min-w-0 items-start gap-3">
          <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-[#dedbd2] bg-white/70 text-[#0f766e]">
            <IconComponent className="h-4 w-4" aria-hidden />
          </span>
          <div className="min-w-0">
            <div className="text-[11px] font-semibold uppercase tracking-wider text-[#66635f]">{eyebrow}</div>
            <h2 className="mt-0.5 text-sm font-semibold text-[#171d22]">{title}</h2>
          </div>
        </div>
        {action}
      </header>
      <div className="p-3">{children}</div>
    </section>
  );
}

function EmptyState({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-md border border-[#dedbd2] bg-white/60 px-4 py-6 text-sm text-[#66635f]">
      {children}
    </div>
  );
}

function AuditDetails({ data }: { data: DashboardData }) {
  return (
    <details className="rounded-lg border border-[#dedbd2] bg-white p-4 text-xs text-[#66635f]">
      <summary className="cursor-pointer text-sm font-semibold text-[#2d2b28]">Data Contract</summary>
      <div className="mt-3 grid gap-2 md:grid-cols-2">
        <div>
          <div className="font-semibold text-[#2d2b28]">Owner</div>
          <div>{data.dataContract?.owner ?? "Unknown"}</div>
        </div>
        <div>
          <div className="font-semibold text-[#2d2b28]">Generated</div>
          <div>{formatDateTime(data.generatedAt)}</div>
        </div>
        <div className="md:col-span-2">
          <div className="font-semibold text-[#2d2b28]">Tables</div>
          <div>{data.dataContract?.tables.join(" · ")}</div>
        </div>
        <div className="md:col-span-2">
          <div className="font-semibold text-[#2d2b28]">Boundary</div>
          <div>{data.dataContract?.note}</div>
        </div>
      </div>
    </details>
  );
}

function maxNumber(rows: DashboardRow[], key: string) {
  return Math.max(...rows.map((row) => numberValue(row[key]) ?? 0), 0);
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
  if (typeof value === "string" && value.trim().length > 0) return value;
  if (typeof value === "number") return String(value);
  if (
    value &&
    typeof value === "object" &&
    "value" in value &&
    typeof (value as { value?: unknown }).value === "string" &&
    (value as { value: string }).value.trim().length > 0
  ) {
    return (value as { value: string }).value;
  }
  return undefined;
}

function formatNumber(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US").format(Math.round(value));
}

function formatCurrency(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatDays(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "-";
  return `${Math.round(value)}d`;
}

function formatAge(minutes: number) {
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZoneName: "short",
  }).format(date);
}

function labelize(value: string): string {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function cleanPhone(value: string) {
  return value.replace(/[^+\d]/g, "");
}

function customerHref(contactSk: string) {
  return `/customers/${encodeURIComponent(contactSk)}?returnTo=${encodeURIComponent("/recovery")}`;
}
