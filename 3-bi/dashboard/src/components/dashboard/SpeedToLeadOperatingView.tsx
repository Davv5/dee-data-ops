import type { ComponentType } from "react";
import { AlertTriangle, PhoneCall, Timer, UserCheck } from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import type { DashboardData, DashboardFilters, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

const BOOKING_SLA_LABEL = "45m";

type MetricCard = {
  title: string;
  numerator: number | null;
  denominator: number | null;
  rate: number | null;
  note: string;
  tone: "green" | "blue" | "amber" | "red";
  icon: ComponentType<{ className?: string; "aria-hidden"?: boolean }>;
};

const sourcePillClass: Record<string, string> = {
  "GHL user": "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
  "Dialer number": "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  "Phone number": "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
  "Deleted GHL user ID": "border-[#fecdd3] bg-[#fff1f2] text-[#be123c]",
  Workflow: "border-[#ddd6fe] bg-[#f5f3ff] text-[#6d28d9]",
};

export function SpeedToLeadOperatingView({ data }: { data: DashboardData }) {
  const counts = data.rows.speed_to_lead_follow_up_counts ?? [];
  const allLeadEvents = metricRow(counts, "All lead events");
  const worked = metricRow(counts, "Leads worked");
  const reached = metricRow(counts, "Reached by phone");
  const notWorked = metricRow(counts, "Still not worked");
  const appointmentBooking = (data.rows.speed_to_lead_trigger_summary ?? []).find(
    (row) => row.trigger_type === "appointment_booking",
  );

  const totalEvents = numberValue(allLeadEvents?.lead_count);
  const workedEvents = numberValue(worked?.lead_count);
  const reachedEvents = numberValue(reached?.lead_count);
  const notWorkedEvents = numberValue(notWorked?.lead_count);
  const bookingSlaHits = numberValue(appointmentBooking?.within_sla);
  const bookingTriggers = numberValue(appointmentBooking?.total_triggers);

  const metrics: MetricCard[] = [
    {
      title: "Lead Events Worked",
      numerator: workedEvents,
      denominator: totalEvents,
      rate: numberValue(worked?.share_of_all_leads),
      note: "Any follow-up after the trigger.",
      tone: "green",
      icon: UserCheck,
    },
    {
      title: "Reached By Phone",
      numerator: reachedEvents,
      denominator: totalEvents,
      rate: numberValue(reached?.share_of_all_leads),
      note: `${formatPercent(numberValue(reached?.share_of_worked_leads))} of worked lead events.`,
      tone: "blue",
      icon: PhoneCall,
    },
    {
      title: "Still Not Worked",
      numerator: notWorkedEvents,
      denominator: totalEvents,
      rate: numberValue(notWorked?.share_of_all_leads),
      note: "No post-trigger follow-up found.",
      tone: "red",
      icon: AlertTriangle,
    },
    {
      title: `Bookings Within ${BOOKING_SLA_LABEL}`,
      numerator: bookingSlaHits,
      denominator: bookingTriggers,
      rate: safeDivide(bookingSlaHits, bookingTriggers),
      note: `Appointment bookings reached within ${BOOKING_SLA_LABEL}.`,
      tone: "amber",
      icon: Timer,
    },
  ];

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">bq-ingest-report</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            Speed-to-Lead
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            Follow-up coverage, phone reach, SLA, and reached-by identity from live BigQuery.
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
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {metrics.map((metric) => (
            <MetricCard key={metric.title} metric={metric} />
          ))}
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.45fr)_minmax(22rem,0.85fr)]">
          <ReachedByPanel
            rows={data.rows.speed_to_lead_phone_reach_by_rep ?? []}
            confidenceRows={data.rows.speed_to_lead_attribution_confidence ?? []}
          />
          <LeakPanel
            totalEvents={totalEvents}
            notWorkedEvents={notWorkedEvents}
            triggerRows={data.rows.speed_to_lead_trigger_summary ?? []}
            agingRows={data.rows.speed_to_lead_not_worked_aging ?? []}
          />
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
              href={`/speed-to-lead?range=${option.value}`}
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

function MetricCard({ metric }: { metric: MetricCard }) {
  const Icon = metric.icon;
  const toneClass = {
    green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
    blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
    amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
    red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  }[metric.tone];

  return (
    <article className="rounded-lg border border-[#dedbd2] bg-white p-3 shadow-sm">
      <div className="flex items-start justify-between gap-3">
        <p className="text-xs font-semibold uppercase text-[#66635f]">{metric.title}</p>
        <span className={`rounded-md border p-1.5 ${toneClass}`}>
          <Icon className="h-4 w-4" aria-hidden />
        </span>
      </div>
      <div className="mt-3 flex items-baseline gap-2">
        <span className="text-2xl font-semibold tracking-normal">
          {formatNumber(metric.numerator)}
        </span>
        <span className="text-sm text-[#66635f]">/ {formatNumber(metric.denominator)}</span>
      </div>
      <div className="mt-2 flex items-center justify-between gap-3">
        <span className="text-lg font-semibold tracking-normal">{formatPercent(metric.rate)}</span>
        <span className="truncate text-xs text-[#66635f]">{metric.note}</span>
      </div>
    </article>
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

function LeakPanel({
  totalEvents,
  notWorkedEvents,
  triggerRows,
  agingRows,
}: {
  totalEvents: number | null;
  notWorkedEvents: number | null;
  triggerRows: DashboardRow[];
  agingRows: DashboardRow[];
}) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <h2 className="text-sm font-semibold">Leak Snapshot</h2>
      <div className="mt-3 rounded-md border border-[#fecaca] bg-[#fef2f2] p-3">
        <div className="flex items-baseline justify-between gap-3">
          <span className="text-sm font-medium text-[#991b1b]">Not worked</span>
          <span className="text-xl font-semibold text-[#991b1b]">
            {formatNumber(notWorkedEvents)}
          </span>
        </div>
        <div className="mt-1 text-xs text-[#7f1d1d]">
          {formatPercent(safeDivide(notWorkedEvents, totalEvents))} of all lead events
        </div>
      </div>

      {agingRows.length > 0 ? (
        <div className="mt-3 grid grid-cols-3 gap-2">
          {agingRows.map((row) => (
            <div key={stringValue(row.age_bucket) ?? "age"} className="rounded-md border border-[#ece9e1] p-2">
              <div className="truncate text-[11px] font-semibold uppercase text-[#66635f]">
                {stringValue(row.age_bucket)}
              </div>
              <div className="mt-1 text-base font-semibold tracking-normal">
                {formatNumber(numberValue(row.lead_events))}
              </div>
              <div className="text-[11px] text-[#66635f]">
                {formatPercent(numberValue(row.share_of_not_worked))}
              </div>
            </div>
          ))}
        </div>
      ) : null}

      <div className="mt-4 space-y-3">
        {triggerRows.map((row) => {
          const total = numberValue(row.total_triggers);
          const touched = numberValue(row.touched);
          const unworked = total !== null && touched !== null ? total - touched : null;
          const rate = safeDivide(unworked, total);
          return (
            <div key={stringValue(row.trigger_type) ?? "trigger-type"} className="text-sm">
              <div className="flex justify-between gap-3">
                <span className="truncate font-medium text-[#2d2b28]">{formatTriggerType(row.trigger_type)}</span>
                <span className="text-[#66635f]">{formatNumber(unworked)} not worked</span>
              </div>
              <div className="mt-1 h-2 rounded-sm bg-[#ece9e1]">
                <div
                  className="h-2 rounded-sm bg-[#991b1b]"
                  style={{ width: `${Math.max(2, (rate ?? 0) * 100)}%` }}
                />
              </div>
              <div className="mt-1 text-xs text-[#66635f]">{formatPercent(rate)} leak rate</div>
            </div>
          );
        })}
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
          filters={data.filters}
          customerReason="reached_by_phone"
          rows={(data.rows.speed_to_lead_reached_examples ?? []).slice(0, 8)}
          columns={[
            { key: "reached_at_et", label: "Reached At" },
            { key: "lead_name", label: "Lead", linkToCustomer: true },
            { key: "source_label", label: "Source" },
            { key: "reached_by", label: "Reached By" },
            { key: "identity_source", label: "Source" },
          ]}
        />
        <TablePanel
          title="Unworked examples"
          filters={data.filters}
          customerReason="not_worked"
          rows={(data.rows.speed_to_lead_no_touch_examples ?? []).slice(0, 8)}
          columns={[
            { key: "trigger_date", label: "Date" },
            { key: "lead_name", label: "Lead", linkToCustomer: true },
            { key: "trigger_type", label: "Type" },
            { key: "source_label", label: "Source" },
            { key: "age_hours", label: "Age Hrs", format: "number" },
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
  filters,
  customerReason,
}: {
  title: string;
  rows: DashboardRow[];
  columns: Array<{ key: string; label: string; format?: "number" | "percent" | "duration"; linkToCustomer?: boolean }>;
  filters?: DashboardFilters;
  customerReason?: string;
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
                    {column.linkToCustomer && filters && stringValue(row.contact_sk) ? (
                      <a
                        href={customerHref(row, filters, customerReason)}
                        className="block truncate font-semibold text-[#0f766e] hover:text-[#115e59]"
                      >
                        {formatValue(row[column.key], column.format)}
                      </a>
                    ) : (
                      <span className="block truncate">
                        {formatValue(row[column.key], column.format)}
                      </span>
                    )}
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

function customerHref(row: DashboardRow, filters: DashboardFilters, reason?: string) {
  const contactSk = stringValue(row.contact_sk);
  if (!contactSk) return `/speed-to-lead?range=${filters.timeRange}`;

  const params = new URLSearchParams({
    from: "speed-to-lead",
    range: filters.timeRange,
  });

  if (reason) params.set("reason", reason);

  return `/customers/${contactSk}?${params.toString()}`;
}

function metricRow(rows: DashboardRow[], metric: string) {
  return rows.find((row) => row.metric === metric);
}

function pillClass(source: string) {
  return sourcePillClass[source] ?? "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]";
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

function formatTriggerType(value: DashboardRowValue | undefined) {
  const label = stringValue(value);
  if (!label) return "N/A";
  return label
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
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
