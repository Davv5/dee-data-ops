import { AlertTriangle, ArrowUpRight, ListChecks, Mail, PhoneCall, ShieldCheck } from "lucide-react";
import Link from "next/link";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import type { DashboardData, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";

type ActionQueueOperatingViewProps = {
  data: DashboardData;
};

const priorityTone = {
  high: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
  medium: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
  low: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
};

export function ActionQueueOperatingView({ data }: ActionQueueOperatingViewProps) {
  const summary = data.rows.customer_action_summary?.[0];
  const byArea = data.rows.customer_action_by_area ?? [];
  const byBucket = data.rows.customer_action_by_bucket ?? [];
  const queue = data.rows.customer_action_queue ?? [];
  const activeArea = data.filters.worklist ?? "all";

  return (
    <div>
      <header className="flex flex-col gap-3 border-b border-[#dedbd2] pb-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">agent-ops</p>
          <h1 className="mt-1 text-2xl font-semibold tracking-normal md:text-3xl">
            Action Queue
          </h1>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-[#66635f]">
            Open revenue, retention, and contract-term work with customer context and money attached.
          </p>
        </div>
        <FreshnessBadge freshness={data.freshness} />
      </header>

      {data.error ? (
        <div className="mt-4 rounded-lg border border-[#fecaca] bg-[#fef2f2] p-4 text-sm text-[#991b1b]">
          {data.error}
        </div>
      ) : null}

      <section className="py-4">
        <AreaControl activeArea={activeArea} />

        <div className="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-5">
          <MetricCard
            label="Open Actions"
            value={formatNumber(numberValue(summary?.open_actions))}
            helper={`${formatNumber(numberValue(summary?.open_customers))} customers`}
            tone="blue"
          />
          <MetricCard
            label="Money at Stake"
            value={formatCurrency(numberValue(summary?.open_money_at_stake))}
            helper="Open queue value"
            tone="green"
          />
          <MetricCard
            label="High Priority"
            value={formatNumber(numberValue(summary?.high_priority_open_actions))}
            helper="Needs attention first"
            tone="red"
          />
          <MetricCard
            label="Contactable"
            value={formatNumber(numberValue(summary?.contactable_open_actions))}
            helper="Call, text, or email route"
            tone="amber"
          />
          <MetricCard
            label="Closed"
            value={formatNumber(numberValue(summary?.closed_actions))}
            helper={`${formatNumber(numberValue(summary?.actions))} total actions`}
            tone="neutral"
          />
        </div>

        <div className="mt-3 grid gap-3 xl:grid-cols-[minmax(0,1.35fr)_minmax(22rem,0.65fr)]">
          <QueuePanel rows={queue} activeArea={activeArea} />
          <div className="grid gap-3 content-start">
            <AreaPressurePanel rows={byArea} />
            <BucketPressurePanel rows={byBucket} />
          </div>
        </div>
      </section>
    </div>
  );
}

function AreaControl({ activeArea }: { activeArea: string }) {
  const options = [
    { value: "all", label: "All" },
    { value: "revenue", label: "Revenue" },
    { value: "retention", label: "Retention" },
    { value: "contract_terms", label: "Contract Terms" },
  ];

  return (
    <div className="flex flex-wrap gap-2">
      {options.map((option) => {
        const isActive = option.value === activeArea;
        const href = option.value === "all" ? "/actions" : `/actions?area=${option.value}`;

        return (
          <Link
            key={option.value}
            href={href}
            aria-current={isActive ? "page" : undefined}
            className={`rounded-md border px-3 py-2 text-xs font-semibold transition ${
              isActive
                ? "border-[#0f766e] bg-[#0f766e] text-white"
                : "border-[#dedbd2] bg-white text-[#3b3936] hover:bg-[#f3f1ea]"
            }`}
          >
            {option.label}
          </Link>
        );
      })}
    </div>
  );
}

function QueuePanel({ rows, activeArea }: { rows: DashboardRow[]; activeArea: string }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white shadow-sm">
      <div className="flex items-center justify-between gap-3 border-b border-[#ece9e1] px-4 py-3">
        <div className="flex items-center gap-2">
          <span className="rounded-md border border-[#bfdbfe] bg-[#eff6ff] p-2 text-[#1d4ed8]">
            <ListChecks className="h-4 w-4" aria-hidden />
          </span>
          <h2 className="text-sm font-semibold">Next Actions</h2>
        </div>
        <span className="rounded-md border border-[#dedbd2] px-2 py-1 text-[11px] font-semibold uppercase text-[#66635f]">
          {activeArea.replace(/_/g, " ")}
        </span>
      </div>

      <div className="divide-y divide-[#ece9e1]">
        {rows.map((row) => {
          const customerName = stringValue(row.customer_display_name) ?? "Unknown customer";
          const contactSk = stringValue(row.contact_sk);
          const customerHref = contactSk
            ? `/customers/${encodeURIComponent(contactSk)}?from=actions&area=${encodeURIComponent(activeArea)}`
            : "/actions";

          return (
            <Link
              key={stringValue(row.customer_action_id) ?? `${customerName}-${stringValue(row.action_bucket)}`}
              href={customerHref}
              className="block px-4 py-3 transition hover:bg-[#fbfaf7]"
            >
              <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded-md border px-2 py-1 text-[11px] font-semibold uppercase ${priorityClass(stringValue(row.priority_label))}`}>
                      {stringValue(row.priority_label) ?? "priority"}
                    </span>
                    <span className="rounded-md border border-[#dedbd2] px-2 py-1 text-[11px] font-semibold uppercase text-[#66635f]">
                      {stringValue(row.action_area)?.replace(/_/g, " ") ?? "action"}
                    </span>
                    <span className="text-xs font-medium text-[#66635f]">
                      {stringValue(row.recommended_channel_label)}
                    </span>
                  </div>
                  <div className="mt-2 truncate text-base font-semibold text-[#171717]">
                    {customerName}
                  </div>
                  <div className="mt-1 text-sm font-semibold text-[#2d2b28]">
                    {stringValue(row.action_label)}
                  </div>
                  <p className="mt-1 line-clamp-2 text-xs leading-5 text-[#66635f]">
                    {stringValue(row.action_reason)}
                  </p>
                </div>

                <div className="grid gap-2 text-sm sm:grid-cols-3 lg:min-w-[26rem]">
                  <MiniStat label="stake" value={formatCurrency(numberValue(row.money_at_stake))} />
                  <MiniStat label="revenue credit" value={stringValue(row.revenue_credit_name) ?? "Unknown"} />
                  <MiniStat label="owner" value={stringValue(row.current_owner_name) ?? "Unknown"} />
                </div>
              </div>

              <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-[#66635f]">
                <ContactPill row={row} />
                <span className="truncate rounded-md border border-[#ece9e1] bg-[#fbfaf7] px-2 py-1">
                  {stringValue(row.top_product_by_net_revenue) ?? "Product unknown"}
                </span>
                <span className="inline-flex items-center gap-1 rounded-md border border-[#ece9e1] bg-[#fbfaf7] px-2 py-1">
                  Open <ArrowUpRight className="h-3 w-3" aria-hidden />
                </span>
              </div>
            </Link>
          );
        })}

        {rows.length === 0 ? (
          <div className="px-4 py-8 text-sm text-[#66635f]">
            No open actions for this view.
          </div>
        ) : null}
      </div>
    </section>
  );
}

function AreaPressurePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-center gap-2">
        <ShieldCheck className="h-4 w-4 text-[#0f766e]" aria-hidden />
        <h2 className="text-sm font-semibold">Area Pressure</h2>
      </div>
      <div className="mt-3 space-y-2">
        {rows.map((row) => (
          <PressureRow
            key={`${stringValue(row.action_area)}-${stringValue(row.priority_label)}`}
            label={`${stringValue(row.action_area)?.replace(/_/g, " ") ?? "area"} · ${stringValue(row.priority_label) ?? "priority"}`}
            count={numberValue(row.open_actions)}
            amount={numberValue(row.open_money_at_stake)}
          />
        ))}
      </div>
    </section>
  );
}

function BucketPressurePanel({ rows }: { rows: DashboardRow[] }) {
  return (
    <section className="rounded-lg border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex items-center gap-2">
        <AlertTriangle className="h-4 w-4 text-[#92400e]" aria-hidden />
        <h2 className="text-sm font-semibold">Largest Buckets</h2>
      </div>
      <div className="mt-3 space-y-2">
        {rows.slice(0, 8).map((row) => (
          <PressureRow
            key={`${stringValue(row.action_area)}-${stringValue(row.action_bucket)}`}
            label={stringValue(row.action_label) ?? "Action bucket"}
            count={numberValue(row.open_actions)}
            amount={numberValue(row.open_money_at_stake)}
          />
        ))}
      </div>
    </section>
  );
}

function MetricCard({
  label,
  value,
  helper,
  tone,
}: {
  label: string;
  value: string;
  helper: string;
  tone: "green" | "blue" | "amber" | "red" | "neutral";
}) {
  const classes = {
    green: "border-[#bbf7d0] bg-[#f0fdf4] text-[#166534]",
    blue: "border-[#bfdbfe] bg-[#eff6ff] text-[#1d4ed8]",
    amber: "border-[#fde68a] bg-[#fffbeb] text-[#92400e]",
    red: "border-[#fecaca] bg-[#fef2f2] text-[#991b1b]",
    neutral: "border-[#dedbd2] bg-white text-[#3b3936]",
  };

  return (
    <div className={`rounded-lg border p-4 shadow-sm ${classes[tone]}`}>
      <div className="text-xs font-semibold uppercase">{label}</div>
      <div className="mt-3 text-2xl font-semibold text-[#171717]">{value}</div>
      <div className="mt-1 text-xs leading-5">{helper}</div>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-2">
      <div className="text-[11px] font-semibold uppercase text-[#66635f]">{label}</div>
      <div className="mt-1 truncate font-semibold text-[#171717]">{value}</div>
    </div>
  );
}

function PressureRow({ label, count, amount }: { label: string; count: number | null; amount: number | null }) {
  return (
    <div className="rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
      <div className="flex items-center justify-between gap-3">
        <div className="min-w-0 truncate text-sm font-semibold capitalize text-[#2d2b28]">{label}</div>
        <div className="text-sm font-semibold text-[#171717]">{formatNumber(count)}</div>
      </div>
      <div className="mt-1 text-xs text-[#66635f]">{formatCurrency(amount)}</div>
    </div>
  );
}

function ContactPill({ row }: { row: DashboardRow }) {
  const hasPhone = booleanValue(row.has_phone);
  const hasEmail = booleanValue(row.has_email);

  if (hasPhone) {
    return (
      <span className="inline-flex items-center gap-1 rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-[#166534]">
        <PhoneCall className="h-3 w-3" aria-hidden />
        Phone
      </span>
    );
  }

  if (hasEmail) {
    return (
      <span className="inline-flex items-center gap-1 rounded-md border border-[#bfdbfe] bg-[#eff6ff] px-2 py-1 text-[#1d4ed8]">
        <Mail className="h-3 w-3" aria-hidden />
        Email
      </span>
    );
  }

  return (
    <span className="rounded-md border border-[#fde68a] bg-[#fffbeb] px-2 py-1 text-[#92400e]">
      Missing route
    </span>
  );
}

function priorityClass(priority: string | null) {
  if (priority === "high" || priority === "medium" || priority === "low") {
    return priorityTone[priority];
  }

  return "border-[#dedbd2] bg-[#f7f7f4] text-[#3b3936]";
}

function formatCurrency(value: number | null) {
  if (value === null) return "$0";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: value >= 1000 ? 0 : 2,
  }).format(value);
}

function formatNumber(value: number | null) {
  if (value === null) return "0";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
}

function numberValue(value: DashboardRowValue | undefined) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  return null;
}

function stringValue(value: DashboardRowValue | undefined) {
  if (typeof value === "string" && value.trim() !== "") return value;
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return value ? "true" : "false";
  return null;
}

function booleanValue(value: DashboardRowValue | undefined) {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value === "true";
  if (typeof value === "number") return value > 0;
  return false;
}
