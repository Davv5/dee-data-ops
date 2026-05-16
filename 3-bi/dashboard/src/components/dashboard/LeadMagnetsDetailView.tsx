import Link from "next/link";

import {
  formatCurrencyCompact,
  formatCurrencyFull,
  formatNumber,
  numberValue,
  queueStatusLabel,
  queueStatusTone,
  stringValue,
} from "@/lib/config/lead-magnet-tokens";
import type { LeadMagnetDetailData } from "@/lib/bigquery/lead-magnets-live";
import type { DashboardRow } from "@/types/dashboard-data";

/**
 * Per-magnet drill page — Spec v1 §6.
 *
 * Four panels: MagnetHero, MagnetFunnelStrip, TimeToBuyerHistogram,
 * MagnetBuyerTable (the smoking gun). Same anatomy as SpeedToLeadRepDetail.
 */
export function LeadMagnetsDetailView({ data }: { data: LeadMagnetDetailData }) {
  const summary = (data.rows.magnet_detail_summary ?? [])[0];
  const buyers = data.rows.magnet_detail_buyers ?? [];
  const firstTouch = (data.rows.magnet_detail_first_touch_breakdown ?? [])[0];

  const magnetName = stringValue(summary?.lead_magnet_reporting_name);
  const headline = magnetName ?? data.magnetId;

  return (
    <div className="stl-page stl-od-page lm-page lm-detail-page space-y-5 pb-12">
      <div className="od-page-head">
        <div className="od-title">
          <Link className="lm-back-link" href="/lead-magnets">
            ← Lead Magnets
          </Link>
          <h1>{headline}</h1>
          <p className="od-blurb">last-touch attribution · per-magnet detail</p>
        </div>
      </div>

      {data.error ? (
        <div className="od-callout od-callout-danger">{data.error}</div>
      ) : null}

      {summary ? (
        <>
          <MagnetHero summary={summary} rangeLabel={data.filters.timeRangeLabel} />
          <BuyerEconomicsCard summary={summary} />
          <MagnetFunnelStrip summary={summary} />
          <AttributionRoleCard firstTouch={firstTouch} />
          <TimeToBuyerHistogram buyers={buyers} />
        </>
      ) : (
        <div className="od-empty-state">No data for this magnet in the selected window.</div>
      )}

      <MagnetBuyerTable buyers={buyers} />
    </div>
  );
}

// ------------------------------------------------------------------
// BuyerEconomicsCard — per-magnet buyer-quality breakdown.
// 4 stats in a small grid: Avg buyer value, First purchase $, Repeat rate,
// Refund rate. Sits between MagnetHero and MagnetFunnelStrip.
// Reference spec §6.6.
// ------------------------------------------------------------------

function BuyerEconomicsCard({ summary }: { summary: DashboardRow }) {
  const buyers = numberValue(summary.buyer_count) ?? 0;
  const attributedRevenue = numberValue(summary.attributed_net_revenue) ?? 0;
  const medianFirstPurchase = numberValue(summary.median_first_purchase_revenue);
  const repeatRate = numberValue(summary.repeat_buyer_rate);
  const gross = numberValue(summary.gross_revenue) ?? 0;
  const lifetimeNet = numberValue(summary.lifetime_net_revenue) ?? 0;

  const avgBuyer = buyers > 0 ? attributedRevenue / buyers : null;
  const refundRate = gross > 0 ? Math.max(0, (gross - lifetimeNet) / gross) : null;

  return (
    <section className="lm-buyer-econ-card">
      <header className="lm-buyer-econ-head">
        <span className="lm-buyer-econ-title">Buyer economics</span>
        <span className="lm-buyer-econ-meta">lifetime</span>
      </header>
      <div className="lm-buyer-econ-grid">
        <div className="lm-buyer-econ-cell">
          <div className="lm-buyer-econ-value">
            {avgBuyer != null ? formatCurrencyFull(avgBuyer) : "—"}
          </div>
          <div className="lm-buyer-econ-label">Avg buyer value</div>
          <div className="lm-buyer-econ-hint">net rev ÷ buyers</div>
        </div>
        <div className="lm-buyer-econ-cell">
          <div className="lm-buyer-econ-value">
            {medianFirstPurchase != null ? formatCurrencyFull(medianFirstPurchase) : "—"}
          </div>
          <div className="lm-buyer-econ-label">First purchase $</div>
          <div className="lm-buyer-econ-hint">median entry ticket</div>
        </div>
        <div className="lm-buyer-econ-cell">
          <div className="lm-buyer-econ-value">
            {repeatRate != null ? formatSharePercent(repeatRate) : "—"}
          </div>
          <div className="lm-buyer-econ-label">Repeat-buyer rate</div>
          <div className="lm-buyer-econ-hint">buyers with 2+ payments</div>
        </div>
        <div className="lm-buyer-econ-cell">
          <div className="lm-buyer-econ-value">
            {refundRate != null ? formatSharePercent(refundRate) : "—"}
          </div>
          <div className="lm-buyer-econ-label">Refund rate</div>
          <div className="lm-buyer-econ-hint">gross − net delta</div>
        </div>
      </div>
    </section>
  );
}

// ------------------------------------------------------------------
// MagnetHero — one-line revenue + buyers + median days.
// ------------------------------------------------------------------

function MagnetHero({
  summary,
  rangeLabel,
}: {
  summary: DashboardRow;
  rangeLabel: string;
}) {
  const revenue = numberValue(summary.attributed_net_revenue);
  const buyerCount = numberValue(summary.buyer_count);
  const medianDays = numberValue(summary.median_days_opt_in_to_purchase);
  const status = stringValue(summary.queue_status);
  const verdictTone = status ? queueStatusTone(status) : "neutral";
  const verdictLabel = status ? queueStatusLabel(status) : "Status unavailable";

  return (
    <div className="od-kpi hero-sla">
      <span className="label">Magnet revenue</span>
      <div className="hero-row">
        <div className="value">{formatCurrencyCompact(revenue)}</div>
      </div>
      <div className="hero-sub">
        {buyerCount != null && buyerCount > 0
          ? `${formatNumber(buyerCount)} buyers${
              medianDays != null ? ` · ${Math.round(medianDays)} days median opt-in to purchase` : ""
            }`
          : "no attributed buyers in this period"}
      </div>
      <div className="hero-row hero-footer">
        <span className="hero-target">{rangeLabel.toLowerCase()}</span>
        <span className={`hero-verdict tone-${verdictTone}`}>{verdictLabel}</span>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// MagnetFunnelStrip — opt-ins → booked → bought, with conversion % to
// opt-in denominator (not stage-to-stage, to avoid lying about quality).
// ------------------------------------------------------------------

function MagnetFunnelStrip({ summary }: { summary: DashboardRow }) {
  const optIns = numberValue(summary.opt_in_lead_count) ?? 0;
  const booked = numberValue(summary.booking_lead_count) ?? 0;
  const bought = numberValue(summary.purchase_lead_count) ?? 0;

  const bookedShare = optIns > 0 ? booked / optIns : null;
  const boughtShare = optIns > 0 ? bought / optIns : null;

  return (
    <section className="lm-funnel-strip">
      <FunnelStage label="Opt-ins" value={optIns} />
      <FunnelArrow />
      <FunnelStage label="Booked" value={booked} share={bookedShare} />
      <FunnelArrow />
      <FunnelStage label="Bought" value={bought} share={boughtShare} />
    </section>
  );
}

function FunnelStage({
  label,
  value,
  share,
}: {
  label: string;
  value: number;
  share?: number | null;
}) {
  return (
    <div className="lm-funnel-stage">
      <div className="lm-funnel-value">{formatNumber(value)}</div>
      <div className="lm-funnel-label">{label}</div>
      {share != null ? (
        <div className="lm-funnel-share">{formatSharePercent(share)}</div>
      ) : null}
    </div>
  );
}

function FunnelArrow() {
  return <div className="lm-funnel-arrow" aria-hidden>→</div>;
}

function formatSharePercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—";
  const pct = value * 100;
  if (pct >= 10) return `${Math.round(pct)}%`;
  return `${pct.toFixed(1)}%`;
}

// ------------------------------------------------------------------
// AttributionRoleCard — classifies this magnet as "introducer + closer",
// "balanced", or "mostly a closer" by comparing first-touch vs last-touch
// attribution among the buyers credited to it by last touch.
//
// Reuses .od-kpi.hero-sla shell for visual parity with the page hero.
// Reference spec §6.5.
// ------------------------------------------------------------------

function AttributionRoleCard({ firstTouch }: { firstTouch: DashboardRow | undefined }) {
  const lastTouchBuyers = numberValue(firstTouch?.last_touch_buyers) ?? 0;
  const firstAndLast = numberValue(firstTouch?.first_and_last_buyers) ?? 0;
  const lastOnly = numberValue(firstTouch?.last_only_buyers) ?? 0;
  const missingFirstTouch = numberValue(firstTouch?.missing_first_touch_buyers) ?? 0;
  const sampleOther = stringValue(firstTouch?.sample_other_magnet);

  if (lastTouchBuyers === 0) {
    return null;
  }

  // Only consider buyers whose first-touch is known when computing the role.
  const knownFirstTouch = firstAndLast + lastOnly;
  const introducerShare = knownFirstTouch > 0 ? firstAndLast / knownFirstTouch : null;

  let verdictLabel = "Role unavailable";
  let verdictTone: "ok" | "neutral" | "warning" | "danger" = "neutral";
  if (introducerShare != null) {
    if (introducerShare >= 0.6) {
      verdictLabel = "Introducer + closer";
      verdictTone = "ok";
    } else if (introducerShare >= 0.3) {
      verdictLabel = "Balanced role";
      verdictTone = "neutral";
    } else {
      verdictLabel = "Mostly a closer";
      verdictTone = "warning";
    }
  }

  return (
    <div className="od-kpi hero-sla lm-role-card">
      <span className="label">Role</span>
      <div className="hero-row">
        <div className="value">{formatNumber(lastTouchBuyers)}</div>
      </div>
      <div className="hero-sub">
        buyers credited to this magnet by last touch
      </div>
      <ul className="lm-role-breakdown">
        <li>
          <strong>{formatNumber(firstAndLast)}</strong> first encountered us through this magnet
          <span className="lm-role-detail">introducer + closer</span>
        </li>
        <li>
          <strong>{formatNumber(lastOnly)}</strong> first encountered us through a different magnet
          {sampleOther ? (
            <span className="lm-role-detail">most common: {sampleOther}</span>
          ) : (
            <span className="lm-role-detail">closer only</span>
          )}
        </li>
        {missingFirstTouch > 0 ? (
          <li>
            <strong>{formatNumber(missingFirstTouch)}</strong> first-touch unknown
            <span className="lm-role-detail">attribution gap</span>
          </li>
        ) : null}
      </ul>
      <div className="hero-row hero-footer">
        <span className="hero-target">
          {introducerShare != null ? `${formatSharePercent(introducerShare)} introducer share` : "no data"}
        </span>
        <span className={`hero-verdict tone-${verdictTone}`}>{verdictLabel}</span>
      </div>
    </div>
  );
}

// ------------------------------------------------------------------
// TimeToBuyerHistogram — 5 buckets derived client-side from buyer rows.
// Buckets: same-day (0), 1–7d, 8–30d, 31–90d, 90+d.
// ------------------------------------------------------------------

type Bucket = { label: string; predicate: (d: number) => boolean };

const TIME_BUCKETS: Bucket[] = [
  { label: "Same-day", predicate: (d) => d === 0 },
  { label: "1–7 days", predicate: (d) => d >= 1 && d <= 7 },
  { label: "8–30 days", predicate: (d) => d >= 8 && d <= 30 },
  { label: "31–90 days", predicate: (d) => d >= 31 && d <= 90 },
  { label: "90+ days", predicate: (d) => d > 90 },
];

function TimeToBuyerHistogram({ buyers }: { buyers: DashboardRow[] }) {
  const lags = buyers
    .map((row) => numberValue(row.days_opt_in_to_purchase))
    .filter((v): v is number => v != null && Number.isFinite(v));

  if (lags.length === 0) {
    return (
      <section className="lm-histogram lm-histogram-empty">
        <h2>Time from opt-in to purchase</h2>
        <p>No buyers with an opt-in lag in this window.</p>
      </section>
    );
  }

  const counts = TIME_BUCKETS.map((bucket) => ({
    label: bucket.label,
    count: lags.filter(bucket.predicate).length,
  }));
  const max = Math.max(...counts.map((c) => c.count), 1);

  return (
    <section className="lm-histogram">
      <header className="lm-leaderboard-head">
        <h2>Time from opt-in to purchase</h2>
        <p className="od-blurb">Distribution of lag in days, across this magnet&apos;s buyers.</p>
      </header>
      <div className="lm-histogram-rows">
        {counts.map(({ label, count }) => {
          const widthPct = (count / max) * 100;
          return (
            <div key={label} className="lm-histogram-row">
              <span className="lm-histogram-label">{label}</span>
              <span className="lm-histogram-bar-wrap">
                <span
                  className="lm-histogram-bar"
                  style={{ width: `${Math.max(2, widthPct)}%` }}
                />
              </span>
              <span className="lm-histogram-count">{formatNumber(count)}</span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

// ------------------------------------------------------------------
// MagnetBuyerTable — the smoking gun. Up to BUYER_TABLE_MAX_ROWS named
// buyers attributed to this magnet. "+ N more" footer when truncated.
// ------------------------------------------------------------------

const BUYER_TABLE_DISPLAY_ROWS = 100;

function MagnetBuyerTable({ buyers }: { buyers: DashboardRow[] }) {
  const shown = buyers.slice(0, BUYER_TABLE_DISPLAY_ROWS);
  const extra = Math.max(0, buyers.length - BUYER_TABLE_DISPLAY_ROWS);

  if (shown.length === 0) {
    return (
      <section className="lm-leaderboard lm-leaderboard-empty">
        <p>No buyers attributed to this magnet in the selected window.</p>
      </section>
    );
  }

  return (
    <section className="lm-leaderboard">
      <header className="lm-leaderboard-head">
        <h2>Buyers attributed to this magnet</h2>
        <p className="od-blurb">
          Sorted by most recent purchase. {extra > 0 ? `Showing first ${BUYER_TABLE_DISPLAY_ROWS} of ${buyers.length}.` : null}
        </p>
      </header>
      <table className="lm-leaderboard-table lm-buyer-table">
        <thead>
          <tr>
            <th className="col-name" scope="col">Buyer</th>
            <th scope="col">First purchase</th>
            <th className="col-num" scope="col">Lag</th>
            <th className="col-num" scope="col">First $</th>
            <th className="col-num" scope="col">Total LTV</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((row, idx) => {
            const key = stringValue(row.contact_sk) ?? stringValue(row.contact_id) ?? String(idx);
            const name = stringValue(row.buyer_name) ?? "Unknown buyer";
            const firstPurchase = formatPurchaseDate(stringValue(row.first_purchase_at));
            const lagDays = numberValue(row.days_opt_in_to_purchase);
            const firstRevenue = numberValue(row.first_purchase_net_revenue);
            const lifetime = numberValue(row.lifetime_net_revenue);
            return (
              <tr key={key} className="lm-leader-row">
                <td className="col-name">
                  <span className="lm-leader-name">{name}</span>
                </td>
                <td>{firstPurchase}</td>
                <td className="col-num">{lagDays != null ? `${Math.round(lagDays)} d` : "—"}</td>
                <td className="col-num">{formatCurrencyFull(firstRevenue)}</td>
                <td className="col-num">{formatCurrencyFull(lifetime)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {extra > 0 ? (
        <p className="lm-buyer-more">+ {formatNumber(extra)} more buyers not shown.</p>
      ) : null}
    </section>
  );
}

function formatPurchaseDate(raw: string | undefined): string {
  if (!raw) return "—";
  try {
    const date = new Date(raw);
    if (Number.isNaN(date.getTime())) return "—";
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    return `${year}-${month}-${day}`;
  } catch {
    return "—";
  }
}
