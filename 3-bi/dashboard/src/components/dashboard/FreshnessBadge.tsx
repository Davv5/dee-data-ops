import type { DashboardFreshness } from "@/types/dashboard-data";

type FreshnessBadgeProps = {
  freshness: DashboardFreshness;
};

export function FreshnessBadge({ freshness }: FreshnessBadgeProps) {
  const tone =
    freshness.status === "live"
      ? "border-[#99d4cb] bg-[#f0fdfa] text-[#115e59]"
      : freshness.status === "stale"
        ? "border-[#facc15] bg-[#fefce8] text-[#854d0e]"
        : "border-[#f5b7b1] bg-[#fef2f2] text-[#991b1b]";

  return (
    <div className={`max-w-xs rounded-md border px-3 py-2 text-xs ${tone}`}>
      <div className="font-semibold">{freshness.label}</div>
      <div className="mt-1 leading-5">{freshness.detail}</div>
    </div>
  );
}
