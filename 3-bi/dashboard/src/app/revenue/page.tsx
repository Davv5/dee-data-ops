import { RevenueFunnelOperatingView } from "@/components/dashboard/RevenueFunnelOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getRevenueFunnelData } from "@/lib/bigquery/revenue-funnel-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type RevenuePageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function RevenuePage({ searchParams }: RevenuePageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const data = await getRevenueFunnelData({ timeRange: range });

  return (
    <AppShell>
      <RevenueFunnelOperatingView data={data} />
    </AppShell>
  );
}
