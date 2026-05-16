import { LeadMagnetsDetailView } from "@/components/dashboard/LeadMagnetsDetailView";
import { AppShell } from "@/components/layout/AppShell";
import { getLeadMagnetDetailData } from "@/lib/bigquery/lead-magnets-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type LeadMagnetDetailPageProps = {
  params: Promise<{ magnetId: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function LeadMagnetDetailPage({
  params,
  searchParams,
}: LeadMagnetDetailPageProps) {
  const { magnetId } = await params;
  const sp = await searchParams;
  const rawRange = Array.isArray(sp?.range) ? sp?.range[0] : sp?.range;
  const range = rawRange ?? "all";

  const data = await getLeadMagnetDetailData({
    magnetId: decodeURIComponent(magnetId),
    timeRange: range,
  });

  return (
    <AppShell>
      <LeadMagnetsDetailView data={data} />
    </AppShell>
  );
}
