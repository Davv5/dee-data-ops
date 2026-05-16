import { SpeedToLeadRepDetail } from "@/components/dashboard/SpeedToLeadRepDetail";
import { AppShell } from "@/components/layout/AppShell";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type RepPageProps = {
  params: Promise<{ repName: string }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function SpeedToLeadRepPage({ params, searchParams }: RepPageProps) {
  const { repName: repNameRaw } = await params;
  const repName = decodeURIComponent(repNameRaw);

  const sp = await searchParams;
  const rawRange = Array.isArray(sp?.range) ? sp?.range[0] : sp?.range;
  const range = rawRange ?? "7d";

  const data = await getSpeedToLeadData({ timeRange: range });

  return (
    <AppShell>
      <SpeedToLeadRepDetail data={data} repName={repName} />
    </AppShell>
  );
}
