import { SpeedToLeadOperatingView } from "@/components/dashboard/SpeedToLeadOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type SpeedToLeadPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function SpeedToLeadPage({ searchParams }: SpeedToLeadPageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const data = await getSpeedToLeadData({ timeRange: range });

  return (
    <AppShell>
      <SpeedToLeadOperatingView data={data} />
    </AppShell>
  );
}
