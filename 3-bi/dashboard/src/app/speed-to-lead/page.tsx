import { SpeedToLeadOperatingView } from "@/components/dashboard/SpeedToLeadOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export default async function SpeedToLeadPage() {
  const data = await getSpeedToLeadData();

  return (
    <AppShell>
      <SpeedToLeadOperatingView data={data} />
    </AppShell>
  );
}
