import { DashboardRenderer } from "@/components/dashboard/DashboardRenderer";
import { AppShell } from "@/components/layout/AppShell";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";
import { speedToLeadDashboard } from "@/lib/dashboards/speed-to-lead";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export default async function SpeedToLeadPage() {
  const data = await getSpeedToLeadData();

  return (
    <AppShell>
      <DashboardRenderer dashboard={speedToLeadDashboard} data={data} />
    </AppShell>
  );
}
