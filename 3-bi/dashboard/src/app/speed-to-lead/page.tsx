import { DashboardRenderer } from "@/components/dashboard/DashboardRenderer";
import { AppShell } from "@/components/layout/AppShell";
import { speedToLeadDashboard } from "@/lib/dashboards/speed-to-lead";

export default function SpeedToLeadPage() {
  return (
    <AppShell>
      <DashboardRenderer dashboard={speedToLeadDashboard} />
    </AppShell>
  );
}
