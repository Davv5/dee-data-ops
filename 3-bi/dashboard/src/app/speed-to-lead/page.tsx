import { SpeedToLeadDashboard } from "@/components/dashboard/SpeedToLeadDashboard";
import { AppShell } from "@/components/layout/AppShell";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type SpeedToLeadPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function SpeedToLeadPage({ searchParams }: SpeedToLeadPageProps) {
  const params = await searchParams;
  // Default to 7d when no range param. "Today" produces an empty dashboard before
  // the SDR shift fills the window — terrible first-load UX. 7d is the smallest
  // window with meaningful sample size for this team. Override via ?range=.
  const rawRange = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const range = rawRange ?? "7d";
  const data = await getSpeedToLeadData({ timeRange: range });

  return (
    <AppShell>
      <SpeedToLeadDashboard data={data} />
    </AppShell>
  );
}
