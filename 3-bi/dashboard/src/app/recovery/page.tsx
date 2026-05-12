import { RecoveryDashboard } from "@/components/dashboard/RecoveryDashboard";
import { AppShell } from "@/components/layout/AppShell";
import { getRecoveryData } from "@/lib/bigquery/recovery-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type RecoveryPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function RecoveryPage({ searchParams }: RecoveryPageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const data = await getRecoveryData({ timeRange: range });

  return (
    <AppShell>
      <RecoveryDashboard data={data} />
    </AppShell>
  );
}
