import { RetentionOperatingView } from "@/components/dashboard/RetentionOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getRetentionData } from "@/lib/bigquery/retention-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type RetentionPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function RetentionPage({ searchParams }: RetentionPageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const worklist = Array.isArray(params?.worklist) ? params?.worklist[0] : params?.worklist;
  const data = await getRetentionData({ timeRange: range, worklist });

  return (
    <AppShell>
      <RetentionOperatingView data={data} />
    </AppShell>
  );
}
