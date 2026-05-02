import { LeadMagnetsOperatingView } from "@/components/dashboard/LeadMagnetsOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getLeadMagnetData } from "@/lib/bigquery/lead-magnets-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type LeadMagnetsPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function LeadMagnetsPage({ searchParams }: LeadMagnetsPageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const data = await getLeadMagnetData({ timeRange: range });

  return (
    <AppShell>
      <LeadMagnetsOperatingView data={data} />
    </AppShell>
  );
}
