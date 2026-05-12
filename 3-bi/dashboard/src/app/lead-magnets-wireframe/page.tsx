import { LeadMagnetsWireframeView } from "@/components/dashboard/LeadMagnetsWireframeView";
import { AppShell } from "@/components/layout/AppShell";
import { getLeadMagnetData } from "@/lib/bigquery/lead-magnets-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type LeadMagnetsWireframePageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function LeadMagnetsWireframePage({ searchParams }: LeadMagnetsWireframePageProps) {
  const params = await searchParams;
  const range = Array.isArray(params?.range) ? params?.range[0] : params?.range;
  const group = Array.isArray(params?.group) ? params?.group[0] : params?.group;
  const data = await getLeadMagnetData({ timeRange: range });

  return (
    <AppShell>
      <LeadMagnetsWireframeView data={data} sourceGroup={group} />
    </AppShell>
  );
}
