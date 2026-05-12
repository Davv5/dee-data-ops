import { AppShell } from "@/components/layout/AppShell";
import { CommandCenterView } from "@/components/dashboard/CommandCenterView";
import { getHomeSummary } from "@/lib/bigquery/home-summary-live";

export const dynamic = "force-dynamic";

type HomeProps = {
  searchParams?: Promise<{ range?: string }>;
};

export default async function Home({ searchParams }: HomeProps) {
  const params = (await searchParams) ?? {};
  const data = await getHomeSummary({ timeRange: params.range });

  return (
    <AppShell>
      <CommandCenterView data={data} />
    </AppShell>
  );
}
