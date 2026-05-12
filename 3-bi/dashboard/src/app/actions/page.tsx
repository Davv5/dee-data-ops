import { ActionQueueOperatingView } from "@/components/dashboard/ActionQueueOperatingView";
import { AppShell } from "@/components/layout/AppShell";
import { getCustomerActionsData } from "@/lib/bigquery/customer-actions-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type ActionsPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function ActionsPage({ searchParams }: ActionsPageProps) {
  const params = await searchParams;
  const area = Array.isArray(params?.area) ? params?.area[0] : params?.area;
  const includeClosed = Array.isArray(params?.includeClosed) ? params?.includeClosed[0] : params?.includeClosed;
  const limit = Array.isArray(params?.limit) ? params?.limit[0] : params?.limit;
  const data = await getCustomerActionsData({ area, includeClosed, limit });

  return (
    <AppShell>
      <ActionQueueOperatingView data={data} />
    </AppShell>
  );
}
