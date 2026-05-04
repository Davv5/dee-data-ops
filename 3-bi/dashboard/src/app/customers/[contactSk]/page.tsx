import { Customer360View } from "@/components/dashboard/Customer360View";
import { AppShell } from "@/components/layout/AppShell";
import { getCustomer360Data } from "@/lib/bigquery/customer-360-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type CustomerPageProps = {
  params: Promise<{
    contactSk: string;
  }>;
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function CustomerPage({ params, searchParams }: CustomerPageProps) {
  const { contactSk } = await params;
  const query = await searchParams;
  const data = await getCustomer360Data({ contactSk });
  const sourceContext = buildSourceContext(query);

  return (
    <AppShell>
      <Customer360View
        data={data}
        contactSk={contactSk}
        returnHref={buildReturnHref(query)}
        sourceContext={sourceContext}
      />
    </AppShell>
  );
}

function buildReturnHref(params: Record<string, string | string[] | undefined> | undefined) {
  const from = firstParam(params?.from);

  if (from === "retention") {
    const query = new URLSearchParams({
      range: firstParam(params?.range) ?? "all",
      worklist: firstParam(params?.worklist) ?? "recovery_queue",
    });

    return `/retention?${query.toString()}`;
  }

  if (from === "revenue") {
    const query = new URLSearchParams({
      range: firstParam(params?.range) ?? "all",
    });

    return `/revenue?${query.toString()}`;
  }

  if (from === "lead-magnets") {
    const query = new URLSearchParams({
      range: firstParam(params?.range) ?? "all",
    });

    return `/lead-magnets?${query.toString()}`;
  }

  if (from === "speed-to-lead") {
    const query = new URLSearchParams({
      range: firstParam(params?.range) ?? "30d",
    });

    return `/speed-to-lead?${query.toString()}`;
  }

  return "/retention?range=all&worklist=recovery_queue";
}

function buildSourceContext(params: Record<string, string | string[] | undefined> | undefined) {
  return {
    from: firstParam(params?.from),
    reason: firstParam(params?.reason),
  };
}

function firstParam(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0];
  return value;
}
