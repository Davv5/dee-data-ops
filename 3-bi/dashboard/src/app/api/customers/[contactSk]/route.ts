import { NextResponse } from "next/server";
import { getCustomer360Data } from "@/lib/bigquery/customer-360-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type CustomerApiRouteProps = {
  params: Promise<{
    contactSk: string;
  }>;
};

export async function GET(_request: Request, { params }: CustomerApiRouteProps) {
  const { contactSk } = await params;
  const data = await getCustomer360Data({ contactSk });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
