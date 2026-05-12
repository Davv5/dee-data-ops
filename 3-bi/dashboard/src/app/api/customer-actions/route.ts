import { NextResponse } from "next/server";
import { getCustomerActionsData } from "@/lib/bigquery/customer-actions-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const area = searchParams.get("area");
  const includeClosed = searchParams.get("includeClosed");
  const limit = searchParams.get("limit");
  const data = await getCustomerActionsData({ area, includeClosed, limit });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
