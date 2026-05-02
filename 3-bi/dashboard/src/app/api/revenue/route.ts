import { NextResponse } from "next/server";
import { getRevenueFunnelData } from "@/lib/bigquery/revenue-funnel-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const range = new URL(request.url).searchParams.get("range");
  const data = await getRevenueFunnelData({ timeRange: range });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
