import { NextResponse } from "next/server";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const range = new URL(request.url).searchParams.get("range");
  const data = await getSpeedToLeadData({ timeRange: range });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
