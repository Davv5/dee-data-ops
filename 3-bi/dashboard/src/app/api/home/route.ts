import { NextResponse } from "next/server";
import { getHomeSummary } from "@/lib/bigquery/home-summary-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const range = new URL(request.url).searchParams.get("range");
  const data = await getHomeSummary({ timeRange: range });
  return NextResponse.json(data);
}
