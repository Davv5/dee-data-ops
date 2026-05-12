import { NextResponse } from "next/server";
import { getRetentionData } from "@/lib/bigquery/retention-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const searchParams = new URL(request.url).searchParams;
  const range = searchParams.get("range");
  const worklist = searchParams.get("worklist");
  const data = await getRetentionData({ timeRange: range, worklist });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
