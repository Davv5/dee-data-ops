import { NextResponse } from "next/server";
import { getLeadMagnetData } from "@/lib/bigquery/lead-magnets-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const range = new URL(request.url).searchParams.get("range");
  const data = await getLeadMagnetData({ timeRange: range });

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
