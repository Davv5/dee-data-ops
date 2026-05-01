import { NextResponse } from "next/server";
import { getSpeedToLeadData } from "@/lib/bigquery/speed-to-lead-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET() {
  const data = await getSpeedToLeadData();

  return NextResponse.json(data, {
    status: data.error ? 503 : 200,
  });
}
