import { NextResponse } from "next/server";
import { searchCustomers } from "@/lib/bigquery/customer-search-live";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function GET(request: Request) {
  const query = new URL(request.url).searchParams.get("q");

  try {
    const rows = await searchCustomers(query);
    return NextResponse.json({ rows });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown customer search error";
    return NextResponse.json({ rows: [], error: message }, { status: 503 });
  }
}
