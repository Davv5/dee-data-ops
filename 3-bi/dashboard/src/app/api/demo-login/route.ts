import { NextResponse } from "next/server";

const SESSION_COOKIE = "ddee_dashboard_access";
const ONE_DAY_SECONDS = 60 * 60 * 24;

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

export async function POST(request: Request) {
  const formData = await request.formData();
  const password = stringValue(formData.get("password"));
  const next = safeNextPath(stringValue(formData.get("next")) ?? "/speed-to-lead");
  const expectedPassword = process.env.DASHBOARD_DEMO_PASSWORD;
  const sessionToken = process.env.DASHBOARD_DEMO_SESSION_TOKEN;

  if (!expectedPassword || !sessionToken || password !== expectedPassword) {
    return NextResponse.redirect(new URL(`/login?error=1&next=${encodeURIComponent(next)}`, publicOrigin(request)));
  }

  const response = NextResponse.redirect(new URL(next, publicOrigin(request)));
  response.cookies.set(SESSION_COOKIE, sessionToken, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    maxAge: ONE_DAY_SECONDS,
    path: "/",
  });

  return response;
}

function stringValue(value: FormDataEntryValue | null) {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function safeNextPath(value: string) {
  if (!value.startsWith("/") || value.startsWith("//")) return "/speed-to-lead";
  return value;
}

function publicOrigin(request: Request) {
  const proto = request.headers.get("x-forwarded-proto") ?? "https";
  const host = request.headers.get("x-forwarded-host") ?? request.headers.get("host") ?? new URL(request.url).host;

  return `${proto}://${host}`;
}
