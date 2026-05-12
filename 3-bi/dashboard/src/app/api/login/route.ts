import { NextResponse } from "next/server";
import {
  SESSION_COOKIE_NAME,
  SESSION_TTL_SECONDS,
  checkSubmittedPassword,
  isGateConfigured,
  issueSessionCookieValue,
} from "@/lib/auth/session";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

const SAFE_NEXT_PATTERN = /^\/[A-Za-z0-9_\-./?=&%]*$/;

function safeNextPath(value: FormDataEntryValue | string | null): string {
  if (typeof value !== "string") return "/speed-to-lead";
  if (!value.startsWith("/")) return "/speed-to-lead";
  if (value.startsWith("//")) return "/speed-to-lead";
  if (!SAFE_NEXT_PATTERN.test(value)) return "/speed-to-lead";
  return value;
}

function redirectResponse(request: Request, path: string): NextResponse {
  const url = new URL(path, request.url);
  return NextResponse.redirect(url, 303);
}

export async function POST(request: Request) {
  const contentType = request.headers.get("content-type") ?? "";
  let password: string | null = null;
  let next: string = "/speed-to-lead";

  if (contentType.includes("application/x-www-form-urlencoded") || contentType.includes("multipart/form-data")) {
    const form = await request.formData();
    const pw = form.get("password");
    password = typeof pw === "string" ? pw : null;
    next = safeNextPath(form.get("next"));
  } else if (contentType.includes("application/json")) {
    const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
    password = typeof body.password === "string" ? body.password : null;
    next = safeNextPath(typeof body.next === "string" ? body.next : null);
  } else {
    return redirectResponse(request, "/login?error=invalid&next=" + encodeURIComponent(next));
  }

  if (!isGateConfigured()) {
    return redirectResponse(request, "/login?error=gate_not_configured");
  }

  if (!checkSubmittedPassword(password)) {
    return redirectResponse(
      request,
      `/login?error=invalid&next=${encodeURIComponent(next)}`,
    );
  }

  const cookieValue = await issueSessionCookieValue();
  if (!cookieValue) {
    return redirectResponse(request, "/login?error=gate_not_configured");
  }

  const response = redirectResponse(request, next);
  response.cookies.set({
    name: SESSION_COOKIE_NAME,
    value: cookieValue,
    httpOnly: true,
    secure: true,
    // SameSite=None so the cookie is sent inside the Cabinet iframe. Operators
    // sign in via a top-level visit once; iframe loads then carry the cookie.
    sameSite: "none",
    path: "/",
    maxAge: SESSION_TTL_SECONDS,
  });
  return response;
}
