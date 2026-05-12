import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";
import {
  SESSION_COOKIE_NAME,
  SMOKE_TOKEN_HEADER,
  isGateConfigured,
  smokeTokenMatches,
  verifySessionCookieValue,
} from "@/lib/auth/session";

// Paths the gate must let through unauthenticated.
// - /login is the gate page itself.
// - /api/login is the POST handler that issues the session cookie.
// - /api/health stays open so Cloud Run probes keep working.
// - /favicon.ico keeps the browser tab clean for the login screen.
const ALWAYS_ALLOWED_PATHS = new Set<string>([
  "/login",
  "/api/login",
  "/api/health",
  "/favicon.ico",
]);

// Prefixes that never carry sensitive data. Next.js already excludes /_next/static
// and /_next/image from middleware via the matcher, but we belt-and-suspenders.
const ALWAYS_ALLOWED_PREFIXES = ["/_next/", "/__nextjs", "/public/"];

function isAllowedPath(pathname: string): boolean {
  if (ALWAYS_ALLOWED_PATHS.has(pathname)) return true;
  for (const prefix of ALWAYS_ALLOWED_PREFIXES) {
    if (pathname.startsWith(prefix)) return true;
  }
  return false;
}

function buildLoginRedirect(request: NextRequest): NextResponse {
  const loginUrl = new URL("/login", request.url);
  const original = request.nextUrl.pathname + request.nextUrl.search;
  if (original && original !== "/login") {
    loginUrl.searchParams.set("next", original);
  }
  return NextResponse.redirect(loginUrl);
}

function unauthorizedJson(): NextResponse {
  return NextResponse.json(
    { error: "Unauthorized. Sign in at /login or pass a valid smoke token header." },
    { status: 401 },
  );
}

export async function proxy(request: NextRequest) {
  // Until DASHBOARD_GATE_PASSWORD and DASHBOARD_GATE_COOKIE_SECRET are set,
  // the gate is dormant and behaves like the pre-patch dashboard. The smoke
  // check verifies that production has the gate enforced.
  if (!isGateConfigured()) return NextResponse.next();

  const { pathname } = request.nextUrl;
  if (isAllowedPath(pathname)) return NextResponse.next();

  // Smoke / canary bypass: a request carrying the smoke token header skips the gate.
  const smokeHeader = request.headers.get(SMOKE_TOKEN_HEADER);
  if (smokeTokenMatches(smokeHeader)) return NextResponse.next();

  const sessionCookie = request.cookies.get(SESSION_COOKIE_NAME)?.value;
  if (await verifySessionCookieValue(sessionCookie)) return NextResponse.next();

  // For API routes, return 401 JSON so client-side fetches do not get the HTML login page.
  if (pathname.startsWith("/api/")) return unauthorizedJson();

  return buildLoginRedirect(request);
}

// Run on everything except the static asset pipeline. Matcher excludes _next/static,
// _next/image, and the favicon to avoid unnecessary middleware invocations.
export const config = {
  matcher: ["/((?!_next/static|_next/image).*)"],
};
