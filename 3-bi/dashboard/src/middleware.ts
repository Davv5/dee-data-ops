import { NextResponse, type NextRequest } from "next/server";

const SESSION_COOKIE = "ddee_dashboard_access";

export function middleware(request: NextRequest) {
  if (!isAuthEnabled() || isPublicPath(request.nextUrl.pathname)) {
    return NextResponse.next();
  }

  const sessionToken = process.env.DASHBOARD_DEMO_SESSION_TOKEN;
  const cookieToken = request.cookies.get(SESSION_COOKIE)?.value;

  if (sessionToken && cookieToken === sessionToken) {
    return NextResponse.next();
  }

  const loginUrl = new URL("/login", request.url);
  loginUrl.searchParams.set("next", request.nextUrl.pathname + request.nextUrl.search);
  return NextResponse.redirect(loginUrl);
}

function isAuthEnabled() {
  return process.env.DASHBOARD_DEMO_AUTH_ENABLED === "true";
}

function isPublicPath(pathname: string) {
  return pathname === "/login" || pathname === "/api/demo-login" || pathname === "/api/health";
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
