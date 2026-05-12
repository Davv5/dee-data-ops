import { isGateConfigured } from "@/lib/auth/session";

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

type LoginPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

function firstParam(value: string | string[] | undefined): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function errorMessage(code: string | undefined): string | null {
  if (!code) return null;
  switch (code) {
    case "invalid":
      return "Incorrect password.";
    case "gate_not_configured":
      return "The dashboard gate is not configured. Set DASHBOARD_GATE_PASSWORD and DASHBOARD_GATE_COOKIE_SECRET on the Cloud Run service.";
    default:
      return "Sign-in failed.";
  }
}

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const params = (await searchParams) ?? {};
  const next = firstParam(params.next) ?? "/speed-to-lead";
  const error = errorMessage(firstParam(params.error));
  const gateConfigured = isGateConfigured();

  return (
    <main className="min-h-screen bg-[#f7f5ef] text-[#1f1d1a]">
      <div className="mx-auto flex min-h-screen max-w-md flex-col justify-center px-6 py-10">
        <h1 className="text-xl font-semibold">D-DEE Dashboard</h1>
        <p className="mt-1 text-sm text-[#66635f]">
          Enter the dashboard password to continue.
        </p>

        <form method="POST" action="/api/login" className="mt-6 space-y-4">
          <input type="hidden" name="next" value={next} />
          <label className="block">
            <span className="text-xs font-medium uppercase tracking-wide text-[#66635f]">
              Password
            </span>
            <input
              type="password"
              name="password"
              autoComplete="current-password"
              required
              className="mt-1 w-full rounded-md border border-[#dedbd2] bg-white px-3 py-2 text-sm focus:border-[#0f766e] focus:outline-none focus:ring-1 focus:ring-[#0f766e]"
            />
          </label>

          {error ? (
            <p className="rounded-md border border-[#d97757] bg-[#fdecec] px-3 py-2 text-xs text-[#7a2e1f]">
              {error}
            </p>
          ) : null}

          {!gateConfigured ? (
            <p className="rounded-md border border-[#d97757] bg-[#fdecec] px-3 py-2 text-xs text-[#7a2e1f]">
              Gate environment variables are missing. The dashboard is currently
              not protected by this gate.
            </p>
          ) : null}

          <button
            type="submit"
            className="w-full rounded-md bg-[#0f766e] px-4 py-2 text-sm font-semibold text-white hover:bg-[#0c5e57]"
          >
            Sign in
          </button>
        </form>
      </div>
    </main>
  );
}
