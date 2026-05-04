export const dynamic = "force-dynamic";

type LoginPageProps = {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
};

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const params = await searchParams;
  const next = firstParam(params?.next) ?? "/speed-to-lead";
  const hasError = firstParam(params?.error) === "1";

  return (
    <main className="min-h-screen bg-[#f7f7f4] px-4 py-8 text-[#171717]">
      <div className="mx-auto flex min-h-[calc(100vh-4rem)] max-w-md items-center">
        <section className="w-full rounded-lg border border-[#dedbd2] bg-white p-5 shadow-sm">
          <div className="mb-4 flex items-center gap-3">
            <span className="flex h-10 w-10 items-center justify-center rounded-md bg-[#0f766e] text-sm font-semibold text-white">
              D
            </span>
            <div>
              <p className="text-sm font-semibold">D-DEE Dashboard</p>
              <p className="mt-0.5 text-xs text-[#66635f]">Demo access</p>
            </div>
          </div>

          <h1 className="text-2xl font-semibold tracking-normal">Enter the dashboard password</h1>
          <p className="mt-2 text-sm leading-6 text-[#66635f]">
            This protects the live client demo while the full magic-link access layer is still parked.
          </p>

          {hasError ? (
            <div className="mt-4 rounded-md border border-[#fecaca] bg-[#fef2f2] px-3 py-2 text-sm font-medium text-[#991b1b]">
              That password did not work.
            </div>
          ) : null}

          <form action="/api/demo-login" method="post" className="mt-5 space-y-3">
            <input type="hidden" name="next" value={safeNextPath(next)} />
            <label className="block">
              <span className="text-xs font-semibold uppercase text-[#66635f]">Password</span>
              <input
                name="password"
                type="password"
                autoComplete="current-password"
                className="mt-1 w-full rounded-md border border-[#dedbd2] px-3 py-2 text-sm outline-none focus:border-[#0f766e]"
                required
              />
            </label>
            <button
              type="submit"
              className="w-full rounded-md bg-[#0f766e] px-3 py-2 text-sm font-semibold text-white hover:bg-[#115e59]"
            >
              Open dashboard
            </button>
          </form>
        </section>
      </div>
    </main>
  );
}

function firstParam(value: string | string[] | undefined) {
  if (Array.isArray(value)) return value[0];
  return value;
}

function safeNextPath(value: string) {
  if (!value.startsWith("/") || value.startsWith("//")) return "/speed-to-lead";
  return value;
}
