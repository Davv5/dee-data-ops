import type { QueryTier } from "@/lib/bigquery/query-tiers";

const TIER_LABEL: Record<QueryTier, string> = {
  critical: "Critical",
  section: "Section",
  audit: "Audit / proof",
};

const TIER_ORDER: QueryTier[] = ["critical", "section", "audit"];

export function DataHealthDisclosure({
  errors,
  tierFor,
}: {
  errors: Partial<Record<string, string>>;
  tierFor: (name: string) => QueryTier;
}) {
  const entries = Object.entries(errors).filter((entry): entry is [string, string] => Boolean(entry[1]));
  if (entries.length === 0) return null;

  const grouped = new Map<QueryTier, [string, string][]>();
  for (const tier of TIER_ORDER) grouped.set(tier, []);
  for (const [name, detail] of entries) {
    const tier = tierFor(name);
    grouped.get(tier)!.push([name, detail]);
  }

  return (
    <details
      data-data-health
      className="rounded-lg border px-3 py-2 text-xs"
      style={{
        background: "var(--stl-card)",
        borderColor: "var(--stl-border)",
      }}
    >
      <summary className="flex cursor-pointer items-center gap-2 text-[var(--stl-text)]">
        <span className="font-semibold">Data Health</span>
        <span
          className="rounded-full px-2 py-0.5 text-[10px] font-semibold"
          style={{
            background: "var(--stl-danger-soft)",
            color: "var(--stl-danger)",
          }}
        >
          {entries.length} query {entries.length === 1 ? "issue" : "issues"}
        </span>
        <span className="opacity-60">— click to expand</span>
      </summary>
      <div className="mt-2 space-y-3">
        {TIER_ORDER.map((tier) => {
          const list = grouped.get(tier) ?? [];
          if (list.length === 0) return null;
          return (
            <section key={tier} className="space-y-1">
              <h4 className="text-[10px] font-semibold uppercase tracking-wide opacity-70">
                {TIER_LABEL[tier]}
              </h4>
              <ul className="space-y-1">
                {list.map(([name, detail]) => (
                  <li key={name} className="rounded border px-2 py-1" style={{ borderColor: "var(--stl-border)" }}>
                    <div className="font-mono text-[10px] opacity-80">{name}</div>
                    <div className="text-[11px]">{detail}</div>
                  </li>
                ))}
              </ul>
            </section>
          );
        })}
      </div>
    </details>
  );
}
