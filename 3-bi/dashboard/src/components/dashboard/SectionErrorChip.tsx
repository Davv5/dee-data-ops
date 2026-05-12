import { AlertTriangle } from "lucide-react";

export function SectionErrorChip({
  title,
  detail,
  queryName,
}: {
  title: string;
  detail: string;
  queryName: string;
}) {
  return (
    <div
      data-query-error={queryName}
      className="flex items-start gap-2 rounded-md border px-3 py-2 text-xs"
      style={{
        background: "var(--stl-danger-soft)",
        borderColor: "var(--stl-danger)",
        color: "var(--stl-text)",
      }}
    >
      <AlertTriangle
        aria-hidden
        className="mt-[1px] h-3.5 w-3.5 shrink-0"
        style={{ color: "var(--stl-danger)" }}
      />
      <div className="space-y-0.5">
        <div className="font-semibold" style={{ color: "var(--stl-danger)" }}>
          {title} unavailable
        </div>
        <div className="opacity-80">{detail}</div>
      </div>
    </div>
  );
}
