import { BarChart3, LineChart, Table2 } from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { queryContracts } from "@/lib/bigquery/named-queries";
import type { DashboardData, DashboardRow, DashboardRowValue } from "@/types/dashboard-data";
import type { DashboardDefinition, DashboardFormat, DashboardTile } from "@/types/dashboard";

type DashboardRendererProps = {
  dashboard: DashboardDefinition;
  data: DashboardData;
};

export function DashboardRenderer({ dashboard, data }: DashboardRendererProps) {
  return (
    <div className="pt-24 md:pt-0">
      <header className="flex flex-col gap-4 border-b border-[#dedbd2] pb-5 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-sm font-medium text-[#0f766e]">
            {dashboard.sourceContract}
          </p>
          <h1 className="mt-1 text-3xl font-semibold tracking-normal md:text-4xl">
            {dashboard.title}
          </h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[#66635f]">
            {dashboard.description}
          </p>
        </div>
        <FreshnessBadge freshness={data.freshness} />
      </header>

      <div className="space-y-8 py-6">
        {dashboard.sections.map((section) => (
          <section key={section.title} id={section.title.toLowerCase().replace(/\s+/g, "-")}>
            <div className="mb-3">
              <h2 className="text-lg font-semibold">{section.title}</h2>
              {section.description ? (
                <p className="mt-1 text-sm text-[#66635f]">
                  {section.description}
                </p>
              ) : null}
            </div>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {section.tiles.map((tile) => (
                <Tile key={`${tile.title}-${tile.query}`} tile={tile} data={data} />
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}

function Tile({ tile, data }: { tile: DashboardTile; data: DashboardData }) {
  if (tile.type === "kpi") {
    return <KpiTile tile={tile} data={data} />;
  }

  if (tile.type === "table") {
    return <TableTile tile={tile} data={data} />;
  }

  return <ChartTile tile={tile} data={data} />;
}

function KpiTile({ tile, data }: { tile: Extract<DashboardTile, { type: "kpi" }>; data: DashboardData }) {
  const rows = rowsForTile(data, tile);
  const value = rows[0]?.[tile.field] ?? null;
  const query = queryContracts[tile.query as keyof typeof queryContracts];

  return (
    <DashboardCard tile={tile}>
      <div className="text-3xl font-semibold tracking-normal">
        {formatValue(value, tile.format)}
      </div>
      <SourceLine table={query?.table} />
    </DashboardCard>
  );
}

function ChartTile({
  tile,
  data,
}: {
  tile: Extract<DashboardTile, { type: "line" | "bar" }>;
  data: DashboardData;
}) {
  const rows = rowsForTile(data, tile);

  return (
    <DashboardCard tile={tile}>
      {tile.type === "line" ? (
        <LineChartTile tile={tile} rows={rows} />
      ) : (
        <BarChartTile tile={tile} rows={rows} />
      )}
      <SourceLine table={queryContracts[tile.query as keyof typeof queryContracts]?.table} />
    </DashboardCard>
  );
}

function TableTile({
  tile,
  data,
}: {
  tile: Extract<DashboardTile, { type: "table" }>;
  data: DashboardData;
}) {
  const rows = rowsForTile(data, tile);

  return (
    <DashboardCard tile={tile} className="md:col-span-2 xl:col-span-3">
      <div className="overflow-x-auto">
        <table className="min-w-full border-separate border-spacing-0 text-left text-sm">
          <thead>
            <tr className="text-xs uppercase text-[#66635f]">
              {tile.columns.map((column) => (
                <th key={column.key} className="border-b border-[#ece9e1] px-3 py-2 font-semibold first:pl-0">
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length > 0 ? (
              rows.map((row, rowIndex) => (
                <tr key={`${tile.query}-${rowIndex}`} className="border-b border-[#ece9e1]">
                  {tile.columns.map((column) => (
                    <td key={column.key} className="border-b border-[#f1eee8] px-3 py-3 first:pl-0">
                      {formatValue(row[column.key] ?? null, column.format)}
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={tile.columns.length} className="py-6 text-sm text-[#66635f]">
                  No rows returned from the live query.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
      <SourceLine table={queryContracts[tile.query as keyof typeof queryContracts]?.table} />
    </DashboardCard>
  );
}

function DashboardCard({
  tile,
  children,
  className,
}: {
  tile: DashboardTile;
  children: React.ReactNode;
  className?: string;
}) {
  const query = queryContracts[tile.query as keyof typeof queryContracts];
  const Icon =
    tile.type === "table" ? Table2 : tile.type === "line" ? LineChart : BarChart3;

  return (
    <Card className={className}>
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold">{tile.title}</h3>
            {tile.description ? (
              <p className="mt-1 text-xs leading-5 text-[#66635f]">
                {tile.description}
              </p>
            ) : null}
          </div>
          <Icon className="h-4 w-4 shrink-0 text-[#0f766e]" aria-hidden="true" />
        </div>
      </CardHeader>
      <CardBody>
        {children}
        {!query ? <p className="mt-3 text-xs text-[#991b1b]">Query contract not registered.</p> : null}
      </CardBody>
    </Card>
  );
}

function LineChartTile({
  tile,
  rows,
}: {
  tile: Extract<DashboardTile, { type: "line" | "bar" }>;
  rows: DashboardRow[];
}) {
  const points = rows
    .map((row, index) => ({
      xLabel: stringLabel(row[tile.x]),
      value: numberValue(row[tile.y]),
      index,
    }))
    .filter((point) => point.value !== null);
  const max = Math.max(...points.map((point) => point.value ?? 0), 0.01);
  const width = 100;
  const height = 48;
  const polyline = points
    .map((point, index) => {
      const x = points.length === 1 ? width : (index / (points.length - 1)) * width;
      const y = height - ((point.value ?? 0) / max) * (height - 6) - 3;
      return `${x},${y}`;
    })
    .join(" ");
  const latest = points[points.length - 1];

  return (
    <div>
      <div className="flex items-end justify-between gap-4">
        <div>
          <div className="text-2xl font-semibold">
            {formatValue(latest?.value ?? null, tile.format)}
          </div>
          <div className="mt-1 text-xs text-[#66635f]">{latest?.xLabel ?? "No recent rows"}</div>
        </div>
      </div>
      <div className="mt-4 h-24 rounded-md border border-[#ece9e1] bg-[#fbfaf7] p-3">
        {points.length > 1 ? (
          <svg viewBox={`0 0 ${width} ${height}`} className="h-full w-full" preserveAspectRatio="none">
            <polyline points={polyline} fill="none" stroke="#0f766e" strokeWidth="2.5" vectorEffect="non-scaling-stroke" />
          </svg>
        ) : (
          <div className="flex h-full items-center text-xs text-[#66635f]">Not enough data for a trend.</div>
        )}
      </div>
    </div>
  );
}

function BarChartTile({
  tile,
  rows,
}: {
  tile: Extract<DashboardTile, { type: "line" | "bar" }>;
  rows: DashboardRow[];
}) {
  const bars = rows
    .map((row) => ({
      label: stringLabel(row[tile.x]),
      value: numberValue(row[tile.y]),
    }))
    .filter((bar) => bar.value !== null)
    .slice(0, 8);
  const max = Math.max(...bars.map((bar) => bar.value ?? 0), 0.01);

  return (
    <div className="space-y-3">
      {bars.length > 0 ? (
        bars.map((bar) => {
          const value = bar.value ?? 0;
          return (
            <div key={`${bar.label}-${value}`} className="grid grid-cols-[minmax(7rem,1fr)_2fr_auto] items-center gap-3 text-sm">
              <div className="truncate text-[#3b3936]">{bar.label}</div>
              <div className="h-2 rounded-sm bg-[#ece9e1]">
                <div
                  className="h-2 rounded-sm bg-[#0f766e]"
                  style={{ width: `${Math.max(3, (value / max) * 100)}%` }}
                />
              </div>
              <div className="w-14 text-right text-xs font-medium text-[#3b3936]">
                {formatValue(value, tile.format)}
              </div>
            </div>
          );
        })
      ) : (
        <div className="py-6 text-sm text-[#66635f]">No rows returned from the live query.</div>
      )}
    </div>
  );
}

function SourceLine({ table }: { table?: string }) {
  if (!table) {
    return null;
  }

  return <div className="mt-4 truncate font-mono text-[11px] text-[#66635f]">{table}</div>;
}

function rowsForTile(data: DashboardData, tile: DashboardTile) {
  return data.rows[tile.query] ?? [];
}

function formatValue(value: DashboardRowValue | undefined, format?: DashboardFormat) {
  if (value === null || value === undefined || value === "") {
    return "N/A";
  }

  if (!format) {
    return String(value);
  }

  const numeric = numberValue(value);

  if (numeric === null) {
    return String(value);
  }

  if (format === "percent") {
    return new Intl.NumberFormat("en-US", {
      style: "percent",
      minimumFractionDigits: numeric > 0 && numeric < 0.1 ? 1 : 0,
      maximumFractionDigits: numeric > 0 && numeric < 0.1 ? 1 : 0,
    }).format(numeric);
  }

  if (format === "currency") {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(numeric);
  }

  if (format === "duration") {
    return formatDuration(numeric);
  }

  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 1,
  }).format(numeric);
}

function formatDuration(minutes: number) {
  if (!Number.isFinite(minutes)) {
    return "N/A";
  }

  if (minutes >= 1440) {
    return `${(minutes / 1440).toFixed(1)}d`;
  }

  if (minutes >= 60) {
    return `${(minutes / 60).toFixed(1)}h`;
  }

  return `${minutes.toFixed(1)}m`;
}

function numberValue(value: DashboardRowValue | undefined) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string" && value.trim() !== "") {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
  }

  return null;
}

function stringLabel(value: DashboardRowValue | undefined) {
  if (typeof value === "string" && value.trim() !== "") {
    return value;
  }

  if (typeof value === "number") {
    return String(value);
  }

  return "N/A";
}
