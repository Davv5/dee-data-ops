import { BarChart3, LineChart, Table2 } from "lucide-react";
import { FreshnessBadge } from "@/components/dashboard/FreshnessBadge";
import { Card, CardBody, CardHeader } from "@/components/ui/Card";
import { queryContracts } from "@/lib/bigquery/named-queries";
import type { DashboardDefinition, DashboardTile } from "@/types/dashboard";

type DashboardRendererProps = {
  dashboard: DashboardDefinition;
};

export function DashboardRenderer({ dashboard }: DashboardRendererProps) {
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
        <FreshnessBadge />
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
                <TilePreview key={`${tile.title}-${tile.query}`} tile={tile} />
              ))}
            </div>
          </section>
        ))}
      </div>
    </div>
  );
}

function TilePreview({ tile }: { tile: DashboardTile }) {
  const query = queryContracts[tile.query as keyof typeof queryContracts];
  const Icon =
    tile.type === "table" ? Table2 : tile.type === "line" ? LineChart : BarChart3;

  return (
    <Card className={tile.type === "table" ? "md:col-span-2 xl:col-span-3" : ""}>
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
        <div className="rounded-md border border-dashed border-[#d5d1c7] bg-[#fbfaf7] p-4">
          <div className="text-xs font-medium uppercase tracking-wide text-[#66635f]">
            Query contract
          </div>
          <div className="mt-2 font-mono text-sm">{tile.query}</div>
          <div className="mt-2 text-xs leading-5 text-[#66635f]">
            {query?.description ?? "Query contract not registered yet."}
          </div>
          {query ? (
            <div className="mt-3 rounded-sm bg-white px-2 py-1 font-mono text-[11px] text-[#3b3936]">
              {query.table}
            </div>
          ) : null}
        </div>
      </CardBody>
    </Card>
  );
}
