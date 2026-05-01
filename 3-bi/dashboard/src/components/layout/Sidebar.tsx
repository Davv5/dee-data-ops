"use client";

import { Activity, ExternalLink } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { dashboardSections } from "@/lib/sections";
import { cn } from "@/lib/utils";

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed inset-x-0 top-0 z-20 border-b border-[#dedbd2] bg-white/92 px-4 py-3 backdrop-blur md:inset-y-0 md:left-0 md:w-64 md:border-b-0 md:border-r md:px-3 md:py-4">
      <div className="flex items-center justify-between gap-3 md:block">
        <Link href="/speed-to-lead" className="flex items-center gap-3">
          <span className="flex h-9 w-9 items-center justify-center rounded-md bg-[#0f766e] text-white">
            <Activity className="h-5 w-5" aria-hidden="true" />
          </span>
          <span>
            <span className="block text-sm font-semibold leading-5">
              D-DEE Dashboard
            </span>
            <span className="block text-xs text-[#66635f]">
              Precision Scaling
            </span>
          </span>
        </Link>
      </div>

      <nav className="mt-4 flex gap-2 overflow-x-auto md:mt-8 md:block md:space-y-1 md:overflow-visible">
        {dashboardSections.map((section) => {
          const Icon = section.icon;
          const isActive =
            section.href === pathname ||
            (section.href.startsWith(pathname) && pathname !== "/");

          return (
            <Link
              key={section.slug}
              href={section.href}
              className={cn(
                "flex min-w-fit items-center gap-2 rounded-md px-3 py-2 text-sm transition",
                isActive
                  ? "bg-[#0f766e] text-white"
                  : "text-[#3b3936] hover:bg-[#efede7]",
              )}
            >
              <Icon className="h-4 w-4" aria-hidden="true" />
              <span>{section.label}</span>
              {section.status !== "active" ? (
                <span className="rounded-sm border border-current/20 px-1.5 py-0.5 text-[10px] uppercase tracking-wide opacity-75">
                  {section.status}
                </span>
              ) : null}
            </Link>
          );
        })}
      </nav>

      <div className="mt-8 hidden rounded-md border border-[#dedbd2] bg-[#fbfaf7] p-3 text-xs text-[#66635f] md:block">
        <div className="flex items-center gap-2 font-medium text-[#3b3936]">
          <ExternalLink className="h-3.5 w-3.5" aria-hidden="true" />
          Data contract
        </div>
        <p className="mt-2 leading-5">
          Speed-to-Lead v1 uses existing bq-ingest report tables while the
          durable dbt mart layer stabilizes.
        </p>
      </div>
    </aside>
  );
}
