"use client";

import { Activity, Gauge, Target } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
  { href: "/speed-to-lead", label: "Speed-to-Lead", icon: Gauge },
  { href: "/lead-magnets", label: "Lead Magnets", icon: Target },
];

export function TopNav() {
  const pathname = usePathname();

  return (
    <nav className="mb-4 flex flex-col gap-3 border-b border-[#dedbd2] pb-3 sm:flex-row sm:items-center sm:justify-between">
      <Link href="/speed-to-lead" className="flex items-center gap-2 text-sm font-semibold">
        <span className="flex h-8 w-8 items-center justify-center rounded-md bg-[#0f766e] text-white">
          <Activity className="h-4 w-4" aria-hidden />
        </span>
        <span>D-DEE Dashboard</span>
      </Link>
      <div className="flex flex-wrap gap-2">
        {navItems.map((item) => {
          const Icon = item.icon;
          const isActive = pathname === item.href;

          return (
            <Link
              key={item.href}
              href={item.href}
              aria-current={isActive ? "page" : undefined}
              className={`flex min-w-fit items-center gap-2 rounded-md border px-3 py-2 text-xs font-semibold transition ${
                isActive
                  ? "border-[#0f766e] bg-[#0f766e] text-white"
                  : "border-[#dedbd2] bg-white text-[#3b3936] hover:bg-[#f3f1ea]"
              }`}
            >
              <Icon className="h-3.5 w-3.5" aria-hidden />
              {item.label}
            </Link>
          );
        })}
      </div>
    </nav>
  );
}
