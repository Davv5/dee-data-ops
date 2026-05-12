import type { ReactNode } from "react";
import { TopNav } from "@/components/layout/TopNav";

type AppShellProps = {
  children: ReactNode;
};

export function AppShell({ children }: AppShellProps) {
  return (
    <div className="min-h-screen bg-transparent text-[var(--foreground)]">
      <main className="min-h-screen px-4 py-4 lg:px-6">
        <div className="mx-auto max-w-[108rem]">
          <TopNav />
          {children}
        </div>
      </main>
    </div>
  );
}
