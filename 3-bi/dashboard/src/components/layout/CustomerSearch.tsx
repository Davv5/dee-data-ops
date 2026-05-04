"use client";

import { Search, UserRound } from "lucide-react";
import Link from "next/link";
import { useEffect, useRef, useState } from "react";

type CustomerSearchRow = {
  contact_sk?: string | null;
  customer_display_name?: string | null;
  email_norm?: string | null;
  phone?: string | null;
  payment_plan_health_status?: string | null;
  retention_operator_next_action?: string | null;
  lifetime_net_revenue_after_refunds?: string | number | null;
  top_product_by_net_revenue?: string | null;
};

export function CustomerSearch() {
  const [query, setQuery] = useState("");
  const [rows, setRows] = useState<CustomerSearchRow[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const trimmed = query.trim();

    if (trimmed.length < 2) {
      return;
    }

    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setIsLoading(true);
      setError(null);

      try {
        const response = await fetch(`/api/customers/search?q=${encodeURIComponent(trimmed)}`, {
          signal: controller.signal,
        });
        const payload = await response.json();

        if (!response.ok) {
          throw new Error(payload.error ?? "Search unavailable");
        }

        setRows(Array.isArray(payload.rows) ? payload.rows : []);
        setIsOpen(true);
      } catch (searchError) {
        if (controller.signal.aborted) return;
        setRows([]);
        setError(searchError instanceof Error ? searchError.message : "Search unavailable");
        setIsOpen(true);
      } finally {
        if (!controller.signal.aborted) setIsLoading(false);
      }
    }, 180);

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [query]);

  function handleFocus() {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    if (query.trim().length >= 2) setIsOpen(true);
  }

  function handleQueryChange(nextQuery: string) {
    setQuery(nextQuery);

    if (nextQuery.trim().length < 2) {
      setRows([]);
      setIsLoading(false);
      setError(null);
      setIsOpen(false);
    }
  }

  function handleBlur() {
    closeTimer.current = setTimeout(() => setIsOpen(false), 160);
  }

  return (
    <div className="relative w-full sm:max-w-sm">
      <label className="sr-only" htmlFor="customer-search">
        Search customers
      </label>
      <div className="flex items-center gap-2 rounded-md border border-[#dedbd2] bg-white px-3 py-2 shadow-sm focus-within:border-[#0f766e]">
        <Search className="h-4 w-4 shrink-0 text-[#66635f]" aria-hidden />
        <input
          id="customer-search"
          value={query}
          onChange={(event) => handleQueryChange(event.target.value)}
          onFocus={handleFocus}
          onBlur={handleBlur}
          placeholder="Search customer"
          className="min-w-0 flex-1 bg-transparent text-sm outline-none placeholder:text-[#99958e]"
        />
      </div>

      {isOpen && query.trim().length >= 2 ? (
        <div className="absolute left-0 right-0 top-full z-30 mt-2 overflow-hidden rounded-lg border border-[#dedbd2] bg-white shadow-lg">
          <div className="border-b border-[#ece9e1] px-3 py-2 text-[11px] font-semibold uppercase text-[#66635f]">
            {isLoading ? "Searching" : error ? "Search issue" : `${rows.length} matches`}
          </div>
          {error ? (
            <div className="px-3 py-3 text-xs text-[#991b1b]">{error}</div>
          ) : rows.length ? (
            <div className="max-h-96 overflow-y-auto">
              {rows.map((row) => {
                const contactSk = row.contact_sk;
                if (!contactSk) return null;

                return (
                  <Link
                    key={contactSk}
                    href={`/customers/${contactSk}`}
                    className="grid gap-2 border-b border-[#ece9e1] px-3 py-3 last:border-b-0 hover:bg-[#f8fffd]"
                    onClick={() => setIsOpen(false)}
                  >
                    <div className="flex items-start gap-2">
                      <span className="mt-0.5 rounded-md border border-[#bbf7d0] bg-[#f0fdf4] p-1 text-[#166534]">
                        <UserRound className="h-3.5 w-3.5" aria-hidden />
                      </span>
                      <span className="min-w-0">
                        <span className="block truncate text-sm font-semibold text-[#2d2b28]">
                          {row.customer_display_name ?? "Unknown customer"}
                        </span>
                        <span className="block truncate text-[11px] text-[#66635f]">
                          {[row.email_norm, row.phone].filter(Boolean).join(" · ") || "No email or phone"}
                        </span>
                      </span>
                    </div>
                    <div className="grid gap-1 pl-8 text-[11px] text-[#66635f]">
                      <span className="truncate">
                        {labelize(row.retention_operator_next_action)} · {formatCurrency(row.lifetime_net_revenue_after_refunds)}
                      </span>
                      <span className="truncate">{row.top_product_by_net_revenue ?? "No product found"}</span>
                    </div>
                  </Link>
                );
              })}
            </div>
          ) : (
            <div className="px-3 py-3 text-xs text-[#66635f]">No matching customers found.</div>
          )}
        </div>
      ) : null}
    </div>
  );
}

function labelize(value: string | null | undefined) {
  if (!value) return "Open profile";
  return value
    .replaceAll("_", " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatCurrency(value: string | number | null | undefined) {
  const numeric = typeof value === "number" ? value : typeof value === "string" ? Number(value) : null;
  if (numeric === null || !Number.isFinite(numeric)) return "N/A";

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(numeric);
}
