export type DashboardRowValue = string | number | boolean | null;

export type DashboardRow = Record<string, DashboardRowValue>;

export type DashboardFreshness = {
  status: "live" | "stale" | "error";
  label: string;
  detail: string;
  refreshedAt?: string;
};

export type DashboardData = {
  rows: Partial<Record<string, DashboardRow[]>>;
  freshness: DashboardFreshness;
  generatedAt: string;
  error?: string;
};
