export type DashboardRowValue = string | number | boolean | null;

export type DashboardRow = Record<string, DashboardRowValue>;

export type DashboardFreshness = {
  status: "live" | "stale" | "error";
  label: string;
  detail: string;
  refreshedAt?: string;
};

export type DashboardFilterOption = {
  value: string;
  label: string;
  description: string;
};

export type DashboardFilters = {
  timeRange: string;
  timeRangeLabel: string;
  timeRangeDescription: string;
  timeRangeOptions: DashboardFilterOption[];
  worklist?: string;
  worklistLabel?: string;
  worklistDescription?: string;
  worklistOptions?: DashboardFilterOption[];
};

export type DashboardData = {
  rows: Partial<Record<string, DashboardRow[]>>;
  freshness: DashboardFreshness;
  filters: DashboardFilters;
  generatedAt: string;
  error?: string;
  dataContract?: {
    owner: "bq-ingest-report" | "dbt-mart";
    projectId: string;
    dataset: string;
    tables: string[];
    note: string;
  };
};
