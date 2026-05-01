export type DashboardFormat = "number" | "percent" | "currency" | "duration";

export type DashboardTile =
  | {
      type: "kpi";
      title: string;
      query: string;
      field: string;
      format: DashboardFormat;
      description?: string;
    }
  | {
      type: "line" | "bar";
      title: string;
      query: string;
      x: string;
      y: string;
      format?: DashboardFormat;
      description?: string;
    }
  | {
      type: "table";
      title: string;
      query: string;
      columns: Array<{
        key: string;
        label: string;
        format?: DashboardFormat;
      }>;
      description?: string;
    };

export type DashboardSection = {
  title: string;
  description?: string;
  tiles: DashboardTile[];
};

export type DashboardDefinition = {
  slug: string;
  title: string;
  description: string;
  sourceContract: "bq-ingest-report" | "dbt-mart";
  sections: DashboardSection[];
};
