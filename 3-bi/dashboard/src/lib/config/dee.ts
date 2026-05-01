export type DeeConfig = {
  tenantId: "dee";
  tenantName: string;
  bigQuery: {
    projectId: string;
    dataset: string;
    tables: {
      speedToLeadOverall: string;
      speedToLeadDaily: string;
      speedToLeadFact: string;
      speedToLeadWeek: string;
      repScorecardWeek: string;
    };
  };
  auth: {
    allowedEmails: string[];
  };
};

export const deeConfig: DeeConfig = {
  tenantId: "dee",
  tenantName: "D-DEE",
  bigQuery: {
    projectId: "project-41542e21-470f-4589-96d",
    dataset: "Marts",
    tables: {
      speedToLeadOverall: "mrt_speed_to_lead_overall",
      speedToLeadDaily: "mrt_speed_to_lead_daily",
      speedToLeadFact: "fct_speed_to_lead",
      speedToLeadWeek: "rpt_speed_to_lead_week",
      repScorecardWeek: "rpt_rep_scorecard_week",
    },
  },
  auth: {
    allowedEmails: [],
  },
};
