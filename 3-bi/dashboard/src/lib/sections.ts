import { BarChart3, Gauge, Target, Users } from "lucide-react";

export const dashboardSections = [
  {
    slug: "speed-to-lead",
    label: "Speed-to-Lead",
    href: "/speed-to-lead",
    icon: Gauge,
    status: "active",
  },
  {
    slug: "rep-breakdown",
    label: "Rep Breakdown",
    href: "/speed-to-lead#rep-breakdown",
    icon: Users,
    status: "planned",
  },
  {
    slug: "lead-magnets",
    label: "Lead Magnets",
    href: "/lead-magnets",
    icon: Target,
    status: "active",
  },
  {
    slug: "revenue",
    label: "Revenue",
    href: "/revenue",
    icon: BarChart3,
    status: "next",
  },
] as const;
