import { LeadMagnetsGuideContent } from "@/components/dashboard/LeadMagnetsGuide";
import { AppShell } from "@/components/layout/AppShell";

export default function LeadMagnetsGuidePage() {
  return (
    <AppShell>
      <div className="pb-8">
        <LeadMagnetsGuideContent mode="page" />
      </div>
    </AppShell>
  );
}
