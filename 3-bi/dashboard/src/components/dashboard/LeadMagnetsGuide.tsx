"use client";

import Link from "next/link";
import {
  ArrowLeft,
  ExternalLink,
  X,
} from "lucide-react";

type LeadMagnetsGuideProps = {
  mode?: "page" | "window";
};

type LeadMagnetsGuideWindowProps = {
  open: boolean;
  onClose: () => void;
};

const walkthroughSteps = [
  {
    title: "Start",
    body: "Read the Decision Brief and Priority 10.",
  },
  {
    title: "Inspect",
    body: "Click the magnet row before making the call.",
  },
  {
    title: "Decide",
    body: "Update, create adjacent, improve handoff, check tracking, or park it.",
  },
];

const weeklyWorkflow = [
  "Set range.",
  "Scan Decision Brief.",
  "Open 2-3 Priority 10 rows.",
  "Check Bench only if needed.",
  "Leave with 1-3 actions.",
];

const questionDefinitions = [
  {
    label: "Influencing buyers",
    value: "Which magnets have real buyer proof and revenue influence?",
  },
  {
    label: "Creating calls",
    value: "Which magnets are turning attention into booked-call intent?",
  },
  {
    label: "Helping sales",
    value: "Which magnets warm buyers even when sales gets final credit?",
  },
  {
    label: "Missing proof",
    value: "Which magnets have demand but thin or missing buyer proof?",
  },
  {
    label: "Next creation",
    value: "Which magnets deserve adjacent versions or refreshed angles?",
  },
];

const metricDefinitions = [
  {
    label: "Influenced buyers",
    value: "Buyers with a lead-magnet signal before first purchase. This is nonexclusive influence.",
  },
  {
    label: "Direct credit",
    value: "Buyers where the true lead magnet kept latest-touch credit before first purchase.",
  },
  {
    label: "Sales handoff",
    value: "Buyers influenced by a magnet but later credited to a sales pipeline motion.",
  },
  {
    label: "Booked calls",
    value: "Calls tied to lead-magnet paths inside the selected buyer window.",
  },
  {
    label: "Influenced net",
    value: "Collected buyer net revenue associated with influenced buyers. It is not exclusive final credit.",
  },
];

const viewDefinitions = [
  {
    label: "Priority 10",
    value: "The everyday command view. Start here when deciding what to update, repeat, or create next.",
  },
  {
    label: "Bench",
    value: "The rest of the magnets with useful data. These are monitored, not ignored.",
  },
  {
    label: "Audit All",
    value: "Full inspection mode for RevOps review, QA, and source-checking.",
  },
];

const rowLabelDefinitions = [
  {
    label: "Buyer proof",
    value: "This magnet has influenced buyers and revenue signal. Consider protecting it, updating it, or creating adjacent versions.",
  },
  {
    label: "Calls ahead",
    value: "This magnet creates calls faster than it creates buyer proof. Inspect the post-call follow-up and buyer conversion path.",
  },
  {
    label: "Sales assist",
    value: "This magnet appears to warm buyers before a sales motion gets final credit. This can be a good signal, not a loss.",
  },
  {
    label: "Tracking gap",
    value: "There is demand, but buyer proof is thin or missing. Check source capture, attribution joins, and follow-up path.",
  },
  {
    label: "Demand signal",
    value: "There are leads or activity, but not enough buyer signal yet to make a big decision.",
  },
  {
    label: "Low signal",
    value: "There is not enough visible signal in the selected range. Keep it available, but do not overwork it yet.",
  },
];

const signalTranslations = [
  {
    signal: "Buyer proof",
    read: "The magnet has influenced buyers and revenue.",
    next: "Protect the core idea, then test a clearer or more specific adjacent version.",
  },
  {
    signal: "Calls ahead",
    read: "The magnet is creating call intent faster than buyer proof.",
    next: "Inspect booking quality, show rates, post-call follow-up, and close path.",
  },
  {
    signal: "Sales assist",
    read: "The magnet may be warming people before a sales motion closes them.",
    next: "Make the sales bridge explicit and keep the handoff measurable.",
  },
  {
    signal: "Tracking gap",
    read: "Demand exists, but attribution proof is thin.",
    next: "Check source capture, form mapping, opportunity joins, and naming consistency.",
  },
  {
    signal: "Low signal",
    read: "The selected window does not show enough evidence yet.",
    next: "Keep it visible, but do not spend strategy time here first.",
  },
];

const referenceGroups = [
  {
    title: "Views",
    eyebrow: "where to stand",
    items: viewDefinitions,
  },
  {
    title: "Row Labels",
    eyebrow: "what the badge means",
    items: rowLabelDefinitions,
  },
  {
    title: "Core Metrics",
    eyebrow: "what the number means",
    items: metricDefinitions,
  },
];

export function LeadMagnetsGuideWindow({ open, onClose }: LeadMagnetsGuideWindowProps) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50">
      <button
        type="button"
        aria-label="Close Lead Magnet guide"
        className="absolute inset-0 bg-[#2d2b28]/30"
        onClick={onClose}
      />
      <aside className="absolute right-0 top-0 flex h-full w-full max-w-2xl flex-col overflow-y-auto border-l border-[#dedbd2] bg-[#fbfaf7] shadow-2xl">
        <div className="sticky top-0 z-10 border-b border-[#dedbd2] bg-white p-4">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase text-[#0f766e]">Dashboard guide</p>
              <h2 className="mt-1 text-2xl font-semibold tracking-normal text-[#171717]">
                Lead Magnet quick walkthrough
              </h2>
            </div>
            <button
              type="button"
              aria-label="Close"
              className="rounded-md border border-[#dedbd2] bg-white p-2 text-[#66635f] hover:bg-[#f3f1ea] hover:text-[#2d2b28]"
              onClick={onClose}
            >
              <X className="h-4 w-4" aria-hidden />
            </button>
          </div>
        </div>

        <div className="p-4">
          <LeadMagnetsGuideContent mode="window" />
        </div>
      </aside>
    </div>
  );
}

export function LeadMagnetsGuideContent({ mode = "page" }: LeadMagnetsGuideProps) {
  return (
    <div className="space-y-4">
      {mode === "page" ? <GuideHero /> : <GuideWindowIntro />}

      <QuickStart />

      <section className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
        <section className="rounded-2xl border border-[#dedbd2] bg-[#fffdf8] p-4 shadow-sm">
          <div className="text-[11px] font-semibold uppercase text-[#0f766e]">Use this when</div>
          <div className="mt-4 grid gap-2">
            {questionDefinitions.map((question) => (
              <article key={question.label} className="rounded-xl border border-[#ece9e1] bg-white p-3">
                <div className="text-sm font-semibold text-[#2d2b28]">{question.label}</div>
                <p className="mt-1 text-xs leading-5 text-[#66635f]">{question.value}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="rounded-2xl border border-[#dedbd2] bg-white p-4 shadow-sm">
          <div className="text-[11px] font-semibold uppercase text-[#0f766e]">5-minute flow</div>
          <ol className="mt-4 flex flex-wrap gap-2">
            {weeklyWorkflow.map((step, index) => (
              <li key={step} className="inline-flex items-center gap-2 rounded-full border border-[#ece9e1] bg-[#fbfaf7] px-3 py-2">
                <span className="flex h-5 w-5 items-center justify-center rounded-full bg-[#0f766e] text-[10px] font-semibold text-white">
                  {index + 1}
                </span>
                <span className="text-xs font-semibold text-[#3b3936]">{step}</span>
              </li>
            ))}
          </ol>
        </section>
      </section>

      <SignalCards />

      <ReferenceField />

      <section className="rounded-2xl border border-[#99d4cb] bg-[#f0fdfa] p-4 shadow-sm">
        <div className="text-[11px] font-semibold uppercase text-[#115e59]">Attribution rule</div>
        <p className="mt-2 text-sm leading-6 text-[#3b3936]">
          Influence and final credit are intentionally separated. A magnet can influence a buyer even when a later sales pipeline, launch, or other motion receives latest-touch credit. That is why this dashboard treats buyer influence, direct credit, and sales handoff as different signals.
        </p>
      </section>
    </div>
  );
}

function GuideHero() {
  return (
    <section className="overflow-hidden rounded-2xl border border-[#2d2b28] bg-[#2d2b28] shadow-sm">
      <div className="grid lg:grid-cols-[minmax(0,1.1fr)_22rem]">
        <div className="p-5 text-white sm:p-6">
          <Link
            href="/lead-magnets"
            className="inline-flex items-center gap-2 rounded-md border border-[#5b564f] bg-[#3b3936] px-3 py-2 text-xs font-semibold text-[#f7f7f4] transition hover:bg-[#4b4742] focus:outline-none focus:ring-2 focus:ring-[#99f6e4]"
          >
            <ArrowLeft className="h-3.5 w-3.5" aria-hidden />
            Back to dashboard
          </Link>
          <div className="mt-8 max-w-3xl">
            <div className="text-[11px] font-semibold uppercase text-[#99f6e4]">RevOps field guide</div>
            <h1 className="mt-3 text-4xl font-semibold tracking-normal text-white">
              Lead Magnet Dashboard Guide
            </h1>
            <p className="mt-3 max-w-2xl text-sm leading-6 text-[#d8d4cc]">
              A practical playbook for deciding what to update, repeat, create, monitor, or investigate without opening the data model.
            </p>
          </div>
        </div>

        <div className="border-t border-[#4b4742] bg-[#171717] p-5 text-white lg:border-l lg:border-t-0">
          <div className="text-[11px] font-semibold uppercase text-[#99f6e4]">Default posture</div>
          <div className="mt-4 space-y-3">
            <HeroRule label="Start" value="Priority 10" />
            <HeroRule label="Park" value="Bench" />
            <HeroRule label="Inspect" value="Audit All" />
            <HeroRule label="Decide" value="Open the row drawer" />
          </div>
        </div>
      </div>
    </section>
  );
}

function GuideWindowIntro() {
  return (
    <section className="overflow-hidden rounded-2xl border border-[#2d2b28] bg-[#2d2b28] text-white shadow-sm">
      <div className="p-4">
        <div className="text-[11px] font-semibold uppercase text-[#99f6e4]">Quick playbook</div>
        <p className="mt-2 text-sm leading-6 text-[#d8d4cc]">
          Use this when you need to decide which magnets deserve action now, which should stay on the bench, and which need attribution review.
        </p>
        <Link
          href="/lead-magnets/guide"
          className="mt-4 inline-flex items-center gap-2 rounded-md border border-[#5b564f] bg-[#3b3936] px-3 py-2 text-xs font-semibold text-[#f7f7f4] transition hover:bg-[#4b4742] focus:outline-none focus:ring-2 focus:ring-[#99f6e4]"
        >
          Open full guide
          <ExternalLink className="h-3.5 w-3.5" aria-hidden />
        </Link>
      </div>
    </section>
  );
}

function HeroRule({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-[#3b3936] bg-[#23211f] p-3">
      <div className="text-[10px] font-semibold uppercase text-[#8a857d]">{label}</div>
      <div className="mt-1 text-sm font-semibold text-white">{value}</div>
    </div>
  );
}

function QuickStart() {
  return (
    <section className="rounded-2xl border border-[#2d2b28] bg-[#171717] p-4 text-white shadow-sm">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="max-w-md">
          <div className="text-[11px] font-semibold uppercase text-[#99f6e4]">How it works</div>
          <h2 className="mt-1 text-xl font-semibold tracking-normal">Keep it simple</h2>
          <p className="mt-2 text-sm leading-6 text-[#d8d4cc]">
            The guide is a three-step loop: start with the priority list, inspect the row, make the next decision.
          </p>
        </div>

        <div className="grid gap-2 sm:grid-cols-3 lg:min-w-[32rem]">
          {walkthroughSteps.map((step, index) => (
            <article key={step.title} className="rounded-xl border border-[#3b3936] bg-[#23211f] p-3">
              <div className="flex items-center gap-2">
                <span className="flex h-6 w-6 items-center justify-center rounded-full bg-[#0f766e] text-[10px] font-semibold text-white">
                  {index + 1}
                </span>
                <h3 className="text-sm font-semibold text-white">{step.title}</h3>
              </div>
              <p className="mt-2 text-xs leading-5 text-[#d8d4cc]">{step.body}</p>
            </article>
          ))}
        </div>
      </div>
    </section>
  );
}

function SignalCards() {
  return (
    <section className="rounded-2xl border border-[#dedbd2] bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="text-[11px] font-semibold uppercase text-[#0f766e]">Signal translation</div>
          <h2 className="mt-1 text-lg font-semibold tracking-normal text-[#2d2b28]">
            Signal guide
          </h2>
        </div>
        <div className="rounded-md border border-[#dedbd2] bg-[#fbfaf7] px-2 py-1 text-xs font-semibold text-[#3b3936]">
          read to next move
        </div>
      </div>

      <div className="mt-4 grid gap-2 md:grid-cols-5">
        {signalTranslations.map((item, index) => {
          return (
            <article
              key={item.signal}
              className="rounded-xl border border-[#ece9e1] bg-[#fbfaf7] p-3"
            >
              <div className="flex items-center justify-between gap-3">
                <div className="text-sm font-semibold text-[#2d2b28]">{item.signal}</div>
                <span className="rounded-full border border-[#dedbd2] bg-white px-2 py-0.5 font-mono text-[10px] text-[#66635f]">
                  {index + 1}
                </span>
              </div>
              <p className="mt-2 text-xs leading-5 text-[#66635f]">{item.read}</p>
              <div className="mt-3 rounded-lg border border-[#dedbd2] bg-white p-2 text-xs leading-5 text-[#3b3936]">
                <span className="font-semibold text-[#0f766e]">Next: </span>
                {item.next}
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}

function ReferenceField() {
  return (
    <section className="rounded-2xl border border-[#dedbd2] bg-[#fffdf8] p-4 shadow-sm">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="text-[11px] font-semibold uppercase text-[#0f766e]">Pocket reference</div>
          <h2 className="mt-1 text-xl font-semibold tracking-normal text-[#2d2b28]">
            Terms you might check while operating
          </h2>
        </div>
        <div className="rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold text-[#3b3936]">
          not required reading
        </div>
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-3">
        {referenceGroups.map((group) => (
          <details
            key={group.title}
            className="rounded-2xl border border-[#ece9e1] bg-white p-4 open:bg-[#fbfaf7]"
          >
            <summary className="cursor-pointer list-none">
              <div className="text-[10px] font-semibold uppercase text-[#8a857d]">
                {group.eyebrow}
              </div>
              <div className="mt-1 text-base font-semibold text-[#2d2b28]">{group.title}</div>
            </summary>
            <div className="mt-4 flex flex-wrap gap-2">
              {group.items.map((item) => (
                <span
                  key={item.label}
                  title={item.value}
                  className="rounded-full border border-[#dedbd2] bg-white px-3 py-1.5 text-xs font-semibold text-[#3b3936]"
                >
                  {item.label}
                </span>
              ))}
            </div>
            <div className="mt-4 space-y-3">
              {group.items.map((item) => (
                <div key={item.label} className="rounded-xl bg-white p-3">
                  <div className="text-sm font-semibold text-[#2d2b28]">{item.label}</div>
                  <p className="mt-1 text-xs leading-5 text-[#66635f]">{item.value}</p>
                </div>
              ))}
            </div>
          </details>
        ))}
      </div>
    </section>
  );
}
