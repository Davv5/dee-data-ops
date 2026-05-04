"use client";

import { useState } from "react";

type ContractTermsReviewFormProps = {
  contactSk: string | null;
  evidenceText?: string | null;
  mentionedAmountsText?: string | null;
  largestMentionedAmount?: unknown;
};

export function ContractTermsReviewForm({
  contactSk,
  evidenceText,
  mentionedAmountsText,
  largestMentionedAmount,
}: ContractTermsReviewFormProps) {
  const [promisedContractValue, setPromisedContractValue] = useState("");
  const [upfrontAgreedAmount, setUpfrontAgreedAmount] = useState("");
  const [balanceExpectedAmount, setBalanceExpectedAmount] = useState("");
  const [reviewConfidence, setReviewConfidence] = useState("medium");
  const [termsSourceNote, setTermsSourceNote] = useState("");
  const [pendingAction, setPendingAction] = useState<"confirm" | "skip" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const isDisabled = !contactSk || pendingAction !== null;
  const amountHint = formatAmountHint(largestMentionedAmount, mentionedAmountsText);

  async function confirmTerms() {
    if (!contactSk) return;

    setError(null);
    setPendingAction("confirm");

    try {
      const response = await fetch("/api/retention/contract-terms", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          contactSk,
          promisedContractValue,
          upfrontAgreedAmount,
          balanceExpectedAmount,
          reviewConfidence,
          termsSourceNote,
          reviewedBy: "dashboard",
        }),
      });

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        throw new Error(payload?.error ?? "Could not confirm contract terms");
      }

      window.location.reload();
    } catch (reviewError) {
      setError(reviewError instanceof Error ? reviewError.message : "Could not confirm contract terms");
    } finally {
      setPendingAction(null);
    }
  }

  async function skipReview() {
    if (!contactSk) return;

    setError(null);
    setPendingAction("skip");

    try {
      const response = await fetch("/api/retention/action-reviews", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          contactSk,
          actionBucket: "contract_terms_review",
          reviewStatus: "wont_fix",
          reviewNote: termsSourceNote.trim() || "Contract terms review skipped.",
          reviewedBy: "dashboard",
        }),
      });

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        throw new Error(payload?.error ?? "Could not skip contract review");
      }

      window.location.reload();
    } catch (reviewError) {
      setError(reviewError instanceof Error ? reviewError.message : "Could not skip contract review");
    } finally {
      setPendingAction(null);
    }
  }

  return (
    <div className="mt-3 w-full rounded-md border border-[#99f6e4] bg-[#f8fffd] p-3">
      <div className="flex flex-col gap-1 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="text-xs font-semibold text-[#0f766e]">Confirm Contract Terms</div>
          {amountHint ? <div className="mt-0.5 text-[11px] text-[#66635f]">{amountHint}</div> : null}
        </div>
        <select
          value={reviewConfidence}
          onChange={(event) => setReviewConfidence(event.target.value)}
          disabled={isDisabled}
          className="w-fit rounded-md border border-[#dedbd2] bg-white px-2 py-1 text-xs font-semibold text-[#2d2b28] outline-none focus:border-[#0f766e] disabled:cursor-not-allowed disabled:opacity-50"
        >
          <option value="high">High confidence</option>
          <option value="medium">Medium confidence</option>
          <option value="low">Low confidence</option>
        </select>
      </div>

      {evidenceText ? (
        <div className="mt-2 line-clamp-3 rounded-md border border-[#dedbd2] bg-white p-2 text-[11px] leading-5 text-[#66635f]">
          {evidenceText}
        </div>
      ) : null}

      <div className="mt-3 grid gap-2 sm:grid-cols-3">
        <MoneyInput
          label="promised"
          value={promisedContractValue}
          onChange={setPromisedContractValue}
          placeholder="2000"
          disabled={isDisabled}
        />
        <MoneyInput
          label="upfront"
          value={upfrontAgreedAmount}
          onChange={setUpfrontAgreedAmount}
          placeholder="500"
          disabled={isDisabled}
        />
        <MoneyInput
          label="balance"
          value={balanceExpectedAmount}
          onChange={setBalanceExpectedAmount}
          placeholder="1500"
          disabled={isDisabled}
        />
      </div>

      <textarea
        value={termsSourceNote}
        onChange={(event) => setTermsSourceNote(event.target.value)}
        placeholder="Source note"
        disabled={isDisabled}
        className="mt-2 min-h-16 w-full rounded-md border border-[#dedbd2] bg-white px-2 py-1.5 text-xs text-[#2d2b28] outline-none placeholder:text-[#9a9791] focus:border-[#0f766e] disabled:cursor-not-allowed disabled:opacity-50"
      />

      <div className="mt-2 flex flex-wrap gap-1.5">
        <button
          type="button"
          disabled={isDisabled}
          onClick={confirmTerms}
          className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2.5 py-1.5 text-[11px] font-semibold text-[#166534] disabled:cursor-not-allowed disabled:opacity-50"
        >
          {pendingAction === "confirm" ? "Saving" : "Confirm Terms"}
        </button>
        <button
          type="button"
          disabled={isDisabled}
          onClick={skipReview}
          className="rounded-md border border-[#dedbd2] bg-white px-2.5 py-1.5 text-[11px] font-semibold text-[#3b3936] disabled:cursor-not-allowed disabled:opacity-50"
        >
          {pendingAction === "skip" ? "Saving" : "Skip"}
        </button>
      </div>

      {error ? <div className="mt-2 text-[11px] font-semibold text-[#991b1b]">{error}</div> : null}
    </div>
  );
}

function MoneyInput({
  label,
  value,
  onChange,
  placeholder,
  disabled,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder: string;
  disabled: boolean;
}) {
  return (
    <label className="block">
      <span className="text-[10px] font-semibold uppercase text-[#66635f]">{label}</span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        inputMode="decimal"
        placeholder={placeholder}
        disabled={disabled}
        className="mt-1 w-full rounded-md border border-[#dedbd2] bg-white px-2 py-1.5 text-xs font-semibold text-[#2d2b28] outline-none placeholder:text-[#9a9791] focus:border-[#0f766e] disabled:cursor-not-allowed disabled:opacity-50"
      />
    </label>
  );
}

function formatAmountHint(largestMentionedAmount?: unknown, mentionedAmountsText?: string | null) {
  const amount = typeof largestMentionedAmount === "number" ? largestMentionedAmount : Number(largestMentionedAmount);

  if (Number.isFinite(amount) && amount > 0) {
    return `Largest transcript amount: ${new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: "USD",
      maximumFractionDigits: 0,
    }).format(amount)}${mentionedAmountsText ? ` · said: ${mentionedAmountsText}` : ""}`;
  }

  return mentionedAmountsText ? `Transcript amounts: ${mentionedAmountsText}` : null;
}
