"use client";

import { useState } from "react";

type OperatorActionReviewButtonsProps = {
  endpoint: string;
  contactSk: string | null;
  actionBucket: string | null;
  fixedNote: string;
  ignoreNote: string;
  fixedLabel?: string;
  ignoreLabel?: string;
  notePlaceholder?: string;
};

export function OperatorActionReviewButtons({
  endpoint,
  contactSk,
  actionBucket,
  fixedNote,
  ignoreNote,
  fixedLabel = "Fixed",
  ignoreLabel = "Ignore",
  notePlaceholder,
}: OperatorActionReviewButtonsProps) {
  const [pendingStatus, setPendingStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [reviewNote, setReviewNote] = useState("");
  const isDisabled = !contactSk || !actionBucket || pendingStatus !== null;

  async function submitReview(reviewStatus: "fixed" | "wont_fix") {
    if (!contactSk || !actionBucket) return;

    setError(null);
    setPendingStatus(reviewStatus);

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          contactSk,
          actionBucket,
          reviewStatus,
          reviewNote: reviewNote.trim() || (reviewStatus === "fixed" ? fixedNote : ignoreNote),
          reviewedBy: "dashboard",
        }),
      });

      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        throw new Error(payload?.error ?? "Could not update review status");
      }

      window.location.reload();
    } catch (reviewError) {
      setError(reviewError instanceof Error ? reviewError.message : "Could not update review status");
    } finally {
      setPendingStatus(null);
    }
  }

  return (
    <div className="mt-2">
      {notePlaceholder ? (
        <textarea
          value={reviewNote}
          onChange={(event) => setReviewNote(event.target.value)}
          placeholder={notePlaceholder}
          disabled={pendingStatus !== null}
          className="mb-2 min-h-20 w-full rounded-md border border-[#dedbd2] bg-white px-2 py-1.5 text-xs text-[#2d2b28] outline-none placeholder:text-[#9a9791] focus:border-[#0f766e] disabled:cursor-not-allowed disabled:opacity-50"
        />
      ) : null}
      <div className="flex gap-1">
        <button
          type="button"
          disabled={isDisabled}
          onClick={() => submitReview("fixed")}
          className="rounded-md border border-[#bbf7d0] bg-[#f0fdf4] px-2 py-1 text-[11px] font-semibold text-[#166534] disabled:cursor-not-allowed disabled:opacity-50"
        >
          {pendingStatus === "fixed" ? "Saving" : fixedLabel}
        </button>
        <button
          type="button"
          disabled={isDisabled}
          onClick={() => submitReview("wont_fix")}
          className="rounded-md border border-[#dedbd2] bg-[#f7f7f4] px-2 py-1 text-[11px] font-semibold text-[#3b3936] disabled:cursor-not-allowed disabled:opacity-50"
        >
          {pendingStatus === "wont_fix" ? "Saving" : ignoreLabel}
        </button>
      </div>
      {error ? <div className="mt-1 text-[10px] font-medium text-[#991b1b]">{error}</div> : null}
    </div>
  );
}
