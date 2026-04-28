#!/usr/bin/env bash
# sessionstart-inject-state.sh
# Emit the curated project-state index as SessionStart additionalContext for Claude Code,
# prefixed with a drift warning if any local feature branches are ahead of origin/main
# without an open or merged PR. Replaces the earlier tail-of-WORKLOG.md injection.
#
# Drift warning rationale: end-of-session WORKLOG entries can describe work that landed
# on a feature branch but never got PR'd. Without a SessionStart guardrail, the next
# session inherits the WORKLOG entry as truth and assumes the feature is on main when
# in fact it isn't. This block scans local branches and surfaces unmerged work loudly,
# so the user notices before starting unrelated work.
#
# The state file lives at `.claude/state/project-state.md` and is hand-regenerated at
# the end of a session via the `/worklog` skill. WORKLOG.md remains the append-only
# audit log; agents `Grep WORKLOG.md` on demand.

set -u

REPO="/Users/david/Documents/data ops"
STATE="$REPO/.claude/state/project-state.md"

# ---------------------------------------------------------------------------
# Drift detection — list local branches ahead of origin/main with no MERGED PR.
#
# Best-effort: if `gh` or network is unavailable, the warning block is omitted
# rather than blocking SessionStart. `git fetch` is run quietly with a short
# deadline so a slow network can't stall the hook.

drift_block=""

if command -v gh >/dev/null 2>&1 && command -v jq >/dev/null 2>&1; then
  cd "$REPO" 2>/dev/null && {
    # 5-second fetch budget; ignore failure (offline session is fine).
    timeout 5 git fetch origin main --quiet 2>/dev/null || true

    drift_lines=""
    while IFS= read -r branch; do
      case "$branch" in main|master|"") continue ;; esac
      ahead=$(git rev-list --count "origin/main..$branch" 2>/dev/null || echo 0)
      [ "$ahead" -gt 0 ] || continue

      # Cap per-call budget — if more than 20 branches exist, the project has
      # bigger problems than a SessionStart warning can solve.
      pr_json=$(gh pr list --head "$branch" --state all --json state,number,url --limit 1 2>/dev/null || echo '[]')
      pr_state=$(printf '%s' "$pr_json" | jq -r '.[0].state // "NONE"')
      pr_number=$(printf '%s' "$pr_json" | jq -r '.[0].number // ""')

      case "$pr_state" in
        MERGED)
          # Branch is ahead but PR merged — usually means the local branch
          # wasn't deleted post-merge. Don't warn; this is benign cleanup.
          continue
          ;;
        OPEN)
          drift_lines="${drift_lines}- ⚠️ \`$branch\` ahead of \`origin/main\` by $ahead commits — **PR #$pr_number is OPEN** (chase merge)\n"
          ;;
        NONE|*)
          drift_lines="${drift_lines}- 🚨 \`$branch\` ahead of \`origin/main\` by $ahead commits — **no PR open** (run \`gh pr create\` or merge to main)\n"
          ;;
      esac
    done < <(git for-each-ref --format='%(refname:short)' refs/heads/ 2>/dev/null | head -20)

    if [ -n "$drift_lines" ]; then
      drift_block=$(printf "## ⚠️ Branch drift — unmerged feature work\n\nThe following local branches are ahead of \`origin/main\`. Confirm before starting unrelated work:\n\n%b\nSurfaced by \`.claude/scripts/sessionstart-inject-state.sh\`. To suppress, merge or close the relevant PRs.\n\n---\n\n" "$drift_lines")
    fi
  }
fi

# ---------------------------------------------------------------------------
# Compose final additionalContext: drift_block (if any) + project-state body.

if [ -f "$STATE" ]; then
  state_body=$(cat "$STATE")
  # %b expands escape sequences in drift_block; %s%s preserves the project-
  # state body verbatim. The double \n\n between drift_block and state body
  # survives `$()` stripping because we re-emit it explicitly here.
  full_context=$(printf '%b\n\n%s' "$drift_block" "$state_body")
  jq -n --arg ctx "$full_context" \
    '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: $ctx}}'
else
  fallback=$(printf '%b\n\n%s' "$drift_block" "No .claude/state/project-state.md yet — regenerate it from WORKLOG.md per \`.claude/rules/worklog.md\`.")
  jq -n --arg ctx "$fallback" \
    '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: $ctx}}'
fi
