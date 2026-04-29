#!/usr/bin/env bash
# post-pivot-memory-reminder.sh
# Fires after Write|Edit. No-ops unless the target path is a memory file under
# this project's auto-memory directory. When it fires, emits a one-line reminder
# to walk the .claude/rules/pivot-discipline.md checklist if the memory captures
# a strategic pivot.
#
# Discoverability hook for the convention rule. The rule is judgment-dependent
# (Claude decides whether the memory is a pivot); this hook just ensures the
# rule fires into Claude's awareness at the right moment — at memory-write
# time, when the trigger event happens. Doesn't block, doesn't mutate.

set -u

MEMORY_DIR_GLOB="*/.claude/projects/-Users-david-Documents-data-ops/memory/*.md"
LOG="/tmp/dataops-pivot-reminder.log"

payload=$(cat)
file=$(printf '%s' "$payload" | jq -r '.tool_response.filePath // .tool_input.file_path // empty')

case "$file" in
  $MEMORY_DIR_GLOB) ;;
  *) exit 0 ;;
esac

# Skip the MEMORY.md index file itself — only fire on per-fact memory writes.
case "$(basename "$file")" in
  MEMORY.md) exit 0 ;;
esac

ts=$(date -Iseconds)
name=$(basename "$file" .md)

cat >&2 <<EOF
[pivot-discipline reminder] Memory file written: $name

If this captures a STRATEGIC PIVOT (BI direction change, source-of-truth
change, scope cut, work-bucket pause/resume, supersession of a prior
memory), the SAME SESSION must walk the .claude/rules/pivot-discipline.md
checklist and update or banner-archive the docs the pivot supersedes:

  - CLAUDE.local.md "Engagement at a glance" / "Stack decisions" / "Current status"
  - .claude/rules/*.md files referencing the superseded term (grep first)
  - docs/plans/*.md (active and historical) — stale-flag affected steps
  - Scope docs (client_v1_*.md, *_build_plan.md) — banner-archive if shipped
  - .claude/state/project-state.md — regenerate
  - MEMORY.md index — mark supersession in the older entry's hook

If a same-session walk isn't feasible, surface "pivot-debt" as an Open
Thread in project-state.md per the rule's escape-valve clause.

If this is NOT a pivot (just a routine fact / role / pointer), no walk
required — disregard.
EOF

echo "$ts $name (pivot-reminder fired)" >>"$LOG" 2>/dev/null || true

exit 0
