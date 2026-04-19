#!/usr/bin/env bash
# sync-rule-to-notebook.sh
# Upsert a .claude/rules/*.md file as a text source in the Data Ops NotebookLM notebook.
# Called by the PostToolUse hook on Write|Edit (see ../settings.json).
# Reads the Claude Code hook payload (JSON) on stdin; extracts the edited file path;
# no-ops unless the path is inside .claude/rules/ and ends in .md.
#
# Behavior: find any existing source with a matching title (e.g. ".claude/rules/mart-naming.md"),
# delete it, then add the current file contents as a fresh text source. This keeps the corpus
# aligned with the repo without accumulating duplicate sources on every edit.

set -u

NOTEBOOK_ID="7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a"
LOG="/tmp/dataops-sync-rule.log"

payload=$(cat)
file=$(printf '%s' "$payload" | jq -r '.tool_response.filePath // .tool_input.file_path // empty')

case "$file" in
  */.claude/rules/*.md) ;;
  *) exit 0 ;;
esac

[ -f "$file" ] || { echo "$(date -Iseconds) skip: $file not on disk" >>"$LOG"; exit 0; }

title=".claude/rules/$(basename "$file")"
ts=$(date -Iseconds)

ids=$(nlm source list "$NOTEBOOK_ID" -j 2>>"$LOG" | jq -r --arg t "$title" '.[] | select(.title==$t) | .id')
if [ -n "$ids" ]; then
  # shellcheck disable=SC2086
  nlm source delete $ids -y >>"$LOG" 2>&1 || echo "$ts delete failed for $title" >>"$LOG"
fi

if nlm source add "$NOTEBOOK_ID" --text "$(cat "$file")" --title "$title" >>"$LOG" 2>&1; then
  echo "$ts synced: $title" >>"$LOG"
else
  echo "$ts add failed: $title" >>"$LOG"
fi

exit 0
