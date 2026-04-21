#!/usr/bin/env bash
# sessionstart-inject-state.sh
# Emit the curated project-state index as SessionStart additionalContext for Claude Code.
# Replaces the earlier tail-of-WORKLOG.md injection: keeps context bounded as the
# project ages, mirrors the retrieval-over-injection pattern used by `.claude/rules/*.md`
# (see the `paths:` frontmatter: rules load only when a matching file is opened, not
# pre-loaded every turn).
#
# The index lives at `.claude/state/project-state.md` and is hand-regenerated at the
# end of a session (see `.claude/rules/worklog.md` for the regeneration command).
# WORKLOG.md remains the append-only audit log; agents `Grep WORKLOG.md` on demand.

set -u

STATE="/Users/david/Documents/data ops/.claude/state/project-state.md"

if [ -f "$STATE" ]; then
  content=$(cat "$STATE")
  jq -n --arg ctx "$content" '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: ("# Project state index\n\n" + $ctx)}}'
else
  jq -n '{hookSpecificOutput: {hookEventName: "SessionStart", additionalContext: "No .claude/state/project-state.md yet — regenerate it from WORKLOG.md per `.claude/rules/worklog.md`."}}'
fi
