#!/usr/bin/env bash
# Audit Orca/git worktrees and local branches without changing anything.
# Use this before cleanup so dirty work and unmerged branches are visible.

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
BASE="${BASE:-origin/main}"
FETCH=0

if [[ "${1:-}" == "--fetch" ]]; then
  FETCH=1
fi

cd "$ROOT"

if [[ "$FETCH" == "1" ]]; then
  git fetch --prune origin
fi

printf '# Orca / git worktree audit\n\n'
printf 'Repo: %s\n' "$ROOT"
printf 'Base: %s\n' "$BASE"
printf 'Generated: %s\n\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

printf '## Current workspace\n\n'
git status --short --branch
printf '\n'

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

emit_record() {
  local path="$1"
  local branch="$2"
  local head="$3"

  [[ -n "$path" ]] || return 0

  local display_branch="${branch#refs/heads/}"
  [[ -n "$display_branch" ]] || display_branch="(detached)"

  local dirty_count
  dirty_count="$(git -C "$path" status --porcelain | wc -l | tr -d ' ')"

  local upstream="-"
  local left="-"
  local right="-"
  if [[ "$display_branch" != "(detached)" ]]; then
    if git rev-parse --verify --quiet "${display_branch}@{upstream}" >/dev/null; then
      upstream="$(git rev-parse --abbrev-ref "${display_branch}@{upstream}")"
      if ! read -r left right < <(git rev-list --left-right --count "${upstream}...${display_branch}" 2>/dev/null); then
        left="-"
        right="-"
      fi
    else
      upstream="(none)"
    fi
  fi

  local merged="unknown"
  if [[ "$display_branch" != "(detached)" ]] && git rev-parse --verify "$BASE" >/dev/null 2>&1; then
    if git merge-base --is-ancestor "$display_branch" "$BASE" 2>/dev/null; then
      merged="yes"
    else
      merged="no"
    fi
  fi

  local last_commit
  last_commit="$(git -C "$path" log -1 --format='%ci %s' 2>/dev/null || true)"

  local bucket=3
  if [[ "$dirty_count" != "0" ]]; then
    bucket=0
  elif [[ "$merged" == "no" ]]; then
    bucket=1
  elif [[ "$upstream" == *": gone]"* || "$upstream" == "(none)" ]]; then
    bucket=2
  fi

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$bucket" "$display_branch" "$dirty_count" "$merged" "$upstream" "$left" "$right" "$path" "$last_commit" >> "$tmp"
}

path=""
branch=""
head=""
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ -z "$line" ]]; then
    emit_record "$path" "$branch" "$head"
    path=""
    branch=""
    head=""
    continue
  fi

  case "$line" in
    worktree\ *) path="${line#worktree }" ;;
    HEAD\ *) head="${line#HEAD }" ;;
    branch\ *) branch="${line#branch }" ;;
  esac
done < <(git worktree list --porcelain)
emit_record "$path" "$branch" "$head"

printf '## Worktrees, ordered by cleanup risk\n\n'
printf 'Order: dirty first, then clean-but-unmerged, then clean cleanup candidates.\n\n'

sort -k1,1n -k2,2 "$tmp" | while IFS=$'\t' read -r bucket branch dirty merged upstream behind ahead path last_commit; do
  [[ "$bucket" =~ ^[0-9]+$ ]] || continue
  printf -- '- `%s`\n' "$branch"
  printf '  path: `%s`\n' "$path"
  printf '  dirty files: %s | merged into %s: %s | upstream: %s | behind/ahead upstream: %s/%s\n' \
    "$dirty" "$BASE" "$merged" "$upstream" "$behind" "$ahead"
  printf '  last: %s\n\n' "$last_commit"
done

printf '## Local branches whose upstream is gone\n\n'
git branch -vv | awk '/: gone]/{sub(/^[*+ ]+/, "", $0); print "- `" $1 "`"}' || true
printf '\n'

printf '## Cleanup command templates\n\n'
printf 'Inspect first:\n'
printf '  git -C "<worktree-path>" status --short --branch\n\n'
printf 'Remove a clean obsolete worktree, then branch:\n'
printf '  git worktree remove "<worktree-path>"\n'
printf '  git branch -d "<branch-name>"\n\n'
printf 'Only after confirming no unique commits remain:\n'
printf '  git branch -D "<branch-name>"\n'
