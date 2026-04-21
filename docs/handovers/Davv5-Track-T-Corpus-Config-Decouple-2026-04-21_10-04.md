# Session Handover — Track T: corpus config decouple (portability for future clients)

**Branch:** `Davv5/Track-T-Corpus-Config-Decouple`
**Timestamp:** `2026-04-21_10-04` (authored by plan-architect; not yet executed)
**Author:** plan-architect (Claude Opus 4.7) — pre-execution plan
**PR:** pending

---

## Session goal

Decouple the `ask-corpus` skill from hardcoded D-DEE notebook IDs so the skill + the three agents (architect/executor/reviewer) become portable to any future PS engagement. A per-project `.claude/corpus.yaml` declares which NotebookLM notebooks this project uses and for what purpose (methodology vs engagement). The skill reads the config and queries the right notebook(s).

## Changed files (expected)

```
.claude/corpus.yaml                    — created — project-level corpus declaration
.claude/skills/ask-corpus/SKILL.md     — edited — read from corpus.yaml instead of hardcoded IDs
.claude/rules/using-the-notebook.md    — edited — document the corpus.yaml pattern
CLAUDE.md                              — edited — add "Corpus config" section pointing at corpus.yaml
WORKLOG.md                             — edited — dated entry
```

## Tasks

- [ ] Read current `.claude/skills/ask-corpus/SKILL.md` to identify where the notebook ID is hardcoded
- [ ] Design the `.claude/corpus.yaml` schema. Proposed shape:
      ```yaml
      # .claude/corpus.yaml — declares the NotebookLM corpora this project uses
      methodology:
        notebook_id: 7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a
        name: Data Ops
        purpose: |
          Portable craft knowledge — dbt conventions, modeling patterns,
          CI/CD, MDS starter guides. Query for "how should I structure X?"
          questions. Clean; safe to carry to the next client.
      engagement:
        notebook_id: 741d85c6-39a7-4612-af7c-cca65043cf19
        name: D-DEE Engagement Memory
        purpose: |
          This engagement's history + oracle + scope docs. Query for
          "what did D-DEE decide about Y?" Disposable when engagement ends.
      ```
- [ ] Update `ask-corpus/SKILL.md` to read `corpus.yaml` at project root. Pseudocode:
      - If the caller passes `scope: methodology|engagement|cross`, query the corresponding notebook (or `cross_notebook_query` for cross)
      - If no scope, default to methodology (portable knowledge first)
- [ ] Update `.claude/rules/using-the-notebook.md` to document:
      - How to read `corpus.yaml` to know which notebooks are available
      - When to use `scope: methodology` vs `scope: engagement` vs no scope
      - How to swap corpora for a new client (edit the yaml, swap notebook IDs)
- [ ] Update `CLAUDE.md` to add a one-paragraph "Corpus config" section pointing at corpus.yaml as the portable mechanism
- [ ] Invoke `ask-corpus` (against the current hardcoded notebook) with a test question to confirm the skill still works end-to-end after the refactor
- [ ] Invoke `ask-corpus scope: engagement` with another test question to confirm the scope routing works
- [ ] Append WORKLOG entry
- [ ] Run `/handover`
- [ ] Commit locally

## Decisions already made

- **Two notebook slots: methodology + engagement.** Mirrors the CLAUDE.md / CLAUDE.local.md boundary. Additional slots can be added post-v1 if a client corpus grows past two.
- **`.claude/corpus.yaml` is gitignored unless the notebook IDs are safe to publish.** Notebook IDs are not secret but are engagement-identifying — the default ignored-when-client-specific pattern keeps the template clean when forked. For D-DEE specifically, commit it (public repo is fine per AGPL).
- **Skill defaults to methodology scope when no scope is passed.** Safer default — methodology is portable, engagement is disposable.
- **Do NOT rename the `ask-corpus` skill.** Keep the name; only change its internals.

## Open questions

- Does the existing `ask-corpus` skill take a `scope` parameter today? If not, adding one needs minor user-facing doc updates. **Pick sensible default**: add the parameter, document it as optional, update the rule file.
- For a future client corpus, do they need access to the Data Ops methodology notebook? Yes (it's portable craft) — same notebook_id in both corpus.yaml files. No migration needed per-client.

## Done when

- `.claude/corpus.yaml` exists and declares both D-DEE notebooks
- `.claude/skills/ask-corpus/SKILL.md` reads from the yaml, not hardcoded IDs
- `.claude/rules/using-the-notebook.md` documents the pattern
- `CLAUDE.md` mentions the corpus config
- Two test queries (methodology + engagement) both succeed from a fresh session
- WORKLOG entry with the two test queries' response summaries
- Commit sits locally

## Context links

- `.claude/skills/ask-corpus/SKILL.md` — the skill being refactored
- `.claude/rules/using-the-notebook.md` — the rule being updated
- Data Ops notebook: `7c7cd5d4-22df-4ef0-8b74-ed87e0ca4e6a`
- D-DEE Engagement Memory notebook: `741d85c6-39a7-4612-af7c-cca65043cf19`
- Motivation in plan: `/Users/david/.claude/plans/this-is-a-sorted-rabbit.md` — template-ability section
