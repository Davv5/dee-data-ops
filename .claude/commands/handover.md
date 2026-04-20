Create a session handover markdown file for this repository.

Requirements:
1. Determine the current git branch name.
2. Determine current local timestamp in `YYYY-MM-DD_HH-mm` format.
3. Create the file at:
   `docs/handovers/<branch>-<timestamp>.md`
4. Use `docs/handovers/TEMPLATE.md` as the structure.
5. Fill every section with concrete details from this session only.
6. Include exact file paths changed, run IDs, PR links, and unresolved risks.
7. Keep language concise and factual.

Quality bar:
- No vague summaries.
- No missing next steps.
- Must include "First task for next session" as a single actionable item.

After writing the file:
- Print the created file path.
- Print a 5-line executive summary.
