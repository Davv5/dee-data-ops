# dee-data-ops

Data engineering monorepo for the D-DEE speed-to-lead program. dbt + BigQuery, custom Python extractors for GHL + Fanbasis, Fivetran for Typeform / Calendly / Stripe.

See [`CLAUDE.md`](CLAUDE.md) for project structure, setup steps, and AI-assisted development conventions.

## Docs

- **dbt docs (prod):** https://davv5.github.io/dee-data-ops/ — auto-published from `main` by [`.github/workflows/dbt-docs.yml`](.github/workflows/dbt-docs.yml) after every push that touches `dbt/**`.
- **Style guide:** [`dbt_style_guide.md`](dbt_style_guide.md)
- **Scope doc (v1):** [`client_v1_scope_speed_to_lead.md`](client_v1_scope_speed_to_lead.md)
- **Worklog:** [`WORKLOG.md`](WORKLOG.md)

## Getting started

See the **Initial Setup** section in [`CLAUDE.md`](CLAUDE.md#initial-setup).
