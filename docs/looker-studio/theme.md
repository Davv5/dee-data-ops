# Theme — D-DEE Looker Studio reports

Apply this once to the report via **Theme and layout → Customize → Custom**.
Every page inherits it. When cloning for client #2, re-apply the palette.

## Typography

| Element | Font | Size | Weight |
|---|---|---|---|
| Page title | Inter (or Arial fallback) | 24 | Semibold |
| Section header | Inter | 16 | Semibold |
| Scorecard primary value | Inter | 32 | Bold |
| Scorecard label | Inter | 11 | Medium |
| Chart axis labels | Inter | 11 | Regular |
| Table body | Inter | 12 | Regular |
| Table header | Inter | 11 | Semibold |

Inter isn't default in Looker Studio — pick **Arial** or **Roboto** if Inter
isn't available in your account's font list. Stay consistent across every
tile; mixed fonts is the #1 thing that makes BI dashboards look amateur.

## Color palette

Professional BI palette — intentionally muted so the **data** is the focal
point, not the chrome.

| Token | Hex | Use |
|---|---|---|
| `bg-primary` | `#FFFFFF` | Page background (light mode) |
| `bg-card` | `#F7F8FA` | Tile / scorecard background |
| `border-subtle` | `#E5E7EB` | Tile borders, table dividers |
| `text-primary` | `#111827` | Headlines, table body, big numbers |
| `text-secondary` | `#6B7280` | Labels, axis text, subtext |
| `accent-primary` | `#1D4ED8` | Primary chart series, positive scorecard delta |
| `accent-success` | `#059669` | Within-SLA color (green; don't scream it) |
| `accent-warn` | `#D97706` | DQ warnings, 1-hr-no-touch |
| `accent-danger` | `#DC2626` | Outside-SLA, lost opportunities |
| `accent-neutral-1` | `#64748B` | Secondary series |
| `accent-neutral-2` | `#94A3B8` | Tertiary series |

### Stacked-series palette (for daily-volume chart)

Ordered by visual weight — top sources get stronger colors:

1. `#1D4ED8` (blue-700)
2. `#0891B2` (cyan-600)
3. `#059669` (emerald-600)
4. `#D97706` (amber-600)
5. `#9333EA` (violet-600)
6. `#DC2626` (red-600)
7. `#475569` (slate-600)
8. `#0F766E` (teal-700)
9. `#B45309` (amber-700)
10. `#65A30D` (lime-600)
- `other` bucket: `#CBD5E1` (slate-300 — intentionally muted)

### Traffic-light conditional formatting (SDR leaderboard)

Applied to `pct_within_5min` column in the leaderboard table:

| Range | Background | Text |
|---|---|---|
| ≥ 70% | `#D1FAE5` (emerald-100) | `#065F46` |
| 40%–70% | `#FEF3C7` (amber-100) | `#92400E` |
| < 40% | `#FEE2E2` (red-100) | `#991B1B` |

## Layout grid

Looker Studio uses a 12-column grid (configurable). Default page size:
**1440 × 1024 px** (desktop report, client-friendly on most monitors;
readable on mobile via Looker's responsive view).

Grid settings (Theme and layout → Layout):
- Viewport: Width 1440, Height 1024
- Grid: 12 columns
- Margin: 24 px all sides
- Tile gutter: 16 px

## Chart defaults

Set these once via **Theme and layout → Chart defaults**:

- Hide chart titles (use bordered containers with header text instead)
- Chart border: `1px solid #E5E7EB`, border-radius `8px`
- Chart padding: 16 px internal
- Gridlines: show only horizontal, color `#F3F4F6`
- Axis line color: `#E5E7EB`
- Legend position: top
- Data labels: off by default (on only where called out in the tile spec)
- Number format defaults: thousands separator on, 1 decimal for percentages

## Logo + header bar (optional for D-DEE)

Add a simple text header "D-DEE Speed-to-Lead — v1" in 18pt Inter Semibold,
left-aligned. Skip graphical logo in v1; add on client request.

Precision Scaling brand (if referenced on internal reports):
- Logo path: (pending — David to supply PNG)
- Place in top-left corner, 32px height max

## Exporting theme for client #2

Looker Studio doesn't have a "save theme" file — the theme lives on the
report. To reuse:
1. **File → Make a copy** of the master D-DEE report
2. Theme settings carry over automatically
3. Only the color palette token overrides need re-application if brand differs

See `README.md` → "Reuse pattern for client #2."
