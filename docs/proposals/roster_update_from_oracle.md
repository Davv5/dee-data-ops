# Proposed roster update — oracle-evidence cross-reference

**Date:** 2026-04-20
**Source:** Master Lead Sheet.xlsx snapshot 2026-03-19 + current `ghl_sdr_roster.csv` (16 rows, all `role=unknown`)
**Rule:** DataOps mandates human-in-loop commits for person-identifying seed data. This document proposes role resolutions; David reviews and commits `ghl_sdr_roster.csv` changes manually.

Evidence is pulled from two oracle validation seeds landed by Track B:

- `dbt/seeds/validation/oracle_sdr_leaderboard_20260319.csv` — 5 SDRs + TOTAL
- `dbt/seeds/validation/oracle_closer_leaderboard_20260319.csv` — 7 closers + TOTAL

## Proposed changes (16 current roster rows)

| user_id | name | current_role | proposed_role | evidence | source_tab | commit? |
|---|---|---|---|---|---|---|
| 9rocXim1JjeIvjSrWLSn | Boipelo Mashigo | unknown | SDR | On SDR Leaderboard: 1,278 dials, 21 sets, $10,044.42 cash this month, $26,115.21 all-time | oracle_sdr_leaderboard | ☐ |
| leBv9MtltaKdfSijVEhb | aariz menon | unknown | SDR | On SDR Leaderboard: 2,694 dials, 157 pickups, 26 sets, $2,680.25 cash all-time | oracle_sdr_leaderboard | ☐ |
| DTtFkB0jtX1ionHhjsGR | Blagoj Veleski | unknown | SDR+Closer (dual) | On BOTH leaderboards — SDR: 938 dials, 12 sets, $2,268.42; Closer: 0 booked recent, $1,012.61 cash this month, 2 closes | oracle_sdr_leaderboard, oracle_closer_leaderboard | ☐ |
| c5ujVqeYHGi1WnmlvtWu | Marco Branco | unknown | SDR | On SDR Leaderboard: 2,810 dials, 183 pickups, 21 sets, $1,792.65 cash all-time | oracle_sdr_leaderboard | ☐ |
| ZOytPUG1jSWRNBzsJYEp | Hammad Ahsan | unknown | Closer | On Closer Leaderboard: 38 booked, 26 showed, 20 closed, 68.4% show rate, $11,011.47 cash this month, $37,194.83 all-time | oracle_closer_leaderboard | ☐ |
| 1D4ZUkV07gGJ25YtUolz | Houssam Bentouati | unknown | Closer | On Closer Leaderboard: 60 booked, 20 showed, 10 closed, 33.3% show rate, $2,979.67 cash this month | oracle_closer_leaderboard | ☐ |
| ILX9jpFp7ycNbWgakiYR | Kevin Maya | unknown | Closer | On Closer Leaderboard: 3 booked, 3 showed, 5 closed, 100% show rate, $1,211.25 cash this month | oracle_closer_leaderboard | ☐ |
| J4eyQWx4oFfPj08qunrS | Jordan Evans | unknown | Closer | On Closer Leaderboard: 4 booked, 2 showed, 3 closed, 50% show rate, $920.55 cash this month | oracle_closer_leaderboard | ☐ |
| BKc6beDhtuJIW1Gfp0wI | Ethan Gerstenberg | unknown | Closer | On Closer Leaderboard: 0 booked recent, 1 close, $6,530.16 all-time | oracle_closer_leaderboard | ☐ |
| P2EYwqPfTXAocmdOFpOW | Dee Briggins | unknown | Owner | Client owner (dee@richfromclothes.com) | — | ☐ |
| eWA0YcbNP3rklPwRFFwM | Ayaan Menon | unknown | UNKNOWN | No evidence on either leaderboard — needs David confirmation | — | ☐ (ask David) |
| XKcL1lmTZn8LFHiUwtn1 | Jake Lynch | unknown | UNKNOWN | No evidence on either leaderboard — needs David confirmation | — | ☐ (ask David) |
| bXFkNE0kp7G80bAB7NCZ | Isaac Davis | unknown | UNKNOWN | No evidence — not on any leaderboard | — | ☐ (ask David) |
| YyBgSVqB1wQoFj8tAe40 | Stanley Macauley | unknown | UNKNOWN | Not on any leaderboard — external domain | — | ☐ (ask David) |
| 7rCcXXi8tFdihhDvTTM3 | Mitchell Naude | unknown | UNKNOWN | Not on any leaderboard | — | ☐ (ask David) |
| NBLGgp7crDRWtPHkdJLx | David Forero | unknown | Owner | David's operator account (helpdav5@gmail.com) | — | ☐ |

## Proposed roster-gap ADDITIONS (on oracle leaderboards but not in current roster)

| name | proposed_role | proposed_status | evidence | commit? |
|---|---|---|---|---|
| Moayad | SDR | departed | On SDR Leaderboard: 0 dials / 0 sets recent, $7,339.20 all-time. Zero recent activity between oracle snapshot (2026-03-19) and today suggests departure. | ☐ (add with status=departed?) |
| Halle | Closer | active | On Closer Leaderboard: 0 booked recent, $7,552.65 all-time. No recent activity but material all-time cash — confirm status before adding. | ☐ (add as active Closer?) |

## David's action

1. Review the evidence column for each row
2. Decide per-row: accept, reject, or mark UNKNOWN for follow-up
3. Manually edit `dbt/seeds/ghl_sdr_roster.csv` to reflect approved changes
4. Optionally add a `role_source` column (values: `oracle_leaderboard`, `david_confirmed`, `unknown`) to encode provenance for future audits
5. Open a separate PR with the roster edits (distinct from this Track B PR)
