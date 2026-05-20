# Mini TZ: Live PoC for "problem" metrics (clinics 50+)

Version: 2026-05-20

Goal
- For clinics starting from #50, re-check only metrics where status == "проблема".
- Replace PoC with live evidence from website pages.
- Do NOT change table statuses as source-of-truth unless user explicitly asks.

Hard Rules
1) PoC is valid only if collected from live website pages.
2) PoC must include:
- evidence URL(s)
- concrete factual signal (snippet/attribute/visible element)
- short conclusion tied to metric
3) It is forbidden to use placeholder PoC values as final evidence:
- "из Google Sheet ..."
- "sheet_raw_value ..."
- any import-only technical markers
4) For this task, only mutate `poc` fields of metrics with status "проблема".
5) Do not edit status/t_f/other metrics/other clinics in the same pass.

Execution Checklist (per clinic)
1) Read audit JSON and list metrics with status "проблема".
2) Visit/scan target site pages relevant to each such metric.
3) Build live PoC lines with URL + fact.
4) Patch only those `poc` arrays.
5) Run diff check: ensure only `poc` lines changed.
6) Report what was updated and what remains.

Quality Gate
- If live evidence cannot be retrieved (site unavailable), PoC must explicitly state access issue and checked URLs.
- No synthetic PoC from previous files.
