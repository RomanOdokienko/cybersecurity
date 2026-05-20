# MINI-TZ AFTER CONTEXT COMPACTION (Live PoC pass)

Date: 2026-05-20
Scope: clinics #50+

1) Source of truth
- Table statuses are immutable in this pass.
- Update only `agent_structured.blocks[].metrics[].poc` where `status == "проблема"`.

2) PoC validity rules
- PoC must come from live website checks (not from sheet/import placeholders).
- Mandatory PoC structure:
  - URL(s)
  - concrete factual signal (checked checkbox / 404 / explicit link / tracker marker / text snippet)
  - short conclusion tied to metric

3) Forbidden in final PoC
- "из Google Sheet ..."
- "sheet_raw_value ..."
- any import-only technical artifacts

4) Conflict handling
- If live facts disagree with current status, DO NOT change status.
- Put explicit note in PoC: "status preserved from source; requires methodological review".

5) Availability handling
- If page/site unavailable, PoC must include checked URLs + HTTP result/errors.

6) Per-clinic process
- list problem metrics
- collect live evidence per metric
- patch only those `poc`
- diff-check no status/t_f changes
- rebuild UI only for current clinic pair

7) Continuity rule
- After each completed pair: immediately move to the next pair from the queue, preserving PoC-only constraints.
- Never switch to status rewrite mode unless user explicitly asks.
