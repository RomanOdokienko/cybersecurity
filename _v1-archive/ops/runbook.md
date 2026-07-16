# Runbook (Single-Site Mode)

## 1. Intake
1. Add domain to `data/input/site_queue.csv`.
2. Assign `site_id`.
3. Set status to `pending`.

## 2. Prepare Workspace
1. Run `scripts/new-site.ps1 -Domain <domain>`.
2. Confirm folders in `data/raw/<site_id>/`.

## 3. Execute Audit
1. Collect raw evidence into `data/raw/<site_id>/`.
2. Normalize into `data/normalized/<site_id>.json`.
3. Validate structure against `schemas/site_audit.schema.json`.

## 4. Generate Outputs
1. Free offer from top findings.
2. Full report from all validated findings.
3. Update dashboard export.

## 5. Quality Gate
1. Check evidence completeness.
2. Confirm no disallowed methods were used.
3. Set queue status to `done` or `needs_review`.
