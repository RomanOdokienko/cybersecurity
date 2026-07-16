# CLAUDE.md

This file is the fast-entry context for AI agents (Claude) working in this repository.

## 1) Project in one paragraph
This project is an MVP for B2B lead qualification of clinic websites via passive cybersecurity/compliance audits. The goal is not "max findings", but reliable business triage: where we have a strong sales offer now, where manual validation is needed, and where offer is weak.

## 2) Primary task for AI agent
1. Understand current pipeline and data model.
2. Run/adjust quick audit safely (passive-only).
3. Keep dashboard data fresh and explainable.
4. Preserve business-oriented output quality for sales/demo.

## 3) First files to read
1. `docs/README.md`
2. `docs/00_project_context.md`
3. `docs/01_architecture.md`
4. `docs/02_current_implementation.md`
5. `ops/runbook.md`

## 4) How to run dashboard (must know)
From repo root:
```powershell
python -m http.server 8080
```
Open:
`http://localhost:8080/dashboard/`

## 5) Core operational commands
Quick audit for one domain:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-site-quick.ps1 -Domain example.com
```

Quick audit for a list (newline/comma/semicolon in file):
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-quick-batch.ps1 -FilePath .\domains.txt
```

Rebuild dashboard dataset:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-dashboard-data.ps1
```

## 6) Safety and legal boundaries (strict)
1. Passive/public-data-only checks.
2. No exploitation, brute force, intrusive scanning.
3. Process one site at a time.

## 7) Key data locations
1. Raw evidence: `data/raw/<site_id>/`
2. Normalized result: `data/normalized/<site_id>.json`
3. Dashboard data: `dashboard/sample_audits.json`
4. Report links index: `dashboard/report_index.json`

## 8) Current audit blocks
1. SSL + redirects
2. File leaks
3. Open admin/CMS exposure
4. Security headers
5. Forms and patient data (includes 152-FZ screening)
6. DNS/email protection
7. Third-party scripts
8. Reputation/OSINT

Important calibration note:
- `Security headers` in quick mode is benchmark-first (usually `warning`), not a default mass `critical` trigger.
- `critical` for headers should appear only on strong compound hardening+transport gaps.

## 9) Team expectations for changes
1. Keep findings reproducible via evidence.
2. Separate confirmed facts from assumptions.
3. If changing scoring/triage logic, update docs in `docs/`.
4. Keep commits atomic and explain verification.
