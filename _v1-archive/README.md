# Cyber Security Audit Workspace

This repository is prepared for passive security audits of clinic websites, processed one domain at a time.

## Team Entry Point
- AI agent context (Claude): `CLAUDE.md`
- Documentation index: `docs/README.md`
- Project context: `docs/00_project_context.md`
- Architecture: `docs/01_architecture.md`
- Current implementation state: `docs/02_current_implementation.md`
- Team workflow: `docs/03_team_workflow.md`
- Onboarding: `docs/04_onboarding.md`
- MVP roadmap: `docs/05_mvp_roadmap.md`
- Contribution rules: `CONTRIBUTING.md`

## Goals
- Keep a repeatable audit scope and legal boundaries.
- Produce two outputs from one dataset:
- Free offer (limited findings, high-impact only).
- Full demo report (complete findings + remediation plan).
- Maintain a minimal local dashboard for fast review.

## Quick Start
1. Run quick audit for one domain:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-site-quick.ps1 -Domain clinic-example.ru
```
2. Or run quick audit for a list of domains (txt/csv/newline):
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-quick-batch.ps1 -FilePath .\domains.txt
```
3. Rebuild dashboard data (batch script already does it automatically):
```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-dashboard-data.ps1
```
4. Run local dashboard:
```powershell
python -m http.server 8080
```
Open `http://localhost:8080/dashboard/`.

## Main Rules
- Passive/public-data-only approach.
- No exploitation, brute force, or port scanning.
- One site at a time workflow.
- Team sync model: `pull -> local audit -> export -> push -> pull`.

## Project Layout
- `docs/`: scope, legal methodology, scoring, offer strategy.
- `config/`: checks catalog, thresholds, source definitions.
- `data/`: input queue, raw evidence, normalized results.
- `schemas/`: result schema.
- `templates/`: offer/report/remediation templates.
- `ops/`: runbook, triage and acceptance criteria.
- `dashboard/`: minimal visualization layer.
- `scripts/`: helper automation for local workflow.
