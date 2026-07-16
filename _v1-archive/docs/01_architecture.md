# System Architecture

Version: 2026-04-16

## 1. High-Level Architecture
Components:
1. Intake: domain list in `data/input/site_queue.csv`.
2. Execution: quick/full scripts in `scripts/`.
3. Data layer: raw evidence + normalized JSON.
4. Reporting layer: HTML/MD/PDF outputs.
5. Visualization layer: dashboard (`dashboard/`).
6. Ops layer: runbook, triage, acceptance criteria (`ops/`).

## 2. Data Flow
1. Domain -> `run-site-quick.ps1`.
2. Script writes evidence to `data/raw/<site_id>/`.
3. Script writes normalized result to `data/normalized/<site_id>.json`.
4. `export-dashboard-data.ps1` builds latest-per-domain into `dashboard/sample_audits.json`.
5. Dashboard reads `sample_audits.json` and `report_index.json`.

## 3. Audit Modes
1. Quick: standard high-throughput run for lead qualification.
2. Full: extended passive run for complex/high-priority cases.
3. Agent/manual: browser-based manual validation of disputed findings.

## 4. Data Contracts
1. Normalized JSON must match `schemas/site_audit.schema.json`.
2. `compliance_152` stores aggregates and criterion-level evidence.
3. `triage.next_action` stores the management decision for the lead.

## 5. Directory Map
1. `config/` - checks, sources, thresholds.
2. `scripts/` - run and export automation.
3. `data/raw/` - raw artifacts and evidence.
4. `data/normalized/` - analytics/dashboard results.
5. `dashboard/` - UI and aggregated JSON.
6. `outputs/` - client-facing documents.
7. `ops/` - process operation rules.
8. `docs/` - business and architecture documentation.

## 6. Core Architecture Principles
1. Reproducibility: every conclusion must be reproducible from evidence.
2. Explainability: every status must be understandable in UI.
3. Separation of concerns: quick qualification is separate from deep audit.
4. Low coupling: dashboard depends on normalized data, not script internals.
