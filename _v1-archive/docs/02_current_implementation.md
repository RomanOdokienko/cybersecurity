# Current Implementation

Snapshot as of 2026-04-16.

## 1. Already Implemented
1. Quick pipeline `scripts/run-site-quick.ps1`.
2. Full pipeline `scripts/run-site-full.ps1`.
3. Dashboard export `scripts/export-dashboard-data.ps1`.
4. Dashboard with these sections: summary, dedicated 152-FZ block, block statuses, lead queue, materials/full markers.
5. `report_index.json` generation to connect dashboard <-> outputs.

## 2. Key Quick v2 Changes (152-FZ)
1. Reduced false fails under low observability.
2. Forms split into relevant and non-relevant.
3. Policy pages searched via expanded link/marker set.
4. Unknown form `method` now maps to `review_required`, not automatic fail.

## 3. Key Quick v3 Changes (Security Headers Calibration)
1. `Security headers` is no longer used as a mass default `critical` trigger.
2. Block is calibrated as benchmark-first (`warning` in typical missing-header cases).
3. `critical` for headers is reserved for a strong compound scenario (hardening + transport gap).

## 4. Current Check Coverage
Blocks:
1. SSL + redirects
2. File leaks
3. Open admin/CMS exposure
4. Security headers
5. Forms and patient data
6. DNS/email protection
7. Third-party scripts
8. Reputation/OSINT

## 5. Current Limitations
1. Quick does not guarantee full coverage of JS-driven form dynamics.
2. Reputation/OSINT in quick mode is limited.
3. Part of conclusions still requires agent/manual validation before sales use.

## 6. Where to See Current Snapshot
1. Latest normalized results: `data/normalized/*.json`.
2. Dashboard dataset: `dashboard/sample_audits.json`.
3. Quick v1/v2 comparison: `outputs/quick/quick_v1_vs_v2_2026-04-16.md`.
