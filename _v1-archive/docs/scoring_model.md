# Scoring Model v1

## Purpose
Provide a stable, explainable risk score for prioritization and sales narrative.

## Finding Score Formula
`finding_score = severity_weight * confidence_weight * exposure_weight`

## Suggested Weights
- Severity:
- `critical = 10`
- `high = 7`
- `medium = 4`
- `low = 2`

- Confidence:
- `high = 1.0`
- `medium = 0.7`
- `low = 0.4`

- Exposure:
- `public_direct = 1.0`
- `conditional = 0.7`
- `historical_only = 0.5`

## Block Score
`block_score = min(100, sum(finding_score_normalized))`

## Overall Score
Weighted average by block priority:
- Priority 1-3: weight 1.5
- Priority 4-5: weight 1.2
- Priority 6-8: weight 1.0

## Risk Bands
- `80-100`: Critical attention
- `60-79`: High risk
- `35-59`: Medium risk
- `0-34`: Low risk

## Reporting Rule
- Free offer includes max 5 findings with highest business impact.
- Full report includes all validated findings + remediation plan.

## Block Calibration Notes (MVP)
- `Security headers` is benchmark-first in quick mode.
- Missing headers alone should usually produce `warning`, not automatic `critical`.
- `critical` on headers should require a compound signal (for example, broad hardening gap plus transport weakness).
