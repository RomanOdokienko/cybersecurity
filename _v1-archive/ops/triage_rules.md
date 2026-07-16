# Triage Rules

## Priority Mapping
1. Critical: direct public exposure of sensitive data or high-impact misconfiguration.
2. High: strong exploitability signal but with constraints.
3. Medium: weakness requiring context or chaining.
4. Low: hardening opportunity with low immediate impact.

## Confidence Mapping
- High: direct evidence observed in current run.
- Medium: indirect but consistent indicators.
- Low: heuristic or historical-only signal.

## Escalation Conditions
- Any likely patient-data exposure.
- Any publicly accessible secret/config dump.
- Any security control gap with legal/compliance implications.

## Reporting Discipline
- Confirmed findings first.
- Keep inferred findings clearly labeled.
