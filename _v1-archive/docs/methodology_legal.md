# Methodology and Legal Boundaries

## Allowed
- DNS lookups.
- HTTP/HTTPS requests to public resources.
- SSL/TLS metadata collection from endpoint handshakes.
- Public OSINT/API datasets with valid terms of use.
- Archived/public snapshots analysis.

## Disallowed
- Any exploit payloads.
- Authentication bypass attempts.
- Brute force or credential stuffing.
- Port scanning and service enumeration outside standard web access.
- High-volume traffic that could impact availability.

## Safety Controls
- Fixed request rate limit per site.
- Fixed path dictionary with explicit review.
- Timestamp and evidence logging for every finding.
- "Needs manual validation" tag for uncertain detections.

## Evidence Standard
Each finding should include:
- Source (HTTP response, DNS record, API dataset, or archive reference).
- Timestamp (UTC).
- Minimal proof snippet (status/header/record).
- Confidence (`high`, `medium`, `low`).

## Delivery Standard
- Free offer must avoid overclaiming.
- Full demo must separate confirmed findings from hypotheses.
