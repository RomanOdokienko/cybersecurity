# Audit Scope v1

## Objective
Build a repeatable passive security audit for clinic websites with a sales-ready output format.

## Processing Mode
- One site at a time.
- Publicly accessible data only.
- No active exploitation.

## Core Blocks (Production)
1. SSL + Redirects
- Certificate validity and expiry.
- HTTP to HTTPS redirect behavior.
- HSTS presence and quality.
- Mixed content checks.

2. File Leaks
- Limited dictionary checks for sensitive files and backups.
- Directory listing indicators.

3. Open Admin / CMS Exposure
- CMS fingerprint from public markers.
- Admin endpoint exposure without auth redirect.
- Public version leakage indicators.

4. Security Headers
- Presence and quality of key headers.
- Header disclosure (`Server`, `X-Powered-By`).

5. Forms and Patient Data
- Form transport security (HTTPS action).
- Privacy policy linkage near forms.
- CAPTCHA/honeypot indicators.

6. DNS / Email Protection
- SPF/DMARC/DKIM checks.
- MTA-STS/TLS-RPT presence (advanced).

7. Third-Party Scripts
- External script domain inventory.
- Outdated library indicators and known risks.
- SRI (`integrity`) coverage.

8. Reputation / OSINT
- Domain reputation in public sources.
- Historical traces where applicable.

## Free Offer vs Demo
- Free offer: high-visibility subset only (3-5 findings).
- Demo report: full scope, prioritization, remediation roadmap.

## Out of Scope
- SQLi/XSS payload testing.
- Brute force attempts.
- Port scanning or intrusive network probing.
