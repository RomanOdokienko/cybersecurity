import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "sites_manifest.json"
AGENT_MODE_LOCK = ROOT / "AGENT_ONLY_MODE.lock"


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_site_ids(args):
    out = set()
    for x in args.site_id or []:
        v = str(x or "").strip()
        if v:
            out.add(v)
    for x in str(args.site_ids or "").split(","):
        v = x.strip()
        if v:
            out.add(v)
    return sorted(out)


def parse_blocks(raw: str):
    vals = [x.strip() for x in str(raw or "").split(",") if x.strip()]
    allowed = {"b2", "b3", "b4"}
    bad = [x for x in vals if x not in allowed]
    if bad:
        raise SystemExit(f"Unsupported blocks: {', '.join(bad)}. Allowed: b2,b3,b4")
    return vals


def ensure_agent_only_mode():
    if not AGENT_MODE_LOCK.exists():
        raise SystemExit(
            "Blocked by policy: AGENT_ONLY_MODE.lock not found.\n"
            "Repository is configured for agent-only verification."
        )
    try:
        text = AGENT_MODE_LOCK.read_text(encoding="utf-8")
    except Exception:
        text = AGENT_MODE_LOCK.read_text(encoding="utf-8", errors="ignore")
    if "mode=agent_only" not in str(text).lower():
        raise SystemExit(
            "Blocked by policy: invalid AGENT_ONLY_MODE.lock content.\n"
            "Required marker: mode=agent_only"
        )


def get_in(obj, path, default=None):
    cur = obj
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def validate_block_payload(audit: dict, block_id: str):
    issues = []
    tech = audit.get("tech") or {}
    med = tech.get("med_trust") or {}
    discovery = audit.get("discovery") or {}

    if block_id == "b4":
        required_bool_fields = [
            ("tech.med_trust.doctors_content_found", med.get("doctors_content_found")),
            ("tech.med_trust.address_found", med.get("address_found")),
            ("tech.med_trust.map_found", med.get("map_found")),
            ("tech.med_trust.hours_found", med.get("hours_found")),
            ("tech.med_trust.reviews_found", med.get("reviews_found")),
            ("tech.med_trust.contact_page_exists", med.get("contact_page_exists")),
            ("tech.med_trust.contact_block_found", med.get("contact_block_found")),
        ]
        for field_path, value in required_bool_fields:
            if not isinstance(value, bool):
                issues.append(f"{field_path} must be boolean, got: {value!r}")

        footer_year = med.get("footer_year") or {}
        footer_present = footer_year.get("present")
        footer_current = footer_year.get("current_year")
        if not isinstance(footer_present, bool):
            issues.append(f"tech.med_trust.footer_year.present must be boolean, got: {footer_present!r}")
        elif footer_present and not isinstance(footer_current, bool):
            issues.append(f"tech.med_trust.footer_year.current_year must be boolean when footer is present, got: {footer_current!r}")

        checked_pages = get_in(med, ["text_typos", "checked_pages"], default=None)
        if not isinstance(checked_pages, list) or len(checked_pages) == 0:
            issues.append("tech.med_trust.text_typos.checked_pages must be a non-empty list")

    elif block_id == "b2":
        analytics_found = get_in(tech, ["analytics", "found"], default=None)
        price_public_found = med.get("price_public_found")
        sitemap_total = discovery.get("sitemap_total_urls")
        if not isinstance(analytics_found, bool):
            issues.append(f"tech.analytics.found must be boolean, got: {analytics_found!r}")
        if not isinstance(price_public_found, bool):
            issues.append(f"tech.med_trust.price_public_found must be boolean, got: {price_public_found!r}")
        if not isinstance(sitemap_total, int):
            issues.append(f"discovery.sitemap_total_urls must be integer, got: {sitemap_total!r}")

    elif block_id == "b3":
        ssl_valid = get_in(tech, ["ssl", "valid"], default=None)
        http_redirect = get_in(tech, ["http_to_https", "redirected_to_https"], default=None)
        sec_present = get_in(tech, ["security_headers", "present"], default=None)
        if not isinstance(ssl_valid, bool):
            issues.append(f"tech.ssl.valid must be boolean, got: {ssl_valid!r}")
        if not isinstance(http_redirect, bool):
            issues.append(f"tech.http_to_https.redirected_to_https must be boolean, got: {http_redirect!r}")
        if not isinstance(sec_present, list):
            issues.append(f"tech.security_headers.present must be list, got: {sec_present!r}")

    return issues


def main():
    ensure_agent_only_mode()
    p = argparse.ArgumentParser(description="Set verification mode for clinic audits")
    p.add_argument("--site-id", action="append", default=[], help="site id (repeatable)")
    p.add_argument("--site-ids", default="", help="comma-separated site ids")
    p.add_argument("--mode", choices=["agent"], required=True, help="verification mode (agent-only)")
    p.add_argument("--set-verified", action="store_true", help="also set verification.<block>=true")
    p.add_argument("--blocks", default="b2,b3,b4", help="blocks for --set-verified (default: b2,b3,b4)")
    args = p.parse_args()

    site_ids = parse_site_ids(args)
    if not site_ids:
        raise SystemExit("No site ids provided. Use --site-id or --site-ids.")

    manifest = read_json(MANIFEST)
    by_id = {str(x.get("id")): x for x in manifest}
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    blocks = parse_blocks(args.blocks)

    updated = 0
    missing = []
    blocked = []

    for sid in site_ids:
        item = by_id.get(sid)
        if not item:
            missing.append(sid)
            continue
        audit_path = ROOT / str(item.get("audit_file"))
        if not audit_path.exists():
            missing.append(f"{sid} (missing audit file)")
            continue
        audit = read_json(audit_path)
        verification = audit.get("verification") or {}
        if not isinstance(verification, dict):
            verification = {}

        if args.set_verified:
            per_site_issues = []
            for b in blocks:
                per_site_issues.extend(validate_block_payload(audit, b))
            if per_site_issues:
                blocked.append((sid, audit_path, per_site_issues))
                continue

        verification["mode"] = args.mode
        verification["mode_updated_at"] = now
        if args.set_verified:
            for b in blocks:
                verification[b] = True
        audit["verification"] = verification
        write_json(audit_path, audit)
        updated += 1
        print(f"updated: {sid} -> {audit_path}")

    print(f"updated_total={updated}")
    if missing:
        print("missing:", ", ".join(missing))
    if blocked:
        print("blocked_total=", len(blocked))
        for sid, audit_path, issues in blocked:
            print(f"blocked: {sid} -> {audit_path}")
            for issue in issues:
                print("  -", issue)
        raise SystemExit("Verification flags were not set for blocked audits: incomplete block payload.")


if __name__ == "__main__":
    main()
