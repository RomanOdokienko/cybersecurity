import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "sites_manifest.json"


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


def main():
    p = argparse.ArgumentParser(description="Set verification mode for clinic audits")
    p.add_argument("--site-id", action="append", default=[], help="site id (repeatable)")
    p.add_argument("--site-ids", default="", help="comma-separated site ids")
    p.add_argument("--mode", choices=["agent", "legacy_script"], required=True, help="verification mode")
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


if __name__ == "__main__":
    main()

