import json
import html
import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

ROOT = Path(r"D:\разработка\Кибербеза 2.0")
MANIFEST = ROOT / "data" / "sites_manifest.json"

EXTERNAL_EMAIL_DOMAINS = {
    "gmail.com", "yandex.ru", "mail.ru", "bk.ru", "inbox.ru", "list.ru", "ya.ru"
}


def site_host(value: str) -> str:
    host = urlparse(site_url(value)).netloc.lower().split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def read_json(path: Path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def esc(value):
    return html.escape(str(value), quote=True)


def site_url(value: str) -> str:
    s = str(value or "").strip()
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return f"https://{s}"


def badge_class(label: str) -> str:
    mapping = {
        "ок": "ok",
        "проблема": "bad",
        "проверить": "warn",
        "н/п": "na",
        "-": "na",
        "текстом": "bad",
        "checked": "bad",
        "не найдено": "bad",
        "unchecked": "ok",
        "слать": "ok",
        "не слать": "bad",
    }
    if label in mapping:
        return mapping[label]
    low = str(label).lower()
    if "unchecked" in low:
        return "ok"
    if any(x in low for x in ["checked", "не найдено", "текстом"]):
        return "bad"
    return "na"


def source_label(source: str) -> str:
    mapping = {
        "": "основной URL",
        "navigation": "навигация",
        "sitemap": "sitemap",
        "sitemap-form": "sitemap: страница с формой",
        "sitemap-booking-candidate": "sitemap: кандидат записи по форме/контенту",
        "booking-candidate": "кандидат записи по форме/контенту",
        "home-booking-candidate": "главная: кандидат записи по форме/контенту",
        "fallback": "fallback-путь",
    }
    return mapping.get(source, source or "основной URL")


def is_core_discovery_source(source: str) -> bool:
    return source in {
        "",
        "navigation",
        "sitemap",
        "sitemap-booking-candidate",
        "booking-candidate",
        "home-booking-candidate",
    }


def select_found_pages_for_availability(pages, source_map):
    non_fallback = [p for p in pages if source_map.get(p.get("requested"), "") != "fallback"]
    core = [p for p in non_fallback if is_core_discovery_source(source_map.get(p.get("requested"), ""))]
    return core if core else non_fallback


def classify_form_consent(form):
    if form.get("has_checkbox"):
        return "checked" if form.get("checked") is True else "unchecked"
    if form.get("has_policy_text"):
        return "текстом"
    return "не найдено"


def parse_email_domain(email: str):
    e = str(email or "").strip().lower()
    if "@" not in e:
        return None
    local, domain = e.rsplit("@", 1)
    domain = domain.strip().strip(".")
    if not local or not domain or "." not in domain:
        return None
    labels = domain.split(".")
    if len(labels) < 2:
        return None
    if not re.fullmatch(r"[a-z]{2,63}", labels[-1]):
        return None
    for label in labels:
        if not re.fullmatch(r"[a-z0-9-]{1,63}", label):
            return None
        if label.startswith("-") or label.endswith("-"):
            return None
    # Reject obviously technical artifacts like 4.2.2.js (no alphabetic host label).
    if not any(re.search(r"[a-z]", lbl) for lbl in labels[:-1]):
        return None
    return domain


def collect_email_candidates(item, audit):
    host = site_host(item.get("site", ""))
    seen = set()
    out = []

    raw = []
    m = str(item.get("contact_email", "") or "").strip()
    if m:
        raw.append(("manifest", m))
    for x in audit.get("emails", []) or []:
        e = str(x or "").strip()
        if e:
            raw.append(("audit", e))

    for source, email in raw:
        em = email.lower()
        if em in seen:
            continue
        seen.add(em)
        domain = parse_email_domain(em)
        if not domain:
            continue
        out.append({
            "source": source,
            "email": em,
            "domain": domain,
            "is_external": domain in EXTERNAL_EMAIL_DOMAINS,
            "is_site_related": (
                domain == host
                or host.endswith("." + domain)
                or domain.endswith("." + host)
            ),
        })
    return out


def pick_email_candidate(item, audit):
    cands = collect_email_candidates(item, audit)
    if not cands:
        return None
    manifest_cands = [c for c in cands if c["source"] == "manifest"]
    if manifest_cands:
        return sorted(
            manifest_cands,
            key=lambda c: (
                1 if c["is_external"] else 0,
                0 if c["is_site_related"] else 1,
                len(c["domain"]),
            ),
        )[0]
    return sorted(
        cands,
        key=lambda c: (
            1 if c["is_external"] else 0,
            0 if c["is_site_related"] else 1,
            len(c["domain"]),
        ),
    )[0]


def normalize_dns_txt(data: str):
    s = str(data or "").strip()
    parts = re.findall(r'"([^"]*)"', s)
    if parts:
        return "".join(parts).strip()
    return s.strip('"').strip()


@lru_cache(maxsize=1024)
def dns_txt_records(name: str):
    query = urlencode({"name": name, "type": "TXT"})
    urls = [
        f"https://dns.google/resolve?{query}",
        f"https://cloudflare-dns.com/dns-query?{query}",
    ]
    last_err = None

    for url in urls:
        try:
            req = Request(url, headers={"accept": "application/dns-json", "user-agent": "Mozilla/5.0"})
            with urlopen(req, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            status = int(data.get("Status", 0))
            answers = data.get("Answer") or []
            txt = [normalize_dns_txt(a.get("data", "")) for a in answers if int(a.get("type", 0)) == 16]
            return {"ok": True, "rcode": status, "txt": txt}
        except Exception as exc:
            last_err = str(exc)

    return {"ok": False, "rcode": None, "txt": [], "error": last_err or "lookup failed"}


def find_tag_value(record: str, tag: str):
    m = re.search(rf"(?i)(?:^|;)\s*{re.escape(tag)}\s*=\s*([^;\s]+)", record or "")
    return m.group(1).strip() if m else None


def short_record(value: str, limit: int = 120):
    s = str(value or "").strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


def evaluate_spf_dmarc(item, audit):
    cand = pick_email_candidate(item, audit)
    if not cand:
        return "проверить", [
            "Email для проверки не найден (ни в манифесте, ни на страницах сайта)."
        ], ""

    email = cand["email"]
    domain = cand["domain"]
    source_label_txt = "манифест" if cand["source"] == "manifest" else "найден на сайте"

    if cand["is_external"]:
        return "н/п", [
            f"Email: {email} ({source_label_txt})",
            f"Домен {domain} — сторонний почтовый сервис, не почтовый домен клиники.",
            "Для этой проверки SPF/DMARC не оценивается.",
            "Что проверить дальше: найти корпоративный email домена клиники и проверить SPF/DMARC уже для него.",
        ], email

    spf_lookup = dns_txt_records(domain)
    dmarc_lookup = dns_txt_records(f"_dmarc.{domain}")

    issues = []
    warns = []
    lines = [
        f"Email: {email} ({source_label_txt})",
        f"Домен проверки: {domain}",
    ]

    spf_info = "не найден"
    if not spf_lookup.get("ok"):
        warns.append(f"SPF DNS lookup error: {spf_lookup.get('error')}")
        spf_info = "ошибка DNS lookup"
    else:
        spf_records = [r for r in spf_lookup.get("txt", []) if r.lower().startswith("v=spf1")]
        if not spf_records:
            issues.append("SPF не найден")
        elif len(spf_records) > 1:
            issues.append(f"Найдено несколько SPF записей ({len(spf_records)})")
            spf_info = short_record(spf_records[0])
        else:
            spf_info = short_record(spf_records[0])
            if re.search(r"(?i)(^|\s)\+all(\s|$)", spf_records[0]):
                warns.append("SPF содержит +all (слишком широкая политика)")

    dmarc_info = "не найден"
    if not dmarc_lookup.get("ok"):
        warns.append(f"DMARC DNS lookup error: {dmarc_lookup.get('error')}")
        dmarc_info = "ошибка DNS lookup"
    else:
        dmarc_records = [r for r in dmarc_lookup.get("txt", []) if r.lower().startswith("v=dmarc1")]
        if not dmarc_records:
            issues.append("DMARC не найден")
        elif len(dmarc_records) > 1:
            issues.append(f"Найдено несколько DMARC записей ({len(dmarc_records)})")
            dmarc_info = short_record(dmarc_records[0])
        else:
            dmarc_info = short_record(dmarc_records[0])
            p = (find_tag_value(dmarc_records[0], "p") or "").lower()
            if not p:
                warns.append("DMARC без p= политики")
            elif p == "none":
                warns.append("DMARC p=none (мониторинг без enforcement)")
            elif p not in {"quarantine", "reject"}:
                warns.append(f"DMARC p={p} (нестандартная политика)")

    lines.append(f"SPF: {spf_info}")
    lines.append(f"DMARC: {dmarc_info}")

    if issues:
        lines.append("Проблемы: " + "; ".join(issues))
    if warns:
        lines.append("Замечания: " + "; ".join(warns))

    if issues:
        status = "проблема"
    elif warns:
        status = "проверить"
    else:
        status = "ок"

    return status, lines, email


def compute_summary(item, audit):
    pages = audit.get("pages", [])
    discovery = audit.get("discovery", {})
    source_map = discovery.get("sources", {})

    forms = audit.get("forms", [])
    forbidden = audit.get("forbidden_hits", [])
    privacy = audit.get("privacy_links", [])
    cert_errors = audit.get("cert_errors", [])

    found_pages = select_found_pages_for_availability(pages, source_map)
    found_ok = [p for p in found_pages if p.get("status") == 200]
    found_bad = [p for p in found_pages if p.get("status") != 200]

    if found_pages and not found_ok:
        availability_status = "проблема"
        availability_poc = "Ни одна найденная страница сайта не открылась со статусом 200."
    elif found_bad:
        availability_status = "проверить"
        availability_poc = f"Часть найденных страниц недоступна: {len(found_bad)} из {len(found_pages)}."
    elif found_pages:
        availability_status = "ок"
        availability_poc = f"Найденные страницы доступны: {len(found_ok)} из {len(found_pages)}."
    else:
        availability_status = "проверить"
        availability_poc = "Нет найденных (не fallback) страниц для оценки доступности."

    cert_status = "ок" if not cert_errors else "проблема"

    bad_https_forms = [f for f in forms if "http://" in str(f.get("action_display", "")).lower()]
    form_https_status = "ок" if not bad_https_forms else "проблема"

    consent_buckets = {"текстом": [], "checked": [], "не найдено": [], "unchecked": []}
    for f in forms:
        consent_buckets[classify_form_consent(f)].append(f)
    consent_counts = {k: len(v) for k, v in consent_buckets.items()}

    if not forms:
        consent_status = "не найдено"
    else:
        negative_labels = []
        for label in ["checked", "не найдено", "текстом"]:
            if consent_buckets[label]:
                negative_labels.append(label)
        consent_status = " + ".join(negative_labels) if negative_labels else "unchecked"

    spf_dmarc_status, spf_dmarc_lines, email = evaluate_spf_dmarc(item, audit)
    spf_dmarc_poc = " | ".join(spf_dmarc_lines)

    meta_status = "ок" if not forbidden else "проблема"
    policy_status = "ок" if privacy else "проблема"

    if availability_status == "проблема":
        return {
            "site_unavailable": True,
            "availability_status": availability_status,
            "availability_poc": availability_poc,
            "cert_status": "-",
            "form_https_status": "-",
            "consent_status": "-",
            "spf_dmarc_status": "-",
            "meta_status": "-",
            "policy_status": "-",
            "result": "-",
            "bad_https_forms": [],
            "consent_buckets": {"текстом": [], "checked": [], "не найдено": [], "unchecked": []},
            "consent_counts": {"текстом": 0, "checked": 0, "не найдено": 0, "unchecked": 0},
            "spf_dmarc_poc": "Не проверено: сайт недоступен.",
            "spf_dmarc_lines": ["Не проверено: сайт недоступен."],
            "email": email,
        }

    return {
        "site_unavailable": False,
        "availability_status": availability_status,
        "availability_poc": availability_poc,
        "cert_status": cert_status,
        "form_https_status": form_https_status,
        "consent_status": consent_status,
        "spf_dmarc_status": spf_dmarc_status,
        "meta_status": meta_status,
        "policy_status": policy_status,
        "result": item.get("result", "проверить"),
        "bad_https_forms": bad_https_forms,
        "consent_buckets": consent_buckets,
        "consent_counts": consent_counts,
        "spf_dmarc_poc": spf_dmarc_poc,
        "spf_dmarc_lines": spf_dmarc_lines,
        "email": email,
    }


def row_html(row_num, site_id, clinic, site, s):
    external = site_url(site)
    return f"""
    <tr id=\"row-{esc(site_id)}\" class=\"clickable\" data-href=\"sites/{esc(site_id)}.html\" tabindex=\"0\">
      <td class=\"row-id\">{esc(row_num)}</td>
      <td><div class=\"clinic\">{esc(clinic)}</div></td>
      <td class=\"site\"><a class=\"site-link\" href=\"{esc(external)}\" target=\"_blank\" rel=\"noopener noreferrer\">{esc(site)}</a></td>
      <td class=\"availability-col\"><span class=\"badge availability-badge {badge_class(s['availability_status'])}\">{esc(s['availability_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['cert_status'])}\">{esc(s['cert_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['form_https_status'])}\">{esc(s['form_https_status'])}</span></td>
      <td><span class=\"badge consent-badge {badge_class(s['consent_status'])}\">{esc(s['consent_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['spf_dmarc_status'])}\">{esc(s['spf_dmarc_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['meta_status'])}\">{esc(s['meta_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['policy_status'])}\">{esc(s['policy_status'])}</span></td>
      <td><span class=\"badge {badge_class(s['result'])}\">{esc(s['result'])}</span></td>
    </tr>
    """


def details_section(title, status, lines):
    content = "".join(f"<li>{esc(line)}</li>" for line in lines) if lines else "<li>Нет данных</li>"
    return f"""
    <section class=\"block\">
      <h2>{esc(title)} <span class=\"badge {badge_class(status)}\">{esc(status)}</span></h2>
      <ul>{content}</ul>
    </section>
    """


def build_detail_page(item, audit, s):
    pages = audit.get("pages", [])
    forbidden = audit.get("forbidden_hits", [])
    privacy = audit.get("privacy_links", [])
    discovery = audit.get("discovery", {})
    source_map = discovery.get("sources", {})

    checked_pages_core = []
    checked_pages_extra = []
    fallback_pages = []
    for p in pages:
        req = p.get("requested")
        src = source_map.get(req, "")
        status_raw = p.get("status")
        status_txt = status_raw if status_raw is not None else "нет ответа"
        line = f"{req} — {status_txt} ({source_label(src)})"
        if p.get("error"):
            line += f" | {p.get('error')}"
        if src == "fallback":
            fallback_pages.append(line)
        else:
            if is_core_discovery_source(src):
                checked_pages_core.append(line)
            else:
                checked_pages_extra.append(line)

    found_pages = select_found_pages_for_availability(pages, source_map)
    found_ok = [p for p in found_pages if p.get("status") == 200]
    found_bad = [p for p in found_pages if p.get("status") != 200]

    if found_pages and not found_ok:
        found_pages_status = "проблема"
    elif found_bad:
        found_pages_status = "проверить"
    else:
        found_pages_status = "ок"

    cert_lines = []
    if s.get("site_unavailable"):
        cert_lines.append("Не проверено: сайт недоступен.")
    elif s["cert_status"] == "ок":
        cert_lines.append("HTTPS-страницы открылись без TLS-ошибок.")
    else:
        for ce in audit.get("cert_errors", []):
            cert_lines.append(f"{ce.get('url')} — {ce.get('error')}")

    form_https_lines = []
    if s.get("site_unavailable"):
        form_https_lines.append("Не проверено: сайт недоступен.")
    elif s["bad_https_forms"]:
        for f in s["bad_https_forms"][:40]:
            form_https_lines.append(f"{f.get('page')} | {f.get('form_id')} | {f.get('action_display')}")
    else:
        sample = sorted({str(f.get("action_display", "")) for f in audit.get("forms", [])})
        for x in sample[:5]:
            form_https_lines.append(f"Пример action: {x}")

    if s.get("site_unavailable"):
        consent_lines = ["Не проверено: сайт недоступен."]
    else:
        consent_lines = [
            f"Всего форм: {len(audit.get('forms', []))}",
            f"unchecked: {s['consent_counts']['unchecked']}",
            f"checked: {s['consent_counts']['checked']}",
            f"текстом: {s['consent_counts']['текстом']}",
            f"не найдено: {s['consent_counts']['не найдено']}",
        ]
        for label in ["не найдено", "checked", "текстом"]:
            for f in s["consent_buckets"][label][:20]:
                consent_lines.append(f"{label} | {f.get('page')} | {f.get('form_id')} | {f.get('action_display')}")

    spf_lines = s.get("spf_dmarc_lines") or [s["spf_dmarc_poc"]]

    meta_lines = []
    if s.get("site_unavailable"):
        meta_lines.append("Не проверено: сайт недоступен.")
    elif forbidden:
        for h in forbidden[:80]:
            meta_lines.append(f"{h.get('token')} | {h.get('page')} | {h.get('visibility')} | {h.get('context')}")
    else:
        meta_lines.append("Совпадений по списку meta/instagram/facebook/whatsapp/messenger/threads не найдено.")

    policy_lines = []
    if s.get("site_unavailable"):
        policy_lines.append("Не проверено: сайт недоступен.")
    elif privacy:
        for x in privacy[:40]:
            policy_lines.append(f"{x.get('page')} -> {x.get('href')} (текст: {x.get('text')})")
    else:
        policy_lines.append("Ссылка на политику не найдена.")

    availability_lines = [s["availability_poc"]]
    if found_bad:
        availability_lines.append(f"Недоступных найденных страниц: {len(found_bad)}.")

    sections = "".join([
        details_section("Доступность сайта", s["availability_status"], availability_lines),
        details_section("Страницы проверки (основные найденные)", found_pages_status, checked_pages_core or ["Нет основных найденных страниц."]),
        details_section("Доп. страницы из sitemap (формы)", "ок", checked_pages_extra) if checked_pages_extra else "",
        details_section("Fallback-пробы URL", "проверить" if fallback_pages else "ок", fallback_pages or ["Не применялся."]),
        details_section("Сертификат", s["cert_status"], cert_lines),
        details_section("Форма: HTTPS", s["form_https_status"], form_https_lines),
        details_section("Согласие", s["consent_status"], consent_lines),
        details_section("SPF/DMARC", s["spf_dmarc_status"], spf_lines),
        details_section("Meta / Instagram", s["meta_status"], meta_lines),
        details_section("Политика", s["policy_status"], policy_lines),
    ])

    return f"""<!doctype html>
<html lang=\"ru\">
<head>
<meta charset=\"utf-8\" />
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<title>{esc(item['clinic'])} — детали проверки</title>
<style>
body{{margin:0;font-family:Segoe UI,Arial,sans-serif;background:#f4f6fb;color:#1f2430}}
.wrap{{max-width:1100px;margin:24px auto;padding:0 16px 24px}}
.top{{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}}
.card{{background:#fff;border:1px solid #e5e8ef;border-radius:12px;padding:12px 14px}}
.block{{background:#fff;border:1px solid #e5e8ef;border-radius:12px;padding:14px 16px;margin-top:12px}}
h1{{margin:0;font-size:30px}}
h2{{margin:0 0 10px 0;font-size:18px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}}
a{{color:#2b4dd7;text-decoration:none}}
a:hover{{text-decoration:underline}}
ul{{margin:0;padding-left:18px;line-height:1.45}}
li{{margin:4px 0}}
.badge{{display:inline-block;padding:3px 9px;border-radius:999px;font-size:12px;font-weight:700;border:1px solid transparent}}
.ok{{background:#e8f8ef;color:#1d9e58;border-color:#c8efd9}}
.warn{{background:#fff6dd;color:#b67a00;border-color:#f0d889}}
.bad{{background:#ffe9ea;color:#c5333a;border-color:#f7c4c8}}
.na{{background:#f0f2f6;color:#6c7280;border-color:#dde2ea}}
.alert{{margin-top:12px;border-radius:10px;padding:10px 12px;border:1px solid transparent;font-size:14px;font-weight:600}}
.alert.ok{{background:#e8f8ef;color:#1d9e58;border-color:#c8efd9}}
.alert.warn{{background:#fff6dd;color:#b67a00;border-color:#f0d889}}
.alert.bad{{background:#ffe9ea;color:#c5333a;border-color:#f7c4c8}}
</style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"top\">
      <div>
        <a href=\"../dashboard.html\">← Назад к дашборду</a>
        <h1>{esc(item['clinic'])}</h1>
        <div>{esc(item['site'])}</div>
      </div>
      <div class=\"card\">Итог: <span class=\"badge {badge_class(s['result'])}\">{esc(s['result'])}</span></div>
    </div>
    <div class=\"alert {badge_class(s['availability_status'])}\">Доступность: {esc(s['availability_status'])}. {esc(s['availability_poc'])}</div>

    {sections}

    <section class=\"block\">
      <h2>Сырые данные</h2>
      <ul>
        <li>Audit JSON: <a href=\"../{esc(item['audit_file'])}\">{esc(item['audit_file'])}</a></li>
      </ul>
    </section>
  </div>
</body>
</html>
"""


def main():
    manifest = read_json(MANIFEST)
    rows = []
    details = []
    counts = {"слать": 0, "проверить": 0, "не слать": 0}
    unavailable = 0

    for idx, item in enumerate(manifest, 1):
        audit_path = ROOT / item["audit_file"]
        audit = read_json(audit_path)
        summary = compute_summary(item, audit)
        counts[summary["result"]] = counts.get(summary["result"], 0) + 1
        if summary["availability_status"] == "проблема":
            unavailable += 1
        rows.append(row_html(idx, item["id"], item["clinic"], item["site"], summary))
        details.append((item["id"], build_detail_page(item, audit, summary)))

    dashboard = f"""<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Скрининг клиник — dashboard</title>
  <style>
    :root {{
      --line: #e6e8ef; --text: #1f2430; --muted: #707887;
      --ok-bg: #e8f8ef; --ok-fg: #1d9e58;
      --warn-bg: #fff6dd; --warn-fg: #b67a00;
      --bad-bg: #ffe9ea; --bad-fg: #c5333a;
      --na-bg: #f0f2f6; --na-fg: #6c7280;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family:Segoe UI,Arial,sans-serif; background:linear-gradient(180deg,#f8f9fc 0%,#f3f5fa 100%); color:var(--text); }}
    .wrap {{ max-width:calc(100vw - 12px); margin:14px auto; padding:0 6px 16px; }}
    h1 {{ margin:0; font-size:40px; letter-spacing:-0.02em; }}
    .sub {{ margin-top:8px; color:var(--muted); font-size:18px; }}
    .meta {{ margin-top:10px; font-size:13px; color:#7f8695; }}
    .cards {{ margin-top:14px; display:grid; grid-template-columns:repeat(5,minmax(110px,1fr)); gap:8px; }}
    .card {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; }}
    .card .n {{ font-size:24px; line-height:1; font-weight:800; }}
    .card .l {{ margin-top:3px; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); font-weight:700; }}
    .n.ok{{color:var(--ok-fg)}} .n.warn{{color:var(--warn-fg)}} .n.bad{{color:var(--bad-fg)}} .n.na{{color:var(--na-fg)}} .n.total{{color:#111827}}
    .table-wrap {{ margin-top:12px; background:#fff; border:1px solid var(--line); border-radius:12px; overflow-x:hidden; }}
    table {{ width:100%; min-width:0; border-collapse:collapse; table-layout:fixed; }}
    thead th {{ text-align:left; background:#fafbfe; border-bottom:1px solid var(--line); color:#576072; font-size:11px; letter-spacing:.04em; text-transform:uppercase; padding:10px 8px; white-space:normal; line-height:1.2; }}
    thead th.availability-col {{ background:#ecf4ff; color:#214b86; border-left:2px solid #cfe1ff; border-right:2px solid #cfe1ff; }}
    tbody td {{ border-bottom:1px solid var(--line); padding:8px 8px; font-size:12px; vertical-align:top; line-height:1.25; }}
    tbody td.availability-col {{ background:#f5faff; border-left:2px solid #cfe1ff; border-right:2px solid #cfe1ff; font-weight:700; }}
    .availability-badge {{ font-size:13px; padding:4px 9px; }}
    tbody tr:last-child td {{ border-bottom:0; }}
    .clickable{{cursor:pointer}} .clickable:hover{{background:#f7f9ff}}
    .site {{ color:#5e6678; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:11px; overflow-wrap:anywhere; }}
    .site-link {{ color:#455066; text-decoration:none; border-bottom:1px dotted #9aa6bd; }}
    .site-link:hover {{ color:#24324f; text-decoration:none; border-bottom-color:#24324f; }}
    .clinic {{ font-weight:700; font-size:11px; overflow-wrap:anywhere; }}
    .badge {{ display:inline-block; padding:3px 8px; border-radius:999px; border:1px solid transparent; font-size:11px; font-weight:700; line-height:1.2; white-space:nowrap; }}
    .consent-badge {{ font-size:10px; padding:3px 7px; white-space:normal; line-height:1.15; max-width:100%; }}
    .ok {{ background:var(--ok-bg); color:var(--ok-fg); border-color:#c8efd9; }}
    .warn {{ background:var(--warn-bg); color:var(--warn-fg); border-color:#f0d889; }}
    .bad {{ background:var(--bad-bg); color:var(--bad-fg); border-color:#f7c4c8; }}
    .na {{ background:var(--na-bg); color:var(--na-fg); border-color:#dde2ea; }}
    .notes {{ margin-top:10px; background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; font-size:12px; color:#4e5565; line-height:1.35; }}
    .row-id {{ color:#6a7385; font-weight:700; }}
    thead th:nth-child(1), tbody td:nth-child(1) {{ width:4%; }}
    thead th:nth-child(2), tbody td:nth-child(2) {{ width:14%; }}
    thead th:nth-child(3), tbody td:nth-child(3) {{ width:10%; }}
    thead th:nth-child(4), tbody td:nth-child(4) {{ width:14%; }}
    thead th:nth-child(5), tbody td:nth-child(5) {{ width:9%; }}
    thead th:nth-child(6), tbody td:nth-child(6) {{ width:8%; }}
    thead th:nth-child(7), tbody td:nth-child(7) {{ width:12%; }}
    thead th:nth-child(8), tbody td:nth-child(8) {{ width:8%; }}
    thead th:nth-child(9), tbody td:nth-child(9) {{ width:9%; }}
    thead th:nth-child(10), tbody td:nth-child(10) {{ width:6%; }}
    thead th:nth-child(11), tbody td:nth-child(11) {{ width:6%; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Скрининг клиник</h1>
    <div class=\"sub\">Быстрая проверка по ключевым критериям</div>
    <div class=\"meta\">Клик по строке открывает страницу с деталями и PoC</div>

    <div class=\"cards\">
      <div class=\"card\"><div class=\"n ok\">{counts.get('слать', 0)}</div><div class=\"l\">Слать</div></div>
      <div class=\"card\"><div class=\"n warn\">{counts.get('проверить', 0)}</div><div class=\"l\">Проверить</div></div>
      <div class=\"card\"><div class=\"n na\">{counts.get('не слать', 0)}</div><div class=\"l\">Не слать</div></div>
      <div class=\"card\"><div class=\"n bad\">{unavailable}</div><div class=\"l\">Недоступны</div></div>
      <div class=\"card\"><div class=\"n total\">{len(manifest)}</div><div class=\"l\">Всего</div></div>
    </div>

    <div class=\"table-wrap\">
      <table>
        <thead>
          <tr>
            <th>ID</th><th>Клиника</th><th>Сайт</th><th class=\"availability-col\">Доступность сайта</th><th>Сертификат</th><th>Форма: HTTPS</th><th>Согласие</th><th>SPF / DMARC</th><th>Meta / Instagram</th><th>Политика</th><th>Итог</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </div>

    <div class=\"notes\">
      Для масштабирования на новые сайты: добавьте audit JSON в <code>data/audits</code>, запись в <code>data/sites_manifest.json</code>, затем запустите <code>python scripts/build_dashboard.py</code>.
    </div>
  </div>
  <script>
    document.querySelectorAll('tr.clickable').forEach(function(row){{
      row.addEventListener('click', function(){{ window.location.href = row.dataset.href; }});
      row.addEventListener('keydown', function(e){{
        if(e.target && e.target.closest && e.target.closest('a')) return;
        if(e.key === 'Enter' || e.key === ' '){{ e.preventDefault(); window.location.href = row.dataset.href; }}
      }});
    }});
    document.querySelectorAll('a.site-link').forEach(function(link){{
      link.addEventListener('click', function(e){{ e.stopPropagation(); }});
    }});
  </script>
</body>
</html>
"""

    (ROOT / "dashboard.html").write_text(dashboard, encoding="utf-8")

    sites_dir = ROOT / "sites"
    sites_dir.mkdir(parents=True, exist_ok=True)
    for site_id, page in details:
        (sites_dir / f"{site_id}.html").write_text(page, encoding="utf-8")

    print("Generated:")
    print(ROOT / "dashboard.html")
    for site_id, _ in details:
        print(sites_dir / f"{site_id}.html")


if __name__ == "__main__":
    main()
