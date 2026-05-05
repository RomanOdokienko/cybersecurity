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
PLACEHOLDER_EMAIL_DOMAINS = {
    "example.com", "example.org", "example.net", "domain.ru", "domain.com", "test.com"
}
PLACEHOLDER_EMAIL_LOCALS = {
    "example", "sample", "test", "username", "yourname", "name", "mail@example"
}
META_STRICT_TOKENS = {
    "meta",
    "мета",
    "instagram",
    "инстаграм",
    "инстаграмм",
    "facebook",
    "фейсбук",
    "threads",
    "instagram.com",
    "facebook.com",
    "fb.com",
    "meta.com",
    "threads.net",
}
META_IGNORED_TOKENS = {
    "whatsapp",
    "вотсап",
    "ватсап",
    "wa.me",
    "messenger",
    "мессенджер фейсбук",
    "m.me",
}
DKIM_SELECTOR_CANDIDATES = [
    "default",
    "selector1",
    "selector2",
    "mail",
    "mx",
    "google",
    "dkim",
]


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
        "частично": "warn",
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
        "sitemap-legal": "sitemap: правовая/документная страница",
        "navigation-legal": "навигация: правовая/документная страница",
        "policy-hint": "найдено по policy-hint в коде",
        "policy-fallback": "fallback: типовой путь политики",
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


def filter_meta_hits(raw_hits):
    filtered = []
    seen = set()
    for hit in raw_hits or []:
        tok = str(hit.get("token", "") or "").strip().lower()
        if not tok or tok in META_IGNORED_TOKENS or tok not in META_STRICT_TOKENS:
            continue
        ctx = str(hit.get("context", "") or "")
        # Historic noise: UIkit classes like uk-text-meta are not Meta links.
        if ctx.lower().startswith("class="):
            continue
        page = str(hit.get("page", "") or "")
        vis = str(hit.get("visibility", "") or "")
        key = (tok, page, ctx, vis)
        if key in seen:
            continue
        seen.add(key)
        filtered.append(hit)
    return filtered


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


def is_placeholder_email(email: str) -> bool:
    e = str(email or "").strip().lower()
    if "@" not in e:
        return False
    local, domain = e.rsplit("@", 1)
    local = local.strip()
    domain = domain.strip().strip(".")
    if domain in PLACEHOLDER_EMAIL_DOMAINS:
        return True
    if local in PLACEHOLDER_EMAIL_LOCALS:
        return True
    if local.startswith("example") or local.startswith("sample") or local.startswith("test"):
        return True
    return False


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
        if is_placeholder_email(em):
            continue
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
        ], "", "проверить", "проверить"

    email = cand["email"]
    domain = cand["domain"]
    source_label_txt = "манифест" if cand["source"] == "manifest" else "найден на сайте"

    if cand["is_external"]:
        return "н/п", [
            f"Email: {email} ({source_label_txt})",
            f"Домен {domain} — сторонний почтовый сервис, не почтовый домен клиники.",
            "Для этой проверки SPF/DMARC не оценивается.",
            "Что проверить дальше: найти корпоративный email домена клиники и проверить SPF/DMARC уже для него.",
        ], email, "н/п", "н/п"

    spf_lookup = dns_txt_records(domain)
    dmarc_lookup = dns_txt_records(f"_dmarc.{domain}")

    critical_issues = []
    issues = []
    warns = []
    lines = [
        f"Email: {email} ({source_label_txt})",
        f"Домен проверки: {domain}",
    ]

    spf_info = "не найден"
    spf_missing = False
    spf_status = "ок"
    if not spf_lookup.get("ok"):
        warns.append(f"SPF DNS lookup error: {spf_lookup.get('error')}")
        spf_info = "ошибка DNS lookup"
        spf_status = "проверить"
    else:
        spf_records = [r for r in spf_lookup.get("txt", []) if r.lower().startswith("v=spf1")]
        if not spf_records:
            issues.append("SPF не найден")
            spf_missing = True
            spf_status = "проблема"
        elif len(spf_records) > 1:
            critical_issues.append(f"Найдено несколько SPF записей ({len(spf_records)})")
            spf_info = short_record(spf_records[0])
            spf_status = "проблема"
        else:
            spf_info = short_record(spf_records[0])
            if re.search(r"(?i)(^|\s)\+all(\s|$)", spf_records[0]):
                warns.append("SPF содержит +all (слишком широкая политика)")
                spf_status = "проверить"

    dmarc_info = "не найден"
    dmarc_missing = False
    dmarc_status = "ок"
    if not dmarc_lookup.get("ok"):
        warns.append(f"DMARC DNS lookup error: {dmarc_lookup.get('error')}")
        dmarc_info = "ошибка DNS lookup"
        dmarc_status = "проверить"
    else:
        dmarc_records = [r for r in dmarc_lookup.get("txt", []) if r.lower().startswith("v=dmarc1")]
        if not dmarc_records:
            issues.append("DMARC не найден")
            dmarc_missing = True
            dmarc_status = "проблема"
        elif len(dmarc_records) > 1:
            issues.append(f"Найдено несколько DMARC записей ({len(dmarc_records)})")
            dmarc_info = short_record(dmarc_records[0])
            dmarc_status = "проблема"
        else:
            dmarc_info = short_record(dmarc_records[0])
            p = (find_tag_value(dmarc_records[0], "p") or "").lower()
            if not p:
                warns.append("DMARC без p= политики")
                dmarc_status = "проверить"
            elif p == "none":
                warns.append("DMARC p=none (мониторинг без enforcement)")
                dmarc_status = "проверить"
            elif p not in {"quarantine", "reject"}:
                warns.append(f"DMARC p={p} (нестандартная политика)")
                dmarc_status = "проверить"

    lines.append(f"SPF: {spf_info}")
    lines.append(f"DMARC: {dmarc_info}")

    if spf_missing and dmarc_missing:
        critical_issues.append("SPF и DMARC не найдены")

    if critical_issues:
        lines.append("Проблемы: " + "; ".join(critical_issues))
    if issues:
        lines.append("Замечания: " + "; ".join(issues))
    if warns:
        lines.append("Замечания: " + "; ".join(warns))

    if critical_issues:
        status = "проблема"
    elif issues or warns:
        status = "проверить"
    else:
        status = "ок"

    return status, lines, email, spf_status, dmarc_status


def evaluate_dkim(item, audit):
    cand = pick_email_candidate(item, audit)
    if not cand:
        return "проверить", ["Email для проверки DKIM не найден."], ""

    email = cand["email"]
    domain = cand["domain"]
    source_label_txt = "манифест" if cand["source"] == "manifest" else "найден на сайте"
    if cand["is_external"]:
        return "н/п", [
            f"Email: {email} ({source_label_txt})",
            f"Домен {domain} — сторонний почтовый сервис, DKIM селекторы домена клиники не проверяются.",
        ], email

    found_selectors = []
    checked_hosts = []
    dns_errors = []

    for selector in DKIM_SELECTOR_CANDIDATES:
        host = f"{selector}._domainkey.{domain}"
        checked_hosts.append(host)
        lookup = dns_txt_records(host)
        if not lookup.get("ok"):
            dns_errors.append(f"{host}: {lookup.get('error')}")
            continue
        txt_records = lookup.get("txt", []) or []
        dkim_records = [r for r in txt_records if "v=dkim1" in r.lower()]
        if dkim_records:
            found_selectors.append(selector)

    if found_selectors:
        return "ок", [
            f"Email: {email} ({source_label_txt})",
            f"Домен проверки: {domain}",
            f"Найдены DKIM селекторы: {', '.join(found_selectors)}",
        ], email

    lines = [
        f"Email: {email} ({source_label_txt})",
        f"Домен проверки: {domain}",
        "DKIM запись по типовым селекторам не найдена.",
        "Проверено: " + ", ".join(checked_hosts),
    ]
    if dns_errors:
        lines.append("DNS ошибки: " + "; ".join(dns_errors[:4]))
    return "проверить", lines, email


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def apply_block2_speed_ranking(summaries):
    scored = []
    for summary in summaries:
        tech = summary.get("audit", {}).get("tech", {}) or {}
        ps = tech.get("pagespeed", {}) or {}
        score = to_float(ps.get("score"))
        lcp = to_float(ps.get("lcp_seconds"))
        if score is None and lcp is None:
            continue
        scored.append({"summary": summary, "score": score, "lcp": lcp})

    if not scored:
        for summary in summaries:
            if summary["summary"].get("site_unavailable"):
                summary["summary"]["b2_speed_status"] = "-"
            else:
                summary["summary"]["b2_speed_status"] = "проверить"
        return

    def rank_key(x):
        score = x["score"]
        lcp = x["lcp"]
        score_key = -score if score is not None else 10_000
        lcp_key = lcp if lcp is not None else 10_000
        return (score_key, lcp_key)

    scored_sorted = sorted(scored, key=rank_key)
    total = len(scored_sorted)
    for i, item in enumerate(scored_sorted, 1):
        pct = i / total
        if pct <= 0.33:
            status = "ок"
        elif pct <= 0.66:
            status = "проверить"
        else:
            status = "проблема"
        item["summary"]["summary"]["b2_speed_status"] = status
        item["summary"]["summary"]["b2_speed_rank"] = i
        item["summary"]["summary"]["b2_speed_rank_total"] = total

    for summary in summaries:
        if summary["summary"].get("b2_speed_status"):
            continue
        if summary["summary"].get("site_unavailable"):
            summary["summary"]["b2_speed_status"] = "-"
        else:
            summary["summary"]["b2_speed_status"] = "проверить"


def block2_statuses(audit, site_unavailable):
    if site_unavailable:
        return {
            "online_slots_status": "-",
            "digital_tool_status": "-",
            "analytics_status": "-",
            "remarketing_status": "-",
            "after_hours_status": "-",
            "anonymous_status": "-",
        }

    discovery = audit.get("discovery", {}) or {}
    forms = audit.get("forms", []) or []
    tech = audit.get("tech", {}) or {}
    analytics = tech.get("analytics", {}) or {}
    engagement = tech.get("engagement", {}) or {}
    remarketing = tech.get("remarketing", {}) or {}

    has_slot_booking = bool(engagement.get("slot_booking_widget"))
    online_slots_status = "ок" if has_slot_booking else "проверить"

    sitemap_total = int(discovery.get("sitemap_total_urls") or 0)
    pages_count = len(audit.get("pages", []) or [])
    functional_flags = 0
    functional_flags += 1 if has_slot_booking else 0
    functional_flags += 1 if bool(analytics.get("found")) else 0
    functional_flags += 1 if bool(remarketing.get("found")) else 0
    functional_flags += 1 if bool(engagement.get("whatsapp") or engagement.get("telegram") or engagement.get("chat_widget")) else 0
    functional_flags += 1 if len(forms) >= 10 else 0

    if sitemap_total >= 35 and functional_flags >= 3:
        digital_tool_status = "ок"
    elif sitemap_total >= 15 or functional_flags >= 2 or pages_count >= 12:
        digital_tool_status = "частично"
    else:
        digital_tool_status = "проверить"

    has_analytics = bool(analytics.get("found"))
    has_goals = bool(analytics.get("goals_found"))
    if has_analytics and has_goals:
        analytics_status = "ок"
    elif has_analytics:
        analytics_status = "проверить"
    else:
        analytics_status = "проблема"

    if remarketing.get("found") is True:
        remarketing_status = "ок"
    elif has_analytics:
        remarketing_status = "проверить"
    else:
        remarketing_status = "проверить"

    has_async_channel = bool(
        engagement.get("whatsapp")
        or engagement.get("telegram")
        or engagement.get("chat_widget")
        or len(forms) > 0
    )
    after_hours_status = "ок" if has_async_channel else "проверить"

    has_chat = bool(engagement.get("chat_widget"))
    has_text_question_without_required_phone = any(
        bool(f.get("has_textarea")) and not bool(f.get("phone_required"))
        for f in forms
    )
    if has_chat or has_text_question_without_required_phone:
        anonymous_status = "ок"
    elif len(forms) > 0:
        anonymous_status = "проверить"
    else:
        anonymous_status = "проверить"

    return {
        "online_slots_status": online_slots_status,
        "digital_tool_status": digital_tool_status,
        "analytics_status": analytics_status,
        "remarketing_status": remarketing_status,
        "after_hours_status": after_hours_status,
        "anonymous_status": anonymous_status,
    }


def block3_statuses(audit, site_unavailable, cert_status, spf_status, dmarc_status, dkim_status):
    if site_unavailable:
        return {
            "ssl_valid_status": "-",
            "ssl_expiry_status": "-",
            "http_to_https_status": "-",
            "hsts_status": "-",
            "mixed_content_status": "-",
            "security_headers_status": "-",
            "spf_status": "-",
            "dmarc_status": "-",
            "dkim_status": "-",
            "broken_internal_links_status": "-",
            "broken_static_resources_status": "-",
            "ttfb_status": "-",
            "pagespeed_status": "-",
            "canonical_status": "-",
            "analytics_goals_status": "-",
        }

    tech = audit.get("tech", {}) or {}
    ssl_info = tech.get("ssl", {}) or {}
    http_to_https = tech.get("http_to_https", {}) or {}
    ttfb = tech.get("ttfb", {}) or {}
    pagespeed = tech.get("pagespeed", {}) or {}
    canonical = tech.get("canonical_www", {}) or {}
    analytics = tech.get("analytics", {}) or {}
    mixed = tech.get("mixed_content", {}) or {}
    broken_links = tech.get("broken_internal_links", {}) or {}
    broken_resources = tech.get("broken_static_resources", {}) or {}
    sec_headers = tech.get("security_headers", {}) or {}

    ssl_expiry_status = "проверить"
    if cert_status == "проблема":
        ssl_expiry_status = "проблема"
    else:
        days_left = ssl_info.get("days_left")
        if isinstance(days_left, int):
            if days_left < 0:
                ssl_expiry_status = "проблема"
            elif days_left <= 14:
                ssl_expiry_status = "проблема"
            elif days_left <= 45:
                ssl_expiry_status = "проверить"
            else:
                ssl_expiry_status = "ок"

    hsts_value = str((sec_headers.get("values") or {}).get("strict-transport-security") or "").lower()
    if not hsts_value:
        hsts_status = "проблема"
    elif "max-age=0" in hsts_value:
        hsts_status = "проблема"
    else:
        hsts_status = "ок"

    redirect_flag = http_to_https.get("redirected_to_https")
    if redirect_flag is True:
        http_to_https_status = "ок"
    elif redirect_flag is False:
        http_to_https_status = "проблема"
    else:
        http_to_https_status = "проверить"

    ttfb_sec = to_float(ttfb.get("seconds"))
    if ttfb_sec is None:
        ttfb_status = "проверить"
    elif ttfb_sec <= 0.8:
        ttfb_status = "ок"
    elif ttfb_sec <= 1.8:
        ttfb_status = "проверить"
    else:
        ttfb_status = "проблема"

    ps_score = to_float(pagespeed.get("score"))
    ps_lcp = to_float(pagespeed.get("lcp_seconds"))
    if ps_score is None and ps_lcp is None:
        pagespeed_status = "проверить"
    else:
        if (ps_score is not None and ps_score < 50) or (ps_lcp is not None and ps_lcp > 4.0):
            pagespeed_status = "проблема"
        elif (ps_score is not None and ps_score < 75) or (ps_lcp is not None and ps_lcp > 2.5):
            pagespeed_status = "проверить"
        else:
            pagespeed_status = "ок"

    checked_links = int(broken_links.get("checked") or 0)
    broken_links_count = int(broken_links.get("broken") or 0)
    if checked_links == 0:
        broken_internal_links_status = "проверить"
    elif broken_links_count == 0:
        broken_internal_links_status = "ок"
    elif broken_links_count <= 3 and (broken_links_count / max(1, checked_links)) <= 0.05:
        broken_internal_links_status = "проверить"
    else:
        broken_internal_links_status = "проблема"

    checked_res = int(broken_resources.get("checked") or 0)
    broken_res_count = int(broken_resources.get("broken") or 0)
    if checked_res == 0:
        broken_static_resources_status = "проверить"
    elif broken_res_count == 0:
        broken_static_resources_status = "ок"
    elif broken_res_count <= 2 and (broken_res_count / max(1, checked_res)) <= 0.03:
        broken_static_resources_status = "проверить"
    else:
        broken_static_resources_status = "проблема"

    canonical_same = canonical.get("same_canonical")
    if canonical_same is True:
        canonical_status = "ок"
    elif canonical_same is False:
        canonical_status = "проблема"
    else:
        canonical_status = "проверить"

    analytics_found = analytics.get("found")
    goals_found = analytics.get("goals_found")
    if analytics_found is True and goals_found is True:
        analytics_goals_status = "ок"
    elif analytics_found is True and goals_found is not True:
        analytics_goals_status = "проверить"
    elif analytics_found is False:
        analytics_goals_status = "проблема"
    else:
        analytics_goals_status = "проверить"

    mixed_count = mixed.get("count")
    if isinstance(mixed_count, int):
        mixed_content_status = "проблема" if mixed_count > 0 else "ок"
    else:
        mixed_content_status = "проверить"

    baseline_headers = [
        "content-security-policy",
        "x-frame-options",
        "x-content-type-options",
        "referrer-policy",
    ]
    present = set(sec_headers.get("present") or [])
    if not present and sec_headers.get("missing") is None:
        security_headers_status = "проверить"
    else:
        missing_count = len([h for h in baseline_headers if h not in present])
        if missing_count == 0:
            security_headers_status = "ок"
        elif missing_count == 1:
            security_headers_status = "проверить"
        else:
            security_headers_status = "проблема"

    ssl_valid_status = cert_status if cert_status in {"ок", "проблема"} else "проверить"

    return {
        "ssl_valid_status": ssl_valid_status,
        "ssl_expiry_status": ssl_expiry_status,
        "http_to_https_status": http_to_https_status,
        "hsts_status": hsts_status,
        "mixed_content_status": mixed_content_status,
        "security_headers_status": security_headers_status,
        "spf_status": spf_status or "проверить",
        "dmarc_status": dmarc_status or "проверить",
        "dkim_status": dkim_status or "проверить",
        "broken_internal_links_status": broken_internal_links_status,
        "broken_static_resources_status": broken_static_resources_status,
        "ttfb_status": ttfb_status,
        "pagespeed_status": pagespeed_status,
        "canonical_status": canonical_status,
        "analytics_goals_status": analytics_goals_status,
    }


def block4_statuses(audit, site_unavailable):
    if site_unavailable:
        return {
            "price_public_status": "-",
            "doctors_page_status": "-",
            "address_map_status": "-",
            "hours_status": "-",
            "reviews_status": "-",
            "services_pages_status": "-",
            "nap_consistency_status": "-",
            "clickable_contacts_status": "-",
            "contacts_page_status": "-",
            "doctor_cards_status": "-",
            "schema_medical_status": "-",
        }

    tech = audit.get("tech", {}) or {}
    med = tech.get("med_trust", {}) or {}
    discovery = audit.get("discovery", {}) or {}
    pages = audit.get("pages", []) or []
    schema = tech.get("schema", {}) or {}

    contact_urls = discovery.get("contact_urls", []) or []
    page_urls = [str(p.get("requested") or "") for p in pages]
    low_urls = [u.lower() for u in page_urls]

    def has_url_hint(hints):
        return any(any(h in u for h in hints) for u in low_urls)

    price_found = med.get("price_public_found")
    if price_found is None:
        price_found = has_url_hint(["/price", "/prices", "/prays", "/ceny", "/tseny", "/stoim", "/uslugi"])
    price_public_status = "ок" if price_found else "проблема"

    doctors_found = med.get("doctors_page_exists")
    if doctors_found is None:
        doctors_found = has_url_hint(["/doctor", "/doctors", "/vrach", "/vrachi", "/specialist", "/team"])
    doctors_page_status = "ок" if doctors_found else "проблема"

    address_found = med.get("address_found")
    map_found = med.get("map_found")
    if address_found is True and map_found is True:
        address_map_status = "ок"
    elif address_found is True or map_found is True:
        address_map_status = "проверить"
    elif address_found is False and map_found is False:
        address_map_status = "проблема"
    else:
        address_map_status = "проверить"

    hours_found = med.get("hours_found")
    if hours_found is True:
        hours_status = "ок"
    elif hours_found is False:
        hours_status = "проверить"
    else:
        hours_status = "проверить"

    reviews_found = med.get("reviews_found")
    if reviews_found is True:
        reviews_status = "ок"
    elif reviews_found is False:
        reviews_status = "проверить"
    else:
        reviews_status = "проверить"

    service_pages_count = med.get("service_pages_count")
    if isinstance(service_pages_count, int):
        if service_pages_count >= 3:
            services_pages_status = "ок"
        elif service_pages_count >= 1:
            services_pages_status = "проверить"
        else:
            services_pages_status = "проблема"
    else:
        services_pages_status = "проверить"

    nap = med.get("nap", {}) or {}
    nap_consistent = nap.get("consistent")
    if nap_consistent is True:
        nap_consistency_status = "ок"
    elif nap_consistent is False:
        nap_consistency_status = "проблема"
    else:
        nap_consistency_status = "проверить"

    clickable = med.get("clickable_contacts", {}) or {}
    has_tel = clickable.get("tel")
    has_mailto = clickable.get("mailto")
    if has_tel is True:
        clickable_contacts_status = "ок"
    elif has_mailto is True:
        clickable_contacts_status = "проверить"
    elif has_tel is False and has_mailto is False:
        clickable_contacts_status = "проблема"
    else:
        clickable_contacts_status = "проверить"

    contacts_exists = med.get("contact_page_exists")
    if contacts_exists is None:
        contacts_exists = bool(contact_urls) or has_url_hint(["/contact", "/contacts", "/kontakty"])
    contacts_page_status = "ок" if contacts_exists else "проблема"

    doctor_cards = med.get("doctor_cards", {}) or {}
    doctor_cards_complete = doctor_cards.get("complete")
    if doctor_cards_complete is True:
        doctor_cards_status = "ок"
    elif doctor_cards_complete is False and doctors_found:
        doctor_cards_status = "проверить"
    elif doctor_cards_complete is False and not doctors_found:
        doctor_cards_status = "проблема"
    else:
        doctor_cards_status = "проверить"

    med_schema = ((med.get("schema", {}) or {}).get("medical"))
    if med_schema is None:
        schema_types = [str(x).lower() for x in (schema.get("types") or [])]
        med_schema = any(x in {"medicalorganization", "medicalclinic", "physician", "dentist", "hospital"} for x in schema_types)
    if med_schema is True:
        schema_medical_status = "ок"
    elif med_schema is False:
        schema_medical_status = "проверить"
    else:
        schema_medical_status = "проверить"

    return {
        "price_public_status": price_public_status,
        "doctors_page_status": doctors_page_status,
        "address_map_status": address_map_status,
        "hours_status": hours_status,
        "reviews_status": reviews_status,
        "services_pages_status": services_pages_status,
        "nap_consistency_status": nap_consistency_status,
        "clickable_contacts_status": clickable_contacts_status,
        "contacts_page_status": contacts_page_status,
        "doctor_cards_status": doctor_cards_status,
        "schema_medical_status": schema_medical_status,
    }


def block_verified(audit, block_id: str) -> bool:
    verification = audit.get("verification", {}) or {}
    value = verification.get(block_id)
    return value is True


def compute_summary(item, audit):
    pages = audit.get("pages", [])
    discovery = audit.get("discovery", {})
    source_map = discovery.get("sources", {})

    forms = audit.get("forms", [])
    forbidden = filter_meta_hits(audit.get("forbidden_hits", []))
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

    ssl_info = (audit.get("tech", {}) or {}).get("ssl", {}) or {}
    ssl_ok = ssl_info.get("ok")
    if isinstance(ssl_ok, bool):
        cert_status = "ок" if ssl_ok and not cert_errors else "проблема"
    else:
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

    spf_dmarc_status, spf_dmarc_lines, email, spf_status, dmarc_status = evaluate_spf_dmarc(item, audit)
    dkim_status, dkim_lines, _ = evaluate_dkim(item, audit)
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
            "dkim_status": "-",
            "dkim_lines": ["Не проверено: сайт недоступен."],
            "email": email,
            "b2": block2_statuses(audit, True),
            "b3": block3_statuses(audit, True, "-", "-", "-", "-"),
            "b4": block4_statuses(audit, True),
            "block2_verified": block_verified(audit, "b2"),
            "block3_verified": block_verified(audit, "b3"),
            "block4_verified": block_verified(audit, "b4"),
        }

    b2 = block2_statuses(audit, False)
    b3 = block3_statuses(audit, False, cert_status, spf_status, dmarc_status, dkim_status)
    b4 = block4_statuses(audit, False)

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
        "dkim_status": dkim_status,
        "dkim_lines": dkim_lines,
        "email": email,
        "b2": b2,
        "b3": b3,
        "b4": b4,
        "block2_verified": block_verified(audit, "b2"),
        "block3_verified": block_verified(audit, "b3"),
        "block4_verified": block_verified(audit, "b4"),
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
      <td><input class=\"comment-input\" data-site-id=\"{esc(site_id)}\" type=\"text\" /></td>
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
    forbidden = filter_meta_hits(audit.get("forbidden_hits", []))
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
        meta_lines.append("Совпадений по списку meta/instagram/facebook/threads не найдено.")

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
        <a href=\"../dashboard.html\">← Скрининг клиник шаг 1</a>
        <span style=\"color:#9aa6bd\">&nbsp;·&nbsp;</span>
        <a href=\"../screening-step-2.html\">Скрининг клиник шаг 2 →</a>
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


def step2_block_schema():
    return [
        {
            "id": "b1",
            "title": "Блок 1",
            "metric_names": [
                "Пациент не давал согласия на обработку данных",
                "Согласие подставлено автоматически — это хуже чем его отсутствие",
                "На сайте нет обязательного документа об обработке данных пациентов",
                "Имя и телефон пациента передаются в открытом виде — любой может перехватить",
                "На сайте упоминается организация, признанная в России экстремистской",
                "Сайт собирает данные пациентов без их уведомления",
                "Яндекс.Метрика собирает данные ваших пациентов — в политике об этом ни слова",
            ],
        },
        {
            "id": "b2",
            "title": "Блок 2",
            "metric_names": [
                "Онлайн-запись со слотами (nice-to-have)",
                "Сайт — цифровая визитка, не инструмент",
                "Вы не знаете кто приходит на сайт и почему уходит",
                "Ушедший пациент потерян навсегда — нет ремаркетинга",
                "Скорость сайта на мобильном — место в рейтинге среди клиник",
                "Пациент не может написать в нерабочее время",
                "Нет возможности задать вопрос анонимно",
            ],
        },
        {
            "id": "b3",
            "title": "Блок 3",
            "metric_names": [
                "SSL валиден",
                "Срок действия SSL (дней до истечения)",
                "HTTP → HTTPS редирект",
                "HSTS включен",
                "Смешанный контент (HTTP на HTTPS)",
                "Security headers baseline (CSP/XFO/XCTO/Referrer)",
                "SPF запись",
                "DMARC запись + p=",
                "DKIM (селекторы/наличие)",
                "Битые внутренние ссылки (4xx/5xx)",
                "Битые статические ресурсы (JS/CSS/img)",
                "TTFB",
                "PageSpeed mobile + LCP",
                "www vs non-www canonical",
                "Веб-аналитика + цели/события",
            ],
        },
        {
            "id": "b4",
            "title": "Блок 4",
            "metric_names": [
                "Прайс-лист доступен без регистрации",
                "Страница врачей / специалистов",
                "Адрес и карта на сайте",
                "Часы работы",
                "Отзывы пациентов на сайте",
                "Ключевые услуги вынесены в отдельные страницы",
                "NAP consistency (название/адрес/телефон согласованы)",
                "Контакты кликабельны (tel:/mailto:)",
                "Есть отдельная страница контактов",
                "Карточки врачей: ФИО + специальность",
                "Schema.org: MedicalOrganization / Physician",
            ],
        },
    ]


def step2_blocks_data(summary):
    consent_counts = summary.get("consent_counts", {}) or {}
    site_unavailable = summary.get("site_unavailable", False)
    b2 = summary.get("b2", {}) or {}
    b3 = summary.get("b3", {}) or {}
    missing_checkbox = int(consent_counts.get("не найдено", 0)) > 0
    prechecked = int(consent_counts.get("checked", 0)) > 0
    cookie_status = "-" if site_unavailable else "проверить"
    third_party_policy_status = "-" if site_unavailable else "проверить"
    b4 = summary.get("b4", {}) or {}
    block2_default_status = "-" if site_unavailable else "проверить"
    block2_speed_status = summary.get("b2_speed_status", block2_default_status)
    block2_verified = bool(summary.get("block2_verified"))
    block3_verified = bool(summary.get("block3_verified"))
    block4_verified = bool(summary.get("block4_verified"))
    no_checkbox_status = "-" if site_unavailable else ("проблема" if missing_checkbox else "ок")
    prechecked_status = "-" if site_unavailable else ("проблема" if prechecked else "ок")

    b2_values = [
        b2.get("online_slots_status", block2_default_status),
        b2.get("digital_tool_status", block2_default_status),
        b2.get("analytics_status", block2_default_status),
        b2.get("remarketing_status", block2_default_status),
        block2_speed_status,
        b2.get("after_hours_status", block2_default_status),
        b2.get("anonymous_status", block2_default_status),
    ]
    if not block2_verified:
        b2_values = ["-"] * len(b2_values)

    b3_values = [
        b3.get("ssl_valid_status", summary["cert_status"]),
        b3.get("ssl_expiry_status", "-" if site_unavailable else "проверить"),
        b3.get("http_to_https_status", "-" if site_unavailable else "проверить"),
        b3.get("hsts_status", "-" if site_unavailable else "проверить"),
        b3.get("mixed_content_status", "-" if site_unavailable else "проверить"),
        b3.get("security_headers_status", "-" if site_unavailable else "проверить"),
        b3.get("spf_status", "-" if site_unavailable else "проверить"),
        b3.get("dmarc_status", "-" if site_unavailable else "проверить"),
        b3.get("dkim_status", "-" if site_unavailable else "проверить"),
        b3.get("broken_internal_links_status", "-" if site_unavailable else "проверить"),
        b3.get("broken_static_resources_status", "-" if site_unavailable else "проверить"),
        b3.get("ttfb_status", "-" if site_unavailable else "проверить"),
        b3.get("pagespeed_status", "-" if site_unavailable else "проверить"),
        b3.get("canonical_status", "-" if site_unavailable else "проверить"),
        b3.get("analytics_goals_status", "-" if site_unavailable else "проверить"),
    ]
    if not block3_verified:
        b3_values = ["-"] * len(b3_values)

    b4_values = [
        b4.get("price_public_status", "-" if site_unavailable else "проверить"),
        b4.get("doctors_page_status", "-" if site_unavailable else "проверить"),
        b4.get("address_map_status", "-" if site_unavailable else "проверить"),
        b4.get("hours_status", "-" if site_unavailable else "проверить"),
        b4.get("reviews_status", "-" if site_unavailable else "проверить"),
        b4.get("services_pages_status", "-" if site_unavailable else "проверить"),
        b4.get("nap_consistency_status", "-" if site_unavailable else "проверить"),
        b4.get("clickable_contacts_status", "-" if site_unavailable else "проверить"),
        b4.get("contacts_page_status", "-" if site_unavailable else "проверить"),
        b4.get("doctor_cards_status", "-" if site_unavailable else "проверить"),
        b4.get("schema_medical_status", "-" if site_unavailable else "проверить"),
    ]
    if not block4_verified:
        b4_values = ["-"] * len(b4_values)

    return {
        "b1": [
            no_checkbox_status,
            prechecked_status,
            summary["policy_status"],
            summary["form_https_status"],
            summary["meta_status"],
            cookie_status,
            third_party_policy_status,
        ],
        "b2": b2_values,
        "b3": b3_values,
        "b4": b4_values,
    }

def step2_header_rows(schema):
    top = ['<th class="id-col-head" rowspan="2">ID</th>', '<th class="clinic-col-head" rowspan="2">Клиника</th>']
    sub = []
    for block_idx, block in enumerate(schema):
        bid = block["id"]
        title = block["title"]
        col_count = len(block["metric_names"])
        edge_cls = " group-edge" if block_idx > 0 else ""
        top.append(
            f'<th class="group-{esc(bid)}{edge_cls}" colspan="{col_count}"><div class="col-head"><span class="col-title">{esc(title)}</span><button class="col-toggle" data-block-toggle="{esc(bid)}" type="button" aria-label="Свернуть блок" title="Свернуть блок">▾</button></div></th>'
        )
        ph_edge = " group-edge" if block_idx > 0 else ""
        sub.append(f'<th class="metric-col {esc(bid)}-ph metric-head-col block-ph{ph_edge} is-hidden-col"></th>')
        for metric_idx, metric_name in enumerate(block["metric_names"]):
            metric_edge = " group-edge" if block_idx > 0 and metric_idx == 0 else ""
            sub.append(f'<th class="metric-col {esc(bid)} metric-head-col{metric_edge}"><span class="metric-label">{esc(metric_name)}</span></th>')
    return "<tr>" + "".join(top) + "</tr><tr>" + "".join(sub) + "</tr>"


def row_html_step2(row_num, site_id, clinic, site, s, schema):
    block_values = step2_blocks_data(s)
    parts = [
        "<tr>",
        f'<td class="id-col">{esc(row_num)}</td>',
        (
            '<td class="clinic-col">'
            f'<div class="clinic-name" title="{esc(clinic)}">{esc(clinic)}</div>'
            "</td>"
        ),
    ]
    for block_idx, block in enumerate(schema):
        bid = block["id"]
        statuses = block_values.get(bid, [])
        ph_edge = " group-edge" if block_idx > 0 else ""
        parts.append(f'<td class="metric-col {esc(bid)}-ph block-ph{ph_edge} is-hidden-col"></td>')
        for metric_idx, status in enumerate(statuses):
            metric_edge = " group-edge" if block_idx > 0 and metric_idx == 0 else ""
            parts.append(f'<td class="metric-col {esc(bid)}{metric_edge}"><span class="badge {badge_class(status)}">{esc(status)}</span></td>')
    parts.append("</tr>")
    return "".join(parts)


def build_screening_step2(rows_step2, counts, unavailable, total, header_rows, block_col_counts_json):
    return f"""<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Скрининг клиник шаг 2</title>
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
    .meta-link {{ color:#455066; border-bottom:1px dotted #9aa6bd; text-decoration:none; }}
    .meta-link:hover {{ color:#24324f; border-bottom-color:#24324f; }}
    .cards {{ margin-top:14px; display:grid; grid-template-columns:repeat(5,minmax(110px,1fr)); gap:8px; }}
    .card {{ background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; }}
    .card .n {{ font-size:24px; line-height:1; font-weight:800; }}
    .card .l {{ margin-top:3px; font-size:11px; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); font-weight:700; }}
    .n.ok{{color:var(--ok-fg)}} .n.warn{{color:var(--warn-fg)}} .n.bad{{color:var(--bad-fg)}} .n.na{{color:var(--na-fg)}} .n.total{{color:#111827}}
    .table-wrap {{ margin-top:12px; background:#fff; border:1px solid var(--line); border-radius:12px; overflow-x:auto; }}
    table {{ width:100%; min-width:6200px; border-collapse:collapse; table-layout:fixed; }}
    thead th {{ text-align:left; background:#fafbfe; border-bottom:1px solid var(--line); color:#576072; font-size:10px; letter-spacing:.01em; text-transform:none; font-weight:700; padding:9px 8px; white-space:normal; line-height:1.2; }}
    tbody td {{ border-bottom:1px solid var(--line); padding:6px 8px; font-size:12px; vertical-align:top; line-height:1.2; }}
    tbody tr:last-child td {{ border-bottom:0; }}
    .site {{ color:#5e6678; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:11px; overflow-wrap:anywhere; }}
    .site-link {{ color:#455066; text-decoration:none; border-bottom:1px dotted #9aa6bd; }}
    .site-link:hover {{ color:#24324f; text-decoration:none; border-bottom-color:#24324f; }}
    .clinic {{ font-weight:700; font-size:11px; overflow-wrap:anywhere; }}
    .badge {{ display:inline-block; max-width:100%; padding:3px 8px; border-radius:999px; border:1px solid transparent; font-size:10px; font-weight:700; line-height:1.2; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .ok {{ background:var(--ok-bg); color:var(--ok-fg); border-color:#c8efd9; }}
    .warn {{ background:var(--warn-bg); color:var(--warn-fg); border-color:#f0d889; }}
    .bad {{ background:var(--bad-bg); color:var(--bad-fg); border-color:#f7c4c8; }}
    .na {{ background:var(--na-bg); color:var(--na-fg); border-color:#dde2ea; }}
    .id-col-head {{ width:52px; min-width:52px; position:sticky; left:0; z-index:4; box-shadow:1px 0 0 #e9edf6; text-align:center; }}
    .id-col {{ width:52px; min-width:52px; position:sticky; left:0; z-index:3; box-shadow:1px 0 0 #e9edf6; background:#fff; text-align:center; color:#6a7385; font-weight:700; font-size:11px; }}
    .clinic-col {{ background:#fff; width:280px; min-width:280px; position:sticky; left:52px; z-index:2; box-shadow:1px 0 0 #e9edf6; padding:8px 10px; }}
    .clinic-name {{ font-weight:500; font-size:14px; color:#1f2430; line-height:1.25; overflow-wrap:anywhere; }}
    .metric-col {{ text-align:center; background:#fcfdff; border-left:1px solid #edf1f7; min-width:140px; }}
    .group-edge {{ border-left:3px solid #d0d8e8 !important; }}
    .col-head {{ position:relative; display:flex; justify-content:flex-end; align-items:center; min-height:24px; }}
    .col-title {{ position:absolute; left:50%; transform:translateX(-50%); width:100%; padding:0 26px 0 8px; text-align:center; font-size:12px; font-weight:800; color:#394153; pointer-events:none; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .col-toggle {{ width:22px; height:22px; border:1px solid #ccd4e2; background:#fff; color:#2b3343; border-radius:7px; padding:0; font-size:14px; font-weight:700; line-height:1; cursor:pointer; }}
    .col-toggle:hover {{ border-color:#9fb2d3; color:#1d2a41; }}
    th.group-collapsed .col-head {{ justify-content:center; }}
    th.group-collapsed .col-title {{ display:none; }}
    .metric-label {{ display:block; }}
    .is-hidden-col {{ display:none !important; }}
    .notes {{ margin-top:10px; background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; font-size:12px; color:#4e5565; line-height:1.35; }}
    .clinic-col-head {{ width:280px; min-width:280px; position:sticky; left:52px; z-index:3; box-shadow:1px 0 0 #e9edf6; }}
    .metric-head-col {{ min-width:140px; font-size:10px; line-height:1.25; font-weight:600; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Скрининг клиник шаг 2</h1>
    <div class=\"sub\">Слева клиники, далее вертикальные блоки метрик по каждой клинике</div>
    <div class=\"meta\"><a class=\"meta-link\" href=\"dashboard.html\">← Скрининг клиник шаг 1</a> &nbsp;·&nbsp; <a class=\"meta-link\" href=\"final-report-blocks.html\">Блоки финального отчёта →</a></div>

    <div class=\"cards\">
      <div class=\"card\"><div class=\"n ok\">{counts.get('слать', 0)}</div><div class=\"l\">Слать</div></div>
      <div class=\"card\"><div class=\"n warn\">{counts.get('проверить', 0)}</div><div class=\"l\">Проверить</div></div>
      <div class=\"card\"><div class=\"n na\">{counts.get('не слать', 0)}</div><div class=\"l\">Не слать</div></div>
      <div class=\"card\"><div class=\"n bad\">{unavailable}</div><div class=\"l\">Недоступны</div></div>
      <div class=\"card\"><div class=\"n total\">{total}</div><div class=\"l\">Всего</div></div>
    </div>

    <div class=\"table-wrap\">
      <table>
        <thead>
          {header_rows}
        </thead>
        <tbody>
          {''.join(rows_step2)}
        </tbody>
      </table>
    </div>

    <div class=\"notes\">
      Сворачивание работает на уровне блока: кнопка в заголовке `Блок 1/2/3/4` скрывает или показывает все метрики этого блока сразу для всех клиник.
    </div>
  </div>
  <script>
    const BLOCK_COL_COUNTS = {block_col_counts_json};

    function setBlockCollapsed(blockId, collapsed) {{
      document.querySelectorAll('.' + blockId).forEach(function(el) {{
        el.classList.toggle('is-hidden-col', collapsed);
      }});
      document.querySelectorAll('.' + blockId + '-ph').forEach(function(el) {{
        el.classList.toggle('is-hidden-col', !collapsed);
      }});
      const groupHead = document.querySelector('.group-' + blockId);
      if (groupHead) {{
        groupHead.colSpan = collapsed ? 1 : (BLOCK_COL_COUNTS[blockId] || 1);
        groupHead.classList.toggle('group-collapsed', collapsed);
      }}
      const btn = document.querySelector('[data-block-toggle=\"' + blockId + '\"]');
      if (btn) {{
        btn.textContent = collapsed ? '▸' : '▾';
        const label = collapsed ? 'Развернуть блок' : 'Свернуть блок';
        btn.setAttribute('aria-label', label);
        btn.title = label;
      }}
    }}

    document.querySelectorAll('[data-block-toggle]').forEach(function(btn) {{
      btn.addEventListener('click', function() {{
        const blockId = btn.dataset.blockToggle;
        if (!blockId) return;
        const sample = document.querySelector('.' + blockId);
        const collapsed = sample ? sample.classList.contains('is-hidden-col') : false;
        setBlockCollapsed(blockId, !collapsed);
      }});
    }});
  </script>
</body>
</html>
"""


def main():
    manifest = read_json(MANIFEST)
    step2_schema = step2_block_schema()
    step2_headers = step2_header_rows(step2_schema)
    step2_col_counts = {block["id"]: len(block["metric_names"]) for block in step2_schema}
    rows = []
    rows_step2 = []
    details = []
    counts = {"слать": 0, "проверить": 0, "не слать": 0}
    unavailable = 0
    items_with_summary = []

    for idx, item in enumerate(manifest, 1):
        audit_path = ROOT / item["audit_file"]
        audit = read_json(audit_path)
        summary = compute_summary(item, audit)
        counts[summary["result"]] = counts.get(summary["result"], 0) + 1
        if summary["availability_status"] == "проблема":
            unavailable += 1
        items_with_summary.append({
            "idx": idx,
            "item": item,
            "audit": audit,
            "summary": summary,
        })

    apply_block2_speed_ranking(items_with_summary)

    for entry in items_with_summary:
        idx = entry["idx"]
        item = entry["item"]
        audit = entry["audit"]
        summary = entry["summary"]
        rows.append(row_html(idx, item["id"], item["clinic"], item["site"], summary))
        rows_step2.append(row_html_step2(idx, item["id"], item["clinic"], item["site"], summary, step2_schema))
        details.append((item["id"], build_detail_page(item, audit, summary)))

    dashboard = f"""<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Скрининг клиник шаг 1 — dashboard</title>
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
    .meta-link {{ color:#455066; border-bottom:1px dotted #9aa6bd; text-decoration:none; }}
    .meta-link:hover {{ color:#24324f; border-bottom-color:#24324f; }}
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
    .comment-input {{ width:100%; min-width:0; border:1px solid #d8deeb; border-radius:8px; padding:5px 7px; font-size:11px; color:#2b3343; background:#fff; }}
    .comment-input:focus {{ outline:none; border-color:#8db5ff; box-shadow:0 0 0 2px rgba(141,181,255,.22); }}
    .ok {{ background:var(--ok-bg); color:var(--ok-fg); border-color:#c8efd9; }}
    .warn {{ background:var(--warn-bg); color:var(--warn-fg); border-color:#f0d889; }}
    .bad {{ background:var(--bad-bg); color:var(--bad-fg); border-color:#f7c4c8; }}
    .na {{ background:var(--na-bg); color:var(--na-fg); border-color:#dde2ea; }}
    .notes {{ margin-top:10px; background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; font-size:12px; color:#4e5565; line-height:1.35; }}
    .row-id {{ color:#6a7385; font-weight:700; }}
    thead th:nth-child(1), tbody td:nth-child(1) {{ width:2%; }}
    thead th:nth-child(2), tbody td:nth-child(2) {{ width:10%; }}
    thead th:nth-child(3), tbody td:nth-child(3) {{ width:8%; }}
    thead th:nth-child(4), tbody td:nth-child(4) {{ width:12%; }}
    thead th:nth-child(5), tbody td:nth-child(5) {{ width:8%; }}
    thead th:nth-child(6), tbody td:nth-child(6) {{ width:7%; }}
    thead th:nth-child(7), tbody td:nth-child(7) {{ width:11%; }}
    thead th:nth-child(8), tbody td:nth-child(8) {{ width:8%; }}
    thead th:nth-child(9), tbody td:nth-child(9) {{ width:9%; }}
    thead th:nth-child(10), tbody td:nth-child(10) {{ width:6%; }}
    thead th:nth-child(11), tbody td:nth-child(11) {{ width:5%; }}
    thead th:nth-child(12), tbody td:nth-child(12) {{ width:14%; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Скрининг клиник шаг 1</h1>
    <div class=\"sub\">Быстрая проверка по ключевым критериям</div>
    <div class=\"meta\">Клик по строке открывает страницу с деталями и PoC &nbsp;·&nbsp; <a class=\"meta-link\" href=\"screening-step-2.html\">Скрининг клиник шаг 2 →</a> &nbsp;·&nbsp; <a class=\"meta-link\" href=\"final-report-blocks.html\">Блоки финального отчёта →</a></div>

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
            <th>ID</th><th>Клиника</th><th>Сайт</th><th class=\"availability-col\">Доступность сайта</th><th>Сертификат</th><th>Форма: HTTPS</th><th>Согласие</th><th>SPF / DMARC</th><th>Meta / Instagram</th><th>Политика</th><th>Итог</th><th>Комментарий</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
    </div>

    <div class=\"notes\">
      Для масштабирования на новые сайты: добавьте audit JSON в <code>data/audits</code>, запись в <code>data/sites_manifest.json</code>, затем запустите <code>python scripts/build_dashboard.py</code>. Для сохранения комментариев в репозиторий открывайте дашборд через <code>python scripts/dashboard_server.py</code>.
    </div>
  </div>
  <script>
    document.querySelectorAll('tr.clickable').forEach(function(row){{
      row.addEventListener('click', function(){{ window.location.href = row.dataset.href; }});
      row.addEventListener('keydown', function(e){{
        if(e.target && e.target.closest && (e.target.closest('a') || e.target.closest('input,textarea,select,button'))) return;
        if(e.key === 'Enter' || e.key === ' '){{ e.preventDefault(); window.location.href = row.dataset.href; }}
      }});
    }});
    document.querySelectorAll('a.site-link').forEach(function(link){{
      link.addEventListener('click', function(e){{ e.stopPropagation(); }});
    }});
    const COMMENT_KEY = 'clinic_audit_comments_v1';
    const COMMENTS_API = '/api/comments';

    function loadLocalComments() {{
      try {{
        const raw = localStorage.getItem(COMMENT_KEY);
        return raw ? JSON.parse(raw) : {{}};
      }} catch (e) {{
        return {{}};
      }}
    }}

    function saveLocalComments(comments) {{
      try {{
        localStorage.setItem(COMMENT_KEY, JSON.stringify(comments));
      }} catch (e) {{}}
    }}

    async function loadApiComments() {{
      const resp = await fetch(COMMENTS_API, {{ cache: 'no-store' }});
      if(!resp.ok) throw new Error('comments load failed');
      const data = await resp.json();
      return data && typeof data === 'object' ? data : {{}};
    }}

    async function saveApiComments(comments) {{
      const resp = await fetch(COMMENTS_API, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(comments),
      }});
      if(!resp.ok) throw new Error('comments save failed');
    }}

    (async function initComments(){{
      const localComments = loadLocalComments();
      let comments = Object.assign({{}}, localComments);
      let apiEnabled = false;

      if (window.location.protocol.startsWith('http')) {{
        try {{
          const apiComments = await loadApiComments();
          comments = Object.assign({{}}, apiComments, localComments);
          apiEnabled = true;
        }} catch (e) {{
          apiEnabled = false;
        }}
      }}

      let saveTimer = null;
      function scheduleApiSave() {{
        if(!apiEnabled) return;
        if(saveTimer) clearTimeout(saveTimer);
        saveTimer = setTimeout(function(){{
          saveApiComments(comments).catch(function(){{ apiEnabled = false; }});
        }}, 350);
      }}

      document.querySelectorAll('.comment-input').forEach(function(input){{
        const siteId = input.dataset.siteId || '';
        if(siteId && comments[siteId]) input.value = comments[siteId];
        ['click','mousedown','focus','keydown'].forEach(function(evt){{
          input.addEventListener(evt, function(e){{ e.stopPropagation(); }});
        }});
        input.addEventListener('input', function(){{
          if(!siteId) return;
          comments[siteId] = input.value;
          saveLocalComments(comments);
          scheduleApiSave();
        }});
      }});

      // Sync initial merged state into repository file when API is available.
      saveLocalComments(comments);
      scheduleApiSave();
    }})();
  </script>
</body>
</html>
"""

    (ROOT / "dashboard.html").write_text(dashboard, encoding="utf-8")
    screening_step_2 = build_screening_step2(
        rows_step2,
        counts,
        unavailable,
        len(manifest),
        step2_headers,
        json.dumps(step2_col_counts, ensure_ascii=False),
    )
    (ROOT / "screening-step-2.html").write_text(screening_step_2, encoding="utf-8")
    (ROOT / "audit-blocks.html").write_text(screening_step_2, encoding="utf-8")

    sites_dir = ROOT / "sites"
    sites_dir.mkdir(parents=True, exist_ok=True)
    for site_id, page in details:
        (sites_dir / f"{site_id}.html").write_text(page, encoding="utf-8")

    print("Generated:")
    print(ROOT / "dashboard.html")
    print(ROOT / "screening-step-2.html")
    print(ROOT / "audit-blocks.html")
    for site_id, _ in details:
        print(sites_dir / f"{site_id}.html")


if __name__ == "__main__":
    main()

