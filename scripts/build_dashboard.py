import argparse
import json
import html
import re
from io import BytesIO
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode, urlparse, urljoin
from urllib.request import Request, urlopen
from urllib.error import HTTPError
try:
    import requests
except Exception:
    requests = None
try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

ROOT = Path(r"D:\разработка\Кибербеза 2.0")
MANIFEST = ROOT / "data" / "sites_manifest.json"
AGENT_MODE_LOCK = ROOT / "AGENT_ONLY_MODE.lock"


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
COOKIE_NOTICE_TOKENS = {
    "cookie",
    "cookies",
    "куки",
    "файл cookie",
    "файлы cookie",
    "cookie policy",
    "политика cookie",
}
COOKIE_NOTICE_REGEX_PATTERNS = [
    r"(?:мы|сайт)[^.\n]{0,80}(?:используем|применяем|собираем)[^.\n]{0,80}(?:cookie|куки)",
    r"(?:cookie|куки)[^.\n]{0,120}(?:соглас|принять|accept|ok|настройк|разреш)",
    r"(?:продолжая[^.\n]{0,120}(?:cookie|куки))|(?:(?:cookie|куки)[^.\n]{0,120}продолжая)",
]
METRIKA_POLICY_TOKENS = {
    "яндекс.метрик",
    "яндекс метрик",
    "yandex metrika",
    "yandex.metrika",
    "mc.yandex.ru",
    "ym(",
}
POLICY_URL_HINT_TOKENS = {
    "policy",
    "privacy",
    "polit",
    "processing-data",
    "personal-data",
    "pdn",
    "confident",
    "конфиден",
    "персональ",
    "обработк",
    "данн",
    "152-fz",
    "152-фз",
}
NON_EVIDENCE_DISCOVERY_SOURCES = {"fallback", "policy-fallback", "policy-hint"}
NON_TEXT_ASSET_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".svg", ".ico", ".bmp", ".tiff", ".avif",
    ".mp4", ".webm", ".mov", ".avi", ".mp3", ".wav", ".ogg", ".zip",
}
POLICY_CONTENT_TOKENS = {
    "персональн",
    "обработк",
    "субъект персональ",
    "оператор персональ",
    "152-фз",
    "152 фз",
    "федеральн",
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

HTTP_FETCH_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    ),
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def site_host(value: str) -> str:
    host = urlparse(site_url(value)).netloc.lower().split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host


def read_json(path: Path):
    with path.open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def read_text_best_effort(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp1251"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", "ignore")


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
        "рекомендация": "warn",
        "н/п": "na",
        "-": "na",
        "текстом": "bad",
        "checked": "bad",
        "не найдено": "bad",
        "unchecked": "ok",
        "слать": "ok",
        "не слать": "bad",
        "агент": "ok",
        "скрипт": "warn",
        "скрипт (устаревший)": "warn",
        "не указан": "na",
    }
    if label in mapping:
        return mapping[label]
    low = str(label).lower()
    if "unchecked" in low:
        return "ok"
    if any(x in low for x in ["checked", "не найдено", "текстом"]):
        return "bad"
    return "na"


def verification_mode_info(audit):
    verification = audit.get("verification", {}) or {}
    raw = str(verification.get("mode") or "").strip().lower()
    if raw in {"agent", "agentic", "агент", "agent_mode"}:
        return {"code": "agent", "label": "агент"}
    if raw in {"legacy_script", "script", "скрипт", "legacy"}:
        return {"code": "legacy_script", "label": "скрипт (устаревший)"}
    return {"code": "unknown", "label": "не указан"}


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
        "sitemap-price": "sitemap: кандидат страницы цен",
        "navigation-price": "навигация: кандидат страницы цен",
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
        "sitemap-price",
        "navigation-price",
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


def classify_meta_status(forbidden_hits):
    if not forbidden_hits:
        return "ок"
    has_visible_mentions = any(
        "видно пользователю" in str(x.get("visibility", "") or "").lower()
        for x in forbidden_hits
    )
    has_direct_forbidden_links = any(
        bool(
            re.search(
                r'href\s*=\s*["\']https?://(?:www\.)?(?:instagram\.com|facebook\.com|fb\.com|threads\.net|meta\.com)\b',
                str(x.get("context", "") or ""),
                flags=re.IGNORECASE,
            )
        )
        for x in forbidden_hits
    )
    return "проблема" if (has_visible_mentions or has_direct_forbidden_links) else "проверить"


def _collect_policy_text_chunks(audit):
    chunks = []
    for x in collect_policy_evidence(audit):
        for key in ("text", "href", "page"):
            v = str(x.get(key) or "").strip().lower()
            if v:
                chunks.append(v)
    discovery = audit.get("discovery", {}) or {}
    for u in (discovery.get("legal_urls", []) or []):
        s = str(u or "").strip().lower()
        if s:
            chunks.append(s)
    for f in (audit.get("forms", []) or []):
        s = str(f.get("policy_poc") or "").strip().lower()
        if s:
            chunks.append(s)
    return chunks


def collect_policy_evidence(audit):
    discovery = audit.get("discovery", {}) or {}
    source_map = discovery.get("sources", {}) or {}
    pages = audit.get("pages", []) or []
    status_by_requested = {str(p.get("requested") or ""): p.get("status") for p in pages}

    evidence = []
    seen = set()

    # 1) Anchor/link evidence from real crawled pages (exclude synthetic fallback sources).
    for x in (audit.get("privacy_links", []) or []):
        page = str(x.get("page") or "").strip()
        href = str(x.get("href") or "").strip()
        text = str(x.get("text") or "").strip()
        if not (page or href or text):
            continue
        if text.lower().startswith("cookie notice:"):
            continue
        if text.lower() == "policy path":
            continue
        page_src = str(source_map.get(page, "") or "").strip().lower()
        if page_src in NON_EVIDENCE_DISCOVERY_SOURCES:
            continue
        key = ("anchor", page, href, text)
        if key in seen:
            continue
        seen.add(key)
        evidence.append({"kind": "anchor", "page": page, "href": href, "text": text, "source": page_src or "unknown"})

    # 2) Legal pages discovered from sitemap/navigation only (exclude policy fallback/hint).
    # Skip obvious binary/media assets that cannot serve as readable policy documents.
    for u in (discovery.get("legal_urls", []) or []):
        url = str(u or "").strip()
        if not url:
            continue
        if not _contains_policy_hint(url):
            continue
        path = urlparse(url).path.lower()
        suffix = Path(path).suffix.lower()
        if suffix in NON_TEXT_ASSET_EXTENSIONS:
            continue
        src = str(source_map.get(url, "") or "").strip().lower()
        if src in NON_EVIDENCE_DISCOVERY_SOURCES:
            continue
        if src not in {"sitemap-legal", "navigation-legal"}:
            continue
        status = status_by_requested.get(url)
        if status != 200:
            continue
        key = ("legal-page", url, src)
        if key in seen:
            continue
        seen.add(key)
        evidence.append({"kind": "legal-page", "page": url, "href": url, "text": "legal page", "source": src})

    # 3) Inline policy notices captured on form pages.
    for f in (audit.get("forms", []) or []):
        if not f.get("has_policy_text"):
            continue
        page = str(f.get("page") or "").strip()
        snippet = str(f.get("policy_poc") or "").strip()
        if not page and not snippet:
            continue
        page_src = str(source_map.get(page, "") or "").strip().lower()
        if page_src in NON_EVIDENCE_DISCOVERY_SOURCES:
            continue
        key = ("form-policy-text", page, snippet)
        if key in seen:
            continue
        seen.add(key)
        evidence.append({
            "kind": "form-policy-text",
            "page": page,
            "href": str(f.get("action_display") or "").strip(),
            "text": snippet[:240] if snippet else "policy text near form",
            "source": page_src or "unknown",
        })

    return evidence


def _contains_any_token(chunks, tokens):
    for c in chunks:
        if any(t in c for t in tokens):
            return True
    return False


def _has_cookie_notice_pattern(text_low: str) -> bool:
    s = str(text_low or "").strip().lower()
    if not s:
        return False
    for pat in COOKIE_NOTICE_REGEX_PATTERNS:
        if re.search(pat, s, flags=re.IGNORECASE):
            return True
    return False


def _contains_policy_hint(text: str) -> bool:
    low = str(text or "").strip().lower()
    return any(tok in low for tok in POLICY_URL_HINT_TOKENS)


def _normalize_candidate_url(domain: str, raw_url: str) -> str:
    u = str(raw_url or "").strip()
    if not u:
        return ""
    low = u.lower()
    # Skip non-URL payloads captured from form action_display (e.g. action="/path/").
    if low.startswith("action="):
        return ""
    if any(x in low for x in ['"', "'", "<", ">", " "]):
        return ""
    if u.startswith("http://") or u.startswith("https://"):
        return u
    base = f"https://{str(domain or '').strip().lstrip('/')}"
    base = base if base.endswith("/") else base + "/"
    if u.startswith("/"):
        return urljoin(base, u)
    # Accept plain relative paths like "corp/polozhenie".
    if re.match(r"^[a-zA-Z0-9._~!$&'()*+,;=:@%/-]+$", u):
        return urljoin(base, u)
    return ""


def collect_semantic_policy_urls(audit, policy_evidence=None):
    discovery = audit.get("discovery", {}) or {}
    domain = str(audit.get("domain") or "").strip()
    urls = []
    seen = set()

    def add_url(raw_url: str, context_text: str):
        abs_url = _normalize_candidate_url(domain, raw_url)
        if not abs_url:
            return
        if not _contains_policy_hint(f"{abs_url} {context_text}"):
            return
        key = abs_url.strip()
        if key in seen:
            return
        seen.add(key)
        urls.append(key)

    for x in (audit.get("privacy_links", []) or []):
        add_url(str(x.get("href") or ""), str(x.get("text") or ""))
        add_url(str(x.get("page") or ""), "")

    source_map = discovery.get("sources", {}) or {}
    for u in (discovery.get("legal_urls", []) or []):
        src = str(source_map.get(str(u or ""), "") or "").strip().lower()
        if src in NON_EVIDENCE_DISCOVERY_SOURCES:
            continue
        add_url(str(u or ""), "")

    for x in (policy_evidence or []):
        add_url(str(x.get("href") or ""), str(x.get("text") or ""))
        add_url(str(x.get("page") or ""), "")

    return urls


@lru_cache(maxsize=1024)
def _probe_url_status(url: str):
    if requests is not None:
        try:
            resp = requests.get(url, headers=HTTP_FETCH_HEADERS, timeout=6, allow_redirects=True)
            return int(resp.status_code or 0)
        except Exception:
            pass
    req = Request(url, headers=HTTP_FETCH_HEADERS)
    try:
        with urlopen(req, timeout=6) as resp:
            return int(resp.getcode() or 0)
    except HTTPError as e:
        try:
            return int(e.code or 0)
        except Exception:
            return 0
    except Exception:
        return 0


def _fetch_url_content(url: str, timeout_sec: int = 8):
    if requests is not None:
        try:
            resp = requests.get(url, headers=HTTP_FETCH_HEADERS, timeout=timeout_sec, allow_redirects=True)
            return int(resp.status_code or 0), str(resp.headers.get("Content-Type", "")), bytes(resp.content or b"")
        except Exception:
            pass

    req = Request(url, headers=HTTP_FETCH_HEADERS)
    try:
        with urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read()
            ctype = str(resp.headers.get("Content-Type", "") if resp.headers else "")
            return int(resp.getcode() or 0), ctype, raw
    except HTTPError as e:
        try:
            raw = e.read() if hasattr(e, "read") else b""
        except Exception:
            raw = b""
        return int(getattr(e, "code", 0) or 0), str(getattr(e, "headers", {}).get("Content-Type", "") if getattr(e, "headers", None) else ""), raw
    except Exception:
        return 0, "", b""


def _extract_policy_doc_links(base_url: str, html_low: str):
    out = []
    seen = set()
    for m in re.finditer(r'(?is)<a\b[^>]*href\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))', str(html_low or "")):
        href = str(m.group(1) or m.group(2) or m.group(3) or "").strip()
        if not href:
            continue
        low = href.lower()
        if any(low.endswith(ext) for ext in [".pdf", ".doc", ".docx"]):
            abs_url = urljoin(base_url, href)
            if abs_url not in seen:
                seen.add(abs_url)
                out.append(abs_url)
    return out


def evaluate_policy_document_presence(audit, policy_evidence=None, max_urls: int = 8):
    scan = scan_policy_pages_for_tokens(
        audit,
        tokens=POLICY_CONTENT_TOKENS,
        policy_evidence=policy_evidence,
        max_urls=max_urls,
    )
    checked_urls = scan.get("checked_urls") or []
    modal_texts = scan.get("modal_texts") or {}
    matched_tokens_union = set()
    valid_urls = []
    broken_doc_urls = []
    per_url_signals = []

    for u in checked_urls:
        if str(u).startswith("modal:"):
            text_low = str(modal_texts.get(u) or "")
            matched_tokens = sorted([tok for tok in POLICY_CONTENT_TOKENS if tok in text_low])
            matched_tokens_union.update(matched_tokens)
            has_docs = False
            docs_ok = False
            docs_broken = False
            policy_hint = any(tok in text_low for tok in ["политик", "конфиден", "персональ"])
            strong_text = bool(_is_probably_readable_text(text_low) and policy_hint and len(matched_tokens) >= 1)
            url_valid = bool(strong_text)
        else:
            text_low, _, readable_text = _fetch_policy_visible_text_lower(u)
            matched_tokens = sorted([tok for tok in POLICY_CONTENT_TOKENS if tok in str(text_low or "")])
            matched_tokens_union.update(matched_tokens)
            html_low = _fetch_url_html_lower(u)
            doc_links = _extract_policy_doc_links(u, html_low)
            doc_statuses = [{"url": du, "status": _probe_url_status(du)} for du in doc_links[:6]]
            has_docs = bool(doc_statuses)
            docs_ok = any((d.get("status") or 0) == 200 for d in doc_statuses)
            docs_broken = has_docs and not docs_ok

            strong_text = bool(readable_text and len(matched_tokens) >= 3)
            url_valid = bool(docs_ok or (strong_text and not docs_broken))
        if url_valid:
            valid_urls.append(u)
        if docs_broken:
            broken_doc_urls.append(u)
        per_url_signals.append({
            "url": u,
            "matched_tokens": matched_tokens,
            "has_doc_links": has_docs,
            "doc_links_ok": docs_ok,
            "doc_links_broken": docs_broken,
            "strong_text": strong_text,
            "valid": url_valid,
        })

    present = bool(valid_urls)
    return {
        "present": present,
        "checked_urls": checked_urls,
        "readable_urls": scan.get("readable_urls") or [],
        "unreadable_urls": scan.get("unreadable_urls") or [],
        "matched_tokens": sorted(matched_tokens_union),
        "candidate_urls": scan.get("candidate_urls") or [],
        "valid_urls": valid_urls,
        "broken_doc_urls": broken_doc_urls,
        "per_url_signals": per_url_signals,
    }


@lru_cache(maxsize=1024)
def _fetch_url_html_lower(url: str) -> str:
    status, ctype, raw = _fetch_url_content(url, timeout_sec=6)
    if status < 200 or status >= 300 or not raw:
        return ""
    if "charset=" in ctype.lower():
        enc = ctype.lower().split("charset=", 1)[1].split(";", 1)[0].strip()
    else:
        enc = "utf-8"
    try:
        text = raw.decode(enc, "ignore")
    except Exception:
        text = raw.decode("utf-8", "ignore")
    return text.lower()


def _html_to_visible_text_lower(html_low: str) -> str:
    """Extract readable page text and drop script/style payload to avoid false positives."""
    src = str(html_low or "")
    if not src:
        return ""
    src = re.sub(r"<script\b[^>]*>.*?</script>", " ", src, flags=re.IGNORECASE | re.DOTALL)
    src = re.sub(r"<style\b[^>]*>.*?</style>", " ", src, flags=re.IGNORECASE | re.DOTALL)
    src = re.sub(r"<noscript\b[^>]*>.*?</noscript>", " ", src, flags=re.IGNORECASE | re.DOTALL)
    src = re.sub(r"<[^>]+>", " ", src)
    src = re.sub(r"\s+", " ", src)
    return src.strip()


def _is_probably_readable_text(text_low: str) -> bool:
    txt = str(text_low or "").strip()
    if not txt:
        return False
    words = re.findall(r"[a-zа-яё]{3,}", txt, flags=re.IGNORECASE)
    # HTML policy pages can be short but still readable; keep PDF/image scans as unreadable.
    return len(words) >= 12 or len(txt) >= 600


@lru_cache(maxsize=1024)
def _fetch_policy_visible_text_lower(url: str):
    status, ctype, raw = _fetch_url_content(url, timeout_sec=8)
    ctype = str(ctype or "").lower()
    if status < 200 or status >= 300 or not raw:
        return "", False, False

    is_pdf = (".pdf" in str(url or "").lower()) or ("application/pdf" in ctype)
    if is_pdf:
        if PdfReader is None:
            return "", True, False
        try:
            reader = PdfReader(BytesIO(raw))
            text = " ".join((p.extract_text() or "") for p in reader.pages).lower()
            text = re.sub(r"\s+", " ", text).strip()
            return text, True, _is_probably_readable_text(text)
        except Exception:
            return "", True, False

    if "charset=" in ctype:
        enc = ctype.split("charset=", 1)[1].split(";", 1)[0].strip()
    else:
        enc = "utf-8"
    try:
        html_text = raw.decode(enc, "ignore")
    except Exception:
        html_text = raw.decode("utf-8", "ignore")
    visible_text = _html_to_visible_text_lower(html_text.lower())
    return visible_text, False, _is_probably_readable_text(visible_text)


def scan_policy_pages_for_metrika(audit, policy_evidence=None, max_urls: int = 3):
    return scan_policy_pages_for_tokens(
        audit,
        tokens=METRIKA_POLICY_TOKENS,
        policy_evidence=policy_evidence,
        max_urls=max_urls,
    )


def _extract_modal_targets_from_forms(audit):
    domain = str(audit.get("domain") or "").strip()
    out = []
    seen = set()
    for f in (audit.get("forms", []) or []):
        page = str(f.get("page") or "").strip()
        if not page:
            continue
        page_abs = _normalize_candidate_url(domain, page) or page
        poc = str(f.get("policy_poc") or "")
        if not poc:
            continue
        for m in re.finditer(r'(?is)data-bs-target\s*=\s*["\']#([^"\']+)["\']', poc):
            modal_id = str(m.group(1) or "").strip().lower()
            if not modal_id:
                continue
            key = (page_abs, modal_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(key)
    return out


def _extract_modal_visible_text_lower(html_low: str, modal_id: str) -> str:
    src = str(html_low or "")
    mid = str(modal_id or "").strip()
    if not src or not mid:
        return ""
    pat = re.compile(
        rf'(?is)<[^>]+\bid\s*=\s*["\']{re.escape(mid)}["\'][^>]*>',
        flags=re.IGNORECASE,
    )
    m = pat.search(src)
    if not m:
        return ""
    # Approximate modal block window from opening tag.
    chunk = src[m.start() : m.start() + 20000]
    return _html_to_visible_text_lower(chunk)


def _scan_policy_modals_for_tokens(audit, tokens):
    checked = []
    readable = []
    unreadable = []
    matched = set()
    modal_text_by_ref = {}
    for page_url, modal_id in _extract_modal_targets_from_forms(audit):
        html_low = _fetch_url_html_lower(page_url)
        modal_text = _extract_modal_visible_text_lower(html_low, modal_id)
        ref = f"modal:{page_url}#{modal_id}"
        checked.append(ref)
        modal_text_by_ref[ref] = modal_text
        if _is_probably_readable_text(modal_text):
            readable.append(ref)
        else:
            unreadable.append(ref)
        for tok in tokens:
            if tok in modal_text:
                matched.add(tok)
    return {
        "checked_refs": checked,
        "readable_refs": readable,
        "unreadable_refs": unreadable,
        "matched_tokens": sorted(matched),
        "texts": modal_text_by_ref,
    }


def scan_policy_pages_for_tokens(audit, tokens, policy_evidence=None, max_urls: int = 3):
    urls = collect_semantic_policy_urls(audit, policy_evidence=policy_evidence)
    checked = []
    readable = []
    unreadable = []
    matched = set()
    for u in urls[:max_urls]:
        text_low, is_pdf, readable_text = _fetch_policy_visible_text_lower(u)
        if not text_low and not is_pdf:
            continue
        checked.append(u)
        if readable_text:
            readable.append(u)
        else:
            unreadable.append(u)
        if not text_low:
            continue
        for tok in tokens:
            if tok in text_low:
                matched.add(tok)

    modal_scan = _scan_policy_modals_for_tokens(audit, tokens=tokens)
    for ref in (modal_scan.get("checked_refs") or []):
        if ref not in checked:
            checked.append(ref)
    for ref in (modal_scan.get("readable_refs") or []):
        if ref not in readable:
            readable.append(ref)
    for ref in (modal_scan.get("unreadable_refs") or []):
        if ref not in unreadable:
            unreadable.append(ref)
    for tok in (modal_scan.get("matched_tokens") or []):
        matched.add(tok)

    return {
        "candidate_urls": urls,
        "checked_urls": checked,
        "readable_urls": readable,
        "unreadable_urls": unreadable,
        "matched_tokens": sorted(matched),
        "modal_texts": modal_scan.get("texts") or {},
    }


def detect_cookie_notice(audit):
    for x in (audit.get("privacy_links", []) or []):
        txt = str(x.get("text") or "").strip().lower()
        if txt.startswith("cookie notice:"):
            return True

    # 1) Direct page HTML signals (cookie banner text can be rendered outside policy docs).
    for p in (audit.get("pages", []) or []):
        if p.get("status") != 200:
            continue
        html_text = str(p.get("html") or "").lower()
        if _has_cookie_notice_pattern(html_text):
            return True

    # 2) Directly fetch homepage HTML as fallback.
    domain = str(audit.get("domain") or "").strip()
    if domain:
        home_low = _fetch_url_html_lower(site_url(domain))
        if _has_cookie_notice_pattern(home_low):
            return True
    return False


def build_cookie_notice_poc(audit, summary):
    if bool(summary.get("site_unavailable")):
        return ["Не проверено: сайт недоступен."]

    cookie_priv_hits = []
    for x in (audit.get("privacy_links", []) or []):
        txt = str(x.get("text") or "").strip()
        if txt.lower().startswith("cookie notice:"):
            cookie_priv_hits.append(
                (
                    str(x.get("page") or "").strip(),
                    txt[:220],
                )
            )

    matches = []
    for p in (audit.get("pages", []) or []):
        if p.get("status") != 200:
            continue
        html_text = str(p.get("html") or "").lower()
        if not html_text:
            continue
        if _has_cookie_notice_pattern(html_text):
            matches.append(
                (
                    str(p.get("requested") or p.get("final_url") or "").strip(),
                    ["cookie-notice-regex"],
                )
            )

    lines = [
        f"cookie_notice_found: {summary.get('cookie_notice_found')}",
        f"cookie_notice_hits_in_privacy_links: {len(cookie_priv_hits)}",
        f"cookie_notice_pages_matched: {len(matches)}",
        "cookie_detection_mode: explicit cookie-notice pattern (без учета policy-URL токенов)",
    ]
    for page, txt in cookie_priv_hits[:8]:
        lines.append(f"{page} | {txt}")
    for url, toks in matches[:8]:
        lines.append(f"{url} | tokens: {', '.join(toks)}")
    if not matches and not cookie_priv_hits:
        lines.append("Совпадений по cookie-токенам не найдено.")
    lines.append("Источник: явные cookie-notice сигналы в тексте страниц/домашней страницы.")
    return lines


def detect_metrika_policy_disclosure(audit):
    analytics = ((audit.get("tech") or {}).get("analytics") or {})
    kinds = [str(x).lower() for x in (analytics.get("kinds") or [])]
    has_yandex_metrika = "yandex_metrika" in kinds
    if not has_yandex_metrika:
        return True

    policy_evidence = collect_policy_evidence(audit)
    policy_scan = scan_policy_pages_for_metrika(audit, policy_evidence=policy_evidence)
    if policy_scan.get("checked_urls") and not policy_scan.get("readable_urls"):
        return None
    # Strict rule: for this metric, count only explicit mentions inside fetched policy page text.
    # Do not trust snippets/anchors because they can include injected script fragments.
    return bool(policy_scan.get("matched_tokens"))


def build_metrika_policy_poc(audit, summary):
    analytics = ((audit.get("tech") or {}).get("analytics") or {})
    kinds = [str(x).lower() for x in (analytics.get("kinds") or [])]
    has_yandex_metrika = "yandex_metrika" in kinds

    policy_evidence = (summary.get("policy_evidence") or [])
    policy_scan = scan_policy_pages_for_metrika(audit, policy_evidence=policy_evidence)
    policy_urls = policy_scan.get("checked_urls") or policy_scan.get("candidate_urls") or []

    matched_tokens = sorted(set(policy_scan.get("matched_tokens") or []))

    lines = [
        f"metrika_policy_disclosed: {summary.get('metrika_policy_disclosed')}",
        "analytics.kinds: " + (", ".join(analytics.get("kinds", []) or []) if analytics.get("kinds") else "не найдены"),
        f"has_yandex_metrika: {has_yandex_metrika}",
        f"policy_evidence_count: {len(policy_evidence)}",
        "checked_policy_urls: " + (", ".join(policy_urls[:8]) if policy_urls else "не найдены"),
        "readable_policy_urls: " + (", ".join((policy_scan.get("readable_urls") or [])[:8]) if policy_scan.get("readable_urls") else "не найдены"),
        "unreadable_policy_urls: " + (", ".join((policy_scan.get("unreadable_urls") or [])[:8]) if policy_scan.get("unreadable_urls") else "не найдены"),
        "matched_tokens_in_policy_text: " + (", ".join(matched_tokens) if matched_tokens else "не найдены"),
    ]

    if has_yandex_metrika and summary.get("metrika_policy_disclosed") is None:
        lines.append("Вывод: policy-URL найден, но текст политики не извлечен надежно (например, скан/PDF без текстового слоя). Статус: проверить.")
    elif has_yandex_metrika and not matched_tokens:
        lines.append("Вывод: Метрика найдена на сайте, но явных упоминаний в тексте политики не найдено.")
    elif has_yandex_metrika and matched_tokens:
        lines.append("Вывод: Метрика найдена на сайте и явно упомянута в тексте политики.")
    else:
        lines.append("Вывод: Яндекс.Метрика на сайте не обнаружена, метрика автоматически ок.")
    return lines


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


def block2_statuses(audit, site_unavailable):
    if site_unavailable:
        return {
            "online_slots_status": "-",
            "digital_tool_status": "-",
            "analytics_status": "-",
            "after_hours_status": "-",
            "price_public_status": "-",
            "schema_supported_status": "-",
        }

    discovery = audit.get("discovery", {}) or {}
    tech = audit.get("tech", {}) or {}
    med = tech.get("med_trust", {}) or {}
    analytics = tech.get("analytics", {}) or {}
    engagement = tech.get("engagement", {}) or {}

    has_slot_booking = bool(engagement.get("slot_booking_widget"))
    source_map = discovery.get("sources", {}) or {}
    pages_by_req = {str(p.get("requested") or ""): p for p in (audit.get("pages", []) or [])}
    booking_urls = [str(u) for u in (discovery.get("booking_urls") or []) if str(u)]
    booking_sources_ok = {
        "sitemap",
        "navigation",
        "booking-candidate",
        "sitemap-booking-candidate",
        "home-booking-candidate",
    }
    booking_page_found = any(
        str(source_map.get(u, "")).strip().lower() in booking_sources_ok
        and int((pages_by_req.get(u) or {}).get("status") or 0) == 200
        for u in booking_urls
    )
    online_slots_status = "ок" if (has_slot_booking or booking_page_found) else "проблема"

    sitemap_total = int(discovery.get("sitemap_total_urls") or 0)
    pages_count = len(audit.get("pages", []) or [])
    indexed_pages = max(sitemap_total, pages_count)
    digital_tool_status = "проблема" if indexed_pages <= 4 else "ок"

    has_analytics = bool(analytics.get("found"))
    analytics_status = "ок" if has_analytics else "проблема"

    has_message_form = any(bool(f.get("has_textarea")) for f in (audit.get("forms", []) or []))
    has_async_channel = bool(
        engagement.get("whatsapp")
        or engagement.get("telegram")
        or engagement.get("max_messenger")
        or engagement.get("chat_widget")
        or has_message_form
    )
    after_hours_status = "ок" if has_async_channel else "проблема"

    price_public_found = med.get("price_public_found")
    price_sources_ok = {
        "sitemap",
        "navigation",
        "sitemap-price",
        "navigation-price",
        "sitemap-form",
        "booking-candidate",
        "sitemap-booking-candidate",
        "home-booking-candidate",
    }

    def is_price_url(u: str) -> bool:
        lu = str(u or "").lower()
        return any(
            t in lu
            for t in [
                "price",
                "prices",
                "prays",
                "ceny",
                "tseny",
                "stoim",
                "cost",
                "tarif",
                "price-list",
                "pricing",
                "cena",
                "stoimost",
                "uslugi",
            ]
        )

    candidate_urls = []
    for u, src in source_map.items():
        if str(src).strip().lower() not in price_sources_ok:
            continue
        if not is_price_url(u):
            continue
        status = int((pages_by_req.get(u) or {}).get("status") or 0)
        if status == 200:
            candidate_urls.append(u)

    if price_public_found is None:
        price_public_found = bool(candidate_urls)

    if price_public_found is True:
        price_public_status = "ок"
    elif candidate_urls:
        # Relevant price-like pages were checked but no public price proof found.
        price_public_status = "проблема"
    else:
        # No reliable evidence either way -> manual recheck, avoid false negative.
        price_public_status = "проверить"
    schema = med.get("schema", {}) or {}
    schema_types = [str(x).strip().lower() for x in (schema.get("types") or []) if str(x).strip()]
    supported = {"organization", "medicalorganization", "medicalclinic", "dentist", "physician", "hospital", "localbusiness"}
    schema_supported_hits = sorted([t for t in schema_types if t in supported])
    schema_supported_status = "ок" if schema_supported_hits else "проблема"

    return {
        "online_slots_status": online_slots_status,
        "digital_tool_status": digital_tool_status,
        "analytics_status": analytics_status,
        "after_hours_status": after_hours_status,
        "price_public_status": price_public_status,
        "price_candidate_urls": candidate_urls[:12],
        "schema_supported_status": schema_supported_status,
        "schema_supported_hits": schema_supported_hits,
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
            "doctors_page_status": "-",
            "address_map_status": "-",
            "hours_status": "-",
            "reviews_status": "-",
            "footer_year_status": "-",
            "contacts_page_status": "-",
            "text_typos_status": "-",
        }

    tech = audit.get("tech", {}) or {}
    med = tech.get("med_trust", {}) or {}
    discovery = audit.get("discovery", {}) or {}
    pages = audit.get("pages", []) or []

    contact_urls = discovery.get("contact_urls", []) or []
    page_urls = [str(p.get("requested") or "") for p in pages]
    low_urls = [u.lower() for u in page_urls]

    def has_url_hint(hints):
        return any(any(h in u for h in hints) for u in low_urls)

    doctors_found = bool(med.get("doctors_page_exists")) or bool(med.get("doctors_content_found"))
    if not doctors_found:
        doctors_found = has_url_hint(["/doctor", "/doctors", "/vrach", "/vrachi", "/specialist", "/team"])
    doctors_page_status = "ок" if doctors_found else "проблема"

    address_found = med.get("address_found")
    map_found = med.get("map_found")
    address_map_status = "ок" if (address_found is True or map_found is True) else "проблема"

    hours_found = med.get("hours_found")
    hours_status = "ок" if (hours_found is True) else "проблема"

    reviews_found = med.get("reviews_found")
    reviews_status = "ок" if (reviews_found is True) else "проблема"

    contacts_exists = (
        bool(med.get("contact_page_exists"))
        or bool(med.get("contact_block_found"))
        or bool(contact_urls)
        or has_url_hint(["/contact", "/contacts", "/kontakty"])
    )
    contacts_page_status = "ок" if contacts_exists else "проблема"

    footer_year = med.get("footer_year", {}) or {}
    footer_present = footer_year.get("present")
    footer_current = footer_year.get("current_year")
    if footer_present is False:
        footer_year_status = "ок"
    elif footer_present is None:
        footer_year_status = "проверить"
    elif footer_current is True:
        footer_year_status = "ок"
    elif footer_current is None:
        footer_year_status = "проверить"
    else:
        footer_year_status = "проблема"
    text_typos = med.get("text_typos", {}) or {}
    typo_errors = int(text_typos.get("error_count") or 0)
    typo_checked_pages = int(len(text_typos.get("checked_pages") or []))
    if typo_checked_pages == 0:
        text_typos_status = "проблема"
    else:
        text_typos_status = "проблема" if typo_errors > 5 else "ок"

    return {
        "doctors_page_status": doctors_page_status,
        "address_map_status": address_map_status,
        "hours_status": hours_status,
        "reviews_status": reviews_status,
        "footer_year_status": footer_year_status,
        "contacts_page_status": contacts_page_status,
        "text_typos_status": text_typos_status,
    }


def block_verified(audit, block_id: str) -> bool:
    verification = audit.get("verification", {}) or {}
    mode = str(verification.get("mode") or "").strip().lower()
    if mode not in {"agent", "agentic", "агент", "agent_mode"}:
        return False
    # Prefer current block ids (b1/b2/b3). Read legacy ids for compatibility.
    legacy_map = {
        "b1": None,
        "b2": "b2",
        "b3": "b4",
    }
    value = verification.get(block_id)
    if value is not True:
        legacy_id = legacy_map.get(block_id)
        if legacy_id:
            value = verification.get(legacy_id)
    return value is True


def _extract_form_action_value(form: dict) -> str:
    action_display = str(form.get("action_display") or "").strip()
    m = re.search(r'(?is)action\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))', action_display)
    if m:
        return str(m.group(1) or m.group(2) or m.group(3) or "").strip()
    open_tag = str(form.get("open_tag") or "").strip()
    m2 = re.search(r'(?is)\baction\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))', open_tag)
    if m2:
        return str(m2.group(1) or m2.group(2) or m2.group(3) or "").strip()
    return ""


def _is_insecure_form_transport(form: dict) -> bool:
    page = str(form.get("page") or "").strip().lower()
    action = _extract_form_action_value(form).lower()
    if action.startswith("http://"):
        return True
    if action.startswith("https://"):
        return False
    # Relative or missing action on HTTP page means submission over HTTP transport.
    if page.startswith("http://"):
        return True
    return False


def compute_summary(item, audit):
    pages = audit.get("pages", [])
    discovery = audit.get("discovery", {})
    source_map = discovery.get("sources", {})

    forms = audit.get("forms", [])
    forbidden = filter_meta_hits(audit.get("forbidden_hits", []))
    policy_evidence = collect_policy_evidence(audit)
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

    bad_https_forms = [f for f in forms if _is_insecure_form_transport(f)]
    form_https_status = "ок" if not bad_https_forms else "проблема"

    consent_buckets = {"текстом": [], "checked": [], "не найдено": [], "unchecked": []}
    for f in forms:
        consent_buckets[classify_form_consent(f)].append(f)
    consent_counts = {k: len(v) for k, v in consent_buckets.items()}

    if not forms:
        consent_status = "не найдено"
    else:
        # Metric 1 only checks missing consent markers.
        # Prechecked consent is tracked separately in metric 2.
        consent_status = "не найдено" if consent_buckets["не найдено"] else "unchecked"

    spf_dmarc_status, spf_dmarc_lines, email, spf_status, dmarc_status = evaluate_spf_dmarc(item, audit)
    dkim_status, dkim_lines, _ = evaluate_dkim(item, audit)
    spf_dmarc_poc = " | ".join(spf_dmarc_lines)

    meta_status = classify_meta_status(forbidden)
    policy_validation = evaluate_policy_document_presence(audit, policy_evidence=policy_evidence)
    policy_status = "ок" if policy_validation.get("present") else "проблема"
    cookie_notice_found = detect_cookie_notice(audit)
    analytics = ((audit.get("tech") or {}).get("analytics") or {})
    analytics_kinds = [str(x).lower() for x in (analytics.get("kinds") or [])]
    has_yandex_metrika = "yandex_metrika" in analytics_kinds
    metrika_policy_disclosed = detect_metrika_policy_disclosure(audit)
    verification_mode = verification_mode_info(audit)

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
            "policy_evidence": [],
            "policy_validation": {"present": None, "checked_urls": [], "readable_urls": [], "unreadable_urls": [], "matched_tokens": [], "candidate_urls": []},
            "cookie_notice_found": None,
            "metrika_policy_disclosed": None,
            "has_yandex_metrika": False,
            "result": "-",
            "verification_mode": verification_mode,
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
            "block1_verified": block_verified(audit, "b1"),
            "block2_verified": block_verified(audit, "b2"),
            "block3_verified": block_verified(audit, "b3"),
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
        "policy_evidence": policy_evidence,
        "policy_validation": policy_validation,
        "cookie_notice_found": cookie_notice_found,
        "metrika_policy_disclosed": metrika_policy_disclosed,
        "has_yandex_metrika": has_yandex_metrika,
        "result": item.get("result", "проверить"),
        "verification_mode": verification_mode,
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
        "block1_verified": block_verified(audit, "b1"),
        "block2_verified": block_verified(audit, "b2"),
        "block3_verified": block_verified(audit, "b3"),
    }


def row_html(row_num, site_id, clinic, site, s):
    external = site_url(site)
    mode_label = (s.get("verification_mode") or {}).get("label", "не указан")
    return f"""
    <tr id=\"row-{esc(site_id)}\" class=\"clickable\" data-href=\"sites/{esc(site_id)}.html\" tabindex=\"0\">
      <td class=\"row-id\">{esc(row_num)}</td>
      <td><div class=\"clinic\">{esc(clinic)}</div><div style=\"margin-top:4px\"><span class=\"badge {badge_class(mode_label)}\">{esc(mode_label)}</span></div></td>
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


def details_section(title, status, lines, extra_class: str = ""):
    content = "".join(f"<li>{esc(line)}</li>" for line in lines) if lines else "<li>Нет данных</li>"
    cls = f"block {extra_class}".strip()
    return f"""
    <section class=\"{esc(cls)}\">
      <h2>{esc(title)} <span class=\"badge {badge_class(status)}\">{esc(status)}</span></h2>
      <ul>{content}</ul>
    </section>
    """


def details_section_grouped(title, status, groups, extra_class: str = ""):
    if not groups:
        groups_html = '<div class="metric-item"><div class="metric-title">Нет данных</div></div>'
    else:
        parts = []
        for g in groups:
            name = esc(g.get("name", "Метрика"))
            mstatus = str(g.get("status", "-"))
            ev = g.get("evidence", []) or []
            if ev:
                ev_parts = []
                for x in ev:
                    raw = str(x)
                    if raw.startswith("__H__:"):
                        ev_parts.append(f'<div class="evidence-subhead">{esc(raw.replace("__H__:", "", 1).strip())}</div>')
                    else:
                        ev_parts.append(f"<li>{esc(raw)}</li>")
                ev_html = "".join(ev_parts)
            else:
                ev_html = "<li>Нет evidences</li>"
            parts.append(
                f'<div class="metric-item">'
                f'<div class="metric-head"><span class="metric-title">{name}</span>'
                f'<span class="badge {badge_class(mstatus)}">{esc(mstatus)}</span></div>'
                f'<ul>{ev_html}</ul>'
                f'</div>'
            )
        groups_html = "".join(parts)
    cls = f"block {extra_class}".strip()
    return f"""
    <section class=\"{esc(cls)}\">
      <h2>{esc(title)} <span class=\"badge {badge_class(status)}\">{esc(status)}</span></h2>
      <div class=\"metric-grid\">{groups_html}</div>
    </section>
    """


def metric_lines(metric_name, status, evidence_lines):
    return {
        "name": metric_name,
        "status": status,
        "evidence": evidence_lines or [],
    }


def block2_poc_lines(audit, summary):
    b2 = summary.get("b2", {}) or {}
    tech = audit.get("tech", {}) or {}
    med = tech.get("med_trust", {}) or {}
    discovery = audit.get("discovery", {}) or {}
    analytics = tech.get("analytics", {}) or {}
    engagement = tech.get("engagement", {}) or {}

    lines = []

    lines.append(metric_lines(
        "Нет онлайн-записи со слотами",
        b2.get("online_slots_status", "-"),
        [
            f"slot_booking_widget: {bool(engagement.get('slot_booking_widget'))}",
            "booking_urls: " + ", ".join((discovery.get("booking_urls", []) or [])[:5]) if discovery.get("booking_urls") else "booking_urls: не найдены",
            "booking_sources: " + ", ".join(
                sorted({
                    str((discovery.get("sources", {}) or {}).get(u, "") or "")
                    for u in (discovery.get("booking_urls", []) or [])
                })
            ) if discovery.get("booking_urls") else "booking_sources: не найдены",
        ],
    ))

    sitemap_total = int(discovery.get("sitemap_total_urls") or 0)
    pages_count = len(audit.get("pages", []) or [])
    lines.append(metric_lines(
        "Сайт — цифровая визитка, не инструмент",
        b2.get("digital_tool_status", "-"),
        [
            f"sitemap_total_urls: {sitemap_total}",
            f"pages_count: {pages_count}",
        ],
    ))

    lines.append(metric_lines(
        "Вы не знаете кто приходит на сайт и почему уходит",
        b2.get("analytics_status", "-"),
        [
            f"analytics.found: {analytics.get('found')}",
            "analytics.kinds: " + ", ".join(analytics.get("kinds", []) or []) if analytics.get("kinds") else "analytics.kinds: не найдены",
            f"analytics.goals_found: {analytics.get('goals_found')}",
            "goal_markers: " + ", ".join(analytics.get("goal_markers", []) or []) if analytics.get("goal_markers") else "goal_markers: не найдены",
        ],
    ))

    lines.append(metric_lines(
        "Пациент не может написать первым",
        b2.get("after_hours_status", "-"),
        [
            f"whatsapp: {bool(engagement.get('whatsapp'))}",
            f"telegram: {bool(engagement.get('telegram'))}",
            f"max_messenger: {bool(engagement.get('max_messenger'))}",
            f"chat_widget: {bool(engagement.get('chat_widget'))}",
            f"message_form_found: {any(bool(f.get('has_textarea')) for f in (audit.get('forms', []) or []))}",
        ],
    ))

    lines.append(metric_lines(
        "Прайс-лист доступен без регистрации",
        b2.get("price_public_status", "-"),
        [
            f"price_public_found: {med.get('price_public_found')}",
            "price_pages: " + ", ".join((med.get("price_pages") or [])[:5]) if med.get("price_pages") else "price_pages: не найдены",
            "price_candidate_urls_checked: " + ", ".join((b2.get("price_candidate_urls") or [])[:8]) if b2.get("price_candidate_urls") else "price_candidate_urls_checked: не найдены",
        ],
    ))

    schema = med.get("schema", {}) or {}
    lines.append(metric_lines(
        "Schema.org Поддерживаемые схемы Schemaorg от Яндекса",
        b2.get("schema_supported_status", "-"),
        [
            f"schema.any: {schema.get('any')}",
            "schema.types: " + ", ".join((schema.get("types") or [])) if schema.get("types") else "schema.types: не найдены",
            "schema.supported_hits: " + ", ".join((b2.get("schema_supported_hits") or [])) if b2.get("schema_supported_hits") else "schema.supported_hits: не найдены",
        ],
    ))

    return lines


def block3_poc_lines(audit, summary):
    b3 = summary.get("b3", {}) or {}
    tech = audit.get("tech", {}) or {}
    ssl_info = tech.get("ssl", {}) or {}
    http_to_https = tech.get("http_to_https", {}) or {}
    sec = tech.get("security_headers", {}) or {}
    mixed = tech.get("mixed_content", {}) or {}
    broken_links = tech.get("broken_internal_links", {}) or {}
    broken_res = tech.get("broken_static_resources", {}) or {}
    ttfb = tech.get("ttfb", {}) or {}
    pagespeed = tech.get("pagespeed", {}) or {}
    canonical = tech.get("canonical_www", {}) or {}
    analytics = tech.get("analytics", {}) or {}

    lines = []
    lines.append(metric_lines("SSL валиден", b3.get("ssl_valid_status", "-"), [
        f"ssl.ok: {ssl_info.get('ok')}",
        f"issuer: {ssl_info.get('issuer_cn')}",
        f"protocol: {ssl_info.get('protocol')}",
    ]))
    lines.append(metric_lines("Срок действия SSL (дней до истечения)", b3.get("ssl_expiry_status", "-"), [
        f"not_after: {ssl_info.get('not_after')}",
        f"days_left: {ssl_info.get('days_left')}",
    ]))
    lines.append(metric_lines("HTTP → HTTPS редирект", b3.get("http_to_https_status", "-"), [
        f"requested: {http_to_https.get('requested')}",
        f"final_url: {http_to_https.get('final_url')}",
        f"redirected_to_https: {http_to_https.get('redirected_to_https')}",
    ]))
    hsts_value = (sec.get("values") or {}).get("strict-transport-security")
    lines.append(metric_lines("HSTS включен", b3.get("hsts_status", "-"), [
        f"HSTS value: {hsts_value if hsts_value else 'отсутствует'}",
    ]))
    mixed_samples = (mixed.get("samples") or [])[:3]
    lines.append(metric_lines("Смешанный контент (HTTP на HTTPS)", b3.get("mixed_content_status", "-"), [
        f"mixed_count: {mixed.get('count')}",
    ] + [f"{x.get('page')} -> {x.get('asset')}" for x in mixed_samples]))
    lines.append(metric_lines("Security headers baseline (CSP/XFO/XCTO/Referrer)", b3.get("security_headers_status", "-"), [
        "present: " + ", ".join(sec.get("present", []) or []) if sec.get("present") else "present: нет",
        "missing: " + ", ".join(sec.get("missing", []) or []) if sec.get("missing") else "missing: нет",
    ]))
    lines.append(metric_lines("SPF запись", b3.get("spf_status", "-"), [x for x in (summary.get("spf_dmarc_lines") or []) if "SPF:" in x][:1] or ["SPF: нет данных"]))
    lines.append(metric_lines("DMARC запись + p=", b3.get("dmarc_status", "-"), [x for x in (summary.get("spf_dmarc_lines") or []) if "DMARC:" in x][:1] or ["DMARC: нет данных"]))
    lines.append(metric_lines("DKIM (селекторы/наличие)", b3.get("dkim_status", "-"), (summary.get("dkim_lines") or ["DKIM: нет данных"])[:3]))
    lines.append(metric_lines("Битые внутренние ссылки (4xx/5xx)", b3.get("broken_internal_links_status", "-"), [
        f"checked: {broken_links.get('checked')}",
        f"broken: {broken_links.get('broken')}",
    ] + [f"{x.get('url')} [{x.get('status')}]" for x in (broken_links.get("samples") or [])[:3]]))
    lines.append(metric_lines("Битые статические ресурсы (JS/CSS/img)", b3.get("broken_static_resources_status", "-"), [
        f"checked: {broken_res.get('checked')}",
        f"broken: {broken_res.get('broken')}",
    ] + [f"{x.get('url')} [{x.get('status')}]" for x in (broken_res.get("samples") or [])[:3]]))
    lines.append(metric_lines("TTFB", b3.get("ttfb_status", "-"), [
        f"ttfb_seconds: {ttfb.get('seconds')}",
        f"source_url: {ttfb.get('source_url')}",
    ]))
    lines.append(metric_lines("PageSpeed mobile + LCP", b3.get("pagespeed_status", "-"), [
        f"score: {pagespeed.get('score')}",
        f"lcp_seconds: {pagespeed.get('lcp_seconds')}",
        f"status: {pagespeed.get('status')}",
    ]))
    lines.append(metric_lines("www vs non-www canonical", b3.get("canonical_status", "-"), [
        f"same_canonical: {canonical.get('same_canonical')}",
        f"non_www.final_url: {(canonical.get('non_www') or {}).get('final_url')}",
        f"www.final_url: {(canonical.get('www') or {}).get('final_url')}",
    ]))
    lines.append(metric_lines("Веб-аналитика + цели/события", b3.get("analytics_goals_status", "-"), [
        f"analytics.found: {analytics.get('found')}",
        f"goals_found: {analytics.get('goals_found')}",
        "goal_markers: " + ", ".join(analytics.get("goal_markers", []) or []) if analytics.get("goal_markers") else "goal_markers: не найдены",
    ]))
    return lines


def block4_poc_lines(audit, summary):
    b4 = summary.get("b4", {}) or {}
    tech = audit.get("tech", {}) or {}
    med = tech.get("med_trust", {}) or {}
    footer_year = med.get("footer_year", {}) or {}
    text_typos = med.get("text_typos", {}) or {}

    lines = []
    lines.append(metric_lines("Страница врачей / специалистов", b4.get("doctors_page_status", "-"), [
        f"doctors_content_found: {med.get('doctors_content_found')}",
        "doctor_pages: " + ", ".join((med.get("doctor_pages") or [])[:5]) if med.get("doctor_pages") else "doctor_pages: не найдены",
    ]))
    lines.append(metric_lines("Адрес и карта на сайте", b4.get("address_map_status", "-"), [
        f"address_found: {med.get('address_found')}",
        f"map_found: {med.get('map_found')}",
    ]))
    lines.append(metric_lines("Часы работы", b4.get("hours_status", "-"), [
        f"hours_found: {med.get('hours_found')}",
    ]))
    reviews_evidence = med.get("reviews_evidence") or []
    review_lines = [f"reviews_found: {med.get('reviews_found')}"]
    if reviews_evidence:
        for ev in reviews_evidence[:5]:
            review_lines.append(
                f"{ev.get('page')} | {ev.get('signal')} | snippet={ev.get('snippet')}"
            )
    else:
        review_lines.append("reviews_evidence: не найдены")
    lines.append(metric_lines("Отзывы пациентов на сайте", b4.get("reviews_status", "-"), review_lines))
    lines.append(metric_lines("Актуальность года в футере (если он вообще есть). Если его нет — ок", b4.get("footer_year_status", "-"), [
        f"footer_present: {footer_year.get('present')}",
        f"current_year: {footer_year.get('current_year')}",
        f"year_value: {footer_year.get('year')}",
    ]))
    lines.append(metric_lines("Есть отдельная страница контактов", b4.get("contacts_page_status", "-"), [
        f"contact_page_exists: {med.get('contact_page_exists')}",
        f"contact_block_found: {med.get('contact_block_found')}",
        "contact_pages: " + ", ".join((med.get("contact_pages") or [])[:5]) if med.get("contact_pages") else "contact_pages: не найдены",
    ]))
    typo_samples = text_typos.get("samples") or []
    type_counts = {}
    type_examples = {}
    for x in typo_samples:
        t = str(x.get("type") or "other")
        type_counts[t] = int(type_counts.get(t) or 0) + 1
        type_examples.setdefault(t, [])
        if len(type_examples[t]) < 2:
            type_examples[t].append(x)
    top_types = sorted(type_counts.items(), key=lambda kv: kv[1], reverse=True)
    type_summary = ", ".join([f"{k}: {v}" for k, v in top_types]) if top_types else "нет"
    top_type_lines = []
    for t, c in top_types[:3]:
        top_type_lines.append(f"__H__:{t} ({c})")
        for ex in type_examples.get(t, []):
            top_type_lines.append(
                f"[{t}] {ex.get('match')} — {ex.get('snippet')} ({ex.get('page')})"
            )

    lines.append(metric_lines("Орфографические ошибки в тексте сайта", b4.get("text_typos_status", "-"), [
        "__H__:Сводка",
        f"Всего ошибок: {text_typos.get('error_count')}",
        f"Проверено страниц: {len(text_typos.get('checked_pages') or [])}",
        f"По типам: {type_summary}",
        "__H__:Топ типы и примеры",
    ] + top_type_lines))
    return lines


def build_detail_page(item, audit, s):
    pages = audit.get("pages", [])
    forbidden = filter_meta_hits(audit.get("forbidden_hits", []))
    policy_evidence = s.get("policy_evidence", []) or []
    discovery = audit.get("discovery", {})
    source_map = discovery.get("sources", {})
    verification_mode_label = (s.get("verification_mode") or {}).get("label", "не указан")

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
        visible_count = sum(
            1 for h in forbidden if "видно пользователю" in str(h.get("visibility", "") or "").lower()
        )
        code_only_count = len(forbidden) - visible_count
        direct_link_count = sum(
            1
            for h in forbidden
            if re.search(
                r'href\s*=\s*["\']https?://(?:www\.)?(?:instagram\.com|facebook\.com|fb\.com|threads\.net|meta\.com)\b',
                str(h.get("context", "") or ""),
                flags=re.IGNORECASE,
            )
        )
        meta_lines.append(f"visible_mentions_count: {visible_count}")
        meta_lines.append(f"code_only_mentions_count: {code_only_count}")
        meta_lines.append(f"direct_forbidden_links_count: {direct_link_count}")
        for h in forbidden[:80]:
            meta_lines.append(f"{h.get('token')} | {h.get('page')} | {h.get('visibility')} | {h.get('context')}")
    else:
        meta_lines.append("Совпадений по списку meta/instagram/facebook/threads не найдено.")

    policy_lines = []
    policy_validation = s.get("policy_validation", {}) or {}
    if s.get("site_unavailable"):
        policy_lines.append("Не проверено: сайт недоступен.")
    else:
        policy_lines.append(f"policy_document_present: {policy_validation.get('present')}")
        policy_lines.append(
            "checked_policy_urls: "
            + (", ".join((policy_validation.get("checked_urls") or [])[:10]) if policy_validation.get("checked_urls") else "не найдены")
        )
        policy_lines.append(
            "readable_policy_urls: "
            + (", ".join((policy_validation.get("readable_urls") or [])[:10]) if policy_validation.get("readable_urls") else "не найдены")
        )
        policy_lines.append(
            "unreadable_policy_urls: "
            + (", ".join((policy_validation.get("unreadable_urls") or [])[:10]) if policy_validation.get("unreadable_urls") else "не найдены")
        )
        policy_lines.append(
            "matched_policy_tokens: "
            + (", ".join((policy_validation.get("matched_tokens") or [])[:10]) if policy_validation.get("matched_tokens") else "не найдены")
        )
        policy_lines.append(
            "valid_policy_urls: "
            + (", ".join((policy_validation.get("valid_urls") or [])[:10]) if policy_validation.get("valid_urls") else "не найдены")
        )
        policy_lines.append(
            "policy_urls_with_broken_doc_links: "
            + (", ".join((policy_validation.get("broken_doc_urls") or [])[:10]) if policy_validation.get("broken_doc_urls") else "не найдены")
        )
        policy_lines.append(f"policy_evidence_count: {len(policy_evidence)}")
        for x in policy_evidence[:40]:
            if str(x.get("kind")) == "legal-page":
                policy_lines.append(f"legal-page | {x.get('page')} | source={x.get('source')}")
            elif str(x.get("kind")) == "form-policy-text":
                policy_lines.append(
                    f"form-policy-text | {x.get('page')} | source={x.get('source')} | snippet={x.get('text')}"
                )
            else:
                policy_lines.append(
                    f"anchor | {x.get('page')} -> {x.get('href')} (текст: {x.get('text')}) | source={x.get('source')}"
                )
        if not policy_evidence:
            policy_lines.append("Ссылка на политику не найдена.")

    availability_lines = [s["availability_poc"]]
    if found_bad:
        availability_lines.append(f"Недоступных найденных страниц: {len(found_bad)}.")

    consent_counts = s.get("consent_counts", {}) or {}
    site_unavailable = bool(s.get("site_unavailable"))
    missing_checkbox = int(consent_counts.get("не найдено", 0)) > 0
    text_only_consent = int(consent_counts.get("текстом", 0)) > 0
    prechecked = int(consent_counts.get("checked", 0)) > 0
    explicit_consent_missing = missing_checkbox or text_only_consent
    no_checkbox_status = "-" if site_unavailable else ("проблема" if missing_checkbox else "ок")
    prechecked_status = "-" if site_unavailable else ("проблема" if (prechecked or explicit_consent_missing) else "ок")
    cookie_status = "-" if site_unavailable else ("ок" if bool(s.get("cookie_notice_found")) else "проблема")
    if site_unavailable:
        third_party_policy_status = "-"
    elif not bool(s.get("has_yandex_metrika")):
        third_party_policy_status = "н/п"
    elif s.get("metrika_policy_disclosed") is None:
        third_party_policy_status = "проверить"
    else:
        third_party_policy_status = "ок" if bool(s.get("metrika_policy_disclosed")) else "проблема"

    block1_lines = [
        metric_lines(
            "Пациент не давал согласия на обработку данных",
            no_checkbox_status,
            [
                f"forms_total: {len(audit.get('forms', []) or [])}",
                f"consent_missing_count: {consent_counts.get('не найдено', 0)}",
            ] + [
                f"{f.get('page')} | {f.get('form_id')} | {f.get('action_display')}"
                for f in (s.get("consent_buckets", {}).get("не найдено", []) or [])[:8]
            ],
        ),
        metric_lines(
            "Согласие подставлено автоматически — это хуже чем его отсутствие",
            prechecked_status,
            [
                f"consent_prechecked_count: {consent_counts.get('checked', 0)}",
                f"explicit_consent_missing_count: {int(consent_counts.get('не найдено', 0)) + int(consent_counts.get('текстом', 0))}",
                f"consent_text_only_count: {consent_counts.get('текстом', 0)}",
                f"consent_checkbox_missing_count: {consent_counts.get('не найдено', 0)}",
            ] + [
                f"{f.get('page')} | {f.get('form_id')} | {f.get('action_display')}"
                for f in (s.get("consent_buckets", {}).get("checked", []) or [])[:8]
            ] + [
                f"text-only | {f.get('page')} | {f.get('form_id')} | {f.get('action_display')}"
                for f in (s.get("consent_buckets", {}).get("текстом", []) or [])[:8]
            ] + [
                f"missing-checkbox | {f.get('page')} | {f.get('form_id')} | {f.get('action_display')}"
                for f in (s.get("consent_buckets", {}).get("не найдено", []) or [])[:8]
            ],
        ),
        metric_lines(
            "На сайте нет обязательного документа об обработке данных пациентов",
            s["policy_status"],
            policy_lines[:12] or ["Ссылка на политику не найдена."],
        ),
        metric_lines(
            "Имя и телефон пациента передаются в открытом виде — любой может перехватить",
            s["form_https_status"],
            form_https_lines[:12] or ["Признаков передачи по HTTP не найдено."],
        ),
        metric_lines(
            "На сайте упоминается организация, признанная в России экстремистской",
            s["meta_status"],
            meta_lines[:12],
        ),
        metric_lines(
            "Сайт собирает данные пациентов без их уведомления",
            cookie_status,
            build_cookie_notice_poc(audit, s),
        ),
        metric_lines(
            "Яндекс.Метрика собирает данные ваших пациентов — в политике об этом ни слова",
            third_party_policy_status,
            build_metrika_policy_poc(audit, s),
        ),
    ]

    block1_metric_statuses = [m.get("status", "-") for m in block1_lines]
    if any(st == "проблема" for st in block1_metric_statuses):
        block1_status = "проблема"
    elif any(st in {"-", "проверить", "частично", "рекомендация"} for st in block1_metric_statuses):
        block1_status = "проверить"
    else:
        block1_status = "ок"

    block2_lines = (
        block2_poc_lines(audit, s)
        if s.get("block2_verified")
        else [metric_lines("Блок 2", "-", ["Блок 2 не верифицирован для этой клиники. Статусы блока скрыты ('-')."])]
    )
    block4_lines = (
        block4_poc_lines(audit, s)
        if s.get("block3_verified")
        else [metric_lines("Блок 3", "-", ["Блок 3 не верифицирован для этой клиники. Статусы блока скрыты ('-')."])]
    )

    sections = "".join([
        details_section("Доступность сайта", s["availability_status"], availability_lines),
        details_section("Согласия (расширенная аналитика)", s.get("consent_status", "-"), consent_lines),
        details_section_grouped("Блок 1 — PoC / Findings", block1_status, block1_lines, "block-tone-b1"),
        details_section_grouped("Блок 2 — PoC / Findings", "ок" if s.get("block2_verified") else "-", block2_lines, "block-tone-b2"),
        details_section_grouped("Блок 3 — PoC / Findings", "ок" if s.get("block3_verified") else "-", block4_lines, "block-tone-b4"),
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
.block-tone-b1{{background:#fff3f4;border-color:#f6d6da;box-shadow:inset 4px 0 0 #e2909a}}
.block-tone-b2{{background:#f4f8ff;border-color:#dbe7ff;box-shadow:inset 4px 0 0 #8db3ff}}
.block-tone-b3{{background:#f4fbf6;border-color:#d8eddd;box-shadow:inset 4px 0 0 #83c596}}
.block-tone-b4{{background:#fff8f2;border-color:#f1e1cf;box-shadow:inset 4px 0 0 #d9ad7b}}
.metric-grid{{display:grid;grid-template-columns:1fr;gap:10px}}
.metric-item{{border:1px solid #e6e8ef;border-radius:10px;padding:10px 12px;background:#fbfcff}}
.metric-head{{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:6px}}
.metric-title{{font-weight:700;color:#1f2430;font-size:14px;line-height:1.3}}
.metric-item ul{{margin:0;padding-left:18px;line-height:1.45}}
.metric-item li{{margin:3px 0;color:#455066}}
.evidence-subhead{{margin:8px 0 4px;padding:4px 8px;border-radius:8px;background:#eef2ff;color:#2f4370;font-size:12px;font-weight:700;display:inline-block}}
</style>
</head>
<body>
  <div class=\"wrap\">
    <div class=\"top\">
      <div>
        <a href=\"../screening-step-2.html\">← Скрининг клиник шаг 2</a>
        <span style=\"color:#9aa6bd\">&nbsp;·&nbsp;</span>
        <a href=\"../final-report-blocks.html\">Блоки финального отчёта →</a>
        <h1>{esc(item['clinic'])}</h1>
        <div>{esc(item['site'])}</div>
      </div>
      <div style=\"display:flex;gap:8px;flex-wrap:wrap\">
        <div class=\"card\">Итог: <span class=\"badge {badge_class(s['result'])}\">{esc(s['result'])}</span></div>
        <div class=\"card\">Проверка: <span class=\"badge {badge_class(verification_mode_label)}\">{esc(verification_mode_label)}</span></div>
      </div>
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
                "Нет онлайн-записи со слотами",
                "Сайт — цифровая визитка, не инструмент",
                "Вы не знаете кто приходит на сайт и почему уходит",
                "Пациент не может написать первым",
                "Прайс-лист доступен без регистрации",
                "Schema.org Поддерживаемые схемы Schemaorg от Яндекса",
            ],
        },
        {
            "id": "b3",
            "title": "Блок 3",
            "metric_names": [
                "Страница врачей / специалистов",
                "Адрес и карта на сайте",
                "Часы работы",
                "Отзывы пациентов на сайте",
                "Актуальность года в футере (если он вообще есть). Если его нет — ок",
                "Есть отдельная страница контактов",
                "Орфографические ошибки в тексте сайта",
            ],
        },
    ]


def metric_tooltip(block_id: str, metric_idx: int, metric_name: str) -> str:
    block2 = {
        0: "Оценка: 'ок' — есть рабочая онлайн-запись (слоты ИЛИ отдельная рабочая страница записи); 'проблема' — только форма/звонок без онлайн-инструмента записи.",
        1: "Оценка: 'ок' — страниц сайта >=5; 'проблема' — страниц сайта <=4.",
        2: "Оценка: 'ок' — аналитика установлена; 'проблема' — аналитика не найдена. Наличие целей/событий показывается как quality-flag в PoC.",
        3: "Оценка: 'ок' — есть хотя бы один рабочий канал первого письменного контакта (WhatsApp/Telegram/Max/чат/форма сообщения); 'проблема' — такого канала нет.",
        4: "Оценка: 'ок' — найдено явное публичное ценовое доказательство; 'проблема' — релевантные ценовые страницы проверены, но доказательства нет; 'проверить' — недостаточно надежного покрытия для отрицательного вывода.",
        5: "Оценка: 'ок' — найден хотя бы один поддерживаемый type (Organization/Medical*/LocalBusiness и др. из whitelist); 'проблема' — поддерживаемые типы не найдены.",
    }
    if block_id == "b2":
        return block2.get(metric_idx, "")
    return ""


def step2_blocks_data(summary):
    consent_counts = summary.get("consent_counts", {}) or {}
    site_unavailable = summary.get("site_unavailable", False)
    b2 = summary.get("b2", {}) or {}
    b3 = summary.get("b3", {}) or {}
    missing_checkbox = int(consent_counts.get("не найдено", 0)) > 0
    text_only_consent = int(consent_counts.get("текстом", 0)) > 0
    prechecked = int(consent_counts.get("checked", 0)) > 0
    explicit_consent_missing = missing_checkbox or text_only_consent
    cookie_status = "-" if site_unavailable else ("ок" if bool(summary.get("cookie_notice_found")) else "проблема")
    if site_unavailable:
        third_party_policy_status = "-"
    elif not bool(summary.get("has_yandex_metrika")):
        third_party_policy_status = "н/п"
    elif summary.get("metrika_policy_disclosed") is None:
        third_party_policy_status = "проверить"
    else:
        third_party_policy_status = "ок" if bool(summary.get("metrika_policy_disclosed")) else "проблема"
    b4 = summary.get("b4", {}) or {}
    block2_default_status = "-" if site_unavailable else "проверить"
    block2_verified = bool(summary.get("block2_verified"))
    block3_verified = bool(summary.get("block3_verified"))
    no_checkbox_status = "-" if site_unavailable else ("проблема" if missing_checkbox else "ок")
    prechecked_status = "-" if site_unavailable else ("проблема" if (prechecked or explicit_consent_missing) else "ок")

    b2_values = [
        b2.get("online_slots_status", block2_default_status),
        b2.get("digital_tool_status", block2_default_status),
        b2.get("analytics_status", block2_default_status),
        b2.get("after_hours_status", block2_default_status),
        b2.get("price_public_status", block2_default_status),
        b2.get("schema_supported_status", block2_default_status),
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
        b4.get("doctors_page_status", "-" if site_unavailable else "проблема"),
        b4.get("address_map_status", "-" if site_unavailable else "проблема"),
        b4.get("hours_status", "-" if site_unavailable else "проблема"),
        b4.get("reviews_status", "-" if site_unavailable else "проблема"),
        b4.get("footer_year_status", "-" if site_unavailable else "проблема"),
        b4.get("contacts_page_status", "-" if site_unavailable else "проблема"),
        b4.get("text_typos_status", "-" if site_unavailable else "проблема"),
    ]
    if not block3_verified:
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
        "b3": b4_values,
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
            f'<th class="group-{esc(bid)}{edge_cls}" colspan="{col_count}"><div class="col-head"><span class="col-title">{esc(title)}</span><div class="col-actions"><button class="method-btn" data-methodology-open="{esc(bid)}" type="button" aria-label="Посмотреть методологию" title="Посмотреть методологию">i</button><button class="col-toggle" data-block-toggle="{esc(bid)}" type="button" aria-label="Свернуть блок" title="Свернуть блок">▾</button></div></div></th>'
        )
        ph_edge = " group-edge" if block_idx > 0 else ""
        sub.append(f'<th class="metric-col {esc(bid)}-ph metric-head-col block-ph{ph_edge} is-hidden-col"></th>')
        for metric_idx, metric_name in enumerate(block["metric_names"]):
            metric_edge = " group-edge" if block_idx > 0 and metric_idx == 0 else ""
            tooltip = metric_tooltip(bid, metric_idx, metric_name)
            tooltip_attr = f' title="{esc(tooltip)}"' if tooltip else ""
            sub.append(
                f'<th class="metric-col {esc(bid)} metric-head-col{metric_edge}">'
                f'<span class="metric-label"{tooltip_attr}>{esc(metric_name)}</span>'
                f'</th>'
            )
    return "<tr>" + "".join(top) + "</tr><tr>" + "".join(sub) + "</tr>"


def row_html_step2(row_num, site_id, clinic, site, s, schema):
    block_values = step2_blocks_data(s)
    external = site_url(site)
    mode_label = (s.get("verification_mode") or {}).get("label", "не указан")
    parts = [
        f'<tr id="row-step2-{esc(site_id)}" class="clickable" data-href="sites/{esc(site_id)}.html" tabindex="0">',
        f'<td class="id-col">{esc(row_num)}</td>',
        (
            '<td class="clinic-col">'
            f'<div class="clinic-name" title="{esc(clinic)}">{esc(clinic)}</div>'
            f'<div class="site"><a class="site-link" href="{esc(external)}" target="_blank" rel="noopener noreferrer">{esc(site)}</a></div>'
            f'<div class="site" style="margin-top:4px"><span class="badge {badge_class(mode_label)}">{esc(mode_label)}</span></div>'
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
    .table-wrap {{ margin-top:12px; background:#fff; border:1px solid var(--line); border-radius:12px; overflow:visible; }}
    .table-scroll {{ overflow-x:auto; overflow-y:visible; border-radius:12px; }}
    table {{ width:100%; min-width:3200px; border-collapse:collapse; table-layout:fixed; }}
    thead th {{ position:sticky; top:0; z-index:5; text-align:left; background:#fafbfe; border-bottom:1px solid var(--line); color:#576072; font-size:10px; letter-spacing:.01em; text-transform:none; font-weight:700; padding:9px 8px; white-space:normal; line-height:1.2; }}
    tbody td {{ border-bottom:1px solid var(--line); padding:6px 8px; font-size:12px; vertical-align:top; line-height:1.2; }}
    tbody tr:last-child td {{ border-bottom:0; }}
    tr.clickable{{cursor:pointer}} tr.clickable:hover td{{background:#f7f9ff}}
    .site {{ color:#5e6678; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:11px; overflow-wrap:anywhere; }}
    .site-link {{ color:#455066; text-decoration:none; border-bottom:1px dotted #9aa6bd; }}
    .site-link:hover {{ color:#24324f; text-decoration:none; border-bottom-color:#24324f; }}
    .clinic {{ font-weight:700; font-size:11px; overflow-wrap:anywhere; }}
    .badge {{ display:inline-block; max-width:100%; padding:3px 8px; border-radius:999px; border:1px solid transparent; font-size:10px; font-weight:700; line-height:1.2; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .ok {{ background:var(--ok-bg); color:var(--ok-fg); border-color:#c8efd9; }}
    .warn {{ background:var(--warn-bg); color:var(--warn-fg); border-color:#f0d889; }}
    .bad {{ background:var(--bad-bg); color:var(--bad-fg); border-color:#f7c4c8; }}
    .na {{ background:var(--na-bg); color:var(--na-fg); border-color:#dde2ea; }}
    .id-col-head {{ width:52px; min-width:52px; position:sticky; left:0; z-index:7; box-shadow:1px 0 0 #e9edf6; text-align:center; }}
    .id-col {{ width:52px; min-width:52px; position:sticky; left:0; z-index:3; box-shadow:1px 0 0 #e9edf6; background:#fff; text-align:center; color:#6a7385; font-weight:700; font-size:11px; }}
    .clinic-col {{ background:#fff; width:220px; min-width:220px; position:sticky; left:52px; z-index:2; box-shadow:1px 0 0 #e9edf6; padding:8px 8px; }}
    .clinic-name {{ font-weight:500; font-size:14px; color:#1f2430; line-height:1.25; overflow-wrap:anywhere; }}
    .metric-col {{ text-align:center; background:#fcfdff; border-left:1px solid #edf1f7; min-width:98px; }}
    .group-edge {{ border-left:3px solid #d0d8e8 !important; }}
    .col-head {{ position:relative; display:flex; justify-content:flex-end; align-items:center; min-height:24px; }}
    .col-title {{ position:absolute; left:50%; transform:translateX(-50%); width:100%; padding:0 56px 0 8px; text-align:center; font-size:12px; font-weight:800; color:#394153; pointer-events:none; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .col-actions {{ position:relative; z-index:1; display:flex; align-items:center; gap:6px; }}
    .method-btn {{ width:22px; height:22px; border:1px solid #ccd4e2; background:#fff; color:#2b3343; border-radius:7px; padding:0; font-size:12px; font-weight:700; line-height:1; cursor:pointer; }}
    .method-btn:hover {{ border-color:#9fb2d3; color:#1d2a41; }}
    .col-toggle {{ width:22px; height:22px; border:1px solid #ccd4e2; background:#fff; color:#2b3343; border-radius:7px; padding:0; font-size:14px; font-weight:700; line-height:1; cursor:pointer; }}
    .col-toggle:hover {{ border-color:#9fb2d3; color:#1d2a41; }}
    th.group-collapsed .col-head {{ justify-content:center; }}
    th.group-collapsed .col-title {{ display:none; }}
    .metric-label {{ display:block; }}
    .is-hidden-col {{ display:none !important; }}
    .notes {{ margin-top:10px; background:#fff; border:1px solid var(--line); border-radius:10px; padding:10px 12px; font-size:12px; color:#4e5565; line-height:1.35; }}
    .clinic-col-head {{ width:220px; min-width:220px; position:sticky; left:52px; z-index:7; box-shadow:1px 0 0 #e9edf6; }}
    .metric-head-col {{ min-width:98px; font-size:9px; line-height:1.2; font-weight:600; }}
    .method-modal {{ position:fixed; inset:0; display:none; z-index:2000; }}
    .method-modal.is-open {{ display:block; }}
    .method-modal-backdrop {{ position:absolute; inset:0; background:rgba(20,28,45,.45); }}
    .method-modal-panel {{ position:relative; width:min(920px, calc(100vw - 24px)); max-height:calc(100vh - 24px); margin:12px auto; background:#fff; border:1px solid #dbe2ef; border-radius:12px; box-shadow:0 20px 50px rgba(18,27,45,.18); overflow:auto; }}
    .method-modal-head {{ display:flex; justify-content:space-between; align-items:center; gap:12px; padding:12px 14px; border-bottom:1px solid #e7ecf5; position:sticky; top:0; background:#fff; }}
    .method-modal-title {{ font-size:18px; font-weight:700; color:#2b3343; }}
    .method-modal-close {{ width:28px; height:28px; border:1px solid #cfd7e6; background:#fff; color:#2a3346; border-radius:8px; font-size:18px; line-height:1; cursor:pointer; }}
    .method-modal-close:hover {{ border-color:#9fb2d3; }}
    .method-modal-body {{ padding:14px; font-size:13px; line-height:1.45; color:#2f3747; }}
    .method-modal-body h4 {{ margin:12px 0 6px; font-size:14px; color:#1f2737; }}
    .method-modal-body p {{ margin:0 0 8px; }}
    .method-modal-body ul {{ margin:0 0 8px 16px; padding:0; }}
    .method-modal-body li {{ margin:0 0 5px; }}
    .method-template {{ display:none; }}
    .table-head-fixed {{ position:fixed; top:0; left:0; z-index:1600; display:none; pointer-events:auto; }}
    .table-head-fixed table {{ border-collapse:collapse; table-layout:fixed; margin:0; }}
    .table-head-fixed thead th {{ background:#fafbfe; }}
  </style>
</head>
<body>
  <div class=\"wrap\">
    <h1>Скрининг клиник шаг 2</h1>
    <div class=\"sub\">Слева клиники, далее вертикальные блоки метрик по каждой клинике</div>
    <div class=\"meta\"><a class=\"meta-link\" href=\"final-report-blocks.html\">Блоки финального отчёта →</a></div>

    <div class=\"cards\">
      <div class=\"card\"><div class=\"n ok\">{counts.get('слать', 0)}</div><div class=\"l\">Слать</div></div>
      <div class=\"card\"><div class=\"n warn\">{counts.get('проверить', 0)}</div><div class=\"l\">Проверить</div></div>
      <div class=\"card\"><div class=\"n na\">{counts.get('не слать', 0)}</div><div class=\"l\">Не слать</div></div>
      <div class=\"card\"><div class=\"n bad\">{unavailable}</div><div class=\"l\">Недоступны</div></div>
      <div class=\"card\"><div class=\"n total\">{total}</div><div class=\"l\">Всего</div></div>
    </div>

    <div class=\"table-wrap\">
      <div class=\"table-scroll\">
        <table>
          <thead>
            {header_rows}
          </thead>
          <tbody>
            {''.join(rows_step2)}
          </tbody>
        </table>
      </div>
    </div>

    <div class=\"notes\">
      Сворачивание работает на уровне блока: кнопка в заголовке `Блок 1/2/3` скрывает или показывает все метрики этого блока сразу для всех клиник.
    </div>
  </div>

  <div class=\"method-template\" id=\"methodology-b1\">
    <h4>Общие правила</h4>
    <ul>
      <li>Один агент = одна клиника = один прогон.</li>
      <li>Проверка выполняется агентом в браузере с рендерингом JS; в audit JSON фиксируются результаты и PoC.</li>
      <li>Статусы: <b>ок</b>, <b>проблема</b>, <b>проверить</b> (если покрытие недостаточно для надежного отрицательного вывода).</li>
      <li>Fallback-эвристики не используются: статус ставится только по фактически найденным сигналам (PoC).</li>
    </ul>
    <h4>1. Пациент не давал согласия на обработку данных</h4>
    <p><b>ок:</b> на всех формах найден маркер согласия (чекбокс или корректный текст согласия). <b>проблема:</b> есть формы без маркера согласия. Предустановленный чекбокс оценивается отдельно в п.2.</p>
    <h4>2. Согласие подставлено автоматически — это хуже чем его отсутствие</h4>
    <p><b>ок:</b> есть отдельное явное согласие до отправки формы (чекбокс/эквивалент) и нет предустановленных чекбоксов. <b>проблема:</b> есть хотя бы один prechecked-чекбокс ИЛИ нет отдельного явного согласия (только текст «нажимая кнопку…» или чекбокс отсутствует).</p>
    <h4>3. На сайте нет обязательного документа об обработке данных пациентов</h4>
    <p><b>ок:</b> найдена реальная policy-страница/документ ПДн по ссылкам сайта (навигация/контент) с PoC URL и контекстом. <b>проблема:</b> такого доказательства нет. Технические псевдо-ссылки (например, action/data-href без реальной страницы) не засчитываются.</p>
    <h4>4. Имя и телефон пациента передаются в открытом виде — любой может перехватить</h4>
    <p><b>ок:</b> формы не отправляют данные на HTTP (используется HTTPS). <b>проблема:</b> найден хотя бы один HTTP action.</p>
    <h4>5. На сайте упоминается организация, признанная в России экстремистской</h4>
    <p><b>ок:</b> упоминаний Meta/Instagram/Facebook/Threads не найдено. <b>проверить:</b> упоминания есть только в коде, без видимого текста и без прямых внешних ссылок. <b>проблема:</b> найдено видимое пользователю упоминание или прямая внешняя ссылка (например, href на instagram.com/facebook.com/threads.net/meta.com).</p>
    <h4>6. Сайт собирает данные пациентов без их уведомления</h4>
    <p><b>ок:</b> на реально открытых страницах найдены признаки уведомления о cookies/согласии (баннер/текст/ссылка на cookie-policy). <b>проблема:</b> признаки не найдены.</p>
    <h4>7. Яндекс.Метрика собирает данные ваших пациентов — в политике об этом ни слова</h4>
    <p><b>ок:</b> либо Метрика не найдена, либо найдена и явно упомянута в найденной policy-странице/документах. <b>проблема:</b> Метрика найдена, но упоминаний в policy-документах нет. Проверяются только реально найденные policy URL (без fallback на типовые пути).</p>
  </div>
  <div class=\"method-template\" id=\"methodology-b2\">
    <h4>Общие правила</h4>
    <ul>
      <li>Один агент = одна клиника = один прогон.</li>
      <li>Проверка в браузере с рендерингом JS.</li>
      <li>Статусы: <b>ок</b>, <b>проблема</b>, <b>проверить</b> (если покрытия недостаточно для надежного отрицательного вывода).</li>
      <li>При противоречивых сигналах используем <b>проверить</b>, а не автоматическую <b>проблему</b>.</li>
    </ul>
    <h4>1. Нет онлайн-записи со слотами</h4>
    <p><b>ок:</b> есть рабочая онлайн-запись: либо выбор даты/времени (слоты), либо отдельная рабочая страница записи. <b>проблема:</b> только форма/звонок без онлайн-инструмента записи.</p>
    <h4>2. Сайт — цифровая визитка, не инструмент</h4>
    <p><b>ок:</b> страниц сайта &gt;= 5. <b>проблема:</b> страниц сайта &lt;= 4.</p>
    <h4>3. Вы не знаете кто приходит на сайт и почему уходит</h4>
    <p><b>ок:</b> аналитика установлена (Яндекс.Метрика/аналог). <b>проблема:</b> аналитика не найдена. Признаки целей/событий фиксируются отдельно как quality-flag в PoC и не являются автоматическим фейлом.</p>
    <h4>4. Пациент не может написать первым</h4>
    <p><b>ок:</b> есть хотя бы один рабочий канал первого письменного контакта (WhatsApp/Telegram/Max/онлайн-чат/форма сообщения). <b>проблема:</b> нет ни одного рабочего канала для первого письменного контакта.</p>
    <h4>5. Прайс-лист доступен без регистрации</h4>
    <p><b>ок:</b> найден публичный ценовой контент (страница/блок) с явными ценами на услуги. <b>проблема:</b> релевантные ценовые страницы проверены, но ценового доказательства нет. <b>проверить:</b> релевантные страницы не обнаружены/непрочитаны, отрицательный вывод ненадежен.</p>
    <h4>6. Schema.org Поддерживаемые схемы Schemaorg от Яндекса</h4>
    <p><b>ок:</b> найден хотя бы один поддерживаемый тип из whitelist (Organization/Medical*/LocalBusiness и др.). <b>проблема:</b> поддерживаемые типы не найдены.</p>
  </div>
  <div class=\"method-template\" id=\"methodology-b3\">
    <h4>Общие правила</h4>
    <ul>
      <li>Один агент = одна клиника = один прогон.</li>
      <li>Проверка строится на фактах из crawl (URL, контент, структурные сигналы).</li>
      <li>Результат строго бинарный: <b>ок</b> или <b>проблема</b>.</li>
    </ul>
    <h4>1. Страница или карточки врачей / специалистов</h4>
    <p><b>ок:</b> найдены блоки с информацией о врачах или отдельные страницы врачей/специалистов. <b>проблема:</b> не найдено ни блоков, ни страниц.</p>
    <h4>2. Адрес и карта на сайте</h4>
    <p><b>ок:</b> найден адрес или embed-карта (Яндекс/2GIS/Google). <b>проблема:</b> не найдено ни адреса, ни карты.</p>
    <h4>3. Часы работы</h4>
    <p><b>ок:</b> найдены явные маркеры режима работы (часы/дни). <b>проблема:</b> не найдено.</p>
    <h4>4. Отзывы пациентов на сайте</h4>
    <p><b>ок:</b> найден блок/виджет отзывов на сайте. <b>проблема:</b> не найдено.</p>
    <h4>5. Актуальность года в футере (если он вообще есть). Если его нет — ок</h4>
    <p><b>ок:</b> футер отсутствует или в футере указан актуальный год. <b>проблема:</b> футер есть, но год неактуален. <b>проверить:</b> данные о футере заполнены не полностью.</p>
    <h4>6. Есть отдельная страница контактов</h4>
    <p><b>ок:</b> найдена отдельная страница контактов или контактный блок на главной. <b>проблема:</b> не найдено ни страницы, ни блока контактов.</p>
    <h4>7. Орфографические ошибки в тексте сайта</h4>
    <p><b>ок:</b> найдено 5 ошибок или меньше. <b>проблема:</b> найдено больше 5 ошибок.</p>
  </div>

  <div class=\"method-modal\" id=\"methodology-modal\" aria-hidden=\"true\">
    <div class=\"method-modal-backdrop\" data-methodology-close=\"1\"></div>
    <div class=\"method-modal-panel\" role=\"dialog\" aria-modal=\"true\" aria-labelledby=\"methodology-modal-title\">
      <div class=\"method-modal-head\">
        <div class=\"method-modal-title\" id=\"methodology-modal-title\">Методология</div>
        <button class=\"method-modal-close\" type=\"button\" data-methodology-close=\"1\" aria-label=\"Закрыть\">×</button>
      </div>
      <div class=\"method-modal-body\" id=\"methodology-modal-body\"></div>
    </div>
  </div>

  <script>
    const BLOCK_COL_COUNTS = {block_col_counts_json};
    const BLOCK_TITLES = {{
      b1: 'Блок 1',
      b2: 'Блок 2',
      b3: 'Блок 3',
    }};

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

    const methodModal = document.getElementById('methodology-modal');
    const methodModalTitle = document.getElementById('methodology-modal-title');
    const methodModalBody = document.getElementById('methodology-modal-body');

    function openMethodology(blockId) {{
      if (!methodModal || !methodModalTitle || !methodModalBody) return;
      const tpl = document.getElementById('methodology-' + blockId);
      const title = BLOCK_TITLES[blockId] || 'Блок';
      methodModalTitle.textContent = title + ' — методология проверки';
      methodModalBody.innerHTML = tpl ? tpl.innerHTML : '<p>Методология пока не добавлена.</p>';
      methodModal.classList.add('is-open');
      methodModal.setAttribute('aria-hidden', 'false');
      document.body.style.overflow = 'hidden';
    }}

    function closeMethodology() {{
      if (!methodModal) return;
      methodModal.classList.remove('is-open');
      methodModal.setAttribute('aria-hidden', 'true');
      document.body.style.overflow = '';
    }}

    document.querySelectorAll('[data-methodology-open]').forEach(function(btn) {{
      btn.addEventListener('click', function(e) {{
        e.stopPropagation();
        const blockId = btn.dataset.methodologyOpen;
        if (!blockId) return;
        openMethodology(blockId);
      }});
    }});

    document.querySelectorAll('[data-methodology-close]').forEach(function(el) {{
      el.addEventListener('click', function() {{
        closeMethodology();
      }});
    }});

    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') closeMethodology();
    }});

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

    function initFixedHeader() {{
      const scrollWrap = document.querySelector('.table-scroll');
      if (!scrollWrap) return;
      const table = scrollWrap.querySelector('table');
      const thead = table ? table.querySelector('thead') : null;
      if (!table || !thead) return;

      const fixed = document.createElement('div');
      fixed.className = 'table-head-fixed';
      const fixedTable = document.createElement('table');
      fixedTable.appendChild(thead.cloneNode(true));
      fixed.appendChild(fixedTable);
      document.body.appendChild(fixed);

      function syncLayout() {{
        const rect = scrollWrap.getBoundingClientRect();
        const tableRect = table.getBoundingClientRect();
        const headHeight = thead.getBoundingClientRect().height || 0;
        const shouldShow = tableRect.top < 0 && tableRect.bottom > headHeight;
        fixed.style.display = shouldShow ? 'block' : 'none';
        if (!shouldShow) return;
        fixed.style.left = Math.round(rect.left) + 'px';
        fixed.style.width = Math.round(rect.width) + 'px';
        fixed.style.overflow = 'hidden';
        fixedTable.style.width = Math.round(table.getBoundingClientRect().width) + 'px';
        fixedTable.style.transform = 'translateX(' + (-scrollWrap.scrollLeft) + 'px)';
      }}

      function refreshClone() {{
        fixedTable.innerHTML = '';
        fixedTable.appendChild(thead.cloneNode(true));
        syncLayout();
      }}

      scrollWrap.addEventListener('scroll', syncLayout, {{ passive: true }});
      window.addEventListener('scroll', syncLayout, {{ passive: true }});
      window.addEventListener('resize', refreshClone);
      document.addEventListener('click', function(e) {{
        const t = e.target;
        if (t && t.closest && t.closest('[data-block-toggle]')) {{
          setTimeout(refreshClone, 0);
        }}
      }});
      refreshClone();
    }}

    initFixedHeader();
  </script>
</body>
</html>
"""



def parse_args():
    parser = argparse.ArgumentParser(description="Build screening dashboards and detail pages")
    parser.add_argument("--site-id", dest="site_ids_single", action="append", default=[], help="Build details only for this site id (repeatable)")
    parser.add_argument("--site-ids", dest="site_ids_csv", default="", help="Comma-separated site ids for detail build")
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Explicitly allow full rebuild for all clinics. Without this flag, only --site-id/--site-ids mode is allowed.",
    )
    return parser.parse_args()


def normalized_site_id_filter(args):
    selected = set()
    for x in args.site_ids_single or []:
        v = str(x or "").strip()
        if v:
            selected.add(v)
    for x in str(args.site_ids_csv or "").split(","):
        v = x.strip()
        if v:
            selected.add(v)
    return selected


def replace_step2_row_in_html(html_text: str, site_id: str, new_row_html: str):
    sid = re.escape(str(site_id))
    pattern = re.compile(rf"<tr id=\"row-step2-{sid}\"[^>]*>.*?</tr>", re.DOTALL)
    return pattern.sub(new_row_html, html_text, count=1), bool(pattern.search(html_text))


def main():
    ensure_agent_only_mode()
    args = parse_args()
    selected_site_ids = normalized_site_id_filter(args)
    selective_details = len(selected_site_ids) > 0

    if not selective_details and not args.full_rebuild:
        raise SystemExit(
            "Blocked by policy: full rebuild is disabled by default.\n"
            "Use --site-id/--site-ids for single-clinic updates, or pass --full-rebuild intentionally."
        )

    manifest = read_json(MANIFEST)
    step2_schema = step2_block_schema()

    # Selective mode: update only chosen clinics (detail pages + their row in step2 dashboards).
    if selective_details:
        manifest_index = {str(item.get("id")): (idx, item) for idx, item in enumerate(manifest, 1)}
        details = []
        updated_rows = []
        missing_ids = []

        for sid in selected_site_ids:
            pair = manifest_index.get(str(sid))
            if not pair:
                missing_ids.append(str(sid))
                continue
            idx, item = pair
            audit = read_json(ROOT / item["audit_file"])
            summary = compute_summary(item, audit)
            details.append((item["id"], build_detail_page(item, audit, summary)))
            updated_rows.append((item["id"], row_html_step2(idx, item["id"], item["clinic"], item["site"], summary, step2_schema)))

        sites_dir = ROOT / "sites"
        sites_dir.mkdir(parents=True, exist_ok=True)
        for site_id, page in details:
            (sites_dir / f"{site_id}.html").write_text(page, encoding="utf-8")

        for page_name in ["dashboard.html", "screening-step-2.html", "audit-blocks.html"]:
            page_path = ROOT / page_name
            if not page_path.exists():
                continue
            txt = read_text_best_effort(page_path)
            changed = False
            for sid, row in updated_rows:
                new_txt, found = replace_step2_row_in_html(txt, sid, row)
                if found:
                    txt = new_txt
                    changed = True
            if changed:
                page_path.write_text(txt, encoding="utf-8")

        print("Generated:")
        for site_id, _ in details:
            print(sites_dir / f"{site_id}.html")
        print("Patched rows in:")
        for page_name in ["dashboard.html", "screening-step-2.html", "audit-blocks.html"]:
            print(ROOT / page_name)
        print(f"Detail pages updated: {len(details)} selected")
        if missing_ids:
            print("Unknown site ids:", ", ".join(sorted(missing_ids)))
        return

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
    .table-wrap {{ margin-top:12px; background:#fff; border:1px solid var(--line); border-radius:12px; overflow:visible; }}
    .table-scroll {{ overflow-x:auto; overflow-y:visible; border-radius:12px; }}
    table {{ width:100%; min-width:0; border-collapse:collapse; table-layout:fixed; }}
    thead th {{ position:sticky; top:0; z-index:5; text-align:left; background:#fafbfe; border-bottom:1px solid var(--line); color:#576072; font-size:11px; letter-spacing:.04em; text-transform:uppercase; padding:10px 8px; white-space:normal; line-height:1.2; }}
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
      <div class=\"table-scroll\">
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

    screening_step_2 = build_screening_step2(
        rows_step2,
        counts,
        unavailable,
        len(manifest),
        step2_headers,
        json.dumps(step2_col_counts, ensure_ascii=False),
    )
    # Keep dashboard.html as the default entry point, but serve Step 2 content.
    (ROOT / "dashboard.html").write_text(screening_step_2, encoding="utf-8")
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

    if selective_details:
        skipped = [str(x.get("id")) for x in manifest if str(x.get("id")) not in selected_site_ids]
        print(f"Detail pages updated: {len(details)} selected")
        if skipped:
            print(f"Detail pages skipped: {len(skipped)}")


if __name__ == "__main__":
    main()


