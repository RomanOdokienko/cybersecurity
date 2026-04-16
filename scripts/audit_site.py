import argparse
import json
import re
import ssl
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlparse, urljoin

FORBIDDEN_TOKENS = [
    'meta', 'мета',
    'instagram', 'инстаграм', 'инстаграмм',
    'facebook', 'фейсбук',
    'threads',
    'instagram.com', 'facebook.com', 'fb.com', 'meta.com', 'threads.net'
]

CONTACT_FALLBACK_PATHS = ['/contacts', '/contact', '/kontakty']
BOOKING_FALLBACK_PATHS = ['/zapis', '/appointment', '/booking']
POLICY_FALLBACK_PATHS = [
    '/policy',
    '/privacy-policy',
    '/privacy',
    '/politika',
    '/agreement',
    '/personal-data',
    '/politika-konfidencialnosti',
]
MAX_SITEMAP_URLS = 120

CHECKBOX_RE = re.compile(r'(?is)<input\b[^>]*\btype\s*=\s*["\']?checkbox["\']?[^>]*>')
FORM_RE = re.compile(r'(?is)<form\b[^>]*>.*?</form>')
ALNUM = r'0-9A-Za-zА-Яа-яЁё'

BOOKING_SIGNAL_HINTS = [
    'запис',
    'консультац',
    'appointment',
    'booking',
    'online',
    'оставьте номер',
    'обратн',
    'перезвон',
]

BOOKING_URL_SOFT_HINTS = [
    'forma',
    'form',
    'regist',
    'regic',
    'callback',
    'consult',
    'anket',
]

LEGAL_URL_HINTS = [
    'documents',
    'document',
    'docs',
    'doc',
    'правов',
    'документ',
    'policy',
    'privacy',
    'polit',
    'legal',
]

PRIVACY_HINTS = [
    'политик',
    'конфиденц',
    'персональн',
    'privacy',
    'policy',
    'polit',
    'pdn',
    '152-фз',
    '152-fz',
]


def normalize_base(url: str) -> str:
    if not url.startswith('http://') and not url.startswith('https://'):
        url = 'https://' + url
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def fetch(url: str, ctx):
    req = urllib.request.Request(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept': (
                'text/html,application/xhtml+xml,application/xml;'
                'q=0.9,image/avif,image/webp,*/*;q=0.8'
            ),
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=40, context=ctx) as r:
            text = r.read().decode('utf-8', 'ignore')
            return {'url': url, 'status': int(r.getcode() or 0), 'final_url': r.geturl(), 'html': text, 'error': None}
    except urllib.error.HTTPError as e:
        try:
            text = e.read().decode('utf-8', 'ignore')
        except Exception:
            text = ''
        return {'url': url, 'status': int(e.code), 'final_url': url, 'html': text, 'error': f'HTTPError {e.code}'}
    except Exception as e:
        return {'url': url, 'status': None, 'final_url': url, 'html': '', 'error': str(e)}


def parse_sitemap(xml_text: str):
    urls = []
    try:
        root = ET.fromstring(xml_text)
        ns = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        for loc in root.findall('.//s:loc', ns):
            if loc.text:
                urls.append(loc.text.strip())
    except Exception:
        for m in re.finditer(r'(?is)<loc>(.*?)</loc>', xml_text):
            urls.append(m.group(1).strip())
    return urls


def dedupe_keep_order(items):
    out = []
    seen = set()
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def trim_sitemap_urls(urls, max_urls: int):
    urls = dedupe_keep_order(urls)
    if len(urls) <= max_urls:
        return urls

    high = []
    other = []
    for u in urls:
        low = u.lower()
        if is_contact_hint(low) or is_booking_hint(low) or is_legal_hint(low):
            high.append(u)
        else:
            other.append(u)

    out = []
    for u in high + other:
        if len(out) >= max_urls:
            break
        out.append(u)
    return out


def get_attr(tag: str, name: str):
    m = re.search(rf'(?is)\b{name}\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))', tag)
    if not m:
        return None, None
    if m.group(1) is not None:
        return m.group(1), 'double'
    if m.group(2) is not None:
        return m.group(2), 'single'
    return m.group(3), 'bare'


def clean(s: str):
    return re.sub(r'\s+', ' ', s).strip()


def strip_tags(text: str):
    return clean(re.sub(r'(?is)<[^>]*>', ' ', text))


def clean_href_value(href: str):
    h = (href or '').strip()
    if not h:
        return ''
    h = h.replace('\\/', '/').replace('\\"', '"').replace("\\'", "'")
    h = h.strip().strip('"').strip("'").strip()
    # Drop obviously broken href artifacts.
    if any(x in h for x in ['\\"', "\\'", '"', "'"]):
        return ''
    return h


def token_found(token: str, text: str):
    t = token.lower()
    v = text.lower()
    if '.' in t:
        return t in v
    patt = re.compile(rf'(?<![{ALNUM}]){re.escape(t)}(?![{ALNUM}])', re.IGNORECASE)
    return bool(patt.search(v))


def first_group(m):
    for i in range(1, (m.re.groups or 0) + 1):
        g = m.group(i)
        if g is not None:
            return g
    return ''


def extract_internal_links(base: str, html: str):
    html = re.sub(r'(?is)<script\b.*?</script>', ' ', html or '')
    html = re.sub(r'(?is)<style\b.*?</style>', ' ', html)
    links = []
    for m in re.finditer(r'(?is)<a\b[^>]*href\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))[^>]*>(.*?)</a>', html):
        href = clean_href_value(m.group(1) or m.group(2) or m.group(3) or '')
        text = strip_tags(m.group(4) or '')
        if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:') or href.startswith('tel:'):
            continue
        abs_url = urljoin(base + '/', href)
        if urlparse(abs_url).netloc == urlparse(base).netloc:
            links.append({'url': abs_url, 'text': text})
    return links


def is_contact_hint(s: str):
    s = s.lower()
    return any(k in s for k in ['контакт', 'contacts', 'contact', 'как добраться'])


def is_booking_hint(s: str):
    s = s.lower()
    return any(k in s for k in ['запис', 'appointment', 'booking', 'online'])


def is_legal_hint(s: str):
    s = s.lower()
    return any(k in s for k in LEGAL_URL_HINTS)


def has_privacy_hint(s: str):
    s = s.lower()
    return any(k in s for k in PRIVACY_HINTS)


def extract_policy_hint_urls(base: str, html: str):
    # Tilda/JS often keeps links in escaped form like \"\/policy\".
    norm = (html or '').replace('\\/', '/')
    out = set()

    for pat in [
        r'(?is)/policy(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/privacy-policy(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/privacy(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/politika(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/agreement(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/personal-data(?:[/?#][^"\'<>\s]*)?',
        r'(?is)/pdn(?:[/?#][^"\'<>\s]*)?',
    ]:
        for m in re.finditer(pat, norm):
            path = m.group(0)
            if path.startswith('/wp-content/') and '.pdf' not in path.lower():
                continue
            out.add(urljoin(base + '/', path))

    return out


def extract_forms(html: str):
    return [m.group(0) for m in FORM_RE.finditer(html)]


def form_open_tag(form_html: str):
    m = re.search(r'(?is)^<form\b[^>]*>', form_html)
    return m.group(0) if m else '<form>'


def is_search_form(form_html: str):
    opentag = form_open_tag(form_html)
    idv, _ = get_attr(opentag, 'id')
    nm, _ = get_attr(opentag, 'name')
    cls, _ = get_attr(opentag, 'class')
    act, _ = get_attr(opentag, 'action')
    attrs_low = ' '.join([x for x in [idv, nm, cls, act] if x]).lower()

    if 'search' in attrs_low:
        return True

    method, _ = get_attr(opentag, 'method')
    method_low = (method or '').lower()
    if method_low == 'get':
        if re.search(r'(?is)\btype\s*=\s*["\']?search["\']?', form_html):
            return True
        if re.search(r'(?is)\bname\s*=\s*(?:"(q|s|search|query)"|\'(q|s|search|query)\'|(q|s|search|query))', form_html):
            return True
    return False


def has_lead_form_fields(form_html: str):
    if is_search_form(form_html):
        return False

    if re.search(r'(?is)\btype\s*=\s*["\']?(tel|email)["\']?', form_html):
        return True

    if re.search(
        r'(?is)\bname\s*=\s*(?:"[^"]*(phone|tel|имя|name|email|message|comment)[^"]*"|\'[^\']*(phone|tel|имя|name|email|message|comment)[^\']*\'|[^\s>]*(phone|tel|имя|name|email|message|comment)[^\s>]*)',
        form_html
    ):
        return True

    opentag = form_open_tag(form_html)
    method, _ = get_attr(opentag, 'method')
    method_low = (method or '').lower()
    if method_low == 'post' and re.search(r'(?is)\btype\s*=\s*["\']?submit["\']?|<button\b', form_html):
        return True

    if re.search(r'(?is)<textarea\b', form_html) and re.search(r'(?is)\btype\s*=\s*["\']?submit["\']?|<button\b', form_html):
        return True

    return False


def has_booking_url_signal(url: str):
    url_low = url.lower()
    return any(k in url_low for k in BOOKING_URL_SOFT_HINTS)


def has_booking_form_signal(form_html: str):
    low = form_html.lower()
    return any(k in low for k in BOOKING_SIGNAL_HINTS)


def run_audit(base_url: str):
    base = normalize_base(base_url)
    host = urlparse(base).netloc
    https_base = f'https://{host}'
    http_base = f'http://{host}'
    ctx = ssl.create_default_context()
    fetch_cache = {}

    def fetch_cached(url: str):
        if url not in fetch_cache:
            fetch_cache[url] = fetch(url, ctx)
        return fetch_cache[url]

    discovery = {
        'contact_urls': [],
        'booking_urls': [],
        'sources': {},
        'fallback_used': {'contact': False, 'booking': False},
        'crawl_base': https_base,
        'crawl_transport': 'https',
        'https_home': None,
        'http_home': None,
        'sitemap_total_urls': 0,
        'sitemap_used_urls': 0,
    }

    # 1) always start from home (prefer HTTPS; fallback to HTTP only for content crawl)
    https_home_url = https_base + '/'
    https_home_resp = fetch_cached(https_home_url)
    discovery['https_home'] = {
        'status': https_home_resp.get('status'),
        'final_url': https_home_resp.get('final_url'),
        'error': https_home_resp.get('error'),
    }

    crawl_base = https_base
    if https_home_resp.get('status') != 200:
        http_home_url = http_base + '/'
        http_home_resp = fetch_cached(http_home_url)
        discovery['http_home'] = {
            'status': http_home_resp.get('status'),
            'final_url': http_home_resp.get('final_url'),
            'error': http_home_resp.get('error'),
        }
        if http_home_resp.get('status') == 200:
            crawl_base = http_base
            discovery['crawl_base'] = crawl_base
            discovery['crawl_transport'] = 'http-fallback'

    # If HTTPS home opened but was downgraded to HTTP, use HTTP for content crawl.
    if (
        https_home_resp.get('status') == 200
        and str(https_home_resp.get('final_url', '')).startswith('http://')
    ):
        crawl_base = http_base
        discovery['crawl_base'] = crawl_base
        discovery['crawl_transport'] = 'http-after-https-downgrade'

    home_url = crawl_base + '/'
    home_resp = fetch_cached(home_url)
    home_html = home_resp.get('html', '') if home_resp.get('status') == 200 else ''

    # 2) discover from sitemap
    sitemap = fetch_cached(crawl_base + '/sitemap.xml')
    sitemap_urls_all = parse_sitemap(sitemap.get('html', '')) if sitemap.get('status') == 200 else []
    sitemap_urls = trim_sitemap_urls(sitemap_urls_all, MAX_SITEMAP_URLS)
    discovery['sitemap_total_urls'] = len(sitemap_urls_all)
    discovery['sitemap_used_urls'] = len(sitemap_urls)

    contact_urls = set()
    booking_urls = set()
    legal_urls = set()
    source_map = {}

    for u in sitemap_urls:
        if is_contact_hint(u):
            contact_urls.add(u)
            source_map[u] = 'sitemap'
        if is_booking_hint(u):
            booking_urls.add(u)
            source_map[u] = 'sitemap'
        if is_legal_hint(u):
            legal_urls.add(u)
            source_map[u] = source_map.get(u, 'sitemap-legal')

    # 3) discover from home navigation/internal links
    if home_html:
        for link in extract_internal_links(crawl_base, home_html):
            combined = (link['url'] + ' ' + link['text']).lower()
            if is_contact_hint(combined):
                contact_urls.add(link['url'])
                source_map[link['url']] = source_map.get(link['url'], 'navigation')
            if is_booking_hint(combined):
                booking_urls.add(link['url'])
                source_map[link['url']] = source_map.get(link['url'], 'navigation')
            if is_legal_hint(combined):
                legal_urls.add(link['url'])
                source_map[link['url']] = source_map.get(link['url'], 'navigation-legal')

    # 3b) discover policy URLs from escaped JS/template markup (e.g. \"\/policy\")
    if home_html:
        for u in extract_policy_hint_urls(crawl_base, home_html):
            legal_urls.add(u)
            source_map[u] = source_map.get(u, 'policy-hint')

    # 3c) if still nothing legal found, try common policy paths.
    if not legal_urls:
        for pth in POLICY_FALLBACK_PATHS:
            u = crawl_base + pth
            legal_urls.add(u)
            source_map[u] = source_map.get(u, 'policy-fallback')

    # 4) collect form pages from sitemap and detect booking candidates by content
    form_pages = set()
    booking_candidates = set()

    if home_html:
        home_forms = extract_forms(home_html)
        lead_home_forms = [fh for fh in home_forms if has_lead_form_fields(fh)]
        if lead_home_forms and (has_booking_url_signal(home_url) or any(has_booking_form_signal(fh) for fh in lead_home_forms)):
            booking_candidates.add(home_url)
            source_map[home_url] = source_map.get(home_url, 'home-booking-candidate')

    for u in sitemap_urls:
        p = fetch_cached(u)
        if p['status'] != 200:
            continue
        page_html = p.get('html', '')
        page_forms = extract_forms(page_html)
        if not page_forms:
            continue

        form_pages.add(u)
        source_map[u] = source_map.get(u, 'sitemap-form')

        lead_forms = [fh for fh in page_forms if has_lead_form_fields(fh)]
        if lead_forms:
            if is_booking_hint(u) or has_booking_url_signal(u) or any(has_booking_form_signal(fh) for fh in lead_forms):
                booking_candidates.add(u)
                source_map[u] = source_map.get(u, 'sitemap-booking-candidate')

    # 5) limited fallback only if nothing found
    if not contact_urls:
        discovery['fallback_used']['contact'] = True
        for pth in CONTACT_FALLBACK_PATHS:
            u = crawl_base + pth
            contact_urls.add(u)
            source_map[u] = 'fallback'

    if not booking_urls:
        if booking_candidates:
            for u in sorted(booking_candidates):
                booking_urls.add(u)
                source_map[u] = source_map.get(u, 'booking-candidate')
        else:
            discovery['fallback_used']['booking'] = True
            for pth in BOOKING_FALLBACK_PATHS:
                u = crawl_base + pth
                booking_urls.add(u)
                source_map[u] = 'fallback'

    urls = []
    seen = set()
    for u in [home_url] + sorted(contact_urls) + sorted(booking_urls) + sorted(legal_urls) + sorted(form_pages):
        if u not in seen:
            seen.add(u)
            urls.append(u)

    pages = [fetch_cached(u) for u in urls]

    forms = []
    privacy_links = []
    forbidden_hits = []
    emails = set()
    cert_errors = []

    # HTTPS probes for certificate/security availability.
    def to_https(u: str):
        p = urlparse(u)
        return f"https://{host}{p.path or '/'}" + (f"?{p.query}" if p.query else "")

    https_probe_urls = []
    seen_probe = set()
    for u in urls:
        hu = to_https(u)
        if hu not in seen_probe:
            seen_probe.add(hu)
            https_probe_urls.append(hu)

    for hu in https_probe_urls:
        hp = fetch_cached(hu)
        if hp.get('status') is None and hp.get('error'):
            cert_errors.append({'url': hu, 'error': hp.get('error')})
            continue
        final_u = str(hp.get('final_url') or '')
        if final_u.startswith('http://'):
            cert_errors.append({'url': hu, 'error': f'HTTPS redirected to HTTP ({final_u})'})

    for p in pages:
        url = p['url']
        status = p['status']
        html = p.get('html', '')

        if status != 200 or not html:
            continue

        if has_privacy_hint(url):
            privacy_links.append({'page': url, 'href': url, 'text': 'policy path'})

        for em in re.findall(r'(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b', html):
            emails.add(em)

        for i, form_html in enumerate(extract_forms(html), 1):
            # Ignore technical/search forms; checkbox requirement applies to lead forms.
            if not has_lead_form_fields(form_html):
                continue

            opentag = form_open_tag(form_html)
            aval, aq = get_attr(opentag, 'action')
            if aval is None:
                action = 'action отсутствует'
            else:
                q = '"' if aq == 'double' else "'" if aq == 'single' else ''
                action = f'action={q}{aval}{q}'

            idv, _ = get_attr(opentag, 'id')
            nm, _ = get_attr(opentag, 'name')
            dfn, _ = get_attr(opentag, 'data-formname')
            cls, _ = get_attr(opentag, 'class')
            fid = idv or nm or dfn or cls or f'form_{i}'

            cbs = list(CHECKBOX_RE.finditer(form_html))
            has_cb = bool(cbs)
            checked = None
            checkbox_poc = None
            if has_cb:
                checked = any(re.search(r'(?is)\bchecked\b', x.group(0)) for x in cbs)
                checkbox_poc = clean(cbs[0].group(0)[:250])

            has_pol = bool(re.search(r'(?is)(политик|персональн|privacy|соглас)', form_html))
            policy_poc = None
            if has_pol:
                mpol = re.search(r'(?is).{0,90}(политик|персональн|privacy|соглас).{0,160}', form_html)
                if mpol:
                    policy_poc = clean(mpol.group(0))

            forms.append({
                'page': url,
                'form_id': fid,
                'action_display': action,
                'open_tag': clean(opentag[:360]),
                'has_checkbox': has_cb,
                'checked': checked,
                'has_policy_text': has_pol,
                'checkbox_poc': checkbox_poc,
                'policy_poc': policy_poc,
            })

        html_links = re.sub(r'(?is)<script\b.*?</script>', ' ', html)
        html_links = re.sub(r'(?is)<style\b.*?</style>', ' ', html_links)
        for am in re.finditer(r'(?is)<a\b[^>]*href\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))[^>]*>(.*?)</a>', html_links):
            href = clean_href_value(am.group(1) or am.group(2) or am.group(3) or '')
            text = strip_tags(am.group(4) or '')
            low = (href + ' ' + text).lower()
            if has_privacy_hint(low):
                privacy_links.append({'page': url, 'href': href, 'text': text[:220]})

        h2 = re.sub(r'(?is)<script\b.*?</script>', ' ', html)
        h2 = re.sub(r'(?is)<style\b.*?</style>', ' ', h2)

        for tm in re.finditer(r'(?is)>([^<]+)<', h2):
            txt = clean(tm.group(1))
            if not txt:
                continue
            for tok in FORBIDDEN_TOKENS:
                if token_found(tok, txt):
                    forbidden_hits.append({'token': tok, 'page': url, 'context': txt[:240], 'visibility': 'видно пользователю'})

        attr_patterns = [
            ('href', r'(?is)\bhref\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))'),
            ('src', r'(?is)\bsrc\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))'),
            ('alt', r'(?is)\balt\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))'),
        ]

        for aname, pat in attr_patterns:
            for am in re.finditer(pat, h2):
                v = clean(first_group(am))
                if not v:
                    continue
                for tok in FORBIDDEN_TOKENS:
                    if token_found(tok, v):
                        forbidden_hits.append({'token': tok, 'page': url, 'context': f'{aname}="{v[:220]}"', 'visibility': 'только в коде'})

    uniq_priv = []
    seen_priv = set()
    for x in privacy_links:
        k = (x['page'], x['href'], x['text'])
        if k in seen_priv:
            continue
        seen_priv.add(k)
        uniq_priv.append(x)

    uniq_forbidden = []
    seen_forbidden = set()
    for x in forbidden_hits:
        k = (x['token'].lower(), x['page'], x['context'], x['visibility'])
        if k in seen_forbidden:
            continue
        seen_forbidden.add(k)
        uniq_forbidden.append(x)

    out = {
        'domain': urlparse(base).netloc,
        'pages': [{'requested': p['url'], 'status': p['status'], 'final_url': p['final_url'], 'error': p['error']} for p in pages],
        'forms': forms,
        'emails': sorted(emails),
        'privacy_links': uniq_priv,
        'forbidden_hits': uniq_forbidden,
        'cert_errors': cert_errors,
        'discovery': {
            'contact_urls': sorted(contact_urls),
            'booking_urls': sorted(booking_urls),
            'legal_urls': sorted(legal_urls),
            'sources': {k: source_map.get(k, '') for k in sorted(set(urls))},
            'fallback_used': discovery['fallback_used'],
            'sitemap_total_urls': discovery['sitemap_total_urls'],
            'sitemap_used_urls': discovery['sitemap_used_urls'],
        },
    }
    return out


def main():
    parser = argparse.ArgumentParser(description='Run site screening audit and save JSON')
    parser.add_argument('site', help='Site URL, e.g. https://example.com')
    parser.add_argument('--out', help='Output JSON path', default=None)
    args = parser.parse_args()

    result = run_audit(args.site)

    out = args.out
    if not out:
        domain = result['domain'].replace('.', '-')
        out = f'data/audits/{domain}.audit.json'

    out_path = out if out.startswith('D:') or out.startswith('C:') else str((Path(__file__).resolve().parents[1] / out))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(out_path)
    print(f"pages={len(result['pages'])} forms={len(result['forms'])} forbidden={len(result['forbidden_hits'])} privacy={len(result['privacy_links'])}")


if __name__ == '__main__':
    main()
