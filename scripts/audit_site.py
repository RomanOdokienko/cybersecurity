import argparse
import json
import re
import socket
import ssl
import shutil
import subprocess
import time
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
MAX_INTERNAL_LINK_CHECKS = 120
MAX_RESOURCE_CHECKS = 160
MAX_BROKEN_SAMPLES = 20
LIGHTHOUSE_TIMEOUT_SEC = 120

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

SECURITY_HEADERS = [
    'strict-transport-security',
    'content-security-policy',
    'x-frame-options',
    'x-content-type-options',
    'referrer-policy',
]

PRICE_URL_HINTS = [
    'price', 'prices', 'prays', 'ceny', 'tseny', 'stoim', 'cost', 'uslugi', 'tarif',
]
DOCTOR_URL_HINTS = [
    'doctor', 'doctors', 'vrach', 'vrachi', 'specialist', 'team', 'staff', 'personnel',
]
MAP_TOKENS = [
    'api-maps.yandex.ru',
    'yandex.ru/maps',
    'yandex.com/maps',
    '2gis.ru',
    'google.com/maps',
]
HOURS_TOKENS = [
    'режим работы',
    'часы работы',
    'график работы',
    'пн-пт',
    'пн–пт',
    'пн-вс',
    'пн–вс',
    'ежедневно',
    'круглосуточно',
]
REVIEW_TOKENS = [
    'отзывы',
    'отзыв',
    'review',
    'prodoctorov',
    'yandex maps',
    'яндекс карты',
    '2gis',
]
SERVICE_KEYWORDS = [
    'implant',
    'имплан',
    'all-on-4',
    'all on 4',
    'bracket',
    'брекет',
    'vinir',
    'винир',
    'koronk',
    'коронк',
    'otbel',
    'отбел',
]
SPECIALTY_KEYWORDS = [
    'стоматолог',
    'ортодонт',
    'хирург',
    'терапевт',
    'пародонтолог',
    'имплантолог',
    'ортопед',
    'эндодонтист',
    'врач',
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
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=40, context=ctx) as r:
            text = r.read().decode('utf-8', 'ignore')
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            headers = {str(k).lower(): str(v) for k, v in (r.headers.items() if r.headers else [])}
            return {
                'url': url,
                'status': int(r.getcode() or 0),
                'final_url': r.geturl(),
                'html': text,
                'error': None,
                'headers': headers,
                'elapsed_ms': elapsed_ms,
            }
    except urllib.error.HTTPError as e:
        try:
            text = e.read().decode('utf-8', 'ignore')
        except Exception:
            text = ''
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        headers = {str(k).lower(): str(v) for k, v in (e.headers.items() if e.headers else [])}
        return {
            'url': url,
            'status': int(e.code),
            'final_url': url,
            'html': text,
            'error': f'HTTPError {e.code}',
            'headers': headers,
            'elapsed_ms': elapsed_ms,
        }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            'url': url,
            'status': None,
            'final_url': url,
            'html': '',
            'error': str(e),
            'headers': {},
            'elapsed_ms': elapsed_ms,
        }


def probe_status(url: str, ctx):
    req = urllib.request.Request(
        url,
        headers={
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            'Accept': '*/*',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
            'Range': 'bytes=0-4095',
        },
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=25, context=ctx) as r:
            try:
                r.read(1024)
            except Exception:
                pass
            elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
            return {
                'url': url,
                'status': int(r.getcode() or 0),
                'final_url': r.geturl(),
                'error': None,
                'elapsed_ms': elapsed_ms,
            }
    except urllib.error.HTTPError as e:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            'url': url,
            'status': int(e.code),
            'final_url': url,
            'error': f'HTTPError {e.code}',
            'elapsed_ms': elapsed_ms,
        }
    except Exception as e:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        return {
            'url': url,
            'status': None,
            'final_url': url,
            'error': str(e),
            'elapsed_ms': elapsed_ms,
        }


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


def comparable_host(host: str):
    h = str(host or '').split(':', 1)[0].lower()
    return h[4:] if h.startswith('www.') else h


def same_site_host(host_a: str, host_b: str):
    return comparable_host(host_a) == comparable_host(host_b)


def canonical_url(url: str):
    p = urlparse(url)
    path = p.path or '/'
    return f"{p.scheme}://{p.netloc}{path}" + (f"?{p.query}" if p.query else "")


def extract_internal_hrefs_from_page(page_url: str, html: str, site_host: str):
    html = re.sub(r'(?is)<script\b.*?</script>', ' ', html or '')
    html = re.sub(r'(?is)<style\b.*?</style>', ' ', html)
    out = []
    for m in re.finditer(r'(?is)<a\b[^>]*href\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))', html):
        href = clean_href_value(m.group(1) or m.group(2) or m.group(3) or '')
        if not href or href.startswith('#') or href.startswith('javascript:') or href.startswith('mailto:') or href.startswith('tel:'):
            continue
        abs_url = urljoin(page_url, href)
        p = urlparse(abs_url)
        if p.scheme not in {'http', 'https'}:
            continue
        if not same_site_host(p.netloc, site_host):
            continue
        out.append(canonical_url(abs_url))
    return out


def extract_static_assets_from_page(page_url: str, html: str, site_host: str):
    html = html or ''
    out = []
    pat = re.compile(
        r'(?is)<(?:img|script|link|iframe|source|video|audio)\b[^>]*(?:src|href)\s*=\s*(?:"([^"]*)"|\'([^\']*)\'|([^\s>]+))'
    )
    for m in pat.finditer(html):
        href = clean_href_value(m.group(1) or m.group(2) or m.group(3) or '')
        if not href or href.startswith('data:') or href.startswith('javascript:') or href.startswith('mailto:') or href.startswith('tel:'):
            continue
        abs_url = urljoin(page_url, href)
        p = urlparse(abs_url)
        if p.scheme not in {'http', 'https'}:
            continue
        if not same_site_host(p.netloc, site_host):
            continue
        out.append(canonical_url(abs_url))
    return out


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


def cert_not_after_to_days_left(not_after: str):
    if not not_after:
        return None, None
    try:
        expires_ts = ssl.cert_time_to_seconds(not_after)
        now_ts = time.time()
        days_left = int((expires_ts - now_ts) // 86400)
        return int(expires_ts), days_left
    except Exception:
        return None, None


def cert_issuer_cn(cert: dict):
    try:
        for issuer_part in cert.get('issuer', []) or []:
            for key, value in issuer_part:
                if str(key).lower() == 'commonname':
                    return str(value)
    except Exception:
        pass
    return None


def fetch_ssl_certificate(host: str, ctx):
    out = {
        'ok': False,
        'protocol': None,
        'issuer_cn': None,
        'not_after': None,
        'days_left': None,
        'error': None,
    }
    try:
        with socket.create_connection((host, 443), timeout=12) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as tls_sock:
                cert = tls_sock.getpeercert() or {}
                not_after = cert.get('notAfter')
                _, days_left = cert_not_after_to_days_left(not_after)
                out.update({
                    'ok': True,
                    'protocol': tls_sock.version(),
                    'issuer_cn': cert_issuer_cn(cert),
                    'not_after': not_after,
                    'days_left': days_left,
                })
        return out
    except Exception as exc:
        out['error'] = str(exc)
        return out


def detect_analytics_markers(html_pages):
    kinds = set()
    for html in html_pages:
        low = (html or '').lower()
        if not low:
            continue
        if 'mc.yandex.ru' in low or 'ym(' in low or 'yandex.metrika' in low:
            kinds.add('yandex_metrika')
        if (
            'googletagmanager.com/gtag/js' in low
            or 'google-analytics.com' in low
            or "gtag('config'" in low
            or 'ga(' in low
        ):
            kinds.add('google_analytics')
        if 'vk.com/rtrg' in low:
            kinds.add('vk_retarg')
        if 'facebook.com/tr' in low or 'connect.facebook.net' in low:
            kinds.add('facebook_pixel')
    return sorted(kinds)


def detect_goal_markers(html_pages):
    markers = set()
    for html in html_pages:
        low = (html or '').lower()
        if not low:
            continue
        if 'reachgoal' in low or 'ym(' in low and 'reachgoal' in low:
            markers.add('yandex_reachgoal')
        if "gtag('event'" in low or 'ga(' in low and 'event' in low:
            markers.add('google_event')
        if 'datalayer.push' in low and 'event' in low:
            markers.add('datalayer_event')
        if 'fbq(' in low and 'track' in low:
            markers.add('facebook_track')
    return sorted(markers)


def detect_engagement_signals(html_pages):
    signals = {
        'whatsapp': False,
        'telegram': False,
        'chat_widget': False,
        'slot_booking_widget': False,
    }
    chat_tokens = [
        'jivosite', 'jivo', 'carrotquest', 'livetex', 'webim', 'tawk.to',
        'usedesk', 'chatra', 'crisp.chat', 'bitrix24', 'onlinechat',
    ]
    booking_tokens_strong = [
        'yclients',
        'medme',
        'dikidi',
        'widget-booking',
        'time-slot',
        'appointment-calendar',
    ]
    booking_phrases = [
        'выбрать время',
        'свободное время',
        'онлайн-запись',
        'онлайн запись',
        'calendar booking',
        'book appointment',
    ]
    for html in html_pages:
        low = (html or '').lower()
        if not low:
            continue
        if any(x in low for x in ['wa.me', 'whatsapp', 'api.whatsapp.com']):
            signals['whatsapp'] = True
        if any(x in low for x in ['t.me/', 'telegram.me', 'telegram']):
            signals['telegram'] = True
        if any(x in low for x in chat_tokens):
            signals['chat_widget'] = True
        if any(x in low for x in booking_tokens_strong) or any(x in low for x in booking_phrases):
            signals['slot_booking_widget'] = True
    return signals


def detect_remarketing_signals(html_pages):
    signals = {
        'vk_pixel': False,
        'meta_pixel': False,
        'google_ads_remarketing': False,
    }
    for html in html_pages:
        low = (html or '').lower()
        if not low:
            continue
        if 'vk.com/rtrg' in low or 'vk.rtrg' in low or 'vk_retargeting' in low:
            signals['vk_pixel'] = True
        if 'fbq(' in low or 'facebook.com/tr' in low or 'connect.facebook.net/en_us/fbevents' in low:
            signals['meta_pixel'] = True
        if 'googletagmanager.com/gtag/js' in low and 'aw-' in low:
            signals['google_ads_remarketing'] = True
        if 'googleadservices.com/pagead/conversion' in low:
            signals['google_ads_remarketing'] = True
    signals['found'] = signals['vk_pixel'] or signals['meta_pixel'] or signals['google_ads_remarketing']
    return signals


def run_lighthouse_mobile(url: str):
    tool = None
    lh_path = shutil.which('lighthouse')
    npx_path = shutil.which('npx') or shutil.which('npx.cmd')
    if lh_path:
        tool = [lh_path]
    elif npx_path:
        tool = [npx_path, '--yes', 'lighthouse']
    else:
        return {
            'status': 'not_available',
            'score': None,
            'lcp_seconds': None,
            'tool': None,
            'error': 'lighthouse CLI не найден (нет lighthouse и npx).',
        }

    cmd = [
        *tool,
        url,
        '--quiet',
        '--only-categories=performance',
        '--emulated-form-factor=mobile',
        '--output=json',
        '--output-path=stdout',
        '--chrome-flags=--headless --no-sandbox --disable-gpu',
    ]

    try:
        run = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=LIGHTHOUSE_TIMEOUT_SEC,
            encoding='utf-8',
            errors='ignore',
        )
    except subprocess.TimeoutExpired:
        return {
            'status': 'timeout',
            'score': None,
            'lcp_seconds': None,
            'tool': ' '.join(tool),
            'error': f'Lighthouse timeout ({LIGHTHOUSE_TIMEOUT_SEC}s).',
        }
    except Exception as exc:
        return {
            'status': 'error',
            'score': None,
            'lcp_seconds': None,
            'tool': ' '.join(tool),
            'error': str(exc),
        }

    raw = (run.stdout or '').strip()
    if not raw:
        raw = (run.stderr or '').strip()
    start = raw.find('{')
    end = raw.rfind('}')
    if start < 0 or end <= start:
        return {
            'status': 'error',
            'score': None,
            'lcp_seconds': None,
            'tool': ' '.join(tool),
            'error': (run.stderr or run.stdout or 'Lighthouse JSON output parse error.')[:500],
        }

    try:
        report = json.loads(raw[start:end + 1])
    except Exception as exc:
        return {
            'status': 'error',
            'score': None,
            'lcp_seconds': None,
            'tool': ' '.join(tool),
            'error': f'JSON decode error: {exc}',
        }

    root = report.get('lighthouseResult', report)
    perf_score = (
        root.get('categories', {})
        .get('performance', {})
        .get('score')
    )
    lcp_ms = (
        root.get('audits', {})
        .get('largest-contentful-paint', {})
        .get('numericValue')
    )
    score = int(round(float(perf_score) * 100)) if isinstance(perf_score, (int, float)) else None
    lcp_seconds = round(float(lcp_ms) / 1000.0, 3) if isinstance(lcp_ms, (int, float)) else None
    status = 'ok' if score is not None or lcp_seconds is not None else 'partial'

    return {
        'status': status,
        'score': score,
        'lcp_seconds': lcp_seconds,
        'tool': ' '.join(tool),
        'error': None if run.returncode == 0 else (run.stderr or '')[:500],
    }


def detect_schema_types(html_pages):
    found = set()
    known = [
        'medicalorganization',
        'medicalclinic',
        'dentist',
        'physician',
        'hospital',
        'localbusiness',
    ]
    for html in html_pages:
        low = (html or '').lower()
        if not low:
            continue
        if 'schema.org' not in low and 'application/ld+json' not in low:
            continue
        for t in known:
            if t in low:
                found.add(t)
        if not found and ('schema.org' in low or 'application/ld+json' in low):
            found.add('detected')
    return sorted(found)


def detect_mixed_content(https_pages):
    samples = []
    pattern = re.compile(
        r'(?is)<(img|script|iframe|link|audio|video|source)\b[^>]*(?:src|href)\s*=\s*["\'](http://[^"\']+)["\']'
    )
    for page in https_pages:
        url = str(page.get('url') or '')
        html = page.get('html', '') or ''
        if not html:
            continue
        for m in pattern.finditer(html):
            ref = clean_href_value(m.group(2) or '')
            if not ref:
                continue
            samples.append({'page': url, 'asset': ref})
            if len(samples) >= 20:
                break
        if len(samples) >= 20:
            break
    return samples


def has_any_token(value: str, tokens):
    low = str(value or '').lower()
    return any(tok in low for tok in tokens)


def normalize_phone(value: str):
    digits = re.sub(r'\D+', '', str(value or ''))
    if len(digits) < 10:
        return ''
    if len(digits) >= 11 and digits[0] in {'7', '8'}:
        return digits[-10:]
    return digits[-10:]


def extract_phones(text: str):
    out = []
    for m in re.finditer(r'(?:(?:\+7|8)[\s\-\(\)]*\d[\d\s\-\(\)]{8,}\d)', text or ''):
        p = normalize_phone(m.group(0))
        if p:
            out.append(p)
    return dedupe_keep_order(out)


def normalize_address(value: str):
    s = re.sub(r'[\s,;:]+', ' ', str(value or '').lower()).strip()
    return s


def extract_address_snippet(text: str):
    if not text:
        return ''
    patterns = [
        r'(?is)(?:г\.?\s*[а-яa-z\- ]+[, ]+)?(?:ул\.?|улица|проспект|пр-т|шоссе|наб\.?|переулок|бул\.?|бульвар)\s*[^\n<]{3,90}\d[^\n<]{0,24}',
        r'(?is)(?:адрес)\s*[:\-]?\s*[^\n<]{8,120}',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return normalize_address(clean(strip_tags(m.group(0))))
    return ''


def count_probable_person_names(text: str):
    cyr = re.findall(r'\b[А-ЯЁ][а-яё]{2,}\s+[А-ЯЁ][а-яё]{2,}(?:\s+[А-ЯЁ][а-яё]{2,})?\b', text or '')
    lat = re.findall(r'\b[A-Z][a-z]{2,}\s+[A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?\b', text or '')
    names = dedupe_keep_order(cyr + lat)
    return len(names), names[:20]


def detect_med_trust_signals(html_ok_pages, contact_urls, schema_types):
    contact_set = set(contact_urls or [])
    home_page = None
    contact_pages = []
    price_pages = []
    doctor_pages = []
    service_pages = []

    reviews_found = False
    hours_found = False
    map_found = False
    address_any = False
    has_tel_link = False
    has_mailto_link = False

    home_phones = []
    contact_phones = []
    home_addr = ''
    contact_addr = ''
    doctor_names_count = 0
    doctor_specialty_found = False

    for page in html_ok_pages:
        url = str(page.get('final_url') or page.get('url') or '')
        html = page.get('html', '') or ''
        if not url or not html:
            continue

        low_url = url.lower()
        low = html.lower()
        text = strip_tags(re.sub(r'(?is)<script\b.*?</script>|<style\b.*?</style>', ' ', html))
        text_low = text.lower()

        if not home_page and (urlparse(url).path in {'', '/'}):
            home_page = page

        if (url in contact_set) or is_contact_hint(low_url):
            contact_pages.append(url)

        if has_any_token(low_url, PRICE_URL_HINTS):
            price_pages.append(url)

        if has_any_token(low_url, DOCTOR_URL_HINTS):
            doctor_pages.append(url)

        if has_any_token(low_url, SERVICE_KEYWORDS):
            service_pages.append(url)

        if not reviews_found and has_any_token(low + ' ' + text_low, REVIEW_TOKENS):
            reviews_found = True
        if not hours_found and has_any_token(text_low, HOURS_TOKENS):
            hours_found = True
        if not map_found and has_any_token(low, MAP_TOKENS):
            map_found = True

        addr = extract_address_snippet(html)
        if addr:
            address_any = True

        if not has_tel_link and re.search(r'(?is)\bhref\s*=\s*["\']tel:[^"\']+["\']', html):
            has_tel_link = True
        if not has_mailto_link and re.search(r'(?is)\bhref\s*=\s*["\']mailto:[^"\']+["\']', html):
            has_mailto_link = True

        if urlparse(url).path in {'', '/'}:
            home_phones = extract_phones(text)
            home_addr = addr or home_addr

        if (url in contact_set) or is_contact_hint(low_url):
            if not contact_phones:
                contact_phones = extract_phones(text)
            if not contact_addr:
                contact_addr = addr

        if has_any_token(low_url, DOCTOR_URL_HINTS):
            names_count, _ = count_probable_person_names(text)
            doctor_names_count = max(doctor_names_count, names_count)
            if has_any_token(text_low, SPECIALTY_KEYWORDS):
                doctor_specialty_found = True

    if not home_page and html_ok_pages:
        home_page = html_ok_pages[0]

    phone_match = bool(set(home_phones) & set(contact_phones)) if (home_phones and contact_phones) else None
    address_match = (home_addr == contact_addr) if (home_addr and contact_addr) else None

    nap_consistent = False
    if phone_match is True:
        nap_consistent = True
    elif phone_match is None and address_match is True:
        nap_consistent = True

    schema_set = {str(x).lower() for x in (schema_types or [])}
    schema_medical = bool(schema_set & {'medicalorganization', 'medicalclinic', 'physician', 'dentist', 'hospital'})
    schema_any = bool(schema_set)

    return {
        'contact_page_exists': bool(contact_pages),
        'contact_pages': dedupe_keep_order(contact_pages),
        'price_public_found': bool(price_pages),
        'price_pages': dedupe_keep_order(price_pages),
        'doctors_page_exists': bool(doctor_pages),
        'doctor_pages': dedupe_keep_order(doctor_pages),
        'address_found': address_any,
        'map_found': map_found,
        'hours_found': hours_found,
        'reviews_found': reviews_found,
        'service_pages_count': len(dedupe_keep_order(service_pages)),
        'service_pages': dedupe_keep_order(service_pages)[:20],
        'clickable_contacts': {
            'tel': has_tel_link,
            'mailto': has_mailto_link,
        },
        'doctor_cards': {
            'names_count': doctor_names_count,
            'specialty_found': doctor_specialty_found,
            'complete': bool(doctor_names_count >= 2 and doctor_specialty_found),
        },
        'nap': {
            'home_phones': home_phones[:5],
            'contact_phones': contact_phones[:5],
            'phone_match': phone_match,
            'home_address_found': bool(home_addr),
            'contact_address_found': bool(contact_addr),
            'address_match': address_match,
            'consistent': nap_consistent,
        },
        'schema': {
            'medical': schema_medical,
            'any': schema_any,
            'types': sorted(schema_set),
        },
    }


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

    # Block 3: technical profile metrics.
    ssl_info = fetch_ssl_certificate(host, ctx)

    http_home_url = http_base + '/'
    http_home_resp = fetch_cached(http_home_url)
    http_final = str(http_home_resp.get('final_url') or '')
    http_to_https = {
        'requested': http_home_url,
        'status': http_home_resp.get('status'),
        'final_url': http_final,
        'redirected_to_https': http_final.startswith('https://'),
        'error': http_home_resp.get('error'),
    }

    canonical_host = host[4:] if host.startswith('www.') else host
    non_www_url = f'https://{canonical_host}/'
    www_url = f'https://www.{canonical_host}/'
    non_www_resp = fetch_cached(non_www_url)
    www_resp = fetch_cached(www_url)
    non_www_final_host = urlparse(str(non_www_resp.get('final_url') or non_www_url)).netloc.lower()
    www_final_host = urlparse(str(www_resp.get('final_url') or www_url)).netloc.lower()
    canonical_same = bool(non_www_final_host and non_www_final_host == www_final_host)
    canonical_www = {
        'non_www': {
            'requested': non_www_url,
            'status': non_www_resp.get('status'),
            'final_url': non_www_resp.get('final_url'),
            'error': non_www_resp.get('error'),
        },
        'www': {
            'requested': www_url,
            'status': www_resp.get('status'),
            'final_url': www_resp.get('final_url'),
            'error': www_resp.get('error'),
        },
        'same_canonical': canonical_same,
        'canonical_host': non_www_final_host if canonical_same else None,
    }

    html_ok_pages = [p for p in pages if p.get('status') == 200 and p.get('html')]
    html_texts = [p.get('html', '') for p in html_ok_pages]
    https_ok_pages = [
        p for p in html_ok_pages
        if str(p.get('final_url') or p.get('url') or '').startswith('https://')
    ]

    analytics_kinds = detect_analytics_markers(html_texts)
    analytics_goal_markers = detect_goal_markers(html_texts)
    engagement_signals = detect_engagement_signals(html_texts)
    remarketing_signals = detect_remarketing_signals(html_texts)
    schema_types = detect_schema_types(html_texts)
    mixed_samples = detect_mixed_content(https_ok_pages)
    med_trust = detect_med_trust_signals(html_ok_pages, sorted(contact_urls), schema_types)

    home_html_low = (home_html or '').lower()
    favicon_from_html = bool(re.search(r'(?is)<link\b[^>]*\brel\s*=\s*["\'][^"\']*icon[^"\']*["\']', home_html_low))
    favicon_probe = fetch_cached(https_base + '/favicon.ico')
    favicon_ok = favicon_from_html or int(favicon_probe.get('status') or 0) == 200

    header_source = https_home_resp if https_home_resp.get('status') is not None else home_resp
    header_map = {str(k).lower(): str(v) for k, v in (header_source.get('headers', {}) or {}).items()}
    present_headers = [h for h in SECURITY_HEADERS if h in header_map]
    missing_headers = [h for h in SECURITY_HEADERS if h not in header_map]

    ttfb_source = https_home_resp if https_home_resp.get('status') is not None else home_resp
    ttfb_ms = ttfb_source.get('elapsed_ms')
    ttfb_seconds = round(float(ttfb_ms) / 1000.0, 3) if isinstance(ttfb_ms, (int, float)) else None
    pagespeed_url = str(https_home_resp.get('final_url') or https_base + '/')
    if not pagespeed_url.startswith('http'):
        pagespeed_url = https_base + '/'
    pagespeed_data = run_lighthouse_mobile(pagespeed_url)

    internal_candidates = []
    for page in html_ok_pages:
        page_url = str(page.get('final_url') or page.get('url') or '')
        if not page_url:
            continue
        internal_candidates.extend(extract_internal_hrefs_from_page(page_url, page.get('html', ''), host))
    internal_candidates = dedupe_keep_order(internal_candidates)[:MAX_INTERNAL_LINK_CHECKS]

    broken_internal = []
    for u in internal_candidates:
        pr = probe_status(u, ctx)
        status = pr.get('status')
        if status is None or int(status) >= 400:
            broken_internal.append({
                'url': u,
                'status': status,
                'error': pr.get('error'),
                'final_url': pr.get('final_url'),
            })

    resource_candidates = []
    for page in html_ok_pages:
        page_url = str(page.get('final_url') or page.get('url') or '')
        if not page_url:
            continue
        resource_candidates.extend(extract_static_assets_from_page(page_url, page.get('html', ''), host))
    resource_candidates = dedupe_keep_order(resource_candidates)[:MAX_RESOURCE_CHECKS]

    broken_resources = []
    for u in resource_candidates:
        pr = probe_status(u, ctx)
        status = pr.get('status')
        if status is None or int(status) >= 400:
            broken_resources.append({
                'url': u,
                'status': status,
                'error': pr.get('error'),
                'final_url': pr.get('final_url'),
            })

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

            has_phone_field = bool(
                re.search(r'(?is)\btype\s*=\s*["\']?tel["\']?', form_html)
                or re.search(r'(?is)\bname\s*=\s*(?:"[^"]*phone[^"]*"|\'[^\']*phone[^\']*\'|[^\s>]*phone[^\s>]*)', form_html)
                or re.search(r'(?is)\bname\s*=\s*(?:"[^"]*tel[^"]*"|\'[^\']*tel[^\']*\'|[^\s>]*tel[^\s>]*)', form_html)
            )
            has_name_field = bool(
                re.search(r'(?is)\bname\s*=\s*(?:"[^"]*name[^"]*"|\'[^\']*name[^\']*\'|[^\s>]*name[^\s>]*)', form_html)
                or re.search(r'(?is)\bplaceholder\s*=\s*(?:"[^"]*имя[^"]*"|\'[^\']*имя[^\']*\'|[^\s>]*имя[^\s>]*)', form_html)
            )
            has_textarea = bool(re.search(r'(?is)<textarea\b', form_html))
            phone_required = bool(
                re.search(
                    r'(?is)<input\b[^>]*(?:type\s*=\s*["\']?tel["\']?|name\s*=\s*["\']?[^"\']*(?:phone|tel)[^"\']*["\']?)[^>]*\b(?:required|data-tilda-req\s*=\s*["\']?1["\']?)',
                    form_html,
                )
            )

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
                'has_phone_field': has_phone_field,
                'has_name_field': has_name_field,
                'has_textarea': has_textarea,
                'phone_required': phone_required,
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
        'tech': {
            'ssl': ssl_info,
            'http_to_https': http_to_https,
            'canonical_www': canonical_www,
            'ttfb': {
                'seconds': ttfb_seconds,
                'source_url': ttfb_source.get('url'),
                'status': ttfb_source.get('status'),
                'error': ttfb_source.get('error'),
            },
            'pagespeed': {
                'status': pagespeed_data.get('status'),
                'score': pagespeed_data.get('score'),
                'lcp_seconds': pagespeed_data.get('lcp_seconds'),
                'tool': pagespeed_data.get('tool'),
                'error': pagespeed_data.get('error'),
            },
            'analytics': {
                'found': bool(analytics_kinds),
                'kinds': analytics_kinds,
                'goals_found': bool(analytics_goal_markers),
                'goal_markers': analytics_goal_markers,
            },
            'engagement': engagement_signals,
            'remarketing': remarketing_signals,
            'favicon': {
                'status': favicon_probe.get('status'),
                'from_html': favicon_from_html,
                'found': favicon_ok,
            },
            'schema': {
                'found': bool(schema_types),
                'types': schema_types,
            },
            'med_trust': med_trust,
            'mixed_content': {
                'count': len(mixed_samples),
                'samples': mixed_samples,
            },
            'broken_internal_links': {
                'checked': len(internal_candidates),
                'broken': len(broken_internal),
                'samples': broken_internal[:MAX_BROKEN_SAMPLES],
            },
            'broken_static_resources': {
                'checked': len(resource_candidates),
                'broken': len(broken_resources),
                'samples': broken_resources[:MAX_BROKEN_SAMPLES],
            },
            'security_headers': {
                'checked': SECURITY_HEADERS,
                'present': present_headers,
                'missing': missing_headers,
                'values': {h: header_map.get(h) for h in SECURITY_HEADERS if h in header_map},
                'source_url': header_source.get('url'),
                'source_status': header_source.get('status'),
            },
        },
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
