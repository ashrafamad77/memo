"""
Profile-scoped headlines via Google News RSS (no API key).

Two separate searches: current city (local) and home country (roots / regional
context). Up to `per_section` items each. Parses RSS media when present; for
Google News, RSS items usually have no images, so we fetch each article URL with
a browser User-Agent and read og:image from the HTML (Google-hosted thumbnails).
"""

from __future__ import annotations

import html
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

try:
    from googlenewsdecoder import gnewsdecoder as _gnewsdecoder
except ImportError:
    _gnewsdecoder = None  # type: ignore[misc, assignment]

_log = logging.getLogger("uvicorn.error")

UA = "MemoPersonalDashboard/1.1 (personal briefing; +https://github.com/)"
# Google returns 400 / bare pages for rss/articles URLs unless the UA looks like a browser.
CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
RSS_SEARCH = "https://news.google.com/rss/search"
MEDIA_NS = "http://search.yahoo.com/mrss/"

_OG_IMAGE_PROP_FIRST = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_IMAGE_CONTENT_FIRST = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]*property=["\']og:image["\']',
    re.IGNORECASE,
)

# Google News reader pages use the same og:image for every story (product logo / generic tile).
_GOOGLE_NEWS_GENERIC_OG_MARK = "J6_coFbogxhRI9iM864NL_liGXvsQp2Aups"

_IMG_IN_HTML = re.compile(r'<img[^>]+src=["\']([^"\']+)["\']', re.IGNORECASE)


def _local_tag(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _ns_uri(tag: str) -> str:
    if tag.startswith("{") and "}" in tag:
        return tag[1:].split("}", 1)[0]
    return ""


def _child_text(el: ET.Element, name: str) -> str:
    for child in el:
        if _local_tag(child.tag) == name:
            t = (child.text or "").strip()
            return html.unescape(t)
    return ""


def _element_raw_html(el: ET.Element) -> str:
    parts: List[str] = []
    if el.text:
        parts.append(el.text)
    for child in el:
        parts.append(ET.tostring(child, encoding="unicode", method="html"))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _image_from_description_html(fragment: str) -> Optional[str]:
    if not fragment or "<img" not in fragment.lower():
        return None
    m = _IMG_IN_HTML.search(fragment)
    if not m:
        return None
    return html.unescape(m.group(1).strip())


def _walk_media_for_image(el: ET.Element) -> Tuple[Optional[str], Optional[str]]:
    """Returns (prefer_large_content_url, thumbnail_url)."""
    content_url: Optional[str] = None
    thumb_url: Optional[str] = None

    def walk(node: ET.Element) -> None:
        nonlocal content_url, thumb_url
        for child in node:
            local = _local_tag(child.tag)
            ns = _ns_uri(child.tag)
            if ns == MEDIA_NS:
                if local == "group":
                    walk(child)
                    continue
                if local == "thumbnail":
                    u = child.get("url")
                    if u:
                        thumb_url = thumb_url or u
                    continue
                if local == "content":
                    u = child.get("url")
                    if not u:
                        continue
                    typ = (child.get("type") or "").lower()
                    med = (child.get("medium") or "").lower()
                    if med == "video" or typ.startswith("video/"):
                        continue
                    if med == "image" or typ.startswith("image/") or (not med and not typ):
                        content_url = content_url or u
                    continue
            if local == "enclosure":
                typ = (child.get("type") or "").lower()
                if typ.startswith("image/"):
                    u = child.get("url")
                    if u:
                        content_url = content_url or u

    walk(el)
    return content_url, thumb_url


def _item_image_url(item: ET.Element, description_html: str) -> Optional[str]:
    content_u, thumb_u = _walk_media_for_image(item)
    if content_u:
        return content_u
    if thumb_u:
        return thumb_u
    return _image_from_description_html(description_html)


def _guess_gl_ceid(prof: Dict[str, Any]) -> tuple[str, str]:
    """Pick Google News region hints from home country / nationality text."""
    blob = f"{prof.get('home_country') or ''} {prof.get('nationality') or ''} {prof.get('current_city') or ''}".lower()
    pairs = [
        (("france", "french", "paris", "lyon", "marseille"), "FR", "FR:en"),
        (("palestine", "palestinian", "gaza", "west bank", "nablus", "ramallah"), "PS", "PS:en"),
        (("israel", "israeli", "tel aviv", "jerusalem"), "IL", "IL:en"),
        (("germany", "german", "berlin", "munich"), "DE", "DE:en"),
        (("united kingdom", "britain", "british", "england", "london", "scotland", "wales"), "GB", "GB:en"),
        (("united states", "u.s.", "usa", "american", "new york"), "US", "US:en"),
        (("canada", "canadian", "toronto", "montreal"), "CA", "CA:en"),
        (("spain", "spanish", "madrid", "barcelona"), "ES", "ES:en"),
        (("italy", "italian", "rome", "milan"), "IT", "IT:en"),
        (("belgium", "belgian", "brussels"), "BE", "BE:en"),
        (("netherlands", "dutch", "amsterdam"), "NL", "NL:en"),
        (("morocco", "moroccan", "casablanca"), "MA", "MA:en"),
        (("algeria", "algerian"), "DZ", "DZ:en"),
        (("tunisia", "tunisian"), "TN", "TN:en"),
        (("egypt", "egyptian", "cairo"), "EG", "EG:en"),
        (("jordan", "jordanian", "amman"), "JO", "JO:en"),
        (("lebanon", "lebanese", "beirut"), "LB", "LB:en"),
        (("iran", "iranian", "tehran"), "IR", "IR:en"),
    ]
    for needles, gl, ceid in pairs:
        if any(n in blob for n in needles):
            return gl, ceid
    return "US", "US:en"


def _home_country_label(prof: Dict[str, Any]) -> str:
    hc = (prof.get("home_country") or "").strip()
    if hc:
        return hc
    return (prof.get("nationality") or "").strip()


def _home_country_search_query(label: str) -> str:
    """
    Home-country feed: one focused OR-query so titles that omit the country name
    (e.g. Gaza, regional conflict) still surface.
    """
    label = label.strip()
    if len(label) < 2:
        return ""
    low = label.casefold()
    levant = (
        "palestine",
        "palestinian",
        "gaza",
        "west bank",
        "ramallah",
        "nablus",
        "jerusalem",
    )
    if any(k in low for k in levant):
        return f'({label}) OR Gaza OR "West Bank" OR Lebanon OR "Iran Israel"'
    return label


def _city_search_query(city: str) -> str:
    city = city.strip()
    return city if len(city) >= 2 else ""


def _parse_rss(xml_bytes: bytes, limit: int) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    channel = None
    for child in root:
        if _local_tag(child.tag) == "channel":
            channel = child
            break
    if channel is None:
        return []
    out: List[Dict[str, Any]] = []
    for child in channel:
        if _local_tag(child.tag) != "item":
            continue
        title = _child_text(child, "title")
        link = _child_text(child, "link")
        pub = _child_text(child, "pubDate")
        src = _child_text(child, "source")
        desc_html = ""
        for sub in child:
            if _local_tag(sub.tag) == "description":
                desc_html = _element_raw_html(sub)
                break
        image = _item_image_url(child, desc_html)
        if not title:
            continue
        out.append(
            {
                "title": title,
                "link": link or "",
                "published": pub or None,
                "source": src or None,
                "image_url": image,
            }
        )
        if len(out) >= limit:
            break
    return out


def _og_image_from_html(page: str) -> Optional[str]:
    for rx in (_OG_IMAGE_PROP_FIRST, _OG_IMAGE_CONTENT_FIRST):
        m = rx.search(page)
        if m:
            u = html.unescape(m.group(1).strip())
            if u.startswith("http"):
                return u
    return None


def _is_google_news_generic_og(url: str) -> bool:
    return _GOOGLE_NEWS_GENERIC_OG_MARK in url


def _ensure_google_news_oc(link: str) -> str:
    if "news.google.com" not in link or "oc=" in link:
        return link
    sep = "&" if "?" in link else "?"
    return f"{link}{sep}oc=5"


def _decode_google_news_publisher_url(rss_article_link: str) -> Optional[str]:
    if _gnewsdecoder is None or "news.google.com" not in rss_article_link:
        return None
    try:
        dec = _gnewsdecoder(rss_article_link.strip(), interval=0)
    except Exception as e:
        _log.debug("news_context: Google News decode error: %s", e)
        return None
    if not dec.get("status"):
        return None
    u = dec.get("decoded_url")
    return u.strip() if isinstance(u, str) and u.startswith("http") else None


def _fetch_og_image_from_page(url: str, *, timeout: float = 4.0, max_total: int = 480_000) -> Optional[str]:
    if not url.startswith("http"):
        return None
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": CHROME_UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            buf = b""
            chunk = 40 * 1024
            while len(buf) < max_total:
                piece = resp.read(chunk)
                if not piece:
                    break
                buf += piece
                text = buf.decode("utf-8", errors="ignore")
                img = _og_image_from_html(text)
                if img:
                    return img
    except Exception as e:
        _log.debug("news_context: og:image fetch failed for %s: %s", url[:96], e)
    return None


def _resolve_news_item_thumbnail(rss_article_link: str) -> Optional[str]:
    """
    Google News RSS links do not include photos. Decode to the publisher URL when possible
    and use that page's og:image; avoid the generic tile all Google News reader pages share.
    """
    link = (rss_article_link or "").strip()
    if not link:
        return None

    pub = _decode_google_news_publisher_url(link)
    if pub:
        img = _fetch_og_image_from_page(pub)
        if img and not _is_google_news_generic_og(img):
            return img

    if "news.google.com" in link:
        img = _fetch_og_image_from_page(_ensure_google_news_oc(link))
        if img and not _is_google_news_generic_og(img):
            return img

    return None


def _enrich_items_with_google_thumbnails(items: List[Dict[str, Any]], *, max_workers: int = 4) -> None:
    """Fill image_url via publisher pages (decoded from Google News URLs)."""
    todo = [(i, it["link"]) for i, it in enumerate(items) if not it.get("image_url") and it.get("link")]
    if not todo:
        return
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_idx = {ex.submit(_resolve_news_item_thumbnail, url): idx for idx, url in todo}
        for fut in as_completed(future_to_idx):
            idx = future_to_idx[fut]
            try:
                img = fut.result()
                if img:
                    items[idx]["image_url"] = img
            except Exception:
                pass


def _fetch_rss_items(q: str, gl: str, ceid: str, limit: int) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Returns (items, error_message)."""
    params = {"q": q, "hl": "en", "gl": gl, "ceid": ceid}
    url = f"{RSS_SEARCH}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=15.0) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        _log.warning("news_context: HTTP %s from Google News RSS", e.code)
        return [], f"News feed unavailable (HTTP {e.code})."
    except urllib.error.URLError as e:
        _log.warning("news_context: %s", e)
        return [], "Could not reach the news feed. Try again later."
    except Exception as e:
        _log.warning("news_context: fetch error: %s", e)
        return [], str(e)

    text = raw.decode("utf-8", errors="replace")
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    try:
        items = _parse_rss(text.encode("utf-8"), limit)
    except ET.ParseError as e:
        _log.warning("news_context: XML parse error: %s", e)
        return [], "Could not parse news response."
    _enrich_items_with_google_thumbnails(items)
    return items, None


def _empty_section(label: str, *, skipped: bool, query_used: str = "") -> Dict[str, Any]:
    return {
        "label": label,
        "query_used": query_used,
        "items": [],
        "skipped": skipped,
        "message": None,
    }


def fetch_profile_news_split(prof: Dict[str, Any], *, per_section: int = 5) -> Dict[str, Any]:
    """
    Two RSS requests: city-only and home-country-only (max `per_section` each).

    Returns:
      ok, gl, city{label, query_used, items, skipped, message},
      home_country{...}, message?, attribution
    """
    per_section = max(1, min(int(per_section), 10))
    gl, ceid = _guess_gl_ceid(prof)

    city_label = (prof.get("current_city") or "").strip()
    city_q = _city_search_query(city_label)

    home_label = _home_country_label(prof)
    home_q = _home_country_search_query(home_label)

    attribution = (
        "Headlines via Google News RSS — results and ranking are determined by Google, not Memo. "
        "Thumbnails come from each publisher’s page (og:image) when the article link can be resolved."
    )

    if not city_q and not home_q:
        return {
            "ok": False,
            "gl": gl,
            "city": _empty_section(city_label, skipped=True),
            "home_country": _empty_section(home_label, skipped=True),
            "message": "Add current city or home country in your profile to tailor headlines.",
            "attribution": attribution,
        }

    city_sec = _empty_section(city_label, skipped=not bool(city_q), query_used=city_q)
    home_sec = _empty_section(home_label, skipped=not bool(home_q), query_used=home_q)

    if city_q:
        items, err = _fetch_rss_items(city_q, gl, ceid, per_section)
        city_sec["items"] = items
        city_sec["message"] = err or (None if items else "No headlines for this search right now.")

    if home_q:
        items, err = _fetch_rss_items(home_q, gl, ceid, per_section)
        home_sec["items"] = items
        home_sec["message"] = err or (None if items else "No headlines for this search right now.")

    return {
        "ok": True,
        "gl": gl,
        "city": city_sec,
        "home_country": home_sec,
        "message": None,
        "attribution": attribution,
    }


def fetch_profile_news(prof: Dict[str, Any], *, limit: int = 10) -> Dict[str, Any]:
    """
    Legacy single-feed shape: OR-query of city + home + nationality.
    Prefer fetch_profile_news_split for the briefing UI.
    """
    limit = max(1, min(int(limit), 20))
    seen = set()
    parts: List[str] = []
    for key in ("current_city", "home_country", "nationality"):
        raw = (prof.get(key) or "").strip()
        if len(raw) < 2:
            continue
        k = raw.casefold()
        if k in seen:
            continue
        seen.add(k)
        parts.append(raw)
    q = ""
    if len(parts) == 1:
        q = parts[0]
    elif len(parts) > 1:
        q = " OR ".join(parts)

    if not q:
        return {
            "ok": False,
            "items": [],
            "query_used": "",
            "gl": "US",
            "message": "Add current city or home country in your profile to tailor headlines.",
            "attribution": "Headlines would come from Google News RSS (search scoped to your profile).",
        }

    gl, ceid = _guess_gl_ceid(prof)
    items, err = _fetch_rss_items(q, gl, ceid, limit)
    if err:
        return {
            "ok": False,
            "items": [],
            "query_used": q,
            "gl": gl,
            "message": err,
            "attribution": "Google News",
        }
    return {
        "ok": True,
        "items": items,
        "query_used": q,
        "gl": gl,
        "message": None if items else "No headlines returned for this search right now.",
        "attribution": "Headlines via Google News RSS — results and ranking are determined by Google, not Memo.",
    }
