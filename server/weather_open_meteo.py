"""Open-Meteo (free, no API key): geocode profile city → forecast. https://open-meteo.com"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

_log = logging.getLogger("uvicorn.error")

GEO_BASE = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

# WMO Weather interpretation codes (Open-Meteo)
def weather_code_label(code: int | None) -> str:
    if code is None:
        return "—"
    m = {
        0: "Clear",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Fog",
        48: "Fog",
        51: "Light drizzle",
        53: "Drizzle",
        55: "Dense drizzle",
        56: "Freezing drizzle",
        57: "Freezing drizzle",
        61: "Slight rain",
        63: "Rain",
        65: "Heavy rain",
        66: "Freezing rain",
        67: "Freezing rain",
        71: "Snow",
        73: "Snow",
        75: "Heavy snow",
        77: "Snow grains",
        80: "Rain showers",
        81: "Rain showers",
        82: "Violent showers",
        85: "Snow showers",
        86: "Snow showers",
        95: "Thunderstorm",
        96: "Thunderstorm & hail",
        99: "Thunderstorm & hail",
    }
    return m.get(int(code), f"Weather ({code})")


def _http_get_json(url: str, timeout: float = 15.0) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "MemoPersonalDashboard/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:500]
        raise RuntimeError(f"HTTP {e.code} from weather API: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Weather API unreachable: {e}") from e


def _result_matches_country_hint(r: dict[str, Any], h: str) -> bool:
    country = (r.get("country") or "").lower()
    cc = (r.get("country_code") or "").lower()
    admin1 = (r.get("admin1") or "").lower()
    if h == cc or h in country or country in h:
        return True
    # Palestine: datasets often use PS, "Palestine", or admin areas — not always same string as user "Palestine".
    if h in ("palestine", "ps", "state of palestine", "palestinian territory", "palestinian territories"):
        if cc == "ps" or "palestine" in country:
            return True
        if "gaza" in admin1 or "west bank" in admin1:
            return True
    return False


def _pick_geocode_result(results: list[dict[str, Any]], country_hint: str | None) -> dict[str, Any] | None:
    if not results:
        return None
    if not (country_hint or "").strip():
        return results[0]
    h = country_hint.strip().lower()
    for r in results:
        if _result_matches_country_hint(r, h):
            return r
    _log.debug(
        "weather geocode: no country match for %r, using first hit %r",
        country_hint,
        results[0].get("name"),
    )
    return results[0]


def geocode_search_parts(current_city_field: str) -> tuple[str, str | None]:
    """
    Split the profile "where I live" field into (name for Open-Meteo search, country hint).

    If the user writes e.g. ``Paris, France`` or ``Austin, TX, USA``, we search the left
    side and use the last comma-separated segment to pick among duplicate city names.

    Do **not** use profile *home_country* for this — that field is often origin /
    nationality, not the country where ``current_city`` is located (mixing them causes
    bogus mismatches, e.g. Paris vs Palestine).
    """
    raw = (current_city_field or "").strip()
    if not raw or "," not in raw:
        return raw, None
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if len(parts) < 2:
        return raw, None
    hint = parts[-1]
    body = ",".join(parts[:-1]).strip()
    if not body:
        return raw, None
    if len(hint) < 2:
        return body, None
    return body, hint


def geocode_city(city: str, country_hint: str | None) -> dict[str, Any]:
    q = urllib.parse.urlencode({"name": city.strip(), "count": 8, "language": "en"})
    url = f"{GEO_BASE}?{q}"
    data = _http_get_json(url)
    results = data.get("results") or []
    hit = _pick_geocode_result(results, country_hint)
    if not hit:
        raise RuntimeError(f"No location found for “{city}”. Try a clearer city name in your profile.")
    admin = hit.get("admin1") or ""
    country = hit.get("country") or ""
    parts = [hit.get("name"), admin, country]
    label = ", ".join(p for p in parts if p)
    return {
        "latitude": float(hit["latitude"]),
        "longitude": float(hit["longitude"]),
        "label": label,
        "timezone": (hit.get("timezone") or "auto").strip() or "auto",
    }


def fetch_forecast(lat: float, lon: float, timezone: str = "auto") -> dict[str, Any]:
    """Always prefer a value Open-Meteo accepts. Profile IANA strings often fail (e.g. Asia/Gaza)."""
    tz = (timezone or "auto").strip() or "auto"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ",".join(
            [
                "temperature_2m",
                "relative_humidity_2m",
                "apparent_temperature",
                "weather_code",
                "wind_speed_10m",
                "is_day",
            ]
        ),
        "hourly": "temperature_2m,weather_code,precipitation_probability",
        "daily": "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max",
        "timezone": tz,
        "forecast_days": 7,
    }
    q = urllib.parse.urlencode(params)
    url = f"{FORECAST_BASE}?{q}"
    try:
        return _http_get_json(url)
    except RuntimeError as e:
        err = str(e)
        if tz != "auto" and ("400" in err or "Invalid timezone" in err):
            _log.warning("weather forecast: timezone %r rejected, retrying with auto", tz)
            params["timezone"] = "auto"
            q2 = urllib.parse.urlencode(params)
            return _http_get_json(f"{FORECAST_BASE}?{q2}")
        raise


def geocode_and_forecast(
    city: str,
    country_hint: str | None = None,
    timezone_hint: str | None = None,
) -> dict[str, Any]:
    search_name, derived_hint = geocode_search_parts(city)
    hint = (country_hint or "").strip() or derived_hint or None
    loc = geocode_city(search_name, hint)
    # Never pass profile timezone to the forecast API — users type values Open-Meteo rejects (HTTP 400).
    # Use the timezone returned with the geocode hit (same DB as forecast); fall back to "auto" on error.
    geo_tz = (loc.get("timezone") or "").strip() or "auto"
    raw = fetch_forecast(loc["latitude"], loc["longitude"], geo_tz)
    forecast_tz = geo_tz

    cur = raw.get("current") or {}
    code = cur.get("weather_code")
    hourly = raw.get("hourly") or {}
    ht = hourly.get("time") or []
    t2 = hourly.get("temperature_2m") or []
    wc = hourly.get("weather_code") or []
    pr = hourly.get("precipitation_probability") or []
    hourly_out: list[dict[str, Any]] = []
    # Next ~24h, every 3 hours
    for i in range(0, min(len(ht), 24), 3):
        hourly_out.append(
            {
                "time": ht[i],
                "temp_c": round(float(t2[i]), 1) if i < len(t2) and t2[i] is not None else None,
                "weather_code": int(wc[i]) if i < len(wc) and wc[i] is not None else None,
                "label": weather_code_label(int(wc[i]) if i < len(wc) and wc[i] is not None else None),
                "precip_prob": (
                    max(0, min(100, int(round(float(pr[i])))))
                    if i < len(pr) and pr[i] is not None
                    else None
                ),
            }
        )

    daily = raw.get("daily") or {}
    d_times = daily.get("time") or []
    d_max = daily.get("temperature_2m_max") or []
    d_min = daily.get("temperature_2m_min") or []
    d_code = daily.get("weather_code") or []
    d_pr = daily.get("precipitation_probability_max") or []
    daily_out: list[dict[str, Any]] = []
    for i in range(len(d_times)):
        daily_out.append(
            {
                "date": d_times[i],
                "max_c": round(float(d_max[i]), 1) if i < len(d_max) and d_max[i] is not None else None,
                "min_c": round(float(d_min[i]), 1) if i < len(d_min) and d_min[i] is not None else None,
                "weather_code": int(d_code[i]) if i < len(d_code) and d_code[i] is not None else None,
                "label": weather_code_label(int(d_code[i]) if i < len(d_code) and d_code[i] is not None else None),
                "precip_prob_max": (
                    max(0, min(100, int(round(float(d_pr[i])))))
                    if i < len(d_pr) and d_pr[i] is not None
                    else None
                ),
            }
        )

    return {
        "query_city": city.strip(),
        "location": {
            "label": loc["label"],
            "latitude": loc["latitude"],
            "longitude": loc["longitude"],
        },
        "timezone_used": raw.get("timezone") or forecast_tz,
        "profile_timezone": (timezone_hint or "").strip() or None,
        "current": {
            "time": cur.get("time"),
            "temperature_c": round(float(cur["temperature_2m"]), 1) if cur.get("temperature_2m") is not None else None,
            "apparent_c": round(float(cur["apparent_temperature"]), 1)
            if cur.get("apparent_temperature") is not None
            else None,
            "humidity_pct": int(cur["relative_humidity_2m"])
            if cur.get("relative_humidity_2m") is not None
            else None,
            "wind_kmh": round(float(cur["wind_speed_10m"]), 1) if cur.get("wind_speed_10m") is not None else None,
            "weather_code": int(code) if code is not None else None,
            "label": weather_code_label(int(code) if code is not None else None),
            "is_day": bool(cur.get("is_day")) if cur.get("is_day") is not None else None,
        },
        "hourly_sample": hourly_out,
        "daily": daily_out,
        "attribution": "Weather data by Open-Meteo (https://open-meteo.com) — CC BY 4.0.",
    }
