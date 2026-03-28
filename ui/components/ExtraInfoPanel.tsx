"use client";

import { useEffect, useState } from "react";

import { apiGet } from "@/lib/api";

type Profile = {
  name: string;
  current_city: string;
  home_country: string;
  nationality: string;
  timezone: string;
  work_context: string;
};

type WeatherOk = {
  ok: true;
  query_city: string;
  location: { label: string; latitude: number; longitude: number };
  timezone_used: string;
  /** Profile value (informational); forecast uses geocoder/auto timezone. */
  profile_timezone?: string | null;
  current: {
    time?: string;
    temperature_c: number | null;
    apparent_c: number | null;
    humidity_pct: number | null;
    wind_kmh: number | null;
    weather_code: number | null;
    label: string;
    /** Open-Meteo may send 0 | 1 */
    is_day?: boolean | number | null;
  };
  hourly_sample: {
    time: string;
    temp_c: number | null;
    weather_code: number | null;
    label: string;
    precip_prob: number | null;
  }[];
  daily: {
    date: string;
    max_c: number | null;
    min_c: number | null;
    weather_code: number | null;
    label: string;
    precip_prob_max: number | null;
  }[];
  attribution: string;
};

type WeatherErr = {
  ok: false;
  code: string;
  message: string;
};

type WeatherResponse = WeatherOk | WeatherErr;

/** WMO codes from Open-Meteo — emoji for quick visual read (day vs night where it matters). */
function weatherEmoji(code: number | null, isDay: boolean = true): string {
  if (code === null) return "🌡️";
  switch (code) {
    case 0:
      return isDay ? "☀️" : "🌙";
    case 1:
      return isDay ? "🌤️" : "🌙";
    case 2:
      return isDay ? "⛅" : "☁️";
    case 3:
      return "☁️";
    case 45:
    case 48:
      return "🌫️";
    case 51:
    case 53:
    case 55:
      return "🌦️";
    case 56:
    case 57:
      return "🌨️";
    case 61:
      return "🌧️";
    case 63:
      return "🌧️";
    case 65:
      return "🌧️";
    case 66:
    case 67:
      return "🧊";
    case 71:
    case 73:
    case 75:
      return "❄️";
    case 77:
      return "❄️";
    case 80:
      return "🌦️";
    case 81:
      return "🌧️";
    case 82:
      return "⛈️";
    case 85:
    case 86:
      return "🌨️";
    case 95:
    case 96:
    case 99:
      return "⛈️";
    default:
      return "🌡️";
  }
}

function isDaytimeFromIso(iso: string): boolean {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return true;
    const h = d.getHours();
    return h >= 7 && h < 19;
  } catch {
    return true;
  }
}

function currentIsDay(c: WeatherOk["current"]): boolean {
  const id = c.is_day;
  if (id === true || id === 1) return true;
  if (id === false || id === 0) return false;
  if (c.time) return isDaytimeFromIso(c.time);
  return true;
}

function formatLocalTime(iso: string | undefined) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleString(undefined, {
      weekday: "short",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

function formatHourLabel(iso: string) {
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso.slice(11, 16);
    return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
  } catch {
    return iso;
  }
}

function formatDayDate(iso: string) {
  try {
    const d = new Date(iso + (iso.length <= 10 ? "T12:00:00" : ""));
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" });
  } catch {
    return iso;
  }
}

export function ExtraInfoPanel() {
  const [profile, setProfile] = useState<Profile | null>(null);
  const [weather, setWeather] = useState<WeatherResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let ignore = false;
    async function run() {
      setLoading(true);
      setError("");
      try {
        const p = await apiGet<Profile>("/profile");
        if (ignore) return;
        setProfile(p);
        const w = await apiGet<WeatherResponse>("/weather");
        if (ignore) return;
        setWeather(w);
      } catch (e: unknown) {
        if (!ignore) setError(e instanceof Error ? e.message : String(e));
      } finally {
        if (!ignore) setLoading(false);
      }
    }
    void run();
    return () => {
      ignore = true;
    };
  }, []);

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Extra info</h2>
        <p className="mt-1 max-w-2xl text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
          Present-moment context from your profile. Later this tab can tie weather and other signals to suggestions (people,
          plans) — for now, local weather only.
        </p>
      </div>

      {loading ? (
        <div className="text-sm text-zinc-500">Loading…</div>
      ) : error ? (
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
          {error}
        </div>
      ) : null}

      {!loading && !error && profile ? (
        <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-zinc-50/80 p-4 dark:bg-zinc-900/40">
          <div className="text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">Your profile</div>
          <dl className="mt-2 grid gap-2 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-[11px] text-zinc-500">Current city</dt>
              <dd className="font-medium text-zinc-900 dark:text-zinc-100">{profile.current_city || "—"}</dd>
            </div>
            <div>
              <dt className="text-[11px] text-zinc-500">Home country</dt>
              <dd className="font-medium text-zinc-900 dark:text-zinc-100">{profile.home_country || "—"}</dd>
            </div>
            <div>
              <dt className="text-[11px] text-zinc-500">Timezone</dt>
              <dd className="font-medium text-zinc-900 dark:text-zinc-100">{profile.timezone || "—"}</dd>
            </div>
            <div>
              <dt className="text-[11px] text-zinc-500">Work context</dt>
              <dd className="text-zinc-800 dark:text-zinc-200">{profile.work_context || "—"}</dd>
            </div>
          </dl>
        </div>
      ) : null}

      {!loading && !error && weather && !weather.ok ? (
        <div className="rounded-2xl border border-amber-500/25 bg-amber-500/5 p-4 text-sm text-amber-900 dark:border-amber-500/20 dark:bg-amber-500/10 dark:text-amber-100">
          <div className="font-semibold">Weather unavailable</div>
          <p className="mt-1 text-[13px] leading-relaxed opacity-90">{weather.message}</p>
        </div>
      ) : null}

      {!loading && !error && weather && weather.ok ? (
        <div className="space-y-4">
          <div className="rounded-2xl border border-sky-200/80 bg-gradient-to-br from-sky-50 to-white p-5 shadow-sm dark:border-sky-900/50 dark:from-sky-950/40 dark:to-zinc-950">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="min-w-0 flex-1">
                <div className="text-xs font-medium uppercase tracking-wide text-sky-700/80 dark:text-sky-300/80">
                  Weather · now
                </div>
                <div className="mt-1 text-lg font-semibold text-zinc-900 dark:text-zinc-50">{weather.location.label}</div>
                <div className="text-[11px] text-zinc-500 dark:text-zinc-400">
                  From profile city “{weather.query_city}” · forecast TZ: {weather.timezone_used}
                  {weather.profile_timezone ? (
                    <span className="block opacity-80">Your profile timezone: {weather.profile_timezone}</span>
                  ) : null}
                </div>
              </div>
              <div className="flex items-center gap-3 sm:gap-5">
                <span
                  className="select-none text-6xl leading-none drop-shadow-sm sm:text-7xl"
                  title={weather.current.label}
                  role="img"
                  aria-label={weather.current.label}
                >
                  {weatherEmoji(weather.current.weather_code, currentIsDay(weather.current))}
                </span>
                <div className="text-right">
                  <div className="text-4xl font-bold tabular-nums text-sky-800 dark:text-sky-200">
                    {weather.current.temperature_c != null ? `${weather.current.temperature_c}°` : "—"}
                  </div>
                  <div className="text-sm text-zinc-600 dark:text-zinc-300">{weather.current.label}</div>
                  <div className="mt-0.5 text-[11px] text-zinc-500">
                    Feels {weather.current.apparent_c != null ? `${weather.current.apparent_c}°` : "—"} · Wind{" "}
                    {weather.current.wind_kmh != null ? `${weather.current.wind_kmh} km/h` : "—"} · Humidity{" "}
                    {weather.current.humidity_pct != null ? `${weather.current.humidity_pct}%` : "—"}
                  </div>
                  <div className="mt-1 text-[10px] text-zinc-400">{formatLocalTime(weather.current.time)}</div>
                </div>
              </div>
            </div>
          </div>

          {weather.hourly_sample.length ? (
            <div>
              <div className="mb-2 text-xs font-semibold text-zinc-600 dark:text-zinc-300">Next hours</div>
              <div className="flex gap-2 overflow-x-auto pb-1">
                {weather.hourly_sample.map((h, i) => (
                  <div
                    key={`${h.time}-${i}`}
                    className="min-w-[4.75rem] shrink-0 rounded-xl border border-zinc-200 bg-white px-2 py-2 text-center dark:border-zinc-700 dark:bg-zinc-900/60"
                  >
                    <div className="text-[10px] font-medium text-zinc-500">{formatHourLabel(h.time)}</div>
                    <div className="mt-0.5 text-2xl leading-none" title={h.label} aria-hidden>
                      {weatherEmoji(h.weather_code, isDaytimeFromIso(h.time))}
                    </div>
                    <div className="mt-1 text-sm font-bold tabular-nums text-zinc-900 dark:text-zinc-100">
                      {h.temp_c != null ? `${h.temp_c}°` : "—"}
                    </div>
                    <div className="mt-0.5 line-clamp-2 text-[9px] leading-tight text-zinc-500">{h.label}</div>
                    {h.precip_prob != null ? (
                      <div className="mt-0.5 text-[9px] text-sky-600 dark:text-sky-400">{h.precip_prob}% rain</div>
                    ) : null}
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          {weather.daily.length ? (
            <div>
              <div className="mb-2 text-xs font-semibold text-zinc-600 dark:text-zinc-300">7-day outlook</div>
              <div className="space-y-1.5">
                {weather.daily.map((d) => (
                  <div
                    key={d.date}
                    className="flex items-center justify-between gap-3 rounded-xl border border-zinc-200/90 bg-white px-3 py-2 dark:border-zinc-700 dark:bg-zinc-900/50"
                  >
                    <div className="flex min-w-0 items-center gap-2.5">
                      <span className="shrink-0 text-3xl leading-none" title={d.label} aria-hidden>
                        {weatherEmoji(d.weather_code, true)}
                      </span>
                      <div className="min-w-0">
                        <div className="text-sm font-medium text-zinc-900 dark:text-zinc-100">{formatDayDate(d.date)}</div>
                        <div className="truncate text-[11px] text-zinc-500">{d.label}</div>
                      </div>
                    </div>
                    <div className="shrink-0 text-right text-sm tabular-nums">
                      <span className="font-semibold text-zinc-900 dark:text-zinc-100">
                        {d.max_c != null ? `${d.max_c}°` : "—"}
                      </span>
                      <span className="text-zinc-400"> / </span>
                      <span className="text-zinc-600 dark:text-zinc-300">{d.min_c != null ? `${d.min_c}°` : "—"}</span>
                      {d.precip_prob_max != null ? (
                        <div className="text-[10px] text-sky-600 dark:text-sky-400">↑{d.precip_prob_max}% precip</div>
                      ) : null}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : null}

          <p className="text-[10px] leading-snug text-zinc-400 dark:text-zinc-500">
            {weather.attribution}{" "}
            <a
              href="https://open-meteo.com"
              target="_blank"
              rel="noopener noreferrer"
              className="underline decoration-zinc-400/60 hover:text-sky-600 dark:hover:text-sky-400"
            >
              open-meteo.com
            </a>
          </p>
        </div>
      ) : null}
    </div>
  );
}
