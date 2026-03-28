"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";
import type { ExplorerNodeVisualGroup } from "@/lib/linkedExplorer/buildLinkedExplorerGraph";

/** Match LinkedGraphView NODE_GRADIENT — activity chips use the same semantic colors. */
const FOCUS_CHIP_GRADIENT: Record<ExplorerNodeVisualGroup, string> = {
  hub: "bg-gradient-to-br from-teal-500 to-cyan-600",
  category: "bg-gradient-to-br from-slate-600 to-zinc-800",
  person: "bg-gradient-to-br from-violet-500 to-fuchsia-600",
  place: "bg-gradient-to-br from-sky-500 to-blue-600",
  situation: "bg-gradient-to-br from-green-500 to-emerald-700",
  feeling: "bg-gradient-to-br from-amber-500 to-orange-600",
  note: "bg-gradient-to-br from-slate-500 to-zinc-600",
  day: "bg-gradient-to-br from-indigo-500 to-purple-700",
  idea: "bg-gradient-to-br from-pink-500 to-rose-600",
  group: "bg-gradient-to-br from-blue-600 to-indigo-800",
  nav: "bg-gradient-to-br from-cyan-500 to-teal-600",
  bucket: "bg-gradient-to-br from-orange-500 to-amber-600",
  system: "bg-gradient-to-br from-rose-600 to-red-800",
  generic: "bg-gradient-to-br from-zinc-500 to-neutral-700",
};

type Profile = {
  name: string;
  current_city: string;
  timezone: string;
};

type WeatherOk = {
  ok: true;
  query_city: string;
  location: { label: string; latitude?: number; longitude?: number };
  timezone_used: string;
  profile_timezone?: string | null;
  current: {
    time?: string;
    temperature_c: number | null;
    apparent_c: number | null;
    humidity_pct: number | null;
    wind_kmh: number | null;
    weather_code: number | null;
    label: string;
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

type WeatherErr = { ok: false; code: string; message: string };
type WeatherResponse = WeatherOk | WeatherErr;

type ActivityFocus = {
  window_hours: number;
  activity_count: number;
  sample_labels: string[];
};

function weatherEmoji(code: number | null, isDay: boolean): string {
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
    case 56:
    case 57:
      return "🌦️";
    case 61:
    case 63:
    case 65:
    case 80:
    case 81:
    case 82:
      return "🌧️";
    case 66:
    case 67:
      return "🧊";
    case 71:
    case 73:
    case 75:
    case 77:
    case 85:
    case 86:
      return "❄️";
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

function currentIsDay(c: WeatherOk["current"]): boolean {
  const id = c.is_day;
  if (id === true || id === 1) return true;
  if (id === false || id === 0) return false;
  if (c.time) return isDaytimeFromIso(c.time);
  return true;
}

function hourInTimeZone(date: Date, timeZone: string | undefined): number {
  if (!timeZone?.trim()) return date.getHours();
  try {
    const parts = new Intl.DateTimeFormat("en-GB", {
      timeZone: timeZone.trim(),
      hour: "numeric",
      hour12: false,
    }).formatToParts(date);
    const h = parts.find((p) => p.type === "hour")?.value;
    return h != null ? parseInt(h, 10) : date.getHours();
  } catch {
    return date.getHours();
  }
}

function greetingForHour(h: number): string {
  if (h >= 5 && h < 12) return "Good morning";
  if (h >= 12 && h < 17) return "Good afternoon";
  if (h >= 17 && h < 22) return "Good evening";
  return "Good evening";
}

function formatDateInTz(date: Date, timeZone: string | undefined) {
  const opts: Intl.DateTimeFormatOptions = {
    weekday: "long",
    month: "long",
    day: "numeric",
    year: "numeric",
  };
  try {
    if (timeZone?.trim()) {
      return new Intl.DateTimeFormat(undefined, { ...opts, timeZone: timeZone.trim() }).format(date);
    }
  } catch {
    /* fall through */
  }
  return new Intl.DateTimeFormat(undefined, opts).format(date);
}

function formatTimeInTz(date: Date, timeZone: string | undefined) {
  const opts: Intl.DateTimeFormatOptions = { hour: "numeric", minute: "2-digit", second: "2-digit" };
  try {
    if (timeZone?.trim()) {
      return new Intl.DateTimeFormat(undefined, { ...opts, timeZone: timeZone.trim() }).format(date);
    }
  } catch {
    /* fall through */
  }
  return new Intl.DateTimeFormat(undefined, opts).format(date);
}

function humanizeActivityLabel(raw: string): string {
  if (!raw || raw === "Activity") return raw || "Activity";
  const spaced = raw
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .replace(/([A-Za-z])([0-9])/g, "$1 $2")
    .replace(/[_-]+/g, " ")
    .trim();
  if (!spaced) return raw;
  return spaced.replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Keyword → emoji + graph visual group (same color family as explorer nodes).
 * Order: more specific patterns first.
 */
function inferActivityVisual(label: string): { emoji: string; visualGroup: ExplorerNodeVisualGroup } {
  const b = label.toLowerCase();

  if (/cheese|fromage|fromagerie/.test(b)) return { emoji: "🧀", visualGroup: "feeling" };
  if (/coffee|café|cafe|espresso/.test(b)) return { emoji: "☕", visualGroup: "feeling" };
  if (/tennis|football|soccer|basket|yoga|gym|swim|run|jog|sport|fitness/.test(b))
    return { emoji: "🎾", visualGroup: "situation" };
  if (/train|metro|rail|rer|tram|bus|commute|vélo|velo|subway|transport/.test(b))
    return { emoji: "🚆", visualGroup: "place" };
  if (/flight|airport|plane|fly/.test(b)) return { emoji: "✈️", visualGroup: "place" };
  if (/drive|car|parking|taxi|uber/.test(b)) return { emoji: "🚗", visualGroup: "place" };
  if (/walk|hike|stroll/.test(b)) return { emoji: "🚶", visualGroup: "situation" };
  if (/office|bureau|desk|cowork|workplace|atoffice|arrival.*office/.test(b))
    return { emoji: "🏢", visualGroup: "situation" };
  if (/home|house|domicile/.test(b)) return { emoji: "🏠", visualGroup: "place" };
  if (/lunch|dinner|breakfast|brunch|meal|eat|restaurant|dining/.test(b))
    return { emoji: "🍽️", visualGroup: "feeling" };
  if (/shop|purchase|buy|achat|store|market|grocery/.test(b)) return { emoji: "🛒", visualGroup: "idea" };
  if (/meet|rencontre|date|social|visit.*friend|catch.?up|apéro|apero/.test(b))
    return { emoji: "🤝", visualGroup: "person" };
  if (/party|celebration|birthday/.test(b)) return { emoji: "🎉", visualGroup: "person" };
  if (/email|mail|slack|message|text|whatsapp/.test(b)) return { emoji: "✉️", visualGroup: "note" };
  if (/call|phone|zoom|meet\.|visio|video/.test(b)) return { emoji: "📞", visualGroup: "note" };
  if (/read|book|study|learn|lecture|course/.test(b)) return { emoji: "📚", visualGroup: "note" };
  if (/write|journal|note|log|draft/.test(b)) return { emoji: "📝", visualGroup: "note" };
  if (/sleep|wake|nap|bed|insomnia/.test(b)) return { emoji: "😴", visualGroup: "day" };
  if (/film|movie|cinema|show|netflix/.test(b)) return { emoji: "🎬", visualGroup: "idea" };
  if (/music|concert|gig|listen/.test(b)) return { emoji: "🎵", visualGroup: "feeling" };

  return { emoji: "✨", visualGroup: "generic" };
}

/** Full forecast (hourly, 7-day, extra “now” stats) — shown when the compact weather card is expanded. */
function WeatherExpandedDetails({ weather }: { weather: WeatherOk }) {
  const c = weather.current;
  return (
    <div className="space-y-4 pt-1">
      <div className="rounded-xl border border-sky-100/90 bg-white/85 p-4 dark:border-sky-900/45 dark:bg-zinc-900/55">
        <div className="text-[10px] font-bold uppercase tracking-wider text-sky-700/90 dark:text-sky-300/90">
          Now · details
        </div>
        <div className="mt-2 grid gap-2 text-sm text-zinc-700 dark:text-zinc-300 sm:grid-cols-2">
          <div>
            Feels like{" "}
            <span className="font-semibold tabular-nums">{c.apparent_c != null ? `${c.apparent_c}°` : "—"}</span>
          </div>
          <div>
            Wind{" "}
            <span className="font-semibold tabular-nums">{c.wind_kmh != null ? `${c.wind_kmh} km/h` : "—"}</span>
          </div>
          <div>
            Humidity{" "}
            <span className="font-semibold tabular-nums">{c.humidity_pct != null ? `${c.humidity_pct}%` : "—"}</span>
          </div>
          <div className="font-mono text-[11px] text-zinc-500 dark:text-zinc-400">{formatLocalTime(c.time)}</div>
        </div>
        <div className="mt-3 text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
          From profile city “{weather.query_city}” · forecast timezone: {weather.timezone_used}
          {weather.profile_timezone ? (
            <span className="mt-0.5 block">Your profile timezone: {weather.profile_timezone}</span>
          ) : null}
        </div>
      </div>

      {weather.hourly_sample?.length ? (
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

      {weather.daily?.length ? (
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
  );
}

function ActivityFocusChip({ label }: { label: string }) {
  const { emoji, visualGroup } = inferActivityVisual(label);
  const gradient = FOCUS_CHIP_GRADIENT[visualGroup] ?? FOCUS_CHIP_GRADIENT.generic;
  const display = humanizeActivityLabel(label);

  return (
    <div
      className="inline-flex max-w-full items-center gap-2.5 rounded-2xl border border-amber-200/60 bg-white/90 py-1.5 pl-1.5 pr-3 shadow-sm dark:border-amber-900/40 dark:bg-zinc-900/70"
      title={`${label} · ${visualGroup}`}
    >
      <span
        className={[
          "flex h-9 w-9 shrink-0 items-center justify-center rounded-full text-base shadow-md ring-2 ring-white/40 dark:ring-zinc-950/60",
          gradient,
        ].join(" ")}
        aria-hidden
      >
        <span className="drop-shadow-sm">{emoji}</span>
      </span>
      <span className="min-w-0 text-sm font-medium leading-snug text-zinc-800 dark:text-zinc-100">{display}</span>
    </div>
  );
}

function focusWindowLabel(hours: number): string {
  return hours === 24 ? "24 hours" : `${hours} hours`;
}

function todayFocusSummaryLine(focus: ActivityFocus | null): string {
  if (!focus) return "Loading recent activity types…";
  const { window_hours, activity_count } = focus;
  const w = focusWindowLabel(window_hours);
  if (activity_count === 0) {
    return `No activities were linked in your graph from journal entries in the last ${w}.`;
  }
  return `Your graph shows ${activity_count} ${activity_count === 1 ? "activity" : "activities"} in the last ${w}.`;
}

export function BasicOverviewPanel({ onGoToSuggestions }: { onGoToSuggestions: () => void }) {
  const [profile, setProfile] = useState<Profile | null>(null);
  const [weather, setWeather] = useState<WeatherResponse | null>(null);
  const [focus, setFocus] = useState<ActivityFocus | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [now, setNow] = useState(() => new Date());
  const [weatherExpanded, setWeatherExpanded] = useState(false);

  const tz = profile?.timezone?.trim();

  useEffect(() => {
    const id = window.setInterval(() => setNow(new Date()), 30_000);
    return () => clearInterval(id);
  }, []);

  const loadBriefing = useCallback(async () => {
    try {
      const f = await apiGet<ActivityFocus>("/briefing/activity-focus?hours=24");
      setFocus(f);
    } catch {
      setFocus({ window_hours: 24, activity_count: 0, sample_labels: [] });
    }
  }, []);

  useEffect(() => {
    let ignore = false;
    async function run() {
      setLoading(true);
      setError("");
      try {
        const [p, w, f] = await Promise.all([
          apiGet<Profile>("/profile"),
          apiGet<WeatherResponse>("/weather"),
          apiGet<ActivityFocus>("/briefing/activity-focus?hours=24").catch(() => ({
            window_hours: 24,
            activity_count: 0,
            sample_labels: [] as string[],
          })),
        ]);
        if (ignore) return;
        setProfile(p);
        setWeather(w);
        setFocus(f);
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

  useEffect(() => {
    function onNewEntry() {
      void loadBriefing();
    }
    window.addEventListener("memo:new-entry", onNewEntry as EventListener);
    return () => window.removeEventListener("memo:new-entry", onNewEntry as EventListener);
  }, [loadBriefing]);

  const hour = useMemo(() => hourInTimeZone(now, tz), [now, tz]);
  const greeting = greetingForHour(hour);
  const displayName = (profile?.name || "there").trim() || "there";
  const focusSummary = useMemo(() => todayFocusSummaryLine(focus), [focus]);
  const focusMoreCount = focus && focus.activity_count > focus.sample_labels.length ? focus.activity_count - focus.sample_labels.length : 0;

  return (
    <div className="space-y-6">
      {loading ? (
        <div className="animate-pulse text-sm text-zinc-500">Loading your briefing…</div>
      ) : null}
      {error ? (
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
          {error}
        </div>
      ) : null}

      {!loading && !error ? (
        <>
          <header className="rounded-2xl border border-emerald-200/60 bg-gradient-to-br from-emerald-50 via-white to-sky-50/80 p-6 shadow-sm dark:border-emerald-900/40 dark:from-emerald-950/50 dark:via-zinc-950 dark:to-sky-950/30">
            <p className="text-xs font-semibold uppercase tracking-widest text-emerald-700/80 dark:text-emerald-400/90">
              Daily briefing
            </p>
            <h1 className="mt-2 text-2xl font-bold tracking-tight text-zinc-900 dark:text-zinc-50 sm:text-3xl">
              {greeting}, {displayName}
            </h1>
            <p className="mt-3 font-mono text-3xl font-semibold tabular-nums text-zinc-800 dark:text-zinc-100 sm:text-4xl">
              {formatTimeInTz(now, tz)}
            </p>
            <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">{formatDateInTz(now, tz)}</p>

            <div className="mt-5 border-t border-emerald-200/60 pt-4 dark:border-emerald-900/40">
              <p className="text-[10px] font-bold uppercase tracking-wider text-emerald-800/70 dark:text-emerald-400/80">
                Where you&apos;re based
              </p>
              <dl className="mt-2 flex flex-wrap gap-x-6 gap-y-2 text-[13px] text-zinc-700 dark:text-zinc-300">
                <div>
                  <dt className="text-[11px] font-medium text-zinc-500 dark:text-zinc-500">Current city</dt>
                  <dd className="mt-0.5 font-semibold text-zinc-900 dark:text-zinc-100">{profile?.current_city || "—"}</dd>
                </div>
                <div>
                  <dt className="text-[11px] font-medium text-zinc-500 dark:text-zinc-500">Timezone</dt>
                  <dd className="mt-0.5 font-mono text-sm text-zinc-800 dark:text-zinc-200">{profile?.timezone || "—"}</dd>
                </div>
              </dl>
            </div>
          </header>

          <div className="overflow-hidden rounded-2xl border border-sky-200/80 bg-gradient-to-b from-sky-50/90 to-white dark:border-sky-900/45 dark:from-sky-950/35 dark:to-zinc-950">
            {weather && weather.ok ? (
              <>
                <button
                  type="button"
                  onClick={() => setWeatherExpanded((v) => !v)}
                  aria-expanded={weatherExpanded}
                  className="group w-full p-5 text-left transition-colors hover:bg-sky-100/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-sky-400/80 focus-visible:ring-offset-2 dark:hover:bg-sky-950/50 dark:focus-visible:ring-sky-500/60 dark:focus-visible:ring-offset-zinc-950"
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="text-[10px] font-bold uppercase tracking-wider text-sky-700/90 dark:text-sky-300/90">
                      Weather
                    </div>
                    <span
                      className={[
                        "shrink-0 text-sky-600 transition-transform duration-200 dark:text-sky-400",
                        weatherExpanded ? "rotate-180" : "",
                      ].join(" ")}
                      aria-hidden
                    >
                      ▼
                    </span>
                  </div>
                  <div className="mt-3 flex items-center gap-4">
                    <span
                      className="select-none text-6xl leading-none drop-shadow-sm"
                      title={weather.current.label}
                      role="img"
                      aria-label={weather.current.label}
                    >
                      {weatherEmoji(weather.current.weather_code, currentIsDay(weather.current))}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="text-4xl font-bold tabular-nums text-sky-900 dark:text-sky-100">
                        {weather.current.temperature_c != null ? `${weather.current.temperature_c}°` : "—"}
                      </div>
                      <div className="text-sm font-medium text-zinc-700 dark:text-zinc-300">{weather.current.label}</div>
                      <div className="mt-1 text-[11px] text-zinc-500 dark:text-zinc-400">{weather.location.label}</div>
                    </div>
                  </div>
                  <p className="mt-3 text-[10px] font-medium text-sky-700/70 group-hover:text-sky-800 dark:text-sky-400/90 dark:group-hover:text-sky-300">
                    {weatherExpanded ? "Tap to collapse forecast" : "Tap for hourly & 7-day forecast"}
                  </p>
                </button>
                {weatherExpanded ? (
                  <div className="border-t border-sky-200/70 px-5 pb-5 pt-4 dark:border-sky-800/50">
                    <WeatherExpandedDetails weather={weather} />
                  </div>
                ) : null}
              </>
            ) : (
              <div className="p-5">
                <div className="text-[10px] font-bold uppercase tracking-wider text-sky-700/90 dark:text-sky-300/90">
                  Weather
                </div>
                <p className="mt-3 text-sm text-zinc-600 dark:text-zinc-400">
                  {weather && !weather.ok ? weather.message : "Add a current city in your profile to see weather."}
                </p>
              </div>
            )}
          </div>

          <section className="rounded-2xl border border-amber-200/70 bg-amber-50/40 p-5 dark:border-amber-900/35 dark:bg-amber-950/20">
            <h2 className="text-xs font-bold uppercase tracking-wider text-amber-900/90 dark:text-amber-200/90">
              Today&apos;s focus
            </h2>
            <p className="mt-2 text-sm leading-relaxed text-zinc-800 dark:text-zinc-200">{focusSummary}</p>

            {focus && focus.activity_count > 0 ? (
              <div className="mt-4">
                <div className="mb-2 flex flex-wrap items-center gap-2 text-[11px] font-medium text-amber-950/80 dark:text-amber-100/90">
                  <span
                    className="inline-flex h-8 min-w-[2rem] items-center justify-center rounded-full bg-gradient-to-br from-amber-400 to-orange-500 px-2.5 text-sm font-bold tabular-nums text-white shadow-md ring-2 ring-amber-200/50 dark:from-amber-500 dark:to-orange-600 dark:ring-amber-900/50"
                    aria-hidden
                  >
                    {focus.activity_count}
                  </span>
                  <span>activity types at a glance</span>
                  <span className="text-zinc-500 dark:text-zinc-500">· emoji + colors match graph semantics</span>
                </div>
                <div className="flex flex-wrap gap-2.5">
                  {focus.sample_labels.map((label) => (
                    <ActivityFocusChip key={label} label={label} />
                  ))}
                  {focusMoreCount > 0 ? (
                    <div className="inline-flex items-center gap-2 rounded-2xl border border-dashed border-amber-400/70 bg-amber-100/30 px-3 py-2 text-xs font-medium text-amber-950/90 dark:border-amber-700/60 dark:bg-amber-950/40 dark:text-amber-100/90">
                      <span
                        className="flex h-8 w-8 items-center justify-center rounded-full bg-gradient-to-br from-zinc-400 to-zinc-600 text-sm text-white shadow-inner dark:from-zinc-600 dark:to-zinc-800"
                        aria-hidden
                      >
                        +
                      </span>
                      <span>
                        {focusMoreCount} more {focusMoreCount === 1 ? "type" : "types"}
                      </span>
                    </div>
                  ) : null}
                </div>
              </div>
            ) : null}

            <p className="mt-3 text-[11px] text-zinc-500 dark:text-zinc-500">
              Distinct E7 labels from your journal entries in Neo4j (rolling window). Same color families as the Linked
              Explorer graph. No AI call.
            </p>
          </section>

          <section className="rounded-2xl border border-dashed border-zinc-300 bg-zinc-50/50 p-5 dark:border-zinc-600 dark:bg-zinc-900/30">
            <h2 className="text-xs font-bold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
              World context
            </h2>
            <p className="mt-2 text-sm text-zinc-600 dark:text-zinc-400">No news sources connected yet.</p>
          </section>

          <div>
            <button
              type="button"
              onClick={onGoToSuggestions}
              className="w-full rounded-2xl border-2 border-violet-400/80 bg-violet-600 px-4 py-3.5 text-center text-sm font-semibold text-white shadow-md transition hover:bg-violet-500 dark:border-violet-500 dark:bg-violet-600 dark:hover:bg-violet-500 sm:text-base"
            >
              Need a recommendation? Go to AI Suggestions →
            </button>
          </div>
        </>
      ) : null}
    </div>
  );
}
