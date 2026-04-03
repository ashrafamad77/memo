"use client";

import type { ReactNode } from "react";

import { KpiHelp } from "@/components/KpiHelp";

export type Insights = {
  window_days: number;
  life_pulse: {
    score: number;
    confidence?: number;
    entries_in_window?: number;
    emotion_load_negative_ratio?: number;
    open_obligations?: number;
    support_ratio?: number;
  };
  emotions_per_day: { day: string; positive: number; negative: number; neutral: number }[];
  people_impact: {
    person: string;
    positive: number;
    negative: number;
    neutral: number;
    sample_size: number;
    net_score: number;
    label: "Supportive" | "Draining" | "Mixed" | "Uncertain";
  }[];
  emerging_support: { person: string; net_score: number; signals: number }[];
  open_obligations: {
    custody_open: { transfer_key: string; transfer_name: string; object_name: string; input_time?: string }[];
    expectations_open: { assignment_key: string; assignment_name: string; input_time?: string }[];
  };
  weekly_recommendations: { title: string; why: string; action: string; confidence: string }[];
};

function confidenceLabel(c: number | undefined): { text: string; widthPct: number } {
  const v = c ?? 0;
  if (v >= 0.65) return { text: "High", widthPct: 100 };
  if (v >= 0.35) return { text: "Medium", widthPct: 55 };
  return { text: "Low", widthPct: 28 };
}

function lifePulseStory(score: number | undefined): { emoji: string; line: string } {
  if (score === undefined || Number.isNaN(score)) return { emoji: "✨", line: "Add a few journal entries to see your pulse." };
  if (score >= 78) return { emoji: "🌟", line: "Strong stretch — lots of lift in your signals." };
  if (score >= 62) return { emoji: "💚", line: "Steady and mostly positive right now." };
  if (score >= 48) return { emoji: "⚖️", line: "Mixed — ups and downs are both showing up." };
  if (score >= 35) return { emoji: "🌧️", line: "Heavier week — worth a gentle check-in with yourself." };
  return { emoji: "🫂", line: "Rough patch on the chart — be kind to yourself." };
}

function impactPresentation(label: Insights["people_impact"][number]["label"]): {
  emoji: string;
  friendly: string;
  pillClass: string;
} {
  switch (label) {
    case "Supportive":
      return {
        emoji: "🫂",
        friendly: "Mostly uplifting",
        pillClass:
          "bg-emerald-100/90 text-emerald-900 dark:bg-emerald-950/60 dark:text-emerald-100 border-emerald-200/80 dark:border-emerald-800/60",
      };
    case "Draining":
      return {
        emoji: "🪫",
        friendly: "Often heavy",
        pillClass: "bg-rose-100/90 text-rose-900 dark:bg-rose-950/50 dark:text-rose-100 border-rose-200/80 dark:border-rose-800/60",
      };
    case "Mixed":
      return {
        emoji: "🔀",
        friendly: "Mixed bag",
        pillClass:
          "bg-amber-100/90 text-amber-950 dark:bg-amber-950/40 dark:text-amber-100 border-amber-200/80 dark:border-amber-800/50",
      };
    default:
      return {
        emoji: "🌤️",
        friendly: "Still figuring it out",
        pillClass: "bg-sky-100/90 text-sky-900 dark:bg-sky-950/50 dark:text-sky-100 border-sky-200/70 dark:border-sky-800/50",
      };
  }
}

function initials(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

function LifePulseRing({ score }: { score: number | undefined }) {
  const s = score ?? 0;
  const pct = Math.max(0, Math.min(100, s)) / 100;
  const r = 42;
  const c = 2 * Math.PI * r;
  const dash = pct * c;
  return (
    <div className="relative flex h-32 w-32 shrink-0 items-center justify-center">
      <svg className="h-32 w-32 -rotate-90" viewBox="0 0 100 100" aria-hidden>
        <circle cx="50" cy="50" r={r} fill="none" className="stroke-lt-border dark:stroke-zinc-700" strokeWidth="10" />
        <circle
          cx="50"
          cy="50"
          r={r}
          fill="none"
          className="stroke-amber-700 transition-[stroke-dashoffset] duration-700 dark:stroke-violet-400"
          strokeWidth="10"
          strokeLinecap="round"
          strokeDasharray={`${dash} ${c}`}
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
        <span className="text-2xl font-bold tabular-nums text-lt-text dark:text-zinc-50">{Number.isFinite(s) ? Math.round(s) : "—"}</span>
        <span className="text-[9px] font-semibold uppercase tracking-wide text-lt-textMuted dark:text-zinc-400">pulse</span>
      </div>
    </div>
  );
}

type MiniStatProps = {
  emoji: string;
  title: string;
  subtitle: string;
  value: ReactNode;
  footer?: ReactNode;
  gradient: string;
  border: string;
  help: ReactNode;
};

function MiniStatCard({ emoji, title, subtitle, value, footer, gradient, border, help }: MiniStatProps) {
  return (
    <div
      className={[
        "flex flex-col rounded-2xl border p-4 shadow-sm",
        "bg-gradient-to-br dark:shadow-none",
        gradient,
        border,
      ].join(" ")}
    >
      <div className="flex items-start justify-between gap-2">
        <div>
          <span className="text-lg" aria-hidden>
            {emoji}
          </span>
          <div className="mt-1 flex items-center gap-1 text-xs font-bold text-lt-textSecondary dark:text-zinc-100">
            {title}
            {help}
          </div>
          <p className="mt-0.5 text-[10px] leading-snug text-lt-textMuted dark:text-zinc-400">{subtitle}</p>
        </div>
      </div>
      <div className="mt-3 text-2xl font-bold tabular-nums text-lt-text dark:text-white">{value}</div>
      {footer ? <div className="mt-1.5 text-[10px] text-lt-textMuted dark:text-zinc-400">{footer}</div> : null}
    </div>
  );
}

function EmptyHint({ emoji, children }: { emoji: string; children: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-2xl border border-dashed border-lt-borderStrong/80 bg-lt-muted/60 px-4 py-8 text-center dark:border-zinc-600 dark:bg-zinc-900/30">
      <span className="text-3xl" aria-hidden>
        {emoji}
      </span>
      <p className="max-w-sm text-sm leading-relaxed text-lt-textMuted dark:text-zinc-400">{children}</p>
    </div>
  );
}

export function InsightsPanel({
  insights,
  onSelectPerson,
}: {
  insights: Insights | null;
  onSelectPerson: (name: string) => void;
}) {
  const days = insights?.window_days ?? 30;
  const lp = insights?.life_pulse;
  const conf = confidenceLabel(lp?.confidence);
  const story = lifePulseStory(lp?.score);

  return (
    <div className="space-y-5">
      <section className="overflow-hidden rounded-2xl border border-amber-200/55 bg-gradient-to-br from-amber-50/88 via-lt-surface to-teal-50/45 p-5 dark:border-violet-900/35 dark:from-violet-950/40 dark:via-zinc-950 dark:to-sky-950/25">
        <div className="flex flex-wrap items-center gap-2 text-xs font-bold uppercase tracking-wider text-amber-900/85 dark:text-violet-200/90">
          <span aria-hidden>📊</span>
          Your patterns
        </div>
        <h2 className="mt-1 text-lg font-semibold text-lt-text dark:text-zinc-50">Insights from your journal</h2>
        <p className="mt-1 max-w-2xl text-sm leading-relaxed text-lt-textMuted dark:text-zinc-400">
          A friendly read on the last <span className="font-semibold text-lt-textSecondary dark:text-zinc-200">{days} days</span> of
          entries—mood mix, people tone, open loops, and light suggestions. Numbers are hints, not grades.
        </p>
      </section>

      <div className="grid gap-4 lg:grid-cols-12">
        <section className="rounded-2xl border border-amber-200/60 bg-gradient-to-b from-amber-50/78 to-lt-surface p-5 dark:border-violet-900/40 dark:from-violet-950/30 dark:to-zinc-950 lg:col-span-5">
          <div className="flex flex-wrap items-center gap-3">
            <LifePulseRing score={lp?.score} />
            <div className="min-w-0 flex-1">
              <div className="flex items-center gap-2">
                <span className="text-2xl" aria-hidden>
                  💓
                </span>
                <span className="text-sm font-bold text-lt-text dark:text-zinc-50">Life Pulse</span>
                <KpiHelp
                  title="Life Pulse"
                  description={
                    "Composite weekly signal (0-100).\nraw = 100 - (negative_ratio * 40) - (open_obligations_weight * 30) + (support_ratio * 30).\nFinal score is confidence-calibrated toward neutral baseline (60) when data is sparse."
                  }
                />
              </div>
              <p className="mt-2 text-sm font-medium leading-snug text-lt-textSecondary dark:text-zinc-200">
                <span aria-hidden>{story.emoji} </span>
                {story.line}
              </p>
              <div className="mt-3">
                <div className="flex items-center justify-between text-[10px] font-semibold uppercase tracking-wide text-lt-textMuted dark:text-zinc-400">
                  <span>How sure we are</span>
                  <span className="tabular-nums text-lt-textSecondary dark:text-zinc-300">{conf.text}</span>
                </div>
                <div className="mt-1.5 h-2 overflow-hidden rounded-full bg-lt-subtle dark:bg-zinc-800">
                  <div
                    className="h-full rounded-full bg-gradient-to-r from-amber-600 to-orange-600 transition-all duration-500 dark:from-violet-400 dark:to-fuchsia-400"
                    style={{ width: `${conf.widthPct}%` }}
                  />
                </div>
                <p className="mt-1 font-mono text-[10px] text-lt-textMuted dark:text-zinc-500">
                  confidence {(lp?.confidence ?? 0).toFixed(2)} · {lp?.entries_in_window ?? 0} entries in window
                </p>
              </div>
            </div>
          </div>
        </section>

        <div className="grid grid-cols-2 gap-3 lg:col-span-7">
          <MiniStatCard
            emoji="🌧️"
            title="Heavy mood share"
            subtitle="Part of your notes that lean difficult"
            value={
              <>
                {Math.round(((lp?.emotion_load_negative_ratio ?? 0) * 100))}
                <span className="text-lg font-bold">%</span>
              </>
            }
            gradient="from-sky-50/90 to-lt-surface dark:from-sky-950/25 dark:to-zinc-950"
            border="border-sky-200/60 dark:border-sky-900/40"
            help={
              <KpiHelp
                title="Negative Emotion Ratio"
                description={
                  "Computed over the selected window.\nnegative_ratio = negative_assignments / total_assignments.\nAssignments are E13 nodes tagged via P141 or P2 type mapping using fixed EN/FR lexicon."
                }
              />
            }
          />
          <MiniStatCard
            emoji="📋"
            title="Open loops"
            subtitle="Commitments still marked open"
            value={lp?.open_obligations ?? 0}
            gradient="from-amber-50/90 to-lt-surface dark:from-amber-950/20 dark:to-zinc-950"
            border="border-amber-200/60 dark:border-amber-900/35"
            help={
              <KpiHelp
                title="Open Obligations"
                description={
                  "Count of unresolved commitment-like items.\nIncludes:\n- custody transfers with return-expectation semantics and no detected return event\n- open expectation assignments."
                }
              />
            }
          />
          <MiniStatCard
            emoji="🤝"
            title="Support balance"
            subtitle="Supportive vs draining people mix"
            value={
              <>
                {Math.round(((lp?.support_ratio ?? 0) * 100))}
                <span className="text-lg font-bold">%</span>
              </>
            }
            footer="Higher ≈ more supportive signal in the graph"
            gradient="from-teal-50/90 to-lt-surface dark:from-teal-950/25 dark:to-zinc-950"
            border="border-teal-200/60 dark:border-teal-900/40"
            help={
              <KpiHelp
                title="Support Ratio"
                description={
                  "Support ratio = aggregate KPI built from counts of people labeled Supportive vs Draining."
                }
              />
            }
          />
          <MiniStatCard
            emoji="🌱"
            title="Emerging support"
            subtitle="People trending positive, early signal"
            value={insights?.emerging_support?.length ?? 0}
            footer={
              (insights?.emerging_support || [])
                .slice(0, 2)
                .map((p) => p.person)
                .join(", ") || "No one on this list yet"
            }
            gradient="from-emerald-50/90 to-lt-surface dark:from-emerald-950/25 dark:to-zinc-950"
            border="border-emerald-200/60 dark:border-emerald-900/40"
            help={
              <KpiHelp
                title="Emerging Support"
                description={
                  "Early positive signal tracker.\nShows people still labeled Uncertain (low evidence) but already trending positive.\nThis does not affect Support Ratio until enough signals exist."
                }
              />
            }
          />
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <section className="rounded-2xl border border-lt-border/80 bg-lt-surface p-5 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="flex items-center gap-2">
            <span className="text-xl" aria-hidden>
              📅
            </span>
            <div>
              <h3 className="text-sm font-bold text-lt-text dark:text-zinc-50">How your days felt</h3>
              <p className="text-[11px] text-lt-textMuted dark:text-zinc-400">Mood mix per day (from tagged feelings in entries)</p>
            </div>
          </div>
          <div className="mt-3 flex flex-wrap gap-4 text-[10px] font-medium text-lt-textMuted dark:text-zinc-400">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-emerald-500" aria-hidden />
              Uplifting
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-rose-500" aria-hidden />
              Heavy
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-2.5 rounded-full bg-zinc-400 dark:bg-zinc-500" aria-hidden />
              Neutral
            </span>
          </div>
          <div className="mt-4 space-y-3">
            {(insights?.emotions_per_day || []).map((d) => {
              const tot = Math.max(1, d.positive + d.negative + d.neutral);
              const p = Math.round((d.positive / tot) * 100);
              const n = Math.round((d.negative / tot) * 100);
              const u = Math.max(0, 100 - p - n);
              return (
                <div key={d.day}>
                  <div className="mb-1 flex items-center justify-between text-xs">
                    <span className="font-medium text-lt-textMuted dark:text-zinc-300">{d.day}</span>
                    <span className="text-[10px] text-zinc-400">
                      +{d.positive} · −{d.negative} · ={d.neutral}
                    </span>
                  </div>
                  <div className="flex h-2.5 w-full overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800">
                    <div className="h-full bg-emerald-500 transition-all duration-500" style={{ width: `${p}%` }} />
                    <div className="h-full bg-rose-500 transition-all duration-500" style={{ width: `${n}%` }} />
                    <div className="h-full bg-zinc-400 dark:bg-zinc-500" style={{ width: `${u}%` }} />
                  </div>
                </div>
              );
            })}
            {!insights?.emotions_per_day?.length ? (
              <EmptyHint emoji="📝">
                Once you log a few days with feelings tagged, you&apos;ll see a simple strip chart here—not a test, just a
                shape of the week.
              </EmptyHint>
            ) : null}
          </div>
        </section>

        <section className="rounded-2xl border border-lt-border/80 bg-lt-surface p-5 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="flex items-center gap-2">
            <span className="text-xl" aria-hidden>
              👥
            </span>
            <div className="flex flex-1 flex-wrap items-center gap-2">
              <h3 className="text-sm font-bold text-lt-text dark:text-zinc-50">People impact</h3>
              <KpiHelp
                title="People impact"
                description={
                  "People impact = per-person classification (Supportive/Draining/Mixed/Uncertain) from that person's own signals.\nLabels: Supportive = mostly positive effect. Draining = mostly negative effect. Mixed = balanced or context-dependent effect. Uncertain = not enough signals yet."
                }
              />
            </div>
          </div>
          <p className="mt-1 text-[11px] text-lt-textMuted dark:text-zinc-400">
            Tap someone for signals-by-day and evidence—same detail as before, prettier cards.
          </p>
          <div className="mt-3 space-y-2">
            {(insights?.people_impact || []).map((p) => {
              const pres = impactPresentation(p.label);
              return (
                <button
                  key={p.person}
                  type="button"
                  onClick={() => onSelectPerson(p.person)}
                  className="group flex w-full items-center gap-3 rounded-2xl border border-lt-border/90 bg-gradient-to-r from-lt-washTop/70 via-lt-muted/40 to-lt-surface px-3 py-3 text-left transition hover:border-lt-accentRing hover:from-amber-50/55 hover:shadow-md dark:border-zinc-700 dark:from-zinc-900/50 dark:to-zinc-950 dark:hover:border-violet-700/50 dark:hover:from-violet-950/30"
                >
                  <span
                    className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-amber-700 to-orange-600 text-sm font-bold text-white shadow-inner ring-2 ring-lt-surface/90 dark:from-violet-500 dark:to-fuchsia-600 dark:ring-zinc-950/40"
                    aria-hidden
                  >
                    {initials(p.person)}
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-semibold text-lt-text dark:text-zinc-50">{p.person}</span>
                      <span
                        className={[
                          "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px] font-bold",
                          pres.pillClass,
                        ].join(" ")}
                      >
                        <span aria-hidden>{pres.emoji}</span>
                        {pres.friendly}
                      </span>
                    </div>
                    <div className="mt-0.5 text-[11px] text-lt-textMuted dark:text-zinc-400">
                      {p.label} · {p.sample_size} signals ·{" "}
                      <span className="font-mono tabular-nums text-lt-textMuted dark:text-zinc-300">net {p.net_score.toFixed(2)}</span>
                    </div>
                  </div>
                  <span className="shrink-0 text-zinc-400 transition group-hover:translate-x-0.5 group-hover:text-violet-600 dark:group-hover:text-violet-300" aria-hidden>
                    →
                  </span>
                </button>
              );
            })}
            {!insights?.people_impact?.length ? (
              <EmptyHint emoji="🤝">
                When the graph links people to your entries with enough tone signals, they&apos;ll show up here. Keep journaling—no
                rush.
              </EmptyHint>
            ) : null}
          </div>
        </section>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <section className="rounded-2xl border border-lt-border/80 bg-lt-surface p-5 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="flex items-center gap-2">
            <span className="text-xl" aria-hidden>
              🧷
            </span>
            <div>
              <h3 className="text-sm font-bold text-lt-text dark:text-zinc-50">Open obligations</h3>
              <p className="text-[11px] text-lt-textMuted dark:text-zinc-400">Things still marked “not done” in the graph</p>
            </div>
          </div>
          <div className="mt-3 space-y-2">
            {(insights?.open_obligations?.custody_open || []).map((o) => (
              <div
                key={o.transfer_key}
                className="rounded-xl border border-amber-200/70 bg-gradient-to-r from-amber-50/60 to-lt-surface p-3 dark:border-amber-900/40 dark:from-amber-950/20 dark:to-zinc-950"
              >
                <div className="flex items-center gap-2 text-sm font-semibold text-lt-text dark:text-zinc-50">
                  <span aria-hidden>🔁</span>
                  {o.transfer_name}
                </div>
                <div className="mt-1 text-xs text-lt-textMuted dark:text-zinc-400">Object: {o.object_name}</div>
              </div>
            ))}
            {(insights?.open_obligations?.expectations_open || []).map((o) => (
              <div
                key={o.assignment_key}
                className="rounded-xl border border-sky-200/70 bg-gradient-to-r from-sky-50/60 to-lt-surface p-3 dark:border-sky-900/40 dark:from-sky-950/20 dark:to-zinc-950"
              >
                <div className="flex items-center gap-2 text-sm font-semibold text-lt-text dark:text-zinc-50">
                  <span aria-hidden>✉️</span>
                  {o.assignment_name}
                </div>
                <div className="mt-1 text-xs text-lt-textMuted dark:text-zinc-400">Open expectation</div>
              </div>
            ))}
            {!insights?.open_obligations?.custody_open?.length && !insights?.open_obligations?.expectations_open?.length ? (
              <EmptyHint emoji="✅">Nothing open here—nice, or not enough tracked yet. Either way, you&apos;re not behind.</EmptyHint>
            ) : null}
          </div>
        </section>

        <section className="rounded-2xl border border-lt-border/80 bg-lt-surface p-5 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="flex items-center gap-2">
            <span className="text-xl" aria-hidden>
              💡
            </span>
            <div>
              <h3 className="text-sm font-bold text-lt-text dark:text-zinc-50">Ideas for this week</h3>
              <p className="text-[11px] text-lt-textMuted dark:text-zinc-400">Light nudges from your patterns—not orders</p>
            </div>
          </div>
          <div className="mt-3 space-y-2">
            {(insights?.weekly_recommendations || []).map((r, i) => (
              <div
                key={`${r.title}-${i}`}
                className="rounded-xl border border-amber-200/60 bg-gradient-to-br from-amber-50/55 via-lt-surface to-orange-50/35 p-4 dark:border-violet-900/40 dark:from-violet-950/25 dark:via-zinc-950 dark:to-fuchsia-950/20"
              >
                <div className="flex items-start gap-2">
                  <span className="text-base" aria-hidden>
                    ✨
                  </span>
                  <div className="min-w-0 flex-1">
                    <div className="text-sm font-bold text-lt-text dark:text-zinc-50">{r.title}</div>
                    <div className="mt-1 text-xs leading-relaxed text-lt-textMuted dark:text-zinc-400">{r.why}</div>
                    <div className="mt-2 rounded-lg border border-lt-border bg-lt-surface/80 px-3 py-2 text-sm text-lt-textSecondary dark:border-violet-900/30 dark:bg-zinc-900/40 dark:text-zinc-100">
                      <span className="text-[10px] font-bold uppercase tracking-wide text-lt-accent dark:text-violet-300">Try</span>
                      <div className="mt-0.5">{r.action}</div>
                    </div>
                    <div className="mt-2 text-[10px] font-medium text-zinc-400 dark:text-zinc-500">Confidence: {r.confidence}</div>
                  </div>
                </div>
              </div>
            ))}
            {!insights?.weekly_recommendations?.length ? (
              <EmptyHint emoji="🌿">Recommendations appear when there&apos;s enough signal. A little more journaling and they&apos;ll pop in.</EmptyHint>
            ) : null}
          </div>
        </section>
      </div>
    </div>
  );
}
