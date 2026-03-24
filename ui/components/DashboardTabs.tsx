"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";
import { InboxQueue } from "@/components/InboxQueue";
import { GraphMindMap } from "@/components/GraphMindMap";
import { EntityTimeline } from "@/components/EntityTimeline";

const tabs = ["Inbox", "Timeline", "Entities", "Entity Timeline", "Graph", "Insights"] as const;
type Tab = (typeof tabs)[number];

type Insights = {
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
  open_obligations: {
    custody_open: { transfer_key: string; transfer_name: string; object_name: string; input_time?: string }[];
    expectations_open: { assignment_key: string; assignment_name: string; input_time?: string }[];
  };
  weekly_recommendations: { title: string; why: string; action: string; confidence: string }[];
};

function KpiHelp({
  title,
  description,
}: {
  title: string;
  description: string;
}) {
  return (
    <span className="group relative ml-1 inline-flex align-middle">
      <span className="inline-flex h-4 w-4 items-center justify-center rounded-full border border-zinc-300 dark:border-zinc-700 text-[10px] font-bold text-zinc-500 dark:text-zinc-400">
        i
      </span>
      <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 -translate-x-1/2 rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-2.5 text-[11px] leading-snug text-zinc-700 dark:text-zinc-200 shadow-xl group-hover:block">
        <span className="block font-semibold text-zinc-900 dark:text-zinc-100">{title}</span>
        <span className="mt-1 block whitespace-pre-wrap text-zinc-600 dark:text-zinc-300">{description}</span>
      </span>
    </span>
  );
}

function TabButton({
  label,
  active,
  onClick,
}: {
  label: Tab;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={[
        "rounded-xl px-3 py-2 text-sm font-semibold",
        active ? "bg-zinc-200 dark:bg-zinc-700 text-zinc-900 dark:text-white" : "bg-zinc-100 dark:bg-zinc-800 text-zinc-600 dark:text-zinc-300 hover:bg-zinc-200 dark:hover:bg-zinc-700",
      ].join(" ")}
    >
      {label}
    </button>
  );
}

export function DashboardTabs() {
  const [tab, setTab] = useState<Tab>("Inbox");
  const [timeline, setTimeline] = useState<
    { id: string; text: string; input_time?: string; day?: string }[]
  >([]);
  const [graphRoots, setGraphRoots] = useState<
    { type: string; name: string; ref: string; mentions?: number; last_seen?: string }[]
  >([]);
  const [entities, setEntities] = useState<
    { type: string; name: string; ref: string; mentions?: number; last_seen?: string }[]
  >([]);
  const [status, setStatus] = useState<string>("");
  const [entitiesOpenType, setEntitiesOpenType] = useState<string>("");
  const [insights, setInsights] = useState<Insights | null>(null);

  useEffect(() => {
    let ignore = false;
    async function load() {
      try {
        setStatus("");
        if (tab === "Timeline") {
          const out = await apiGet<{ items: any[] }>("/timeline?limit=30");
          if (!ignore) setTimeline(out.items || []);
        }
        if (tab === "Entities") {
          const out = await apiGet<{ items: any[] }>("/entities?limit=80");
          if (!ignore) setEntities(out.items || []);
        }
        if (tab === "Graph") {
          const out = await apiGet<{ items: any[] }>("/entities?limit=120");
          if (!ignore) setGraphRoots(out.items || []);
        }
        if (tab === "Insights") {
          const out = await apiGet<Insights>("/insights?days=30");
          if (!ignore) setInsights(out || null);
        }
      } catch (e: any) {
        if (!ignore) setStatus(e?.message || String(e));
      }
    }
    load();
    return () => {
      ignore = true;
    };
  }, [tab]);

  useEffect(() => {
    function onNewEntry() {
      if (tab !== "Timeline") return;
      apiGet<{ items: any[] }>("/timeline?limit=30")
        .then((out) => setTimeline(out.items || []))
        .catch(() => {});
    }
    window.addEventListener("memo:new-entry", onNewEntry as any);
    return () => window.removeEventListener("memo:new-entry", onNewEntry as any);
  }, [tab]);

  const content = useMemo(() => {
    switch (tab) {
      case "Inbox":
        return (
          <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Needs review</div>
            <InboxQueue />
          </div>
        );
      case "Timeline":
        return (
          <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entries</div>
            <div className="mt-3 space-y-3">
              {timeline.map((e) => (
                <div key={e.id} className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-xs font-semibold text-zinc-700 dark:text-zinc-200">
                      {e.day || "—"}
                    </div>
                    <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                  </div>
                  <div className="mt-2 text-sm text-zinc-700 dark:text-zinc-200">
                    {(e.text || "").slice(0, 240)}
                    {(e.text || "").length > 240 ? "…" : ""}
                  </div>
                </div>
              ))}
              {!timeline.length ? (
                <div className="text-sm text-zinc-500">No entries yet.</div>
              ) : null}
            </div>
          </div>
        );
      case "Entities":
        {
          const grouped = (entities || []).reduce<Record<string, typeof entities>>((acc, e) => {
            const k = e.type || "Other";
            if (!acc[k]) acc[k] = [];
            acc[k].push(e);
            return acc;
          }, {});
          const typeOrder = Object.keys(grouped).sort((a, b) => {
            const priority = [
              "Person",
              "User",
              "Event",
              "EventType",
              "Place",
              "Concept",
              "Organization",
              "Day",
              "Emotion",
            ];
            const ia = priority.indexOf(a);
            const ib = priority.indexOf(b);
            if (ia >= 0 && ib >= 0) return ia - ib;
            if (ia >= 0) return -1;
            if (ib >= 0) return 1;
            return a.localeCompare(b);
          });

        return (
          <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entities</div>
            <div className="mt-3 space-y-2">
              {typeOrder.map((type) => {
                const items = grouped[type] || [];
                const isOpen = entitiesOpenType === type;
                return (
                  <div key={type} className="overflow-hidden rounded-xl border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40">
                    <button
                      onClick={() => setEntitiesOpenType((prev) => (prev === type ? "" : type))}
                      className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left hover:bg-zinc-100 dark:hover:bg-zinc-800/40"
                    >
                      <div className="text-sm font-semibold">{type}</div>
                      <div className="text-[11px] text-zinc-500 dark:text-zinc-400">
                        {items.length} item{items.length > 1 ? "s" : ""} {isOpen ? "▲" : "▼"}
                      </div>
                    </button>
                    {isOpen ? (
                      <div className="grid gap-2 border-t border-zinc-200 dark:border-zinc-800 p-3 sm:grid-cols-2 lg:grid-cols-3">
                        {items.map((e) => (
                          <div
                            key={e.ref}
                            className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950/70 p-2.5"
                          >
                            <div className="flex items-center justify-between gap-2">
                              <div className="text-sm font-semibold">{e.name}</div>
                              <div className="text-[11px] text-zinc-500">{e.mentions ?? 0}</div>
                            </div>
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                );
              })}
              {!typeOrder.length ? (
                <div className="text-sm text-zinc-500">No entities yet.</div>
              ) : null}
            </div>
          </div>
        );
        }
      case "Graph":
        return (
          <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Graph view</div>
            <GraphMindMap initialRoots={graphRoots} />
          </div>
        );
      case "Entity Timeline":
        return (
          <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entity timeline</div>
            <EntityTimeline />
          </div>
        );
      case "Insights":
        return (
          <div className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4">
                <div className="text-xs text-zinc-500">
                  Life Pulse
                  <KpiHelp
                    title="Life Pulse"
                    description={
                      "Composite weekly signal (0-100).\nraw = 100 - (negative_ratio * 40) - (open_obligations_weight * 30) + (support_ratio * 30).\nFinal score is confidence-calibrated toward neutral baseline (60) when data is sparse."
                    }
                  />
                </div>
                <div className="mt-1 text-2xl font-bold">{insights?.life_pulse?.score ?? "—"}</div>
                <div className="mt-1 text-[11px] text-zinc-500">
                  Confidence {(insights?.life_pulse?.confidence ?? 0).toFixed(2)}
                </div>
              </div>
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4">
                <div className="text-xs text-zinc-500">
                  Negative Emotion Ratio
                  <KpiHelp
                    title="Negative Emotion Ratio"
                    description={
                      "Computed over the selected window.\nnegative_ratio = negative_assignments / total_assignments.\nAssignments are E13 nodes tagged via P141 or P2 type mapping using fixed EN/FR lexicon."
                    }
                  />
                </div>
                <div className="mt-1 text-2xl font-bold">
                  {Math.round(((insights?.life_pulse?.emotion_load_negative_ratio ?? 0) * 100))}
                  %
                </div>
              </div>
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4">
                <div className="text-xs text-zinc-500">
                  Open Obligations
                  <KpiHelp
                    title="Open Obligations"
                    description={
                      "Count of unresolved commitment-like items.\nIncludes:\n- custody transfers with return-expectation semantics and no detected return event\n- open expectation assignments."
                    }
                  />
                </div>
                <div className="mt-1 text-2xl font-bold">{insights?.life_pulse?.open_obligations ?? 0}</div>
              </div>
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4">
                <div className="text-xs text-zinc-500">
                  Support Ratio
                  <KpiHelp
                    title="Support Ratio"
                    description={
                      "support_ratio = supportive_people / (supportive_people + draining_people).\nPeople labels come from net emotional impact:\nnet = (positive - negative) / sample_size.\nThresholds: >=0.25 Supportive, <=-0.25 Draining, otherwise Mixed; low samples -> Uncertain."
                    }
                  />
                </div>
                <div className="mt-1 text-2xl font-bold">
                  {Math.round(((insights?.life_pulse?.support_ratio ?? 0) * 100))}
                  %
                </div>
                <div className="mt-1 text-[11px] text-zinc-500">
                  Entries {insights?.life_pulse?.entries_in_window ?? 0}
                </div>
              </div>
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
                <div className="text-sm font-semibold">Emotions per day</div>
                <div className="mt-3 space-y-2">
                  {(insights?.emotions_per_day || []).map((d) => {
                    const tot = Math.max(1, d.positive + d.negative + d.neutral);
                    const p = Math.round((d.positive / tot) * 100);
                    const n = Math.round((d.negative / tot) * 100);
                    const u = Math.max(0, 100 - p - n);
                    return (
                      <div key={d.day}>
                        <div className="mb-1 flex items-center justify-between text-xs">
                          <span className="text-zinc-500">{d.day}</span>
                          <span className="text-zinc-400">
                            +{d.positive} / -{d.negative} / ={d.neutral}
                          </span>
                        </div>
                        <div className="h-2 w-full overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800">
                          <div className="h-2 bg-emerald-500" style={{ width: `${p}%`, float: "left" }} />
                          <div className="h-2 bg-rose-500" style={{ width: `${n}%`, float: "left" }} />
                          <div className="h-2 bg-zinc-400" style={{ width: `${u}%`, float: "left" }} />
                        </div>
                      </div>
                    );
                  })}
                  {!insights?.emotions_per_day?.length ? (
                    <div className="text-sm text-zinc-500">No emotion timeline yet.</div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
                <div className="text-sm font-semibold">People impact</div>
                <div className="mt-3 space-y-2">
                  {(insights?.people_impact || []).map((p) => (
                    <div
                      key={p.person}
                      className="flex items-center justify-between rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 px-3 py-2"
                    >
                      <div>
                        <div className="text-sm font-semibold">{p.person}</div>
                        <div className="text-xs text-zinc-500">
                          {p.label} · sample {p.sample_size}
                        </div>
                      </div>
                      <div className="text-sm font-mono">{p.net_score.toFixed(2)}</div>
                    </div>
                  ))}
                  {!insights?.people_impact?.length ? (
                    <div className="text-sm text-zinc-500">Not enough signal yet.</div>
                  ) : null}
                </div>
              </div>
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
                <div className="text-sm font-semibold">Open obligations</div>
                <div className="mt-3 space-y-2">
                  {(insights?.open_obligations?.custody_open || []).map((o) => (
                    <div key={o.transfer_key} className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 p-3">
                      <div className="text-sm font-semibold">{o.transfer_name}</div>
                      <div className="text-xs text-zinc-500">Object: {o.object_name}</div>
                    </div>
                  ))}
                  {(insights?.open_obligations?.expectations_open || []).map((o) => (
                    <div key={o.assignment_key} className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 p-3">
                      <div className="text-sm font-semibold">{o.assignment_name}</div>
                      <div className="text-xs text-zinc-500">Expectation</div>
                    </div>
                  ))}
                  {!insights?.open_obligations?.custody_open?.length &&
                  !insights?.open_obligations?.expectations_open?.length ? (
                    <div className="text-sm text-zinc-500">No open obligations.</div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-5">
                <div className="text-sm font-semibold">Weekly recommendations</div>
                <div className="mt-3 space-y-2">
                  {(insights?.weekly_recommendations || []).map((r, i) => (
                    <div key={`${r.title}-${i}`} className="rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 dark:bg-zinc-900/40 p-3">
                      <div className="text-sm font-semibold">{r.title}</div>
                      <div className="mt-1 text-xs text-zinc-500">{r.why}</div>
                      <div className="mt-2 text-sm">{r.action}</div>
                      <div className="mt-1 text-[11px] text-zinc-500">Confidence: {r.confidence}</div>
                    </div>
                  ))}
                  {!insights?.weekly_recommendations?.length ? (
                    <div className="text-sm text-zinc-500">No recommendations yet.</div>
                  ) : null}
                </div>
              </div>
            </div>
          </div>
        );
      default:
        return null;
    }
  }, [tab, graphRoots, entities, timeline, entitiesOpenType, insights]);

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between gap-3 border-b border-zinc-200 dark:border-zinc-700 px-4 py-3">
        <div className="flex flex-wrap gap-2">
          {tabs.map((t) => (
            <TabButton key={t} label={t} active={t === tab} onClick={() => setTab(t)} />
          ))}
        </div>
        <div className="text-xs text-zinc-500 dark:text-zinc-400">dashboard</div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto p-4">
        <div className={["mx-auto", tab === "Graph" ? "max-w-none" : "max-w-5xl"].join(" ")}>
          {status ? (
            <div className="mb-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
              {status}
            </div>
          ) : null}
          {content}
        </div>
      </div>
    </div>
  );
}

