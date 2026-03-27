"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

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
  emerging_support: { person: string; net_score: number; signals: number }[];
  open_obligations: {
    custody_open: { transfer_key: string; transfer_name: string; object_name: string; input_time?: string }[];
    expectations_open: { assignment_key: string; assignment_name: string; input_time?: string }[];
  };
  weekly_recommendations: { title: string; why: string; action: string; confidence: string }[];
};

type PersonImpactDetail = {
  person: string;
  window_days: number;
  counts: { positive: number; negative: number; neutral: number; signals_total: number };
  net_score: number;
  label: "Supportive" | "Draining" | "Mixed" | "Uncertain";
  confidence: number;
  formula: string;
  signals_per_day: { day: string; positive: number; negative: number; neutral: number }[];
  evidence: {
    entry_id: string;
    input_time: string;
    day: string;
    tag: string;
    polarity: "positive" | "negative" | "neutral";
    assignment_name: string;
    event_name: string;
    event_key: string;
    text_preview: string;
  }[];
};

type ImpactEvidence = PersonImpactDetail["evidence"][number];

function PersonSignalTimeline({
  days,
}: {
  days: { day: string; positive: number; negative: number; neutral: number }[];
}) {
  if (!days.length) return null;
  const maxTot = Math.max(1, ...days.map((d) => d.positive + d.negative + d.neutral));
  return (
    <div className="mt-3">
      <div className="mb-1.5 flex flex-wrap items-center justify-between gap-2 text-[10px] text-zinc-500 dark:text-zinc-400">
        <span className="font-medium text-zinc-600 dark:text-zinc-300">Signals by day</span>
        <span className="font-mono text-zinc-400">
          {days[0]?.day?.slice(5)} → {days[days.length - 1]?.day?.slice(5)}
        </span>
      </div>
      <div className="flex gap-px overflow-x-auto rounded-xl border border-zinc-200/80 bg-zinc-100/80 p-1.5 dark:border-zinc-700/80 dark:bg-zinc-900/50">
        {days.map((d) => {
          const tot = d.positive + d.negative + d.neutral;
          const scale = tot > 0 ? Math.max(0.35, tot / maxTot) : 0.12;
          const innerH = Math.round(48 * scale);
          const pPct = tot ? (d.positive / tot) * 100 : 0;
          const nPct = tot ? (d.negative / tot) * 100 : 0;
          const uPct = tot ? (d.neutral / tot) * 100 : 0;
          return (
            <div
              key={d.day}
              title={`${d.day}\n+${d.positive}  −${d.negative}  =${d.neutral}`}
              className="flex min-w-[7px] flex-1 flex-col items-center justify-end gap-0.5"
            >
              <div
                className="flex w-full max-w-[12px] flex-col-reverse overflow-hidden rounded-md bg-zinc-200/90 shadow-inner dark:bg-zinc-800/90"
                style={{ height: `${Math.max(innerH, 6)}px` }}
              >
                {tot === 0 ? (
                  <div className="h-1 w-full rounded-sm bg-zinc-400/25 dark:bg-zinc-500/25" />
                ) : (
                  <>
                    {pPct > 0 ? (
                      <div className="min-h-[2px] w-full bg-emerald-500/90 transition-all duration-500" style={{ height: `${pPct}%` }} />
                    ) : null}
                    {nPct > 0 ? (
                      <div className="min-h-[2px] w-full bg-rose-500/90 transition-all duration-500" style={{ height: `${nPct}%` }} />
                    ) : null}
                    {uPct > 0 ? (
                      <div className="min-h-[2px] w-full bg-zinc-500/60 transition-all duration-500 dark:bg-zinc-400/50" style={{ height: `${uPct}%` }} />
                    ) : null}
                  </>
                )}
              </div>
            </div>
          );
        })}
      </div>
      <div className="mt-1.5 flex flex-wrap gap-3 text-[10px] text-zinc-500">
        <span>
          <span className="inline-block h-2 w-2 rounded-sm bg-emerald-500/90 align-middle" /> positive
        </span>
        <span>
          <span className="inline-block h-2 w-2 rounded-sm bg-rose-500/90 align-middle" /> negative
        </span>
        <span>
          <span className="inline-block h-2 w-2 rounded-sm bg-zinc-500/60 align-middle dark:bg-zinc-400/50" /> neutral
        </span>
      </div>
    </div>
  );
}

/** Full journal text — only rendered after user explicitly opens it from the activity panel. */
function LedgerJournalContent({
  data,
  loading,
  error,
}: {
  data: unknown;
  loading: boolean;
  error: string;
}) {
  if (loading) {
    return <div className="animate-pulse text-xs text-zinc-500">Loading journal…</div>;
  }
  if (error) {
    return <div className="text-xs text-rose-600 dark:text-rose-400">{error}</div>;
  }
  if (!data) return null;
  const d = data as Record<string, unknown>;
  return (
    <div className="mt-3 rounded-xl border border-zinc-200 bg-zinc-50/90 p-3 text-xs dark:border-zinc-700 dark:bg-zinc-900/40">
      <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">Step 3 · Full journal entry (E73)</div>
      <div className="mt-1 flex flex-wrap gap-2 font-mono text-[10px] text-zinc-500">
        <span>{String(d.day || "")}</span>
        <span>{String(d.input_time || "")}</span>
      </div>
      <div className="mt-2 max-h-56 overflow-y-auto whitespace-pre-wrap text-sm leading-relaxed text-zinc-800 dark:text-zinc-100">
        {String(d.text || "")}
      </div>
    </div>
  );
}

/**
 * Feeling → modelled activity (E7) → optional full journal (one place for the long text).
 */
function ImpactEventLedgerPanel({
  data,
  loading,
  error,
  activityNameFallback,
  sourceEntryId,
}: {
  data: unknown;
  loading: boolean;
  error: string;
  activityNameFallback?: string;
  sourceEntryId: string;
}) {
  const [journalVisible, setJournalVisible] = useState(false);
  const [journalData, setJournalData] = useState<unknown>(null);
  const [journalLoading, setJournalLoading] = useState(false);
  const [journalError, setJournalError] = useState("");

  useEffect(() => {
    setJournalVisible(false);
    setJournalData(null);
    setJournalError("");
    setJournalLoading(false);
  }, [data, sourceEntryId]);

  async function toggleJournal() {
    if (journalVisible) {
      setJournalVisible(false);
      return;
    }
    setJournalVisible(true);
    if (journalData || journalLoading) return;
    if (!sourceEntryId) {
      setJournalError("No entry id on this signal.");
      return;
    }
    setJournalLoading(true);
    setJournalError("");
    try {
      const d = await apiGet<Record<string, unknown>>(`/entry/${encodeURIComponent(sourceEntryId)}`);
      setJournalData(d);
    } catch (e: unknown) {
      setJournalError(e instanceof Error ? e.message : String(e));
    } finally {
      setJournalLoading(false);
    }
  }

  if (loading) {
    return <div className="animate-pulse border-t border-zinc-200 pt-3 text-xs text-zinc-500 dark:border-zinc-700">Loading activity…</div>;
  }
  if (error) {
    return <div className="border-t border-zinc-200 pt-3 text-xs text-rose-600 dark:text-rose-400">{error}</div>;
  }
  if (!data) return null;

  const d = data as Record<string, unknown>;
  const apiName = String(d.activity_name || "").trim();
  const title = apiName || String(activityNameFallback || "").trim() || "Unnamed activity";
  const eventType = String(d.event_type || "").trim();
  const timeIso = String(d.event_time_iso || "").trim();
  const timeText = String(d.event_time_text || "").trim();
  const day = String(d.day || "").trim();

  return (
    <div className="space-y-3 border-t border-zinc-200 pt-3 text-xs text-zinc-600 dark:border-zinc-700 dark:text-zinc-300">
      <div>
        <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">Step 2 · Modeled activity (E7)</div>
        <div className="mt-1 text-base font-semibold leading-snug text-zinc-900 dark:text-zinc-50">{title}</div>
        {eventType ? (
          <div className="mt-1 text-[11px] text-zinc-500">
            Type · <span className="text-zinc-700 dark:text-zinc-300">{eventType}</span>
          </div>
        ) : null}
        <div className="mt-1 flex flex-wrap gap-x-2 gap-y-0.5 font-mono text-[10px] text-zinc-500">
          {day ? <span>{day}</span> : null}
          {timeIso ? <span>{timeIso}</span> : null}
          {timeText ? <span className="font-sans italic">{timeText}</span> : null}
        </div>
      </div>

      {sourceEntryId ? (
        <div>
          <button
            type="button"
            onClick={() => void toggleJournal()}
            className={[
              "rounded-lg border px-3 py-1.5 text-[11px] font-semibold transition-colors",
              journalVisible
                ? "border-zinc-400 bg-zinc-200/80 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
                : "border-sky-300/80 bg-sky-500/10 text-sky-900 hover:bg-sky-500/15 dark:border-sky-700 dark:text-sky-200 dark:hover:bg-sky-500/20",
            ].join(" ")}
          >
            {journalVisible ? "Hide full journal note" : "View full journal note →"}
          </button>
          {journalVisible ? (
            <LedgerJournalContent data={journalData} loading={journalLoading} error={journalError} />
          ) : null}
        </div>
      ) : (
        <div className="text-[11px] text-zinc-500">No journal entry id on this signal.</div>
      )}
    </div>
  );
}

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
  const [selectedImpactPerson, setSelectedImpactPerson] = useState<string | null>(null);
  const [impactDetail, setImpactDetail] = useState<PersonImpactDetail | null>(null);
  const [impactDetailLoading, setImpactDetailLoading] = useState<boolean>(false);
  const [impactDetailError, setImpactDetailError] = useState<string>("");
  const [impactPanelEntered, setImpactPanelEntered] = useState(false);
  const [impactPanelExiting, setImpactPanelExiting] = useState(false);
  const impactDrawerCloseTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const [impactLedgerKey, setImpactLedgerKey] = useState<string | null>(null);
  const [impactLedgerData, setImpactLedgerData] = useState<unknown>(null);
  const [impactLedgerLoading, setImpactLedgerLoading] = useState(false);
  const [impactLedgerError, setImpactLedgerError] = useState("");

  useEffect(() => {
    return () => {
      if (impactDrawerCloseTimer.current) clearTimeout(impactDrawerCloseTimer.current);
    };
  }, []);

  useEffect(() => {
    setImpactLedgerKey(null);
    setImpactLedgerData(null);
    setImpactLedgerLoading(false);
    setImpactLedgerError("");
  }, [selectedImpactPerson]);

  useEffect(() => {
    if (!selectedImpactPerson) {
      setImpactPanelEntered(false);
      setImpactPanelExiting(false);
      return;
    }
    setImpactPanelExiting(false);
    setImpactPanelEntered(false);
    const t = window.setTimeout(() => setImpactPanelEntered(true), 10);
    return () => clearTimeout(t);
  }, [selectedImpactPerson]);

  const closeImpactDrawer = useCallback(() => {
    if (!selectedImpactPerson || impactPanelExiting) return;
    setImpactPanelExiting(true);
    if (impactDrawerCloseTimer.current) clearTimeout(impactDrawerCloseTimer.current);
    impactDrawerCloseTimer.current = window.setTimeout(() => {
      impactDrawerCloseTimer.current = null;
      setSelectedImpactPerson(null);
      setImpactPanelExiting(false);
      setImpactPanelEntered(false);
    }, 300);
  }, [selectedImpactPerson, impactPanelExiting]);

  useEffect(() => {
    if (!selectedImpactPerson) return;
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") closeImpactDrawer();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [selectedImpactPerson, closeImpactDrawer]);

  async function toggleImpactEventLedger(rowKey: string, ev: ImpactEvidence) {
    if (impactLedgerKey === rowKey) {
      setImpactLedgerKey(null);
      setImpactLedgerData(null);
      setImpactLedgerError("");
      setImpactLedgerLoading(false);
      return;
    }
    setImpactLedgerKey(rowKey);
    setImpactLedgerData(null);
    setImpactLedgerError("");
    setImpactLedgerLoading(true);
    try {
      if (!ev.event_key) throw new Error("No linked activity key for this signal.");
      const d = await apiGet<Record<string, unknown>>(
        `/entity/overview?ref=${encodeURIComponent(`Event:${ev.event_key}`)}`
      );
      setImpactLedgerData(d);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setImpactLedgerError(msg);
    } finally {
      setImpactLedgerLoading(false);
    }
  }

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
    let ignore = false;
    async function loadImpactDetail() {
      if (!selectedImpactPerson) {
        setImpactDetail(null);
        setImpactDetailLoading(false);
        setImpactDetailError("");
        return;
      }
      setImpactDetailLoading(true);
      setImpactDetailError("");
      try {
        const out = await apiGet<PersonImpactDetail>(
          `/insights/person?person=${encodeURIComponent(selectedImpactPerson)}&days=30&limit=40`
        );
        if (!ignore) setImpactDetail(out || null);
      } catch (e: any) {
        if (!ignore) {
          setImpactDetail(null);
          setImpactDetailError(e?.message || String(e));
        }
      } finally {
        if (!ignore) setImpactDetailLoading(false);
      }
    }
    loadImpactDetail();
    return () => {
      ignore = true;
    };
  }, [selectedImpactPerson]);

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
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
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
                      "Support ratio = aggregate KPI built from counts of people labeled Supportive vs Draining."
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
              <div className="rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 p-4">
                <div className="text-xs text-zinc-500">
                  Emerging Support
                  <KpiHelp
                    title="Emerging Support"
                    description={
                      "Early positive signal tracker.\nShows people still labeled Uncertain (low evidence) but already trending positive.\nThis does not affect Support Ratio until enough signals exist."
                    }
                  />
                </div>
                <div className="mt-1 text-2xl font-bold">{insights?.emerging_support?.length ?? 0}</div>
                <div className="mt-1 truncate text-[11px] text-zinc-500">
                  {(insights?.emerging_support || [])
                    .slice(0, 2)
                    .map((p) => p.person)
                    .join(", ") || "No emerging signal"}
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
                <div className="text-sm font-semibold">
                  People impact
                  <KpiHelp
                    title="People impact"
                    description={
                      "People impact = per-person classification (Supportive/Draining/Mixed/Uncertain) from that person's own signals.\nLabels: Supportive = mostly positive effect. Draining = mostly negative effect. Mixed = balanced or context-dependent effect. Uncertain = not enough signals yet."
                    }
                  />
                </div>
                <div className="mt-3 space-y-2">
                  {(insights?.people_impact || []).map((p) => (
                    <button
                      key={p.person}
                      onClick={() => setSelectedImpactPerson(p.person)}
                      className="flex w-full items-center justify-between rounded-lg border border-zinc-200 dark:border-zinc-800 bg-zinc-50 px-3 py-2 text-left transition hover:border-zinc-300 hover:bg-zinc-100 dark:bg-zinc-900/40 dark:hover:border-zinc-700 dark:hover:bg-zinc-800/50"
                    >
                      <div>
                        <div className="text-sm font-semibold">{p.person}</div>
                        <div className="text-xs text-zinc-500">
                          {p.label} · signals {p.sample_size}
                        </div>
                      </div>
                      <div className="text-sm font-mono">{p.net_score.toFixed(2)}</div>
                    </button>
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

      {selectedImpactPerson ? (
        <div className="fixed inset-0 z-30">
          <div
            className={[
              "absolute inset-0 bg-black/50 transition-opacity duration-300 ease-out",
              impactPanelEntered && !impactPanelExiting ? "opacity-100" : "opacity-0",
            ].join(" ")}
            onClick={closeImpactDrawer}
            aria-hidden
          />
          <div
            className={[
              "absolute inset-y-0 right-0 w-full max-w-xl overflow-y-auto rounded-l-2xl border-l border-zinc-200/90 bg-white p-5 shadow-[0_0_40px_-10px_rgba(0,0,0,0.25)] transition-transform duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] dark:border-zinc-800 dark:bg-zinc-950 dark:shadow-[0_0_50px_-12px_rgba(0,0,0,0.65)]",
              impactPanelExiting || !impactPanelEntered ? "translate-x-full" : "translate-x-0",
            ].join(" ")}
            onClick={(e) => e.stopPropagation()}
            role="dialog"
            aria-modal="true"
            aria-labelledby="impact-drawer-title"
          >
            <div className="border-b border-zinc-100 pb-4 dark:border-zinc-800/80">
              <div className="text-[11px] font-medium uppercase tracking-wide text-zinc-500">People impact explainability</div>
              <div id="impact-drawer-title" className="mt-1 text-xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50">
                {selectedImpactPerson}
              </div>
              <div className="mt-1 text-xs text-zinc-500">Click outside to close · Esc</div>
            </div>

            {impactDetailLoading ? (
              <div className="mt-6 text-sm text-zinc-500">Loading signal evidence…</div>
            ) : null}
            {impactDetailError ? (
              <div className="mt-4 rounded-xl border border-amber-500/35 bg-amber-500/10 p-3 text-sm text-amber-800 dark:text-amber-200">
                {impactDetailError}
              </div>
            ) : null}

            {impactDetail ? (
              <div className="mt-5 space-y-5">
                <div className="rounded-2xl border border-zinc-200/90 bg-gradient-to-b from-zinc-50 to-white p-4 dark:border-zinc-800 dark:from-zinc-900/50 dark:to-zinc-950">
                  <div className="text-xs font-medium text-zinc-500">How this score is computed</div>
                  <div className="mt-1 font-mono text-[13px] text-zinc-800 dark:text-zinc-200">{impactDetail.formula}</div>
                  <div className="mt-3 grid grid-cols-2 gap-x-3 gap-y-2 text-sm">
                    <div>
                      Label <span className="font-semibold text-zinc-900 dark:text-zinc-100">{impactDetail.label}</span>
                    </div>
                    <div>
                      Net <span className="font-mono font-medium">{impactDetail.net_score.toFixed(2)}</span>
                    </div>
                    <div>
                      Signals <span className="font-semibold">{impactDetail.counts.signals_total}</span>
                    </div>
                    <div>
                      Confidence <span className="font-mono">{impactDetail.confidence.toFixed(2)}</span>
                    </div>
                    <div className="text-emerald-700 dark:text-emerald-400">+ {impactDetail.counts.positive}</div>
                    <div className="text-rose-700 dark:text-rose-400">− {impactDetail.counts.negative}</div>
                    <div className="text-zinc-600 dark:text-zinc-400">= {impactDetail.counts.neutral}</div>
                    <div className="text-zinc-500">Window {impactDetail.window_days}d</div>
                  </div>
                  <PersonSignalTimeline days={impactDetail.signals_per_day || []} />
                </div>

                <div>
                  <div className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Metric ledger</div>
                  <div className="mt-1 text-xs leading-relaxed text-zinc-500">
                    Each row is <span className="font-medium text-zinc-600 dark:text-zinc-400">step 1 · the feeling</span>
                    . Open <span className="font-medium text-zinc-600 dark:text-zinc-400">modelled activity</span> for the E7
                    micro-event, then <span className="font-medium text-zinc-600 dark:text-zinc-400">view journal</span> only
                    if you want the full note — the long text appears once, in that last step.
                  </div>
                  <div className="mt-3 space-y-3">
                    {(impactDetail.evidence || []).map((ev, idx) => {
                      const rowKey = `${idx}-${ev.entry_id}-${ev.input_time}`;
                      const eventOpen = impactLedgerKey === rowKey;
                      return (
                        <div
                          key={rowKey}
                          className="rounded-2xl border border-zinc-200/90 bg-zinc-50/80 p-3 transition-[border-color,box-shadow] duration-200 hover:border-zinc-300/90 hover:shadow-sm dark:border-zinc-800 dark:bg-zinc-900/35 dark:hover:border-zinc-600"
                        >
                          <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">Step 1 · Signal</div>
                          <div className="mt-1 flex items-center justify-between gap-3 text-xs">
                            <div className="text-zinc-500">{ev.day || ev.input_time || "—"}</div>
                            <div
                              className={[
                                "rounded-full px-2 py-0.5 text-[11px] font-semibold",
                                ev.polarity === "positive"
                                  ? "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300"
                                  : ev.polarity === "negative"
                                  ? "bg-rose-500/15 text-rose-800 dark:text-rose-300"
                                  : "bg-zinc-400/20 text-zinc-700 dark:text-zinc-300",
                              ].join(" ")}
                            >
                              {ev.polarity}
                            </div>
                          </div>
                          <div className="mt-1 text-sm font-semibold text-zinc-900 dark:text-zinc-100">
                            {ev.tag || ev.assignment_name || "signal"}
                          </div>
                          {ev.event_name ? (
                            <div className="mt-0.5 text-xs text-zinc-500">
                              Linked micro-event · <span className="text-zinc-700 dark:text-zinc-300">{ev.event_name}</span>
                            </div>
                          ) : (
                            <div className="mt-0.5 text-xs text-zinc-500">No activity label on this signal.</div>
                          )}
                          <div className="mt-3">
                            <button
                              type="button"
                              disabled={!ev.event_key}
                              title="Open the modeled E7 activity; journal text is available as the next step inside."
                              onClick={() => void toggleImpactEventLedger(rowKey, ev)}
                              className={[
                                "rounded-lg border px-3 py-1.5 text-[11px] font-semibold transition-colors disabled:cursor-not-allowed disabled:opacity-40",
                                eventOpen
                                  ? "border-sky-500/50 bg-sky-500/10 text-sky-900 dark:text-sky-200"
                                  : "border-zinc-200 bg-white text-zinc-700 hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-200 dark:hover:bg-zinc-900",
                              ].join(" ")}
                            >
                              {eventOpen ? "Hide modelled activity" : "Open modelled activity →"}
                            </button>
                          </div>
                          {eventOpen ? (
                            <ImpactEventLedgerPanel
                              data={impactLedgerData}
                              loading={impactLedgerLoading}
                              error={impactLedgerError}
                              activityNameFallback={ev.event_name}
                              sourceEntryId={ev.entry_id}
                            />
                          ) : null}
                        </div>
                      );
                    })}
                    {!impactDetail.evidence?.length ? (
                      <div className="text-sm text-zinc-500">No evidence rows in this window.</div>
                    ) : null}
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}

