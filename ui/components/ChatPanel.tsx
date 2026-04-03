"use client";

import { useEffect, useId, useMemo, useRef, useState } from "react";

import { API_BASE, apiGet } from "@/lib/api";

type WikidataCandidate = {
  wikidata_id: string;
  label?: string;
  description?: string;
  confidence?: string;
};

type InboxTask = {
  id: string;
  type?: string;
  mention: string;
  score?: number;
  created_at?: string;
  status: "open" | "resolved";
  candidate_person_id?: string;
  candidate_name?: string;
  candidate_role?: string;
  proposed_person_id?: string;
  proposed_name?: string;
  proposed_role?: string;
  entry_id?: string;
  place_key?: string;
  entity_label?: string;
  candidates?: WikidataCandidate[] | null;
};

type HintContext = {
  msgId: string;
  entryId: string;
  taskId: string;
  mention: string;
};

/** LLM mention disambiguation (pre-store); drives inline buttons in the chat bubble. */
type ClarificationPayload = {
  id: string;
  name: string;
  candidates: string[];
  suggestion: string | null;
};

type JournalProcessingState = {
  pct: number;
  label: string;
  log: string[];
  preview: JournalPreview | null;
  /** Shown immediately from the client before SSE delivers server stats. */
  sourcePreview?: string;
};

type ChatMsg = {
  id: string;
  role: "user" | "assistant";
  text: string;
  badge?: "Onboarding mode" | "Clarification needed";
  /** When set, this assistant bubble includes inline disambiguation for this entry. */
  entryId?: string;
  openTasks?: InboxTask[];
  /** Snapshot before a hint — shown collapsed above fresh choices. */
  frozenOpenTasks?: InboxTask[];
  /** New assistant row after a hint (visual “continuation”). */
  hintFollowUp?: boolean;
  /** Structured picks for clarification mode (server `/chat` → `clarification`). */
  clarification?: ClarificationPayload;
  /** Live journal pipeline progress (replaced by the real reply when done). */
  processing?: JournalProcessingState;
};

const JOURNAL_LOG_MAX = 8;

type JournalPreview = {
  node_count?: number;
  edge_count?: number;
  places?: string[];
  people?: string[];
  activities?: string[];
  types?: string[];
  snippet?: string;
  char_count?: number;
  word_count?: number;
  entry_id?: string;
};

type StreamStagePayload = {
  type?: string;
  stage?: string;
  label?: string;
  pct?: number;
  detail?: string;
  preview?: JournalPreview;
};

type StreamDonePayload = {
  type: string;
  payload?: Record<string, unknown>;
  detail?: string;
};

function consumeSseBuffer(buf: string): { events: StreamDonePayload[]; rest: string } {
  const events: StreamDonePayload[] = [];
  const sep = "\n\n";
  let rest = buf;
  let at: number;
  while ((at = rest.indexOf(sep)) !== -1) {
    const block = rest.slice(0, at).trim();
    rest = rest.slice(at + sep.length);
    if (!block) continue;
    const line = block.split("\n").find((l) => l.startsWith("data: "));
    if (!line) continue;
    try {
      events.push(JSON.parse(line.slice(6)) as StreamDonePayload);
    } catch {
      /* ignore malformed chunk */
    }
  }
  return { events, rest };
}

function uid(): string {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

const PROCESS_RING_R = 54;
const PROCESS_RING_C = 2 * Math.PI * PROCESS_RING_R;

/** Matches server stage percentages (connect 5 → … → vector 100). */
const PIPELINE_TRACK = [
  { minPct: 5, label: "Ingest", hint: "Text received & measured" },
  { minPct: 12, label: "Prep", hint: "LLM: micro-events & mentions" },
  { minPct: 25, label: "WSD", hint: "Word sense & context" },
  { minPct: 45, label: "Spec", hint: "CIDOC graph structure" },
  { minPct: 58, label: "Disambig", hint: "Resolve surface forms" },
  { minPct: 72, label: "Link", hint: "Wikidata / BabelNet" },
  { minPct: 84, label: "People", hint: "Actor resolution" },
  { minPct: 94, label: "Graph", hint: "Neo4j write" },
  { minPct: 100, label: "Vector", hint: "Embeddings for search" },
] as const;

const PIPELINE_TIPS = [
  "We decompose your prose into micro-events (actions, places, people) before building the CIDOC graph.",
  "Places and types are anchored with Wikidata / BabelNet when possible — the same IDs used in open knowledge graphs.",
  "Neo4j stores the memory graph; Weaviate powers “find similar entries” search across your journal.",
  "Disambiguation picks which real-world thing each name refers to (e.g. “Victoria” → station vs queen).",
  "If something can’t be resolved safely, we may ask you a clarification before storing.",
];

function JournalProcessingBubble({ state }: { state: JournalProcessingState }) {
  const gradId = useId().replace(/:/g, "");
  const pct = Math.min(100, Math.max(0, state.pct));
  const dash = (pct / 100) * PROCESS_RING_C;
  const pv = state.preview;
  const [tipIdx, setTipIdx] = useState(0);
  const showTips = state.log.length === 0;

  useEffect(() => {
    if (!showTips) return;
    const id = window.setInterval(() => {
      setTipIdx((i) => (i + 1) % PIPELINE_TIPS.length);
    }, 4200);
    return () => window.clearInterval(id);
  }, [showTips]);

  const showQuote = Boolean((pv?.snippet || state.sourcePreview || "").trim());
  const quoteText = (pv?.snippet || state.sourcePreview || "").trim();
  const nextStageIndex = PIPELINE_TRACK.findIndex((s) => pct < s.minPct);

  return (
    <div className="relative w-full min-w-0 overflow-hidden rounded-2xl border border-indigo-200/60 bg-gradient-to-br from-indigo-50/95 via-white to-violet-50/90 p-4 shadow-lg shadow-indigo-500/10 ring-1 ring-indigo-500/5 sm:p-5 dark:border-indigo-800/50 dark:from-indigo-950/80 dark:via-zinc-900 dark:to-violet-950/50 dark:shadow-indigo-950/30 dark:ring-indigo-400/10">
      <div className="pointer-events-none absolute -right-8 -top-8 h-32 w-32 rounded-full bg-indigo-400/10 blur-2xl dark:bg-indigo-500/15" />
      <div className="pointer-events-none absolute -bottom-6 -left-6 h-24 w-24 rounded-full bg-violet-400/10 blur-2xl dark:bg-violet-500/10" />

      {/* Ring + status share one row; cards below use full width so nothing sits in an empty column beside the ring. */}
      <div className="relative flex flex-col gap-4 sm:flex-row sm:items-center sm:gap-6">
        <div className="mx-auto flex shrink-0 flex-col items-center sm:mx-0">
          <div className="relative h-[132px] w-[132px]">
            <svg
              className="h-[132px] w-[132px] -rotate-90 text-indigo-200/90 dark:text-zinc-700"
              viewBox="0 0 120 120"
              aria-hidden
            >
              <defs>
                <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="100%">
                  <stop offset="0%" stopColor="#6366f1" />
                  <stop offset="100%" stopColor="#8b5cf6" />
                </linearGradient>
              </defs>
              <circle cx="60" cy="60" r={PROCESS_RING_R} fill="none" stroke="currentColor" strokeWidth="9" />
              <circle
                cx="60"
                cy="60"
                r={PROCESS_RING_R}
                fill="none"
                stroke={`url(#${gradId})`}
                strokeWidth="9"
                strokeLinecap="round"
                strokeDasharray={`${dash} ${PROCESS_RING_C}`}
                className="transition-[stroke-dasharray] duration-500 ease-out"
              />
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center text-center">
              <span className="text-2xl font-bold tabular-nums tracking-tight text-indigo-950 dark:text-indigo-100">
                {Math.round(pct)}%
              </span>
              <span className="mt-0.5 text-[10px] font-medium uppercase tracking-wider text-indigo-600/80 dark:text-indigo-300/80">
                progress
              </span>
            </div>
          </div>
        </div>

        <div className="min-w-0 flex-1 text-center sm:text-left">
          <div className="flex flex-wrap items-center justify-center gap-2 sm:justify-start">
            <span className="inline-flex items-center gap-1.5 rounded-full bg-indigo-600/10 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide text-indigo-800 dark:bg-indigo-400/15 dark:text-indigo-200">
              <span className="relative flex h-2 w-2">
                <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-indigo-400 opacity-60" />
                <span className="relative inline-flex h-2 w-2 rounded-full bg-indigo-600 dark:bg-indigo-400" />
              </span>
              Processing your entry
            </span>
          </div>
          <p className="mt-2 text-base font-semibold leading-snug text-zinc-900 dark:text-zinc-50">
            {state.label || "Working on your journal…"}
          </p>
        </div>
      </div>

      <div className="relative mt-4 w-full min-w-0 space-y-3">
          {showQuote ? (
            <div className="w-full rounded-xl border border-zinc-200/80 bg-white/70 p-3 shadow-sm sm:p-4 dark:border-zinc-600/60 dark:bg-zinc-950/50">
              <p className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
                Your entry
              </p>
              <blockquote className="mt-1.5 border-l-4 border-indigo-400/80 pl-3 text-sm italic leading-relaxed text-zinc-800 dark:border-indigo-500/70 dark:text-zinc-200">
                “{quoteText}”
              </blockquote>
              {(pv?.word_count != null || pv?.char_count != null || pv?.entry_id) && (
                <div className="mt-3 flex flex-wrap gap-2">
                  {pv?.word_count != null ? (
                    <span className="rounded-md bg-zinc-900/5 px-2 py-px text-[11px] font-medium tabular-nums text-zinc-700 dark:bg-white/10 dark:text-zinc-300">
                      {pv.word_count} words
                    </span>
                  ) : null}
                  {pv?.char_count != null ? (
                    <span className="rounded-md bg-zinc-900/5 px-2 py-px text-[11px] font-medium tabular-nums text-zinc-700 dark:bg-white/10 dark:text-zinc-300">
                      {pv.char_count} characters
                    </span>
                  ) : null}
                  {pv?.entry_id ? (
                    <span className="rounded-md bg-indigo-500/10 px-2 py-px font-mono text-[10px] text-indigo-800 dark:bg-indigo-400/15 dark:text-indigo-200">
                      id {String(pv.entry_id).slice(0, 8)}…
                    </span>
                  ) : null}
                </div>
              )}
            </div>
          ) : null}

          <div className="w-full rounded-xl border border-indigo-200/50 bg-indigo-500/[0.06] p-3 sm:p-4 dark:border-indigo-800/40 dark:bg-indigo-950/30">
            <p className="text-[10px] font-semibold uppercase tracking-wide text-indigo-700 dark:text-indigo-300">
              Pipeline stages
            </p>
            <div className="mt-2 flex flex-wrap gap-2">
              {PIPELINE_TRACK.map((s, i) => {
                const done = pct >= s.minPct;
                const pulse = nextStageIndex >= 0 && nextStageIndex === i;
                return (
                  <span
                    key={s.label}
                    title={s.hint}
                    className={`rounded-lg border px-2 py-1 text-[10px] font-semibold transition-colors ${
                      done
                        ? "border-emerald-400/50 bg-emerald-500/15 text-emerald-900 dark:border-emerald-600/40 dark:text-emerald-100"
                        : pulse
                          ? "border-indigo-400/60 bg-indigo-500/20 text-indigo-950 shadow-sm shadow-indigo-500/20 dark:border-indigo-500/50 dark:text-indigo-50"
                          : "border-zinc-200/80 bg-white/50 text-zinc-500 dark:border-zinc-700 dark:bg-zinc-800/50 dark:text-zinc-400"
                    }`}
                  >
                    {done ? "✓ " : ""}
                    {s.label}
                  </span>
                );
              })}
            </div>
          </div>

          {pv &&
          (pv.node_count != null ||
            pv.places?.length ||
            pv.people?.length ||
            pv.activities?.length ||
            pv.types?.length) ? (
            <div className="w-full space-y-1.5">
              <p className="text-[11px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
                Live graph preview
              </p>
              <div className="flex flex-wrap gap-1.5">
                {pv.node_count != null ? (
                  <span className="rounded-lg border border-zinc-200/80 bg-white/80 px-2.5 py-1 text-xs font-medium text-zinc-700 shadow-sm dark:border-zinc-600 dark:bg-zinc-800/80 dark:text-zinc-200">
                    {pv.node_count} nodes
                    {pv.edge_count != null ? ` · ${pv.edge_count} edges` : ""}
                  </span>
                ) : null}
                {(pv.places || []).slice(0, 4).map((p) => (
                  <span
                    key={`pl-${p}`}
                    className="max-w-[140px] truncate rounded-lg border border-teal-300/40 bg-teal-500/10 px-2.5 py-1 text-xs font-medium text-teal-900 dark:border-teal-600/40 dark:text-teal-100"
                  >
                    {p}
                  </span>
                ))}
                {(pv.people || []).slice(0, 3).map((p) => (
                  <span
                    key={`pe-${p}`}
                    className="max-w-[140px] truncate rounded-lg border border-violet-300/40 bg-violet-500/10 px-2.5 py-1 text-xs font-medium text-violet-900 dark:border-violet-600/40 dark:text-violet-100"
                  >
                    {p}
                  </span>
                ))}
                {(pv.activities || []).slice(0, 3).map((p) => (
                  <span
                    key={`ac-${p}`}
                    className="max-w-[140px] truncate rounded-lg border border-amber-300/40 bg-amber-500/10 px-2.5 py-1 text-xs font-medium text-amber-950 dark:border-amber-600/40 dark:text-amber-100"
                  >
                    {p}
                  </span>
                ))}
                {(pv.types || []).slice(0, 4).map((p) => (
                  <span
                    key={`ty-${p}`}
                    className="max-w-[120px] truncate rounded-lg border border-sky-300/35 bg-sky-500/10 px-2 py-1 text-[11px] text-sky-950 dark:border-sky-600/35 dark:text-sky-100"
                  >
                    {p}
                  </span>
                ))}
              </div>
            </div>
          ) : null}

          {state.log.length > 0 ? (
            <div className="w-full rounded-xl border border-zinc-200/70 bg-white/60 p-3 sm:p-4 dark:border-zinc-700/80 dark:bg-zinc-950/40">
              <p className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
                Activity
              </p>
              <ul className="max-h-[140px] space-y-1.5 overflow-y-auto text-[13px] leading-snug text-zinc-700 dark:text-zinc-300">
                {state.log.slice(-6).map((line, i) => (
                  <li key={`${i}-${line.slice(0, 24)}`} className="flex gap-2 border-l-2 border-indigo-400/50 pl-2 dark:border-indigo-500/40">
                    <span className="shrink-0 text-indigo-500 dark:text-indigo-400">→</span>
                    <span className="min-w-0 break-words">{line}</span>
                  </li>
                ))}
              </ul>
            </div>
          ) : showTips ? (
            <div className="w-full rounded-xl border border-violet-200/50 bg-gradient-to-br from-violet-50/90 to-indigo-50/50 p-3 sm:p-4 dark:border-violet-800/40 dark:from-violet-950/40 dark:to-indigo-950/30">
              <p className="text-[10px] font-semibold uppercase tracking-wide text-violet-700 dark:text-violet-300">
                What’s happening
              </p>
              <p key={tipIdx} className="mt-2 text-sm leading-relaxed text-violet-950/90 dark:text-violet-100/95">
                {PIPELINE_TIPS[tipIdx % PIPELINE_TIPS.length]}
              </p>
            </div>
          ) : null}
      </div>
    </div>
  );
}

function FrozenTaskSnapshot({ task: t }: { task: InboxTask }) {
  const isPlace = t.type === "place_wikidata";
  return (
    <div className="rounded-lg border border-zinc-200/80 bg-zinc-50/80 p-2 dark:border-zinc-700 dark:bg-zinc-900/50">
      <div className="text-[11px] font-semibold text-zinc-600 dark:text-zinc-300">
        {isPlace ? `Place: ${t.mention}` : `Person: ${t.mention}`}
      </div>
      {isPlace ? (
        <div className="mt-1 flex flex-wrap gap-1">
          {(t.candidates || []).map((c) => (
            <span
              key={c.wikidata_id}
              className="inline-block max-w-full rounded border border-zinc-200 bg-white/90 px-2 py-1 text-[10px] text-zinc-600 dark:border-zinc-600 dark:bg-zinc-950 dark:text-zinc-400"
            >
              <span className="font-medium text-zinc-800 dark:text-zinc-200">{c.label || c.wikidata_id}</span>
              {c.description ? (
                <span className="mt-0.5 block line-clamp-1 text-zinc-500">{c.description}</span>
              ) : null}
              <span className="font-mono text-zinc-400">{c.wikidata_id}</span>
            </span>
          ))}
        </div>
      ) : (
        <p className="mt-1 text-[10px] text-zinc-500">
          {t.candidate_name || "—"} → {t.proposed_name || "—"}
        </p>
      )}
    </div>
  );
}

export function ChatPanel() {
  const [msgs, setMsgs] = useState<ChatMsg[]>([
    { id: uid(), role: "assistant", text: "Type a journal entry and press Send." },
  ]);
  const [text, setText] = useState<string>("");
  const [busy, setBusy] = useState<boolean>(false);
  const [inlineBusy, setInlineBusy] = useState<string>("");
  const [hintContext, setHintContext] = useState<HintContext | null>(null);
  const endRef = useRef<HTMLDivElement | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const autoScrollRef = useRef<boolean>(true);

  const canSend = useMemo(() => text.trim().length > 0, [text]);

  const lastClarificationIndex = useMemo(() => {
    for (let i = msgs.length - 1; i >= 0; i--) {
      if (msgs[i].role === "assistant" && msgs[i].clarification) return i;
    }
    return -1;
  }, [msgs]);

  function clearHintContext() {
    setHintContext(null);
  }

  useEffect(() => {
    if (!autoScrollRef.current) return;
    const el = scrollRef.current;
    if (!el) return;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (!autoScrollRef.current) return;
        el.scrollTop = el.scrollHeight;
      });
    });
  }, [msgs, busy, inlineBusy]);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;

    const onScroll = () => {
      const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
      autoScrollRef.current = distanceFromBottom < 60;
    };

    onScroll();
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  function patchOpenTasksForMessage(msgId: string, tasks: InboxTask[]) {
    setMsgs((prev) =>
      prev.map((m) => {
        if (m.id !== msgId) return m;
        const nextText =
          tasks.length === 0
            ? "All set — every open link for this entry is resolved. You can keep chatting or finish later in the inbox anytime."
            : m.text;
        return { ...m, openTasks: tasks, text: nextText };
      }),
    );
  }

  function mergeCandidatesOntoTasks(
    tasks: InboxTask[],
    taskId: string,
    candidates: WikidataCandidate[],
  ): InboxTask[] {
    return tasks.map((t) => (t.id === taskId ? { ...t, candidates } : t));
  }

  async function fetchOpenTasksForEntry(entryId: string): Promise<InboxTask[]> {
    const out = await apiGet<{ items: InboxTask[] }>(
      `/inbox?status=open&limit=50&entry_id=${encodeURIComponent(entryId)}`,
    );
    return out.items || [];
  }

  async function resolvePlaceInline(msgId: string, entryId: string, taskId: string, wikidataId?: string) {
    setInlineBusy(taskId);
    try {
      const body: Record<string, string> = { decision: wikidataId ? "pick" : "skip" };
      if (wikidataId) body.wikidata_id = wikidataId;
      const res = await fetch(`${API_BASE}/inbox/${encodeURIComponent(taskId)}/resolve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error(`resolve failed: ${res.status} ${t}`);
      }
      const tasks = await fetchOpenTasksForEntry(entryId);
      patchOpenTasksForMessage(msgId, tasks);
      window.dispatchEvent(new CustomEvent("memo:inbox-changed"));
    } catch (e: any) {
      setMsgs((prev) => [
        ...prev,
        { id: uid(), role: "assistant", text: "Error: " + (e?.message || String(e)) },
      ]);
    } finally {
      setInlineBusy("");
    }
  }

  async function resolvePersonInline(msgId: string, entryId: string, taskId: string, decision: "merge" | "split") {
    setInlineBusy(taskId);
    try {
      const res = await fetch(`${API_BASE}/inbox/${encodeURIComponent(taskId)}/resolve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ decision }),
      });
      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error(`resolve failed: ${res.status} ${t}`);
      }
      const tasks = await fetchOpenTasksForEntry(entryId);
      patchOpenTasksForMessage(msgId, tasks);
      window.dispatchEvent(new CustomEvent("memo:inbox-changed"));
    } catch (e: any) {
      setMsgs((prev) => [
        ...prev,
        { id: uid(), role: "assistant", text: "Error: " + (e?.message || String(e)) },
      ]);
    } finally {
      setInlineBusy("");
    }
  }

  async function applyChatPayload(out: Record<string, unknown>, hint: HintContext | null) {
    if (out?.type === "disambiguation_hint") {
      const entryId = String(out.entry_id || "").trim();
      const taskId = String(out.task_id || "").trim();
      const ok = Boolean(out.ok);
      const apiCands: WikidataCandidate[] = Array.isArray(out.candidates) ? out.candidates : [];
      let tasksFromApi: InboxTask[] = entryId ? await fetchOpenTasksForEntry(entryId) : [];
      if (ok && taskId && apiCands.length) {
        tasksFromApi = mergeCandidatesOntoTasks(tasksFromApi, taskId, apiCands);
      }
      const followText = String(
        out?.message || (ok ? "Updated the list from your hint." : "No new matches from that hint."),
      );

      setMsgs((prev) => {
        const anchorMsgId =
          hint?.msgId ||
          [...prev]
            .reverse()
            .find((m) => m.role === "assistant" && m.entryId === entryId && m.openTasks?.length)
            ?.id;

        let next = prev;
        if (ok && anchorMsgId) {
          next = prev.map((m) => {
            if (m.id !== anchorMsgId || !m.openTasks?.length) return m;
            return {
              ...m,
              frozenOpenTasks: JSON.parse(JSON.stringify(m.openTasks)) as InboxTask[],
              openTasks: undefined,
            };
          });
        }

        if (ok && tasksFromApi.length > 0) {
          next = [
            ...next,
            {
              id: uid(),
              role: "assistant",
              text: followText,
              entryId,
              openTasks: tasksFromApi,
              hintFollowUp: true,
            },
          ];
        } else {
          next = [
            ...next,
            {
              id: uid(),
              role: "assistant",
              text: followText,
            },
          ];
        }
        return next;
      });
      window.dispatchEvent(new CustomEvent("memo:inbox-changed"));
      return;
    }
    if (out?.type === "question" && out?.question) {
      const mode = String(out?.mode || "").toLowerCase();
      const badge =
        mode === "onboarding"
          ? "Onboarding mode"
          : mode === "clarification"
            ? "Clarification needed"
            : undefined;
      const rawClar = out?.clarification;
      let clarification: ClarificationPayload | undefined;
      if (
        mode === "clarification" &&
        rawClar &&
        typeof rawClar === "object" &&
        typeof (rawClar as { id?: string }).id === "string"
      ) {
        const rc = rawClar as { id: string; name?: string; candidates?: unknown[]; suggestion?: unknown };
        const cands = Array.isArray(rc.candidates)
          ? rc.candidates.map((x: unknown) => String(x)).filter(Boolean)
          : [];
        clarification = {
          id: String(rc.id),
          name: String(rc.name || ""),
          candidates: cands.slice(0, 3),
          suggestion: rc.suggestion != null && String(rc.suggestion).trim()
            ? String(rc.suggestion).trim()
            : null,
        };
      }
      setMsgs((prev) => [
        ...prev,
        { id: uid(), role: "assistant", text: String(out.question), badge, clarification },
      ]);
    } else if (out?.type === "profile_saved") {
      setMsgs((prev) => [
        ...prev,
        { id: uid(), role: "assistant", text: String(out?.message || "Profile saved.") },
      ]);
    } else {
      const result = out?.result as { entry_id?: string } | undefined;
      const id = result?.entry_id || "—";
      const openTasks: InboxTask[] = Array.isArray(out?.open_tasks) ? (out.open_tasks as InboxTask[]) : [];
      const n = openTasks.length;
      const summary =
        n === 0
          ? `Stored your entry (${id}). Nothing needs linking right now — you’re all set.`
          : `Stored your entry (${id}). ${n === 1 ? "One item" : `${n} items`} below could use your help — pick a button, skip, or use “Describe where this was…” and then type a short hint in the box (city, country, station…). You can always finish in the Inbox tab.`;
      const msgId = uid();
      setMsgs((prev) => [
        ...prev,
        {
          id: msgId,
          role: "assistant",
          text: summary,
          entryId: String(result?.entry_id || ""),
          openTasks: n > 0 ? openTasks : undefined,
        },
      ]);
      window.dispatchEvent(new CustomEvent("memo:new-entry"));
      window.dispatchEvent(new CustomEvent("memo:inbox-changed"));
    }
  }

  async function postChatMessage(userLine: string, opts?: { fromTextarea?: boolean }) {
    const t = userLine.trim();
    if (!t) return;
    const fromTa = opts?.fromTextarea ?? false;
    if (fromTa) {
      setText("");
    }
    const workId = uid();
    const initialProcessing: JournalProcessingState = {
      pct: 2,
      label: "Sending to Memo…",
      log: [],
      preview: null,
      sourcePreview: t.slice(0, 600),
    };
    setBusy(true);
    setMsgs((prev) => [
      ...prev,
      { id: workId, role: "assistant", text: "", processing: initialProcessing },
    ]);
    const hint = hintContext;
    if (hint) {
      clearHintContext();
    }
    const body: Record<string, string> = { message: t };
    if (hint?.taskId) {
      body.disambiguation_hint_task_id = hint.taskId;
    }

    function patchWorkMsg(updater: (p: JournalProcessingState) => JournalProcessingState) {
      setMsgs((prev) =>
        prev.map((m) =>
          m.id === workId && m.processing ? { ...m, processing: updater(m.processing) } : m,
        ),
      );
    }

    type StreamResult =
      | { kind: "done"; payload: Record<string, unknown> }
      | { kind: "error"; msg: string }
      | { kind: "fallback" };

    async function tryChatStream(): Promise<StreamResult> {
      let res: Response;
      try {
        res = await fetch(API_BASE + "/chat/stream", {
          method: "POST",
          headers: { "content-type": "application/json", accept: "text/event-stream" },
          body: JSON.stringify(body),
        });
      } catch {
        return { kind: "fallback" };
      }
      if (!res.ok || !res.body) {
        return { kind: "fallback" };
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const { events, rest } = consumeSseBuffer(buffer);
          buffer = rest;
          for (const ev of events) {
            const e = ev as StreamDonePayload & StreamStagePayload;
            if (e.type === "stage") {
              patchWorkMsg((p) => ({
                pct: typeof e.pct === "number" ? Math.min(100, Math.max(0, e.pct)) : p.pct,
                label: (e.label as string) || p.label,
                log: e.detail
                  ? [...p.log, String(e.detail)].slice(-JOURNAL_LOG_MAX)
                  : p.log,
                preview:
                  e.preview && typeof e.preview === "object"
                    ? { ...(p.preview || {}), ...(e.preview as JournalPreview) }
                    : p.preview,
                sourcePreview: p.sourcePreview,
              }));
              continue;
            }
            if (e.type === "done" && e.payload && typeof e.payload === "object") {
              return { kind: "done", payload: e.payload as Record<string, unknown> };
            }
            if (e.type === "error") {
              const msg =
                typeof (e as { detail?: string }).detail === "string"
                  ? (e as { detail: string }).detail
                  : "Processing failed";
              return { kind: "error", msg };
            }
          }
        }
      } finally {
        try {
          reader.releaseLock();
        } catch {
          /* already released */
        }
      }
      return { kind: "fallback" };
    }

    try {
      const streamed = await tryChatStream();
      if (streamed.kind === "done") {
        setMsgs((prev) => prev.filter((m) => m.id !== workId));
        await applyChatPayload(streamed.payload, hint);
        return;
      }
      if (streamed.kind === "error") {
        setMsgs((prev) => [
          ...prev.filter((m) => m.id !== workId),
          { id: uid(), role: "assistant", text: "Error: " + streamed.msg },
        ]);
        return;
      }

      setMsgs((prev) => prev.filter((m) => m.id !== workId));
      const res = await fetch(API_BASE + "/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      if (!res.ok) {
        const err = await res.text().catch(() => "");
        throw new Error("API error " + res.status + ": " + err);
      }
      const out = (await res.json()) as Record<string, unknown>;
      await applyChatPayload(out, hint);
    } catch (e: any) {
      if (hint) {
        setHintContext(hint);
      }
      setMsgs((prev) => [
        ...prev.filter((m) => m.id !== workId),
        { id: uid(), role: "assistant", text: "Error: " + (e?.message || String(e)) },
      ]);
    } finally {
      setBusy(false);
    }
  }

  async function send() {
    const t = text.trim();
    if (!t) return;
    setMsgs((prev) => [...prev, { id: uid(), role: "user", text: t }]);
    await postChatMessage(t, { fromTextarea: true });
  }

  async function sendClarificationPick(answer: string) {
    const a = answer.trim();
    if (!a) return;
    setMsgs((prev) => [...prev, { id: uid(), role: "user", text: a }]);
    await postChatMessage(a, { fromTextarea: false });
  }

  return (
    <div className="flex h-full flex-col min-h-0">
      <div className="flex items-center justify-between gap-3 border-b border-zinc-200 dark:border-zinc-800 px-4 py-3">
        <div className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Memo</div>
        <div className="flex items-center gap-2 text-xs text-zinc-400 dark:text-zinc-500">
          {busy ? (
            <span className="inline-flex items-center gap-1.5 rounded-full bg-indigo-500/10 px-2 py-0.5 text-[11px] font-medium text-indigo-700 dark:text-indigo-300">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-indigo-500" />
              Working
            </span>
          ) : null}
          <span className="hidden sm:inline">chat</span>
        </div>
      </div>

      <div
        ref={scrollRef}
        className="flex-1 min-h-0 overflow-y-scroll px-4 py-4 overflow-x-hidden overscroll-contain"
      >
        <div className="space-y-3">
          {msgs.map((m, msgIdx) => (
            <div
              key={m.id}
              className={
                m.role === "user"
                  ? "ml-auto max-w-[95%] rounded-2xl border border-indigo-200/50 bg-indigo-50/90 px-3 py-2 text-sm text-indigo-950 dark:border-indigo-800/40 dark:bg-indigo-950/35 dark:text-indigo-50"
                  : `rounded-2xl border text-sm ${
                      m.processing
                        ? "w-full max-w-[min(95%,56rem)] border-transparent bg-transparent p-0 shadow-none"
                        : `max-w-[95%] border px-3 py-2 ${
                            m.hintFollowUp
                              ? "border-indigo-500/35 bg-gradient-to-br from-slate-900/90 to-indigo-950/50 text-slate-100 shadow-md shadow-indigo-950/20"
                              : "border-zinc-200/80 bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-900 text-zinc-900 dark:text-zinc-100"
                          }`
                    }`
              }
            >
              {m.processing ? (
                <JournalProcessingBubble state={m.processing} />
              ) : (
                <>
                  {m.role === "assistant" && m.badge ? (
                    <div className="mb-1 inline-flex rounded-full border border-zinc-300 dark:border-zinc-700 bg-zinc-100 dark:bg-zinc-800 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-300">
                      {m.badge}
                    </div>
                  ) : null}
                  {m.role === "assistant" && m.hintFollowUp ? (
                    <div className="mb-2 flex items-center gap-2 text-[11px] font-semibold uppercase tracking-wide text-indigo-200">
                      <span className="inline-block h-2 w-2 rounded-full bg-red-500 shadow-sm shadow-red-500/50" />
                      After your hint — updated choices
                    </div>
                  ) : null}
                  <div className="whitespace-pre-wrap">{m.text}</div>
                </>
              )}
              {m.role === "assistant" &&
              m.clarification &&
              msgIdx === lastClarificationIndex ? (
                <div className="mt-3 space-y-2 rounded-xl border border-amber-200/70 bg-amber-50/80 p-3 dark:border-amber-900/50 dark:bg-amber-950/30">
                  <div className="text-[11px] font-semibold uppercase tracking-wide text-amber-900 dark:text-amber-200">
                    Your answer
                  </div>
                  {m.clarification.candidates.length > 0 ? (
                    <div className="flex flex-col gap-2">
                      {m.clarification.candidates.map((c) => (
                        <button
                          key={c}
                          type="button"
                          disabled={busy}
                          onClick={() => void sendClarificationPick(c)}
                          className="rounded-lg border border-amber-300/80 bg-white px-3 py-2 text-left text-sm font-medium text-amber-950 shadow-sm hover:bg-amber-100 disabled:opacity-50 dark:border-amber-800 dark:bg-zinc-900 dark:text-amber-50 dark:hover:bg-zinc-800"
                        >
                          {c}
                        </button>
                      ))}
                    </div>
                  ) : null}
                  {m.clarification.suggestion &&
                  !m.clarification.candidates.some(
                    (c) => c.toLowerCase() === m.clarification!.suggestion!.toLowerCase(),
                  ) ? (
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => void sendClarificationPick(m.clarification!.suggestion!)}
                      className="w-full rounded-lg border border-emerald-400/70 bg-emerald-700 px-3 py-2 text-left text-sm font-semibold text-white shadow-sm hover:bg-emerald-600 disabled:opacity-50 dark:bg-emerald-800 dark:hover:bg-emerald-700"
                    >
                      Use suggestion: {m.clarification.suggestion}
                    </button>
                  ) : null}
                  <p className="text-[11px] text-amber-900/80 dark:text-amber-200/90">
                    Or type a different answer in the box below and press <span className="font-semibold">Send</span>
                    {" — or "}
                    <button
                      type="button"
                      disabled={busy}
                      onClick={() => void sendClarificationPick("skip")}
                      className="font-semibold text-amber-950 underline decoration-amber-600/60 underline-offset-2 dark:text-amber-100"
                    >
                      skip
                    </button>
                    .
                  </p>
                </div>
              ) : null}
              {m.role === "assistant" && m.frozenOpenTasks && m.frozenOpenTasks.length > 0 ? (
                <details className="mt-3 rounded-xl border border-zinc-300/60 bg-zinc-100/50 dark:border-zinc-700 dark:bg-black/20">
                  <summary className="cursor-pointer select-none px-3 py-2 text-xs font-medium text-zinc-600 dark:text-zinc-400">
                    Earlier suggestions (before your hint) — tap to expand
                  </summary>
                  <div className="space-y-2 border-t border-zinc-200/80 p-3 dark:border-zinc-800">
                    {m.frozenOpenTasks.map((t) => (
                      <FrozenTaskSnapshot key={`f-${t.id}`} task={t} />
                    ))}
                  </div>
                </details>
              ) : null}
              {m.role === "assistant" && m.entryId && m.openTasks && m.openTasks.length > 0 ? (
                <div
                  className={
                    m.hintFollowUp
                      ? "mt-3 space-y-3 border-l-2 border-red-500/70 pl-3"
                      : "mt-3 space-y-3 border-t border-indigo-200/30 pt-3 dark:border-indigo-800/30"
                  }
                >
                  {m.openTasks.map((t) => {
                    const isPlace = t.type === "place_wikidata";
                    return (
                      <div
                        key={t.id}
                        className="rounded-xl border border-indigo-200/40 bg-white/70 p-3 dark:border-indigo-800/40 dark:bg-slate-950/60"
                      >
                        <div className="text-xs font-semibold text-indigo-950 dark:text-indigo-100">
                          {isPlace ? (
                            <>
                              Link place:{" "}
                              <span className="text-indigo-900 dark:text-white">{t.mention}</span>
                              {t.entity_label ? (
                                <span className="ml-1 font-normal text-indigo-600/80 dark:text-indigo-300/80">
                                  ({t.entity_label})
                                </span>
                              ) : null}
                            </>
                          ) : (
                            <>
                              Person:{" "}
                              <span className="text-indigo-900 dark:text-white">{t.mention}</span>
                            </>
                          )}
                        </div>
                        {isPlace ? (
                          <div className="mt-1 space-y-1 text-[11px] text-indigo-800/80 dark:text-indigo-200/80">
                            <p>Choose a match or skip. You can also describe the location in your own words.</p>
                            <button
                              type="button"
                              disabled={inlineBusy === t.id || busy}
                              onClick={() =>
                                setHintContext({
                                  msgId: m.id,
                                  entryId: m.entryId!,
                                  taskId: t.id,
                                  mention: t.mention,
                                })
                              }
                              className="text-left font-medium text-indigo-700 underline decoration-indigo-400/70 underline-offset-2 hover:text-indigo-900 dark:text-indigo-200 dark:hover:text-white"
                            >
                              Add a hint for this place…
                            </button>
                          </div>
                        ) : (
                          <p className="mt-1 text-[11px] text-indigo-800/80 dark:text-indigo-200/80">
                            candidate: {t.candidate_name || "—"} ({t.candidate_role || "—"}) · proposed:{" "}
                            {t.proposed_name || "—"} ({t.proposed_role || "—"})
                          </p>
                        )}
                        {isPlace ? (
                          <div className="mt-2 flex flex-wrap gap-2">
                            {(t.candidates || []).map((c) => (
                              <button
                                key={c.wikidata_id}
                                disabled={inlineBusy === t.id}
                                type="button"
                                title={c.description || c.wikidata_id}
                                onClick={() =>
                                  void resolvePlaceInline(m.id, m.entryId!, t.id, c.wikidata_id)
                                }
                                className="max-w-full rounded-lg border border-indigo-300/50 bg-indigo-600 px-3 py-2 text-left text-xs font-semibold text-white shadow-sm transition hover:bg-indigo-500 disabled:opacity-50 dark:border-indigo-500/40 dark:bg-indigo-600 dark:hover:bg-indigo-500"
                              >
                                <span className="block truncate">{c.label || c.wikidata_id}</span>
                                {c.description ? (
                                  <span className="mt-0.5 block line-clamp-2 text-[10px] font-normal text-indigo-100/95">
                                    {c.description}
                                  </span>
                                ) : null}
                                <span className="block font-mono text-[10px] font-normal text-indigo-200/90">
                                  {c.wikidata_id}
                                </span>
                              </button>
                            ))}
                            <button
                              disabled={inlineBusy === t.id}
                              type="button"
                              onClick={() => void resolvePlaceInline(m.id, m.entryId!, t.id)}
                              className="rounded-lg border-2 border-indigo-400/40 bg-transparent px-3 py-2 text-xs font-semibold text-indigo-800 hover:bg-indigo-50 disabled:opacity-50 dark:border-indigo-500/50 dark:text-indigo-100 dark:hover:bg-indigo-950/50"
                            >
                              Skip
                            </button>
                          </div>
                        ) : (
                          <div className="mt-2 flex flex-wrap gap-2">
                            <button
                              disabled={inlineBusy === t.id}
                              type="button"
                              onClick={() => void resolvePersonInline(m.id, m.entryId!, t.id, "merge")}
                              className="rounded-lg border border-indigo-300/50 bg-indigo-600 px-3 py-2 text-xs font-semibold text-white hover:bg-indigo-500 disabled:opacity-50 dark:bg-indigo-600"
                            >
                              Merge (same person)
                            </button>
                            <button
                              disabled={inlineBusy === t.id}
                              type="button"
                              onClick={() => void resolvePersonInline(m.id, m.entryId!, t.id, "split")}
                              className="rounded-lg border-2 border-indigo-400/40 px-3 py-2 text-xs font-semibold text-indigo-800 dark:text-indigo-100"
                            >
                              Split (different)
                            </button>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              ) : null}
            </div>
          ))}
          <div ref={endRef} />
        </div>
      </div>

      <div className="border-t border-zinc-200 dark:border-zinc-800 p-3">
        {hintContext ? (
          <div className="mb-2 flex flex-wrap items-center justify-between gap-2 rounded-lg border border-amber-200/80 bg-amber-50/90 px-3 py-2 text-xs text-amber-950 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-100">
            <span>
              Sending as a <span className="font-semibold">hint</span> for place{" "}
              <span className="font-semibold">{hintContext.mention}</span> — not a new journal entry.
            </span>
            <button
              type="button"
              onClick={clearHintContext}
              className="shrink-0 rounded-md border border-amber-300 bg-white px-2 py-0.5 font-medium text-amber-900 hover:bg-amber-100 dark:border-amber-800 dark:bg-zinc-900 dark:text-amber-50 dark:hover:bg-zinc-800"
            >
              Cancel
            </button>
          </div>
        ) : null}
        <div className="flex gap-2">
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key !== "Enter") return;
              if (e.shiftKey) return;
              if ((e.nativeEvent as any).isComposing) return;
              e.preventDefault();
              if (!busy && canSend) void send();
            }}
            rows={4}
            className="min-h-[96px] flex-1 resize-y rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 px-3 py-2 text-sm text-zinc-900 dark:text-zinc-100 outline-none placeholder:text-zinc-500 focus:border-zinc-400 dark:focus:border-zinc-600"
            placeholder={
              hintContext
                ? `e.g. central London, weekend in the UK, Victoria BC…`
                : "Write an entry…"
            }
          />
          <button
            disabled={!canSend || busy}
            onClick={send}
            className="rounded-xl bg-zinc-200 hover:bg-zinc-300 dark:bg-zinc-700 dark:hover:bg-zinc-600 px-4 text-sm font-semibold text-zinc-900 dark:text-white disabled:cursor-not-allowed disabled:opacity-40"
          >
            {busy ? "…" : "Send"}
          </button>
        </div>
      </div>
    </div>
  );
}
