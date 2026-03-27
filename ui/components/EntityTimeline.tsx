"use client";

import type { ReactNode } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { apiGet } from "@/lib/api";

type EntityItem = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  places?: string[];
  text_preview?: string;
  event_key?: string;
  activity_name?: string;
};

type EntityRef = {
  type: string;
  name: string;
  /** Backend must send a non-null ref; journal rows use E73_Information_Object:<entry id>. */
  ref: string | null;
  /** Present for E73: journal vs context excerpt in the graph. */
  note_role?: string | null;
};

type PersonFeelingTag = {
  name: string;
  count: number;
  ref: string;
};

type OverviewPerson = {
  kind: "Person";
  ref: string;
  name: string;
  role?: string;
  mentions?: number;
  items: EntityItem[];
  feeling_tags?: PersonFeelingTag[];
};

type OverviewEvent = {
  kind: "Event";
  ref: string;
  activity_name?: string;
  /** First journal snippet when this Event-shaped payload is a place lens. */
  summary_preview?: string;
  event_type?: string;
  day?: string;
  event_time_iso?: string;
  event_time_text?: string;
  places?: string[];
  persons?: { id: string; name: string; role?: string; mentions?: number }[];
  users?: { name: string; mentions?: number }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

type FeelingPerson = { id: string; name: string; role?: string };

type FeelingOccurrence = {
  assignment_key: string;
  assignment_label: string;
  input_time: string;
  day: string;
  entry_id: string;
  entry_preview: string;
  event_key: string;
  activity_name: string;
  activity_kind: string;
  activity_day: string;
  persons: FeelingPerson[];
};

type OverviewFeeling = {
  kind: "Feeling";
  ref: string;
  name: string;
  occurrences: FeelingOccurrence[];
  anchor_person_id?: string;
  anchor_person_name?: string;
  anchor_person_ref?: string;
};

type JournalLink = {
  ref: string;
  name: string;
  bucket: string;
  ref_type: string;
  source: string;
};

type OverviewContext = {
  kind: "E73_Information_Object";
  ref: string;
  name?: string;
  event_type?: string;
  day?: string;
  text?: string;
  /** journal_entry for main notes; empty or other for context-only nodes. */
  entry_kind?: string;
  /** Direct P67 targets plus people/places via linked activities. */
  linked?: JournalLink[];
  topics?: { type: string; name: string }[];
  concepts?: { type: string; name: string }[];
  mentions?: { type: string; name: string }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

type DaySituation = {
  ref: string;
  event_key: string;
  title: string;
  event_type: string;
  places: string[];
};

type DayJournalEntry = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  text_preview?: string;
  places?: string[];
  event_key?: string;
  activity_name?: string;
};

type OverviewDay = {
  kind: "Day";
  ref: string;
  day: string;
  focus: string;
  situations: DaySituation[];
  entries: DayJournalEntry[];
  persons: { id: string; name: string; role?: string; mentions?: number }[];
  users: { name: string }[];
  feeling_tags: PersonFeelingTag[];
};

type Overview = OverviewPerson | OverviewEvent | OverviewContext | OverviewFeeling | OverviewDay;

/** Default: show situation + people panels expanded so cast is visible without extra clicks. */
const DEFAULT_MOMENT_PANEL_FLOW = { journal: false, situation: true, people: true };

/** One row in the chain: feeling line or journal line → situation → people / jump. */
type NormalizedMoment = {
  id: string;
  flavor: "feeling" | "journal";
  step1Label: string;
  tagBadge?: string;
  recordTitle: string;
  day: string;
  time: string;
  entryPreview: string;
  entryId: string;
  eventKey: string;
  activityName: string;
  activityKind: string;
  activityDay: string;
  persons: FeelingPerson[];
};

type HubSituation = {
  kind: "situation";
  /** True when overview ref is E53_Place:… (aggregate of activities & notes here). */
  placeLens: boolean;
  /** Short excerpt (e.g. latest journal line linked to this place). */
  summaryText: string;
  ref: string;
  title: string;
  subtitle: string;
  day: string;
  places: string[];
  eventType: string;
  persons: { id: string; name: string; role?: string }[];
  users: { name: string }[];
  entries: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

type HubContext = {
  kind: "context";
  anchorKind: "journal" | "context";
  name: string;
  day: string;
  eventType: string;
  text: string;
  linked: JournalLink[];
  topics: { name: string }[];
  concepts: { name: string }[];
  mentions: { name: string }[];
  entries: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

type HubData = HubSituation | HubContext;

type NavOption = {
  key: string;
  title: string;
  description: string;
  count: number;
  enabled: boolean;
};

type NavResponse = {
  ref: string;
  display_name: string;
  options: NavOption[];
};

const EXPLORER_CATEGORIES = [
  { id: "person", label: "People", hint: "Named in your journal" },
  { id: "feeling_tag", label: "Feelings & tags", hint: "e.g. satisfaction, stress" },
  { id: "situation", label: "Situations", hint: "Activities & events" },
  { id: "place", label: "Places", hint: "Where it happened" },
  { id: "day", label: "Days", hint: "Situations, notes, people & tags on that date" },
  { id: "idea", label: "Ideas & topics", hint: "Concepts" },
  { id: "note", label: "Notes & context", hint: "Journal entries and short context excerpts from them" },
  { id: "group", label: "Groups", hint: "Circles / teams" },
] as const;

const LINK_BUCKET_LABEL: Record<string, string> = {
  person: "People",
  situation: "Situations",
  place: "Places",
  idea: "Ideas & topics",
  group: "Groups",
  tag: "Tags & activity types",
  day: "Dates",
  other: "Other",
};

const LINK_BUCKET_ORDER = ["person", "situation", "place", "idea", "group", "tag", "day", "other"];

type WizardStep = "category" | "pick_entity" | "pick_exploration" | "blocked" | "content";

function formatEntityOption(e: EntityRef): string {
  switch (e.type) {
    case "E55_Type":
      return `${e.name} · feeling or tag`;
    case "E21_Person":
      return `${e.name} · person`;
    case "E7_Activity":
      return `${e.name} · situation`;
    case "E53_Place":
      return `${e.name} · place`;
    case "E28_Conceptual_Object":
      return `${e.name} · idea / topic`;
    case "E52_Time_Span":
      return `${e.name} · day`;
    case "E73_Information_Object":
      if (e.note_role === "journal") return `${e.name} · journal entry`;
      if (e.note_role === "context") return `${e.name} · context excerpt`;
      return `${e.name} · note`;
    case "E74_Group":
      return `${e.name} · group`;
    default:
      return `${e.name} · ${e.type}`;
  }
}

function overviewToMoments(o: Overview): NormalizedMoment[] {
  if (o.kind === "Feeling") {
    return o.occurrences.map((occ) => ({
      id: occ.assignment_key || `${occ.entry_id}-${occ.input_time}`,
      flavor: "feeling",
      step1Label: "What you recorded",
      tagBadge: o.name,
      recordTitle: occ.assignment_label,
      day: occ.day,
      time: occ.input_time,
      entryPreview: occ.entry_preview,
      entryId: occ.entry_id,
      eventKey: occ.event_key,
      activityName: occ.activity_name,
      activityKind: occ.activity_kind,
      activityDay: occ.activity_day,
      persons: occ.persons || [],
    }));
  }
  if (o.kind === "Person") {
    return (o.items || []).map((it, i) => ({
      id: `${it.entry_id}-${i}`,
      flavor: "journal",
      step1Label: "Journal moment",
      recordTitle: (it.activity_name || "").trim() || it.event_type || "Appearance in your notes",
      day: it.day || "",
      time: it.input_time || "",
      entryPreview: (it.text_preview || "").slice(0, 140),
      entryId: it.entry_id,
      eventKey: it.event_key || "",
      activityName: (it.activity_name || "").trim(),
      activityKind: it.event_type || "",
      activityDay: it.day || "",
      persons: [],
    }));
  }
  return [];
}

function overviewToHub(o: Overview): HubData | null {
  if (o.kind === "Event") {
    const placeLens = o.ref.startsWith("E53_Place:");
    const ev = o as OverviewEvent;
    const title =
      (ev.activity_name || "").trim() ||
      (placeLens ? `Notes linked to ${(ev.places || [])[0] || "this place"}` : "Situation");
    const parts = [o.event_type, o.day, (o.places || []).join(", ")].filter(Boolean);
    const summaryText = (ev.summary_preview || "").trim();
    return {
      kind: "situation",
      placeLens,
      summaryText,
      ref: o.ref,
      title,
      subtitle: parts.join(" · "),
      day: o.day || "",
      places: o.places || [],
      eventType: o.event_type || "",
      persons: o.persons || [],
      users: o.users || [],
      entries: o.entries || [],
    };
  }
  if (o.kind === "E73_Information_Object") {
    const isJournal = (o.entry_kind || "").trim() === "journal_entry";
    return {
      kind: "context",
      anchorKind: isJournal ? "journal" : "context",
      name: o.name || (isJournal ? "Journal" : "Context"),
      day: o.day || "",
      eventType: o.event_type || "",
      text: o.text || "",
      linked: o.linked || [],
      topics: (o.topics || []).map((t) => ({ name: t.name })),
      concepts: (o.concepts || []).map((c) => ({ name: c.name })),
      mentions: (o.mentions || []).map((m) => ({ name: m.name })),
      entries: o.entries || [],
    };
  }
  return null;
}

function exploreKindLabel(o: Overview | null): string {
  if (!o) return "";
  switch (o.kind) {
    case "Feeling":
      return "Feeling or tag";
    case "Person":
      return "Person";
    case "Event":
      return o.ref.startsWith("E53_Place:") ? "Place · notes & people" : "Situation";
    case "E73_Information_Object":
      return o.entry_kind === "journal_entry" ? "Journal entry" : "Context excerpt";
    case "Day":
      return "Day";
    default:
      return "";
  }
}

function dayFocusLabel(focus: string): string {
  switch (focus) {
    case "situations":
      return "Situations";
    case "journal":
      return "Journal notes";
    case "people":
      return "People";
    case "feelings":
      return "Feelings & tags";
    default:
      return "Full day";
  }
}

function DayExplorer({
  day,
  onNavigate,
}: {
  day: OverviewDay;
  onNavigate: (ref: string) => void;
}) {
  const [sitFilter, setSitFilter] = useState("");
  const [openNotes, setOpenNotes] = useState(day.focus === "journal");
  const fl = day.focus || "all";
  const showAll = fl === "all";
  const showSit = showAll || fl === "situations";
  const showPeople = showAll || fl === "people";
  const showFeel = showAll || fl === "feelings";
  const showJournal = showAll || fl === "journal";

  const situations = useMemo(() => {
    const q = sitFilter.trim().toLowerCase();
    if (!q) return day.situations;
    return day.situations.filter(
      (s) =>
        s.title.toLowerCase().includes(q) ||
        s.event_type.toLowerCase().includes(q) ||
        s.places.some((p) => p.toLowerCase().includes(q))
    );
  }, [day.situations, sitFilter]);

  const hasAnything =
    day.situations.length > 0 ||
    day.entries.length > 0 ||
    day.persons.length > 0 ||
    day.feeling_tags.length > 0;

  return (
    <div className="mt-4 space-y-5">
      {fl !== "all" ? (
        <div className="rounded-lg border border-zinc-200 bg-zinc-50/80 px-3 py-2 text-[11px] text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-400">
          View: <span className="font-semibold text-zinc-800 dark:text-zinc-200">{dayFocusLabel(fl)}</span>. Use{" "}
          <span className="font-medium">Change view</span> to pick another lens (situations, notes, people, feelings).
        </div>
      ) : (
        <p className="text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
          <span className="font-medium text-zinc-600 dark:text-zinc-300">Direct</span>: situations on this date (P4 time
          span). <span className="font-medium text-zinc-600 dark:text-zinc-300">By proxy</span>: notes written this day or
          linked to those situations; people on those situations; feelings on those notes.
        </p>
      )}

      {!hasAnything ? (
        <div className="rounded-xl border border-zinc-200 bg-zinc-50/80 p-4 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-400">
          Nothing in the graph for this date yet. Try another day or add journal entries that reference activities on
          this day.
        </div>
      ) : null}

      {showSit ? (
        <div className="rounded-2xl border border-sky-200/60 bg-sky-500/5 p-4 dark:border-sky-900/40 dark:bg-sky-950/20">
          <div className="text-[10px] font-medium uppercase tracking-wide text-sky-800/90 dark:text-sky-300/90">
            Situations · direct
          </div>
          {day.situations.length > 0 ? (
            <>
              <input
                value={sitFilter}
                onChange={(e) => setSitFilter(e.target.value)}
                placeholder="Filter situations…"
                className="mt-2 w-full rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-950"
              />
              <div className="mt-3 max-h-[min(40vh,16rem)] space-y-2 overflow-y-auto pr-1">
                {situations.length ? (
                  situations.map((s) => (
                    <button
                      key={s.event_key}
                      type="button"
                      onClick={() => onNavigate(s.ref)}
                      className="flex w-full flex-col items-start gap-0.5 rounded-xl border border-zinc-200 bg-white px-3 py-2 text-left text-sm transition-colors hover:border-sky-400/60 dark:border-zinc-800 dark:bg-zinc-950 dark:hover:border-sky-800"
                    >
                      <span className="font-semibold text-zinc-900 dark:text-zinc-100">{s.title}</span>
                      <span className="text-[11px] text-zinc-500">
                        {[s.event_type, s.places.join(", ")].filter(Boolean).join(" · ") || "Activity"}
                      </span>
                    </button>
                  ))
                ) : (
                  <div className="text-sm text-zinc-500">No situations match this filter.</div>
                )}
              </div>
            </>
          ) : (
            <p className="mt-2 text-sm text-zinc-500">No activities with this day as time span.</p>
          )}
        </div>
      ) : null}

      {showPeople ? (
        <div className="rounded-2xl border border-violet-200/60 bg-violet-500/5 p-4 dark:border-violet-900/40">
          <div className="text-[10px] font-medium uppercase tracking-wide text-violet-800/90 dark:text-violet-300/90">
            People · via situations
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            {day.persons.map((p) => (
              <button
                key={p.id}
                type="button"
                onClick={() => onNavigate(`E21_Person:${p.id}`)}
                className="rounded-full border border-violet-300/70 bg-white px-3 py-1 text-[11px] font-semibold text-violet-900 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200"
              >
                {p.name}
                {p.role ? ` · ${p.role}` : ""}
              </button>
            ))}
            {!day.persons.length ? <span className="text-sm text-zinc-500">No one on cast for this day.</span> : null}
          </div>
        </div>
      ) : null}

      {showFeel && day.feeling_tags.length > 0 ? (
        <div className="rounded-2xl border border-amber-200/60 bg-amber-500/5 p-4 dark:border-amber-900/40">
          <div className="text-[10px] font-medium uppercase tracking-wide text-amber-900/80 dark:text-amber-300/90">
            Feelings & tags · via notes this day
          </div>
          <div className="mt-2 flex flex-wrap gap-2">
            {day.feeling_tags.map((t) => (
              <button
                key={t.ref}
                type="button"
                onClick={() => onNavigate(t.ref)}
                className="rounded-full border border-amber-400/50 bg-white px-3 py-1.5 text-[11px] font-semibold text-amber-950 dark:border-amber-800 dark:bg-zinc-950 dark:text-amber-100"
              >
                {t.name}
                <span className="ml-1 font-normal opacity-70">×{t.count}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {showFeel && !day.feeling_tags.length && (showAll || fl === "feelings") ? (
        <div className="text-sm text-zinc-500">No feeling assignments on notes for this day.</div>
      ) : null}

      {showJournal ? (
        <div className="rounded-2xl border border-zinc-200/90 bg-zinc-50/50 p-4 dark:border-zinc-800 dark:bg-zinc-900/30">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">Journal notes</div>
            {showAll ? (
              <button
                type="button"
                onClick={() => setOpenNotes((v) => !v)}
                className="text-[11px] font-semibold text-zinc-600 underline dark:text-zinc-400"
              >
                {openNotes ? "Hide" : "Show"} list
              </button>
            ) : null}
          </div>
          {(openNotes || !showAll) && day.entries.length ? (
            <div className="mt-3 space-y-2">
              {day.entries.map((e) => (
                <div
                  key={e.entry_id}
                  className="rounded-lg border border-zinc-200 bg-white/90 p-2.5 dark:border-zinc-800 dark:bg-zinc-950/80"
                >
                  <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                  <div className="text-sm text-zinc-800 dark:text-zinc-200">{e.text_preview || ""}</div>
                  {e.event_key ? (
                    <button
                      type="button"
                      onClick={() => onNavigate(`Event:${e.event_key}`)}
                      className="mt-1 text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
                    >
                      Linked situation → {e.activity_name || e.event_type || e.event_key}
                    </button>
                  ) : null}
                </div>
              ))}
            </div>
          ) : null}
          {showJournal && !day.entries.length ? (
            <p className="mt-2 text-sm text-zinc-500">No journal entries for this lens.</p>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}

function PersonFeelingTagsBar({
  tags,
  personRef,
  onPickTag,
}: {
  tags: PersonFeelingTag[];
  personRef: string;
  onPickTag: (tagRef: string, personRef: string) => void;
}) {
  if (!tags.length) return null;
  return (
    <div className="mt-4 rounded-xl border border-amber-200/60 bg-amber-500/5 p-4 dark:border-amber-900/40 dark:bg-amber-950/20">
      <div className="text-[10px] font-medium uppercase tracking-wide text-amber-900/80 dark:text-amber-300/90">
        Go deeper · feelings and tags with this person
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-zinc-600 dark:text-zinc-400">
        Tags from journal assignments tied to the same situations as this person. Open one to see those moments and who
        else was there.
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        {tags.map((t) => (
          <button
            key={t.ref}
            type="button"
            onClick={() => onPickTag(t.ref, personRef)}
            className="rounded-full border border-amber-400/50 bg-white px-3 py-1.5 text-[11px] font-semibold text-amber-950 shadow-sm transition-colors hover:bg-amber-50 dark:border-amber-800 dark:bg-zinc-950 dark:text-amber-100 dark:hover:bg-amber-950/40"
          >
            {t.name}
            <span className="ml-1 font-normal opacity-70">×{t.count}</span>
          </button>
        ))}
      </div>
    </div>
  );
}

function StepToggle({
  open,
  onClick,
  children,
  variant = "neutral",
}: {
  open: boolean;
  onClick: () => void;
  children: ReactNode;
  variant?: "neutral" | "accent" | "people";
}) {
  const styles = open
    ? "border-zinc-400 bg-zinc-200/80 text-zinc-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
    : variant === "accent"
    ? "border-sky-300/80 bg-sky-500/10 text-sky-900 hover:bg-sky-500/15 dark:border-sky-700 dark:text-sky-200"
    : variant === "people"
    ? "border-violet-300/80 bg-violet-500/10 text-violet-900 hover:bg-violet-500/15 dark:border-violet-700 dark:text-violet-200"
    : "border-zinc-200 bg-white text-zinc-700 hover:bg-zinc-50 dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-200 dark:hover:bg-zinc-900";
  return (
    <button
      type="button"
      onClick={onClick}
      className={["rounded-lg border px-3 py-1.5 text-left text-[11px] font-semibold transition-colors", styles].join(" ")}
    >
      {children}
    </button>
  );
}

function LazyJournalBody({ entryId, visible }: { entryId: string; visible: boolean }) {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!visible || !entryId) return;
    if (text) return;
    let ignore = false;
    setLoading(true);
    setErr("");
    apiGet<{ text?: string }>(`/entry/${encodeURIComponent(entryId)}`)
      .then((d) => {
        if (!ignore) setText(String(d?.text || ""));
      })
      .catch((e) => {
        if (!ignore) setErr(e?.message || String(e));
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [visible, entryId, text]);

  if (!visible) return null;
  if (loading) return <div className="mt-2 animate-pulse text-xs text-zinc-500">Loading note…</div>;
  if (err) return <div className="mt-2 text-xs text-rose-600 dark:text-rose-400">{err}</div>;
  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white/80 p-2.5 text-sm leading-relaxed text-zinc-800 dark:border-zinc-700 dark:bg-zinc-950/50 dark:text-zinc-100">
      {text || "—"}
    </div>
  );
}

function MomentChainCard({
  moment,
  flowKey,
  flow,
  onToggle,
  onNavigate,
}: {
  moment: NormalizedMoment;
  flowKey: string;
  flow: { journal: boolean; situation: boolean; people: boolean };
  onToggle: (key: string, part: "journal" | "situation" | "people") => void;
  onNavigate: (ref: string) => void;
}) {
  const hasSituation = Boolean(moment.eventKey);
  const st = flow;

  return (
    <div className="rounded-2xl border border-zinc-200/90 bg-gradient-to-b from-zinc-50/90 to-white p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
      <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">Step 1 · {moment.step1Label}</div>
      <div className="mt-1 flex flex-wrap items-center justify-between gap-2 text-xs text-zinc-500">
        <span>{moment.day || moment.time || "—"}</span>
        {moment.tagBadge ? (
          <span className="rounded-full bg-zinc-200/80 px-2 py-0.5 text-[10px] font-semibold text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200">
            {moment.tagBadge}
          </span>
        ) : null}
      </div>
      <div className="mt-1 text-sm font-semibold text-zinc-900 dark:text-zinc-100">{moment.recordTitle}</div>
      {moment.entryPreview ? (
        <div className="mt-1 text-[11px] italic text-zinc-400 dark:text-zinc-500">
          Hint · {moment.entryPreview}
          {moment.entryPreview.length >= 140 ? "…" : ""}
        </div>
      ) : null}

      <div className="mt-3 flex flex-wrap gap-2">
        {moment.entryId ? (
          <StepToggle open={st.journal} onClick={() => onToggle(flowKey, "journal")} variant="neutral">
            {st.journal ? "Hide full note ↑" : "Read full journal note →"}
          </StepToggle>
        ) : null}
        {hasSituation ? (
          <StepToggle open={st.situation} onClick={() => onToggle(flowKey, "situation")} variant="accent">
            {st.situation ? "Hide situation ↑" : "Open situation →"}
          </StepToggle>
        ) : (
          <span className="text-[11px] text-zinc-500">No linked situation in the graph for this moment.</span>
        )}
      </div>

      <LazyJournalBody entryId={moment.entryId} visible={st.journal} />

      {st.situation && hasSituation ? (
        <div className="mt-3 rounded-xl border border-sky-200/60 bg-sky-500/5 p-3 dark:border-sky-900/40 dark:bg-sky-500/10">
          <div className="text-[10px] font-medium uppercase tracking-wide text-sky-800/80 dark:text-sky-300/90">Step 2 · Situation</div>
          <div className="mt-1 text-base font-semibold text-zinc-900 dark:text-zinc-50">
            {moment.activityName || "Unnamed situation"}
          </div>
          {moment.activityKind ? (
            <div className="mt-0.5 text-[11px] text-zinc-500">
              Kind · <span className="text-zinc-700 dark:text-zinc-300">{moment.activityKind}</span>
            </div>
          ) : null}
          <div className="mt-1 font-mono text-[10px] text-zinc-500">
            {[moment.activityDay, moment.time].filter(Boolean).join(" · ")}
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => onNavigate(`Event:${moment.eventKey}`)}
              className="rounded-lg border border-sky-400/60 bg-white px-3 py-1.5 text-[11px] font-semibold text-sky-900 hover:bg-sky-50 dark:border-sky-700 dark:bg-zinc-950 dark:text-sky-200 dark:hover:bg-sky-950/30"
            >
              Focus this situation (details & cast) →
            </button>
            {moment.persons.length ? (
              <StepToggle open={st.people} onClick={() => onToggle(flowKey, "people")} variant="people">
                {st.people ? "Hide people ↑" : "People here →"}
              </StepToggle>
            ) : null}
          </div>
        </div>
      ) : null}

      {st.situation && st.people && hasSituation && moment.persons.length ? (
        <div className="mt-3 rounded-xl border border-violet-200/60 bg-violet-500/5 p-3 dark:border-violet-900/40 dark:bg-violet-500/10">
          <div className="text-[10px] font-medium uppercase tracking-wide text-violet-800/80 dark:text-violet-300/90">Step 3 · People</div>
          <div className="mt-2 flex flex-wrap gap-2">
            {moment.persons.map((p) => (
              <button
                key={p.id}
                type="button"
                onClick={() => onNavigate(`E21_Person:${p.id}`)}
                className="rounded-full border border-violet-300/70 bg-white px-3 py-1 text-[11px] font-semibold text-violet-900 transition-colors hover:bg-violet-50 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200 dark:hover:bg-violet-950/40"
              >
                {p.name}
                {p.role ? ` · ${p.role}` : ""}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {st.situation && hasSituation && !moment.persons.length ? (
        <p className="mt-2 text-[11px] text-zinc-500">
          Cast not listed on this card — use <span className="font-medium">Focus this situation</span> to see everyone linked there.
        </p>
      ) : null}
    </div>
  );
}

function HubExplorer({
  hub,
  onNavigate,
}: {
  hub: HubData;
  onNavigate: (ref: string) => void;
}) {
  const [openPeople, setOpenPeople] = useState(false);
  const [openNotes, setOpenNotes] = useState(false);
  const [openRelated, setOpenRelated] = useState(true);
  const [openEntries, setOpenEntries] = useState(false);

  const contextLinkGroups = useMemo(() => {
    if (hub.kind !== "context") return [];
    const m = new Map<string, JournalLink[]>();
    for (const L of hub.linked) {
      const b = (L.bucket || "other").trim() || "other";
      if (!m.has(b)) m.set(b, []);
      m.get(b)!.push(L);
    }
    for (const arr of m.values()) {
      arr.sort((a, b) => a.name.localeCompare(b.name));
    }
    return LINK_BUCKET_ORDER.filter((b) => (m.get(b) || []).length > 0).map((b) => [b, m.get(b)!] as const);
  }, [hub]);

  if (hub.kind === "situation") {
    return (
      <div className="mt-4 space-y-3">
        <div className="rounded-2xl border border-zinc-200/90 bg-gradient-to-b from-zinc-50/90 to-white p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
          <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">
            {hub.placeLens ? "Anchor · Place" : "Anchor · Situation"}
          </div>
          <div className="mt-1 text-lg font-semibold text-zinc-900 dark:text-zinc-50">{hub.title}</div>
          {hub.subtitle ? <div className="mt-1 text-xs text-zinc-500">{hub.subtitle}</div> : null}
          {hub.summaryText ? (
            <div className="mt-2 text-sm leading-relaxed text-zinc-600 dark:text-zinc-300">{hub.summaryText}</div>
          ) : hub.placeLens ? (
            <div className="mt-2 text-xs text-zinc-500">
              No journal text preview yet for this place lens — expand <span className="font-medium">Journal notes</span> below if
              entries are linked.
            </div>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-2">
          <StepToggle open={openPeople} onClick={() => setOpenPeople((v) => !v)} variant="people">
            {openPeople ? "Hide people ↑" : "People involved →"}
          </StepToggle>
          <StepToggle open={openNotes} onClick={() => setOpenNotes((v) => !v)} variant="neutral">
            {openNotes ? "Hide journal notes ↑" : "Journal notes →"}
          </StepToggle>
        </div>

        {openPeople ? (
          <div className="rounded-xl border border-violet-200/60 bg-violet-500/5 p-3 dark:border-violet-900/40">
            <div className="flex flex-wrap gap-2">
              {hub.persons.map((p) => (
                <button
                  key={p.id}
                  type="button"
                  onClick={() => onNavigate(`E21_Person:${p.id}`)}
                  className="rounded-full border border-violet-300/70 bg-white px-3 py-1 text-[11px] font-semibold text-violet-900 hover:bg-violet-50 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200"
                >
                  {p.name}
                </button>
              ))}
              {hub.users.map((u, i) => (
                <span
                  key={`${u.name}-${i}`}
                  className="rounded-full border border-zinc-200 px-3 py-1 text-[11px] text-zinc-600 dark:border-zinc-700 dark:text-zinc-400"
                >
                  {u.name}
                </span>
              ))}
              {!hub.persons.length && !hub.users.length ? (
                <div className="text-sm text-zinc-500">No one linked here.</div>
              ) : null}
            </div>
          </div>
        ) : null}

        {openNotes ? (
          <div className="space-y-2 rounded-xl border border-zinc-200 bg-zinc-50/80 p-3 dark:border-zinc-800 dark:bg-zinc-900/40">
            {hub.entries.map((e) => (
              <div key={e.entry_id} className="text-sm text-zinc-700 dark:text-zinc-200">
                <div className="text-[11px] text-zinc-500">{e.input_time || e.day || ""}</div>
                <div>{e.text_preview || ""}</div>
              </div>
            ))}
            {!hub.entries.length ? <div className="text-sm text-zinc-500">No notes found.</div> : null}
          </div>
        ) : null}
      </div>
    );
  }

  /* context hub */
  return (
    <div className="mt-4 space-y-3">
      <div className="rounded-2xl border border-zinc-200/90 bg-gradient-to-b from-zinc-50/90 to-white p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
        <div className="text-[10px] font-medium uppercase tracking-wide text-zinc-500">
          {hub.anchorKind === "journal" ? "Anchor · Journal entry" : "Anchor · Context excerpt"}
        </div>
        <div className="mt-1 text-lg font-semibold text-zinc-900 dark:text-zinc-50">{hub.name}</div>
        <div className="mt-1 flex flex-wrap gap-2 text-xs text-zinc-500">
          {hub.day ? <span>{hub.day}</span> : null}
          {hub.eventType ? <span>{hub.eventType}</span> : null}
        </div>
        {hub.text ? <div className="mt-2 text-sm text-zinc-600 dark:text-zinc-300">{hub.text}</div> : null}
      </div>

      <div className="flex flex-wrap gap-2">
        <StepToggle open={openRelated} onClick={() => setOpenRelated((v) => !v)} variant="accent">
          {openRelated ? "Hide linked entities ↑" : "Linked entities →"}
        </StepToggle>
        <StepToggle open={openEntries} onClick={() => setOpenEntries((v) => !v)} variant="neutral">
          {openEntries ? "Hide related journal notes ↑" : "Related journal notes →"}
        </StepToggle>
      </div>

      {openRelated ? (
        <div className="space-y-4 rounded-xl border border-sky-200/50 bg-sky-500/5 p-3 text-sm dark:border-sky-900/40">
          {contextLinkGroups.length ? (
            contextLinkGroups.map(([bucket, items]) => (
              <div key={bucket}>
                <div className="text-[10px] font-semibold uppercase tracking-wide text-sky-900/80 dark:text-sky-300/90">
                  {LINK_BUCKET_LABEL[bucket] || bucket}
                </div>
                <div className="mt-2 flex flex-wrap gap-2">
                  {items.map((L) => (
                    <button
                      key={`${L.ref}-${L.ref_type}-${L.source}`}
                      type="button"
                      onClick={() => onNavigate(L.ref)}
                      className="flex max-w-full flex-col items-start rounded-lg border border-sky-300/60 bg-white px-2.5 py-1.5 text-left text-[11px] font-semibold text-sky-950 hover:bg-sky-50 dark:border-sky-800 dark:bg-zinc-950 dark:text-sky-100 dark:hover:bg-sky-950/40"
                    >
                      <span className="truncate">{L.name}</span>
                      <span className="mt-0.5 font-normal text-[10px] text-zinc-500">
                        {L.ref_type || "link"}
                        {L.source === "situation" ? " · via situation" : ""}
                      </span>
                    </button>
                  ))}
                </div>
              </div>
            ))
          ) : (
            <div className="text-zinc-500">
              No people, situations, or topics are linked from this node yet (no matching{" "}
              <span className="font-mono text-[10px]">P67_refers_to</span> edges in the graph).
            </div>
          )}
        </div>
      ) : null}

      {openEntries ? (
        <div className="space-y-2 rounded-xl border border-zinc-200 bg-zinc-50/80 p-3 dark:border-zinc-800 dark:bg-zinc-900/40">
          {hub.entries.map((e) => (
            <div key={e.entry_id} className="text-sm text-zinc-700 dark:text-zinc-200">
              <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
              <div>{e.text_preview || ""}</div>
            </div>
          ))}
          {!hub.entries.length ? <div className="text-sm text-zinc-500">No notes found.</div> : null}
        </div>
      ) : null}
    </div>
  );
}

function StepRail({
  step,
  categoryLabel,
  entityLabel,
}: {
  step: WizardStep;
  categoryLabel: string;
  entityLabel: string;
}) {
  const phases = [
    { key: "category", label: "Type", done: step !== "category" },
    { key: "entity", label: "Item", done: step === "pick_exploration" || step === "content" || step === "blocked" },
    { key: "explore", label: "View", done: step === "content" },
  ];
  return (
    <div className="mb-4 flex flex-wrap items-center gap-2 border-b border-zinc-200 pb-3 text-[11px] dark:border-zinc-800">
      {phases.map((p, i) => (
        <div key={p.key} className="flex items-center gap-2">
          {i > 0 ? <span className="text-zinc-400">→</span> : null}
          <span
            className={[
              "rounded-full px-2 py-0.5 font-semibold",
              p.done ? "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300" : "bg-zinc-100 text-zinc-500 dark:bg-zinc-800",
            ].join(" ")}
          >
            {p.label}
          </span>
        </div>
      ))}
      {categoryLabel ? (
        <span className="ml-auto truncate text-zinc-500">
          {categoryLabel}
          {entityLabel ? ` · ${entityLabel}` : ""}
        </span>
      ) : null}
    </div>
  );
}

export function EntityTimeline() {
  const [wizardStep, setWizardStep] = useState<WizardStep>("category");
  const [categoryId, setCategoryId] = useState<string>("");
  const [categoryLabel, setCategoryLabel] = useState<string>("");

  const [entityList, setEntityList] = useState<EntityRef[]>([]);
  const [entityLoading, setEntityLoading] = useState(false);
  const [entityError, setEntityError] = useState<string>("");
  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [entityFetchNonce, setEntityFetchNonce] = useState(0);

  const [selectedRef, setSelectedRef] = useState<string>("");
  const [selectedDisplayName, setSelectedDisplayName] = useState<string>("");

  const [navOptions, setNavOptions] = useState<NavResponse | null>(null);
  const [navLoading, setNavLoading] = useState(false);
  const [navError, setNavError] = useState<string>("");

  const [overview, setOverview] = useState<Overview | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewError, setOverviewError] = useState<string>("");

  const [momentFlow, setMomentFlow] = useState<Record<string, { journal: boolean; situation: boolean; people: boolean }>>(
    {}
  );

  /** When opening a feeling tag from a person card, scope overview + nav counts to that person. */
  const feelingAnchorPersonRef = useRef<string | null>(null);
  /** Nav option key passed to /entity/overview as focus= (e.g. day lenses: all, situations, journal). */
  const overviewNavFocusRef = useRef<string | null>(null);

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedSearch(searchInput.trim()), 350);
    return () => clearTimeout(t);
  }, [searchInput]);

  const toggleMoment = useCallback((key: string, part: "journal" | "situation" | "people") => {
    setMomentFlow((prev) => {
      const cur = prev[key] || { ...DEFAULT_MOMENT_PANEL_FLOW };
      return { ...prev, [key]: { ...cur, [part]: !cur[part] } };
    });
  }, []);

  const loadOverview = useCallback(async (ref: string): Promise<boolean> => {
    setOverviewLoading(true);
    setOverviewError("");
    setOverview(null);
    setMomentFlow({});
    const ap = feelingAnchorPersonRef.current;
    const anchorQ =
      ap && ap.includes(":") ? `&anchor_person=${encodeURIComponent(ap)}` : "";
    const fk = overviewNavFocusRef.current;
    const focusQ = fk ? `&focus=${encodeURIComponent(fk)}` : "";
    try {
      const data = await apiGet<Overview>(
        `/entity/overview?ref=${encodeURIComponent(ref)}&limit=120${anchorQ}${focusQ}`
      );
      setOverview(data);
      return true;
    } catch (e: unknown) {
      setOverviewError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setOverviewLoading(false);
    }
  }, []);

  const runNavThenMaybeOverview = useCallback(
    async (ref: string) => {
      const r = (ref || "").trim();
      if (!r || r === "null" || r === "undefined") {
        setNavError("Missing item reference — pick another row or refresh the list.");
        return;
      }
      setNavLoading(true);
      setNavError("");
      setNavOptions(null);
      setOverview(null);
      setOverviewError("");
      const ap = feelingAnchorPersonRef.current;
      const anchorQ =
        ap && ap.includes(":") ? `&anchor_person=${encodeURIComponent(ap)}` : "";
      try {
        const nav = await apiGet<NavResponse>(
          `/entity/nav-options?ref=${encodeURIComponent(r)}${anchorQ}`
        );
        setNavOptions(nav);
        setSelectedDisplayName(nav.display_name || r);
        const enabled = (nav.options || []).filter((o) => o.enabled);
        if (enabled.length === 1) {
          overviewNavFocusRef.current = enabled[0].key;
          await loadOverview(r);
          setWizardStep("content");
        } else if (enabled.length === 0) {
          setWizardStep("blocked");
        } else {
          setWizardStep("pick_exploration");
        }
      } catch (e: unknown) {
        setNavError(e instanceof Error ? e.message : String(e));
        setWizardStep("pick_entity");
      } finally {
        setNavLoading(false);
      }
    },
    [loadOverview]
  );

  const jumpToEntity = useCallback(
    async (ref: string | null | undefined, opts?: { anchorPerson?: string | null }) => {
      const r = (ref || "").trim();
      if (!r || r === "null" || r === "undefined") {
        setNavError("This row has no graph reference (missing entry id). Try refreshing the list.");
        return;
      }
      if (opts?.anchorPerson !== undefined && opts.anchorPerson) {
        feelingAnchorPersonRef.current = opts.anchorPerson;
      } else {
        feelingAnchorPersonRef.current = null;
      }
      overviewNavFocusRef.current = null;
      setSelectedRef(r);
      await runNavThenMaybeOverview(r);
    },
    [runNavThenMaybeOverview]
  );

  useEffect(() => {
    if (wizardStep !== "pick_entity" || !categoryId) return;
    let ignore = false;
    setEntityLoading(true);
    setEntityError("");
    const q = encodeURIComponent(debouncedSearch);
    const cat = encodeURIComponent(categoryId);
    apiGet<{ items: EntityRef[] }>(`/entities?category=${cat}&query=${q}&limit=150`)
      .then((out) => {
        if (!ignore) setEntityList(out.items || []);
      })
      .catch((e: unknown) => {
        if (!ignore) setEntityError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!ignore) setEntityLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [wizardStep, categoryId, debouncedSearch, entityFetchNonce]);

  const startCategory = useCallback((id: string, label: string) => {
    setCategoryId(id);
    setCategoryLabel(label);
    setSearchInput("");
    setDebouncedSearch("");
    setEntityList([]);
    setSelectedRef("");
    setSelectedDisplayName("");
    setNavOptions(null);
    setNavError("");
    setOverview(null);
    setOverviewError("");
    setEntityFetchNonce(0);
    feelingAnchorPersonRef.current = null;
    overviewNavFocusRef.current = null;
    setWizardStep("pick_entity");
  }, []);

  const restartWizard = useCallback(() => {
    feelingAnchorPersonRef.current = null;
    overviewNavFocusRef.current = null;
    setWizardStep("category");
    setCategoryId("");
    setCategoryLabel("");
    setEntityList([]);
    setSearchInput("");
    setSelectedRef("");
    setSelectedDisplayName("");
    setNavOptions(null);
    setNavError("");
    setOverview(null);
    setOverviewError("");
    setEntityFetchNonce(0);
  }, []);

  const moments = useMemo(() => (overview ? overviewToMoments(overview) : []), [overview]);
  const hub = useMemo(() => (overview ? overviewToHub(overview) : null), [overview]);

  const contentHeader = useMemo(() => {
    if (!overview) return "Details";
    const kind = exploreKindLabel(overview);
    if (overview.kind === "Person") return `${overview.name} · ${kind}`;
    if (overview.kind === "Feeling") return `${overview.name} · ${kind}`;
    if (overview.kind === "Day") return `${overview.day} · ${dayFocusLabel(overview.focus)}`;
    if (overview.kind === "E73_Information_Object") return `${overview.name || "Context"} · ${kind}`;
    const ev = overview as OverviewEvent;
    return `${(ev.activity_name || "").trim() || "Situation"} · ${kind}`;
  }, [overview]);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-zinc-800 dark:text-zinc-100">Linked explorer</div>
        <button
          type="button"
          onClick={restartWizard}
          className="rounded-lg border border-zinc-200 px-2 py-1 text-[11px] font-semibold text-zinc-600 hover:bg-zinc-50 dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-900"
        >
          Start over
        </button>
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
        Three steps: pick a <span className="font-medium text-zinc-600 dark:text-zinc-300">category</span>, then an{" "}
        <span className="font-medium text-zinc-600 dark:text-zinc-300">item</span>, then how to{" "}
        <span className="font-medium text-zinc-600 dark:text-zinc-300">view</span> it. If the graph has no links, we tell
        you instead of failing silently.
      </p>

      <div className="mt-3 rounded-2xl border border-zinc-200 bg-white p-5 dark:border-zinc-800 dark:bg-zinc-950">
        <StepRail step={wizardStep} categoryLabel={categoryLabel} entityLabel={selectedDisplayName} />

        {wizardStep === "category" ? (
          <div>
            <div className="text-xs font-semibold text-zinc-500">1 · What kind of thing?</div>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              {EXPLORER_CATEGORIES.map((c) => (
                <button
                  key={c.id}
                  type="button"
                  onClick={() => startCategory(c.id, c.label)}
                  className="rounded-xl border border-zinc-200 bg-zinc-50/80 p-3 text-left transition-colors hover:border-sky-300/60 hover:bg-sky-500/5 dark:border-zinc-800 dark:bg-zinc-900/40 dark:hover:border-sky-800"
                >
                  <div className="text-sm font-semibold text-zinc-900 dark:text-zinc-50">{c.label}</div>
                  <div className="mt-0.5 text-[11px] text-zinc-500">{c.hint}</div>
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {wizardStep === "pick_entity" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">2 · Pick {categoryLabel.toLowerCase()}</div>
              <button
                type="button"
                onClick={() => {
                  setWizardStep("category");
                  setNavError("");
                }}
                className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
              >
                ← Change type
              </button>
            </div>
            <input
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              placeholder="Filter by name…"
              className="mt-2 w-full rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-950"
            />
            {entityLoading ? (
              <div className="mt-4 animate-pulse text-sm text-zinc-500">Loading list…</div>
            ) : null}
            {navError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                Couldn&apos;t check how to open this item: {navError}
                {selectedRef ? (
                  <button
                    type="button"
                    className="ml-2 font-semibold underline"
                    onClick={() => void runNavThenMaybeOverview(selectedRef)}
                  >
                    Retry
                  </button>
                ) : null}
                <button
                  type="button"
                  className="ml-2 font-semibold text-zinc-600 underline dark:text-zinc-400"
                  onClick={() => setNavError("")}
                >
                  Dismiss
                </button>
              </div>
            ) : null}
            {entityError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                {entityError}
                <button
                  type="button"
                  className="ml-2 font-semibold underline"
                  onClick={() => setEntityFetchNonce((n) => n + 1)}
                >
                  Retry
                </button>
              </div>
            ) : null}
            {!entityLoading && !entityError && !entityList.length ? (
              <div className="mt-4 text-sm text-zinc-500">No matches. Try another filter or category.</div>
            ) : null}
            <div className="mt-3 max-h-[min(50vh,22rem)] space-y-1 overflow-y-auto pr-1">
              {entityList.map((e, i) => (
                <button
                  key={e.ref?.trim() ? e.ref : `${e.type}-${i}-${e.name.slice(0, 32)}`}
                  type="button"
                  onClick={() => void jumpToEntity(e.ref)}
                  disabled={navLoading || !e.ref?.trim()}
                  className="flex w-full items-center justify-between gap-2 rounded-lg border border-zinc-100 bg-zinc-50/50 px-3 py-2 text-left text-sm hover:border-zinc-300 hover:bg-white disabled:opacity-50 dark:border-zinc-800 dark:bg-zinc-900/30 dark:hover:bg-zinc-900"
                >
                  <span className="font-medium text-zinc-900 dark:text-zinc-100">{e.name}</span>
                  <span className="shrink-0 text-[10px] text-zinc-400">{formatEntityOption(e)}</span>
                </button>
              ))}
            </div>
            {navLoading ? <div className="mt-2 text-[11px] text-zinc-500">Checking what we can open…</div> : null}
          </div>
        ) : null}

        {wizardStep === "pick_exploration" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">3 · How do you want to explore?</div>
              <button
                type="button"
                onClick={() => {
                  setWizardStep("pick_entity");
                  setNavOptions(null);
                  setNavError("");
                }}
                className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
              >
                ← Other item
              </button>
            </div>
            <div className="mt-1 text-sm text-zinc-700 dark:text-zinc-200">
              <span className="font-semibold">{selectedDisplayName || selectedRef}</span>
            </div>
            {navError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                {navError}
                <button type="button" className="ml-2 font-semibold underline" onClick={() => void runNavThenMaybeOverview(selectedRef)}>
                  Retry
                </button>
              </div>
            ) : null}
            <div className="mt-3 space-y-2">
              {(navOptions?.options || []).map((opt) => (
                <button
                  key={opt.key}
                  type="button"
                  disabled={!opt.enabled || overviewLoading}
                  onClick={() =>
                    void (async () => {
                      overviewNavFocusRef.current = opt.key;
                      const ok = await loadOverview(selectedRef);
                      if (ok) setWizardStep("content");
                    })()
                  }
                  className={[
                    "w-full rounded-xl border p-3 text-left transition-colors",
                    opt.enabled
                      ? "border-zinc-200 bg-white hover:border-sky-300/70 hover:bg-sky-500/5 dark:border-zinc-800 dark:bg-zinc-950 dark:hover:border-sky-800"
                      : "cursor-not-allowed border-zinc-100 bg-zinc-50/50 opacity-60 dark:border-zinc-800/60",
                  ].join(" ")}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-sm font-semibold text-zinc-900 dark:text-zinc-50">{opt.title}</span>
                    <span className="text-[11px] text-zinc-500">{opt.count} link{opt.count === 1 ? "" : "s"}</span>
                  </div>
                  <div className="mt-1 text-[11px] text-zinc-500">{opt.description}</div>
                  {!opt.enabled ? (
                    <div className="mt-2 text-[11px] font-medium text-amber-700 dark:text-amber-300">Nothing here yet in your graph.</div>
                  ) : null}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {wizardStep === "blocked" ? (
          <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-900 dark:text-amber-100">
            <div className="font-semibold">No exploration path for this item yet</div>
            <p className="mt-1 text-[13px] opacity-90">
              The graph doesn&apos;t have journal links for <span className="font-medium">{selectedDisplayName}</span>. Try another
              entry or add more notes.
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => {
                  setWizardStep("pick_entity");
                  setNavOptions(null);
                }}
                className="rounded-lg border border-amber-600/40 px-3 py-1.5 text-[11px] font-semibold"
              >
                Pick another item
              </button>
              <button type="button" onClick={restartWizard} className="rounded-lg border border-zinc-300 px-3 py-1.5 text-[11px] font-semibold dark:border-zinc-600">
                Start over
              </button>
            </div>
          </div>
        ) : null}

        {wizardStep === "content" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">Details</div>
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => {
                    setWizardStep("pick_exploration");
                    setOverview(null);
                  }}
                  className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
                >
                  ← Change view
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setWizardStep("pick_entity");
                    setOverview(null);
                    setOverviewError("");
                    setNavOptions(null);
                    setNavError("");
                    setSelectedRef("");
                    setSelectedDisplayName("");
                  }}
                  className="text-[11px] font-semibold text-zinc-500 hover:underline"
                >
                  ← Other item
                </button>
              </div>
            </div>

            {overviewLoading ? <div className="mt-4 animate-pulse text-sm text-zinc-500">Loading details…</div> : null}

            {overviewError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                {overviewError}
                <button type="button" className="ml-2 font-semibold underline" onClick={() => void loadOverview(selectedRef)}>
                  Retry
                </button>
              </div>
            ) : null}

            {!overviewLoading && overview ? (
              <>
                <div className="mt-3 flex flex-wrap items-baseline justify-between gap-2">
                  <div className="text-sm font-semibold text-zinc-900 dark:text-zinc-50">{contentHeader}</div>
                  <span className="rounded-full bg-zinc-100 px-2 py-0.5 text-[10px] font-medium text-zinc-600 dark:bg-zinc-800 dark:text-zinc-400">
                    {exploreKindLabel(overview)}
                  </span>
                </div>

                {overview.kind === "Person" && overview.feeling_tags && overview.feeling_tags.length > 0 ? (
                  <PersonFeelingTagsBar
                    personRef={selectedRef}
                    tags={overview.feeling_tags}
                    onPickTag={(tagRef, personRef) => void jumpToEntity(tagRef, { anchorPerson: personRef })}
                  />
                ) : null}

                {overview.kind === "Feeling" && overview.anchor_person_name ? (
                  <div className="mt-3 rounded-lg border border-sky-500/25 bg-sky-500/5 px-3 py-2 text-[11px] text-sky-950 dark:border-sky-900/40 dark:bg-sky-950/30 dark:text-sky-100">
                    Scoped to <span className="font-semibold">{overview.anchor_person_name}</span> — only notes where
                    this tag is assigned to them or tied to a situation they are in.{" "}
                    {overview.anchor_person_ref ? (
                      <button
                        type="button"
                        className="font-semibold underline"
                        onClick={() => void jumpToEntity(overview.anchor_person_ref!)}
                      >
                        Back to person
                      </button>
                    ) : null}
                  </div>
                ) : null}

                {moments.length ? (
                  <div className="mt-4 space-y-4">
                    {moments.map((m) => {
                      const fk = m.id;
                      const flow = momentFlow[fk] || { ...DEFAULT_MOMENT_PANEL_FLOW };
                      return (
                        <MomentChainCard
                          key={fk}
                          moment={m}
                          flowKey={fk}
                          flow={flow}
                          onToggle={toggleMoment}
                          onNavigate={(ref) => void jumpToEntity(ref)}
                        />
                      );
                    })}
                  </div>
                ) : null}

                {hub ? <HubExplorer hub={hub} onNavigate={(ref) => void jumpToEntity(ref)} /> : null}

                {overview.kind === "Day" ? (
                  <DayExplorer day={overview} onNavigate={(ref) => void jumpToEntity(ref)} />
                ) : null}

                {!moments.length && !hub && overview.kind !== "Day" ? (
                  overview.kind === "Person" ? (
                    <div className="mt-4 rounded-xl border border-zinc-200 bg-zinc-50/80 p-4 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
                      <p className="font-medium text-zinc-800 dark:text-zinc-100">No timeline rows yet</p>
                      <p className="mt-1 text-[13px] leading-relaxed">
                        Notes show up here when they are linked to a situation that lists this person as a participant
                        (Activity → person). The person can still exist in your graph from mentions or aliases.
                      </p>
                      {typeof overview.mentions === "number" && overview.mentions > 0 ? (
                        <p className="mt-2 text-[13px] text-zinc-500 dark:text-zinc-400">
                          Recorded mentions in the graph: {overview.mentions}
                        </p>
                      ) : null}
                    </div>
                  ) : (
                    <div className="mt-4 text-sm text-zinc-500">Overview loaded but there is nothing to show for this view.</div>
                  )
                ) : null}
              </>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
