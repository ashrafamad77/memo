"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";
import {
  DEFAULT_MOMENT_PANEL_FLOW,
  LINK_BUCKET_LABEL,
  LINK_BUCKET_ORDER,
  type HubData,
  type JournalLink,
  type NormalizedMoment,
  type Overview,
  type OverviewDay,
  type PersonFeelingTag,
  type WizardStep,
  dayFocusLabel,
  exploreKindLabel,
} from "@/lib/linkedExplorer/model";

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
        <div className="rounded-lg border border-lt-border bg-zinc-50/80 px-3 py-2 text-[11px] text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-400">
          View: <span className="font-semibold text-lt-textSecondary dark:text-zinc-200">{dayFocusLabel(fl)}</span>. Use{" "}
          <span className="font-medium">Change view</span> to pick another lens (situations, notes, people, feelings).
        </div>
      ) : (
        <p className="text-[11px] leading-relaxed text-lt-textMuted dark:text-zinc-400">
          <span className="font-medium text-lt-textMuted dark:text-zinc-300">Direct</span>: situations on this date (P4 time
          span). <span className="font-medium text-lt-textMuted dark:text-zinc-300">By proxy</span>: notes written this day or
          linked to those situations; people on those situations; feelings on those notes.
        </p>
      )}

      {!hasAnything ? (
        <div className="rounded-xl border border-lt-border bg-zinc-50/80 p-4 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-400">
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
                className="mt-2 w-full rounded-lg border border-lt-border bg-lt-surface px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-950"
              />
              <div className="mt-3 max-h-[min(40vh,16rem)] space-y-2 overflow-y-auto pr-1">
                {situations.length ? (
                  situations.map((s) => (
                    <button
                      key={s.event_key}
                      type="button"
                      onClick={() => onNavigate(s.ref)}
                      className="flex w-full flex-col items-start gap-0.5 rounded-xl border border-lt-border bg-lt-surface px-3 py-2 text-left text-sm transition-colors hover:border-sky-400/60 dark:border-zinc-800 dark:bg-zinc-950 dark:hover:border-sky-800"
                    >
                      <span className="font-semibold text-lt-text dark:text-zinc-100">{s.title}</span>
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
                className="rounded-full border border-violet-300/70 bg-lt-surface px-3 py-1 text-[11px] font-semibold text-violet-900 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200"
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
                className="rounded-full border border-amber-400/50 bg-lt-surface px-3 py-1.5 text-[11px] font-semibold text-amber-950 dark:border-amber-800 dark:bg-zinc-950 dark:text-amber-100"
              >
                {t.name}
                <span className="ml-1 font-normal opacity-70">×{t.count}</span>
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {showFeel && !day.feeling_tags.length && (showAll || fl === "feelings") ? (
        <div className="text-sm text-lt-textMuted">No feeling assignments on notes for this day.</div>
      ) : null}

      {showJournal ? (
        <div className="rounded-2xl border border-lt-border/90 bg-lt-raised/70 p-4 dark:border-zinc-800 dark:bg-zinc-900/30">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="text-[10px] font-medium uppercase tracking-wide text-lt-textMuted">Journal notes</div>
            {showAll ? (
              <button
                type="button"
                onClick={() => setOpenNotes((v) => !v)}
                className="text-[11px] font-semibold text-lt-textSecondary underline dark:text-zinc-400"
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
                  className="rounded-lg border border-lt-border bg-lt-surface/90 p-2.5 dark:border-zinc-800 dark:bg-zinc-950/80"
                >
                  <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                  <div className="text-sm text-lt-textSecondary dark:text-zinc-200">{e.text_preview || ""}</div>
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
      <p className="mt-1 text-[11px] leading-relaxed text-lt-textMuted dark:text-zinc-400">
        Tags from journal assignments tied to the same situations as this person. Open one to see those moments and who
        else was there.
      </p>
      <div className="mt-3 flex flex-wrap gap-2">
        {tags.map((t) => (
          <button
            key={t.ref}
            type="button"
            onClick={() => onPickTag(t.ref, personRef)}
            className="rounded-full border border-amber-400/50 bg-lt-surface px-3 py-1.5 text-[11px] font-semibold text-amber-950 shadow-sm transition-colors hover:bg-amber-50 dark:border-amber-800 dark:bg-zinc-950 dark:text-amber-100 dark:hover:bg-amber-950/40"
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
    ? "border-lt-borderStrong bg-lt-accentSoft/90 text-lt-accent dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-100"
    : variant === "accent"
    ? "border-sky-300/80 bg-sky-500/10 text-sky-900 hover:bg-sky-500/15 dark:border-sky-700 dark:text-sky-200"
    : variant === "people"
    ? "border-violet-300/80 bg-violet-500/10 text-violet-900 hover:bg-violet-500/15 dark:border-violet-700 dark:text-violet-200"
    : "border-lt-border bg-lt-surface text-lt-textSecondary hover:bg-lt-muted dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-200 dark:hover:bg-zinc-900";
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

export function LazyJournalBody({ entryId, visible }: { entryId: string; visible: boolean }) {
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
    <div className="mt-2 rounded-lg border border-lt-border bg-lt-surface/80 p-2.5 text-sm leading-relaxed text-lt-textSecondary dark:border-zinc-700 dark:bg-zinc-950/50 dark:text-zinc-100">
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
    <div className="rounded-2xl border border-lt-border/90 bg-gradient-to-b from-lt-washTop via-lt-muted/30 to-lt-surface p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
      <div className="text-[10px] font-medium uppercase tracking-wide text-lt-textMuted">Step 1 · {moment.step1Label}</div>
      <div className="mt-1 flex flex-wrap items-center justify-between gap-2 text-xs text-lt-textMuted">
        <span>{moment.day || moment.time || "—"}</span>
        {moment.tagBadge ? (
          <span className="rounded-full bg-lt-accentSoft/90 px-2 py-0.5 text-[10px] font-semibold text-lt-accent dark:bg-zinc-800 dark:text-zinc-200">
            {moment.tagBadge}
          </span>
        ) : null}
      </div>
      <div className="mt-1 text-sm font-semibold text-lt-text dark:text-zinc-100">{moment.recordTitle}</div>
      {moment.entryPreview ? (
        <div className="mt-1 text-[11px] italic text-lt-textMuted dark:text-zinc-500">
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
          <div className="mt-1 text-base font-semibold text-lt-text dark:text-zinc-50">
            {moment.activityName || "Unnamed situation"}
          </div>
          {moment.activityKind ? (
            <div className="mt-0.5 text-[11px] text-zinc-500">
              Kind · <span className="text-lt-textSecondary dark:text-zinc-300">{moment.activityKind}</span>
            </div>
          ) : null}
          <div className="mt-1 font-mono text-[10px] text-zinc-500">
            {[moment.activityDay, moment.time].filter(Boolean).join(" · ")}
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              onClick={() => onNavigate(`Event:${moment.eventKey}`)}
              className="rounded-lg border border-sky-400/60 bg-lt-surface px-3 py-1.5 text-[11px] font-semibold text-sky-900 hover:bg-sky-50 dark:border-sky-700 dark:bg-zinc-950 dark:text-sky-200 dark:hover:bg-sky-950/30"
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
                className="rounded-full border border-violet-300/70 bg-lt-surface px-3 py-1 text-[11px] font-semibold text-violet-900 transition-colors hover:bg-violet-50 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200 dark:hover:bg-violet-950/40"
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
        <div className="rounded-2xl border border-lt-border/90 bg-gradient-to-b from-lt-washTop via-lt-muted/30 to-lt-surface p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
          <div className="text-[10px] font-medium uppercase tracking-wide text-lt-textMuted">
            {hub.placeLens ? "Anchor · Place" : "Anchor · Situation"}
          </div>
          <div className="mt-1 text-lg font-semibold text-lt-text dark:text-zinc-50">{hub.title}</div>
          {hub.subtitle ? <div className="mt-1 text-xs text-lt-textMuted">{hub.subtitle}</div> : null}
          {hub.summaryText ? (
            <div className="mt-2 text-sm leading-relaxed text-lt-textMuted dark:text-zinc-300">{hub.summaryText}</div>
          ) : hub.placeLens ? (
            <div className="mt-2 text-xs text-lt-textMuted">
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
                  className="rounded-full border border-violet-300/70 bg-lt-surface px-3 py-1 text-[11px] font-semibold text-violet-900 hover:bg-violet-50 dark:border-violet-700 dark:bg-zinc-950 dark:text-violet-200"
                >
                  {p.name}
                </button>
              ))}
              {hub.users.map((u, i) => (
                <span
                  key={`${u.name}-${i}`}
                  className="rounded-full border border-lt-border px-3 py-1 text-[11px] text-lt-textSecondary dark:border-zinc-700 dark:text-zinc-400"
                >
                  {u.name}
                </span>
              ))}
              {!hub.persons.length && !hub.users.length ? (
                <div className="text-sm text-lt-textMuted">No one linked here.</div>
              ) : null}
            </div>
          </div>
        ) : null}

        {openNotes ? (
          <div className="space-y-2 rounded-xl border border-lt-border bg-lt-raised/80 p-3 dark:border-zinc-800 dark:bg-zinc-900/40">
            {hub.entries.map((e) => (
              <div key={e.entry_id} className="text-sm text-lt-textSecondary dark:text-zinc-200">
                <div className="text-[11px] text-lt-textMuted">{e.input_time || e.day || ""}</div>
                <div>{e.text_preview || ""}</div>
              </div>
            ))}
            {!hub.entries.length ? <div className="text-sm text-lt-textMuted">No notes found.</div> : null}
          </div>
        ) : null}
      </div>
    );
  }

  /* context hub */
  return (
    <div className="mt-4 space-y-3">
      <div className="rounded-2xl border border-lt-border/90 bg-gradient-to-b from-lt-washTop via-lt-muted/30 to-lt-surface p-4 dark:border-zinc-800 dark:from-zinc-900/40 dark:to-zinc-950">
        <div className="text-[10px] font-medium uppercase tracking-wide text-lt-textMuted">
          {hub.anchorKind === "journal" ? "Anchor · Journal entry" : "Anchor · Context excerpt"}
        </div>
        <div className="mt-1 text-lg font-semibold text-lt-text dark:text-zinc-50">{hub.name}</div>
        <div className="mt-1 flex flex-wrap gap-2 text-xs text-lt-textMuted">
          {hub.day ? <span>{hub.day}</span> : null}
          {hub.eventType ? <span>{hub.eventType}</span> : null}
        </div>
        {hub.text ? <div className="mt-2 text-sm text-lt-textMuted dark:text-zinc-300">{hub.text}</div> : null}
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
                      className="flex max-w-full flex-col items-start rounded-lg border border-sky-300/60 bg-lt-surface px-2.5 py-1.5 text-left text-[11px] font-semibold text-sky-950 hover:bg-sky-50 dark:border-sky-800 dark:bg-zinc-950 dark:text-sky-100 dark:hover:bg-sky-950/40"
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
        <div className="space-y-2 rounded-xl border border-lt-border bg-zinc-50/80 p-3 dark:border-zinc-800 dark:bg-zinc-900/40">
          {hub.entries.map((e) => (
            <div key={e.entry_id} className="text-sm text-lt-textSecondary dark:text-zinc-200">
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

export function StepRail({
  step,
  categoryLabel,
  entityLabel,
  compact = false,
}: {
  step: WizardStep;
  categoryLabel: string;
  entityLabel: string;
  /** Tighter layout, no bottom rule — e.g. Graph tab above the canvas. */
  compact?: boolean;
}) {
  const phases = [
    { key: "category", label: "Type", done: step !== "category" },
    { key: "entity", label: "Item", done: step === "pick_exploration" || step === "content" || step === "blocked" },
    { key: "explore", label: "View", done: step === "content" },
  ];
  return (
    <div
      className={[
        "flex flex-wrap items-center gap-2 text-[11px]",
        compact ? "" : "mb-4 border-b border-lt-border pb-3 dark:border-zinc-800",
      ].join(" ")}
    >
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

export type LinkedExplorerDetailsProps = {
  overview: Overview | null;
  overviewLoading: boolean;
  overviewError: string;
  selectedRef: string;
  moments: NormalizedMoment[];
  hub: HubData | null;
  momentFlow: Record<string, { journal: boolean; situation: boolean; people: boolean }>;
  toggleMoment: (key: string, part: "journal" | "situation" | "people") => void;
  jumpToEntity: (ref: string | null | undefined, opts?: { anchorPerson?: string | null }) => void | Promise<void>;
  loadOverview: (ref: string) => Promise<boolean>;
  contentHeader: string;
};

export function LinkedExplorerDetails({
  overview,
  overviewLoading,
  overviewError,
  selectedRef,
  moments,
  hub,
  momentFlow,
  toggleMoment,
  jumpToEntity,
  loadOverview,
  contentHeader,
}: LinkedExplorerDetailsProps) {
  return (
    <>
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
            <div className="text-sm font-semibold text-lt-text dark:text-zinc-50">{contentHeader}</div>
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
              Scoped to <span className="font-semibold">{overview.anchor_person_name}</span> — only notes where this tag is
              assigned to them or tied to a situation they are in.{" "}
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

          {overview.kind === "Day" ? <DayExplorer day={overview} onNavigate={(ref) => void jumpToEntity(ref)} /> : null}

          {!moments.length && !hub && overview.kind !== "Day" ? (
            overview.kind === "Person" ? (
              <div className="mt-4 rounded-xl border border-lt-border bg-zinc-50/80 p-4 text-sm text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/40 dark:text-zinc-300">
                <p className="font-medium text-lt-textSecondary dark:text-zinc-100">No timeline rows yet</p>
                <p className="mt-1 text-[13px] leading-relaxed">
                  Notes show up here when they are linked to a situation that lists this person as a participant (Activity →
                  person). The person can still exist in your graph from mentions or aliases.
                </p>
                {typeof overview.mentions === "number" && overview.mentions > 0 ? (
                  <p className="mt-2 text-[13px] text-lt-textMuted dark:text-zinc-400">
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
    </>
  );
}
