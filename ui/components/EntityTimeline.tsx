"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";

type EntityItem = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  places?: string[];
  text_preview?: string;
};

type EntityRef = {
  type: string;
  name: string;
  ref: string;
};

type OverviewPerson = {
  kind: "Person";
  ref: string;
  name: string;
  role?: string;
  mentions?: number;
  items: EntityItem[];
};

type OverviewEvent = {
  kind: "Event";
  ref: string;
  event_type?: string;
  day?: string;
  places?: string[];
  persons?: { id: string; name: string; role?: string; mentions?: number }[];
  users?: { name: string; mentions?: number }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

type OverviewContext = {
  kind: "E73_Information_Object";
  ref: string;
  name?: string;
  event_type?: string;
  day?: string;
  text?: string;
  topics?: { type: string; name: string }[];
  concepts?: { type: string; name: string }[];
  mentions?: { type: string; name: string }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

export function EntityTimeline() {
  const [entities, setEntities] = useState<EntityRef[]>([]);
  const [selectedRef, setSelectedRef] = useState<string>("");
  const [overview, setOverview] = useState<OverviewPerson | OverviewEvent | OverviewContext | null>(null);
  const [status, setStatus] = useState<string>("");

  useEffect(() => {
    // Fetch a mixed entity list so the user can navigate by clicking entity types.
    apiGet<{ items: EntityRef[] }>("/entities?limit=120")
      .then((out) => {
        const list = out.items || [];
        setEntities(list);
        if (!selectedRef && list.length) setSelectedRef(list[0].ref);
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    if (!selectedRef) return;
    let ignore = false;
    setStatus("");
    apiGet<OverviewPerson | OverviewEvent | OverviewContext>(
      `/entity/overview?ref=${encodeURIComponent(selectedRef)}&limit=120`
    )
      .then((out: any) => {
        if (!ignore) setOverview(out || null);
      })
      .catch((e: any) => {
        if (!ignore) setStatus(e?.message || String(e));
      });
    return () => {
      ignore = true;
    };
  }, [selectedRef]);

  // Quick helper: avoid TS narrowing issues in JSX.
  const header = useMemo(() => {
    if (!overview) return "Entity timeline";
    if (overview.kind === "Person") {
      return `Entity timeline · ${overview.name}${overview.role ? ` (${overview.role})` : ""}`;
    }
    if (overview.kind === "E73_Information_Object") {
      return `Entity timeline · Context`;
    }
    return `Entity timeline · Event`;
  }, [overview]);

  const count = useMemo(() => {
    if (!overview) return 0;
    if (overview.kind === "Person") return overview.items?.length || 0;
    if (overview.kind === "E73_Information_Object") return overview.entries?.length || 0;
    return overview.entries?.length || 0;
  }, [overview]);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="text-xs font-semibold text-zinc-300">Entity</div>
          <select
            value={selectedRef}
            onChange={(e) => setSelectedRef(e.target.value)}
            className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
          >
            {entities.map((e) => (
              <option key={e.ref} value={e.ref}>
                {e.type}: {e.name}
              </option>
            ))}
          </select>
        </div>
        <div className="text-[11px] text-zinc-500">
          {count} items
        </div>
      </div>

      {status ? (
        <div className="mt-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
          {status}
        </div>
      ) : null}

      <div className="mt-3 rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
        <div className="text-sm font-semibold">{header}</div>
        {overview ? (
          overview.kind === "Person" ? (
            <div className="mt-3 space-y-3">
              {overview.items.map((it) => (
                <div key={it.entry_id} className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <div className="text-xs font-semibold text-zinc-200">{it.day || "—"}</div>
                      {it.event_type ? (
                        <div className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-0.5 text-[11px] text-zinc-200">
                          {it.event_type}
                        </div>
                      ) : null}
                      {Array.isArray(it.places) && it.places.length ? (
                        <div className="text-[11px] text-zinc-400">{it.places.join(", ")}</div>
                      ) : null}
                    </div>
                    <div className="text-[11px] text-zinc-500">{it.input_time || ""}</div>
                  </div>
                  <div className="mt-2 text-sm text-zinc-200">
                    {it.text_preview || ""}
                    {(it.text_preview || "").length >= 260 ? "…" : ""}
                  </div>
                </div>
              ))}
              {!overview.items.length ? (
                <div className="text-sm text-zinc-500">No interactions yet.</div>
              ) : null}
            </div>
          ) : overview.kind === "E73_Information_Object" ? (
            <div className="mt-3 space-y-3">
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <div className="text-xs font-semibold text-zinc-200">{overview.day || "—"}</div>
                  {overview.event_type ? (
                    <div className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-0.5 text-[11px] text-zinc-200">
                      {overview.event_type}
                    </div>
                  ) : null}
                </div>
                <div className="mt-2 text-sm text-zinc-200">
                  <div className="text-xs font-semibold text-zinc-300">Context phrase</div>
                  <div className="mt-1 text-zinc-300">{overview.text || ""}</div>
                </div>
              </div>

              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-sm font-semibold text-zinc-200">Context entities</div>
                <div className="mt-2 space-y-1">
                  {(overview.topics || []).map((t, idx) => (
                    <div key={`topic:${idx}:${t.name}`} className="text-sm text-zinc-200">
                      {t.type}: {t.name}
                    </div>
                  ))}
                  {(overview.concepts || []).map((c, idx) => (
                    <div key={`concept:${idx}:${c.name}`} className="text-sm text-zinc-300">
                      {c.type}: {c.name}
                    </div>
                  ))}
                  {(overview.mentions || []).map((m, idx) => (
                    <div key={`mention:${idx}:${m.name}`} className="text-sm text-zinc-300">
                      {m.type}: {m.name}
                    </div>
                  ))}
                  {!(overview.topics?.length || overview.mentions?.length) ? (
                    <div className="text-sm text-zinc-500">No context entities found.</div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-sm font-semibold text-zinc-200">Entries</div>
                <div className="mt-2 space-y-2">
                  {(overview.entries || []).map((e) => (
                    <div key={e.entry_id} className="text-sm text-zinc-200">
                      <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                      <div>{e.text_preview || ""}</div>
                    </div>
                  ))}
                  {!(overview.entries?.length) ? (
                    <div className="text-sm text-zinc-500">No entries found.</div>
                  ) : null}
                </div>
              </div>
            </div>
          ) : (
            <div className="mt-3 space-y-3">
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <div className="text-xs font-semibold text-zinc-200">{overview.day || "—"}</div>
                  {overview.event_type ? (
                    <div className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-0.5 text-[11px] text-zinc-200">
                      {overview.event_type}
                    </div>
                  ) : null}
                  {Array.isArray(overview.places) && overview.places.length ? (
                    <div className="text-[11px] text-zinc-400">{overview.places.join(", ")}</div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-sm font-semibold text-zinc-200">Participants</div>
                <div className="mt-2 space-y-2">
                  {(overview.persons || []).map((p) => (
                    <div key={p.id} className="text-sm text-zinc-200">
                      {p.name}
                      {p.role ? ` (${p.role})` : ""}
                    </div>
                  ))}
                  {(overview.users || []).map((u, idx) => (
                    <div key={`${u.name}:${idx}`} className="text-sm text-zinc-200 text-zinc-300">
                      {u.name}
                    </div>
                  ))}
                  {!(overview.persons?.length || overview.users?.length) ? (
                    <div className="text-sm text-zinc-500">No participants found.</div>
                  ) : null}
                </div>
              </div>

              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-sm font-semibold text-zinc-200">Entries</div>
                <div className="mt-2 space-y-2">
                  {(overview.entries || []).map((e) => (
                    <div key={e.entry_id} className="text-sm text-zinc-200">
                      <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                      <div>{e.text_preview || ""}</div>
                    </div>
                  ))}
                  {!(overview.entries?.length) ? (
                    <div className="text-sm text-zinc-500">No entries found.</div>
                  ) : null}
                </div>
              </div>
            </div>
          )
        ) : (
          <div className="mt-3 text-sm text-zinc-500">Select an entity to load details.</div>
        )}
      </div>
    </div>
  );
}

