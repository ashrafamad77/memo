"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";

type Person = { id: string; name: string; role?: string; mentions?: number };
type Item = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  places?: string[];
  text_preview?: string;
};

export function EntityTimeline({ initialPeople }: { initialPeople: Person[] }) {
  const [people, setPeople] = useState<Person[]>(initialPeople || []);
  const [selectedId, setSelectedId] = useState<string>(initialPeople?.[0]?.id || "");
  const [items, setItems] = useState<Item[]>([]);
  const [status, setStatus] = useState<string>("");

  useEffect(() => {
    if (people.length) return;
    apiGet<{ items: Person[] }>("/persons?limit=50")
      .then((out) => setPeople(out.items || []))
      .catch(() => {});
  }, [people.length]);

  useEffect(() => {
    if (!selectedId) return;
    let ignore = false;
    setStatus("");
    apiGet<{ items: Item[] }>(`/person/${encodeURIComponent(selectedId)}/timeline?limit=120`)
      .then((out) => {
        if (!ignore) setItems(out.items || []);
      })
      .catch((e: any) => {
        if (!ignore) setStatus(e?.message || String(e));
      });
    return () => {
      ignore = true;
    };
  }, [selectedId]);

  const header = useMemo(() => {
    const p = people.find((x) => x.id === selectedId);
    if (!p) return "Entity timeline";
    return `Entity timeline · ${p.name}${p.role ? ` (${p.role})` : ""}`;
  }, [people, selectedId]);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="text-xs font-semibold text-zinc-300">Person</div>
          <select
            value={selectedId}
            onChange={(e) => setSelectedId(e.target.value)}
            className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
          >
            {people.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
                {p.role ? ` (${p.role})` : ""}
              </option>
            ))}
          </select>
        </div>
        <div className="text-[11px] text-zinc-500">{items.length} interactions</div>
      </div>

      {status ? (
        <div className="mt-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
          {status}
        </div>
      ) : null}

      <div className="mt-3 rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
        <div className="text-sm font-semibold">{header}</div>
        <div className="mt-3 space-y-3">
          {items.map((it) => (
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
          {!items.length ? <div className="text-sm text-zinc-500">No interactions yet.</div> : null}
        </div>
      </div>
    </div>
  );
}

