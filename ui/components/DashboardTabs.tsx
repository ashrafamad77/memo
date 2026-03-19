"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";
import { InboxQueue } from "@/components/InboxQueue";
import { GraphMindMap } from "@/components/GraphMindMap";
import { EntityTimeline } from "@/components/EntityTimeline";

const tabs = ["Inbox", "Timeline", "Entities", "Entity Timeline", "Graph"] as const;
type Tab = (typeof tabs)[number];

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
        active ? "bg-zinc-100 text-zinc-950" : "bg-zinc-900 text-zinc-200 hover:bg-zinc-800",
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
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Needs review</div>
            <InboxQueue />
          </div>
        );
      case "Timeline":
        return (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entries</div>
            <div className="mt-3 space-y-3">
              {timeline.map((e) => (
                <div key={e.id} className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-xs font-semibold text-zinc-200">
                      {e.day || "—"}
                    </div>
                    <div className="text-[11px] text-zinc-500">{e.input_time || ""}</div>
                  </div>
                  <div className="mt-2 text-sm text-zinc-200">
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
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entities</div>
            <div className="mt-3 space-y-2">
              {typeOrder.map((type) => {
                const items = grouped[type] || [];
                const isOpen = entitiesOpenType === type;
                return (
                  <div key={type} className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-900/40">
                    <button
                      onClick={() => setEntitiesOpenType((prev) => (prev === type ? "" : type))}
                      className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left hover:bg-zinc-800/40"
                    >
                      <div className="text-sm font-semibold">{type}</div>
                      <div className="text-[11px] text-zinc-400">
                        {items.length} item{items.length > 1 ? "s" : ""} {isOpen ? "▲" : "▼"}
                      </div>
                    </button>
                    {isOpen ? (
                      <div className="grid gap-2 border-t border-zinc-800 p-3 sm:grid-cols-2 lg:grid-cols-3">
                        {items.map((e) => (
                          <div
                            key={e.ref}
                            className="rounded-lg border border-zinc-800 bg-zinc-950/70 p-2.5"
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
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Graph view</div>
            <GraphMindMap initialRoots={graphRoots} />
          </div>
        );
      case "Entity Timeline":
        return (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entity timeline</div>
            <EntityTimeline />
          </div>
        );
      default:
        return null;
    }
  }, [tab, graphRoots, entities, timeline, entitiesOpenType]);

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between gap-3 border-b border-zinc-800 px-4 py-3">
        <div className="flex flex-wrap gap-2">
          {tabs.map((t) => (
            <TabButton key={t} label={t} active={t === tab} onClick={() => setTab(t)} />
          ))}
        </div>
        <div className="text-xs text-zinc-400">dashboard</div>
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

