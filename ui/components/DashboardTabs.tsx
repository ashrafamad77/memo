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
  const [people, setPeople] = useState<
    { id: string; name: string; role?: string; mentions?: number }[]
  >([]);
  const [status, setStatus] = useState<string>("");

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
          const out = await apiGet<{ items: any[] }>("/persons?limit=30");
          if (!ignore) setPeople(out.items || []);
        }
        if (tab === "Graph") {
          // GraphMindMap loads its own neighborhood; we only need a people list.
          const out = await apiGet<{ items: any[] }>("/persons?limit=30");
          if (!ignore) setPeople(out.items || []);
        }
        if (tab === "Entity Timeline") {
          // EntityTimeline loads its own timeline; we only need a people list.
          const out = await apiGet<{ items: any[] }>("/persons?limit=50");
          if (!ignore) setPeople(out.items || []);
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
        return (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">People</div>
            <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {people.map((p) => (
                <div key={p.id} className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-semibold">{p.name}</div>
                    <div className="text-[11px] text-zinc-500">
                      {p.mentions ?? 0} mentions
                    </div>
                  </div>
                  <div className="mt-1 text-xs text-zinc-400">{p.role || "—"}</div>
                </div>
              ))}
              {!people.length ? (
                <div className="text-sm text-zinc-500">No people yet.</div>
              ) : null}
            </div>
          </div>
        );
      case "Graph":
        return (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Graph view</div>
            <GraphMindMap initialPeople={people} />
          </div>
        );
      case "Entity Timeline":
        return (
          <div className="rounded-2xl border border-zinc-800 bg-zinc-950 p-5">
            <div className="text-sm font-semibold">Entity timeline</div>
            <EntityTimeline initialPeople={people} />
          </div>
        );
      default:
        return null;
    }
  }, [tab, people, timeline]);

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

