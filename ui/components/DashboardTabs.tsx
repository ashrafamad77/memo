"use client";

import { useEffect, useMemo, useState } from "react";

import { apiGet } from "@/lib/api";
import { InboxQueue } from "@/components/InboxQueue";

const tabs = ["Inbox", "Timeline", "Entities", "Graph"] as const;
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
  const [graph, setGraph] = useState<{ nodes: any[]; edges: any[] } | null>(null);
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
          const out = await apiGet<{ items: any[] }>("/persons?limit=1");
          const first = (out.items || [])[0];
          if (first?.id) {
            const g = await apiGet<{ nodes: any[]; edges: any[] }>(
              `/graph/neighborhood?ref=${encodeURIComponent(`Person:${first.id}`)}&depth=1`
            );
            if (!ignore) setGraph(g);
          } else {
            if (!ignore) setGraph({ nodes: [], edges: [] });
          }
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
            <div className="mt-3 grid gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-xs font-semibold text-zinc-300">Nodes</div>
                <div className="mt-2 text-xs text-zinc-400">
                  {(graph?.nodes || []).slice(0, 8).map((n, i) => (
                    <div key={n._elementId || i}>
                      {(n._labels || []).join(",")}: {n.name || n.id || n.key || n.date || "—"}
                    </div>
                  ))}
                  {(graph?.nodes || []).length > 8 ? "…" : ""}
                </div>
              </div>
              <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
                <div className="text-xs font-semibold text-zinc-300">Edges</div>
                <div className="mt-2 text-xs text-zinc-400">
                  {(graph?.edges || []).slice(0, 10).map((e, i) => (
                    <div key={i}>{e.type}</div>
                  ))}
                  {(graph?.edges || []).length > 10 ? "…" : ""}
                </div>
              </div>
            </div>
          </div>
        );
      default:
        return null;
    }
  }, [tab, graph, people, timeline]);

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
        <div className="mx-auto max-w-5xl">
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

