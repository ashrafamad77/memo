"use client";

import { useEffect, useState } from "react";

import { API_BASE, apiGet } from "@/lib/api";

type Task = {
  id: string;
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
};

export function InboxQueue() {
  const [items, setItems] = useState<Task[]>([]);
  const [busy, setBusy] = useState<string>("");
  const [err, setErr] = useState<string>("");

  async function refresh() {
    const out = await apiGet<{ items: Task[] }>("/inbox?status=open&limit=50");
    setItems(out.items || []);
  }

  useEffect(() => {
    refresh().catch((e) => setErr(e?.message || String(e)));
  }, []);

  async function resolve(taskId: string, decision: "merge" | "split") {
    setErr("");
    setBusy(taskId);
    try {
      const res = await fetch(`${API_BASE}/inbox/${taskId}/resolve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ decision }),
      });
      if (!res.ok) {
        const t = await res.text().catch(() => "");
        throw new Error(`resolve failed: ${res.status} ${t}`);
      }
      await refresh();
    } catch (e: any) {
      setErr(e?.message || String(e));
    } finally {
      setBusy("");
    }
  }

  if (err) {
    return <div className="mt-2 text-sm text-amber-200">{err}</div>;
  }

  if (!items.length) {
    return <div className="mt-2 text-sm text-zinc-400">No pending tasks.</div>;
  }

  return (
    <div className="mt-3 space-y-3">
      {items.map((t) => (
        <div key={t.id} className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-3">
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <div className="text-sm font-semibold">
                Disambiguate: <span className="text-zinc-100">{t.mention}</span>
              </div>
              <div className="mt-1 text-xs text-zinc-400">
                candidate: {t.candidate_name || "—"} ({t.candidate_role || "—"}) · proposed:{" "}
                {t.proposed_name || "—"} ({t.proposed_role || "—"}) · score:{" "}
                {typeof t.score === "number" ? t.score.toFixed(2) : "—"}
              </div>
            </div>
            <div className="shrink-0 text-[11px] text-zinc-500">{t.created_at || ""}</div>
          </div>
          <div className="mt-3 flex gap-2">
            <button
              disabled={busy === t.id}
              onClick={() => resolve(t.id, "merge")}
              className="rounded-lg bg-zinc-100 px-3 py-2 text-xs font-semibold text-zinc-950 disabled:opacity-50"
            >
              Merge (same person)
            </button>
            <button
              disabled={busy === t.id}
              onClick={() => resolve(t.id, "split")}
              className="rounded-lg border border-zinc-700 bg-transparent px-3 py-2 text-xs font-semibold text-zinc-100 disabled:opacity-50"
            >
              Split (different)
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}

