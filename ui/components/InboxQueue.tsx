"use client";

import { useEffect, useState } from "react";

import { API_BASE, apiGet } from "@/lib/api";

type WikidataCandidate = {
  wikidata_id: string;
  label?: string;
  description?: string;
  confidence?: string;
};

type Task = {
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

  useEffect(() => {
    function onInboxChanged() {
      refresh().catch(() => {});
    }
    window.addEventListener("memo:inbox-changed", onInboxChanged);
    return () => window.removeEventListener("memo:inbox-changed", onInboxChanged);
  }, []);

  async function resolvePerson(taskId: string, decision: "merge" | "split") {
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

  async function resolvePlace(taskId: string, decision: "pick" | "skip", wikidataId?: string) {
    setErr("");
    setBusy(taskId);
    try {
      const body: Record<string, string> = { decision };
      if (decision === "pick" && wikidataId) {
        body.wikidata_id = wikidataId;
      }
      const res = await fetch(`${API_BASE}/inbox/${taskId}/resolve`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
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
    return <div className="mt-2 text-sm text-lt-textMuted dark:text-zinc-400">No pending tasks.</div>;
  }

  return (
    <div className="mt-3 space-y-3">
      {items.map((t) => {
        const isPlace = t.type === "place_wikidata";
        return (
          <div
            key={t.id}
            className="rounded-xl border border-lt-border dark:border-zinc-800 bg-lt-raised dark:bg-zinc-900/40 p-3"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="text-sm font-semibold">
                  {isPlace ? (
                    <>
                      Link place:{" "}
                      <span className="text-lt-text dark:text-zinc-100">{t.mention}</span>
                      {t.entity_label ? (
                        <span className="ml-2 font-normal text-lt-textMuted dark:text-zinc-400">
                          ({t.entity_label})
                        </span>
                      ) : null}
                    </>
                  ) : (
                    <>
                      Disambiguate:{" "}
                      <span className="text-lt-text dark:text-zinc-100">{t.mention}</span>
                    </>
                  )}
                </div>
                {isPlace ? (
                  <div className="mt-1 text-xs text-lt-textMuted dark:text-zinc-400">
                    Pick a Wikidata match or skip to leave the place unlinked.
                  </div>
                ) : (
                  <div className="mt-1 text-xs text-lt-textMuted dark:text-zinc-400">
                    candidate: {t.candidate_name || "—"} ({t.candidate_role || "—"}) · proposed:{" "}
                    {t.proposed_name || "—"} ({t.proposed_role || "—"}) · score:{" "}
                    {typeof t.score === "number" ? t.score.toFixed(2) : "—"}
                  </div>
                )}
              </div>
              <div className="shrink-0 text-[11px] text-zinc-500">{t.created_at || ""}</div>
            </div>
            {isPlace ? (
              <div className="mt-3 flex flex-wrap gap-2">
                {(t.candidates || []).map((c) => (
                  <button
                    key={c.wikidata_id}
                    disabled={busy === t.id}
                    type="button"
                    title={c.description || c.wikidata_id}
                    onClick={() => resolvePlace(t.id, "pick", c.wikidata_id)}
                    className="max-w-full rounded-lg bg-lt-raised dark:bg-zinc-900 px-3 py-2 text-left text-xs font-semibold text-zinc-950 disabled:opacity-50"
                  >
                    <span className="block truncate">{c.label || c.wikidata_id}</span>
                    {c.description ? (
                      <span className="mt-0.5 block line-clamp-2 text-[10px] font-normal font-sans text-lt-textMuted dark:text-zinc-400">
                        {c.description}
                      </span>
                    ) : null}
                    <span className="block font-mono text-[10px] font-normal text-zinc-500">
                      {c.wikidata_id}
                    </span>
                  </button>
                ))}
                <button
                  disabled={busy === t.id}
                  type="button"
                  onClick={() => resolvePlace(t.id, "skip")}
                  className="rounded-lg border border-lt-borderStrong dark:border-zinc-700 bg-transparent px-3 py-2 text-xs font-semibold text-lt-text dark:text-zinc-100 disabled:opacity-50"
                >
                  Skip
                </button>
              </div>
            ) : (
              <div className="mt-3 flex gap-2">
                <button
                  disabled={busy === t.id}
                  onClick={() => resolvePerson(t.id, "merge")}
                  className="rounded-lg bg-lt-raised dark:bg-zinc-900 px-3 py-2 text-xs font-semibold text-zinc-950 disabled:opacity-50"
                >
                  Merge (same person)
                </button>
                <button
                  disabled={busy === t.id}
                  onClick={() => resolvePerson(t.id, "split")}
                  className="rounded-lg border border-lt-borderStrong dark:border-zinc-700 bg-transparent px-3 py-2 text-xs font-semibold text-lt-text dark:text-zinc-100 disabled:opacity-50"
                >
                  Split (different)
                </button>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
