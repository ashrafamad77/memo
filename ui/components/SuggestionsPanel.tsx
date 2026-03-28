"use client";

import { useCallback, useState } from "react";

import { apiGet } from "@/lib/api";

type ProposalPersonRef = { name: string; tier: string };

type ProposalV1 = {
  id: string;
  kind: string;
  title: string;
  body: string;
  anchor_date: string;
  valid_from?: string;
  valid_until?: string;
  priority?: number;
  people: ProposalPersonRef[];
  evidence?: Record<string, unknown>[];
};

type ProposalsResponse = {
  generated_at: string;
  proposals: ProposalV1[];
  meta: Record<string, unknown>;
};

function tierLabel(tier: string): string {
  if (tier === "supportive") return "supportive";
  if (tier === "emerging_support") return "emerging";
  return tier || "neutral";
}

function evidenceSummary(ev: Record<string, unknown>): string {
  const t = String(ev.type ?? "");
  const bits: string[] = [];
  for (const [k, v] of Object.entries(ev)) {
    if (k === "type") continue;
    if (v === null || v === undefined || v === "") continue;
    if (typeof v === "object") {
      bits.push(`${k}=${JSON.stringify(v)}`);
    } else {
      bits.push(`${k}=${String(v)}`);
    }
  }
  return bits.length ? `${t}: ${bits.join(" · ")}` : t;
}

export function SuggestionsPanel() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [data, setData] = useState<ProposalsResponse | null>(null);

  const runSuggestions = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const out = await apiGet<ProposalsResponse>("/proposals?days_ahead=10");
      setData(out);
    } catch (e: unknown) {
      setData(null);
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  const meta = data?.meta ?? null;
  const skipped = meta && typeof meta.skipped === "string" ? meta.skipped : "";
  const llmError = meta && typeof meta.llm_error === "string" ? meta.llm_error : "";
  const engine = meta && typeof meta.engine === "string" ? meta.engine : "";

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Suggestions</h2>
        <p className="mt-1 max-w-2xl text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
          Run the AI when you want ideas grounded in your journal graph and current context (time, city, weather). Nothing
          is fetched until you click the button.
        </p>
      </div>

      <div>
        <button
          type="button"
          onClick={() => void runSuggestions()}
          disabled={loading}
          className={[
            "rounded-xl px-4 py-2.5 text-sm font-semibold shadow-sm transition-colors",
            loading
              ? "cursor-not-allowed bg-zinc-200 text-zinc-500 dark:bg-zinc-800 dark:text-zinc-500"
              : "bg-violet-600 text-white hover:bg-violet-500 dark:bg-violet-600 dark:hover:bg-violet-500",
          ].join(" ")}
        >
          {loading ? "Working…" : "AI suggestions"}
        </button>
      </div>

      {error ? (
        <div className="rounded-xl border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
          {error}
        </div>
      ) : null}

      {data && !error ? (
        <div className="space-y-4">
          <div className="rounded-xl border border-zinc-200/90 bg-zinc-50/80 px-3 py-2 text-[11px] text-zinc-600 dark:border-zinc-700 dark:bg-zinc-900/40 dark:text-zinc-400">
            <div>
              <span className="font-medium text-zinc-500 dark:text-zinc-500">Generated</span>{" "}
              <span className="font-mono text-zinc-700 dark:text-zinc-300">{data.generated_at}</span>
            </div>
            {engine ? (
              <div className="mt-0.5">
                <span className="font-medium text-zinc-500 dark:text-zinc-500">Engine</span> {engine}
              </div>
            ) : null}
            {skipped ? (
              <div className="mt-1 text-amber-800 dark:text-amber-200">
                <span className="font-medium">Skipped:</span> {skipped}
              </div>
            ) : null}
            {llmError ? (
              <div className="mt-1 text-rose-700 dark:text-rose-300">
                <span className="font-medium">LLM</span> {llmError}
              </div>
            ) : null}
            {typeof meta?.count === "number" ? (
              <div className="mt-0.5">
                <span className="font-medium text-zinc-500 dark:text-zinc-500">Count</span> {meta.count}
              </div>
            ) : null}
          </div>

          {!data.proposals.length && !skipped && !llmError ? (
            <div className="text-sm text-zinc-500">No suggestions this run.</div>
          ) : null}

          <div className="space-y-3">
            {data.proposals.map((prop) => (
              <article
                key={prop.id}
                className="rounded-2xl border border-violet-200/80 bg-violet-50/50 p-4 shadow-sm dark:border-violet-900/45 dark:bg-violet-950/20"
              >
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <div className="flex min-w-0 items-start gap-2">
                    <span className="shrink-0 text-lg leading-none" aria-hidden>
                      ✨
                    </span>
                    <div className="min-w-0">
                      <h3 className="text-sm font-semibold text-violet-950 dark:text-violet-100">{prop.title}</h3>
                      <p className="mt-1 text-[13px] leading-relaxed text-zinc-700 dark:text-zinc-300">{prop.body}</p>
                    </div>
                  </div>
                  {prop.priority != null ? (
                    <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 text-[10px] font-semibold tabular-nums text-violet-800 ring-1 ring-violet-200/70 dark:bg-zinc-900/80 dark:text-violet-200 dark:ring-violet-800/60">
                      priority {prop.priority}
                    </span>
                  ) : null}
                </div>

                <dl className="mt-3 grid gap-1.5 text-[11px] text-zinc-600 dark:text-zinc-400 sm:grid-cols-2">
                  <div>
                    <dt className="font-medium text-zinc-500 dark:text-zinc-500">Kind</dt>
                    <dd className="font-mono text-zinc-800 dark:text-zinc-200">{prop.kind}</dd>
                  </div>
                  <div>
                    <dt className="font-medium text-zinc-500 dark:text-zinc-500">Anchor date</dt>
                    <dd className="font-mono text-zinc-800 dark:text-zinc-200">{prop.anchor_date}</dd>
                  </div>
                  <div className="sm:col-span-2">
                    <dt className="font-medium text-zinc-500 dark:text-zinc-500">Proposal id</dt>
                    <dd className="break-all font-mono text-[10px] text-zinc-700 dark:text-zinc-300">{prop.id}</dd>
                  </div>
                  {prop.valid_from || prop.valid_until ? (
                    <div className="sm:col-span-2">
                      <dt className="font-medium text-zinc-500 dark:text-zinc-500">Valid window</dt>
                      <dd className="font-mono text-[10px] text-zinc-700 dark:text-zinc-300">
                        {prop.valid_from ?? "—"} → {prop.valid_until ?? "—"}
                      </dd>
                    </div>
                  ) : null}
                </dl>

                {prop.people?.length ? (
                  <div className="mt-3">
                    <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-500">
                      People
                    </div>
                    <div className="mt-1.5 flex flex-wrap gap-1.5">
                      {prop.people.map((person) => (
                        <span
                          key={`${prop.id}-${person.name}`}
                          className="rounded-md bg-white/90 px-2 py-0.5 text-[11px] font-medium text-zinc-800 ring-1 ring-violet-200/60 dark:bg-zinc-900/70 dark:text-zinc-200 dark:ring-violet-800/50"
                        >
                          {person.name}
                          <span className="ml-1 font-normal text-zinc-500 dark:text-zinc-400">{tierLabel(person.tier)}</span>
                        </span>
                      ))}
                    </div>
                  </div>
                ) : null}

                {prop.evidence && prop.evidence.length > 0 ? (
                  <details className="mt-3 rounded-lg border border-violet-200/50 bg-white/60 p-2 dark:border-violet-900/40 dark:bg-zinc-900/40">
                    <summary className="cursor-pointer text-[11px] font-semibold text-violet-900 dark:text-violet-200">
                      Evidence & traceability
                    </summary>
                    <ul className="mt-2 space-y-1.5 border-t border-violet-200/40 pt-2 dark:border-violet-900/35">
                      {prop.evidence.map((ev, i) => (
                        <li
                          key={`${prop.id}-ev-${i}`}
                          className="break-words text-[11px] leading-snug text-zinc-600 dark:text-zinc-400"
                        >
                          {evidenceSummary(ev)}
                        </li>
                      ))}
                    </ul>
                  </details>
                ) : null}
              </article>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
