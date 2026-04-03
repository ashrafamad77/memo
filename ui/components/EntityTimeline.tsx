"use client";

import { LinkedExplorerDetails, StepRail } from "@/components/LinkedExplorerPanels";
import { useLinkedExplorer } from "@/hooks/useLinkedExplorer";
import { EXPLORER_CATEGORIES, formatEntityOption } from "@/lib/linkedExplorer/model";

export function EntityTimeline() {
  const ex = useLinkedExplorer();

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm font-semibold text-lt-textSecondary dark:text-zinc-100">Linked explorer</div>
        <button
          type="button"
          onClick={ex.restartWizard}
          className="rounded-lg border border-lt-border px-2 py-1 text-[11px] font-semibold text-zinc-600 hover:bg-lt-muted dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-900"
        >
          Start over
        </button>
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-lt-textMuted dark:text-zinc-400">
        Three steps: pick a <span className="font-medium text-lt-textMuted dark:text-zinc-300">category</span>, then an{" "}
        <span className="font-medium text-lt-textMuted dark:text-zinc-300">item</span>, then how to{" "}
        <span className="font-medium text-lt-textMuted dark:text-zinc-300">view</span> it. If the graph has no links, we tell
        you instead of failing silently.
      </p>

      <div className="mt-3 rounded-2xl border border-lt-border bg-lt-surface p-5 dark:border-zinc-800 dark:bg-zinc-950">
        <StepRail
          step={ex.wizardStep}
          categoryLabel={ex.categoryLabel}
          entityLabel={ex.selectedDisplayName}
        />

        {ex.wizardStep === "category" ? (
          <div>
            <div className="text-xs font-semibold text-zinc-500">1 · What kind of thing?</div>
            <div className="mt-3 grid gap-2 sm:grid-cols-2">
              {EXPLORER_CATEGORIES.map((c) => (
                <button
                  key={c.id}
                  type="button"
                  onClick={() => ex.startCategory(c.id, c.label)}
                  className="rounded-xl border border-lt-border bg-zinc-50/80 p-3 text-left transition-colors hover:border-sky-300/60 hover:bg-sky-500/5 dark:border-zinc-800 dark:bg-zinc-900/40 dark:hover:border-sky-800"
                >
                  <div className="text-sm font-semibold text-lt-text dark:text-zinc-50">{c.label}</div>
                  <div className="mt-0.5 text-[11px] text-zinc-500">{c.hint}</div>
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {ex.wizardStep === "pick_entity" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">2 · Pick {ex.categoryLabel.toLowerCase()}</div>
              <button
                type="button"
                onClick={() => {
                  ex.setWizardStep("category");
                  ex.setNavError("");
                }}
                className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
              >
                ← Change type
              </button>
            </div>
            <input
              value={ex.searchInput}
              onChange={(e) => ex.setSearchInput(e.target.value)}
              placeholder="Filter by name…"
              className="mt-2 w-full rounded-lg border border-lt-border bg-lt-surface px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-950"
            />
            {ex.entityLoading ? (
              <div className="mt-4 animate-pulse text-sm text-zinc-500">Loading list…</div>
            ) : null}
            {ex.navError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                Couldn&apos;t check how to open this item: {ex.navError}
                {ex.selectedRef ? (
                  <button
                    type="button"
                    className="ml-2 font-semibold underline"
                    onClick={() => void ex.runNavThenMaybeOverview(ex.selectedRef)}
                  >
                    Retry
                  </button>
                ) : null}
                <button
                  type="button"
                  className="ml-2 font-semibold text-zinc-600 underline dark:text-zinc-400"
                  onClick={() => ex.setNavError("")}
                >
                  Dismiss
                </button>
              </div>
            ) : null}
            {ex.entityError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                {ex.entityError}
                <button
                  type="button"
                  className="ml-2 font-semibold underline"
                  onClick={() => ex.setEntityFetchNonce((n) => n + 1)}
                >
                  Retry
                </button>
              </div>
            ) : null}
            {!ex.entityLoading && !ex.entityError && !ex.entityList.length ? (
              <div className="mt-4 text-sm text-zinc-500">No matches. Try another filter or category.</div>
            ) : null}
            <div className="mt-3 max-h-[min(50vh,22rem)] space-y-1 overflow-y-auto pr-1">
              {ex.entityList.map((e, i) => (
                <button
                  key={e.ref?.trim() ? e.ref : `${e.type}-${i}-${e.name.slice(0, 32)}`}
                  type="button"
                  onClick={() => void ex.jumpToEntity(e.ref)}
                  disabled={ex.navLoading || !e.ref?.trim()}
                  className="flex w-full items-center justify-between gap-2 rounded-lg border border-lt-border/60 bg-lt-raised/50 px-3 py-2 text-left text-sm hover:border-lt-borderStrong hover:bg-lt-surface disabled:opacity-50 dark:border-zinc-800 dark:bg-zinc-900/30 dark:hover:bg-zinc-900"
                >
                  <span className="font-medium text-lt-text dark:text-zinc-100">{e.name}</span>
                  <span className="shrink-0 text-[10px] text-zinc-400">{formatEntityOption(e)}</span>
                </button>
              ))}
            </div>
            {ex.navLoading ? <div className="mt-2 text-[11px] text-zinc-500">Checking what we can open…</div> : null}
          </div>
        ) : null}

        {ex.wizardStep === "pick_exploration" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">3 · How do you want to explore?</div>
              <button
                type="button"
                onClick={() => {
                  ex.setWizardStep("pick_entity");
                  ex.setNavOptions(null);
                  ex.setNavError("");
                }}
                className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
              >
                ← Other item
              </button>
            </div>
            <div className="mt-1 text-sm text-lt-textSecondary dark:text-zinc-200">
              <span className="font-semibold">{ex.selectedDisplayName || ex.selectedRef}</span>
            </div>
            {ex.navError ? (
              <div className="mt-3 rounded-lg border border-rose-500/30 bg-rose-500/10 p-3 text-sm text-rose-800 dark:text-rose-200">
                {ex.navError}
                <button
                  type="button"
                  className="ml-2 font-semibold underline"
                  onClick={() => void ex.runNavThenMaybeOverview(ex.selectedRef)}
                >
                  Retry
                </button>
              </div>
            ) : null}
            <div className="mt-3 space-y-2">
              {(ex.navOptions?.options || []).map((opt) => (
                <button
                  key={opt.key}
                  type="button"
                  disabled={!opt.enabled || ex.overviewLoading}
                  onClick={() => void ex.selectExplorationOption(opt.key)}
                  className={[
                    "w-full rounded-xl border p-3 text-left transition-colors",
                    opt.enabled
                      ? "border-lt-border bg-lt-surface hover:border-sky-300/70 hover:bg-sky-500/5 dark:border-zinc-800 dark:bg-zinc-950 dark:hover:border-sky-800"
                      : "cursor-not-allowed border-zinc-100 bg-zinc-50/50 opacity-60 dark:border-zinc-800/60",
                  ].join(" ")}
                >
                  <div className="flex items-center justify-between gap-2">
                    <span className="text-sm font-semibold text-lt-text dark:text-zinc-50">{opt.title}</span>
                    <span className="text-[11px] text-zinc-500">
                      {opt.count} link{opt.count === 1 ? "" : "s"}
                    </span>
                  </div>
                  <div className="mt-1 text-[11px] text-zinc-500">{opt.description}</div>
                  {!opt.enabled ? (
                    <div className="mt-2 text-[11px] font-medium text-amber-700 dark:text-amber-300">
                      Nothing here yet in your graph.
                    </div>
                  ) : null}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {ex.wizardStep === "blocked" ? (
          <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-4 text-sm text-amber-900 dark:text-amber-100">
            <div className="font-semibold">No exploration path for this item yet</div>
            <p className="mt-1 text-[13px] opacity-90">
              The graph doesn&apos;t have journal links for{" "}
              <span className="font-medium">{ex.selectedDisplayName}</span>. Try another entry or add more notes.
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                onClick={() => {
                  ex.setWizardStep("pick_entity");
                  ex.setNavOptions(null);
                }}
                className="rounded-lg border border-amber-600/40 px-3 py-1.5 text-[11px] font-semibold"
              >
                Pick another item
              </button>
              <button
                type="button"
                onClick={ex.restartWizard}
                className="rounded-lg border border-zinc-300 px-3 py-1.5 text-[11px] font-semibold dark:border-zinc-600"
              >
                Start over
              </button>
            </div>
          </div>
        ) : null}

        {ex.wizardStep === "content" ? (
          <div>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="text-xs font-semibold text-zinc-500">Details</div>
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => {
                    ex.setWizardStep("pick_exploration");
                    ex.setOverview(null);
                  }}
                  className="text-[11px] font-semibold text-sky-700 hover:underline dark:text-sky-400"
                >
                  ← Change view
                </button>
                <button
                  type="button"
                  onClick={() => {
                    ex.setWizardStep("pick_entity");
                    ex.setOverview(null);
                    ex.setOverviewError("");
                    ex.setNavOptions(null);
                    ex.setNavError("");
                    ex.setSelectedRef("");
                    ex.setSelectedDisplayName("");
                  }}
                  className="text-[11px] font-semibold text-zinc-500 hover:underline"
                >
                  ← Other item
                </button>
              </div>
            </div>

            <LinkedExplorerDetails
              overview={ex.overview}
              overviewLoading={ex.overviewLoading}
              overviewError={ex.overviewError}
              selectedRef={ex.selectedRef}
              moments={ex.moments}
              hub={ex.hub}
              momentFlow={ex.momentFlow}
              toggleMoment={ex.toggleMoment}
              jumpToEntity={ex.jumpToEntity}
              loadOverview={ex.loadOverview}
              contentHeader={ex.contentHeader}
            />
          </div>
        ) : null}
      </div>
    </div>
  );
}
