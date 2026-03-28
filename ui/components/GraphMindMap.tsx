"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import { KpiHelp } from "@/components/KpiHelp";
import { LinkedGraphView } from "@/components/LinkedGraphView";
import { StepRail } from "@/components/LinkedExplorerPanels";
import { useLinkedExplorer } from "@/hooks/useLinkedExplorer";
import {
  type ExplorerGraphNode,
  type PersonGraphBucket,
  buildLinkedExplorerGraph,
} from "@/lib/linkedExplorer/buildLinkedExplorerGraph";

const GRAPH_EXPLORER_HELP =
  "Same Linked Explorer as Entity Timeline — nodes and edges. Hover the ⓘ on a node for full text and previews (like Insights KPI help).\n\nEdges meet nodes. For people, the first ring is lanes (Situations, Feelings, Notes, …) before individual instances.\n\nScroll to zoom toward the cursor. Drag empty space to pan. Drag nodes to rearrange; click without dragging to open a node.";

/** Linked Explorer as a node–edge graph. */
export function GraphMindMap({ initialRoots: _initialRoots }: { initialRoots: { ref: string; name: string; type: string }[] }) {
  const ex = useLinkedExplorer();
  const [personGraphBucket, setPersonGraphBucket] = useState<PersonGraphBucket | null>(null);

  useEffect(() => {
    setPersonGraphBucket(null);
  }, [ex.wizardStep, ex.selectedRef, ex.overview?.kind]);

  const graphModel = useMemo(
    () =>
      buildLinkedExplorerGraph({
        wizardStep: ex.wizardStep,
        categoryId: ex.categoryId,
        categoryLabel: ex.categoryLabel,
        entityList: ex.entityList,
        entityLoading: ex.entityLoading,
        entityError: ex.entityError,
        navError: ex.navError,
        navOptions: ex.navOptions,
        navLoading: ex.navLoading,
        selectedRef: ex.selectedRef,
        selectedDisplayName: ex.selectedDisplayName,
        overview: ex.overview,
        overviewLoading: ex.overviewLoading,
        overviewError: ex.overviewError,
        moments: ex.moments,
        hub: ex.hub,
        contentHeader: ex.contentHeader,
        startCategory: ex.startCategory,
        jumpToEntity: ex.jumpToEntity,
        selectExplorationOption: ex.selectExplorationOption,
        setWizardStep: ex.setWizardStep,
        setNavOptions: ex.setNavOptions,
        setNavError: ex.setNavError,
        setOverview: ex.setOverview,
        setOverviewError: ex.setOverviewError,
        setSelectedRef: ex.setSelectedRef,
        setSelectedDisplayName: ex.setSelectedDisplayName,
        runNavThenMaybeOverview: ex.runNavThenMaybeOverview,
        loadOverview: ex.loadOverview,
        restartWizard: ex.restartWizard,
        personGraphBucket,
        setPersonGraphBucket,
      }),
    [
      personGraphBucket,
      ex.wizardStep,
      ex.categoryId,
      ex.categoryLabel,
      ex.entityList,
      ex.entityLoading,
      ex.entityError,
      ex.navError,
      ex.navOptions,
      ex.navLoading,
      ex.selectedRef,
      ex.selectedDisplayName,
      ex.overview,
      ex.overviewLoading,
      ex.overviewError,
      ex.moments,
      ex.hub,
      ex.contentHeader,
      ex.startCategory,
      ex.jumpToEntity,
      ex.selectExplorationOption,
      ex.setWizardStep,
      ex.setNavOptions,
      ex.setNavError,
      ex.setOverview,
      ex.setOverviewError,
      ex.setSelectedRef,
      ex.setSelectedDisplayName,
      ex.runNavThenMaybeOverview,
      ex.loadOverview,
      ex.restartWizard,
    ]
  );

  const onActivateNode = useCallback(
    (n: ExplorerGraphNode) => {
      if (n.disabled) return;
      if (n.ref) {
        void ex.jumpToEntity(n.ref, n.navigateOpts);
        return;
      }
      n.onActivate?.();
    },
    [ex]
  );

  return (
    <div className="flex h-full min-h-0 flex-col gap-2">
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 flex-wrap items-center gap-1.5">
          <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Graph explorer</h2>
          <KpiHelp description={GRAPH_EXPLORER_HELP} />
        </div>
        <button
          type="button"
          onClick={ex.restartWizard}
          className="shrink-0 rounded-xl border border-zinc-200 bg-white/80 px-3 py-2 text-[11px] font-bold text-zinc-700 shadow-sm dark:border-zinc-700 dark:bg-zinc-900/80 dark:text-zinc-200"
        >
          Start over
        </button>
      </div>

      <div className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-2xl border border-zinc-200/80 bg-gradient-to-b from-zinc-50 to-sky-50/40 shadow-xl dark:border-zinc-800 dark:from-zinc-950 dark:to-slate-950/80">
        <div className="bg-white/60 px-3 py-1.5 dark:bg-black/25">
          <StepRail
            compact
            step={ex.wizardStep}
            categoryLabel={ex.categoryLabel}
            entityLabel={ex.selectedDisplayName}
          />
        </div>

        {ex.wizardStep === "pick_entity" ? (
          <div className="border-b border-zinc-200/60 px-3 py-2 dark:border-zinc-800">
            <input
              value={ex.searchInput}
              onChange={(e) => ex.setSearchInput(e.target.value)}
              placeholder="Filter entities (does not change the graph layout)…"
              className="w-full max-w-md rounded-lg border border-zinc-200 bg-white/90 px-3 py-2 text-sm dark:border-zinc-700 dark:bg-zinc-950/90"
            />
          </div>
        ) : null}

        {ex.wizardStep === "content" ? (
          <div className="flex flex-wrap gap-2 border-b border-zinc-200/60 px-3 py-2 text-[11px] dark:border-zinc-800">
            <button
              type="button"
              onClick={() => {
                ex.setWizardStep("pick_exploration");
                ex.setOverview(null);
              }}
              className="font-bold text-indigo-600 underline dark:text-indigo-400"
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
              className="font-bold text-zinc-500 underline"
            >
              ← Other item
            </button>
          </div>
        ) : null}

        <div className="flex min-h-0 flex-1 flex-col px-2 pb-2 pt-0.5 sm:px-3 sm:pb-3">
          <LinkedGraphView fillHeight model={graphModel} onActivateNode={onActivateNode} />
        </div>
      </div>
    </div>
  );
}
