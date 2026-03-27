"use client";

import { useEffect, useState } from "react";

import { LinkedExplorerDetails, StepRail } from "@/components/LinkedExplorerPanels";
import { useLinkedExplorer } from "@/hooks/useLinkedExplorer";
import { EXPLORER_CATEGORIES, formatEntityOption } from "@/lib/linkedExplorer/model";

function useIsDark() {
  const [dark, setDark] = useState(true);
  useEffect(() => {
    const check = () => setDark(document.documentElement.classList.contains("dark"));
    check();
    const obs = new MutationObserver(check);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => obs.disconnect();
  }, []);
  return dark;
}

const CATEGORY_VISUAL: Record<string, { emoji: string; gradient: string; shadow: string }> = {
  person: { emoji: "◎", gradient: "from-emerald-500 to-teal-600", shadow: "shadow-emerald-500/25" },
  feeling_tag: { emoji: "✦", gradient: "from-amber-500 to-orange-600", shadow: "shadow-amber-500/25" },
  situation: { emoji: "◆", gradient: "from-sky-500 to-indigo-600", shadow: "shadow-sky-500/25" },
  place: { emoji: "⌖", gradient: "from-blue-400 to-cyan-600", shadow: "shadow-blue-500/20" },
  day: { emoji: "◷", gradient: "from-violet-500 to-purple-700", shadow: "shadow-violet-500/25" },
  idea: { emoji: "◇", gradient: "from-fuchsia-500 to-pink-600", shadow: "shadow-fuchsia-500/25" },
  note: { emoji: "▤", gradient: "from-slate-500 to-zinc-700", shadow: "shadow-zinc-500/20" },
  group: { emoji: "▣", gradient: "from-indigo-500 to-blue-700", shadow: "shadow-indigo-500/25" },
};

function VisualBubble({
  title,
  subtitle,
  emoji,
  gradient,
  shadow,
  disabled,
  selected,
  onClick,
  size = "lg",
}: {
  title: string;
  subtitle?: string;
  emoji?: string;
  gradient: string;
  shadow: string;
  disabled?: boolean;
  selected?: boolean;
  onClick?: () => void;
  size?: "lg" | "md" | "sm";
}) {
  const dim = size === "lg" ? "h-32 w-32 sm:h-36 sm:w-36" : size === "md" ? "h-24 w-24 sm:h-28 sm:w-28" : "h-[4.5rem] w-[4.5rem]";
  const textTitle = size === "sm" ? "text-[10px] leading-tight" : size === "md" ? "text-[11px] leading-tight" : "text-xs leading-tight";
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={[
        "group relative flex flex-col items-center justify-center rounded-full bg-gradient-to-br p-3 text-center font-semibold text-white transition-all duration-300",
        gradient,
        shadow,
        dim,
        disabled ? "cursor-not-allowed opacity-40 grayscale" : "hover:scale-105 hover:brightness-110 active:scale-95",
        selected ? "ring-4 ring-amber-400 ring-offset-2 ring-offset-zinc-950 scale-105" : "ring-0",
      ].join(" ")}
    >
      {emoji ? <span className="mb-0.5 text-lg opacity-90">{emoji}</span> : null}
      <span className={`${textTitle} line-clamp-3 px-1`}>{title}</span>
      {subtitle && size !== "sm" ? (
        <span className="mt-1 line-clamp-2 px-1 text-[9px] font-normal text-white/80">{subtitle}</span>
      ) : null}
    </button>
  );
}

/**
 * Same Linked Explorer flow as Entity Timeline (category → item → view → details),
 * with a visual bubble UI instead of text lists. Uses identical APIs and state machine.
 */
export function GraphMindMap({ initialRoots: _initialRoots }: { initialRoots: { ref: string; name: string; type: string }[] }) {
  const ex = useLinkedExplorer();
  const isDark = useIsDark();

  const canvasBg = isDark
    ? "linear-gradient(165deg, #0c0c12 0%, #09090b 40%, #0f172a 100%)"
    : "linear-gradient(165deg, #eff6ff 0%, #f8fafc 45%, #e0f2fe 100%)";

  return (
    <div className="mt-3 space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="text-sm font-semibold text-zinc-800 dark:text-zinc-100">Visual linked explorer</div>
          <p className="mt-0.5 max-w-xl text-[11px] leading-relaxed text-zinc-500 dark:text-zinc-400">
            Same steps as Entity Timeline — pick a kind of thing, then an item, then a view. Here each choice is a bubble;
            details use the exact same panels as the timeline.
          </p>
        </div>
        <button
          type="button"
          onClick={ex.restartWizard}
          className="rounded-xl border border-zinc-200 bg-white/80 px-3 py-2 text-[11px] font-bold text-zinc-700 shadow-sm dark:border-zinc-700 dark:bg-zinc-900/80 dark:text-zinc-200"
        >
          Start over
        </button>
      </div>

      <div
        className="overflow-hidden rounded-2xl border border-zinc-200/80 shadow-xl dark:border-zinc-800"
        style={{ background: canvasBg }}
      >
        <div className="border-b border-white/10 bg-black/10 px-4 py-3 dark:border-zinc-800 dark:bg-black/20">
          <StepRail
            step={ex.wizardStep}
            categoryLabel={ex.categoryLabel}
            entityLabel={ex.selectedDisplayName}
          />
        </div>

        <div className="p-4 sm:p-6">
          {ex.wizardStep === "category" ? (
            <div>
              <div className="text-center text-[10px] font-bold uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">
                1 · What kind of thing?
              </div>
              <div className="mx-auto mt-6 flex max-w-4xl flex-wrap justify-center gap-5 sm:gap-8">
                {EXPLORER_CATEGORIES.map((c) => {
                  const v = CATEGORY_VISUAL[c.id] || {
                    emoji: "○",
                    gradient: "from-zinc-500 to-zinc-700",
                    shadow: "shadow-zinc-500/20",
                  };
                  return (
                    <VisualBubble
                      key={c.id}
                      emoji={v.emoji}
                      gradient={v.gradient}
                      shadow={v.shadow}
                      title={c.label}
                      subtitle={c.hint}
                      onClick={() => ex.startCategory(c.id, c.label)}
                    />
                  );
                })}
              </div>
            </div>
          ) : null}

          {ex.wizardStep === "pick_entity" ? (
            <div>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-center text-[10px] font-bold uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400 sm:text-left">
                  2 · Pick {ex.categoryLabel.toLowerCase()}
                </div>
                <button
                  type="button"
                  onClick={() => {
                    ex.setWizardStep("category");
                    ex.setNavError("");
                  }}
                  className="text-[11px] font-bold text-indigo-600 underline dark:text-indigo-400"
                >
                  ← Change type
                </button>
              </div>
              <input
                value={ex.searchInput}
                onChange={(e) => ex.setSearchInput(e.target.value)}
                placeholder="Filter by name…"
                className="mx-auto mt-3 block w-full max-w-xl rounded-xl border border-white/20 bg-white/90 px-4 py-2.5 text-sm shadow-inner dark:border-zinc-700 dark:bg-zinc-950/90"
              />
              {ex.entityLoading ? (
                <div className="mt-8 text-center text-sm text-zinc-500 animate-pulse">Loading…</div>
              ) : null}
              {ex.navError ? (
                <div className="mx-auto mt-4 max-w-xl rounded-xl border border-rose-500/40 bg-rose-500/10 p-3 text-sm text-rose-900 dark:text-rose-100">
                  {ex.navError}
                  {ex.selectedRef ? (
                    <button
                      type="button"
                      className="ml-2 font-bold underline"
                      onClick={() => void ex.runNavThenMaybeOverview(ex.selectedRef)}
                    >
                      Retry
                    </button>
                  ) : null}
                </div>
              ) : null}
              {ex.entityError ? (
                <div className="mx-auto mt-4 max-w-xl rounded-xl border border-rose-500/40 bg-rose-500/10 p-3 text-sm">
                  {ex.entityError}
                  <button type="button" className="ml-2 font-bold underline" onClick={() => ex.setEntityFetchNonce((n) => n + 1)}>
                    Retry
                  </button>
                </div>
              ) : null}
              {!ex.entityLoading && !ex.entityError && !ex.entityList.length ? (
                <div className="mt-8 text-center text-sm text-zinc-500">No matches.</div>
              ) : null}
              <div className="mx-auto mt-6 flex max-h-[min(52vh,28rem)] max-w-5xl flex-wrap justify-center gap-3 overflow-y-auto py-2 pr-1">
                {ex.entityList.map((e, i) => {
                  const v = CATEGORY_VISUAL[ex.categoryId] || {
                    emoji: "●",
                    gradient: "from-zinc-500 to-slate-700",
                    shadow: "shadow-zinc-500/15",
                  };
                  return (
                    <VisualBubble
                      key={e.ref?.trim() ? e.ref : `${e.type}-${i}`}
                      emoji={v.emoji}
                      gradient={v.gradient}
                      shadow={v.shadow}
                      title={e.name}
                      subtitle={formatEntityOption(e)}
                      size="md"
                      disabled={ex.navLoading || !e.ref?.trim()}
                      selected={ex.selectedRef === e.ref}
                      onClick={() => void ex.jumpToEntity(e.ref)}
                    />
                  );
                })}
              </div>
              {ex.navLoading ? (
                <div className="mt-4 text-center text-[11px] text-zinc-500">Opening…</div>
              ) : null}
            </div>
          ) : null}

          {ex.wizardStep === "pick_exploration" ? (
            <div>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-zinc-500 dark:text-zinc-400">
                  3 · How do you want to explore?
                </div>
                <button
                  type="button"
                  onClick={() => {
                    ex.setWizardStep("pick_entity");
                    ex.setNavOptions(null);
                    ex.setNavError("");
                  }}
                  className="text-[11px] font-bold text-indigo-600 underline dark:text-indigo-400"
                >
                  ← Other item
                </button>
              </div>
              <div className="mt-2 text-center text-sm font-semibold text-zinc-800 dark:text-zinc-100">
                {ex.selectedDisplayName || ex.selectedRef}
              </div>
              {ex.navError ? (
                <div className="mx-auto mt-4 max-w-xl rounded-xl border border-rose-500/40 bg-rose-500/10 p-3 text-sm">
                  {ex.navError}
                  <button
                    type="button"
                    className="ml-2 font-bold underline"
                    onClick={() => void ex.runNavThenMaybeOverview(ex.selectedRef)}
                  >
                    Retry
                  </button>
                </div>
              ) : null}
              <div className="mx-auto mt-8 flex max-w-3xl flex-col items-stretch gap-3">
                {(ex.navOptions?.options || []).map((opt) => (
                  <button
                    key={opt.key}
                    type="button"
                    disabled={!opt.enabled || ex.overviewLoading}
                    onClick={() => void ex.selectExplorationOption(opt.key)}
                    className={[
                      "rounded-2xl border px-4 py-4 text-left transition-all",
                      opt.enabled
                        ? "border-white/25 bg-white/70 shadow-lg hover:scale-[1.01] hover:bg-white/90 dark:border-zinc-700 dark:bg-zinc-950/70 dark:hover:bg-zinc-900"
                        : "cursor-not-allowed border-zinc-200/40 bg-zinc-100/40 opacity-60 dark:border-zinc-800 dark:bg-zinc-900/30",
                    ].join(" ")}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="text-sm font-bold text-zinc-900 dark:text-zinc-50">{opt.title}</span>
                      <span className="rounded-full bg-indigo-500/15 px-2 py-0.5 text-[11px] font-semibold text-indigo-800 dark:text-indigo-200">
                        {opt.count}
                      </span>
                    </div>
                    <div className="mt-1 text-[11px] text-zinc-600 dark:text-zinc-400">{opt.description}</div>
                    {!opt.enabled ? (
                      <div className="mt-2 text-[11px] font-medium text-amber-800 dark:text-amber-200">
                        Nothing here yet in your graph.
                      </div>
                    ) : null}
                  </button>
                ))}
              </div>
            </div>
          ) : null}

          {ex.wizardStep === "blocked" ? (
            <div className="mx-auto max-w-lg rounded-2xl border border-amber-500/40 bg-amber-500/15 p-6 text-center text-amber-950 dark:text-amber-50">
              <div className="font-bold">No path for this item yet</div>
              <p className="mt-2 text-sm opacity-90">Try another bubble or add journal links in the graph.</p>
              <div className="mt-4 flex justify-center gap-2">
                <button
                  type="button"
                  onClick={() => {
                    ex.setWizardStep("pick_entity");
                    ex.setNavOptions(null);
                  }}
                  className="rounded-xl border border-amber-700/40 px-4 py-2 text-[11px] font-bold"
                >
                  Pick another item
                </button>
                <button
                  type="button"
                  onClick={ex.restartWizard}
                  className="rounded-xl border border-zinc-400 px-4 py-2 text-[11px] font-bold dark:border-zinc-600"
                >
                  Start over
                </button>
              </div>
            </div>
          ) : null}

          {ex.wizardStep === "content" ? (
            <div className="mx-auto max-w-3xl">
              <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
                <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-zinc-500">Details</div>
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      ex.setWizardStep("pick_exploration");
                      ex.setOverview(null);
                    }}
                    className="text-[11px] font-bold text-indigo-600 underline dark:text-indigo-400"
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
                    className="text-[11px] font-bold text-zinc-500 underline"
                  >
                    ← Other item
                  </button>
                </div>
              </div>
              <div className="rounded-2xl border border-white/20 bg-white/85 p-4 shadow-xl backdrop-blur-md dark:border-zinc-800 dark:bg-zinc-950/85">
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
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
