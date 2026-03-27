"use client";

import { useMemo, useState } from "react";

import { LazyJournalBody } from "@/components/LinkedExplorerPanels";
import type {
  ExplorerGraphModel,
  ExplorerGraphNode,
  ExplorerNodeVisualGroup,
} from "@/lib/linkedExplorer/buildLinkedExplorerGraph";

/** One gradient per semantic type — all places share blue, all situations green, etc. */
const NODE_GRADIENT: Record<ExplorerNodeVisualGroup, { from: string; to: string }> = {
  hub: { from: "from-teal-500", to: "to-cyan-600" },
  category: { from: "from-slate-600", to: "to-zinc-800" },
  person: { from: "from-violet-500", to: "to-fuchsia-600" },
  place: { from: "from-sky-500", to: "to-blue-600" },
  situation: { from: "from-green-500", to: "to-emerald-700" },
  feeling: { from: "from-amber-500", to: "to-orange-600" },
  note: { from: "from-slate-500", to: "to-zinc-600" },
  day: { from: "from-indigo-500", to: "to-purple-700" },
  idea: { from: "from-pink-500", to: "to-rose-600" },
  group: { from: "from-blue-600", to: "to-indigo-800" },
  nav: { from: "from-cyan-500", to: "to-teal-600" },
  bucket: { from: "from-orange-500", to: "to-amber-600" },
  system: { from: "from-rose-600", to: "to-red-800" },
  generic: { from: "from-zinc-500", to: "to-neutral-700" },
};

function gradientForNode(node: ExplorerGraphNode): { from: string; to: string } {
  const g = node.visualGroup ?? "generic";
  return NODE_GRADIENT[g] ?? NODE_GRADIENT.generic;
}

function NodeInfoPeek({
  title,
  body,
  entryId,
}: {
  title?: string;
  body?: string;
  entryId?: string;
}) {
  const [open, setOpen] = useState(false);

  return (
    <span
      className="pointer-events-auto absolute -right-1 -top-1 z-20 flex flex-col items-end"
      onClick={(e) => e.stopPropagation()}
      onPointerDown={(e) => e.stopPropagation()}
      onPointerEnter={() => setOpen(true)}
      onPointerLeave={() => setOpen(false)}
    >
      <span className="inline-flex h-4 w-4 shrink-0 cursor-default items-center justify-center rounded-full border border-white/60 bg-black/30 text-[9px] font-bold text-white backdrop-blur-sm">
        i
      </span>
      {open ? (
        <div className="absolute right-0 top-full z-[200] mt-1 w-72 max-h-72 overflow-y-auto rounded-lg border border-zinc-200 bg-white p-2.5 text-left text-[11px] leading-snug text-zinc-700 shadow-xl dark:border-zinc-700 dark:bg-zinc-950 dark:text-zinc-200">
          {title ? <span className="block font-semibold text-zinc-900 dark:text-zinc-100">{title}</span> : null}
          {entryId ? (
            <div className="mt-1">
              <LazyJournalBody entryId={entryId} visible />
            </div>
          ) : body ? (
            <span className="mt-1 block whitespace-pre-wrap text-zinc-600 dark:text-zinc-300">{body}</span>
          ) : null}
        </div>
      ) : null}
    </span>
  );
}

function GraphNodeVisual({
  node,
  isCenter,
  onClick,
}: {
  node: ExplorerGraphNode;
  isCenter: boolean;
  onClick: () => void;
}) {
  const pal = gradientForNode(node);
  const hasInfo = Boolean(node.infoTitle || node.infoBody || node.entryId);

  return (
    <button
      type="button"
      disabled={node.disabled}
      onClick={onClick}
      className={[
        "relative flex flex-col items-center justify-center rounded-full bg-gradient-to-br p-2 text-center font-semibold text-white shadow-lg transition-transform hover:scale-[1.04] active:scale-95 disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:scale-100",
        pal.from,
        pal.to,
        isCenter ? "h-[4.5rem] w-[4.5rem] sm:h-[5.25rem] sm:w-[5.25rem] text-[11px] ring-2 ring-white/35" : "h-[3.25rem] w-[3.25rem] sm:h-14 sm:w-14 text-[10px] ring-1 ring-white/25",
      ].join(" ")}
    >
      <span className="line-clamp-2 max-w-[5.5rem] leading-tight">{node.label}</span>
      {node.sub ? (
        <span className="mt-0.5 line-clamp-1 max-w-[5rem] text-[8px] font-normal text-white/90">{node.sub}</span>
      ) : null}
      {hasInfo ? <NodeInfoPeek title={node.infoTitle} body={node.infoBody} entryId={node.entryId} /> : null}
    </button>
  );
}

export function LinkedGraphView({
  model,
  onActivateNode,
}: {
  model: ExplorerGraphModel;
  onActivateNode: (n: ExplorerGraphNode) => void;
}) {
  const layout = useMemo(() => {
    const cx = 50;
    const cy = 48;
    const r = 40;
    const nSat = model.satellites.length;
    const satPos = model.satellites.map((s, i) => {
      const a = (2 * Math.PI * i) / Math.max(nSat, 1) - Math.PI / 2;
      return { node: s, x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
    });
    return { cx, cy, satPos };
  }, [model.satellites, model.center]);

  /** Endpoints on disc rims: C→S unit vector, inset from centers by approximate radii (viewBox units). */
  const edges = useMemo(() => {
    if (!model.center || !model.satellites.length) return [];
    const rCenter = 6.2;
    const rSat = 4.8;
    return layout.satPos.map(({ x, y }) => {
      const dx = x - layout.cx;
      const dy = y - layout.cy;
      const len = Math.hypot(dx, dy);
      if (len < rCenter + rSat + 0.5) {
        return { x1: layout.cx, y1: layout.cy, x2: x, y2: y };
      }
      const ux = dx / len;
      const uy = dy / len;
      return {
        x1: layout.cx + ux * rCenter,
        y1: layout.cy + uy * rCenter,
        x2: layout.cx + ux * (len - rSat),
        y2: layout.cy + uy * (len - rSat),
      };
    });
  }, [model.center, layout]);

  return (
    <div className="relative mx-auto w-full min-h-[min(62vh,520px)] select-none">
      <svg
        className="pointer-events-none absolute inset-0 h-full w-full"
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
        aria-hidden
      >
        {edges.map((e, i) => (
          <line
            key={i}
            x1={e.x1}
            y1={e.y1}
            x2={e.x2}
            y2={e.y2}
            stroke="currentColor"
            strokeWidth={0.35}
            className="text-zinc-400/55 dark:text-zinc-500/50"
          />
        ))}
      </svg>

      {model.center ? (
        <div
          className="absolute -translate-x-1/2 -translate-y-1/2"
          style={{ left: `${layout.cx}%`, top: `${layout.cy}%` }}
        >
          <GraphNodeVisual
            node={model.center}
            isCenter
            onClick={() => onActivateNode(model.center!)}
          />
        </div>
      ) : null}

      {layout.satPos.map(({ node, x, y }) => (
        <div
          key={node.id}
          className="absolute -translate-x-1/2 -translate-y-1/2"
          style={{ left: `${x}%`, top: `${y}%` }}
        >
          <GraphNodeVisual
            node={node}
            isCenter={false}
            onClick={() => onActivateNode(node)}
          />
        </div>
      ))}

      <p className="pointer-events-none absolute bottom-2 left-0 right-0 px-3 text-center text-[10px] leading-snug text-zinc-500 dark:text-zinc-400">
        {model.hint}
      </p>
    </div>
  );
}
