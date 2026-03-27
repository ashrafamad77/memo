"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

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

const RIM_CENTER = 6.2;
const RIM_SAT = 4.8;
const DRAG_THRESHOLD_PX = 6;
/** Neo4j-like: dragging one node nudges the hub and siblings slightly. */
const PULL_CENTER = 0.12;
const PULL_SIBLING = 0.04;

function gradientForNode(node: ExplorerGraphNode): { from: string; to: string } {
  const g = node.visualGroup ?? "generic";
  return NODE_GRADIENT[g] ?? NODE_GRADIENT.generic;
}

function clampPct(n: number) {
  return Math.min(96, Math.max(4, n));
}

function computeDefaultPositions(model: ExplorerGraphModel): Record<string, { x: number; y: number }> {
  const cx = 50;
  const cy = 48;
  const r = 40;
  const out: Record<string, { x: number; y: number }> = {};
  if (model.center) {
    out[model.center.id] = { x: cx, y: cy };
  }
  const n = model.satellites.length;
  model.satellites.forEach((s, i) => {
    const a = (2 * Math.PI * i) / Math.max(n, 1) - Math.PI / 2;
    out[s.id] = { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
  });
  return out;
}

function applyStarFollowDrag(
  base: Record<string, { x: number; y: number }>,
  draggedId: string,
  dx: number,
  dy: number,
  centerId: string,
  satIds: string[]
): Record<string, { x: number; y: number }> {
  const next: Record<string, { x: number; y: number }> = { ...base };
  const clampPos = (p: { x: number; y: number }) => ({ x: clampPct(p.x), y: clampPct(p.y) });
  const p0 = next[draggedId];
  if (!p0) return base;
  next[draggedId] = clampPos({ x: p0.x + dx, y: p0.y + dy });

  if (draggedId === centerId) {
    for (const sid of satIds) {
      const p = next[sid];
      if (!p) continue;
      next[sid] = clampPos({ x: p.x + dx * PULL_CENTER, y: p.y + dy * PULL_CENTER });
    }
  } else {
    const pc = next[centerId];
    if (pc) {
      next[centerId] = clampPos({ x: pc.x + dx * PULL_CENTER, y: pc.y + dy * PULL_CENTER });
    }
    for (const sid of satIds) {
      if (sid === draggedId) continue;
      const p = next[sid];
      if (!p) continue;
      next[sid] = clampPos({ x: p.x + dx * PULL_SIBLING, y: p.y + dy * PULL_SIBLING });
    }
  }
  return next;
}

function computeEdges(
  positions: Record<string, { x: number; y: number }>,
  center: ExplorerGraphNode,
  satellites: ExplorerGraphNode[]
): { x1: number; y1: number; x2: number; y2: number }[] {
  const c = positions[center.id];
  if (!c) return [];
  return satellites
    .map((s) => {
      const p = positions[s.id];
      if (!p) return null;
      const dx = p.x - c.x;
      const dy = p.y - c.y;
      const len = Math.hypot(dx, dy);
      if (len < RIM_CENTER + RIM_SAT + 0.3) {
        return { x1: c.x, y1: c.y, x2: p.x, y2: p.y };
      }
      const ux = dx / len;
      const uy = dy / len;
      return {
        x1: c.x + ux * RIM_CENTER,
        y1: c.y + uy * RIM_CENTER,
        x2: c.x + ux * (len - RIM_SAT),
        y2: c.y + uy * (len - RIM_SAT),
      };
    })
    .filter((e): e is NonNullable<typeof e> => e != null);
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
      data-graph-node-info="true"
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
}: {
  node: ExplorerGraphNode;
  isCenter: boolean;
}) {
  const pal = gradientForNode(node);
  const hasInfo = Boolean(node.infoTitle || node.infoBody || node.entryId);

  return (
    <div
      role="presentation"
      className={[
        "relative flex flex-col items-center justify-center rounded-full bg-gradient-to-br p-2 text-center font-semibold text-white shadow-lg",
        pal.from,
        pal.to,
        isCenter ? "h-[4.5rem] w-[4.5rem] sm:h-[5.25rem] sm:w-[5.25rem] text-[11px] ring-2 ring-white/35" : "h-[3.25rem] w-[3.25rem] sm:h-14 sm:w-14 text-[10px] ring-1 ring-white/25",
        node.disabled ? "cursor-not-allowed opacity-40" : "",
      ].join(" ")}
    >
      <span className="line-clamp-2 max-w-[5.5rem] leading-tight">{node.label}</span>
      {node.sub ? (
        <span className="mt-0.5 line-clamp-1 max-w-[5rem] text-[8px] font-normal text-white/90">{node.sub}</span>
      ) : null}
      {hasInfo ? <NodeInfoPeek title={node.infoTitle} body={node.infoBody} entryId={node.entryId} /> : null}
    </div>
  );
}

export function LinkedGraphView({
  model,
  onActivateNode,
}: {
  model: ExplorerGraphModel;
  onActivateNode: (n: ExplorerGraphNode) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const defaultPositions = useMemo(() => computeDefaultPositions(model), [model]);
  const defaultRef = useRef(defaultPositions);
  defaultRef.current = defaultPositions;

  const topologyKey = useMemo(() => {
    const ids = model.satellites
      .map((s) => s.id)
      .slice()
      .sort()
      .join("|");
    return `${model.center?.id ?? "_"}::${ids}`;
  }, [model.center?.id, model.satellites]);

  const [customPos, setCustomPos] = useState<Record<string, { x: number; y: number }> | null>(null);

  useEffect(() => {
    setCustomPos(null);
  }, [topologyKey]);

  const positions = customPos ?? defaultPositions;

  const centerId = model.center?.id ?? "";
  const satIds = useMemo(() => model.satellites.map((s) => s.id), [model.satellites]);

  const edges = useMemo(() => {
    if (!model.center || !model.satellites.length) return [];
    return computeEdges(positions, model.center, model.satellites);
  }, [model.center, model.satellites, positions]);

  const dragRef = useRef<{
    id: string;
    pointerId: number;
    lastClientX: number;
    lastClientY: number;
    moved: boolean;
    startClientX: number;
    startClientY: number;
  } | null>(null);

  const nodeById = useMemo(() => {
    const m = new Map<string, ExplorerGraphNode>();
    if (model.center) m.set(model.center.id, model.center);
    for (const s of model.satellites) m.set(s.id, s);
    return m;
  }, [model.center, model.satellites]);

  const onNodePointerDown = useCallback(
    (e: React.PointerEvent, node: ExplorerGraphNode) => {
      if (node.disabled || e.button !== 0) return;
      const t = e.target as HTMLElement | null;
      if (t?.closest?.("[data-graph-node-info='true']")) return;
      e.preventDefault();
      (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
      dragRef.current = {
        id: node.id,
        pointerId: e.pointerId,
        lastClientX: e.clientX,
        lastClientY: e.clientY,
        startClientX: e.clientX,
        startClientY: e.clientY,
        moved: false,
      };
    },
    []
  );

  const onNodePointerMove = useCallback(
    (e: React.PointerEvent) => {
      const d = dragRef.current;
      if (!d || e.pointerId !== d.pointerId || !model.center) return;
      const rect = containerRef.current?.getBoundingClientRect();
      if (!rect?.width || !rect.height) return;

      const dxPct = ((e.clientX - d.lastClientX) / rect.width) * 100;
      const dyPct = ((e.clientY - d.lastClientY) / rect.height) * 100;
      d.lastClientX = e.clientX;
      d.lastClientY = e.clientY;

      if (Math.abs(e.clientX - d.startClientX) + Math.abs(e.clientY - d.startClientY) > DRAG_THRESHOLD_PX) {
        d.moved = true;
      }

      if (d.moved && (Math.abs(dxPct) > 0.0001 || Math.abs(dyPct) > 0.0001)) {
        setCustomPos((prev) =>
          applyStarFollowDrag(prev ?? defaultRef.current, d.id, dxPct, dyPct, centerId, satIds)
        );
      }
    },
    [centerId, model.center, satIds]
  );

  const finishPointer = useCallback(
    (e: React.PointerEvent, node: ExplorerGraphNode) => {
      const d = dragRef.current;
      if (!d || e.pointerId !== d.pointerId) return;
      try {
        (e.currentTarget as HTMLElement).releasePointerCapture(e.pointerId);
      } catch {
        /* already released */
      }
      dragRef.current = null;
      if (!d.moved && !node.disabled) {
        onActivateNode(node);
      }
    },
    [onActivateNode]
  );

  const onNodeKeyDown = useCallback(
    (e: React.KeyboardEvent, node: ExplorerGraphNode) => {
      if (node.disabled) return;
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onActivateNode(node);
      }
    },
    [onActivateNode]
  );

  return (
    <div
      ref={containerRef}
      className="relative mx-auto w-full min-h-[min(62vh,520px)] touch-none select-none"
    >
      <svg
        className="pointer-events-none absolute inset-0 h-full w-full"
        viewBox="0 0 100 100"
        preserveAspectRatio="none"
        aria-hidden
      >
        {edges.map((edge, i) => (
          <line
            key={i}
            x1={edge.x1}
            y1={edge.y1}
            x2={edge.x2}
            y2={edge.y2}
            stroke="currentColor"
            strokeWidth={0.35}
            className="text-zinc-400/55 dark:text-zinc-500/50"
          />
        ))}
      </svg>

      {model.center ? (
        <div
          className={[
            "absolute z-10 -translate-x-1/2 -translate-y-1/2",
            model.center.disabled ? "" : "cursor-grab active:cursor-grabbing",
          ].join(" ")}
          style={{ left: `${positions[model.center.id]?.x ?? 50}%`, top: `${positions[model.center.id]?.y ?? 48}%` }}
          onPointerDown={(e) => onNodePointerDown(e, model.center!)}
          onPointerMove={onNodePointerMove}
          onPointerUp={(e) => finishPointer(e, model.center!)}
          onPointerCancel={(e) => finishPointer(e, model.center!)}
        >
          <div
            role="button"
            tabIndex={model.center.disabled ? -1 : 0}
            onKeyDown={(e) => onNodeKeyDown(e, model.center!)}
            className="outline-none focus-visible:ring-2 focus-visible:ring-amber-400/80 focus-visible:ring-offset-2 focus-visible:ring-offset-transparent rounded-full"
          >
            <GraphNodeVisual node={model.center} isCenter />
          </div>
        </div>
      ) : null}

      {model.satellites.map((node) => {
        const p = positions[node.id];
        if (!p) return null;
        return (
          <div
            key={node.id}
            className={[
              "absolute z-10 -translate-x-1/2 -translate-y-1/2",
              node.disabled ? "" : "cursor-grab active:cursor-grabbing",
            ].join(" ")}
            style={{ left: `${p.x}%`, top: `${p.y}%` }}
            onPointerDown={(e) => onNodePointerDown(e, node)}
            onPointerMove={onNodePointerMove}
            onPointerUp={(e) => finishPointer(e, node)}
            onPointerCancel={(e) => finishPointer(e, node)}
          >
            <div
              role="button"
              tabIndex={node.disabled ? -1 : 0}
              onKeyDown={(e) => onNodeKeyDown(e, node)}
              className="outline-none focus-visible:ring-2 focus-visible:ring-amber-400/80 focus-visible:ring-offset-2 focus-visible:ring-offset-transparent rounded-full"
            >
              <GraphNodeVisual node={node} isCenter={false} />
            </div>
          </div>
        );
      })}

      <p className="pointer-events-none absolute bottom-2 left-0 right-0 px-3 text-center text-[10px] leading-snug text-zinc-500 dark:text-zinc-400">
        {model.hint}
        <span className="mt-0.5 block text-zinc-500/90 dark:text-zinc-500/80">
          Drag nodes to rearrange (edges follow); click without dragging to navigate.
        </span>
      </p>
    </div>
  );
}
