"use client";

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";

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

const RIM_CENTER = 7.1;
const RIM_SAT = 5.4;
const DRAG_THRESHOLD_PX = 6;
/** Neo4j-like: dragging one node nudges the hub and siblings slightly. */
const PULL_CENTER = 0.12;
const PULL_SIBLING = 0.04;

const ZOOM_MIN = 0.45;
const ZOOM_MAX = 2.75;
const ZOOM_DEFAULT = 1;
const ZOOM_WHEEL_SENS = 0.00115;

/** Soft spring toward layout targets — slight overshoot / settle like Neo4j graph drag. */
const SPRING_STRENGTH = 0.26;
const SPRING_DAMP = 0.76;
const SPRING_EPS_POS = 0.02;
const SPRING_EPS_VEL = 0.016;

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

function springIntegrateDisplay(
  layout: Record<string, { x: number; y: number }>,
  prevDisplay: Record<string, { x: number; y: number }>,
  dragId: string | null,
  vel: Record<string, { vx: number; vy: number }>
): { next: Record<string, { x: number; y: number }>; animating: boolean } {
  const next: Record<string, { x: number; y: number }> = {};
  let animating = false;

  for (const id of Object.keys(layout)) {
    const t = layout[id];
    const cur = prevDisplay[id] ?? t;

    if (id === dragId) {
      next[id] = { x: t.x, y: t.y };
      vel[id] = { vx: 0, vy: 0 };
      if (Math.abs(cur.x - t.x) + Math.abs(cur.y - t.y) > 0.001) animating = true;
      continue;
    }

    let { vx, vy } = vel[id] ?? { vx: 0, vy: 0 };
    const ax = (t.x - cur.x) * SPRING_STRENGTH;
    const ay = (t.y - cur.y) * SPRING_STRENGTH;
    vx = (vx + ax) * SPRING_DAMP;
    vy = (vy + ay) * SPRING_DAMP;
    const nx = cur.x + vx;
    const ny = cur.y + vy;
    next[id] = { x: nx, y: ny };
    vel[id] = { vx, vy };

    if (Math.hypot(t.x - nx, t.y - ny) > SPRING_EPS_POS || Math.hypot(vx, vy) > SPRING_EPS_VEL) {
      animating = true;
    }
  }

  return { next, animating };
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
      <span className="inline-flex h-[1.125rem] w-[1.125rem] shrink-0 cursor-default items-center justify-center rounded-full border border-white/60 bg-black/30 text-[10px] font-bold text-white backdrop-blur-sm">
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
        isCenter
          ? "h-[5.5rem] w-[5.5rem] sm:h-[6.25rem] sm:w-[6.25rem] text-xs sm:text-[13px] ring-2 ring-white/35"
          : "h-16 w-16 sm:h-[4.75rem] sm:w-[4.75rem] text-[11px] sm:text-xs ring-1 ring-white/25",
        node.disabled ? "cursor-not-allowed opacity-40" : "",
      ].join(" ")}
    >
      <span className="line-clamp-2 max-w-[6.75rem] leading-tight">{node.label}</span>
      {node.sub ? (
        <span className="mt-0.5 line-clamp-1 max-w-[6rem] text-[10px] font-normal text-white/90 sm:text-[11px]">
          {node.sub}
        </span>
      ) : null}
      {hasInfo ? <NodeInfoPeek title={node.infoTitle} body={node.infoBody} entryId={node.entryId} /> : null}
    </div>
  );
}

export function LinkedGraphView({
  model,
  onActivateNode,
  fillHeight,
}: {
  model: ExplorerGraphModel;
  onActivateNode: (n: ExplorerGraphNode) => void;
  /** Use full height of a flex parent for a taller canvas. */
  fillHeight?: boolean;
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
  const [zoom, setZoom] = useState(ZOOM_DEFAULT);
  const zoomRef = useRef(zoom);
  zoomRef.current = zoom;

  const [pan, setPan] = useState({ x: 0, y: 0 });
  const panRef = useRef(pan);
  panRef.current = pan;

  useEffect(() => {
    setCustomPos(null);
    setZoom(ZOOM_DEFAULT);
    setPan({ x: 0, y: 0 });
  }, [topologyKey]);

  const layoutPositions = customPos ?? defaultPositions;

  const layoutTargetRef = useRef(layoutPositions);
  layoutTargetRef.current = layoutPositions;

  const displayWorkRef = useRef<Record<string, { x: number; y: number }>>(defaultPositions);
  const velocityRef = useRef<Record<string, { vx: number; vy: number }>>({});
  const draggingIdRef = useRef<string | null>(null);
  const rafRef = useRef<number | null>(null);
  const springLoopRef = useRef<() => void>(() => {});

  const [displayPositions, setDisplayPositions] = useState(defaultPositions);

  useEffect(() => {
    const d = { ...defaultPositions };
    displayWorkRef.current = d;
    setDisplayPositions(d);
    velocityRef.current = {};
    if (rafRef.current != null) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    }
  }, [topologyKey, defaultPositions]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const onWheel = (e: WheelEvent) => {
      e.preventDefault();
      const rect = el.getBoundingClientRect();
      const mx = e.clientX - rect.left;
      const my = e.clientY - rect.top;
      const factor = Math.exp(-e.deltaY * ZOOM_WHEEL_SENS);
      setZoom((prevZ) => {
        const newZ = Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, prevZ * factor));
        setPan((prevP) => ({
          x: mx - (newZ / prevZ) * (mx - prevP.x),
          y: my - (newZ / prevZ) * (my - prevP.y),
        }));
        return newZ;
      });
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);

  useEffect(() => {
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
    };
  }, []);

  springLoopRef.current = () => {
    const layout = layoutTargetRef.current;
    const dragId = draggingIdRef.current;
    const prev = displayWorkRef.current;
    const { next, animating } = springIntegrateDisplay(layout, prev, dragId, velocityRef.current);
    displayWorkRef.current = next;
    setDisplayPositions(next);
    rafRef.current = null;
    if (animating) {
      rafRef.current = requestAnimationFrame(() => springLoopRef.current());
    }
  };

  useLayoutEffect(() => {
    if (rafRef.current == null) {
      rafRef.current = requestAnimationFrame(() => springLoopRef.current());
    }
  }, [layoutPositions]);

  const centerId = model.center?.id ?? "";
  const satIds = useMemo(() => model.satellites.map((s) => s.id), [model.satellites]);

  const edges = useMemo(() => {
    if (!model.center || !model.satellites.length) return [];
    return computeEdges(displayPositions, model.center, model.satellites);
  }, [model.center, model.satellites, displayPositions]);

  const dragRef = useRef<{
    id: string;
    pointerId: number;
    lastClientX: number;
    lastClientY: number;
    moved: boolean;
    startClientX: number;
    startClientY: number;
  } | null>(null);

  const canvasPanRef = useRef<{
    pointerId: number;
    lastClientX: number;
    lastClientY: number;
  } | null>(null);

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
      draggingIdRef.current = node.id;
      if (rafRef.current == null) {
        rafRef.current = requestAnimationFrame(() => springLoopRef.current());
      }
    },
    []
  );

  const onNodePointerMove = useCallback(
    (e: React.PointerEvent) => {
      const d = dragRef.current;
      if (!d || e.pointerId !== d.pointerId || !model.center) return;
      const rect = containerRef.current?.getBoundingClientRect();
      if (!rect?.width || !rect.height) return;

      const z = Math.max(zoomRef.current, 0.01);
      const dxPct = ((e.clientX - d.lastClientX) / rect.width / z) * 100;
      const dyPct = ((e.clientY - d.lastClientY) / rect.height / z) * 100;
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
      draggingIdRef.current = null;
      if (rafRef.current == null) {
        rafRef.current = requestAnimationFrame(() => springLoopRef.current());
      }
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

  const zoomTowardPoint = useCallback((mx: number, my: number, nextZFactor: number) => {
    setZoom((prevZ) => {
      const newZ =
        nextZFactor >= 1
          ? Math.min(ZOOM_MAX, prevZ * nextZFactor)
          : Math.max(ZOOM_MIN, prevZ * nextZFactor);
      setPan((prevP) => ({
        x: mx - (newZ / prevZ) * (mx - prevP.x),
        y: my - (newZ / prevZ) * (my - prevP.y),
      }));
      return newZ;
    });
  }, []);

  const zoomOut = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    zoomTowardPoint(r.width / 2, r.height / 2, 1 / 1.18);
  }, [zoomTowardPoint]);

  const zoomIn = useCallback(() => {
    const el = containerRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    zoomTowardPoint(r.width / 2, r.height / 2, 1.18);
  }, [zoomTowardPoint]);

  const zoomReset = useCallback(() => {
    setZoom(ZOOM_DEFAULT);
    setPan({ x: 0, y: 0 });
  }, []);

  const onCanvasPointerDown = useCallback((e: React.PointerEvent) => {
    if (e.button !== 0) return;
    e.preventDefault();
    (e.currentTarget as HTMLElement).setPointerCapture(e.pointerId);
    canvasPanRef.current = {
      pointerId: e.pointerId,
      lastClientX: e.clientX,
      lastClientY: e.clientY,
    };
  }, []);

  const onCanvasPointerMove = useCallback((e: React.PointerEvent) => {
    const p = canvasPanRef.current;
    if (!p || e.pointerId !== p.pointerId) return;
    const dx = e.clientX - p.lastClientX;
    const dy = e.clientY - p.lastClientY;
    p.lastClientX = e.clientX;
    p.lastClientY = e.clientY;
    setPan((prev) => ({ x: prev.x + dx, y: prev.y + dy }));
  }, []);

  const onCanvasPointerUp = useCallback((e: React.PointerEvent) => {
    const p = canvasPanRef.current;
    if (!p || e.pointerId !== p.pointerId) return;
    try {
      (e.currentTarget as HTMLElement).releasePointerCapture(e.pointerId);
    } catch {
      /* already released */
    }
    canvasPanRef.current = null;
  }, []);

  return (
    <div
      ref={containerRef}
      className={[
        "relative mx-auto w-full touch-none select-none overflow-hidden",
        fillHeight ? "h-full min-h-0 flex-1" : "min-h-[min(72vh,640px)]",
      ].join(" ")}
    >
      <div
        className="absolute inset-0 will-change-transform"
        style={{
          transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
          transformOrigin: "0 0",
        }}
      >
        <div
          className="absolute inset-0 z-[1] cursor-grab touch-none active:cursor-grabbing"
          aria-hidden
          onPointerDown={onCanvasPointerDown}
          onPointerMove={onCanvasPointerMove}
          onPointerUp={onCanvasPointerUp}
          onPointerCancel={onCanvasPointerUp}
        />
        <svg
          className="pointer-events-none absolute inset-0 z-[2] h-full w-full"
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
            style={{
              left: `${displayPositions[model.center.id]?.x ?? 50}%`,
              top: `${displayPositions[model.center.id]?.y ?? 48}%`,
            }}
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
          const p = displayPositions[node.id];
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
      </div>

      <div
        className="pointer-events-auto absolute right-2 top-2 z-30 flex items-center gap-0.5 rounded-lg border border-zinc-200/90 bg-white/95 px-1 py-0.5 shadow-sm backdrop-blur-sm dark:border-zinc-600/90 dark:bg-zinc-900/95"
        onWheel={(e) => e.stopPropagation()}
      >
        <button
          type="button"
          aria-label="Zoom out"
          className="flex h-7 w-7 items-center justify-center rounded-md text-sm font-medium text-zinc-700 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
          onClick={zoomOut}
        >
          −
        </button>
        <button
          type="button"
          aria-label={`Zoom ${Math.round(zoom * 100)} percent, reset to 100 percent`}
          title="Reset zoom to 100%"
          className="min-w-[2.75rem] px-1 text-center text-[10px] font-medium tabular-nums text-zinc-600 dark:text-zinc-300"
          onClick={zoomReset}
        >
          {Math.round(zoom * 100)}%
        </button>
        <button
          type="button"
          aria-label="Zoom in"
          className="flex h-7 w-7 items-center justify-center rounded-md text-sm font-medium text-zinc-700 hover:bg-zinc-100 dark:text-zinc-200 dark:hover:bg-zinc-800"
          onClick={zoomIn}
        >
          +
        </button>
      </div>

      {model.hint ? (
        <p className="pointer-events-none absolute bottom-2 left-0 right-0 px-3 text-center text-[10px] leading-snug text-zinc-500 dark:text-zinc-400">
          {model.hint}
        </p>
      ) : null}
    </div>
  );
}
