"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { apiGet } from "@/lib/api";

const MAX_GRAPH_NODES = 220;
const INITIAL_DEPTH = 2;
const EXPAND_DEPTH = 2;

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

function usePrefersReducedMotion() {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    const fn = () => setReduced(mq.matches);
    fn();
    mq.addEventListener("change", fn);
    return () => mq.removeEventListener("change", fn);
  }, []);
  return reduced;
}

type RootEntity = { ref: string; name: string; type: string; mentions?: number };

type ApiNode = {
  _elementId: string;
  _labels?: string[];
  [k: string]: unknown;
};

type ApiEdge = {
  type: string;
  start: string;
  end: string;
  properties?: Record<string, unknown>;
};

type GraphApiOut = { nodes: ApiNode[]; edges: ApiEdge[] };

function mergeGraph(a: GraphApiOut, b: GraphApiOut): GraphApiOut {
  const nodeMap = new Map<string, ApiNode>();
  for (const n of [...(a.nodes || []), ...(b.nodes || [])]) {
    if (n && n._elementId) nodeMap.set(n._elementId, n);
  }
  const edgeKey = (e: ApiEdge) => `${e.type}|${e.start}|${e.end}`;
  const edgeMap = new Map<string, ApiEdge>();
  for (const e of [...(a.edges || []), ...(b.edges || [])]) {
    if (e?.start && e?.end) edgeMap.set(edgeKey(e), e);
  }
  return { nodes: [...nodeMap.values()], edges: [...edgeMap.values()] };
}

type OverviewAny = {
  kind?: string;
  ref?: string;
  name?: string;
  activity_name?: string;
  summary_preview?: string;
  event_type?: string;
  day?: string;
  event_time_iso?: string;
  event_time_text?: string;
  places?: string[];
  persons?: { id?: string; name?: string; role?: string; mentions?: number }[];
  users?: { name?: string; mentions?: number }[];
  entries?: { entry_id?: string; input_time?: string; day?: string; text_preview?: string }[];
  text?: string;
  entry_kind?: string;
  linked?: { ref: string; name: string; bucket?: string }[];
  feeling_tags?: { name: string; count?: number; ref: string }[];
  occurrences?: unknown[];
  situations?: unknown[];
  role?: string;
  mentions?: number;
};

function kindFromLabels(labels?: string[]) {
  const s = new Set(labels || []);
  if (s.has("E13_Attribute_Assignment")) return "E13_Attribute_Assignment";
  if (s.has("E55_Type")) return "E55_Type";
  if (s.has("E7_Activity")) return "E7_Activity";
  if (s.has("E21_Person")) return "E21_Person";
  if (s.has("E52_Time_Span")) return "E52_Time_Span";
  if (s.has("E53_Place")) return "E53_Place";
  if (s.has("E39_Actor")) return "E39_Actor";
  if (s.has("E73_Information_Object")) return "E73_Information_Object";
  if (s.has("E28_Conceptual_Object")) return "E28_Conceptual_Object";
  if (s.has("E74_Group")) return "E74_Group";
  if (s.has("Alias")) return "Alias";
  if (s.has("DisambiguationTask")) return "DisambiguationTask";
  return (labels && labels[0]) || "Node";
}

function labelForNode(n: ApiNode) {
  const labels = n._labels || [];
  if (labels.includes("E73_Information_Object") && typeof n.text === "string") {
    const t = n.text.replace(/\s+/g, " ").trim();
    return t.length > 80 ? t.slice(0, 80) + "…" : t;
  }
  if (labels.includes("E7_Activity")) {
    return String(n.name || "E7 Activity");
  }
  if (labels.includes("E73_Information_Object")) {
    const t = String(n.content || n.name || "").replace(/\s+/g, " ").trim();
    return t.length > 90 ? t.slice(0, 90) + "…" : t || "E73 Information Object";
  }
  return (
    (n.name as string) ||
    (n.text as string) ||
    (n.mention as string) ||
    (n.date as string) ||
    (typeof n.key === "string" ? n.key.split("|")[0] : "") ||
    (n.id as string) ||
    "—"
  );
}

function humanizeRel(type: string) {
  const t = String(type || "").trim();
  const map: Record<string, string> = {
    PARTICIPATED_IN: "participated in",
    OCCURRED_AT: "at",
    HAS_EMOTION: "emotion",
    P17_was_motivated_by: "motivated by",
    P15_was_influenced_by: "influenced by",
    P7_took_place_at: "at place",
    P4_has_time_span: "time",
    P14_carried_out_by: "actor",
    P14i_performed: "performed",
    P67_refers_to: "refers to",
    P140_assigned_attribute_to: "assignee",
    P141_assigned: "assigned",
    P2_has_type: "type",
    FROM_ENTRY: "from entry",
    ON_DAY: "on day",
    HAS_ALIAS: "has alias",
    ALIAS_OF: "alias of",
  };
  if (map[t]) return map[t];
  return t ? t.replace(/_/g, " ").toLowerCase() : "rel";
}

function isDirectionalRel(type: string) {
  return true;
}

function colorForKind(kind: string): { fill: string; stroke: string; glow: string } {
  switch (kind) {
    case "E7_Activity":
      return { fill: "#f59e0b", stroke: "#d97706", glow: "rgba(245,158,11,0.45)" };
    case "E52_Time_Span":
      return { fill: "#fbbf24", stroke: "#ca8a04", glow: "rgba(251,191,36,0.4)" };
    case "E53_Place":
      return { fill: "#38bdf8", stroke: "#0284c7", glow: "rgba(56,189,248,0.45)" };
    case "E39_Actor":
      return { fill: "#34d399", stroke: "#059669", glow: "rgba(52,211,153,0.4)" };
    case "E73_Information_Object":
      return { fill: "#a78bfa", stroke: "#7c3aed", glow: "rgba(167,139,250,0.45)" };
    case "E28_Conceptual_Object":
      return { fill: "#f472b6", stroke: "#db2777", glow: "rgba(244,114,182,0.4)" };
    case "E74_Group":
      return { fill: "#c084fc", stroke: "#9333ea", glow: "rgba(192,132,252,0.4)" };
    case "E21_Person":
      return { fill: "#10b981", stroke: "#047857", glow: "rgba(16,185,129,0.45)" };
    case "E13_Attribute_Assignment":
      return { fill: "#fb923c", stroke: "#ea580c", glow: "rgba(251,146,60,0.45)" };
    case "E55_Type":
      return { fill: "#ec4899", stroke: "#be185d", glow: "rgba(236,72,153,0.45)" };
    default:
      return { fill: "#a1a1aa", stroke: "#52525b", glow: "rgba(161,161,170,0.35)" };
  }
}

function refForNode(n: { kind: string; raw: ApiNode }): string {
  const r = n.raw;
  switch (n.kind) {
    case "E21_Person":
      return `E21_Person:${String(r.id || "")}`;
    case "E53_Place":
      return `E53_Place:${String(r.name || "")}`;
    case "E28_Conceptual_Object":
      return `E28_Conceptual_Object:${String(r.name || "")}`;
    case "E73_Information_Object":
      return `E73_Information_Object:${String(r.key || r.id || "")}`;
    case "E7_Activity":
      return `Event:${String(r.key || "")}`;
    case "E52_Time_Span":
      return `E52_Time_Span:${String(r.key || r.date || "")}`;
    case "E55_Type":
      return `E55_Type:${String(r.name || "")}`;
    case "E74_Group":
      return `E74_Group:${String(r.name || "")}`;
    case "E13_Attribute_Assignment":
      return `E13_Attribute_Assignment:${String(r.key || r.id || "")}`;
    default:
      return "";
  }
}

function kindEmoji(kind: string): string {
  switch (kind) {
    case "E21_Person":
      return "◎";
    case "E7_Activity":
      return "◆";
    case "E53_Place":
      return "⌖";
    case "E73_Information_Object":
      return "▤";
    case "E28_Conceptual_Object":
      return "◇";
    case "E74_Group":
      return "▣";
    case "E55_Type":
      return "✦";
    case "E13_Attribute_Assignment":
      return "◎";
    case "E52_Time_Span":
      return "◷";
    default:
      return "○";
  }
}

function shortTitle(label: string, max = 22): string {
  const line = label.split("\n")[0].trim();
  if (line.length <= max) return line;
  return line.slice(0, max - 1) + "…";
}

type GraphNode = {
  id: string;
  kind: string;
  label: string;
  ref: string;
};

type GraphEdge = { source: string; target: string; type: string; label: string };

type LayoutPos = { x: number; y: number; depth: number; radius: number };

function computeMindMapLayout(
  nodes: GraphNode[],
  edges: GraphEdge[],
  centerId: string | null
): {
  positions: Map<string, LayoutPos>;
  treeEdges: GraphEdge[];
  branchNodes: GraphNode[];
  centerId: string | null;
} {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  if (!nodes.length) {
    return { positions: new Map(), treeEdges: [], branchNodes: [], centerId: null };
  }

  let cid = centerId;
  if (!cid || !byId.has(cid)) {
    cid = nodes[0].id;
  }

  const adj = new Map<string, Set<string>>();
  for (const n of nodes) adj.set(n.id, new Set());
  for (const e of edges) {
    adj.get(e.source)?.add(e.target);
    adj.get(e.target)?.add(e.source);
  }

  const depth = new Map<string, number>();
  const parent = new Map<string, string>();
  const q: string[] = [cid];
  depth.set(cid, 0);

  while (q.length) {
    const u = q.shift()!;
    const du = depth.get(u)!;
    for (const v of adj.get(u) || []) {
      if (!depth.has(v)) {
        depth.set(v, du + 1);
        parent.set(v, u);
        q.push(v);
      }
    }
  }

  for (const n of nodes) {
    if (!depth.has(n.id)) {
      depth.set(n.id, 99);
      parent.set(n.id, cid);
    }
  }

  const treeEdges: GraphEdge[] = [];
  for (const n of nodes) {
    if (n.id === cid) continue;
    const p = parent.get(n.id);
    if (p && byId.has(p)) {
      treeEdges.push({ source: p, target: n.id, type: "tree", label: "" });
    }
  }

  const maxD = Math.min(8, Math.max(...nodes.map((n) => depth.get(n.id) || 0)));
  const layers = new Map<number, string[]>();
  for (const n of nodes) {
    const d = Math.min(depth.get(n.id) || 0, maxD);
    if (!layers.has(d)) layers.set(d, []);
    layers.get(d)!.push(n.id);
  }
  for (const [, ids] of layers) {
    ids.sort((a, b) => {
      const ka = byId.get(a)?.kind || "";
      const kb = byId.get(b)?.kind || "";
      if (ka !== kb) return ka.localeCompare(kb);
      return (byId.get(a)?.label || "").localeCompare(byId.get(b)?.label || "");
    });
  }

  const positions = new Map<string, LayoutPos>();
  positions.set(cid, { x: 0, y: 0, depth: 0, radius: 76 });

  const baseRing = 150;
  const ringGap = 118;

  for (let d = 1; d <= maxD; d++) {
    const ids = layers.get(d) || [];
    if (!ids.length) continue;
    const n = ids.length;
    const spread = 1 + Math.max(0, n - 8) * 0.06;
    const rad = (baseRing + (d - 1) * ringGap) * spread;
    const phase = d % 2 === 0 ? Math.PI / n : 0;
    const rNode = d === 1 ? 46 : d === 2 ? 38 : 32;

    for (let i = 0; i < n; i++) {
      const ang = -Math.PI / 2 + (2 * Math.PI * i) / n + phase;
      const id = ids[i];
      positions.set(id, {
        x: rad * Math.cos(ang),
        y: rad * Math.sin(ang),
        depth: d,
        radius: rNode,
      });
    }
  }

  const branchNodes = (layers.get(1) || [])
    .map((id) => byId.get(id))
    .filter((x): x is GraphNode => Boolean(x));
  return { positions, treeEdges, branchNodes, centerId: cid };
}

function bezierMindLink(
  x1: number,
  y1: number,
  x2: number,
  y2: number,
  depth: number
): string {
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const dx = x2 - x1;
  const dy = y2 - y1;
  const len = Math.hypot(dx, dy) || 1;
  const nx = -dy / len;
  const ny = dx / len;
  const bow = 28 + depth * 10;
  const cx = mx + nx * bow;
  const cy = my + ny * bow;
  return `M ${x1.toFixed(1)} ${y1.toFixed(1)} Q ${cx.toFixed(1)} ${cy.toFixed(1)} ${x2.toFixed(1)} ${y2.toFixed(1)}`;
}

export function GraphMindMap({ initialRoots }: { initialRoots: RootEntity[] }) {
  const isDark = useIsDark();
  const reducedMotion = usePrefersReducedMotion();

  const [roots, setRoots] = useState<RootEntity[]>(initialRoots || []);
  const [mapRootRef, setMapRootRef] = useState<string>(initialRoots?.[0]?.ref || "");
  const [focusedRef, setFocusedRef] = useState<string>("");
  const [mergedData, setMergedData] = useState<GraphApiOut>({ nodes: [], edges: [] });
  const [fetchStatus, setFetchStatus] = useState<string>("");
  const [compactMode, setCompactMode] = useState(true);
  const [exploreMode, setExploreMode] = useState(true);

  const [panelOpen, setPanelOpen] = useState(true);
  const [overview, setOverview] = useState<OverviewAny | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewErr, setOverviewErr] = useState<string>("");
  const [expandBusy, setExpandBusy] = useState(false);

  const [trail, setTrail] = useState<string[]>([]);

  const [hoverId, setHoverId] = useState<string | null>(null);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const dragRef = useRef<{ px: number; py: number; sx: number; sy: number } | null>(null);

  useEffect(() => {
    if (roots.length) return;
    apiGet<{ items: RootEntity[] }>("/entities?limit=120")
      .then((out) => {
        const items = out.items || [];
        setRoots(items);
        if (!mapRootRef && items[0]?.ref) {
          setMapRootRef(items[0].ref);
          setFocusedRef(items[0].ref);
        }
      })
      .catch(() => {});
  }, [roots.length, mapRootRef]);

  const loadNeighborhood = useCallback(async (ref: string, depth: number): Promise<GraphApiOut> => {
    const out = await apiGet<GraphApiOut>(
      `/graph/neighborhood?ref=${encodeURIComponent(ref)}&depth=${depth}&limit=400`
    );
    return { nodes: out.nodes || [], edges: out.edges || [] };
  }, []);

  useEffect(() => {
    if (!mapRootRef) return;
    let ignore = false;
    setFetchStatus("");
    loadNeighborhood(mapRootRef, INITIAL_DEPTH)
      .then((out) => {
        if (!ignore) {
          setMergedData(out);
          setFocusedRef(mapRootRef);
          setTrail([mapRootRef]);
          setPan({ x: 0, y: 0 });
          setZoom(1);
        }
      })
      .catch((e: unknown) => {
        if (!ignore) setFetchStatus(e instanceof Error ? e.message : String(e));
      });
    return () => {
      ignore = true;
    };
  }, [mapRootRef, loadNeighborhood]);

  const fetchOverview = useCallback(async (ref: string) => {
    if (!ref) {
      setOverview(null);
      return;
    }
    setOverviewLoading(true);
    setOverviewErr("");
    try {
      const o = await apiGet<OverviewAny>(
        `/entity/overview?ref=${encodeURIComponent(ref)}&limit=20&focus=hub`
      );
      setOverview(o);
    } catch (e: unknown) {
      setOverview(null);
      setOverviewErr(e instanceof Error ? e.message : String(e));
    } finally {
      setOverviewLoading(false);
    }
  }, []);

  useEffect(() => {
    if (focusedRef) void fetchOverview(focusedRef);
  }, [focusedRef, fetchOverview]);

  const expandFromRef = useCallback(
    async (ref: string) => {
      if (!ref) return;
      setExpandBusy(true);
      setFetchStatus("");
      try {
        const add = await loadNeighborhood(ref, EXPAND_DEPTH);
        setMergedData((prev) => {
          const next = mergeGraph(prev, add);
          if (next.nodes.length > MAX_GRAPH_NODES) {
            setFetchStatus(
              `Graph has ${next.nodes.length} nodes (cap ~${MAX_GRAPH_NODES}). Try “Reset map” or a narrower root.`
            );
          }
          return next;
        });
        setTrail((t) => (t[t.length - 1] === ref ? t : [...t, ref].slice(-12)));
      } catch (e: unknown) {
        setFetchStatus(e instanceof Error ? e.message : String(e));
      } finally {
        setExpandBusy(false);
      }
    },
    [loadNeighborhood]
  );

  const elements = useMemo(() => {
    const data = mergedData;
    const nodesAll = (data.nodes || []).map((n) => {
      const kind = kindFromLabels(n._labels);
      return {
        id: n._elementId,
        kind,
        label: labelForNode(n),
        raw: n,
        ref: refForNode({ kind, raw: n }),
      };
    });
    const nodes = nodesAll.filter((n) => n.kind !== "DisambiguationTask");
    const nodeIds = new Set(nodes.map((n) => n.id));
    const edgeMap = new Map<string, GraphEdge>();
    for (const e of data.edges || []) {
      if (!nodeIds.has(e.start) || !nodeIds.has(e.end)) continue;
      const a = e.start < e.end ? e.start : e.end;
      const b = e.start < e.end ? e.end : e.start;
      const key = `${e.type}|${a}|${b}`;
      if (edgeMap.has(key)) continue;
      edgeMap.set(key, {
        source: e.start,
        target: e.end,
        type: e.type,
        label: humanizeRel(e.type),
      });
    }
    const edges = Array.from(edgeMap.values());

    const adj = new Map<string, Set<string>>();
    for (const e of edges) {
      if (!adj.has(e.source)) adj.set(e.source, new Set());
      if (!adj.has(e.target)) adj.set(e.target, new Set());
      adj.get(e.source)!.add(e.target);
      adj.get(e.target)!.add(e.source);
    }

    const root = nodes.find((n) => n.ref === mapRootRef);
    let nodes2 = nodes;
    let edges2 = edges;

    if (!exploreMode && root) {
      const keep = new Set<string>([root.id]);
      for (const nbr of Array.from(adj.get(root.id) || [])) {
        keep.add(nbr);
        const node = nodes.find((x) => x.id === nbr);
        if (node?.kind === "E7_Activity") {
          for (const ctx of Array.from(adj.get(nbr) || [])) keep.add(ctx);
        }
      }
      nodes2 = nodes.filter((n) => keep.has(n.id));
      const ids2 = new Set(nodes2.map((n) => n.id));
      edges2 = edges.filter((e) => ids2.has(e.source) && ids2.has(e.target));
    }

    const nodeById = new Map(nodes2.map((n) => [n.id, n]));
    const eventCtx = new Map<
      string,
      { day?: string; place?: string; topics: Set<string>; emotions: Set<string>; type?: string }
    >();
    for (const e of edges2) {
      const s = nodeById.get(e.source);
      const t = nodeById.get(e.target);
      const eventId = s?.kind === "E7_Activity" ? e.source : t?.kind === "E7_Activity" ? e.target : "";
      if (!eventId) continue;
      const otherId = eventId === e.source ? e.target : e.source;
      const other = nodeById.get(otherId);
      if (!other) continue;
      if (!eventCtx.has(eventId)) eventCtx.set(eventId, { topics: new Set(), emotions: new Set() });
      const ctx = eventCtx.get(eventId)!;
      if (e.type === "P4_has_time_span" && other.kind === "E52_Time_Span") ctx.day = other.label;
      if (e.type === "P7_took_place_at" && other.kind === "E53_Place") ctx.place = other.label;
      if (e.type === "P67_refers_to" && (other.kind === "E28_Conceptual_Object" || other.kind === "E74_Group"))
        ctx.topics.add(other.label);
      if (e.type === "P67_refers_to" && other.kind === "E55_Type") ctx.emotions.add(other.label);
      if (e.type === "P2_has_type" && other.kind === "E55_Type") ctx.type = other.label;
    }

    const nodes3: GraphNode[] = nodes2.map((n) => {
      const prefix = n.kind || "Node";
      if (!compactMode) {
        let base = n.label;
        if (n.kind === "E7_Activity") {
          const ctx = eventCtx.get(n.id);
          if (ctx?.place) base = `${base} @ ${ctx.place}`;
        }
        const label = n.kind === "E73_Information_Object" ? `Entry: ${base}` : `${prefix}: ${base}`;
        return { id: n.id, kind: n.kind, label, ref: n.ref };
      }
      if (n.kind === "E7_Activity") {
        const ctx = eventCtx.get(n.id);
        const raw = (n.raw || {}) as Record<string, unknown>;
        const day =
          ctx?.day ||
          (typeof raw.event_time_iso === "string" ? String(raw.event_time_iso).slice(0, 10) : "") ||
          (typeof raw.key === "string" ? String(raw.key).split("|")[0] : "");
        const place = ctx?.place || "";
        const typeVal =
          ctx?.type || (typeof raw.event_type === "string" ? String(raw.event_type) : "") || n.label;
        const topics = Array.from(ctx?.topics || []).slice(0, 2);
        const line1 = typeVal;
        const line2 = [day, place].filter(Boolean).join(" · ");
        const line3 = topics.length ? topics.join(" · ") : "";
        const label = [line1, line2, line3].filter(Boolean).join("\n");
        return { id: n.id, kind: n.kind, label, ref: n.ref };
      }
      if (n.kind === "E13_Attribute_Assignment") {
        return { id: n.id, kind: n.kind, label: n.label.slice(0, 96) + (n.label.length > 96 ? "…" : ""), ref: n.ref };
      }
      const label =
        n.kind === "E73_Information_Object"
          ? n.label
          : n.kind === "E21_Person"
            ? n.label
            : `${prefix.slice(0, 3)} · ${n.label}`;
      return { id: n.id, kind: n.kind, label, ref: n.ref };
    });

    if (!compactMode) return { nodes: nodes3, edges: edges2 };

    const baseKinds = new Set<string>([
      "E21_Person",
      "E7_Activity",
      "E73_Information_Object",
      "E28_Conceptual_Object",
      "E52_Time_Span",
      "E53_Place",
      "E39_Actor",
      "E74_Group",
      "E13_Attribute_Assignment",
      "E55_Type",
    ]);
    const idsDisplayed = new Set<string>();
    for (const n of nodes3) {
      if (baseKinds.has(n.kind)) idsDisplayed.add(n.id);
    }
    const nodesDisplayed = nodes3.filter((n) => idsDisplayed.has(n.id));
    const edgesDisplayed = edges2.filter((e) => idsDisplayed.has(e.source) && idsDisplayed.has(e.target));
    return { nodes: nodesDisplayed, edges: edgesDisplayed };
  }, [mergedData, mapRootRef, compactMode, exploreMode]);

  const centerNodeId = useMemo(() => {
    const hit = elements.nodes.find((n) => n.ref === mapRootRef);
    return hit?.id ?? elements.nodes[0]?.id ?? null;
  }, [elements.nodes, mapRootRef]);

  const layout = useMemo(
    () => computeMindMapLayout(elements.nodes, elements.edges, centerNodeId),
    [elements.nodes, elements.edges, centerNodeId]
  );

  const rootDisplayName =
    roots.find((r) => r.ref === mapRootRef)?.name ||
    elements.nodes.find((n) => n.id === layout.centerId)?.label.split("\n")[0] ||
    "Your map";

  const bounds = useMemo(() => {
    let minX = 0,
      minY = 0,
      maxX = 0,
      maxY = 0;
    for (const [, p] of layout.positions) {
      const pad = p.radius + 40;
      minX = Math.min(minX, p.x - pad);
      minY = Math.min(minY, p.y - pad);
      maxX = Math.max(maxX, p.x + pad);
      maxY = Math.max(maxY, p.y + pad);
    }
    const margin = 120;
    return { minX: minX - margin, minY: minY - margin, w: maxX - minX + margin * 2, h: maxY - minY + margin * 2 };
  }, [layout.positions]);

  const handleNodeClick = useCallback((ref: string) => {
    if (!ref) return;
    setFocusedRef(ref);
    setPanelOpen(true);
    setTrail((t) => (t[t.length - 1] === ref ? t : [...t, ref].slice(-12)));
  }, []);

  const panelTitle = overview?.activity_name || overview?.name || focusedRef || "Entity";

  const hubGradient = isDark
    ? { id: "hubGrad", c1: "#4f46e5", c2: "#7c3aed" }
    : { id: "hubGrad", c1: "#2563eb", c2: "#4f46e5" };

  const bgCanvas = isDark ? "linear-gradient(165deg, #0c0c12 0%, #09090b 45%, #0f172a 100%)" : "linear-gradient(165deg, #eff6ff 0%, #f8fafc 50%, #e0f2fe 100%)";

  const transition = reducedMotion ? "none" : "transform 0.35s cubic-bezier(0.22, 1, 0.36, 1)";

  return (
    <div className="mt-3 space-y-3">
      <p className="text-[12px] leading-relaxed text-zinc-600 dark:text-zinc-400">
        Radial mind map: the <span className="font-medium text-zinc-800 dark:text-zinc-200">center</span> is your map
        root; branches are linked knowledge. Click any bubble to open the detail card — same API as Entity Timeline.{" "}
        <span className="font-medium text-zinc-800 dark:text-zinc-200">Grow map</span> adds neighbors without losing the
        view.
      </p>

      <div className="flex flex-wrap items-center gap-2">
        <div className="text-xs font-semibold text-zinc-500 dark:text-zinc-300">Center idea</div>
        <select
          value={mapRootRef}
          onChange={(e) => setMapRootRef(e.target.value)}
          className="rounded-xl border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 px-3 py-2 text-xs text-zinc-700 dark:text-zinc-200 outline-none max-w-[min(100%,280px)] shadow-sm"
        >
          {roots.map((r) => (
            <option key={r.ref} value={r.ref}>
              {r.type}: {r.name}
            </option>
          ))}
        </select>
        <button
          type="button"
          onClick={() => {
            setMergedData({ nodes: [], edges: [] });
            void loadNeighborhood(mapRootRef, INITIAL_DEPTH).then(setMergedData).catch(() => {});
            setTrail([mapRootRef]);
            setFocusedRef(mapRootRef);
            setPan({ x: 0, y: 0 });
            setZoom(1);
          }}
          className="rounded-xl border border-zinc-200 dark:border-zinc-700 bg-white dark:bg-zinc-900 px-3 py-2 text-[11px] font-semibold text-zinc-700 dark:text-zinc-200 shadow-sm"
        >
          Reset
        </button>
        {trail.length > 1 ? (
          <button
            type="button"
            onClick={() => {
              const prev = trail[trail.length - 2];
              if (prev) {
                setTrail((t) => t.slice(0, -1));
                setFocusedRef(prev);
              }
            }}
            className="rounded-xl border border-zinc-200 dark:border-zinc-700 px-3 py-2 text-[11px] font-medium text-zinc-600 dark:text-zinc-300"
          >
            ← Back
          </button>
        ) : null}
        <button
          type="button"
          onClick={() => {
            setPan({ x: 0, y: 0 });
            setZoom(1);
          }}
          className="rounded-xl border border-zinc-200 dark:border-zinc-700 px-3 py-2 text-[11px] text-zinc-500"
        >
          Fit view
        </button>
      </div>

      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 text-[11px] text-zinc-500">
        <label className="flex items-center gap-2 select-none cursor-pointer">
          <input
            type="checkbox"
            checked={compactMode}
            onChange={(e) => setCompactMode(e.target.checked)}
            className="h-4 w-4 rounded border-zinc-300 dark:border-zinc-700"
          />
          Compact labels
        </label>
        <label className="flex items-center gap-2 select-none cursor-pointer">
          <input
            type="checkbox"
            checked={exploreMode}
            onChange={(e) => setExploreMode(e.target.checked)}
            className="h-4 w-4 rounded border-zinc-300 dark:border-zinc-700"
          />
          Full explored graph
        </label>
        <label className="flex items-center gap-2 select-none cursor-pointer">
          <input
            type="checkbox"
            checked={panelOpen}
            onChange={(e) => setPanelOpen(e.target.checked)}
            className="h-4 w-4 rounded border-zinc-300 dark:border-zinc-700"
          />
          Detail panel
        </label>
        <span>
          {elements.nodes.length} nodes · {layout.treeEdges.length} branches
        </span>
      </div>

      {fetchStatus ? (
        <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-800 dark:text-amber-200">
          {fetchStatus}
        </div>
      ) : null}

      <div className="flex flex-col xl:flex-row gap-3 xl:items-stretch">
        <div
          className="min-w-0 flex-1 overflow-hidden rounded-2xl border border-zinc-200/80 dark:border-zinc-800 shadow-lg dark:shadow-none"
          style={{ background: bgCanvas }}
        >
          <div className="flex min-h-[min(78vh,820px)] flex-col md:flex-row">
            {/* Prezi-style topic rail */}
            <aside className="w-full shrink-0 border-b border-white/10 bg-black/5 dark:bg-white/[0.03] md:w-[200px] md:border-b-0 md:border-r dark:border-zinc-800 p-3 flex flex-col gap-3">
              <div className="text-[10px] font-bold uppercase tracking-widest text-zinc-500 dark:text-zinc-400">
                Branches
              </div>
              <div className="rounded-xl bg-white/70 dark:bg-zinc-950/50 border border-zinc-200/60 dark:border-zinc-800 p-2 aspect-[4/3] flex items-center justify-center">
                <div className="text-center text-[10px] text-zinc-500 leading-tight">
                  <div className="font-semibold text-zinc-700 dark:text-zinc-300 mb-1">Overview</div>
                  {layout.branchNodes.length} direct links
                  <br />
                  from center
                </div>
              </div>
              <ol className="flex-1 space-y-1 overflow-y-auto max-h-[220px] md:max-h-none text-[11px]">
                {layout.branchNodes.map((bn, i) => (
                  <li key={bn.id}>
                    <button
                      type="button"
                      onClick={() => bn.ref && handleNodeClick(bn.ref)}
                      className={[
                        "w-full text-left rounded-lg px-2 py-2 transition-colors",
                        focusedRef === bn.ref
                          ? "bg-indigo-500/20 text-indigo-900 dark:text-indigo-100 ring-1 ring-indigo-400/40"
                          : "hover:bg-white/60 dark:hover:bg-zinc-900/80 text-zinc-700 dark:text-zinc-300",
                      ].join(" ")}
                    >
                      <span className="text-zinc-400 mr-1">{i + 1}.</span>
                      <span className="mr-1 opacity-80">{kindEmoji(bn.kind)}</span>
                      <span className="font-medium line-clamp-2">{shortTitle(bn.label, 40)}</span>
                    </button>
                  </li>
                ))}
                {!layout.branchNodes.length ? (
                  <li className="text-zinc-500 px-2 py-3">No branches yet — try Grow map.</li>
                ) : null}
              </ol>
            </aside>

            {/* SVG mind map */}
            <div
              className="relative flex-1 cursor-grab active:cursor-grabbing touch-none min-h-[420px]"
              onWheel={(e) => {
                e.preventDefault();
                const z = Math.max(0.35, Math.min(2.2, zoom * (e.deltaY > 0 ? 0.92 : 1.08)));
                setZoom(z);
              }}
              onMouseDown={(e) => {
                if (e.button !== 0) return;
                dragRef.current = { px: e.clientX, py: e.clientY, sx: pan.x, sy: pan.y };
              }}
              onMouseMove={(e) => {
                const d = dragRef.current;
                if (!d) return;
                setPan({ x: d.sx + (e.clientX - d.px), y: d.sy + (e.clientY - d.py) });
              }}
              onMouseUp={() => {
                dragRef.current = null;
              }}
              onMouseLeave={() => {
                dragRef.current = null;
              }}
            >
              <svg
                className="w-full h-full min-h-[420px]"
                viewBox={`${bounds.minX} ${bounds.minY} ${bounds.w} ${bounds.h}`}
                preserveAspectRatio="xMidYMid meet"
              >
                <defs>
                  <radialGradient id={hubGradient.id} cx="35%" cy="30%" r="70%">
                    <stop offset="0%" stopColor={hubGradient.c1} />
                    <stop offset="100%" stopColor={hubGradient.c2} />
                  </radialGradient>
                  <filter id="nodeShadow" x="-50%" y="-50%" width="200%" height="200%">
                    <feDropShadow
                      dx="0"
                      dy="4"
                      stdDeviation="6"
                      floodColor={isDark ? "#000" : "#64748b"}
                      floodOpacity={isDark ? "0.55" : "0.25"}
                    />
                  </filter>
                  <filter id="glow" x="-100%" y="-100%" width="300%" height="300%">
                    <feGaussianBlur stdDeviation="4" result="b" />
                    <feMerge>
                      <feMergeNode in="b" />
                      <feMergeNode in="SourceGraphic" />
                    </feMerge>
                  </filter>
                </defs>

                <g transform={`translate(${pan.x} ${pan.y}) scale(${zoom})`}>
                  {layout.treeEdges.map((e) => {
                    const pa = layout.positions.get(e.source);
                    const pb = layout.positions.get(e.target);
                    if (!pa || !pb) return null;
                    const depth = pb.depth;
                    const d = bezierMindLink(pa.x, pa.y, pb.x, pb.y, depth);
                    const focused =
                      elements.nodes.find((n) => n.id === e.target)?.ref === focusedRef ||
                      elements.nodes.find((n) => n.id === e.source)?.ref === focusedRef;
                    return (
                      <path
                        key={`${e.source}-${e.target}`}
                        d={d}
                        fill="none"
                        stroke={isDark ? "rgba(148,163,184,0.35)" : "rgba(59,130,246,0.35)"}
                        strokeWidth={focused ? 3.2 : 2}
                        strokeLinecap="round"
                        style={{ transition: reducedMotion ? "none" : "stroke-width 0.25s ease" }}
                      />
                    );
                  })}

                  {elements.nodes.map((n) => {
                    const p = layout.positions.get(n.id);
                    if (!p) return null;
                    const colors = colorForKind(n.kind);
                    const isHub = n.id === layout.centerId;
                    const isFocused = n.ref === focusedRef;
                    const isHover = hoverId === n.id;
                    const r = isHub ? p.radius : isHover ? p.radius * 1.08 : p.radius;
                    const lines = n.label.split("\n").slice(0, 3);
                    const hubLines = isHub ? [shortTitle(rootDisplayName, 24), ...lines.slice(1)] : lines;
                    const fs = isHub ? 13 : p.depth <= 1 ? 11 : 10;
                    const scale = isHover && !reducedMotion ? 1.06 : 1;

                    return (
                      <g
                        key={n.id}
                        transform={`translate(${p.x},${p.y}) scale(${scale})`}
                        style={{
                          transition,
                          cursor: n.ref ? "pointer" : "default",
                        }}
                        onMouseEnter={() => setHoverId(n.id)}
                        onMouseLeave={() => setHoverId(null)}
                        onClick={(ev) => {
                          ev.stopPropagation();
                          if (n.ref) handleNodeClick(n.ref);
                        }}
                      >
                        {!isHub ? (
                          <circle
                            r={r + 6}
                            fill={colors.glow}
                            opacity={isFocused ? 0.55 : 0.2}
                            style={{ transition }}
                          />
                        ) : null}
                        <circle
                          r={r}
                          fill={isHub ? `url(#${hubGradient.id})` : colors.fill}
                          stroke={
                            isFocused ? (isDark ? "#fbbf24" : "#ca8a04") : isHub ? "rgba(255,255,255,0.35)" : colors.stroke
                          }
                          strokeWidth={isFocused ? 4 : isHub ? 3 : 2}
                          filter="url(#nodeShadow)"
                          style={{ transition }}
                        />
                        <text
                          textAnchor="middle"
                          fill={isHub ? "#ffffff" : isDark ? "#fafafa" : "#18181b"}
                          fontSize={fs}
                          fontWeight={isHub ? 700 : 600}
                          style={{ pointerEvents: "none", userSelect: "none" }}
                        >
                          {hubLines.map((line, i) => (
                            <tspan key={i} x={0} dy={i === 0 ? (isHub ? -6 : -4) : 14}>
                              {shortTitle(line, isHub ? 18 : 16)}
                            </tspan>
                          ))}
                        </text>
                        {isHub ? (
                          <text
                            y={r - 10}
                            textAnchor="middle"
                            fill="rgba(255,255,255,0.85)"
                            fontSize={9}
                            fontWeight={700}
                            letterSpacing="0.12em"
                            style={{ pointerEvents: "none" }}
                          >
                            CENTER
                          </text>
                        ) : null}
                      </g>
                    );
                  })}
                </g>
              </svg>

              <div className="pointer-events-none absolute bottom-3 right-3 rounded-full bg-black/40 dark:bg-white/10 px-3 py-1 text-[10px] font-medium text-white backdrop-blur-sm">
                Drag to pan · Wheel to zoom
              </div>
            </div>
          </div>
        </div>

        {panelOpen ? (
          <aside className="w-full shrink-0 xl:w-[380px] xl:max-w-[40vw] rounded-2xl border border-zinc-200 dark:border-zinc-800 bg-white/90 dark:bg-zinc-950/90 backdrop-blur-md p-4 flex flex-col gap-3 max-h-[min(78vh,820px)] overflow-y-auto shadow-xl">
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-wide text-zinc-500">Card</div>
                <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100 leading-snug">{panelTitle}</h3>
                {focusedRef ? (
                  <div className="mt-1 font-mono text-[10px] text-zinc-500 break-all">{focusedRef}</div>
                ) : null}
              </div>
              <button
                type="button"
                onClick={() => setPanelOpen(false)}
                className="text-[11px] text-zinc-500 hover:text-zinc-800 dark:hover:text-zinc-200"
              >
                Hide
              </button>
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                disabled={!focusedRef || expandBusy}
                onClick={() => void expandFromRef(focusedRef)}
                className="rounded-xl bg-gradient-to-r from-indigo-600 to-violet-600 px-3 py-2 text-xs font-bold text-white shadow-md disabled:opacity-40"
              >
                {expandBusy ? "Growing…" : "Grow map"}
              </button>
              <button
                type="button"
                disabled={!focusedRef}
                onClick={() => focusedRef && setMapRootRef(focusedRef)}
                className="rounded-xl border border-zinc-300 dark:border-zinc-600 px-3 py-2 text-xs font-semibold text-zinc-700 dark:text-zinc-200 disabled:opacity-40"
              >
                Make center
              </button>
            </div>

            {overviewLoading ? (
              <div className="text-xs text-zinc-500">Loading details…</div>
            ) : overviewErr ? (
              <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 p-2 text-xs text-rose-800 dark:text-rose-200">
                {overviewErr}
                <div className="mt-2 text-zinc-600 dark:text-zinc-400">
                  You can still use <b>Grow map</b> to pull more links onto the canvas.
                </div>
              </div>
            ) : overview ? (
              <EntityOverviewCard o={overview} onJump={(ref) => handleNodeClick(ref)} />
            ) : (
              <div className="text-xs text-zinc-500">Click a bubble on the map.</div>
            )}
          </aside>
        ) : null}
      </div>

      <div className="text-[11px] text-zinc-500">
        Center label: <span className="font-medium text-zinc-700 dark:text-zinc-300">{shortTitle(rootDisplayName, 48)}</span>
        — change with the dropdown or <b>Make center</b> on any card.
      </div>
    </div>
  );
}

function EntityOverviewCard({ o, onJump }: { o: OverviewAny; onJump: (ref: string) => void }) {
  const k = o.kind || "";
  return (
    <div className="space-y-3 text-xs text-zinc-700 dark:text-zinc-300">
      <div className="inline-flex rounded-full bg-zinc-200/80 dark:bg-zinc-800 px-2 py-0.5 text-[10px] font-semibold text-zinc-600 dark:text-zinc-400">
        {k || "Entity"}
      </div>

      {k === "Person" ? (
        <>
          {o.role ? <div className="text-zinc-500">Role: {o.role}</div> : null}
          {typeof o.mentions === "number" ? <div>Mentions: {o.mentions}</div> : null}
          {o.feeling_tags && o.feeling_tags.length > 0 ? (
            <div>
              <div className="font-semibold text-zinc-800 dark:text-zinc-200 mb-1">Feelings (sample)</div>
              <ul className="space-y-1">
                {o.feeling_tags.slice(0, 8).map((t) => (
                  <li key={t.ref}>
                    <button
                      type="button"
                      className="text-left text-indigo-600 dark:text-indigo-400 underline-offset-2 hover:underline"
                      onClick={() => onJump(t.ref)}
                    >
                      {t.name}
                    </button>
                    {typeof t.count === "number" ? <span className="text-zinc-500"> · {t.count}</span> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : null}

      {k === "Event" ? (
        <>
          {o.day ? <div>Day: {o.day}</div> : null}
          {o.event_time_text || o.event_time_iso ? (
            <div>
              Time: {o.event_time_text || ""}{" "}
              {o.event_time_iso ? <span className="text-zinc-500">({o.event_time_iso})</span> : null}
            </div>
          ) : null}
          {o.places && o.places.length > 0 ? <div>Places: {o.places.join(", ")}</div> : null}
          {o.summary_preview ? (
            <div className="rounded-lg bg-white/60 dark:bg-zinc-950/60 p-2 text-[11px] leading-relaxed border border-zinc-200/60 dark:border-zinc-800">
              {o.summary_preview}
            </div>
          ) : null}
          {o.persons && o.persons.length > 0 ? (
            <div>
              <div className="font-semibold text-zinc-800 dark:text-zinc-200 mb-1">People</div>
              <ul className="space-y-1">
                {o.persons.map((p) => (
                  <li key={p.id || p.name}>
                    {p.id ? (
                      <button
                        type="button"
                        className="text-indigo-600 dark:text-indigo-400 underline-offset-2 hover:underline"
                        onClick={() => onJump(`E21_Person:${p.id}`)}
                      >
                        {p.name}
                      </button>
                    ) : (
                      p.name
                    )}
                    {p.role ? <span className="text-zinc-500"> · {p.role}</span> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : null}

      {k === "Feeling" ? (
        <>
          {Array.isArray(o.occurrences) ? <div>Occurrences: {o.occurrences.length}</div> : null}
          <div className="text-zinc-500">Open Entity Timeline for full occurrence list.</div>
        </>
      ) : null}

      {k === "Day" ? (
        <>
          {Array.isArray(o.situations) ? <div>Situations: {o.situations.length}</div> : null}
          {Array.isArray(o.entries) ? <div>Journal rows: {o.entries.length}</div> : null}
        </>
      ) : null}

      {k === "E73_Information_Object" ? (
        <>
          {o.entry_kind ? <div className="text-zinc-500">Kind: {o.entry_kind}</div> : null}
          {o.text ? (
            <div className="rounded-lg bg-white/60 dark:bg-zinc-950/60 p-2 text-[11px] leading-relaxed border border-zinc-200/60 dark:border-zinc-800 max-h-40 overflow-y-auto whitespace-pre-wrap">
              {o.text.slice(0, 1200)}
              {o.text.length > 1200 ? "…" : ""}
            </div>
          ) : null}
          {o.linked && o.linked.length > 0 ? (
            <div>
              <div className="font-semibold text-zinc-800 dark:text-zinc-200 mb-1">Linked</div>
              <ul className="space-y-1 max-h-36 overflow-y-auto">
                {o.linked.slice(0, 24).map((l) => (
                  <li key={l.ref}>
                    <button
                      type="button"
                      className="text-left text-indigo-600 dark:text-indigo-400 underline-offset-2 hover:underline"
                      onClick={() => onJump(l.ref)}
                    >
                      {l.name}
                    </button>
                    {l.bucket ? <span className="text-zinc-500"> · {l.bucket}</span> : null}
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      ) : null}

      {o.entries && o.entries.length > 0 ? (
        <div>
          <div className="font-semibold text-zinc-800 dark:text-zinc-200 mb-1">Journal</div>
          <ul className="space-y-2">
            {o.entries.slice(0, 6).map((e) => (
              <li key={e.entry_id} className="rounded-md border border-zinc-200/80 dark:border-zinc-800 p-2">
                <div className="text-[10px] text-zinc-500">
                  {e.day || ""} · {e.input_time || ""}
                </div>
                <div className="text-[11px] mt-0.5">{e.text_preview || "—"}</div>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
