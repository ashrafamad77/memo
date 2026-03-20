"use client";

import cytoscape, { Core } from "cytoscape";
import { useEffect, useMemo, useRef, useState } from "react";

import { apiGet } from "@/lib/api";

type RootEntity = { ref: string; name: string; type: string; mentions?: number };

type ApiNode = {
  _elementId: string;
  _labels?: string[];
  [k: string]: any;
};

type ApiEdge = {
  type: string;
  start: string;
  end: string;
  properties?: Record<string, any>;
};

type GraphApiOut = { nodes: ApiNode[]; edges: ApiEdge[] };

function kindFromLabels(labels?: string[]) {
  const s = new Set(labels || []);
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
  // Prefer compact, human-friendly captions (Neo4j Browser style).
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
  return n.name || n.text || n.mention || n.date || (typeof n.key === "string" ? n.key.split("|")[0] : "") || n.id || "—";
}

function humanizeRel(type: string) {
  const t = String(type || "").trim();
  const map: Record<string, string> = {
    PARTICIPATED_IN: "participated in",
    OCCURRED_AT: "at",
    HAS_EMOTION: "emotion",
    P17_was_motivated_by: "P17 motivated by",
    P7_took_place_at: "P7 took place at",
    P4_has_time_span: "P4 has time-span",
    P14_carried_out_by: "P14 carried out by",
    P14i_performed: "P14i performed",
    P67_refers_to: "P67 refers to",
    FROM_ENTRY: "from entry",
    ON_DAY: "on day",
    HAS_ALIAS: "has alias",
    ALIAS_OF: "alias of",
  };
  if (map[t]) return map[t];
  return t ? t.toLowerCase().replace(/_/g, " ") : "rel";
}

function isDirectionalRel(type: string) {
  // In this graph, most relationships are directional. If you add true undirected
  // relationships later, put them here.
  const undirected = new Set<string>([]);
  return !undirected.has(String(type || "").trim());
}

function colorForKind(kind: string) {
  switch (kind) {
    case "E7_Activity":
      return "#fbbf24"; // amber-300
    case "E52_Time_Span":
      return "#fcd34d"; // amber-200
    case "E53_Place":
      return "#93c5fd"; // blue-300
    case "E39_Actor":
      return "#6ee7b7"; // emerald-300
    case "E73_Information_Object":
      return "#c4b5fd"; // violet-300
    case "E28_Conceptual_Object":
      return "#f9a8d4"; // pink-300
    case "E74_Group":
      return "#d1d5db"; // gray-300
    case "E21_Person":
      return "#34d399"; // emerald
    case "Alias":
      return "#e5e7eb"; // zinc-200
    case "DisambiguationTask":
      return "#fb7185"; // rose
    default:
      return "#e5e7eb"; // zinc-200
  }
}

export function GraphMindMap({
  initialRoots,
}: {
  initialRoots: RootEntity[];
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const [roots, setRoots] = useState<RootEntity[]>(initialRoots || []);
  const [selectedRef, setSelectedRef] = useState<string>(initialRoots?.[0]?.ref || "");
  const [data, setData] = useState<GraphApiOut>({ nodes: [], edges: [] });
  const [status, setStatus] = useState<string>("");
  const [compactMode, setCompactMode] = useState<boolean>(true);

  // Refs so cytoscape tap handler always reads latest state.
  const compactModeRef = useRef<boolean>(compactMode);
  const selectedRefRef = useRef<string>(selectedRef);

  useEffect(() => {
    compactModeRef.current = compactMode;
  }, [compactMode]);

  useEffect(() => {
    selectedRefRef.current = selectedRef;
  }, [selectedRef]);

  useEffect(() => {
    // Ensure we have a list of root entities for selection.
    if (roots.length) return;
    apiGet<{ items: RootEntity[] }>("/entities?limit=120")
      .then((out) => {
        const items = out.items || [];
        setRoots(items);
        if (!selectedRef && items[0]?.ref) setSelectedRef(items[0].ref);
      })
      .catch(() => {});
  }, [roots.length, selectedRef]);

  useEffect(() => {
    if (!selectedRef) return;
    let ignore = false;
    setStatus("");
    // depth=2 gives: root entity -> Event -> context nodes.
    apiGet<GraphApiOut>(
      `/graph/neighborhood?ref=${encodeURIComponent(selectedRef)}&depth=2`
    )
      .then((out) => {
        if (!ignore) setData(out);
      })
      .catch((e: any) => {
        if (!ignore) setStatus(e?.message || String(e));
      });
    return () => {
      ignore = true;
    };
  }, [selectedRef]);

  const elements = useMemo(() => {
    const nodesAll = (data.nodes || []).map((n) => {
      const kind = kindFromLabels(n._labels);
      return {
        id: n._elementId,
        kind,
        label: labelForNode(n),
        raw: n,
      };
    });
    const nodes = nodesAll.filter((n) => n.kind !== "DisambiguationTask");
    const nodeIds = new Set(nodes.map((n) => n.id));
    // Neo4j neighborhood queries often return duplicate edges (both directions,
    // repeated path expansions, etc.). Deduplicate so Cytoscape doesn't render
    // many parallel curves between the same two nodes.
    const edgeMap = new Map<
      string,
      { id: string; source: string; target: string; type: string; label: string; arrow: string }
    >();
    for (const e of data.edges || []) {
      if (!nodeIds.has(e.start) || !nodeIds.has(e.end)) continue;
      const a = e.start < e.end ? e.start : e.end;
      const b = e.start < e.end ? e.end : e.start;
      const key = `${e.type}|${a}|${b}`;
      if (edgeMap.has(key)) continue;
      const directed = isDirectionalRel(e.type);
      edgeMap.set(key, {
        id: key,
        source: e.start,
        target: e.end,
        type: e.type,
        label: humanizeRel(e.type),
        arrow: directed ? "triangle" : "none",
      });
    }
    const edges = Array.from(edgeMap.values());

    // Prune to: selected root + its events + event context (keeps it mind-map like)
    const root = nodes.find((n) => {
      if (!selectedRef) return false;
      const [label, key] = selectedRef.split(":", 2);
      if (label === "E21_Person") return n.kind === "E21_Person" && String(n.raw?.id || "") === key;
      if (label === "E53_Place") return n.kind === "E53_Place" && String(n.raw?.name || "") === key;
      if (label === "E28_Conceptual_Object") return n.kind === "E28_Conceptual_Object" && String(n.raw?.name || "") === key;
      if (label === "E73_Information_Object") return n.kind === "E73_Information_Object" && String(n.raw?.key || "") === key;
      if (label === "Event") return n.kind === "E7_Activity" && String(n.raw?.key || "") === key;
      if (label === "E52_Time_Span") return n.kind === "E52_Time_Span" && String(n.raw?.key || n.raw?.date || "") === key;
      if (label === "E55_Type") return n.kind === "E55_Type" && String(n.raw?.name || "") === key;
      return false;
    });
    if (!root) return { nodes, edges };
    const adj = new Map<string, Set<string>>();
    for (const e of edges) {
      if (!adj.has(e.source)) adj.set(e.source, new Set());
      if (!adj.has(e.target)) adj.set(e.target, new Set());
      adj.get(e.source)!.add(e.target);
      adj.get(e.target)!.add(e.source);
    }
    const keep = new Set<string>([root.id]);
    for (const nbr of Array.from(adj.get(root.id) || [])) {
      keep.add(nbr);
      const node = nodes.find((n) => n.id === nbr);
      if (node?.kind === "E7_Activity") {
        for (const ctx of Array.from(adj.get(nbr) || [])) keep.add(ctx);
      }
    }
    const nodes2 = nodes.filter((n) => keep.has(n.id));
    const ids2 = new Set(nodes2.map((n) => n.id));
    const edges2 = edges.filter((e) => ids2.has(e.source) && ids2.has(e.target));

    const nodeById = new Map(nodes2.map((n) => [n.id, n]));

    // Collect per-Event attributes by scanning edges to context nodes.
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

      if (!eventCtx.has(eventId)) {
        eventCtx.set(eventId, { topics: new Set(), emotions: new Set() });
      }
      const ctx = eventCtx.get(eventId)!;

      // Relationship-driven enrichment
      if (e.type === "P4_has_time_span" && other.kind === "E52_Time_Span") ctx.day = other.label;
      if (e.type === "P7_took_place_at" && other.kind === "E53_Place") ctx.place = other.label;
      if (e.type === "P67_refers_to" && (other.kind === "E28_Conceptual_Object" || other.kind === "E74_Group")) ctx.topics.add(other.label);
      if (e.type === "P67_refers_to" && other.kind === "E55_Type") ctx.emotions.add(other.label);
      if (e.type === "P2_has_type" && other.kind === "E55_Type") ctx.type = other.label;
    }

    const nodes3 = nodes2.map((n) => {
      const prefix = n.kind || "Node";

      if (!compactMode) {
        // Non-compact: keep the current behavior (show context nodes & edges).
        let base = n.label;
        if (n.kind === "E7_Activity") {
          const ctx = eventCtx.get(n.id);
          if (ctx?.place) base = `${base} @ ${ctx.place}`;
        }
        const label = n.kind === "E73_Information_Object" ? `Entry: ${base}` : `${prefix}: ${base}`;
        return { ...n, label };
      }

      // Compact card: encode Event attributes; hide context nodes later.
      if (n.kind === "E7_Activity") {
        const ctx = eventCtx.get(n.id);
        const raw = (n.raw || {}) as any;

        const day =
          ctx?.day ||
          (typeof raw?.event_time_iso === "string" ? String(raw.event_time_iso).slice(0, 10) : "") ||
          (typeof raw?.key === "string" ? String(raw.key).split("|")[0] : "");
        const place = ctx?.place || "";
        const typeVal = ctx?.type || (typeof raw?.event_type === "string" ? raw.event_type : "") || n.label;
        const topics = Array.from(ctx?.topics || []).slice(0, 3);
        const emotions = Array.from(ctx?.emotions || []).slice(0, 2);

        const line1 = `Event: ${typeVal}`;
        const line2 = day ? `on ${day}` : "on —";
        const line3 =
          place && topics.length
            ? `at ${place} · topics: ${topics.join(", ")}`
            : place
              ? `at ${place}`
              : topics.length
                ? `topics: ${topics.join(", ")}`
                : "";
        const line4 = emotions.length ? `emotions: ${emotions.join(", ")}` : "";
        const label = [line1, line2, line3, line4].filter(Boolean).join("\n");

        return { ...n, label };
      }

      // Keep Person readable with type prefix.
      const label = n.kind === "E73_Information_Object" ? `Entry: ${n.label}` : `${prefix}: ${n.label}`;
      return { ...n, label };
    });

    if (!compactMode) {
      return { nodes: nodes3, edges: edges2 };
    }

    // Compact: only CIDOC core nodes.
    const baseKinds = new Set<string>([
      "E21_Person",
      "E7_Activity",
      "E73_Information_Object",
      "E28_Conceptual_Object",
      "E52_Time_Span",
      "E53_Place",
      "E39_Actor",
      "E74_Group",
    ]);
    const idsDisplayed = new Set<string>();
    for (const n of nodes3) {
      if (baseKinds.has(n.kind)) idsDisplayed.add(n.id);
    }
    const nodesDisplayed = nodes3.filter((n) => idsDisplayed.has(n.id));
    const edgesDisplayed = edges2.filter((e) => idsDisplayed.has(e.source) && idsDisplayed.has(e.target));
    return { nodes: nodesDisplayed, edges: edgesDisplayed };
  }, [data.edges, data.nodes, selectedRef, compactMode]);

  const cyElements = useMemo(() => {
    const els: any[] = [];
    for (const n of elements.nodes) {
      els.push({
        data: {
          id: n.id,
          kind: n.kind,
          label: n.label,
          personId: n.kind === "E21_Person" ? String(n.raw?.id || "") : "",
          ref:
            n.kind === "E21_Person"
              ? `E21_Person:${String(n.raw?.id || "")}`
              : n.kind === "E53_Place"
                ? `E53_Place:${String(n.raw?.name || "")}`
                : n.kind === "E28_Conceptual_Object"
                  ? `E28_Conceptual_Object:${String(n.raw?.name || "")}`
                  : n.kind === "E73_Information_Object"
                      ? `E73_Information_Object:${String(n.raw?.key || "")}`
                    : n.kind === "E7_Activity"
                      ? `Event:${String(n.raw?.key || "")}`
                      : n.kind === "E52_Time_Span"
                        ? `E52_Time_Span:${String(n.raw?.key || n.raw?.date || "")}`
                        : n.kind === "E55_Type"
                          ? `E55_Type:${String(n.raw?.name || "")}`
                          : "",
          color: colorForKind(n.kind),
        },
      });
    }
    for (const e of elements.edges) {
      els.push({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          type: e.type,
          label: e.label,
          arrow: e.arrow,
        },
      });
    }
    return els;
  }, [elements.edges, elements.nodes]);

  // Create/destroy Cytoscape instance once.
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const cy = cytoscape({
      container,
      elements: [],
      style: [],
      layout: { name: "grid" },
    });
    cyRef.current = cy;

    const onTap = (evt: any) => {
      const node = evt.target;
      const kind = node.data("kind");
      const ref = node.data("ref");
      if (ref && ref !== selectedRefRef.current) {
        setSelectedRef(ref);
        return;
      }

      // In compact view we only show card nodes; no progressive expansion.
    };
    cy.on("tap", "node", onTap);

    return () => {
      cy.off("tap", "node", onTap);
      cy.destroy();
      cyRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const layout = useMemo(() => {
    // Breadth-first gives a stable “mind map” with controlled spacing.
    return {
      name: "breadthfirst",
      directed: false,
      padding: compactMode ? 90 : 120,
      circle: false,
      spacingFactor: compactMode ? 2.15 : 2.8,
      animate: false,
      // We'll explicitly call cy.fit() after layout completes.
      // Leaving layout.fit enabled can "fit" twice (double scaling) across toggles.
      fit: false,
      // Important: roots must be a selector string or node IDs; passing a live collection
      // can crash react-cytoscapejs during mount/update.
      roots: undefined,
    } as any;
  }, [selectedRef, cyElements.length, compactMode]);

  const stylesheet = useMemo(
    () => [
      {
        selector: "node",
        style: {
          "background-color": "data(color)",
          label: "data(label)",
          color: "rgba(244,244,245,0.95)",
          "font-size": 24,
          "text-outline-width": 4,
          "text-outline-color": "#09090b",
          "text-valign": "center",
          "text-halign": "center",
          "text-wrap": "wrap",
          "text-max-width": 220,
          width: 62,
          height: 62,
        },
      },
      {
        selector: 'node[kind = "E7_Activity"]',
        style: {
          shape: "round-rectangle",
          width: compactMode ? 160 : 92,
          height: compactMode ? 118 : 74,
          "text-max-width": compactMode ? 260 : 260,
        },
      },
      {
        selector: 'node[kind = "E73_Information_Object"]',
        style: {
          shape: "round-rectangle",
          width: 118,
          height: 98,
          "text-max-width": 360,
          "font-size": 18,
        },
      },
      {
        selector: 'node[kind = "E73_Information_Object"]',
        style: {
          shape: "ellipse",
          width: compactMode ? 150 : 95,
          height: compactMode ? 110 : 70,
          "text-max-width": compactMode ? 260 : 240,
        },
      },
      {
        selector: 'node[kind = "E52_Time_Span"]',
        style: { "font-size": 24, width: 86, height: 86, "text-max-width": 320 },
      },
      {
        selector: "edge",
        style: {
          width: 2,
          "line-color": "rgba(244,244,245,0.25)",
          "target-arrow-color": "rgba(244,244,245,0.25)",
          "target-arrow-shape": "data(arrow)",
          "curve-style": "bezier",
          label: "data(label)",
          color: "rgba(244,244,245,0.80)",
          "font-size": compactMode ? 16 : 22,
          "text-rotation": "autorotate",
          "text-margin-y": -6,
          "text-background-opacity": 0.70,
          "text-background-color": "#09090b",
          "text-background-padding": compactMode ? 4 : 6,
          "text-border-opacity": 0.35,
          "text-border-color": "rgba(244,244,245,0.40)",
          "text-border-width": 1,
        },
      },
      {
        selector: "node:selected",
        style: {
          "border-width": 4,
          "border-color": "rgba(244,244,245,0.75)",
        },
      },
    ],
    [compactMode]
  );

  // Update Cytoscape whenever inputs change.
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;

    // Replace graph (fast + predictable)
    cy.batch(() => {
      cy.elements().remove();
      cy.add(cyElements as any);
      cy.style().fromJson(stylesheet as any).update();
    });

    // Layout runs async; fit must happen after layout completes, otherwise
    // viewport transforms can compound across toggles.
    const padding = compactMode ? 60 : 120;

    // Reset viewport before layout, so any previous zoom doesn't influence layout positioning.
    cy.zoom(1);
    cy.center(cy.elements());

    const l = cy.layout(layout as any);

    const onStop = () => {
      if (cy.elements().length > 0) cy.fit(cy.elements(), padding);
      else cy.zoom(1);
      l.off("layoutstop", onStop);
    };

    l.on("layoutstop", onStop);
    l.run();

    return () => {
      try {
        l.off("layoutstop", onStop);
        l.stop();
      } catch {
        // ignore
      }
    };
  }, [cyElements, layout, stylesheet, compactMode]);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="text-xs font-semibold text-zinc-300">Root</div>
          <select
            value={selectedRef}
            onChange={(e) => setSelectedRef(e.target.value)}
            className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
          >
            {roots.map((r) => (
              <option key={r.ref} value={r.ref}>
                {r.type}: {r.name}
              </option>
            ))}
          </select>
        </div>
        <label className="flex items-center gap-2 text-[11px] text-zinc-500 select-none">
          <input
            type="checkbox"
            checked={compactMode}
            onChange={(e) => setCompactMode(e.target.checked)}
            className="h-4 w-4 rounded border-zinc-700 bg-zinc-900"
          />
          Compact events
        </label>
        <div className="text-[11px] text-zinc-500">
          {elements.nodes.length} nodes · {elements.edges.length} edges
        </div>
      </div>

      {status ? (
        <div className="mt-3 rounded-xl border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-200">
          {status}
        </div>
      ) : null}

      <div className="mt-3 overflow-hidden rounded-2xl border border-zinc-800 bg-zinc-950">
        <div
          ref={containerRef}
          className="h-[78vh] min-h-[620px] w-full"
          style={{ background: "#09090b" }}
        />
      </div>

      <div className="mt-2 text-[11px] text-zinc-500">
        Tip: scroll to zoom, drag to pan, click a node to focus.
      </div>
    </div>
  );
}

