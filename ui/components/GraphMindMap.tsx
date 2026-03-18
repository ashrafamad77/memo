"use client";

import cytoscape, { Core } from "cytoscape";
import { useEffect, useMemo, useRef, useState } from "react";

import { apiGet } from "@/lib/api";

type Person = { id: string; name: string; role?: string; mentions?: number };

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
  if (s.has("Person")) return "Person";
  if (s.has("User")) return "User";
  if (s.has("Place")) return "Place";
  if (s.has("Concept")) return "Concept";
  if (s.has("Event")) return "Event";
  if (s.has("Entry")) return "Entry";
  if (s.has("Day")) return "Day";
  if (s.has("Emotion")) return "Emotion";
  if (s.has("EventType")) return "EventType";
  if (s.has("Alias")) return "Alias";
  if (s.has("DisambiguationTask")) return "DisambiguationTask";
  return (labels && labels[0]) || "Node";
}

function labelForNode(n: ApiNode) {
  // Prefer compact, human-friendly captions (Neo4j Browser style).
  const labels = n._labels || [];
  if (labels.includes("Entry") && typeof n.text === "string") {
    const t = n.text.replace(/\s+/g, " ").trim();
    return t.length > 80 ? t.slice(0, 80) + "…" : t;
  }
  if (labels.includes("Event") && typeof n.key === "string") {
    // event key is typically: day|event_type|... (keep the action, not the date)
    const parts = n.key.split("|").map((s: string) => s.trim());
    const action =
      (typeof n.event_type === "string" && n.event_type) ||
      (typeof n.action === "string" && n.action) ||
      (typeof n.type === "string" && n.type) ||
      parts[1] ||
      "event";
    return String(action);
  }
  return n.name || n.text || n.mention || n.date || (typeof n.key === "string" ? n.key.split("|")[0] : "") || n.id || "—";
}

function humanizeRel(type: string) {
  const t = String(type || "").trim();
  const map: Record<string, string> = {
    PARTICIPATED_IN: "participated in",
    OCCURRED_AT: "at",
    HAS_TOPIC: "topic",
    HAS_EMOTION: "emotion",
    FROM_ENTRY: "from entry",
    ON_DAY: "on day",
    HAS_ALIAS: "has alias",
    ALIAS_OF: "alias of",
    MENTIONS: "mentions",
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
    case "Person":
      return "#34d399"; // emerald
    case "User":
      return "#a78bfa"; // violet
    case "Place":
      return "#60a5fa"; // blue
    case "Concept":
      return "#f472b6"; // pink
    case "Event":
      return "#f59e0b"; // amber
    case "Day":
      return "#94a3b8"; // slate
    case "Alias":
      return "#e5e7eb"; // zinc-200
    case "DisambiguationTask":
      return "#fb7185"; // rose
    default:
      return "#e5e7eb"; // zinc-200
  }
}

export function GraphMindMap({
  initialPeople,
}: {
  initialPeople: Person[];
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const [people, setPeople] = useState<Person[]>(initialPeople || []);
  const [selectedPersonId, setSelectedPersonId] = useState<string>(initialPeople?.[0]?.id || "");
  const [data, setData] = useState<GraphApiOut>({ nodes: [], edges: [] });
  const [status, setStatus] = useState<string>("");

  useEffect(() => {
    // Ensure we have a list of people for selection.
    if (people.length) return;
    apiGet<{ items: Person[] }>("/persons?limit=50")
      .then((out) => setPeople(out.items || []))
      .catch(() => {});
  }, [people.length]);

  useEffect(() => {
    if (!selectedPersonId) return;
    let ignore = false;
    setStatus("");
    // depth=2 gives: Person -> Event -> (Day/Place/Concept/...)
    apiGet<GraphApiOut>(
      `/graph/neighborhood?ref=${encodeURIComponent(`Person:${selectedPersonId}`)}&depth=2`
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
  }, [selectedPersonId]);

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

    // Prune to: root person + its events + event context (keeps it mind-map like)
    const root = nodes.find((n) => n.kind === "Person" && String(n.raw?.id || "") === selectedPersonId);
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
      if (node?.kind === "Event") {
        for (const ctx of Array.from(adj.get(nbr) || [])) keep.add(ctx);
      }
    }
    const nodes2 = nodes.filter((n) => keep.has(n.id));
    const ids2 = new Set(nodes2.map((n) => n.id));
    const edges2 = edges.filter((e) => ids2.has(e.source) && ids2.has(e.target));

    // Enrich labels:
    // - Prefix with type, e.g. "Person: Marie"
    // - For Events, include a nearby Place, e.g. "Event: call @ Lyon"
    const nodeById = new Map(nodes2.map((n) => [n.id, n]));
    const neighbors = new Map<string, Set<string>>();
    for (const e of edges2) {
      if (!neighbors.has(e.source)) neighbors.set(e.source, new Set());
      if (!neighbors.has(e.target)) neighbors.set(e.target, new Set());
      neighbors.get(e.source)!.add(e.target);
      neighbors.get(e.target)!.add(e.source);
    }

    const nodes3 = nodes2.map((n) => {
      let base = n.label;
      if (n.kind === "Event") {
        let place = "";
        for (const nid of Array.from(neighbors.get(n.id) || [])) {
          const other = nodeById.get(nid);
          if (other?.kind === "Place") {
            place = other.label;
            break;
          }
        }
        base = place ? `${base} @ ${place}` : base;
      }
      const prefix = n.kind || "Node";
      // Keep Entry readable without over-prefixing the whole snippet.
      const label = n.kind === "Entry" ? `Entry: ${base}` : `${prefix}: ${base}`;
      return { ...n, label };
    });

    return { nodes: nodes3, edges: edges2 };
  }, [data.edges, data.nodes, selectedPersonId]);

  const cyElements = useMemo(() => {
    const els: any[] = [];
    for (const n of elements.nodes) {
      els.push({
        data: {
          id: n.id,
          kind: n.kind,
          label: n.label,
          personId: n.kind === "Person" ? String(n.raw?.id || "") : "",
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
      const pid = node.data("personId");
      if (kind === "Person" && pid && pid !== selectedPersonId) setSelectedPersonId(pid);
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
      padding: 120,
      circle: false,
      spacingFactor: 2.8,
      animate: false,
      fit: true,
      // Important: roots must be a selector string or node IDs; passing a live collection
      // can crash react-cytoscapejs during mount/update.
      roots: `node[kind = "Person"][personId = "${selectedPersonId}"]`,
    } as any;
  }, [selectedPersonId, cyElements.length]);

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
        selector: 'node[kind = "Event"]',
        style: {
          shape: "round-rectangle",
          width: 92,
          height: 74,
          "text-max-width": 260,
        },
      },
      {
        selector: 'node[kind = "Entry"]',
        style: {
          shape: "round-rectangle",
          width: 118,
          height: 98,
          "text-max-width": 360,
          "font-size": 18,
        },
      },
      {
        selector: 'node[kind = "Day"]',
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
          "font-size": 22,
          "text-rotation": "autorotate",
          "text-margin-y": -6,
          "text-background-opacity": 0.70,
          "text-background-color": "#09090b",
          "text-background-padding": 6,
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
    []
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

    const l = cy.layout(layout as any);
    l.run();
  }, [cyElements, layout, stylesheet]);

  return (
    <div className="mt-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <div className="text-xs font-semibold text-zinc-300">Root</div>
          <select
            value={selectedPersonId}
            onChange={(e) => setSelectedPersonId(e.target.value)}
            className="rounded-lg border border-zinc-800 bg-zinc-950 px-2 py-1 text-xs text-zinc-200 outline-none"
          >
            {people.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
                {p.role ? ` (${p.role})` : ""}
              </option>
            ))}
          </select>
        </div>
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
        Tip: scroll to zoom, drag to pan, click a Person to focus.
      </div>
    </div>
  );
}

