import {
  EXPLORER_CATEGORIES,
  type EntityRef,
  type HubData,
  type NavOption,
  type NavResponse,
  type NormalizedMoment,
  type Overview,
  type OverviewPerson,
  formatEntityOption,
  exploreKindLabel,
} from "@/lib/linkedExplorer/model";

/** Person content step: first ring = these groups; click one to show instances, then ← Types to go back. */
export type PersonGraphBucket = "situations" | "feelings" | "notes" | "people";

export type ExplorerGraphNode = {
  id: string;
  role: "center" | "satellite";
  label: string;
  sub?: string;
  ref?: string;
  /** Passed to jumpToEntity when opening by ref (e.g. feeling tags scoped to a person). */
  navigateOpts?: { anchorPerson?: string | null };
  onActivate?: () => void;
  disabled?: boolean;
  infoTitle?: string;
  infoBody?: string;
  entryId?: string;
};

export type ExplorerGraphModel = {
  hint: string;
  center: ExplorerGraphNode | null;
  satellites: ExplorerGraphNode[];
};

function trunc(s: string, n: number) {
  const t = s.trim();
  if (t.length <= n) return t;
  return `${t.slice(0, n - 1)}…`;
}

function addSat(
  map: Map<string, ExplorerGraphNode>,
  n: Omit<ExplorerGraphNode, "role"> & { role?: ExplorerGraphNode["role"] }
) {
  if (map.has(n.id)) return;
  map.set(n.id, { ...n, role: "satellite" });
}

function collectPersonGraphNeighbors(
  center: ExplorerGraphNode,
  overview: OverviewPerson,
  moments: NormalizedMoment[],
  selectedRef: string,
  bucket: PersonGraphBucket | null,
  setBucket: (v: PersonGraphBucket | null) => void
): { center: ExplorerGraphNode; satellites: ExplorerGraphNode[] } {
  const eventKeys = new Set(moments.map((m) => m.eventKey).filter(Boolean) as string[]);
  const nSit = eventKeys.size;
  const nNotes = moments.filter((m) => m.entryId).length;
  const nFeel = overview.feeling_tags?.length ?? 0;
  const peopleIds = new Set<string>();
  for (const m of moments) {
    for (const p of m.persons) peopleIds.add(p.id);
  }
  const nPeople = peopleIds.size;

  const backNode: ExplorerGraphNode = {
    id: "pb:back-types",
    role: "satellite",
    label: "Types",
    sub: "←",
    onActivate: () => setBucket(null),
    infoTitle: "Back to category ring",
    infoBody:
      "Return to Situations / Feelings / Notes / People — pick a lane first, then open specific instances so the graph stays readable.",
  };

  if (bucket === null) {
    const rings: ExplorerGraphNode[] = [];
    if (nSit > 0) {
      rings.push({
        id: "pb:ring:situations",
        role: "satellite",
        label: "Situations",
        sub: String(nSit),
        onActivate: () => setBucket("situations"),
        infoTitle: "Situations",
        infoBody: `${nSit} distinct activities linked through journal moments that include this person.`,
      });
    }
    if (nFeel > 0) {
      rings.push({
        id: "pb:ring:feelings",
        role: "satellite",
        label: "Feelings",
        sub: String(nFeel),
        onActivate: () => setBucket("feelings"),
        infoTitle: "Feelings & tags",
        infoBody: "Tags tied to this person in the graph; open one to follow that thread.",
      });
    }
    if (nNotes > 0) {
      rings.push({
        id: "pb:ring:notes",
        role: "satellite",
        label: "Notes",
        sub: String(nNotes),
        onActivate: () => setBucket("notes"),
        infoTitle: "Journal notes",
        infoBody: "Timeline rows (moments) where this person appears in linked notes.",
      });
    }
    if (nPeople > 0) {
      rings.push({
        id: "pb:ring:people",
        role: "satellite",
        label: "People",
        sub: String(nPeople),
        onActivate: () => setBucket("people"),
        infoTitle: "People",
        infoBody: "Other people listed on the same moment rows as co-participants when the graph provides them.",
      });
    }

    if (!rings.length) {
      let emptyBody =
        "Notes appear when linked through situations that list this person as a participant. Mentions alone may not produce rows.";
      if (typeof overview.mentions === "number" && overview.mentions > 0) {
        emptyBody += `\n\nRecorded mentions in the graph: ${overview.mentions}`;
      }
      return {
        center,
        satellites: [
          {
            id: "empty-person",
            role: "satellite",
            label: "No links",
            sub: "yet",
            disabled: true,
            infoTitle: "No timeline rows",
            infoBody: emptyBody,
          },
        ],
      };
    }
    return { center, satellites: rings };
  }

  if (bucket === "situations") {
    const map = new Map<string, ExplorerGraphNode>();
    for (const m of moments) {
      if (!m.eventKey) continue;
      const ref = `Event:${m.eventKey}`;
      addSat(map, {
        id: ref,
        label: trunc(m.activityName || "Situation", 16),
        sub: trunc(m.activityKind || "", 14),
        ref,
        infoTitle: m.activityName || "Situation",
        infoBody: [m.activityName, m.activityKind, m.activityDay].filter(Boolean).join("\n"),
      });
    }
    return { center, satellites: [backNode, ...map.values()] };
  }

  if (bucket === "feelings") {
    const map = new Map<string, ExplorerGraphNode>();
    for (const t of overview.feeling_tags || []) {
      addSat(map, {
        id: `tag:${t.ref}`,
        label: trunc(t.name, 14),
        sub: `×${t.count}`,
        ref: t.ref,
        navigateOpts: { anchorPerson: selectedRef },
        infoTitle: t.name,
        infoBody: "Feeling or tag · opens that thread in the graph (scoped to this person when supported).",
      });
    }
    return { center, satellites: [backNode, ...map.values()] };
  }

  if (bucket === "notes") {
    const map = new Map<string, ExplorerGraphNode>();
    for (const m of moments) {
      if (!m.entryId) continue;
      addSat(map, {
        id: `entry:${m.entryId}`,
        label: "Note",
        sub: trunc(m.day || m.time || "Journal", 12),
        entryId: m.entryId,
        infoTitle: "Journal note",
        infoBody: [m.recordTitle, m.entryPreview].filter(Boolean).join("\n\n") || undefined,
      });
    }
    return { center, satellites: [backNode, ...map.values()] };
  }

  const map = new Map<string, ExplorerGraphNode>();
  for (const m of moments) {
    for (const p of m.persons) {
      const ref = `E21_Person:${p.id}`;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "Person",
        ref,
        infoTitle: p.name,
      });
    }
  }
  return { center, satellites: [backNode, ...map.values()] };
}

function collectContentNeighbors(
  overview: Overview,
  moments: NormalizedMoment[],
  hub: HubData | null,
  contentHeader: string,
  selectedRef: string,
  personGraphBucket: PersonGraphBucket | null,
  setPersonGraphBucket: (v: PersonGraphBucket | null) => void
): { center: ExplorerGraphNode; satellites: ExplorerGraphNode[] } {
  const map = new Map<string, ExplorerGraphNode>();

  const centerBodyParts: string[] = [exploreKindLabel(overview)];
  if (overview.kind === "Feeling" && overview.anchor_person_name) {
    centerBodyParts.push(
      `Scoped to ${overview.anchor_person_name} — assignments tied to them or their situations.`
    );
  }
  if (hub?.kind === "situation" && hub.summaryText) {
    centerBodyParts.push(hub.summaryText);
  }
  if (hub?.kind === "context" && hub.text) {
    centerBodyParts.push(hub.text);
  }

  const center: ExplorerGraphNode = {
    id: "center",
    role: "center",
    label: trunc(contentHeader || selectedRef, 22),
    sub: exploreKindLabel(overview),
    infoTitle: contentHeader || "Focus",
    infoBody: centerBodyParts.join("\n\n"),
    ref: undefined,
  };

  if (overview.kind === "Person") {
    return collectPersonGraphNeighbors(
      center,
      overview,
      moments,
      selectedRef,
      personGraphBucket,
      setPersonGraphBucket
    );
  }

  for (const m of moments) {
    if (m.entryId) {
      addSat(map, {
        id: `entry:${m.entryId}`,
        label: "Note",
        sub: trunc(m.day || m.time || "Journal", 12),
        entryId: m.entryId,
        infoTitle: "Journal note",
        infoBody: [m.recordTitle, m.entryPreview].filter(Boolean).join("\n\n") || "Hover loaded body via ⓘ",
      });
    }
    if (m.eventKey) {
      const ref = `Event:${m.eventKey}`;
      addSat(map, {
        id: ref,
        label: trunc(m.activityName || "Situation", 16),
        sub: trunc(m.activityKind || "", 14),
        ref,
        infoTitle: m.activityName || "Situation",
        infoBody: [m.activityName, m.activityKind, m.activityDay].filter(Boolean).join("\n"),
      });
    }
    for (const p of m.persons) {
      const ref = `E21_Person:${p.id}`;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "Person",
        ref,
        infoTitle: p.name,
      });
    }
  }

  if (hub?.kind === "situation") {
    for (const p of hub.persons) {
      const ref = `E21_Person:${p.id}`;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "Person",
        ref,
        infoTitle: p.name,
      });
    }
    for (const e of hub.entries) {
      addSat(map, {
        id: `entry:${e.entry_id}`,
        label: "Note",
        sub: trunc(e.input_time || e.day || "", 12),
        entryId: e.entry_id,
        infoTitle: "Journal note",
        infoBody: (e.text_preview || "").slice(0, 600) || undefined,
      });
    }
  }

  if (hub?.kind === "context") {
    for (const L of hub.linked) {
      addSat(map, {
        id: `link:${L.ref}:${L.source}`,
        label: trunc(L.name, 14),
        sub: trunc(L.ref_type || "link", 10),
        ref: L.ref,
        infoTitle: L.name,
        infoBody: L.source === "situation" ? "Linked via situation" : "Linked entity",
      });
    }
    for (const e of hub.entries) {
      addSat(map, {
        id: `entry:${e.entry_id}`,
        label: "Note",
        sub: "",
        entryId: e.entry_id,
        infoTitle: "Related note",
        infoBody: (e.text_preview || "").slice(0, 600),
      });
    }
  }

  if (overview.kind === "Day") {
    const day = overview;
    for (const s of day.situations) {
      addSat(map, {
        id: s.ref,
        label: trunc(s.title, 16),
        sub: trunc(s.event_type, 12),
        ref: s.ref,
        infoTitle: s.title,
        infoBody: [s.event_type, s.places.join(", ")].filter(Boolean).join("\n"),
      });
    }
    for (const p of day.persons) {
      const ref = `E21_Person:${p.id}`;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "",
        ref,
        infoTitle: p.name,
      });
    }
    for (const t of day.feeling_tags) {
      addSat(map, {
        id: t.ref,
        label: trunc(t.name, 12),
        sub: `×${t.count}`,
        ref: t.ref,
        infoTitle: t.name,
      });
    }
    for (const e of day.entries) {
      addSat(map, {
        id: `entry:${e.entry_id}`,
        label: "Note",
        sub: "",
        entryId: e.entry_id,
        infoTitle: "Journal note",
        infoBody: (e.text_preview || "").slice(0, 500),
      });
      if (e.event_key) {
        const ref = `Event:${e.event_key}`;
        addSat(map, {
          id: `${ref}:from:${e.entry_id}`,
          label: trunc(e.activity_name || "Situation", 14),
          sub: "from note",
          ref,
          infoTitle: e.activity_name || "Situation",
        });
      }
    }
  }

  if (overview.kind === "Feeling" && overview.anchor_person_ref) {
    addSat(map, {
      id: `back:${overview.anchor_person_ref}`,
      label: "Person",
      sub: "back",
      ref: overview.anchor_person_ref,
      infoTitle: overview.anchor_person_name || "Person",
      infoBody: "Return to the anchored person view.",
    });
  }

  return { center, satellites: [...map.values()] };
}

export type BuildGraphInput = {
  wizardStep: string;
  categoryId: string;
  categoryLabel: string;
  entityList: EntityRef[];
  entityLoading: boolean;
  entityError: string;
  navError: string;
  navOptions: { options: NavOption[] } | null;
  navLoading: boolean;
  selectedRef: string;
  selectedDisplayName: string;
  overview: Overview | null;
  overviewLoading: boolean;
  overviewError: string;
  moments: NormalizedMoment[];
  hub: HubData | null;
  contentHeader: string;
  startCategory: (id: string, label: string) => void;
  jumpToEntity: (ref: string | null | undefined, opts?: { anchorPerson?: string | null }) => void | Promise<void>;
  selectExplorationOption: (key: string) => void | Promise<void>;
  setWizardStep: (s: "category" | "pick_entity" | "pick_exploration" | "blocked" | "content") => void;
  setNavOptions: (v: NavResponse | null) => void;
  setNavError: (s: string) => void;
  setOverview: (v: Overview | null) => void;
  setOverviewError: (s: string) => void;
  setSelectedRef: (s: string) => void;
  setSelectedDisplayName: (s: string) => void;
  runNavThenMaybeOverview: (ref: string) => void | Promise<void>;
  loadOverview: (ref: string) => void | Promise<boolean>;
  restartWizard: () => void;
  personGraphBucket: PersonGraphBucket | null;
  setPersonGraphBucket: (v: PersonGraphBucket | null) => void;
};

export function buildLinkedExplorerGraph(ex: BuildGraphInput): ExplorerGraphModel {
  const {
    wizardStep,
    categoryLabel,
    entityList,
    entityLoading,
    entityError,
    navError,
    navOptions,
    navLoading,
    selectedRef,
    selectedDisplayName,
    overview,
    overviewLoading,
    overviewError,
    moments,
    hub,
    contentHeader,
    startCategory,
    jumpToEntity,
    selectExplorationOption,
    setWizardStep,
    setNavOptions,
    setNavError,
    setOverview,
    setOverviewError,
    setSelectedRef,
    setSelectedDisplayName,
    runNavThenMaybeOverview,
    loadOverview,
    restartWizard,
    personGraphBucket,
    setPersonGraphBucket,
  } = ex;

  if (wizardStep === "category") {
    return {
      hint: "Choose a category — each node links to the next graph.",
      center: {
        id: "hub",
        role: "center",
        label: "Explore",
        sub: "Start",
        infoTitle: "Linked graph",
        infoBody: "Pick what kind of entity you want in the center next. Same data as Entity Timeline.",
      },
      satellites: EXPLORER_CATEGORIES.map((c) => ({
        id: `cat:${c.id}`,
        role: "satellite" as const,
        label: trunc(c.label, 14),
        sub: trunc(c.hint, 18),
        onActivate: () => startCategory(c.id, c.label),
        infoTitle: c.label,
        infoBody: c.hint,
      })),
    };
  }

  if (wizardStep === "pick_entity") {
    const center: ExplorerGraphNode = {
      id: "center-cat",
      role: "center",
      label: trunc(categoryLabel || "Type", 18),
      sub: "Category",
      onActivate: () => {
        setWizardStep("category");
        setNavError("");
      },
      infoTitle: categoryLabel,
      infoBody: "Click center to go back to categories. Pick a satellite to open it in the graph.",
    };

    const sats: ExplorerGraphNode[] = entityList.map((e, i) => ({
      id: e.ref?.trim() ? e.ref : `entity-${i}`,
      role: "satellite" as const,
      label: trunc(e.name, 16),
      sub: trunc(formatEntityOption(e), 20),
      ref: e.ref?.trim() || undefined,
      disabled: navLoading || !e.ref?.trim(),
      onActivate: e.ref?.trim() ? () => void jumpToEntity(e.ref) : undefined,
      infoTitle: e.name,
      infoBody: formatEntityOption(e),
    }));

    if (entityLoading) {
      return {
        hint: "Loading entities…",
        center,
        satellites: [],
      };
    }
    if (entityError) {
      return {
        hint: entityError,
        center: {
          id: "err",
          role: "center",
          label: "Error",
          sub: "List",
          infoTitle: "Could not load list",
          infoBody: entityError,
        },
        satellites: [],
      };
    }
    if (navError) {
      return {
        hint: navError,
        center,
        satellites: [
          {
            id: "retry-nav",
            role: "satellite",
            label: "Retry",
            sub: "open",
            onActivate: () => void runNavThenMaybeOverview(selectedRef),
            infoTitle: "Retry navigation",
            infoBody: navError,
          },
        ],
      };
    }
    return {
      hint: sats.length ? `Pick an item · ${sats.length} shown` : "No matches — adjust filter above.",
      center,
      satellites: sats,
    };
  }

  if (wizardStep === "pick_exploration") {
    const center: ExplorerGraphNode = {
      id: "center-entity",
      role: "center",
      label: trunc(selectedDisplayName || selectedRef, 20),
      sub: "Selected",
      infoTitle: selectedDisplayName || selectedRef,
      infoBody: "Choose how to lens this node. Each option pulls the same overview data as the timeline tab.",
      onActivate: () => {
        setWizardStep("pick_entity");
        setNavOptions(null);
        setNavError("");
      },
    };

    if (navError) {
      return {
        hint: navError,
        center,
        satellites: [
          {
            id: "retry-nav2",
            role: "satellite",
            label: "Retry",
            onActivate: () => void runNavThenMaybeOverview(selectedRef),
            infoTitle: "Retry",
            infoBody: navError,
          },
        ],
      };
    }

    const opts = navOptions?.options || [];
    const sats: ExplorerGraphNode[] = opts.map((opt) => ({
      id: `opt:${opt.key}`,
      role: "satellite" as const,
      label: trunc(opt.title, 16),
      sub: String(opt.count),
      disabled: !opt.enabled,
      onActivate: () => void selectExplorationOption(opt.key),
      infoTitle: opt.title,
      infoBody: opt.description,
    }));

    return {
      hint: "Choose a view — satellites connect to the next neighborhood.",
      center,
      satellites: sats,
    };
  }

  if (wizardStep === "blocked") {
    return {
      hint: "No enabled path for this ref.",
      center: {
        id: "blocked",
        role: "center",
        label: "Blocked",
        sub: "No lens",
        infoTitle: "Nothing to open yet",
        infoBody: "Try another entity or enrich the graph with journal links.",
      },
      satellites: [
        {
          id: "back-entities",
          role: "satellite",
          label: "Others",
          sub: "items",
          onActivate: () => {
            setWizardStep("pick_entity");
            setNavOptions(null);
          },
          infoTitle: "Pick another item",
        },
        {
          id: "restart",
          role: "satellite",
          label: "Restart",
          sub: "wizard",
          onActivate: () => restartWizard(),
          infoTitle: "Start over",
        },
      ],
    };
  }

  if (wizardStep === "content") {
    if (overviewLoading) {
      return {
        hint: "Loading neighborhood…",
        center: {
          id: "loading",
          role: "center",
          label: "…",
          sub: "Loading",
          infoTitle: "Loading",
          infoBody: "Fetching overview from the same API as Entity Timeline.",
        },
        satellites: [],
      };
    }
    if (overviewError) {
      return {
        hint: overviewError,
        center: {
          id: "ov-err",
          role: "center",
          label: "Error",
          sub: "Overview",
          infoTitle: "Overview failed",
          infoBody: overviewError,
          onActivate: () => void loadOverview(selectedRef),
        },
        satellites: [],
      };
    }
    if (!overview) {
      return {
        hint: "No overview",
        center: {
          id: "empty",
          role: "center",
          label: "Empty",
          infoTitle: "No data",
          infoBody: "Nothing loaded.",
        },
        satellites: [],
      };
    }

    const { center, satellites } = collectContentNeighbors(
      overview,
      moments,
      hub,
      contentHeader,
      selectedRef,
      personGraphBucket,
      setPersonGraphBucket
    );

    if (!satellites.length) {
      satellites.push({
        id: "empty-generic",
        role: "satellite",
        label: "No edges",
        sub: "empty",
        disabled: true,
        infoTitle: "Nothing to show",
        infoBody: "This lens has no extra nodes to draw yet.",
      });
    }

    const hintPerson =
      overview.kind === "Person"
        ? personGraphBucket === null
          ? "Person · first pick a lane (Situations, Feelings, Notes, People), then open instances. ⓘ = detail."
          : "Instances in this lane — click Types ← to return to lanes. ⓘ = previews & full notes."
        : null;

    return {
      hint:
        hintPerson ||
        "Click a node to move the graph. Hover ⓘ for previews and full note text when available.",
      center,
      satellites,
    };
  }

  return { hint: "", center: null, satellites: [] };
}
