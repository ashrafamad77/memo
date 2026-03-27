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

/** Stable graph node color — same semantic type → same gradient in the UI. */
export type ExplorerNodeVisualGroup =
  | "hub"
  | "category"
  | "person"
  | "place"
  | "situation"
  | "feeling"
  | "note"
  | "day"
  | "idea"
  | "group"
  | "nav"
  | "bucket"
  | "system"
  | "generic";

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
  visualGroup?: ExplorerNodeVisualGroup;
};

export function visualGroupFromExplorerCategoryId(id: string): ExplorerNodeVisualGroup {
  const m: Record<string, ExplorerNodeVisualGroup> = {
    person: "person",
    feeling_tag: "feeling",
    situation: "situation",
    place: "place",
    day: "day",
    idea: "idea",
    note: "note",
    group: "group",
  };
  return m[id] || "category";
}

export function visualGroupFromEntityType(type: string): ExplorerNodeVisualGroup {
  const m: Record<string, ExplorerNodeVisualGroup> = {
    E21_Person: "person",
    E53_Place: "place",
    E7_Activity: "situation",
    E55_Type: "feeling",
    E73_Information_Object: "note",
    E52_Time_Span: "day",
    E28_Conceptual_Object: "idea",
    E74_Group: "group",
  };
  return m[(type || "").trim()] || "generic";
}

export function visualGroupFromRef(ref: string): ExplorerNodeVisualGroup {
  const r = (ref || "").trim();
  if (!r) return "generic";
  if (r.startsWith("E21_Person:")) return "person";
  if (r.startsWith("E53_Place:")) return "place";
  if (r.startsWith("Event:")) return "situation";
  if (r.startsWith("E52_Time_Span:")) return "day";
  if (r.startsWith("E55_Type:")) return "feeling";
  if (r.startsWith("E73_Information_Object:")) return "note";
  if (r.startsWith("E28_Conceptual_Object:")) return "idea";
  if (r.startsWith("E74_Group:")) return "group";
  return "generic";
}

function visualGroupFromNavOptionKey(key: string): ExplorerNodeVisualGroup {
  switch (key) {
    case "moments":
    case "journal":
    case "context":
      return "note";
    case "feelings":
      return "feeling";
    case "activity_type":
    case "situations":
    case "hub":
      return "situation";
    case "people":
      return "person";
    case "all":
      return "day";
    default:
      return "nav";
  }
}

function visualGroupFromLinkBucket(bucket: string): ExplorerNodeVisualGroup {
  const b = (bucket || "other").trim().toLowerCase();
  if (b === "person") return "person";
  if (b === "situation") return "situation";
  if (b === "place") return "place";
  if (b === "tag") return "feeling";
  if (b === "idea") return "idea";
  if (b === "group") return "group";
  if (b === "day") return "day";
  return "generic";
}

function visualGroupForOverviewCenter(overview: Overview, selectedRef: string): ExplorerNodeVisualGroup {
  const ref = (overview.kind === "Event" ? overview.ref : selectedRef) || selectedRef;
  switch (overview.kind) {
    case "Person":
      return "person";
    case "Feeling":
      return "feeling";
    case "Day":
      return "day";
    case "Event":
      return ref.startsWith("E53_Place:") ? "place" : "situation";
    case "E73_Information_Object":
      return "note";
    default:
      return "generic";
  }
}

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

/** Same E21 person whether ref is `E21_Person:id` or variants with same id. */
function graphRefsEqualPerson(a: string, b: string): boolean {
  const na = a.trim();
  const nb = b.trim();
  if (!na || !nb) return false;
  if (na === nb) return true;
  const tail = (s: string) => {
    const i = s.lastIndexOf(":");
    return i >= 0 ? s.slice(i + 1).trim() : s;
  };
  return tail(na) === tail(nb) && tail(na).length > 0;
}

/** On Feeling scoped to a person, that person is only the "← back" node — omit duplicate person satellites. */
function skipPersonSatelliteForFeelingAnchor(overview: Overview, personRef: string): boolean {
  if (overview.kind !== "Feeling" || !overview.anchor_person_ref) return false;
  return graphRefsEqualPerson(personRef, overview.anchor_person_ref);
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
    visualGroup: "bucket",
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
        visualGroup: "situation",
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
        visualGroup: "feeling",
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
        visualGroup: "note",
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
        visualGroup: "person",
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
            visualGroup: "system",
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
        visualGroup: "situation",
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
        visualGroup: "feeling",
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
        visualGroup: "note",
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
        visualGroup: "person",
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
    visualGroup: visualGroupForOverviewCenter(overview, selectedRef),
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
        visualGroup: "note",
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
        visualGroup: "situation",
        infoTitle: m.activityName || "Situation",
        infoBody: [m.activityName, m.activityKind, m.activityDay].filter(Boolean).join("\n"),
      });
    }
    for (const p of m.persons) {
      const ref = `E21_Person:${p.id}`;
      if (skipPersonSatelliteForFeelingAnchor(overview, ref)) continue;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "Person",
        ref,
        visualGroup: "person",
        infoTitle: p.name,
      });
    }
  }

  if (hub?.kind === "situation") {
    for (const p of hub.persons) {
      const ref = `E21_Person:${p.id}`;
      if (skipPersonSatelliteForFeelingAnchor(overview, ref)) continue;
      addSat(map, {
        id: ref,
        label: trunc(p.name, 14),
        sub: p.role || "Person",
        ref,
        visualGroup: "person",
        infoTitle: p.name,
      });
    }
    for (const e of hub.entries) {
      addSat(map, {
        id: `entry:${e.entry_id}`,
        label: "Note",
        sub: trunc(e.input_time || e.day || "", 12),
        entryId: e.entry_id,
        visualGroup: "note",
        infoTitle: "Journal note",
        infoBody: (e.text_preview || "").slice(0, 600) || undefined,
      });
    }
  }

  if (hub?.kind === "context") {
    for (const L of hub.linked) {
      if (visualGroupFromRef(L.ref) === "person" && skipPersonSatelliteForFeelingAnchor(overview, L.ref)) {
        continue;
      }
      addSat(map, {
        id: `link:${L.ref}:${L.source}`,
        label: trunc(L.name, 14),
        sub: trunc(L.ref_type || "link", 10),
        ref: L.ref,
        visualGroup: (() => {
          const g = visualGroupFromRef(L.ref);
          return g !== "generic" ? g : visualGroupFromLinkBucket(L.bucket);
        })(),
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
        visualGroup: "note",
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
        visualGroup: visualGroupFromRef(s.ref),
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
        visualGroup: "person",
        infoTitle: p.name,
      });
    }
    for (const t of day.feeling_tags) {
      addSat(map, {
        id: t.ref,
        label: trunc(t.name, 12),
        sub: `×${t.count}`,
        ref: t.ref,
        visualGroup: "feeling",
        infoTitle: t.name,
      });
    }
    for (const e of day.entries) {
      addSat(map, {
        id: `entry:${e.entry_id}`,
        label: "Note",
        sub: "",
        entryId: e.entry_id,
        visualGroup: "note",
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
          visualGroup: "situation",
          infoTitle: e.activity_name || "Situation",
        });
      }
    }
  }

  if (overview.kind === "Feeling" && overview.anchor_person_ref) {
    const anchorName = (overview.anchor_person_name || "").trim() || "Person";
    addSat(map, {
      id: `back:${overview.anchor_person_ref}`,
      label: trunc(anchorName, 18),
      sub: "← back",
      ref: overview.anchor_person_ref,
      visualGroup: "person",
      infoTitle: `Back to ${anchorName}`,
      infoBody: `Re-open the person overview you came from. Any other person nodes here are different people linked to this tag.`,
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
    categoryId,
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
        visualGroup: "hub",
        infoTitle: "Linked graph",
        infoBody: "Pick what kind of entity you want in the center next. Same data as Entity Timeline.",
      },
      satellites: EXPLORER_CATEGORIES.map((c) => ({
        id: `cat:${c.id}`,
        role: "satellite" as const,
        label: trunc(c.label, 14),
        sub: trunc(c.hint, 18),
        visualGroup: visualGroupFromExplorerCategoryId(c.id),
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
      visualGroup: visualGroupFromExplorerCategoryId(categoryId),
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
      visualGroup: visualGroupFromEntityType(e.type),
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
          visualGroup: "system",
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
            visualGroup: "system",
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
      visualGroup: visualGroupFromRef(selectedRef),
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
            visualGroup: "system",
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
      visualGroup: visualGroupFromNavOptionKey(opt.key),
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
        visualGroup: "system",
        infoTitle: "Nothing to open yet",
        infoBody: "Try another entity or enrich the graph with journal links.",
      },
      satellites: [
        {
          id: "back-entities",
          role: "satellite",
          label: "Others",
          sub: "items",
          visualGroup: "nav",
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
          visualGroup: "bucket",
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
          visualGroup: "system",
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
          visualGroup: "system",
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
          visualGroup: "system",
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
        visualGroup: "generic",
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
