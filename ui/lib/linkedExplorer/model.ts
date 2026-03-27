/** Shared types, constants, and pure helpers for Linked Explorer (Entity Timeline + Visual Graph). */

export type EntityItem = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  places?: string[];
  text_preview?: string;
  event_key?: string;
  activity_name?: string;
};

export type EntityRef = {
  type: string;
  name: string;
  ref: string | null;
  note_role?: string | null;
};

export type PersonFeelingTag = {
  name: string;
  count: number;
  ref: string;
};

export type OverviewPerson = {
  kind: "Person";
  ref: string;
  name: string;
  role?: string;
  mentions?: number;
  items: EntityItem[];
  feeling_tags?: PersonFeelingTag[];
};

export type OverviewEvent = {
  kind: "Event";
  ref: string;
  activity_name?: string;
  summary_preview?: string;
  event_type?: string;
  day?: string;
  event_time_iso?: string;
  event_time_text?: string;
  places?: string[];
  persons?: { id: string; name: string; role?: string; mentions?: number }[];
  users?: { name: string; mentions?: number }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

export type FeelingPerson = { id: string; name: string; role?: string };

export type FeelingOccurrence = {
  assignment_key: string;
  assignment_label: string;
  input_time: string;
  day: string;
  entry_id: string;
  entry_preview: string;
  event_key: string;
  activity_name: string;
  activity_kind: string;
  activity_day: string;
  persons: FeelingPerson[];
};

export type OverviewFeeling = {
  kind: "Feeling";
  ref: string;
  name: string;
  occurrences: FeelingOccurrence[];
  anchor_person_id?: string;
  anchor_person_name?: string;
  anchor_person_ref?: string;
};

export type JournalLink = {
  ref: string;
  name: string;
  bucket: string;
  ref_type: string;
  source: string;
};

export type OverviewContext = {
  kind: "E73_Information_Object";
  ref: string;
  name?: string;
  event_type?: string;
  day?: string;
  text?: string;
  entry_kind?: string;
  linked?: JournalLink[];
  topics?: { type: string; name: string }[];
  concepts?: { type: string; name: string }[];
  mentions?: { type: string; name: string }[];
  entries?: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

export type DaySituation = {
  ref: string;
  event_key: string;
  title: string;
  event_type: string;
  places: string[];
};

export type DayJournalEntry = {
  entry_id: string;
  input_time?: string;
  day?: string;
  event_type?: string;
  text_preview?: string;
  places?: string[];
  event_key?: string;
  activity_name?: string;
};

export type OverviewDay = {
  kind: "Day";
  ref: string;
  day: string;
  focus: string;
  situations: DaySituation[];
  entries: DayJournalEntry[];
  persons: { id: string; name: string; role?: string; mentions?: number }[];
  users: { name: string }[];
  feeling_tags: PersonFeelingTag[];
};

export type Overview = OverviewPerson | OverviewEvent | OverviewContext | OverviewFeeling | OverviewDay;

export const DEFAULT_MOMENT_PANEL_FLOW = { journal: false, situation: true, people: true };

export type NormalizedMoment = {
  id: string;
  flavor: "feeling" | "journal";
  step1Label: string;
  tagBadge?: string;
  recordTitle: string;
  day: string;
  time: string;
  entryPreview: string;
  entryId: string;
  eventKey: string;
  activityName: string;
  activityKind: string;
  activityDay: string;
  persons: FeelingPerson[];
};

export type HubSituation = {
  kind: "situation";
  placeLens: boolean;
  summaryText: string;
  ref: string;
  title: string;
  subtitle: string;
  day: string;
  places: string[];
  eventType: string;
  persons: { id: string; name: string; role?: string }[];
  users: { name: string }[];
  entries: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

export type HubContext = {
  kind: "context";
  anchorKind: "journal" | "context";
  name: string;
  day: string;
  eventType: string;
  text: string;
  linked: JournalLink[];
  topics: { name: string }[];
  concepts: { name: string }[];
  mentions: { name: string }[];
  entries: { entry_id: string; input_time?: string; day?: string; text_preview?: string }[];
};

export type HubData = HubSituation | HubContext;

export type NavOption = {
  key: string;
  title: string;
  description: string;
  count: number;
  enabled: boolean;
};

export type NavResponse = {
  ref: string;
  display_name: string;
  options: NavOption[];
};

export const EXPLORER_CATEGORIES = [
  { id: "person", label: "People", hint: "Named in your journal" },
  { id: "feeling_tag", label: "Feelings & tags", hint: "e.g. satisfaction, stress" },
  { id: "situation", label: "Situations", hint: "Activities & events" },
  { id: "place", label: "Places", hint: "Where it happened" },
  { id: "day", label: "Days", hint: "Situations, notes, people & tags on that date" },
  { id: "idea", label: "Ideas & topics", hint: "Concepts" },
  { id: "note", label: "Notes & context", hint: "Journal entries and short context excerpts from them" },
  { id: "group", label: "Groups", hint: "Circles / teams" },
] as const;

export const LINK_BUCKET_LABEL: Record<string, string> = {
  person: "People",
  situation: "Situations",
  place: "Places",
  idea: "Ideas & topics",
  group: "Groups",
  tag: "Tags & activity types",
  day: "Dates",
  other: "Other",
};

export const LINK_BUCKET_ORDER = ["person", "situation", "place", "idea", "group", "tag", "day", "other"];

export type WizardStep = "category" | "pick_entity" | "pick_exploration" | "blocked" | "content";

export function formatEntityOption(e: EntityRef): string {
  switch (e.type) {
    case "E55_Type":
      return `${e.name} · feeling or tag`;
    case "E21_Person":
      return `${e.name} · person`;
    case "E7_Activity":
      return `${e.name} · situation`;
    case "E53_Place":
      return `${e.name} · place`;
    case "E28_Conceptual_Object":
      return `${e.name} · idea / topic`;
    case "E52_Time_Span":
      return `${e.name} · day`;
    case "E73_Information_Object":
      if (e.note_role === "journal") return `${e.name} · journal entry`;
      if (e.note_role === "context") return `${e.name} · context excerpt`;
      return `${e.name} · note`;
    case "E74_Group":
      return `${e.name} · group`;
    default:
      return `${e.name} · ${e.type}`;
  }
}

export function overviewToMoments(o: Overview): NormalizedMoment[] {
  if (o.kind === "Feeling") {
    return o.occurrences.map((occ) => ({
      id: occ.assignment_key || `${occ.entry_id}-${occ.input_time}`,
      flavor: "feeling" as const,
      step1Label: "What you recorded",
      tagBadge: o.name,
      recordTitle: occ.assignment_label,
      day: occ.day,
      time: occ.input_time,
      entryPreview: occ.entry_preview,
      entryId: occ.entry_id,
      eventKey: occ.event_key,
      activityName: occ.activity_name,
      activityKind: occ.activity_kind,
      activityDay: occ.activity_day,
      persons: occ.persons || [],
    }));
  }
  if (o.kind === "Person") {
    return (o.items || []).map((it, i) => ({
      id: `${it.entry_id}-${i}`,
      flavor: "journal" as const,
      step1Label: "Journal moment",
      recordTitle: (it.activity_name || "").trim() || it.event_type || "Appearance in your notes",
      day: it.day || "",
      time: it.input_time || "",
      entryPreview: (it.text_preview || "").slice(0, 140),
      entryId: it.entry_id,
      eventKey: it.event_key || "",
      activityName: (it.activity_name || "").trim(),
      activityKind: it.event_type || "",
      activityDay: it.day || "",
      persons: [],
    }));
  }
  return [];
}

export function overviewToHub(o: Overview): HubData | null {
  if (o.kind === "Event") {
    const placeLens = o.ref.startsWith("E53_Place:");
    const ev = o as OverviewEvent;
    const title =
      (ev.activity_name || "").trim() ||
      (placeLens ? `Notes linked to ${(ev.places || [])[0] || "this place"}` : "Situation");
    const parts = [o.event_type, o.day, (o.places || []).join(", ")].filter(Boolean);
    const summaryText = (ev.summary_preview || "").trim();
    return {
      kind: "situation",
      placeLens,
      summaryText,
      ref: o.ref,
      title,
      subtitle: parts.join(" · "),
      day: o.day || "",
      places: o.places || [],
      eventType: o.event_type || "",
      persons: o.persons || [],
      users: o.users || [],
      entries: o.entries || [],
    };
  }
  if (o.kind === "E73_Information_Object") {
    const isJournal = (o.entry_kind || "").trim() === "journal_entry";
    return {
      kind: "context",
      anchorKind: isJournal ? "journal" : "context",
      name: o.name || (isJournal ? "Journal" : "Context"),
      day: o.day || "",
      eventType: o.event_type || "",
      text: o.text || "",
      linked: o.linked || [],
      topics: (o.topics || []).map((t) => ({ name: t.name })),
      concepts: (o.concepts || []).map((c) => ({ name: c.name })),
      mentions: (o.mentions || []).map((m) => ({ name: m.name })),
      entries: o.entries || [],
    };
  }
  return null;
}

export function exploreKindLabel(o: Overview | null): string {
  if (!o) return "";
  switch (o.kind) {
    case "Feeling":
      return "Feeling or tag";
    case "Person":
      return "Person";
    case "Event":
      return o.ref.startsWith("E53_Place:") ? "Place · notes & people" : "Situation";
    case "E73_Information_Object":
      return o.entry_kind === "journal_entry" ? "Journal entry" : "Context excerpt";
    case "Day":
      return "Day";
    default:
      return "";
  }
}

export function dayFocusLabel(focus: string): string {
  switch (focus) {
    case "situations":
      return "Situations";
    case "journal":
      return "Journal notes";
    case "people":
      return "People";
    case "feelings":
      return "Feelings & tags";
    default:
      return "Full day";
  }
}
