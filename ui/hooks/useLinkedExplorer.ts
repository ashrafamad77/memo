"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { apiGet } from "@/lib/api";
import {
  type EntityRef,
  type NavResponse,
  type Overview,
  type WizardStep,
  dayFocusLabel,
  exploreKindLabel,
  overviewToHub,
  overviewToMoments,
} from "@/lib/linkedExplorer/model";

export function useLinkedExplorer() {
  const [wizardStep, setWizardStep] = useState<WizardStep>("category");
  const [categoryId, setCategoryId] = useState<string>("");
  const [categoryLabel, setCategoryLabel] = useState<string>("");

  const [entityList, setEntityList] = useState<EntityRef[]>([]);
  const [entityLoading, setEntityLoading] = useState(false);
  const [entityError, setEntityError] = useState<string>("");
  const [searchInput, setSearchInput] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [entityFetchNonce, setEntityFetchNonce] = useState(0);

  const [selectedRef, setSelectedRef] = useState<string>("");
  const [selectedDisplayName, setSelectedDisplayName] = useState<string>("");

  const [navOptions, setNavOptions] = useState<NavResponse | null>(null);
  const [navLoading, setNavLoading] = useState(false);
  const [navError, setNavError] = useState<string>("");

  const [overview, setOverview] = useState<Overview | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewError, setOverviewError] = useState<string>("");

  const [momentFlow, setMomentFlow] = useState<
    Record<string, { journal: boolean; situation: boolean; people: boolean }>
  >({});

  const feelingAnchorPersonRef = useRef<string | null>(null);
  const overviewNavFocusRef = useRef<string | null>(null);

  useEffect(() => {
    const t = window.setTimeout(() => setDebouncedSearch(searchInput.trim()), 350);
    return () => window.clearTimeout(t);
  }, [searchInput]);

  const toggleMoment = useCallback((key: string, part: "journal" | "situation" | "people") => {
    setMomentFlow((prev) => {
      const cur = prev[key] || { journal: false, situation: true, people: true };
      return { ...prev, [key]: { ...cur, [part]: !cur[part] } };
    });
  }, []);

  const loadOverview = useCallback(async (ref: string): Promise<boolean> => {
    setOverviewLoading(true);
    setOverviewError("");
    setOverview(null);
    setMomentFlow({});
    const ap = feelingAnchorPersonRef.current;
    const anchorQ = ap && ap.includes(":") ? `&anchor_person=${encodeURIComponent(ap)}` : "";
    const fk = overviewNavFocusRef.current;
    const focusQ = fk ? `&focus=${encodeURIComponent(fk)}` : "";
    try {
      const data = await apiGet<Overview>(
        `/entity/overview?ref=${encodeURIComponent(ref)}&limit=120${anchorQ}${focusQ}`
      );
      setOverview(data);
      return true;
    } catch (e: unknown) {
      setOverviewError(e instanceof Error ? e.message : String(e));
      return false;
    } finally {
      setOverviewLoading(false);
    }
  }, []);

  const runNavThenMaybeOverview = useCallback(
    async (ref: string) => {
      const r = (ref || "").trim();
      if (!r || r === "null" || r === "undefined") {
        setNavError("Missing item reference — pick another row or refresh the list.");
        return;
      }
      setNavLoading(true);
      setNavError("");
      setNavOptions(null);
      setOverview(null);
      setOverviewError("");
      const ap = feelingAnchorPersonRef.current;
      const anchorQ = ap && ap.includes(":") ? `&anchor_person=${encodeURIComponent(ap)}` : "";
      try {
        const nav = await apiGet<NavResponse>(
          `/entity/nav-options?ref=${encodeURIComponent(r)}${anchorQ}`
        );
        setNavOptions(nav);
        setSelectedDisplayName(nav.display_name || r);
        const enabled = (nav.options || []).filter((o) => o.enabled);
        if (enabled.length === 1) {
          overviewNavFocusRef.current = enabled[0].key;
          await loadOverview(r);
          setWizardStep("content");
        } else if (enabled.length === 0) {
          setWizardStep("blocked");
        } else {
          setWizardStep("pick_exploration");
        }
      } catch (e: unknown) {
        setNavError(e instanceof Error ? e.message : String(e));
        setWizardStep("pick_entity");
      } finally {
        setNavLoading(false);
      }
    },
    [loadOverview]
  );

  const jumpToEntity = useCallback(
    async (ref: string | null | undefined, opts?: { anchorPerson?: string | null }) => {
      const r = (ref || "").trim();
      if (!r || r === "null" || r === "undefined") {
        setNavError("This row has no graph reference (missing entry id). Try refreshing the list.");
        return;
      }
      if (opts?.anchorPerson !== undefined && opts.anchorPerson) {
        feelingAnchorPersonRef.current = opts.anchorPerson;
      } else {
        feelingAnchorPersonRef.current = null;
      }
      overviewNavFocusRef.current = null;
      setSelectedRef(r);
      await runNavThenMaybeOverview(r);
    },
    [runNavThenMaybeOverview]
  );

  useEffect(() => {
    if (wizardStep !== "pick_entity" || !categoryId) return;
    let ignore = false;
    setEntityLoading(true);
    setEntityError("");
    const q = encodeURIComponent(debouncedSearch);
    const cat = encodeURIComponent(categoryId);
    apiGet<{ items: EntityRef[] }>(`/entities?category=${cat}&query=${q}&limit=150`)
      .then((out) => {
        if (!ignore) setEntityList(out.items || []);
      })
      .catch((e: unknown) => {
        if (!ignore) setEntityError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!ignore) setEntityLoading(false);
      });
    return () => {
      ignore = true;
    };
  }, [wizardStep, categoryId, debouncedSearch, entityFetchNonce]);

  const startCategory = useCallback((id: string, label: string) => {
    setCategoryId(id);
    setCategoryLabel(label);
    setSearchInput("");
    setDebouncedSearch("");
    setEntityList([]);
    setSelectedRef("");
    setSelectedDisplayName("");
    setNavOptions(null);
    setNavError("");
    setOverview(null);
    setOverviewError("");
    setEntityFetchNonce(0);
    feelingAnchorPersonRef.current = null;
    overviewNavFocusRef.current = null;
    setWizardStep("pick_entity");
  }, []);

  const restartWizard = useCallback(() => {
    feelingAnchorPersonRef.current = null;
    overviewNavFocusRef.current = null;
    setWizardStep("category");
    setCategoryId("");
    setCategoryLabel("");
    setEntityList([]);
    setSearchInput("");
    setSelectedRef("");
    setSelectedDisplayName("");
    setNavOptions(null);
    setNavError("");
    setOverview(null);
    setOverviewError("");
    setEntityFetchNonce(0);
  }, []);

  const moments = useMemo(() => (overview ? overviewToMoments(overview) : []), [overview]);
  const hub = useMemo(() => (overview ? overviewToHub(overview) : null), [overview]);

  const contentHeader = useMemo(() => {
    if (!overview) return "Details";
    const kind = exploreKindLabel(overview);
    if (overview.kind === "Person") return `${overview.name} · ${kind}`;
    if (overview.kind === "Feeling") return `${overview.name} · ${kind}`;
    if (overview.kind === "Day") return `${overview.day} · ${dayFocusLabel(overview.focus)}`;
    if (overview.kind === "E73_Information_Object") return `${overview.name || "Context"} · ${kind}`;
    const ev = overview as import("@/lib/linkedExplorer/model").OverviewEvent;
    return `${(ev.activity_name || "").trim() || "Situation"} · ${kind}`;
  }, [overview]);

  const selectExplorationOption = useCallback(
    async (optKey: string) => {
      overviewNavFocusRef.current = optKey;
      const ok = await loadOverview(selectedRef);
      if (ok) setWizardStep("content");
    },
    [loadOverview, selectedRef]
  );

  return {
    wizardStep,
    setWizardStep,
    categoryId,
    categoryLabel,
    entityList,
    entityLoading,
    entityError,
    searchInput,
    setSearchInput,
    entityFetchNonce,
    setEntityFetchNonce,
    selectedRef,
    setSelectedRef,
    selectedDisplayName,
    setSelectedDisplayName,
    navOptions,
    setNavOptions,
    navLoading,
    navError,
    setNavError,
    overview,
    setOverview,
    overviewLoading,
    overviewError,
    setOverviewError,
    momentFlow,
    toggleMoment,
    startCategory,
    restartWizard,
    jumpToEntity,
    runNavThenMaybeOverview,
    loadOverview,
    moments,
    hub,
    contentHeader,
    selectExplorationOption,
  };
}
