"use client";

import { useLayoutEffect, useState } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

import { ChatPanel } from "@/components/ChatPanel";
import { DashboardTabs } from "@/components/DashboardTabs";

const MD_MEDIA = "(min-width: 768px)";

type MobileSection = "journal" | "dashboard";

/**
 * `md` breakpoint after mount. Initial `true` optimizes SSR / first HTML for desktop (split
 * panels) so cold refresh does not show stacked layout until JS loads. `useLayoutEffect`
 * syncs `matchMedia` **before paint** so narrow viewports switch to stacked immediately.
 */
function useMdUp() {
  const [mdUp, setMdUp] = useState(true);
  useLayoutEffect(() => {
    const mq = window.matchMedia(MD_MEDIA);
    const apply = () => setMdUp(mq.matches);
    apply();
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, []);
  return mdUp;
}

function MobileBottomNav({
  section,
  onSection,
}: {
  section: MobileSection;
  onSection: (s: MobileSection) => void;
}) {
  const item = (id: MobileSection, label: string) => {
    const active = section === id;
    return (
      <button
        type="button"
        onClick={() => onSection(id)}
        aria-current={active ? "page" : undefined}
        className={[
          "flex flex-1 flex-col items-center justify-center gap-0.5 py-2.5 text-xs font-semibold transition-colors",
          active
            ? "text-indigo-600 dark:text-indigo-400"
            : "text-zinc-500 hover:text-zinc-800 dark:text-zinc-400 dark:hover:text-zinc-200",
        ].join(" ")}
      >
        {id === "journal" ? (
          <svg
            aria-hidden
            className="h-5 w-5"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"
            />
          </svg>
        ) : (
          <svg
            aria-hidden
            className="h-5 w-5"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z"
            />
          </svg>
        )}
        <span>{label}</span>
      </button>
    );
  };

  return (
    <nav
      className="flex shrink-0 border-t border-zinc-200 bg-zinc-50/95 pb-[max(0.25rem,env(safe-area-inset-bottom))] pt-0.5 backdrop-blur-sm dark:border-zinc-800 dark:bg-zinc-950/95"
      aria-label="Primary"
    >
      {item("journal", "Journal")}
      {item("dashboard", "Dashboard")}
    </nav>
  );
}

export function AppSplitLayout() {
  const mdUp = useMdUp();
  const [mobileSection, setMobileSection] = useState<MobileSection>("journal");

  if (!mdUp) {
    return (
      <div className="flex h-full min-h-0 flex-col">
        <div className="min-h-0 flex-1 overflow-hidden">
          {mobileSection === "journal" ? (
            <section className="flex h-full min-h-0 min-w-0 flex-col">
              <ChatPanel />
            </section>
          ) : (
            <section className="flex h-full min-h-0 min-w-0 flex-col">
              <DashboardTabs />
            </section>
          )}
        </div>
        <MobileBottomNav section={mobileSection} onSection={setMobileSection} />
      </div>
    );
  }

  return (
    <PanelGroup
      autoSaveId="memo-chat-dashboard"
      direction="horizontal"
      className="h-full min-h-0"
    >
      <Panel
        defaultSize={30}
        minSize={18}
        maxSize={55}
        className="min-h-0 min-w-0"
      >
        <section className="flex h-full min-h-0 flex-col border-r border-zinc-200 dark:border-zinc-800">
          <ChatPanel />
        </section>
      </Panel>
      <PanelResizeHandle
        aria-label="Resize chat and dashboard panels"
        className="group relative flex w-2 shrink-0 cursor-col-resize items-stretch justify-center bg-zinc-200/40 px-3 outline-none transition-colors hover:bg-indigo-400/30 focus-visible:bg-indigo-400/40 focus-visible:ring-2 focus-visible:ring-indigo-500/50 dark:bg-zinc-800/60 dark:hover:bg-indigo-500/25 dark:focus-visible:ring-indigo-400/40"
      >
        <span
          aria-hidden
          className="pointer-events-none my-auto h-12 w-1 rounded-full bg-zinc-400/90 group-hover:bg-indigo-500/80 dark:bg-zinc-500 dark:group-hover:bg-indigo-400/70"
        />
      </PanelResizeHandle>
      <Panel defaultSize={70} minSize={40} className="min-h-0 min-w-0">
        <section className="flex h-full min-h-0 flex-col">
          <DashboardTabs />
        </section>
      </Panel>
    </PanelGroup>
  );
}
