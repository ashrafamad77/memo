"use client";

import { useLayoutEffect, useState } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

const CHAT_SIZE_KEY = "memo-chat-panel-size";

function readSavedChatSize(): number | null {
  try {
    const raw = localStorage.getItem(CHAT_SIZE_KEY);
    if (raw !== null) {
      const v = parseFloat(raw);
      if (Number.isFinite(v) && v >= 18 && v <= 55) return v;
    }
  } catch {}
  return null;
}

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
          "flex flex-1 flex-col items-center justify-center gap-1 rounded-xl py-2 text-[11px] font-semibold transition-colors outline-none focus-visible:ring-2 focus-visible:ring-lt-accentRing/80 dark:focus-visible:ring-zinc-500/55 nebula:focus-visible:ring-cyan-400/50",
          active
            ? "bg-lt-accentSoft text-lt-accent dark:bg-zinc-800 dark:text-zinc-100 nebula:bg-cyan-950/50 nebula:text-cyan-50 nebula:shadow-neb-glow-cyan"
            : "text-lt-textMuted hover:bg-lt-muted/80 hover:text-lt-text dark:text-zinc-400 dark:hover:bg-zinc-800 dark:hover:text-zinc-100 nebula:text-fuchsia-200/70 nebula:hover:bg-neb-haze/80 nebula:hover:text-cyan-50",
        ].join(" ")}
      >
        {id === "journal" ? (
          <svg
            aria-hidden
            className="h-5 w-5 shrink-0"
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
            className="h-5 w-5 shrink-0"
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
      className="flex shrink-0 gap-1.5 border-t border-lt-border bg-lt-surface/90 px-2 pt-2 pb-[max(0.5rem,env(safe-area-inset-bottom))] backdrop-blur-md dark:border-zinc-800 dark:bg-zinc-950/95 nebula:border-cyan-500/25 nebula:bg-neb-panel/75 nebula:shadow-[0_-8px_32px_-12px_rgba(168,85,246,0.35)] nebula:backdrop-blur-xl"
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
  // null = not yet read from localStorage; useLayoutEffect resolves before paint
  const [chatSize, setChatSize] = useState<number | null>(null);

  useLayoutEffect(() => {
    setChatSize(readSavedChatSize() ?? 50);
  }, []);

  if (!mdUp) {
    return (
      <div className="flex h-full min-h-0 flex-col">
        <div className="min-h-0 min-w-0 max-w-full flex-1 overflow-x-hidden overflow-y-hidden">
          {mobileSection === "journal" ? (
            <section className="flex h-full min-h-0 min-w-0 max-w-full flex-col overflow-x-hidden">
              <ChatPanel />
            </section>
          ) : (
            <section className="flex h-full min-h-0 min-w-0 max-w-full flex-col overflow-x-hidden">
              <DashboardTabs />
            </section>
          )}
        </div>
        <MobileBottomNav section={mobileSection} onSection={setMobileSection} />
      </div>
    );
  }

  // chatSize is null until useLayoutEffect reads localStorage (runs before paint).
  // Returning null here is never visible to the user.
  if (chatSize === null) return null;

  return (
    <PanelGroup
      direction="horizontal"
      className="h-full min-h-0"
      onLayout={(sizes: number[]) => {
        try { localStorage.setItem(CHAT_SIZE_KEY, String(sizes[0])); } catch {}
      }}
    >
      <Panel
        defaultSize={chatSize}
        minSize={18}
        maxSize={55}
        className="min-h-0 min-w-0"
      >
        <section className="flex h-full min-h-0 flex-col border-r border-lt-border dark:border-zinc-800 nebula:border-fuchsia-500/20">
          <ChatPanel />
        </section>
      </Panel>
      <PanelResizeHandle
        aria-label="Resize chat and dashboard panels"
        className="group relative flex w-2 shrink-0 cursor-col-resize items-stretch justify-center bg-transparent px-3 outline-none hover:bg-transparent focus-visible:bg-transparent focus-visible:ring-0 dark:bg-zinc-950 dark:hover:bg-zinc-950 dark:focus-visible:bg-zinc-950 dark:focus-visible:ring-0 nebula:bg-transparent nebula:hover:bg-transparent nebula:focus-visible:bg-transparent nebula:focus-visible:ring-0"
      >
        <span
          aria-hidden
          className="pointer-events-none my-auto h-12 w-1 rounded-full bg-transparent group-hover:bg-transparent dark:bg-zinc-950 dark:group-hover:bg-zinc-950 nebula:bg-transparent nebula:group-hover:bg-transparent"
        />
      </PanelResizeHandle>
      <Panel defaultSize={100 - chatSize} minSize={40} className="min-h-0 min-w-0">
        <section className="flex h-full min-h-0 flex-col">
          <DashboardTabs />
        </section>
      </Panel>
    </PanelGroup>
  );
}
