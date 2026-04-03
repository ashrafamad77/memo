"use client";

import { useLayoutEffect, useState } from "react";
import { Panel, PanelGroup, PanelResizeHandle } from "react-resizable-panels";

import { ChatPanel } from "@/components/ChatPanel";
import { DashboardTabs } from "@/components/DashboardTabs";

const MD_MEDIA = "(min-width: 768px)";

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

export function AppSplitLayout() {
  const mdUp = useMdUp();

  if (!mdUp) {
    return (
      <div className="flex h-full min-h-0 flex-col">
        <section className="flex min-h-0 min-w-0 flex-1 flex-col border-b border-zinc-200 dark:border-zinc-800">
          <ChatPanel />
        </section>
        <section className="flex min-h-0 min-w-0 flex-1 flex-col">
          <DashboardTabs />
        </section>
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
