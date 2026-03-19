import { ChatPanel } from "@/components/ChatPanel";
import { DashboardTabs } from "@/components/DashboardTabs";

export default function Home() {
  return (
    <main className="h-screen w-screen overflow-hidden">
      <div className="grid h-full grid-cols-1 md:grid-cols-[420px_1fr]">
        <section className="min-h-0 border-b border-zinc-800 md:border-b-0 md:border-r">
          <ChatPanel />
        </section>
        <section className="min-h-0 min-w-0">
          <DashboardTabs />
        </section>
      </div>
    </main>
  );
}

