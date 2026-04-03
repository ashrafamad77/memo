import { AppSplitLayout } from "@/components/AppSplitLayout";
import { ThemeToggle } from "@/components/ThemeToggle";

export default function Home() {
  return (
    <main className="fixed inset-0 z-0 flex h-dvh max-h-dvh min-h-0 min-w-0 max-w-full flex-col overflow-hidden pl-[env(safe-area-inset-left,0px)] pr-[env(safe-area-inset-right,0px)]">
      <div className="fixed z-50 bottom-[max(0.75rem,env(safe-area-inset-bottom))] right-[max(0.75rem,env(safe-area-inset-right))]">
        <ThemeToggle />
      </div>
      <div className="flex min-h-0 min-w-0 max-w-full flex-1 flex-col">
        <AppSplitLayout />
      </div>
    </main>
  );
}
