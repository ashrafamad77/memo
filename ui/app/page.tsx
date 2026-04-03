import { AppSplitLayout } from "@/components/AppSplitLayout";
import { ThemeToggle } from "@/components/ThemeToggle";

export default function Home() {
  return (
    <main className="h-screen w-screen overflow-hidden">
      <div className="fixed bottom-3 right-3 z-50">
        <ThemeToggle />
      </div>
      <div className="h-full min-h-0">
        <AppSplitLayout />
      </div>
    </main>
  );
}
