"use client";
import { useLayoutEffect, useState } from "react";

export type MemoTheme = "light" | "dark" | "nebula";

const STORAGE_KEY = "memo-theme";

function applyTheme(mode: MemoTheme) {
  const root = document.documentElement;
  root.classList.toggle("dark", mode !== "light");
  root.classList.toggle("theme-nebula", mode === "nebula");
}

function readStored(): MemoTheme {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === "light" || raw === "dark" || raw === "nebula") return raw;
  } catch {}
  return "dark";
}

export function ThemeToggle() {
  const [mode, setMode] = useState<MemoTheme>("dark");

  useLayoutEffect(() => {
    const stored = readStored();
    setMode(stored);
    applyTheme(stored);
  }, []);

  const cycle = () => {
    const order: MemoTheme[] = ["light", "dark", "nebula"];
    const next = order[(order.indexOf(mode) + 1) % order.length];
    setMode(next);
    applyTheme(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {}
  };

  const label =
    mode === "light" ? "☀ Light" : mode === "dark" ? "◆ Zinc" : "✦ Nebula";
  const title =
    mode === "light"
      ? "Switch to Zinc dark"
      : mode === "dark"
        ? "Switch to Nebula (alt dark)"
        : "Switch to light";

  return (
    <button
      type="button"
      onClick={cycle}
      className="rounded-md border border-lt-border bg-lt-surface px-2 py-1 text-xs font-medium text-lt-textSecondary shadow-sm transition
        hover:border-lt-borderStrong hover:bg-lt-accentSoft hover:text-lt-accent
        dark:border-white/10 dark:bg-zinc-900/80 dark:text-zinc-200 dark:hover:border-fuchsia-500/40 dark:hover:bg-violet-950/60 dark:hover:text-fuchsia-100
        nebula:border-cyan-500/35 nebula:bg-neb-panel/85 nebula:text-cyan-100 nebula:shadow-neb-glow-cyan nebula:backdrop-blur-md
        nebula:hover:border-fuchsia-400/50 nebula:hover:bg-neb-haze/90 nebula:hover:text-fuchsia-100"
      title={title}
    >
      {label}
    </button>
  );
}
