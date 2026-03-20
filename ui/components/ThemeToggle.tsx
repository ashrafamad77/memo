"use client";
import { useEffect, useState } from "react";

export function ThemeToggle() {
  const [dark, setDark] = useState(true);

  useEffect(() => {
    const saved = localStorage.getItem("memo-theme");
    if (saved === "light") {
      setDark(false);
      document.documentElement.classList.remove("dark");
    } else {
      setDark(true);
      document.documentElement.classList.add("dark");
    }
  }, []);

  const toggle = () => {
    const next = !dark;
    setDark(next);
    if (next) {
      document.documentElement.classList.add("dark");
      localStorage.setItem("memo-theme", "dark");
    } else {
      document.documentElement.classList.remove("dark");
      localStorage.setItem("memo-theme", "light");
    }
  };

  return (
    <button
      onClick={toggle}
      className="rounded-md px-2 py-1 text-xs font-medium transition
        bg-zinc-200 text-zinc-700 hover:bg-zinc-300
        dark:bg-zinc-800 dark:text-zinc-300 dark:hover:bg-zinc-700"
      title={dark ? "Switch to light mode" : "Switch to dark mode"}
    >
      {dark ? "☀ Light" : "● Dark"}
    </button>
  );
}
