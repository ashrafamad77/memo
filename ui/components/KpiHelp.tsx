"use client";

export function KpiHelp({
  title,
  description,
}: {
  title?: string;
  description: string;
}) {
  return (
    <span className="group relative ml-1 inline-flex align-middle">
      <span className="inline-flex h-4 w-4 items-center justify-center rounded-full border border-zinc-300 text-[10px] font-bold text-zinc-500 dark:border-zinc-700 dark:text-zinc-400">
        i
      </span>
      <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 max-h-[min(70vh,22rem)] -translate-x-1/2 overflow-y-auto rounded-lg border border-lt-border bg-lt-surface p-2.5 text-[11px] leading-snug text-lt-textSecondary shadow-xl dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-200 group-hover:block">
        {title ? (
          <span className="block font-semibold text-lt-text dark:text-zinc-100">{title}</span>
        ) : null}
        <span
          className={[
            "block whitespace-pre-wrap text-lt-textMuted dark:text-zinc-300",
            title ? "mt-1" : "",
          ].join(" ")}
        >
          {description}
        </span>
      </span>
    </span>
  );
}
