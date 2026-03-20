"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { API_BASE } from "@/lib/api";

type ChatMsg = {
  id: string;
  role: "user" | "assistant";
  text: string;
  badge?: "Onboarding mode" | "Clarification needed";
};

function uid(): string {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

export function ChatPanel() {
  const [msgs, setMsgs] = useState<ChatMsg[]>([
    { id: uid(), role: "assistant", text: "Type a journal entry and press Send." },
  ]);
  const [text, setText] = useState<string>("");
  const [busy, setBusy] = useState<boolean>(false);
  const endRef = useRef<HTMLDivElement | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const autoScrollRef = useRef<boolean>(true);

  const canSend = useMemo(() => text.trim().length > 0, [text]);

  useEffect(() => {
    if (!autoScrollRef.current) return;
    const el = scrollRef.current;
    if (!el) return;
    // Wait a tick so layout is updated before scrolling.
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (!autoScrollRef.current) return;
        el.scrollTop = el.scrollHeight;
      });
    });
  }, [msgs, busy]);

  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;

    const onScroll = () => {
      const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
      // If you're within 60px of the bottom, keep chatbot auto-focus.
      autoScrollRef.current = distanceFromBottom < 60;
    };

    // Initialize based on current scroll position.
    onScroll();
    el.addEventListener("scroll", onScroll, { passive: true });
    return () => el.removeEventListener("scroll", onScroll);
  }, []);

  async function send() {
    const t = text.trim();
    if (!t) return;
    setMsgs((prev) => [...prev, { id: uid(), role: "user", text: t }]);
    setText("");
    setBusy(true);
    try {
      const res = await fetch(API_BASE + "/chat", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ message: t }),
      });
      if (!res.ok) {
        const err = await res.text().catch(() => "");
        throw new Error("API error " + res.status + ": " + err);
      }
      const out = await res.json();
      if (out?.type === "question" && out?.question) {
        const mode = String(out?.mode || "").toLowerCase();
        const badge =
          mode === "onboarding"
            ? "Onboarding mode"
            : mode === "clarification"
              ? "Clarification needed"
              : undefined;
        setMsgs((prev) => [...prev, { id: uid(), role: "assistant", text: String(out.question), badge }]);
      } else if (out?.type === "profile_saved") {
        setMsgs((prev) => [
          ...prev,
          { id: uid(), role: "assistant", text: String(out?.message || "Profile saved.") },
        ]);
      } else {
        const result = out?.result;
        const id = result?.entry_id || "—";
        setMsgs((prev) => [...prev, { id: uid(), role: "assistant", text: "Stored: " + id }]);
        window.dispatchEvent(new CustomEvent("memo:new-entry"));
      }
    } catch (e: any) {
      setMsgs((prev) => [
        ...prev,
        { id: uid(), role: "assistant", text: "Error: " + (e?.message || String(e)) },
      ]);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="flex h-full flex-col min-h-0">
      <div className="flex items-center justify-between gap-3 border-b border-zinc-800 px-4 py-3">
        <div className="text-sm font-semibold">Memo</div>
        <div className="text-xs text-zinc-400">chat</div>
      </div>

      <div
        ref={scrollRef}
        className="flex-1 min-h-0 overflow-y-scroll px-4 py-4 overflow-x-hidden overscroll-contain"
      >
        <div className="space-y-3">
          {msgs.map((m) => (
            <div
              key={m.id}
              className={
                m.role === "user"
                  ? "ml-auto max-w-[95%] rounded-2xl bg-zinc-100 px-3 py-2 text-sm text-zinc-950"
                  : "max-w-[95%] rounded-2xl bg-zinc-900 px-3 py-2 text-sm text-zinc-100"
              }
            >
              {m.role === "assistant" && m.badge ? (
                <div className="mb-1 inline-flex rounded-full border border-zinc-700 bg-zinc-800 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-zinc-300">
                  {m.badge}
                </div>
              ) : null}
              {m.text}
            </div>
          ))}
          <div ref={endRef} />
        </div>
      </div>

      <div className="border-t border-zinc-800 p-3">
        <div className="flex gap-2">
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key !== "Enter") return;
              if (e.shiftKey) return;
              // Keep IME composition (e.g. accented chars) working correctly.
              if ((e.nativeEvent as any).isComposing) return;
              e.preventDefault();
              if (!busy && canSend) void send();
            }}
            rows={4}
            className="min-h-[96px] flex-1 resize-y rounded-xl border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm outline-none placeholder:text-zinc-500 focus:border-zinc-600"
            placeholder="Write an entry…"
          />
          <button
            disabled={!canSend || busy}
            onClick={send}
            className="rounded-xl bg-emerald-400 px-4 text-sm font-semibold text-zinc-950 disabled:cursor-not-allowed disabled:opacity-40"
          >
            {busy ? "…" : "Send"}
          </button>
        </div>
      </div>
    </div>
  );
}

