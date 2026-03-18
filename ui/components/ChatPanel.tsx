"use client";

import { useMemo, useState } from "react";

import { API_BASE } from "@/lib/api";

type ChatMsg = { id: string; role: "user" | "assistant"; text: string };

function uid(): string {
  return Math.random().toString(16).slice(2) + Date.now().toString(16);
}

export function ChatPanel() {
  const [msgs, setMsgs] = useState<ChatMsg[]>([
    { id: uid(), role: "assistant", text: "Type a journal entry and press Send." },
  ]);
  const [text, setText] = useState<string>("");
  const [busy, setBusy] = useState<boolean>(false);

  const canSend = useMemo(() => text.trim().length > 0, [text]);

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
      const result = out?.result;
      const id = result?.entry_id || "—";
      setMsgs((prev) => [...prev, { id: uid(), role: "assistant", text: "Stored: " + id }]);
      window.dispatchEvent(new CustomEvent("memo:new-entry"));
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
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between gap-3 border-b border-zinc-800 px-4 py-3">
        <div className="text-sm font-semibold">Memo</div>
        <div className="text-xs text-zinc-400">chat</div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-4">
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
              {m.text}
            </div>
          ))}
        </div>
      </div>

      <div className="border-t border-zinc-800 p-3">
        <div className="flex gap-2">
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            className="h-12 flex-1 resize-none rounded-xl border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm outline-none placeholder:text-zinc-500 focus:border-zinc-600"
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

