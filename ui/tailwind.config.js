const plugin = require("tailwindcss/plugin")

/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ["./app/**/*.{js,ts,jsx,tsx,mdx}", "./components/**/*.{js,ts,jsx,tsx,mdx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // Light theme only — Kimbie Dark–inspired warm parchment (browns / tan / terracotta; dark modes unchanged).
        lt: {
          canvas: "#f2e8dc",
          surface: "#fffbf6",
          raised: "#faf3ea",
          muted: "#f0e4d4",
          subtle: "#e8dcc8",
          border: "#d2c2a8",
          borderStrong: "#a57a4c",
          text: "#221a12",
          textSecondary: "#4a3d2e",
          textMuted: "#7d6b52",
          accent: "#7c5021",
          accentBright: "#a65f1a",
          accentSoft: "#f2e4cf",
          accentRing: "#c9a06a",
          washTop: "#faf0e4",
          washBottom: "#efe0cc",
        },
        // Second dark theme ("Nebula") — use with `nebula:` variant only; default dark stays zinc.
        neb: {
          void: "#050014",
          deep: "#0c0224",
          mist: "#12082e",
          haze: "#1a0f3a",
          panel: "#14082c",
          cyan: "#22d3ee",
          cyanDim: "#0891b2",
          fuchsia: "#e879f9",
          rose: "#fb7185",
          lime: "#bef264",
        },
      },
      boxShadow: {
        "neb-glow-cyan": "0 0 32px -6px rgba(34, 211, 238, 0.35)",
        "neb-glow-fuchsia": "0 0 40px -8px rgba(232, 121, 249, 0.4)",
        "neb-inset": "inset 0 1px 0 0 rgba(255, 255, 255, 0.06)",
      },
      keyframes: {
        "neb-drift": {
          "0%, 100%": { transform: "translate(0, 0) scale(1)" },
          "33%": { transform: "translate(2%, -1%) scale(1.03)" },
          "66%": { transform: "translate(-1%, 2%) scale(0.98)" },
        },
        "neb-pulse-soft": {
          "0%, 100%": { opacity: "0.45" },
          "50%": { opacity: "0.75" },
        },
      },
      animation: {
        "neb-drift": "neb-drift 22s ease-in-out infinite",
        "neb-pulse-soft": "neb-pulse-soft 6s ease-in-out infinite",
      },
    },
  },
  plugins: [
    plugin(({ addVariant }) => {
      addVariant("nebula", "html.dark.theme-nebula &")
    }),
  ],
}

