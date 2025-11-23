#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
index_html.py

Large HTML template for the built-in head-to-head UI.
"""

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <title>Rugby Head-to-Head</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b1120;
      --bg-alt: #020617;
      --card: #020617;
      --accent: #38bdf8;
      --accent-soft: rgba(56, 189, 248, 0.15);
      --text: #e5e7eb;
      --text-muted: #9ca3af;
      --border-subtle: rgba(148, 163, 184, 0.35);
      --shadow-soft: 0 18px 40px rgba(15, 23, 42, 0.85);
      --radius-xl: 20px;
      --radius-2xl: 24px;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
      background: radial-gradient(circle at top, #1e293b 0, #020617 55%, black 100%);
      color: var(--text);
      min-height: 100vh;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 24px;
    }

    .app-shell {
      width: 100%;
      max-width: 1200px;
      background: radial-gradient(circle at top left, rgba(56, 189, 248, 0.08), transparent 55%),
                  radial-gradient(circle at bottom right, rgba(129, 140, 248, 0.08), transparent 55%),
                  linear-gradient(to bottom right, rgba(15,23,42,0.98), rgba(15,23,42,0.9));
      border-radius: 32px;
      border: 1px solid rgba(148, 163, 184, 0.35);
      box-shadow:
        0 25px 80px rgba(15,23,42,0.95),
        0 0 0 1px rgba(15,23,42,0.7);
      padding: 24px 24px 28px;
      position: relative;
      overflow: hidden;
    }

    .app-shell::before {
      content: "";
      position: absolute;
      inset: 0;
      background:
        radial-gradient(circle at top left, rgba(37,99,235,0.14), transparent 60%),
        radial-gradient(circle at bottom right, rgba(56,189,248,0.12), transparent 65%);
      opacity: 0.9;
      pointer-events: none;
    }

    .app-shell-inner {
      position: relative;
      z-index: 1;
    }

    header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 24px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .logo-pill {
      width: 40px;
      height: 40px;
      border-radius: 999px;
      background:
        conic-gradient(from 190deg, #38bdf8, #22c55e, #a855f7, #38bdf8);
      padding: 1.5px;
      box-shadow:
        0 0 0 1px rgba(15,23,42,0.9),
        0 0 22px rgba(56,189,248,0.65);
    }

    .logo-inner {
      width: 100%;
      height: 100%;
      border-radius: inherit;
      background: radial-gradient(circle at 20% 0%, rgba(248,250,252,0.16), transparent 45%),
                  radial-gradient(circle at 80% 120%, rgba(248,250,252,0.1), transparent 50%),
                  #020617;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .logo-mark {
      width: 20px;
      height: 20px;
      border-radius: 8px;
      border: 1px solid rgba(148, 163, 184, 0.75);
      display: grid;
      grid-template-columns: repeat(2, 1fr);
      gap: 1.5px;
      padding: 2px;
    }

    .logo-mark span {
      border-radius: 3px;
      background: linear-gradient(135deg, rgba(56,189,248,0.8), rgba(14,165,233,0.35));
      box-shadow: 0 0 10px rgba(56,189,248,0.7);
    }

    .logo-mark span:nth-child(2),
    .logo-mark span:nth-child(3) {
      background: linear-gradient(135deg, rgba(34,197,94,0.9), rgba(16,185,129,0.35));
    }

    .brand-text {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .brand-title {
      display: flex;
      align-items: baseline;
      gap: 7px;
    }

    .brand-title h1 {
      font-size: 22px;
      font-weight: 650;
      letter-spacing: 0.02em;
      margin: 0;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .brand-chip {
      font-size: 11px;
      padding: 4px 7px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.45);
      background: radial-gradient(circle at top, rgba(15,23,42,0.9), rgba(15,23,42,0.95));
      color: var(--text-muted);
    }

    .brand-subtitle {
      font-size: 12px;
      color: var(--text-muted);
      display: flex;
      gap: 10px;
      align-items: center;
    }

    .brand-subtitle span {
      display: inline-flex;
      align-items: center;
      gap: 4px;
    }

    .brand-subtitle svg {
      width: 13px;
      height: 13px;
      opacity: 0.85;
    }

    .meta {
      display: flex;
      flex-direction: column;
      align-items: flex-end;
      gap: 6px;
      font-size: 11px;
      color: var(--text-muted);
    }

    .meta-row {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      padding: 4px 9px;
      border-radius: 999px;
      border: 1px solid rgba(148, 163, 184, 0.4);
      background: radial-gradient(circle at top, rgba(15,23,42,0.9), rgba(15,23,42,0.98));
      backdrop-filter: blur(12px);
    }

    .meta-dot {
      width: 7px;
      height: 7px;
      border-radius: 999px;
      background: #22c55e;
      box-shadow: 0 0 10px rgba(34,197,94,0.9);
    }

    .meta-pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 8px;
      border-radius: 999px;
      background: radial-gradient(circle at top, rgba(15,23,42,0.95), rgba(15,23,42,1));
      border: 1px solid rgba(148, 163, 184, 0.5);
    }

    .meta-pill strong {
      font-weight: 600;
      color: #e5e7eb;
    }

    .meta-pill span {
      color: var(--text-muted);
    }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(0, 1.3fr);
      gap: 20px;
    }

    /* ... SNIP ...  (rest of CSS & JS from your existing INDEX_HTML) */
  </style>
</head>
<body>
  <!-- KEEP the rest of your existing HTML + JS exactly as in your current main.py -->
</body>
</html>
"""
