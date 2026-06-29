// Bridge to the local JARVIS brain (the Python `jarvis serve` process).
// All calls stay on localhost. If the brain isn't running, callers fall back
// to the built-in NL task parser, so the app keeps working without it.

const BRAIN_URL = process.env.JARVIS_BRAIN_URL || 'http://127.0.0.1:11500';

async function withTimeout(promise, ms) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await promise(ctrl.signal);
  } finally {
    clearTimeout(t);
  }
}

// Quick liveness probe. Returns { ok, ollama } or { ok:false } if unreachable.
async function health() {
  try {
    return await withTimeout(async (signal) => {
      const res = await fetch(`${BRAIN_URL}/health`, { signal });
      if (!res.ok) return { ok: false };
      return await res.json();
    }, 1500);
  } catch (_) {
    return { ok: false };
  }
}

// Send a directive to the brain. Returns { reply, actions }.
// Throws if the brain is unreachable or errored, so callers can fall back.
async function ask(text, tasks) {
  return withTimeout(async (signal) => {
    const res = await fetch(`${BRAIN_URL}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, tasks: tasks || [] }),
      signal
    });
    if (!res.ok) {
      const detail = await res.json().catch(() => ({}));
      throw new Error(detail.error || `brain HTTP ${res.status}`);
    }
    return res.json();
  }, 120000); // local LLM turns can take a while on first token
}

module.exports = { health, ask, url: BRAIN_URL };
