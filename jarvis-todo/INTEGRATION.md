# JARVIS — HUD + Brain integration

The gold HUD (`jarvis-todo/`, Electron) is the **face**. The local LLM
(`jarvis/`, Python — Ollama + Qwen 3) is the **brain**. This wires them together
so any directive you speak/type into the HUD is understood, answered, and acted
on — while still working as a plain task app if the brain isn't running.

## How it flows

```
⇧⌘Space  →  HUD directive box  ──askBrain(text)──►  main.js IPC
                                                        │  HTTP POST /chat
                                                        ▼
                                          jarvis serve  (Python, :11500)
                                          Qwen 3 + tools: shell, apps,
                                          files, web, add_task/complete_task
                                                        │  { reply, actions }
                                          ◄─────────────┘
   HUD speaks the reply (jarvis-voice) ◄── main applies task actions to the
   and stays open to converse              store and broadcasts tasks:changed
```

Input routing (in `quickadd.js`): instant rule-based intents (thanks / greet /
reschedule / snooze / complete / "make a tag") are handled locally for snappiness;
an explicit "remind me to X" makes a directive directly; **everything ambiguous**
("open Safari", "what's 20% of 340", "email Sam") goes to the brain. You can type
**or speak** (🎙 mic button, Web Speech) — spoken text flows through the same path.

- **Brain online** → the directive goes to Qwen 3. "open Safari" launches it;
  "remind me to call mum at 5" creates a task (via the `add_task` action the
  main process applies to the same store the dashboard reads); "what's 20% of
  340?" is answered aloud. The HUD shows the reply and stays open to continue.
- **Brain offline** → the HUD falls back to the original local NL parser and
  just adds a task. Nothing breaks; you simply lose thinking/acting.

The status line in the HUD reads **JARVIS · ONLINE** when the brain is reachable,
**JARVIS · LISTENING** when it's local-only, and **JARVIS · THINKING** mid-turn.

## Pieces added

| File | Role |
|---|---|
| `jarvis/jarvis/server.py` | `jarvis serve` — localhost HTTP brain (`/health`, `/chat`) |
| `jarvis/jarvis/brain/actions.py` | per-request action bridge (task ops the app applies) |
| `jarvis/jarvis/brain/tools/tasks.py` | `add_task` / `list_tasks` / `complete_task` tools |
| `jarvis-todo/src/brain.js` | main-process HTTP client to the brain |
| `jarvis-todo/main.js` | `brain:ask` / `brain:health` IPC + applies task actions |
| `jarvis-todo/preload.js` | exposes `askBrain` / `brainHealth` to the renderer |
| `jarvis-todo/renderer/js/quickadd.js` | routes directives to the brain, falls back to NL task add |

## Run it (macOS)

One command (starts Ollama + brain + HUD):

```zsh
cd ~/Documents/dee-data-ops/jarvis-todo
./start-jarvis.sh
```

Or manually, two terminals:

```zsh
# terminal 1 — the brain
cd ~/jarvis-brain/jarvis && source .venv/bin/activate && jarvis serve

# terminal 2 — the HUD
cd ~/Documents/dee-data-ops/jarvis-todo && open JARVIS.app
```

(First-time brain setup lives in `jarvis/README.md` → `./install.sh`.)

## Acting on the Mac — the safety flag

System actions (`run_shell`, `run_applescript`, `write_file`) are **destructive**
and gated by `safety.confirm_destructive` in `jarvis/config.yaml`. The HUD has no
y/N prompt, so while that flag is **on** the brain will *decline* those actions
and say so. For full hands-on autonomy ("do anything I say"), set:

```yaml
safety:
  confirm_destructive: false
```

Non-destructive things (open apps, read files, web search, manage tasks, answer
questions) work either way. Flip the flag only when you're comfortable letting
Jarvis run commands unattended.

## Known edges (honest list)

- One shared brain session — conversation memory persists across HUD summons
  (nice), but two overlapping requests are serialized, not parallel.
- First token after the model loads can take a few seconds; the HUD shows
  THINKING and the request timeout is generous (120s).
- `web_search` scrapes DuckDuckGo HTML — fine to start, brittle if their markup
  changes. Swappable for a real API later.
- Voice **input** uses the browser's Web Speech API (the 🎙 button), which on
  macOS routes through Apple's recognizer — not the fully-local whisper.cpp path
  yet. Swapping to whisper.cpp (and a "hey jarvis" wake word) is the next step.
