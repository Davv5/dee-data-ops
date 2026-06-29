# Jarvis — local voice assistant for Apple Silicon

A fully local, voice-driven assistant for a MacBook (M-series). It listens, thinks, and
acts on your machine — no cloud, no recurring API fees. Built on the standard local stack:

| Role        | Tool              |
|-------------|-------------------|
| Brain       | **Ollama** + **Qwen 3 8B** |
| Speech → text | **whisper.cpp** |
| Text → speech | **Piper**       |
| Wake word   | **openWakeWord** ("hey jarvis") |

Claude Code is used only to *write* this code, never at runtime.

## Status — phased build

The endgame is hands-free "movie Jarvis." We get there in layers; each phase runs on its own.

- **Phase 0 — Brain online.** Ollama + Qwen 3 answering in a text REPL. ✅
- **Phase 1 — Tool-calling agent.** Shell, AppleScript, app launching, files, web search —
  the "do anything" core, driven by typed input. ✅
- **HUD bridge — `jarvis serve`.** Local HTTP brain the macOS JARVIS HUD talks to;
  adds task tools (`add_task`/`list_tasks`/`complete_task`). See
  `../jarvis-todo/INTEGRATION.md`. ✅
- **Phase 2 — Voice in/out** (whisper.cpp + Piper, push-to-talk). _next_
- **Phase 3 — Hands-free** (openWakeWord always-listening). _planned_
- **Phase 4 — Movie polish** (streaming speech, barge-in, latency tuning, memory). _planned_
- **Phase 5 — Skills** (email, calendar, deeper automation). _planned_

## Quick start (macOS)

```bash
./install.sh          # Homebrew, Ollama, pulls qwen3:8b, Python venv, config.yaml
source .venv/bin/activate
jarvis doctor         # verify Ollama + model + deps
jarvis chat           # talk to it by text
jarvis serve          # run the brain for the macOS HUD (port 11500)
```

Then try, in the REPL:

```
you › what's in my Downloads folder?
you › open Safari
you › make a file on my Desktop called todo.md with three startup ideas
```

## Configuration

`config.yaml` (created from `config.example.yaml`) controls the model, voice, and the
safety policy:

```yaml
safety:
  confirm_destructive: true   # ask y/N before shell/file-write/AppleScript/email
```

Set `confirm_destructive: false` for fully autonomous operation. It ships **on** as a
seatbelt you control — not a leash.

## macOS permissions

The voice and automation features need permissions granted once, under
**System Settings → Privacy & Security**:

- **Microphone** — for the terminal app you run Jarvis from (Phase 2+).
- **Accessibility** & **Automation** — for AppleScript / controlling other apps.

## Tests

```bash
pip install pytest
pytest                # registry + agent loop run against a fake LLM, no Ollama needed
```

## Layout

```
jarvis/
  install.sh                 setup for macOS
  config.example.yaml        copy to config.yaml
  scripts/download_models.sh whisper + piper models (Phase 2)
  jarvis/
    cli.py                   `jarvis chat|doctor|voice|run`
    config.py
    brain/
      llm.py                 Ollama client
      agent.py               conversation + tool-calling loop
      prompts.py             persona
      tools/                 shell, applescript, files, web (the action layer)
    loop/
      text.py                Phase 1 text REPL
  tests/
```
