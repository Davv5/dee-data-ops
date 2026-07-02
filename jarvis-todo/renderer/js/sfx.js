// JARVIS sound design — tiny synthesized sci-fi cues via WebAudio.
// No audio assets; everything is generated. Quiet by design and mute-aware.
window.JARVIS_SFX = (function () {
  let ctx = null;
  let muted = false;

  function ac() {
    if (!ctx) { try { ctx = new (window.AudioContext || window.webkitAudioContext)(); } catch (_) {} }
    if (ctx && ctx.state === 'suspended') { try { ctx.resume(); } catch (_) {} }
    return ctx;
  }

  function tone(freq, t0, dur, type = 'sine', gain = 0.035, sweep = null) {
    const c = ac(); if (!c || muted) return;
    const o = c.createOscillator();
    const g = c.createGain();
    o.type = type;
    o.frequency.setValueAtTime(freq, c.currentTime + t0);
    if (sweep) o.frequency.exponentialRampToValueAtTime(sweep, c.currentTime + t0 + dur);
    g.gain.setValueAtTime(0.0001, c.currentTime + t0);
    g.gain.linearRampToValueAtTime(gain, c.currentTime + t0 + 0.015);
    g.gain.exponentialRampToValueAtTime(0.0001, c.currentTime + t0 + dur);
    o.connect(g).connect(c.destination);
    o.start(c.currentTime + t0);
    o.stop(c.currentTime + t0 + dur + 0.05);
  }

  return {
    setMuted(m) { muted = !!m; },
    // HUD summoned — soft rising sweep
    summon() { tone(520, 0, 0.14, 'sine', 0.028, 1040); tone(1240, 0.07, 0.18, 'sine', 0.02); },
    // directive committed — two-note confirm
    commit() { tone(880, 0, 0.09, 'triangle', 0.032); tone(1320, 0.07, 0.15, 'triangle', 0.026); },
    // directive completed — warm resolve
    done() { tone(660, 0, 0.1, 'sine', 0.03); tone(990, 0.09, 0.18, 'sine', 0.028); },
    // reminder alert — three-note motif
    alert() { tone(740, 0, 0.12, 'sine', 0.04); tone(988, 0.11, 0.12, 'sine', 0.04); tone(1480, 0.22, 0.24, 'sine', 0.03); }
  };
})();
