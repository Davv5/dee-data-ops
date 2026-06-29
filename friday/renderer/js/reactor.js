// The Reactor — a glossy "solar core" rendered on canvas. Rotating HUD rings,
// a molten plasma core, orbiting sparks and intermittent solar flares. Drives
// the FRIDAY look on both the quick-add HUD and the dashboard. Intensity is
// modulated externally (e.g. spikes while listening / speaking).
window.Reactor = class Reactor {
  constructor(canvas, opts = {}) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.dpr = window.devicePixelRatio || 1;
    this.t = 0;
    this.intensity = 0;          // eased 0..1 "energy" level
    this.idle = opts.idle ?? 0.25;
    this.targetIntensity = this.idle;
    this.hue = opts.hue ?? 38;   // solar gold
    this.sparks = [];
    this.flares = [];
    this.running = false;
    this._seedSparks(opts.sparks ?? 70);
    this._resize();
    window.addEventListener('resize', () => this._resize());
  }

  _resize() {
    const r = this.canvas.getBoundingClientRect();
    this.w = r.width; this.h = r.height;
    this.canvas.width = r.width * this.dpr;
    this.canvas.height = r.height * this.dpr;
    this.ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    this.cx = this.w / 2; this.cy = this.h / 2;
    this.R = Math.min(this.w, this.h) * 0.32;
  }

  _seedSparks(n) {
    for (let i = 0; i < n; i++) {
      this.sparks.push({
        a: Math.random() * Math.PI * 2,
        r: 0.6 + Math.random() * 1.5,        // radius multiplier of core
        spd: (0.2 + Math.random() * 0.8) * (Math.random() < 0.5 ? 1 : -1),
        size: 0.5 + Math.random() * 1.8,
        life: Math.random()
      });
    }
  }

  setEnergy(v) { this.targetIntensity = Math.max(0, Math.min(1, v)); }
  pulse() { this.targetIntensity = 1; this.flares.push({ a: Math.random() * Math.PI * 2, t: 0, len: 0.6 + Math.random() * 0.5 }); }
  setHue(h) { this.hue = h; }

  start() { if (this.running) return; this.running = true; this._loop(); }
  stop() { this.running = false; }

  _loop() {
    if (!this.running) return;
    this.t += 0.016;
    this.intensity += (this.targetIntensity - this.intensity) * 0.06;
    // breathe back toward idle
    this.targetIntensity += (this.idle - this.targetIntensity) * 0.01;
    this._draw();
    requestAnimationFrame(() => this._loop());
  }

  _draw() {
    const { ctx, cx, cy, R, hue } = this;
    const e = this.intensity;
    ctx.clearRect(0, 0, this.w, this.h);
    ctx.globalCompositeOperation = 'lighter';

    // --- outer aura ---
    const aura = ctx.createRadialGradient(cx, cy, R * 0.2, cx, cy, R * (2.6 + e));
    aura.addColorStop(0, `hsla(${hue},100%,60%,${0.18 + e * 0.22})`);
    aura.addColorStop(0.5, `hsla(${hue + 8},100%,50%,${0.06 + e * 0.1})`);
    aura.addColorStop(1, 'hsla(220,100%,50%,0)');
    ctx.fillStyle = aura;
    ctx.fillRect(0, 0, this.w, this.h);

    // --- HUD rings ---
    this._ring(R * 1.95, this.t * 0.10, 64, 0.10 + e * 0.10, 1);
    this._ring(R * 1.62, -this.t * 0.18, 28, 0.16 + e * 0.14, 1.4, true);
    this._ring(R * 1.30, this.t * 0.30, 12, 0.22 + e * 0.22, 1.2);
    this._tickRing(R * 2.18, -this.t * 0.06, 120, 0.06 + e * 0.06);

    // --- orbiting sparks ---
    for (const s of this.sparks) {
      s.a += s.spd * 0.01 * (0.6 + e);
      s.life += 0.01;
      const rr = R * (1.0 + s.r * (0.5 + 0.5 * Math.sin(s.life)));
      const x = cx + Math.cos(s.a) * rr;
      const y = cy + Math.sin(s.a) * rr * 0.96;
      const tw = 0.5 + 0.5 * Math.sin(this.t * 3 + s.a * 5);
      ctx.beginPath();
      ctx.fillStyle = `hsla(${hue + 6},100%,${70 + tw * 20}%,${(0.25 + e * 0.5) * tw})`;
      ctx.arc(x, y, s.size * (0.8 + e), 0, Math.PI * 2);
      ctx.fill();
    }

    // --- solar flares ---
    this.flares = this.flares.filter((f) => f.t < 1);
    for (const f of this.flares) {
      f.t += 0.02;
      const grow = Math.sin(f.t * Math.PI);
      const r1 = R * 1.1;
      const r2 = R * (1.1 + f.len * grow * 1.6);
      const x1 = cx + Math.cos(f.a) * r1, y1 = cy + Math.sin(f.a) * r1;
      const x2 = cx + Math.cos(f.a) * r2, y2 = cy + Math.sin(f.a) * r2;
      const g = ctx.createLinearGradient(x1, y1, x2, y2);
      g.addColorStop(0, `hsla(${hue},100%,70%,${0.6 * grow})`);
      g.addColorStop(1, `hsla(${hue + 20},100%,60%,0)`);
      ctx.strokeStyle = g; ctx.lineWidth = 2.5 * grow; ctx.lineCap = 'round';
      ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
    }

    // --- molten core ---
    const pulse = 1 + Math.sin(this.t * 2.2) * 0.04 + e * 0.08;
    const core = ctx.createRadialGradient(cx, cy, 1, cx, cy, R * pulse);
    core.addColorStop(0, '#fff7e6');
    core.addColorStop(0.25, `hsl(${hue + 10},100%,72%)`);
    core.addColorStop(0.6, `hsl(${hue},100%,55%)`);
    core.addColorStop(1, `hsla(${hue - 6},100%,42%,0)`);
    ctx.fillStyle = core;
    ctx.beginPath();
    ctx.arc(cx, cy, R * pulse, 0, Math.PI * 2);
    ctx.fill();

    // --- gloss highlight on the core (the "glossy" ask) ---
    ctx.globalCompositeOperation = 'source-over';
    const gloss = ctx.createRadialGradient(cx - R * 0.3, cy - R * 0.35, 1, cx - R * 0.3, cy - R * 0.35, R * 0.9);
    gloss.addColorStop(0, 'rgba(255,255,255,0.5)');
    gloss.addColorStop(0.4, 'rgba(255,255,255,0.05)');
    gloss.addColorStop(1, 'rgba(255,255,255,0)');
    ctx.fillStyle = gloss;
    ctx.beginPath(); ctx.arc(cx, cy, R * pulse, 0, Math.PI * 2); ctx.fill();
    ctx.globalCompositeOperation = 'lighter';
  }

  _ring(radius, rot, segments, alpha, lw, dashed) {
    const { ctx, cx, cy, hue } = this;
    ctx.save();
    ctx.translate(cx, cy); ctx.rotate(rot);
    ctx.strokeStyle = `hsla(${hue + 4},100%,65%,${alpha})`;
    ctx.lineWidth = lw;
    if (dashed) {
      const step = (Math.PI * 2) / segments;
      for (let i = 0; i < segments; i++) {
        ctx.beginPath();
        ctx.arc(0, 0, radius, i * step, i * step + step * 0.55);
        ctx.stroke();
      }
    } else {
      ctx.beginPath(); ctx.arc(0, 0, radius, 0, Math.PI * 2); ctx.stroke();
    }
    ctx.restore();
  }

  _tickRing(radius, rot, ticks, alpha) {
    const { ctx, cx, cy, hue } = this;
    ctx.save();
    ctx.translate(cx, cy); ctx.rotate(rot);
    ctx.strokeStyle = `hsla(${hue},100%,70%,${alpha})`;
    ctx.lineWidth = 1;
    const step = (Math.PI * 2) / ticks;
    for (let i = 0; i < ticks; i++) {
      const big = i % 10 === 0;
      const r1 = radius, r2 = radius + (big ? 8 : 3);
      const a = i * step;
      ctx.beginPath();
      ctx.moveTo(Math.cos(a) * r1, Math.sin(a) * r1);
      ctx.lineTo(Math.cos(a) * r2, Math.sin(a) * r2);
      ctx.stroke();
    }
    ctx.restore();
  }
};
