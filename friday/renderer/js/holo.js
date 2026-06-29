// HoloField — a symmetric, centered Iron-Man-style holographic HUD.
// Concentric counter-rotating arc-rings, segmented data blocks, fine tick
// scales, a reticle and an arc-reactor core. No radar sweep. A radial mask
// fades the edges so nothing ever clips into a hard disk.
window.HoloField = class HoloField {
  constructor(canvas, opts = {}) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.dpr = window.devicePixelRatio || 1;
    this.t = 0;
    this.cyan = opts.hue ?? 194;
    this.gold = opts.gold ?? 42;
    this.energy = 0.35;
    this.targetEnergy = 0.35;
    this.ripples = [];
    this.motes = [];
    this.running = false;
    this.cxBias = opts.cxBias ?? 0.5;
    this.cyBias = opts.cyBias ?? 0.5;
    this.scale = opts.scale ?? 1;
    this._resize();
    for (let i = 0; i < (opts.motes ?? 40); i++) {
      this.motes.push({ a: Math.random() * 6.28, r: 0.5 + Math.random() * 0.9, sp: (Math.random() - 0.5) * 0.004, s: 0.5 + Math.random() * 1.4, ph: Math.random() * 6.28 });
    }
    this._ro = () => this._resize();
    window.addEventListener('resize', this._ro);
    this._auto = setInterval(() => this.pulse(), 5200 + Math.random() * 3000);
  }

  _resize() {
    const r = this.canvas.getBoundingClientRect();
    this.w = r.width || 600; this.h = r.height || 600;
    this.canvas.width = this.w * this.dpr;
    this.canvas.height = this.h * this.dpr;
    this.ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    this.cx = this.w * this.cxBias;
    this.cy = this.h * this.cyBias;
    this.R = Math.min(this.w, this.h) * 0.34 * this.scale;
  }

  setEnergy(v) { this.targetEnergy = Math.max(0, Math.min(1, v)); }
  setHue() {}
  pulse() { this.ripples.push({ t: 0 }); this.targetEnergy = 1; }

  start() { if (this.running) return; this.running = true; this._loop(); }
  stop() { this.running = false; clearInterval(this._auto); window.removeEventListener('resize', this._ro); }

  _maybeResize() {
    const r = this.canvas.getBoundingClientRect();
    if (r.width > 0 && (Math.abs(r.width - this.w) > 1 || Math.abs(r.height - this.h) > 1)) this._resize();
  }

  _loop() {
    if (!this.running) return;
    this._maybeResize();           // keep circles true once the panel is laid out
    this.t += 0.016;
    this.energy += (this.targetEnergy - this.energy) * 0.05;
    this.targetEnergy += (0.35 - this.targetEnergy) * 0.012;
    this._draw();
    requestAnimationFrame(() => this._loop());
  }

  _draw() {
    const { ctx, cx, cy, R } = this;
    const e = this.energy;
    const C = this.cyan, G = this.gold;
    ctx.clearRect(0, 0, this.w, this.h);
    ctx.globalCompositeOperation = 'lighter';
    ctx.save();
    ctx.translate(cx, cy);

    // --- arc-reactor core glow ---
    const core = ctx.createRadialGradient(0, 0, 1, 0, 0, R * 0.42);
    core.addColorStop(0, `hsla(${C},100%,85%,${0.22 + e * 0.18})`);
    core.addColorStop(0.5, `hsla(${C},100%,60%,${0.08 + e * 0.08})`);
    core.addColorStop(1, 'hsla(0,0%,0%,0)');
    ctx.fillStyle = core;
    ctx.beginPath(); ctx.arc(0, 0, R * 0.42, 0, 6.2832); ctx.fill();

    // --- outermost: fine tick scale ---
    this._ticks(R * 1.32, 120, this.t * 0.05, 0.06 + e * 0.05, C);

    // --- big segmented arc ring (gold accent), rotating ---
    this._arcs(R * 1.16, 3, 0.22, this.t * 0.12, 3.2, 0.55 + e * 0.3, G);
    this._arcs(R * 1.16, 3, 0.05, this.t * 0.12 + 0.34, 2, 0.35, G); // trailing slivers
    this._arcs(R * 1.24, 2, 0.14, -this.t * 0.08, 1.6, 0.3 + e * 0.2, C); // thin counter arc

    // --- dashed data ring, counter-rotating (cyan) ---
    this._dashes(R * 0.98, 54, -this.t * 0.16, 0.18 + e * 0.12, C);

    // --- segmented blocks ring, a few "lit" ---
    this._blocks(R * 0.82, 40, this.t * 0.07, e, C);

    // --- double inner rings ---
    this._circle(R * 0.64, 1.2, 0.22 + e * 0.12, C);
    this._circle(R * 0.6, 0.8, 0.12, C);

    // --- inner reticle (counter-rotating bracket arcs + crosshair) ---
    this._arcs(R * 0.5, 4, 0.16, -this.t * 0.22, 2, 0.3 + e * 0.2, C);
    this._crosshair(R * 0.5, C, 0.18 + e * 0.1);

    // --- orbiting nodes on a fixed ring ---
    for (let i = 0; i < 3; i++) {
      const a = this.t * 0.3 + (i * 6.2832 / 3);
      const x = Math.cos(a) * R * 1.16, y = Math.sin(a) * R * 1.16;
      ctx.fillStyle = `hsla(${G},100%,75%,${0.5 + e * 0.4})`;
      ctx.beginPath(); ctx.arc(x, y, 2.4, 0, 6.2832); ctx.fill();
    }

    // --- drifting motes ---
    for (const m of this.motes) {
      m.a += m.sp * (0.6 + e); m.ph += 0.02;
      const rr = R * m.r;
      const tw = 0.5 + 0.5 * Math.sin(m.ph);
      ctx.fillStyle = `hsla(${C + 4},100%,82%,${(0.10 + e * 0.2) * tw})`;
      ctx.beginPath(); ctx.arc(Math.cos(m.a) * rr, Math.sin(m.a) * rr, m.s, 0, 6.2832); ctx.fill();
    }

    // --- expanding ripple pulses (the "woosh") ---
    this.ripples = this.ripples.filter((p) => p.t < 1);
    for (const p of this.ripples) {
      p.t += 0.02;
      const rr = R * (0.4 + p.t * 1.2);
      ctx.strokeStyle = `hsla(${C},100%,75%,${0.4 * (1 - p.t)})`;
      ctx.lineWidth = 2 * (1 - p.t);
      ctx.beginPath(); ctx.arc(0, 0, rr, 0, 6.2832); ctx.stroke();
    }

    ctx.restore();

    // --- radial mask: fade to transparent at the edges ---
    ctx.globalCompositeOperation = 'destination-in';
    const mask = ctx.createRadialGradient(cx, cy, R * 0.2, cx, cy, R * 1.5);
    mask.addColorStop(0, 'rgba(0,0,0,1)');
    mask.addColorStop(0.72, 'rgba(0,0,0,0.9)');
    mask.addColorStop(1, 'rgba(0,0,0,0)');
    ctx.fillStyle = mask;
    ctx.fillRect(0, 0, this.w, this.h);
    ctx.globalCompositeOperation = 'source-over';
  }

  // ---- primitives (origin already translated to center) ----
  _circle(r, lw, alpha, hue) {
    const ctx = this.ctx;
    ctx.strokeStyle = `hsla(${hue},100%,68%,${alpha})`; ctx.lineWidth = lw;
    ctx.beginPath(); ctx.arc(0, 0, r, 0, 6.2832); ctx.stroke();
  }
  _arcs(r, count, span, rot, lw, alpha, hue) {
    const ctx = this.ctx;
    ctx.strokeStyle = `hsla(${hue},100%,68%,${alpha})`; ctx.lineWidth = lw; ctx.lineCap = 'round';
    const step = 6.2832 / count;
    for (let i = 0; i < count; i++) {
      const a0 = rot + i * step;
      ctx.beginPath(); ctx.arc(0, 0, r, a0, a0 + span * 6.2832); ctx.stroke();
    }
  }
  _dashes(r, n, rot, alpha, hue) {
    const ctx = this.ctx;
    ctx.strokeStyle = `hsla(${hue},100%,70%,${alpha})`; ctx.lineWidth = 1.3;
    const step = 6.2832 / n;
    for (let i = 0; i < n; i++) {
      const a = rot + i * step;
      ctx.beginPath(); ctx.arc(0, 0, r, a, a + step * 0.45); ctx.stroke();
    }
  }
  _blocks(r, n, rot, e, hue) {
    const ctx = this.ctx;
    const step = 6.2832 / n;
    for (let i = 0; i < n; i++) {
      const a = rot + i * step;
      const lit = (Math.sin(i * 1.7 + this.t * 1.5) > 0.4);
      const al = lit ? 0.35 + e * 0.4 : 0.1;
      ctx.strokeStyle = `hsla(${hue},100%,72%,${al})`; ctx.lineWidth = 3.4;
      ctx.beginPath();
      ctx.arc(0, 0, r, a, a + step * 0.5); ctx.stroke();
    }
  }
  _ticks(r, n, rot, alpha, hue) {
    const ctx = this.ctx;
    ctx.save(); ctx.rotate(rot);
    ctx.strokeStyle = `hsla(${hue},100%,72%,${alpha})`; ctx.lineWidth = 1;
    const step = 6.2832 / n;
    for (let i = 0; i < n; i++) {
      const big = i % 10 === 0; const a = i * step;
      const len = big ? 10 : 4;
      ctx.beginPath();
      ctx.moveTo(Math.cos(a) * r, Math.sin(a) * r);
      ctx.lineTo(Math.cos(a) * (r + len), Math.sin(a) * (r + len));
      ctx.stroke();
    }
    ctx.restore();
  }
  _crosshair(r, hue, alpha) {
    const ctx = this.ctx;
    ctx.strokeStyle = `hsla(${hue},100%,75%,${alpha})`; ctx.lineWidth = 1;
    for (const a of [0, Math.PI / 2, Math.PI, Math.PI * 1.5]) {
      ctx.beginPath();
      ctx.moveTo(Math.cos(a) * r * 0.78, Math.sin(a) * r * 0.78);
      ctx.lineTo(Math.cos(a) * r * 0.96, Math.sin(a) * r * 0.96);
      ctx.stroke();
    }
  }
};
