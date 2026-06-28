// HoloField — ambient holographic motion for panel backgrounds. Rotating HUD
// rings, a sweeping radar scan, periodic light "wooshes", and drifting motes.
// Everything is thin-edged and a radial mask fades it to transparent at the
// borders, so it never produces the hard clipped disk that read as a "glitch".
window.HoloField = class HoloField {
  constructor(canvas, opts = {}) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.dpr = window.devicePixelRatio || 1;
    this.t = 0;
    this.hue = opts.hue ?? 196;          // holographic cyan
    this.gold = opts.gold ?? 40;
    this.energy = 0.3;
    this.targetEnergy = 0.3;
    this.wooshes = [];
    this.motes = [];
    this.running = false;
    this.cxBias = opts.cxBias ?? 0.5;    // center of the ring system (0..1)
    this.cyBias = opts.cyBias ?? 0.5;
    this._resize();
    this._seed(opts.motes ?? 46);
    this._ro = () => this._resize();
    window.addEventListener('resize', this._ro);
    // a woosh on a relaxed cadence keeps it feeling alive
    this._auto = setInterval(() => this.pulse(), 4200 + Math.random() * 2600);
  }

  _resize() {
    const r = this.canvas.getBoundingClientRect();
    this.w = r.width || 600; this.h = r.height || 600;
    this.canvas.width = this.w * this.dpr;
    this.canvas.height = this.h * this.dpr;
    this.ctx.setTransform(this.dpr, 0, 0, this.dpr, 0, 0);
    this.cx = this.w * this.cxBias;
    this.cy = this.h * this.cyBias;
    this.R = Math.min(this.w, this.h) * 0.42;
  }

  _seed(n) {
    for (let i = 0; i < n; i++) {
      this.motes.push({ x: Math.random(), y: Math.random(), s: 0.4 + Math.random() * 1.6, v: 0.01 + Math.random() * 0.04, ph: Math.random() * 6.28 });
    }
  }

  setEnergy(v) { this.targetEnergy = Math.max(0, Math.min(1, v)); }
  setHue(h) { this.hue = h; }
  pulse() { this.wooshes.push({ a: Math.random() * Math.PI * 2, t: 0, dir: Math.random() < 0.5 ? 1 : -1 }); this.targetEnergy = 1; }

  start() { if (this.running) return; this.running = true; this._loop(); }
  stop() { this.running = false; clearInterval(this._auto); window.removeEventListener('resize', this._ro); }

  _loop() {
    if (!this.running) return;
    this.t += 0.016;
    this.energy += (this.targetEnergy - this.energy) * 0.05;
    this.targetEnergy += (0.3 - this.targetEnergy) * 0.01;
    this._draw();
    requestAnimationFrame(() => this._loop());
  }

  _draw() {
    const { ctx, cx, cy, R, hue } = this;
    const e = this.energy;
    ctx.clearRect(0, 0, this.w, this.h);
    ctx.globalCompositeOperation = 'lighter';

    // rotating concentric HUD rings (thin — clip-safe)
    this._ring(R * 1.30, this.t * 0.05, 1, 0.05 + e * 0.05, 0);
    this._ring(R * 1.05, -this.t * 0.12, 1.3, 0.10 + e * 0.08, 30);
    this._ring(R * 0.82, this.t * 0.20, 1.1, 0.12 + e * 0.10, 0);
    this._ring(R * 0.6, -this.t * 0.3, 1, 0.10 + e * 0.10, 12);
    this._ticks(R * 1.40, -this.t * 0.03, 110, 0.05 + e * 0.05);

    // radar sweep — the classic scan "woosh"
    this._sweep(this.t * 0.55, R * 1.34);

    // transient wooshes (expanding bright arcs)
    this.wooshes = this.wooshes.filter((w) => w.t < 1);
    for (const w of this.wooshes) {
      w.t += 0.022;
      const grow = Math.sin(w.t * Math.PI);
      const r = R * (0.4 + w.t * 1.1);
      ctx.strokeStyle = `hsla(${hue},100%,72%,${0.5 * grow})`;
      ctx.lineWidth = 2.2 * grow;
      ctx.beginPath();
      ctx.arc(cx, cy, r, w.a, w.a + w.dir * (0.5 + w.t * 1.4));
      ctx.stroke();
    }

    // drifting motes
    for (const m of this.motes) {
      m.y -= m.v * 0.01 * (0.6 + e);
      if (m.y < -0.02) { m.y = 1.02; m.x = Math.random(); }
      const tw = 0.5 + 0.5 * Math.sin(this.t * 2 + m.ph);
      ctx.fillStyle = `hsla(${hue + 6},100%,80%,${(0.12 + e * 0.25) * tw})`;
      ctx.beginPath();
      ctx.arc(m.x * this.w, m.y * this.h, m.s * (0.8 + e), 0, Math.PI * 2);
      ctx.fill();
    }

    // soft radial mask -> fade everything out toward the edges (no hard clip)
    ctx.globalCompositeOperation = 'destination-in';
    const mask = ctx.createRadialGradient(cx, cy, R * 0.2, cx, cy, R * 1.55);
    mask.addColorStop(0, 'rgba(0,0,0,1)');
    mask.addColorStop(0.7, 'rgba(0,0,0,0.85)');
    mask.addColorStop(1, 'rgba(0,0,0,0)');
    ctx.fillStyle = mask;
    ctx.fillRect(0, 0, this.w, this.h);
    ctx.globalCompositeOperation = 'source-over';
  }

  _ring(radius, rot, lw, alpha, dashes) {
    const { ctx, cx, cy, hue } = this;
    ctx.save(); ctx.translate(cx, cy); ctx.rotate(rot);
    ctx.strokeStyle = `hsla(${hue + 4},100%,68%,${alpha})`;
    ctx.lineWidth = lw;
    if (dashes) {
      const step = (Math.PI * 2) / dashes;
      for (let i = 0; i < dashes; i++) { ctx.beginPath(); ctx.arc(0, 0, radius, i * step, i * step + step * 0.55); ctx.stroke(); }
    } else { ctx.beginPath(); ctx.arc(0, 0, radius, 0, Math.PI * 2); ctx.stroke(); }
    ctx.restore();
  }

  _ticks(radius, rot, n, alpha) {
    const { ctx, cx, cy, hue } = this;
    ctx.save(); ctx.translate(cx, cy); ctx.rotate(rot);
    ctx.strokeStyle = `hsla(${hue},100%,72%,${alpha})`; ctx.lineWidth = 1;
    const step = (Math.PI * 2) / n;
    for (let i = 0; i < n; i++) {
      const big = i % 10 === 0; const a = i * step;
      ctx.beginPath();
      ctx.moveTo(Math.cos(a) * radius, Math.sin(a) * radius);
      ctx.lineTo(Math.cos(a) * (radius + (big ? 9 : 4)), Math.sin(a) * (radius + (big ? 9 : 4)));
      ctx.stroke();
    }
    ctx.restore();
  }

  _sweep(angle, radius) {
    const { ctx, cx, cy, hue } = this;
    ctx.save(); ctx.translate(cx, cy); ctx.rotate(angle);
    const g = ctx.createConicGradient ? ctx.createConicGradient(0, 0, 0) : null;
    if (g) {
      g.addColorStop(0, `hsla(${hue},100%,70%,0.22)`);
      g.addColorStop(0.08, 'hsla(0,0%,0%,0)');
      g.addColorStop(1, 'hsla(0,0%,0%,0)');
      ctx.fillStyle = g;
      ctx.beginPath(); ctx.moveTo(0, 0); ctx.arc(0, 0, radius, 0, Math.PI * 0.5); ctx.closePath(); ctx.fill();
    } else {
      ctx.strokeStyle = `hsla(${hue},100%,70%,0.25)`; ctx.lineWidth = 2;
      ctx.beginPath(); ctx.moveTo(0, 0); ctx.lineTo(radius, 0); ctx.stroke();
    }
    ctx.restore();
  }
};
