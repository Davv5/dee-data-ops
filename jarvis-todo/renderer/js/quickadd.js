// Quick-add HUD controller — summon animation, live NL parsing, explicit time
// setting (chips + custom picker), colour-class selection, and commit with a
// spoken acknowledgement.
(function () {
  const C = window.JARVIS_COLORS;
  const V = window.JARVIS_VOICE;
  const stage = document.getElementById('stage');
  const input = document.getElementById('input');
  const preview = document.getElementById('preview');
  const catsEl = document.getElementById('cats');
  const timesEl = document.getElementById('times');
  const customTime = document.getElementById('qaCustomTime');
  const commitBtn = document.getElementById('commit');

  let manualCategory = null;       // user override via pills/Tab
  // undefined = follow the typed text; null = explicitly "no time"; Date = set
  let manualDue;
  let lastParse = { due: null, cleanTitle: '' };

  // --- holographic field ---
  const reactor = new window.HoloField(document.getElementById('holo'), { hue: 194, motes: 30, cxBias: 0.5, cyBias: 0.5, scale: 1.1 });
  reactor.start();
  // keep .pulse()/.setEnergy()/.setHue() call-sites working unchanged

  // --- category pills ---
  C.order.forEach((key) => {
    const c = C.byKey(key);
    const el = document.createElement('div');
    el.className = `qa-cat c-${c.color}`;
    el.dataset.key = key;
    el.innerHTML = `<span class="dot"></span>${c.label}`;
    el.title = c.meaning;
    el.addEventListener('click', () => { setCategory(key, true); input.focus(); });
    catsEl.appendChild(el);
  });

  // --- time chips ---
  const TIME_PRESETS = [
    { id: 'none', label: 'No time', get: () => null },
    { id: 'h1', label: '+1 hr', get: () => offset(60) },
    { id: 'h3', label: '+3 hr', get: () => offset(180) },
    { id: 'eve', label: 'Tonight 8pm', get: () => at(0, 20, 0) },
    { id: 'tmr', label: 'Tomorrow 9am', get: () => at(1, 9, 0) },
    { id: 'custom', label: 'Pick…', custom: true }
  ];
  function offset(mins) { const d = new Date(); d.setMinutes(d.getMinutes() + mins); return d; }
  function at(addDays, h, m) { const d = new Date(); d.setDate(d.getDate() + addDays); d.setHours(h, m, 0, 0); return d; }

  TIME_PRESETS.forEach((p) => {
    const el = document.createElement('div');
    el.className = 'qa-time' + (p.custom ? ' custom' : '');
    el.dataset.id = p.id;
    el.textContent = p.label;
    el.addEventListener('click', () => {
      if (p.custom) {
        customTime.classList.add('show');
        if (!customTime.value) customTime.value = toLocalInput(lastParse.due || offset(60));
        customTime.focus();
        manualDue = new Date(customTime.value);
      } else {
        customTime.classList.remove('show');
        manualDue = p.get();        // null for "none", Date otherwise
      }
      markTime(p.id);
      renderPreview();
      input.focus();
    });
    timesEl.appendChild(el);
  });
  customTime.addEventListener('input', () => {
    if (customTime.value) { manualDue = new Date(customTime.value); markTime('custom'); renderPreview(); }
  });

  function markTime(id) {
    [...timesEl.children].forEach((el) => el.classList.toggle('active', el.dataset.id === id));
  }
  function toLocalInput(d) {
    const z = new Date(d.getTime() - d.getTimezoneOffset() * 60000);
    return z.toISOString().slice(0, 16);
  }

  // --- category state ---
  function activeCategory() { return manualCategory || C.infer(input.value) || 'standard'; }
  function setCategory(key, manual) {
    if (manual) manualCategory = key;
    const active = activeCategory();
    [...catsEl.children].forEach((el) => el.classList.toggle('active', el.dataset.key === active));
    reactor.setHue(hueFor(C.byKey(active).color));
    reactor.pulse();
  }
  function hueFor(color) { return ({ red: 354, amber: 38, cyan: 192, green: 150, violet: 268, gold: 44 })[color] ?? 38; }

  // effective due: manual override wins, else the parsed time from the text
  function effectiveDue() { return manualDue !== undefined ? manualDue : lastParse.due; }

  // --- live preview ---
  function renderPreview() {
    const raw = input.value.trim();
    preview.innerHTML = '';
    if (!raw) { setCategory(); return; }
    lastParse = window.JARVIS_NLP.parse(raw);
    const cat = C.byKey(activeCategory());

    const t1 = document.createElement('span');
    t1.className = `qa-tag c-${cat.color}`;
    t1.innerHTML = `<span class="dot"></span>${cat.label}`;
    t1.title = cat.meaning;
    preview.appendChild(t1);

    const due = effectiveDue();
    if (due) {
      const t2 = document.createElement('span');
      t2.className = 'qa-tag due';
      t2.textContent = '◷ ' + V.humanWhen(due);
      preview.appendChild(t2);
    }
    // if the typed text alone implies a time and the user hasn't overridden,
    // reflect that on the chip row by clearing manual highlight
    if (manualDue === undefined) markTime(lastParse.due ? '' : '');
    setCategory();
    reactor.setEnergy(Math.min(1, 0.4 + raw.length / 60));
  }

  // --- commit ---
  function commit() {
    const raw = input.value.trim();
    if (!raw) { input.focus(); return; }
    const parsed = window.JARVIS_NLP.parse(raw);
    const category = activeCategory();
    const c = C.byKey(category);
    const due = manualDue !== undefined ? manualDue : parsed.due;

    const task = {
      title: parsed.cleanTitle || raw,
      notes: '', category, color: c.color,
      due: due ? due.toISOString() : null
    };

    reactor.pulse(); reactor.pulse();
    window.jarvis.addTask(task).then((rec) => V.acknowledge(rec));

    stage.classList.remove('in'); stage.classList.add('out');
    setTimeout(() => { resetForm(); window.jarvis.closeQuickAdd(); }, 280);
  }

  function resetForm() {
    input.value = ''; manualCategory = null; manualDue = undefined;
    preview.innerHTML = ''; customTime.classList.remove('show'); customTime.value = '';
    markTime('');
  }

  // --- keyboard ---
  input.addEventListener('input', renderPreview);
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); commit(); }
    else if (e.key === 'Escape') { e.preventDefault(); dismiss(); }
    else if (e.key === 'Tab') {
      e.preventDefault();
      const idx = C.order.indexOf(activeCategory());
      setCategory(C.order[(idx + 1) % C.order.length], true);
      renderPreview();
    }
  });
  commitBtn.addEventListener('click', commit);

  function dismiss() {
    stage.classList.remove('in'); stage.classList.add('out');
    setTimeout(() => window.jarvis.closeQuickAdd(), 240);
  }

  // --- summon / dismiss ---
  function summon(settings) {
    if (settings) V.configure({ address: settings.address, voiceURI: settings.voiceURI, mute: settings.mute });
    stage.classList.remove('out'); stage.classList.add('in');
    resetForm();
    setCategory();
    reactor.pulse();
    setTimeout(() => input.focus(), 80);
    if (Math.random() < 0.6) {
      const cues = ['Yes?', 'Ready when you are.', "What's the directive?", "I'm listening.", 'Go ahead.', 'At your service.'];
      V.say(cues[Math.floor(Math.random() * cues.length)]);
    }
  }
  window.jarvis.onSummon(summon);
  window.jarvis.onDismiss(() => stage.classList.remove('in'));

  setCategory();
  markTime('');
})();
