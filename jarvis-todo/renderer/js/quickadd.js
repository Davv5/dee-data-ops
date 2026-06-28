// Quick-add HUD controller — summon animation, live NL parsing preview,
// colour-class selection, and commit-with-spoken-acknowledgement.
(function () {
  const C = window.JARVIS_COLORS;
  const V = window.JARVIS_VOICE;
  const stage = document.getElementById('stage');
  const input = document.getElementById('input');
  const preview = document.getElementById('preview');
  const catsEl = document.getElementById('cats');
  const commitBtn = document.getElementById('commit');
  const eq = document.getElementById('eq');

  let manualCategory = null;     // user override via pills/Tab
  let lastParse = { due: null, cleanTitle: '' };
  let greeted = false;

  // --- reactor ---
  const reactor = new window.Reactor(document.getElementById('reactor'), { idle: 0.35, hue: 38, sparks: 60 });
  reactor.start();

  // --- build category pills ---
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

  function activeCategory() {
    return manualCategory || C.infer(input.value) || 'standard';
  }

  function setCategory(key, manual) {
    if (manual) manualCategory = key;
    const active = activeCategory();
    [...catsEl.children].forEach((el) => el.classList.toggle('active', el.dataset.key === active));
    const c = C.byKey(active);
    reactor.setHue(hueFor(c.color));
    reactor.pulse();
  }

  function hueFor(color) {
    return ({ red: 354, amber: 38, cyan: 192, green: 150, violet: 268, gold: 44 })[color] ?? 38;
  }

  // --- live parse preview ---
  function renderPreview() {
    const raw = input.value.trim();
    preview.innerHTML = '';
    if (!raw) { setCategory(); return; }

    lastParse = window.JARVIS_NLP.parse(raw);
    const cat = C.byKey(activeCategory());

    // colour/class tag
    const t1 = document.createElement('span');
    t1.className = `qa-tag c-${cat.color}`;
    t1.innerHTML = `<span class="dot"></span>${cat.label}`;
    t1.title = cat.meaning;
    preview.appendChild(t1);

    // deadline tag
    if (lastParse.due) {
      const t2 = document.createElement('span');
      t2.className = 'qa-tag due';
      t2.textContent = '◷ ' + V.humanWhen(lastParse.due);
      preview.appendChild(t2);
    }
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

    const task = {
      title: parsed.cleanTitle || raw,
      notes: '',
      category,
      color: c.color,
      due: parsed.due ? parsed.due.toISOString() : null
    };

    reactor.pulse(); reactor.pulse();
    window.jarvis.addTask(task).then((rec) => {
      V.acknowledge(rec);
    });

    // exit animation, then reset + close
    stage.classList.remove('in'); stage.classList.add('out');
    setTimeout(() => {
      input.value = ''; manualCategory = null; preview.innerHTML = '';
      window.jarvis.closeQuickAdd();
    }, 260);
  }

  // --- keyboard ---
  input.addEventListener('input', renderPreview);
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); commit(); }
    else if (e.key === 'Escape') { e.preventDefault(); dismiss(); }
    else if (e.key === 'Tab') {
      e.preventDefault();
      const cur = activeCategory();
      const idx = C.order.indexOf(cur);
      setCategory(C.order[(idx + 1) % C.order.length], true);
      renderPreview();
    }
  });
  commitBtn.addEventListener('click', commit);

  function dismiss() {
    stage.classList.remove('in'); stage.classList.add('out');
    setTimeout(() => window.jarvis.closeQuickAdd(), 220);
  }

  // --- summon / dismiss from main ---
  function summon(settings) {
    if (settings) V.configure({ address: settings.address, voiceURI: settings.voiceURI, mute: settings.mute });
    stage.classList.remove('out'); stage.classList.add('in');
    input.value = ''; manualCategory = null; preview.innerHTML = '';
    setCategory();
    reactor.pulse();
    setTimeout(() => input.focus(), 60);

    // A short spoken cue on summon — varied, only occasionally to avoid nagging.
    if (Math.random() < 0.6) {
      const cues = ['Yes?', 'Ready when you are.', "What's the directive?", 'I\'m listening.', 'Go ahead.', 'At your service.'];
      V.say(cues[Math.floor(Math.random() * cues.length)]);
    }
  }

  window.jarvis.onSummon(summon);
  window.jarvis.onDismiss(() => { stage.classList.remove('in'); });

  // first load (window is created hidden; summon drives the rest)
  setCategory();
})();
