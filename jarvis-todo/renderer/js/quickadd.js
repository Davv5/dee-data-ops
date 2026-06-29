// Quick-add HUD controller — create directives, set times, AND converse with
// JARVIS by typing or speaking (mic). Routes input through the command parser:
// thanks/greet -> spoken reply; reschedule/snooze/done -> act on the active
// directive; "make a tag ..." -> custom tag; otherwise create a directive.
(function () {
  const C = window.JARVIS_COLORS;
  const V = window.JARVIS_VOICE;
  const CMD = window.JARVIS_COMMANDS;
  const NLP = window.JARVIS_NLP;

  const stage = document.getElementById('stage');
  const input = document.getElementById('input');
  const preview = document.getElementById('preview');
  const catsEl = document.getElementById('cats');
  const timesEl = document.getElementById('times');
  const customTime = document.getElementById('qaCustomTime');
  const commitBtn = document.getElementById('commit');
  const micBtn = document.getElementById('micBtn');

  let manualCategory = null;
  let manualDue;                 // undefined=follow text, null=no time, Date=set
  let lastParse = { due: null, cleanTitle: '' };
  let settings = { address: 'Sir', customTags: {} };

  const pick = (a) => a[Math.floor(Math.random() * a.length)];
  const addr = () => settings.address || 'Sir';

  const reactor = new window.HoloField(document.getElementById('holo'), { hue: 194, motes: 30, cxBias: 0.5, cyBias: 0.5, scale: 1.05 });
  reactor.start();

  // ---- category pills (rebuilt when custom tags change) ----
  function buildCats() {
    catsEl.innerHTML = '';
    C.order.forEach((key) => {
      const c = C.byKey(key);
      const el = document.createElement('div');
      el.className = 'qa-cat';
      el.dataset.key = key;
      el.style.setProperty('--c', c.hex);
      el.innerHTML = `<span class="dot"></span>${c.label}`;
      el.title = c.meaning;
      el.addEventListener('click', () => { setCategory(key, true); input.focus(); });
      catsEl.appendChild(el);
    });
    setCategory();
  }

  // ---- time chips ----
  const TIME_PRESETS = [
    { id: 'none', label: 'No time', get: () => null },
    { id: 'h1', label: '+1 hr', get: () => offset(60) },
    { id: 'h3', label: '+3 hr', get: () => offset(180) },
    { id: 'eve', label: 'Tonight 8pm', get: () => at(0, 20) },
    { id: 'tmr', label: 'Tomorrow 9am', get: () => at(1, 9) },
    { id: 'custom', label: 'Pick…', custom: true }
  ];
  const offset = (m) => { const d = new Date(); d.setMinutes(d.getMinutes() + m); return d; };
  const at = (add, h) => { const d = new Date(); d.setDate(d.getDate() + add); d.setHours(h, 0, 0, 0); return d; };
  const toLocalInput = (d) => { const z = new Date(d.getTime() - d.getTimezoneOffset() * 60000); return z.toISOString().slice(0, 16); };

  TIME_PRESETS.forEach((p) => {
    const el = document.createElement('div');
    el.className = 'qa-time' + (p.custom ? ' custom' : '');
    el.dataset.id = p.id;
    el.textContent = p.label;
    el.addEventListener('click', () => {
      if (p.custom) {
        customTime.classList.add('show');
        if (!customTime.value) customTime.value = toLocalInput(lastParse.due || offset(60));
        customTime.focus(); manualDue = new Date(customTime.value);
      } else { customTime.classList.remove('show'); manualDue = p.get(); }
      markTime(p.id); renderPreview(); input.focus();
    });
    timesEl.appendChild(el);
  });
  customTime.addEventListener('input', () => { if (customTime.value) { manualDue = new Date(customTime.value); markTime('custom'); renderPreview(); } });
  const markTime = (id) => [...timesEl.children].forEach((el) => el.classList.toggle('active', el.dataset.id === id));

  // ---- category state ----
  const activeCategory = () => manualCategory || C.infer(input.value) || 'standard';
  function setCategory(key, manual) {
    if (manual) manualCategory = key;
    const active = activeCategory();
    [...catsEl.children].forEach((el) => el.classList.toggle('active', el.dataset.key === active));
  }
  const effectiveDue = () => (manualDue !== undefined ? manualDue : lastParse.due);

  // ---- live preview ----
  function renderPreview() {
    const raw = input.value.trim();
    preview.innerHTML = '';
    if (!raw) { setCategory(); return; }
    lastParse = NLP.parse(raw);
    const cat = C.byKey(activeCategory());
    const t1 = document.createElement('span');
    t1.className = 'qa-tag'; t1.style.setProperty('--c', cat.hex);
    t1.innerHTML = `<span class="dot"></span>${cat.label}`;
    preview.appendChild(t1);
    const due = effectiveDue();
    if (due) {
      const t2 = document.createElement('span'); t2.className = 'qa-tag due';
      t2.textContent = '◷ ' + V.humanWhen(due); preview.appendChild(t2);
    }
    setCategory();
    reactor.setEnergy(Math.min(1, 0.4 + raw.length / 60));
  }

  // ---- create a directive ----
  function createTask(titleText) {
    const parsed = NLP.parse(titleText);
    const category = manualCategory || C.infer(titleText) || 'standard';
    const c = C.byKey(category);
    const due = manualDue !== undefined ? manualDue : parsed.due;
    reactor.pulse(); reactor.pulse();
    window.jarvis.addTask({
      title: parsed.cleanTitle || titleText, notes: '', category, color: c.color,
      due: due ? due.toISOString() : null
    }).then((rec) => V.acknowledge(rec));
  }

  // ---- pick the directive a "reschedule/done" refers to ----
  function pickTarget(tasks, lastId) {
    const active = tasks.filter((t) => !t.done);
    if (lastId) { const x = active.find((t) => t.id === lastId); if (x) return x; }
    const withDue = active.filter((t) => t.due).sort((a, b) => new Date(a.due) - new Date(b.due));
    const overdue = withDue.filter((t) => new Date(t.due) <= new Date());
    if (overdue.length) return overdue[overdue.length - 1];   // most-recent overdue
    if (withDue.length) return withDue[0];                     // soonest upcoming
    return active[active.length - 1] || null;
  }

  // ---- handle a non-create command ----
  async function handleCommand(cmd) {
    const a = addr();
    if (['thanks', 'greet', 'status', 'dismiss', 'wake', 'huh'].includes(cmd.type)) {
      V.say(CMD.line(cmd.type, a)); return exit();
    }
    if (cmd.type === 'tag') {
      const made = C.makeTag(cmd.name, cmd.color, '');
      const tags = Object.assign({}, settings.customTags || {}, { [made.key]: made.tag });
      await window.jarvis.saveSettings({ customTags: tags });
      settings.customTags = tags; C.configure(tags); buildCats();
      V.say(`Done, ${a}. New tag, "${made.tag.label}", in ${made.tag.color}.`);
      return exit();
    }
    // reschedule / complete operate on the active directive
    const [tasks, s] = await Promise.all([window.jarvis.getTasks(), window.jarvis.getSettings()]);
    const target = pickTarget(tasks, s.lastAlertedId);
    if (!target) { V.say(`I don't see a directive to ${cmd.type === 'complete' ? 'clear' : 'reschedule'}, ${a}.`); return exit(); }

    if (cmd.type === 'complete') {
      await window.jarvis.updateTask(target.id, { done: true });
      V.reactComplete(target); return exit();
    }
    // reschedule
    let newDue = cmd.snoozeMins ? new Date(Date.now() + cmd.snoozeMins * 60000) : cmd.due;
    if (!newDue) { V.say(`To when, ${a}? Try “reschedule to 5pm”.`); return exit(); }
    await window.jarvis.updateTask(target.id, { due: newDue.toISOString(), announcedDue: false, announcedSoon: false, lastNudge: undefined });
    V.say(`Rescheduled, ${a}. "${target.title}" — now ${V.humanWhen(newDue)}.`);
    return exit();
  }

  // ---- commit (typed Enter, button, or mic result) ----
  function commit() {
    const raw = input.value.trim();
    if (!raw) { input.focus(); return; }
    const cmd = CMD.classify(raw);
    if (cmd.type === 'create' || cmd.type === 'maybeCreate') {
      createTask(cmd.title || raw); return exit();
    }
    handleCommand(cmd);
  }

  function exit() {
    stage.classList.remove('in'); stage.classList.add('out');
    setTimeout(() => { resetForm(); window.jarvis.closeQuickAdd(); }, 280);
  }
  function resetForm() {
    input.value = ''; manualCategory = null; manualDue = undefined;
    preview.innerHTML = ''; customTime.classList.remove('show'); customTime.value = ''; markTime('');
  }

  // ---- microphone: record + transcribe via Whisper (reliable in Electron) ----
  let recorder = null, chunks = [], recording = false;
  function setStatus(txt) { const s = document.querySelector('.qa-status'); if (s) s.innerHTML = `<span class="blink">●</span>&nbsp; ${txt}`; }

  function micClick() {
    if (recording) { stopRecording(); return; }
    if (settings.sttKey) { startRecording(); return; }
    tryWebSpeech();   // no key configured — attempt the (often unsupported) built-in
  }

  async function startRecording() {
    let stream;
    try { stream = await navigator.mediaDevices.getUserMedia({ audio: true }); }
    catch (_) { V.say(`I need microphone access, ${addr()} — allow it in System Settings, Privacy, Microphone.`); return; }
    try { recorder = new MediaRecorder(stream); } catch (_) { stream.getTracks().forEach((t) => t.stop()); V.say(`I can't record here, ${addr()}.`); return; }
    chunks = [];
    recorder.ondataavailable = (e) => { if (e.data && e.data.size) chunks.push(e.data); };
    recorder.onstop = async () => {
      stream.getTracks().forEach((t) => t.stop());
      micBtn.classList.remove('on'); recording = false;
      setStatus('JARVIS · TRANSCRIBING');
      let text = null;
      try { text = await transcribe(new Blob(chunks, { type: 'audio/webm' })); } catch (e) { console.warn('[stt]', e); }
      setStatus('JARVIS · LISTENING');
      if (text) { input.value = text; renderPreview(); setTimeout(commit, 200); }
      else V.say(`I couldn't make that out, ${addr()}.`);
    };
    recorder.start(); recording = true; micBtn.classList.add('on'); setStatus('JARVIS · RECORDING…');
  }
  function stopRecording() { if (recorder && recorder.state !== 'inactive') recorder.stop(); }

  async function transcribe(blob) {
    const fd = new FormData();
    fd.append('file', blob, 'audio.webm');
    fd.append('model', settings.sttModel || 'whisper-1');
    const res = await fetch('https://api.openai.com/v1/audio/transcriptions', {
      method: 'POST', headers: { Authorization: 'Bearer ' + settings.sttKey }, body: fd
    });
    if (!res.ok) throw new Error('stt ' + res.status);
    const j = await res.json();
    return (j.text || '').trim();
  }

  function tryWebSpeech() {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    let rec;
    try { rec = SR && new SR(); } catch (_) { rec = null; }
    if (!rec) { V.say(`Voice input needs a quick setup, ${addr()}. Add a speech key in Settings, or just type to me.`); return; }
    rec.lang = 'en-US'; rec.interimResults = false;
    micBtn.classList.add('on'); setStatus('JARVIS · RECORDING…');
    rec.onresult = (e) => { input.value = e.results[0][0].transcript; renderPreview(); setTimeout(commit, 250); };
    rec.onerror = (e) => {
      micBtn.classList.remove('on'); setStatus('JARVIS · LISTENING');
      if (e.error !== 'aborted') V.say(`Voice input isn't supported in this build, ${addr()}. Add an OpenAI key in Settings for voice — or simply type to me.`);
    };
    rec.onend = () => { micBtn.classList.remove('on'); setStatus('JARVIS · LISTENING'); };
    try { rec.start(); } catch (_) {}
  }
  if (micBtn) micBtn.addEventListener('click', micClick);

  // ---- keyboard ----
  input.addEventListener('input', renderPreview);
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') { e.preventDefault(); commit(); }
    else if (e.key === 'Escape') { e.preventDefault(); exit(); }
    else if (e.key === 'Tab') { e.preventDefault(); const i = C.order.indexOf(activeCategory()); setCategory(C.order[(i + 1) % C.order.length], true); renderPreview(); }
  });
  commitBtn.addEventListener('click', commit);

  // ---- summon / dismiss ----
  function summon(s) {
    if (s) { settings = Object.assign(settings, s); V.configure({ address: s.address, voiceURI: s.voiceURI, mute: s.mute }); C.configure(s.customTags || {}); }
    buildCats();
    stage.classList.remove('out'); stage.classList.add('in');
    resetForm();
    reactor.pulse();
    setTimeout(() => input.focus(), 80);
    if (Math.random() < 0.55) V.say(pick(['Yes?', 'Ready when you are.', "What's the directive?", "I'm listening.", 'Go ahead.', 'At your service.']));
  }
  window.jarvis.onSummon(summon);
  window.jarvis.onDismiss(() => stage.classList.remove('in'));

  buildCats();
})();
