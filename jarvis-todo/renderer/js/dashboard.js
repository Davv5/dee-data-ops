// JARVIS dashboard controller — rendering, stats, settings, and the deadline
// scheduler that fires the spoken "it's time" alerts. This window stays alive
// in the background (hidden to the tray), so the scheduler keeps running even
// when the dashboard isn't visible.
(function () {
  const C = window.JARVIS_COLORS;
  const V = window.JARVIS_VOICE;

  let tasks = [];
  let settings = {};
  let filter = 'all';

  const $ = (id) => document.getElementById(id);

  // ---- holographic background field ----
  const holo = new window.HoloField($('holoField'), { hue: 198, gold: 28, red: 6, suit: 46, motes: 42, cxBias: 0.5, cyBias: 0.5, scale: 0.92 });
  holo.start();

  // ---- clock ----
  function tickClock() {
    const d = new Date();
    $('clockTime').textContent = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    $('clockDate').textContent = d.toLocaleDateString([], { weekday: 'long', day: 'numeric', month: 'long' });
  }
  setInterval(tickClock, 1000); tickClock();

  // ---- legend ----
  function renderLegend() {
    $('legend').innerHTML = C.order.map((k) => {
      const c = C.byKey(k);
      return `<div class="row" style="--c:${c.hex}"><span class="dot chip-c"></span><span><b>${c.label}</b> — ${c.meaning}</span></div>`;
    }).join('');
  }
  renderLegend();

  // ---- task helpers ----
  function dueState(t) {
    if (!t.due) return 'none';
    const diff = new Date(t.due) - new Date();
    if (diff < 0) return 'past';
    if (diff < 60 * 60 * 1000) return 'soon';
    return 'future';
  }
  function isToday(t) {
    if (!t.due) return false;
    return new Date(t.due).toDateString() === new Date().toDateString();
  }

  function visible() {
    const active = tasks.filter((t) => !t.done);
    if (filter === 'all') return tasks.filter((t) => !t.done);
    if (filter === 'today') return active.filter(isToday);
    if (filter === 'overdue') return active.filter((t) => dueState(t) === 'past');
    if (filter === 'done') return tasks.filter((t) => t.done);
    return active;
  }

  // ---- render ----
  function render() {
    const list = $('tasks');
    const items = visible().sort(sortTasks);
    if (!items.length) {
      list.innerHTML = `<div class="empty"><div class="big">CLEAR SKIES</div>No directives here. Press <b>＋ Directive</b> or hit your hotkey.</div>`;
    } else {
      list.innerHTML = items.map(taskHTML).join('');
      wireTasks();
    }
    renderStats();
  }

  function sortTasks(a, b) {
    const order = { critical: 0, deadline: 1, work: 2, personal: 3, idea: 4, standard: 5 };
    const ad = a.due ? new Date(a.due).getTime() : Infinity;
    const bd = b.due ? new Date(b.due).getTime() : Infinity;
    if (ad !== bd) return ad - bd;
    return (order[a.category] ?? 9) - (order[b.category] ?? 9);
  }

  function taskHTML(t) {
    const c = C.byKey(t.category);
    const ds = dueState(t);
    const overdue = ds === 'past' && !t.done;
    let dueLabel = '';
    if (t.due) {
      const cls = ds === 'past' ? 'past' : ds === 'soon' ? 'soon' : '';
      dueLabel = `<span class="t-due ${cls}">◷ ${V.humanWhen(new Date(t.due))}</span>`;
    }
    return `
      <div class="task c-${c.color} ${overdue ? 'overdue' : ''} ${t.done ? 'done' : ''}" data-id="${t.id}" style="--c:${c.hex}">
        <div class="t-check" data-act="toggle"></div>
        <div class="t-body">
          <div class="t-title">${esc(t.title)}</div>
          <div class="t-meta">
            <span class="t-cat"><span class="dot chip-c"></span>${c.label}</span>
            ${dueLabel}
          </div>
        </div>
        <div class="t-actions">
          <button class="t-act speak" data-act="speak" title="Have JARVIS read it">▶</button>
          <button class="t-act" data-act="del" title="Delete">✕</button>
        </div>
      </div>`;
  }

  function wireTasks() {
    document.querySelectorAll('.task').forEach((el) => {
      const id = el.dataset.id;
      el.addEventListener('click', () => openEditor(id));
      el.querySelectorAll('[data-act]').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const act = btn.dataset.act;
          const t = tasks.find((x) => x.id === id);
          if (!t) return;
          if (act === 'toggle') {
            const done = !t.done;
            window.jarvis.updateTask(id, { done }).then(() => { if (done) V.reactComplete(t); });
          } else if (act === 'del') {
            window.jarvis.removeTask(id);
          } else if (act === 'speak') {
            holo.pulse();
            V.alert(t, dueState(t) === 'past' ? 'overdue' : 'due');
          }
        });
      });
    });
  }

  function renderStats() {
    const active = tasks.filter((t) => !t.done);
    $('sActive').textContent = active.length;
    $('sToday').textContent = active.filter(isToday).length;
    $('sOverdue').textContent = active.filter((t) => dueState(t) === 'past').length;
    $('sDone').textContent = tasks.filter((t) => t.done).length;
  }

  function esc(s) { return (s || '').replace(/[&<>"]/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[m])); }

  // ---- filters ----
  $('filters').addEventListener('click', (e) => {
    const f = e.target.closest('.filter');
    if (!f) return;
    filter = f.dataset.f;
    document.querySelectorAll('.filter').forEach((x) => x.classList.toggle('active', x === f));
    render();
  });

  // ---- scheduler: the spoken deadline engine ----
  function scheduleSweep() {
    const now = Date.now();
    tasks.forEach((t) => {
      if (t.done || !t.due) return;
      const due = new Date(t.due).getTime();
      const diff = due - now;

      // pre-warning ~10 min out
      if (diff > 0 && diff <= 10 * 60 * 1000 && !t.announcedSoon) {
        window.jarvis.updateTask(t.id, { announcedSoon: true });
        holo.pulse();
        V.alert(t, 'soon');
        notify(t, 'soon');
        return;
      }
      // the moment it's due
      if (diff <= 0 && !t.announcedDue) {
        window.jarvis.updateTask(t.id, { announcedDue: true, lastNudge: now });
        window.jarvis.saveSettings({ lastAlertedId: t.id }); // for "reschedule that"
        holo.setEnergy(1); holo.pulse();
        V.alert(t, 'due');
        notify(t, 'due');
        return;
      }
      // periodic overdue nudge (every 30 min) for things left open
      if (diff <= 0 && t.announcedDue) {
        const last = t.lastNudge || due;
        if (now - last >= 30 * 60 * 1000) {
          window.jarvis.updateTask(t.id, { lastNudge: now });
          V.alert(t, 'overdue');
          notify(t, 'overdue');
        }
      }
    });
  }

  function notify(t, kind) {
    // Jarvis HUD card (animated, actionable: Done / +10m / +1h / Edit)
    try {
      window.jarvis.showAlert({
        id: t.id, title: t.title, category: t.category, kind,
        dueLabel: t.due ? V.humanWhen(new Date(t.due)) : '',
        customTags: settings.customTags || {}
      });
    } catch (_) {}
    // silent native notification too, so it lands in Notification Center history
    if (!('Notification' in window)) return;
    const title = kind === 'soon' ? 'JARVIS · Coming up' : kind === 'overdue' ? 'JARVIS · Overdue' : 'JARVIS · It’s time';
    const body = V.cue(t, kind);   // intent-aware: "Time to call the CEO"
    try {
      if (Notification.permission === 'granted') new Notification(title, { body, silent: true });
      else if (Notification.permission !== 'denied') Notification.requestPermission();
    } catch (_) {}
  }

  setInterval(scheduleSweep, 15000);

  // ---- task editor ----
  const editBack = $('editBack');
  let editingId = null;

  const ED_TIMES = [
    { id: 'none', label: 'No time', get: () => null },
    { id: 'h1', label: '+1 hr', get: () => offs(60) },
    { id: 'h3', label: '+3 hr', get: () => offs(180) },
    { id: 'eve', label: 'Tonight 8pm', get: () => atd(0, 20) },
    { id: 'tmr', label: 'Tomorrow 9am', get: () => atd(1, 9) }
  ];
  function offs(m) { const d = new Date(); d.setMinutes(d.getMinutes() + m); return d; }
  function atd(add, h) { const d = new Date(); d.setDate(d.getDate() + add); d.setHours(h, 0, 0, 0); return d; }
  function toLocalInput(d) { const z = new Date(d.getTime() - d.getTimezoneOffset() * 60000); return z.toISOString().slice(0, 16); }

  // editor category chips (rebuilt when custom tags change)
  function buildEditorCats() {
    const host = $('edCats');
    host.innerHTML = '';
    C.order.forEach((key) => {
      const c = C.byKey(key);
      const el = document.createElement('div');
      el.className = 'pchip';
      el.dataset.key = key;
      el.style.setProperty('--c', c.hex);
      el.innerHTML = `<span class="dot"></span>${c.label}`;
      el.addEventListener('click', () => {
        [...host.children].forEach((x) => x.classList.toggle('active', x === el));
      });
      host.appendChild(el);
    });
  }
  buildEditorCats();
  ED_TIMES.forEach((p) => {
    const el = document.createElement('div');
    el.className = 'pchip';
    el.textContent = p.label;
    el.addEventListener('click', () => {
      const d = p.get();
      $('edDue').value = d ? toLocalInput(d) : '';
    });
    $('edTimes').appendChild(el);
  });

  function openEditor(id) {
    const t = tasks.find((x) => x.id === id);
    if (!t) return;
    editingId = id;
    $('edTitle').value = t.title;
    [...$('edCats').children].forEach((x) => x.classList.toggle('active', x.dataset.key === t.category));
    $('edDue').value = t.due ? toLocalInput(new Date(t.due)) : '';
    editBack.classList.add('show');
    setTimeout(() => $('edTitle').focus(), 60);
  }
  function closeEditor() { editBack.classList.remove('show'); editingId = null; }

  // alert card's "Time / Priority" button routes here
  window.jarvis.onEditorOpen((id) => openEditor(id));

  $('edCancel').addEventListener('click', closeEditor);
  editBack.addEventListener('click', (e) => { if (e.target === editBack) closeEditor(); });
  $('edDelete').addEventListener('click', () => { if (editingId) window.jarvis.removeTask(editingId); closeEditor(); });
  $('edSave').addEventListener('click', () => {
    if (!editingId) return;
    const t = tasks.find((x) => x.id === editingId);
    const activeCat = [...$('edCats').children].find((x) => x.classList.contains('active'));
    const category = activeCat ? activeCat.dataset.key : (t ? t.category : 'standard');
    const due = $('edDue').value ? new Date($('edDue').value).toISOString() : null;
    // changing the due time resets the spoken-alert flags so JARVIS will speak again
    const patch = { title: $('edTitle').value.trim() || 'Untitled directive', category, color: C.byKey(category).color, due };
    if (!t || t.due !== due) { patch.announcedDue = false; patch.announcedSoon = false; patch.lastNudge = undefined; }
    window.jarvis.updateTask(editingId, patch);
    closeEditor();
  });

  // ---- settings modal ----
  const back = $('settingsBack');
  function openSettings() {
    $('setAddress').value = settings.address || 'Sir';
    $('setHotkey').value = settings.hotkey || 'CommandOrControl+Shift+Space';
    if ($('setSttKey')) $('setSttKey').value = settings.sttKey || '';
    fillVoices();
    renderTagManager();
    back.classList.add('show');
  }
  function fillVoices() {
    const sel = $('setVoice');
    const voices = V.listVoices();
    sel.innerHTML = '<option value="">Auto (best British match)</option>' +
      voices.map((v) => `<option value="${v.voiceURI}">${v.name} — ${v.lang}</option>`).join('');
    sel.value = settings.voiceURI || '';
  }
  $('settingsBtn').addEventListener('click', openSettings);
  back.addEventListener('click', (e) => { if (e.target === back) back.classList.remove('show'); });
  $('setTest').addEventListener('click', () => {
    V.configure({ address: $('setAddress').value || 'Sir', voiceURI: $('setVoice').value || null, mute: false });
    V.say(`${$('setAddress').value || 'Sir'}, systems nominal. Voice calibration complete.`, { force: true });
  });
  $('setSave').addEventListener('click', () => {
    const patch = {
      address: $('setAddress').value || 'Sir',
      voiceURI: $('setVoice').value || null,
      hotkey: $('setHotkey').value || 'CommandOrControl+Shift+Space',
      sttKey: ($('setSttKey') && $('setSttKey').value.trim()) || ''
    };
    window.jarvis.saveSettings(patch);
    settings = Object.assign(settings, patch);
    V.configure(patch);
    back.classList.remove('show');
    V.say('Configuration saved.', { force: true });
  });

  // ---- mute toggle ----
  let muted = false;
  $('muteBtn').addEventListener('click', () => {
    muted = !muted;
    V.configure({ mute: muted });
    window.jarvis.saveSettings({ mute: muted });
    $('muteBtn').textContent = muted ? '🔇 Muted' : '🔊 Voice';
    $('muteBtn').classList.toggle('on', !muted);
  });

  // ---- tag manager (Settings) ----
  function renderTagManager() {
    const sel = $('tagColor');
    if (sel && !sel.dataset.filled) {
      sel.innerHTML = C.palette.map((c) => `<option value="${c}">${c}</option>`).join('');
      sel.dataset.filled = '1';
    }
    const list = $('tagList'); if (!list) return;
    const custom = settings.customTags || {};
    const keys = Object.keys(custom);
    list.innerHTML = keys.length
      ? keys.map((k) => { const c = C.byKey(k); return `<span class="tag-item" style="--c:${c.hex}"><span class="dot"></span>${c.label}<button data-k="${k}" class="tag-rm" title="Remove">✕</button></span>`; }).join('')
      : '<span class="dim" style="font-size:12px">No custom tags yet — add one below or by voice.</span>';
    list.querySelectorAll('.tag-rm').forEach((b) => b.addEventListener('click', () => {
      const tags = Object.assign({}, settings.customTags || {}); delete tags[b.dataset.k];
      settings.customTags = tags; window.jarvis.saveSettings({ customTags: tags });
      C.configure(tags); renderTagManager(); renderLegend(); buildEditorCats(); render();
    }));
  }
  if ($('tagAdd')) $('tagAdd').addEventListener('click', () => {
    const name = $('tagName').value.trim(); if (!name) return;
    const made = C.makeTag(name, $('tagColor').value || 'cyan', '');
    const tags = Object.assign({}, settings.customTags || {}, { [made.key]: made.tag });
    settings.customTags = tags; window.jarvis.saveSettings({ customTags: tags });
    C.configure(tags); $('tagName').value = '';
    renderTagManager(); renderLegend(); buildEditorCats(); render();
    V.say(`Added "${made.tag.label}", ${settings.address || 'Sir'}.`);
  });

  // ---- buttons ----
  $('newBtn').addEventListener('click', () => V.say(pick(['Use your hotkey to summon me — Command-Shift-Space.', 'Press Command-Shift-Space anywhere to give me a directive.'])));
  function pick(a) { return a[Math.floor(Math.random() * a.length)]; }

  // ---- live data ----
  function refresh(t) { tasks = t || []; render(); }
  function applyTags(s) { C.configure((s && s.customTags) || {}); renderLegend(); buildEditorCats(); renderTagManager(); render(); }
  window.jarvis.onTasksChanged(refresh);
  window.jarvis.onSettingsChanged((s) => { settings = s; V.configure(s); applyTags(s); });

  // ---- boot ----
  Promise.all([window.jarvis.getTasks(), window.jarvis.getSettings()]).then(([t, s]) => {
    tasks = t || []; settings = s || {};
    muted = !!settings.mute;
    $('muteBtn').textContent = muted ? '🔇 Muted' : '🔊 Voice';
    $('muteBtn').classList.toggle('on', !muted);
    V.configure(settings);
    applyTags(settings);
    render();
    if ('Notification' in window && Notification.permission === 'default') Notification.requestPermission();

    // greet once voices are ready
    const stats = {
      total: tasks.filter((x) => !x.done).length,
      overdue: tasks.filter((x) => !x.done && dueState(x) === 'past').length,
      dueToday: tasks.filter((x) => !x.done && isToday(x)).length
    };
    setTimeout(() => V.greeting(stats), 900);
    // One-time nudge: if only a low-quality (robotic) voice is installed, point
    // the way to a far better one. Voices finish loading a few seconds in.
    if (!settings.voiceNudged) {
      setTimeout(() => {
        if (!V.hasGoodVoice()) {
          V.say(`${settings.address || 'Sir'}, I can sound considerably more natural. Add an Enhanced voice in System Settings, Accessibility, Spoken Content — I'd suggest Daniel, English UK — then select it in my settings.`, { force: true });
          window.jarvis.saveSettings({ voiceNudged: true });
        }
      }, 6500);
    }
    scheduleSweep();
  });
})();
