// Alert HUD controller — receives a reminder payload, shows the Jarvis card,
// and acts on the directive: done / snooze / open the editor for time+priority.
(function () {
  const C = window.JARVIS_COLORS;
  const $ = (id) => document.getElementById(id);
  const card = $('card');
  let cur = null;
  let hideTimer = null;

  const KINDS = { soon: 'COMING UP', due: "IT'S TIME", overdue: 'OVERDUE' };

  window.jarvis.onAlertData((p) => {
    cur = p;
    C.configure(p.customTags || {});
    const c = C.byKey(p.category);
    document.documentElement.style.setProperty('--ac', p.kind === 'overdue' ? '#ff4d5e' : c.hex);
    $('aKind').textContent = KINDS[p.kind] || "IT'S TIME";
    $('aTitle').textContent = p.title;
    $('aMeta').textContent = c.label + (p.dueLabel ? '  ·  ' + p.dueLabel : '') + (p.repeat ? '  ·  ⟳ ' + p.repeat.freq : '');
    if (window.JARVIS_SFX) { window.JARVIS_SFX.setMuted(!!p.mute); window.JARVIS_SFX.alert(); }
    // restart the slide-in + countdown
    card.classList.remove('in');
    void card.offsetWidth;
    card.classList.add('in');
    clearTimeout(hideTimer);
    hideTimer = setTimeout(dismiss, 22000);
  });

  function dismiss() {
    clearTimeout(hideTimer);
    card.classList.remove('in');
    setTimeout(() => window.jarvis.closeAlert(), 300);
  }

  function snooze(mins) {
    if (!cur) return dismiss();
    const d = new Date(Date.now() + mins * 60000);
    window.jarvis.updateTask(cur.id, {
      due: d.toISOString(), announcedDue: false, announcedSoon: true, lastNudge: undefined
    });
    dismiss();
  }

  $('bDone').addEventListener('click', () => {
    if (cur) {
      if (cur.repeat && cur.due) {
        // repeating directive: completing it schedules the next occurrence
        window.jarvis.updateTask(cur.id, {
          due: window.JARVIS_NLP.nextOccurrence(cur.due, cur.repeat),
          announcedDue: false, announcedSoon: false, lastNudge: undefined
        });
      } else {
        window.jarvis.updateTask(cur.id, { done: true });
      }
      if (window.JARVIS_SFX) window.JARVIS_SFX.done();
    }
    dismiss();
  });
  $('b10').addEventListener('click', () => snooze(10));
  $('b60').addEventListener('click', () => snooze(60));
  $('bEdit').addEventListener('click', () => { if (cur) window.jarvis.editFromAlert(cur.id); });
  $('bX').addEventListener('click', dismiss);
})();
