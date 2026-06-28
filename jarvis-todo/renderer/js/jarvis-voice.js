// JARVIS voice — contextual speech composition.
//
// The brief: it must *speak*, and it must not feel templated. We get there by
// composing each line at runtime from independent fragment banks, chosen by
// context (time of day, task category, how close/overdue the deadline is,
// workload), then de-duplicated against recent history so phrasing keeps
// shifting. Every utterance is assembled fresh — there is no fixed sentence.
window.JARVIS_VOICE = (function () {
  const C = window.JARVIS_COLORS;

  // ---- voice selection -----------------------------------------------------
  let chosenVoice = null;
  let settings = { address: 'Sir', voiceURI: null, rate: 0.98, pitch: 0.9, mute: false };
  const recent = [];                       // anti-repeat ring buffer

  function pickVoice() {
    const voices = window.speechSynthesis.getVoices();
    if (!voices.length) return;
    if (settings.voiceURI) {
      chosenVoice = voices.find((v) => v.voiceURI === settings.voiceURI) || chosenVoice;
      if (chosenVoice) return;
    }
    // Prefer a calm British male — the JARVIS register.
    const prefs = ['daniel', 'arthur', 'oliver', 'en-gb', 'google uk english male', 'jamie'];
    for (const p of prefs) {
      const v = voices.find((x) => (x.name + x.lang + (x.voiceURI || '')).toLowerCase().includes(p));
      if (v) { chosenVoice = v; return; }
    }
    chosenVoice = voices.find((v) => /en[-_]?gb/i.test(v.lang)) || voices[0];
  }
  if ('speechSynthesis' in window) {
    pickVoice();
    window.speechSynthesis.onvoiceschanged = pickVoice;
  }

  function configure(patch) {
    settings = { ...settings, ...patch };
    pickVoice();
  }
  function listVoices() {
    return ('speechSynthesis' in window) ? window.speechSynthesis.getVoices() : [];
  }

  // ---- low-level speak -----------------------------------------------------
  function say(text, opts = {}) {
    if (!text) return;
    if (settings.mute && !opts.force) return;
    if (!('speechSynthesis' in window)) return;
    try {
      window.speechSynthesis.cancel();
      const u = new SpeechSynthesisUtterance(text);
      if (chosenVoice) u.voice = chosenVoice;
      u.rate = opts.rate || settings.rate;
      u.pitch = opts.pitch || settings.pitch;
      u.volume = 1;
      window.speechSynthesis.speak(u);
    } catch (e) { console.warn('[voice]', e); }
    if (opts.onText) opts.onText(text);
    return text;
  }

  // ---- composition helpers -------------------------------------------------
  function pick(arr) {
    // avoid the last few picks for this exact array
    for (let i = 0; i < 6; i++) {
      const cand = arr[Math.floor(Math.random() * arr.length)];
      if (!recent.includes(cand)) {
        recent.push(cand);
        if (recent.length > 14) recent.shift();
        return cand;
      }
    }
    return arr[Math.floor(Math.random() * arr.length)];
  }
  function maybe(p, frag) { return Math.random() < p ? frag : ''; }
  function join(...parts) {
    return parts.filter(Boolean).join(' ').replace(/\s+/g, ' ')
      .replace(/\s+([,.!?])/g, '$1').replace(/,\s*\./g, '.').trim();
  }
  function addr() { return settings.address || 'Sir'; }

  function partOfDay(d = new Date()) {
    const h = d.getHours();
    if (h < 5) return 'lateNight';
    if (h < 12) return 'morning';
    if (h < 17) return 'afternoon';
    if (h < 22) return 'evening';
    return 'lateNight';
  }

  // ---- fragment banks ------------------------------------------------------
  const ADDRESS = [() => addr() + ',', () => '', () => '', () => 'Very good,', () => addr() + '.'];

  const ACK = [
    'logged.', 'noted.', 'consider it tracked.', 'on the board.',
    'committed to memory.', 'I have it.', 'filed.', 'queued.',
    'I will keep an eye on it.', 'understood.'
  ];

  const ACK_LEAD = [
    'I have', "I've recorded", 'Adding', 'Capturing', 'Tracking', "I'll watch"
  ];

  const GREET = {
    morning: ['Good morning,', 'Systems online. Morning,', 'Online and ready. Good morning,'],
    afternoon: ['Good afternoon,', 'Back online. Afternoon,', 'At your service this afternoon,'],
    evening: ['Good evening,', 'Evening,', 'Powering up. Good evening,'],
    lateNight: ['Burning the midnight oil,', 'Still with you,', 'Late hours, I see,']
  };

  const DEADLINE_NEAR = [
    'that one is tight', 'the clock is already short on this',
    'not much runway on that', 'that deadline is close'
  ];

  const FLOURISH = [
    "I'll sound the alert when it's time.", "I'll remind you when the moment arrives.",
    "leave the timing to me.", "I'll prompt you in good time.",
    "you'll hear from me before it's due.", ''
  ];

  // ---- public: acknowledge a newly captured task ---------------------------
  function acknowledge(task) {
    const cat = C.byKey(task.category);
    const due = task.due ? new Date(task.due) : null;
    const now = new Date();
    const lead = pick(ACK_LEAD);
    const gist = shorten(task.title);

    let dueBit = '';
    if (due) {
      const mins = (due - now) / 60000;
      const when = humanWhen(due);
      if (mins < 90 && mins > 0) dueBit = join(', ' + pick(DEADLINE_NEAR) + ' —', when + '.');
      else dueBit = 'for ' + when + '.';
    }

    const classBit = (cat.color !== 'gold' && cat.color !== 'standard')
      ? join('Filed as', cat.spoken + (dueBit ? ',' : '.'))
      : '';

    // Two structural shapes, chosen at random, so it never reads the same way.
    const opener = ADDRESS[Math.floor(Math.random() * ADDRESS.length)]();
    let line;
    if (Math.random() < 0.5) {
      line = join(opener, lead, '"' + gist + '"', dueBit, classBit, maybe(0.4, pick(FLOURISH)));
    } else {
      line = join(opener, '"' + gist + '" —', pick(ACK), classBit, dueBit && due ? 'Due ' + humanWhen(due) + '.' : '', maybe(0.35, pick(FLOURISH)));
    }
    return say(line, { onText: settings.onText });
  }

  // ---- public: greeting when the HUD/dashboard opens -----------------------
  function greeting(stats = {}) {
    const g = pick(GREET[partOfDay()]);
    const a = addr();
    const bits = [];
    if (stats.overdue > 0) bits.push(`${stats.overdue} ${stats.overdue === 1 ? 'directive is' : 'directives are'} overdue`);
    if (stats.dueToday > 0) bits.push(`${stats.dueToday} due today`);
    let body;
    if (!stats.total) body = pick(['the board is clear.', 'nothing on the slate. What shall we line up?', 'no active directives. Where would you like to begin?']);
    else if (bits.length) body = bits.join(', ') + '. ' + pick(['Shall we begin?', 'I suggest we start there.', 'Your move.']);
    else body = pick([`${stats.total} directives tracked, all on schedule.`, 'everything is on schedule.', `${stats.total} tracked and nothing pressing.`]);
    return say(join(g, a + ',', body));
  }

  // ---- public: the deadline alert (the "it's time" moment) -----------------
  function alert(task, kind) {
    const cat = C.byKey(task.category);
    const gist = shorten(task.title);
    const a = addr();
    const due = task.due ? new Date(task.due) : null;

    const URGENCY = {
      red: ['This is critical.', 'Top priority.', 'This cannot slip.'],
      amber: ['Hard deadline.', 'The clock is up.', "Time's in."],
      cyan: ['Focus time.', 'Work block.', ''],
      green: ['For you, this one.', 'Look after yourself.', ''],
      violet: ['When you have a moment.', 'A thought to chase.', ''],
      gold: ['', 'A directive.', '']
    };
    const tone = URGENCY[cat.color] || [''];

    let line;
    if (kind === 'soon') {
      const mins = due ? Math.max(1, Math.round((due - new Date()) / 60000)) : 10;
      line = join(
        pick([a + ',', 'Heads up,', 'A moment,', 'Quick word,']),
        pick(['just ahead —', 'coming up —', 'on the horizon —', 'shortly —']),
        '"' + gist + '"',
        pick([`in about ${mins} minutes.`, `due in roughly ${mins} minutes.`, `${mins} minutes out.`]),
        maybe(0.5, pick(tone))
      );
    } else if (kind === 'overdue') {
      line = join(
        pick([a + ',', 'A reminder,', 'Still outstanding,', 'Circling back,']),
        '"' + gist + '"',
        pick(['has slipped past its deadline.', 'is now overdue.', 'was due and is still open.', 'is past time.']),
        pick(tone),
        pick(['Shall I keep it live?', 'I recommend we clear it.', 'Your call on this one.', ''])
      );
    } else { // 'due' — the headline "it's time" event
      line = join(
        pick([a + '.', a + ',', 'It is time,', 'The hour is here,', 'Now,']),
        pick(['it is time for', 'this is the moment for', 'time to attend to', 'this is your cue for', 'the moment has arrived for']),
        '"' + gist + '".',
        pick(tone),
        maybe(0.45, pick(['Shall I clear the way?', 'I have you covered.', "Let's see it done.", "I'll mark it the moment it's complete."]))
      );
    }
    return say(line, { force: kind === 'due' || cat.color === 'red', onText: settings.onText, pitch: cat.color === 'red' ? 0.86 : settings.pitch });
  }

  // ---- public: completion + misc reactions ---------------------------------
  function reactComplete(task) {
    const gist = shorten(task.title);
    return say(join(
      pick([addr() + ',', 'Done.', 'Excellent.', 'Very good,']),
      pick([`"${gist}" is cleared.`, `"${gist}" — complete.`, `that's "${gist}" off the board.`, `marking "${gist}" done.`]),
      maybe(0.4, pick(['One less on the slate.', 'Onward.', 'Momentum is good.', '']))
    ));
  }

  // ---- text helpers --------------------------------------------------------
  function shorten(title) {
    let t = (title || '').trim();
    if (t.length > 70) t = t.slice(0, 67).replace(/\s+\S*$/, '') + '…';
    return t;
  }
  function humanWhen(d) {
    const now = new Date();
    const sameDay = d.toDateString() === now.toDateString();
    const tmr = new Date(now); tmr.setDate(now.getDate() + 1);
    const isTmr = d.toDateString() === tmr.toDateString();
    const time = d.toLocaleTimeString([], { hour: 'numeric', minute: d.getMinutes() ? '2-digit' : undefined });
    if (sameDay) return 'today at ' + time;
    if (isTmr) return 'tomorrow at ' + time;
    const within7 = (d - now) / 86400000 < 7;
    const day = d.toLocaleDateString([], within7 ? { weekday: 'long' } : { weekday: 'short', month: 'short', day: 'numeric' });
    return day + ' at ' + time;
  }

  return { say, acknowledge, greeting, alert, reactComplete, configure, listVoices, humanWhen, partOfDay };
})();
