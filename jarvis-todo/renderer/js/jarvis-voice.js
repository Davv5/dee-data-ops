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
  let settings = { address: 'Sir', voiceURI: null, rate: 0.94, pitch: 0.92, mute: false };
  const recent = [];                       // anti-repeat ring buffer

  function isEnglish(v) { return /^en/i.test(v.lang); }
  function quality(v) {
    // Higher = better. Enhanced/Premium system voices are dramatically less
    // robotic than the default "compact" ones, so we weight them up heavily.
    const n = (v.name + ' ' + (v.voiceURI || '')).toLowerCase();
    let s = 0;
    if (/premium/.test(n)) s += 60;
    if (/enhanced/.test(n)) s += 50;
    if (/siri/.test(n)) s += 45;
    if (/neural|natural/.test(n)) s += 40;
    // calm British males read most like JARVIS
    if (/daniel|arthur|oliver|jamie|serena/.test(n)) s += 25;
    if (/en[-_]?gb/i.test(v.lang)) s += 18;
    if (/google uk english/.test(n)) s += 15;
    if (/compact/.test(n)) s -= 30;     // the robotic ones
    if (isEnglish(v)) s += 5;
    return s;
  }

  function pickVoice() {
    const voices = window.speechSynthesis.getVoices();
    if (!voices.length) return;
    if (settings.voiceURI) {
      const exact = voices.find((v) => v.voiceURI === settings.voiceURI);
      if (exact) { chosenVoice = exact; return; }
    }
    // Otherwise auto-pick the highest-quality English voice available.
    const ranked = voices.filter(isEnglish).sort((a, b) => quality(b) - quality(a));
    chosenVoice = ranked[0] || voices[0];
  }

  // true if the system has at least one genuinely high-quality voice installed
  function hasGoodVoice() {
    return ('speechSynthesis' in window) &&
      window.speechSynthesis.getVoices().some((v) => isEnglish(v) && quality(v) >= 40);
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
  const ADDRESS = [() => addr() + ',', () => '', () => '', () => 'Right then,', () => addr() + '.'];

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

  // ---- intent: turn a typed directive into a natural spoken action ---------
  // "sleep at 11.30pm"  -> "head to sleep"
  // "Call CEO at 3am"   -> "call the CEO"
  // so JARVIS can say "it's time to call the CEO" rather than quoting the text.
  function cap(s) { return s ? s.charAt(0).toUpperCase() + s.slice(1) : s; }

  const ROLE_NOUNS = ['ceo', 'cfo', 'cto', 'coo', 'boss', 'client', 'customer', 'doctor',
    'dentist', 'lawyer', 'accountant', 'landlord', 'manager', 'team', 'bank', 'office',
    'gym', 'store', 'shop', 'pharmacy', 'hospital', 'school', 'airport', 'vet', 'barber',
    'plumber', 'realtor', 'agent', 'recruiter', 'investor', 'supplier', 'vendor'];

  function withArticle(obj) {
    if (!obj) return obj;
    const first = obj.split(/\s+/)[0].toLowerCase().replace(/[^a-z]/g, '');
    if (/^(the|a|an|my|your|his|her|our|their|some)$/.test(first)) return obj;
    if (ROLE_NOUNS.includes(first)) return 'the ' + obj;
    return obj; // proper names / relations (mum, John, ...) stay bare
  }

  const VERB_PHRASES = {
    sleep: () => pick(['head to sleep', 'get some rest', 'wind down for the night', 'turn in for the night']),
    bed: () => 'head to bed', rest: () => 'get some rest', nap: () => 'take a nap', relax: () => 'unwind',
    wake: () => 'wake up', wakeup: () => 'wake up', getup: () => 'get up',
    call: (o) => 'call ' + withArticle(o), ring: (o) => 'call ' + withArticle(o), phone: (o) => 'call ' + withArticle(o), dial: (o) => 'call ' + withArticle(o), facetime: (o) => 'FaceTime ' + withArticle(o),
    text: (o) => 'message ' + withArticle(o), message: (o) => 'message ' + withArticle(o), msg: (o) => 'message ' + withArticle(o), dm: (o) => 'message ' + withArticle(o),
    email: (o) => 'email ' + withArticle(o), mail: (o) => 'email ' + withArticle(o),
    meet: (o) => o ? 'meet ' + withArticle(o) : 'head into your meeting', meeting: (o) => o ? 'meet ' + withArticle(o) : 'head into your meeting',
    pay: (o) => 'pay ' + withArticle(o), transfer: (o) => 'transfer ' + (o || 'the funds'), invoice: (o) => 'send the invoice' + (o ? ' ' + o : ''),
    submit: (o) => 'submit ' + (o || 'it'), file: (o) => 'file ' + (o || 'it'),
    send: (o) => 'send ' + (o || 'it'), deliver: (o) => 'deliver ' + (o || 'it'), ship: (o) => 'ship ' + (o || 'it'),
    buy: (o) => 'pick up ' + (o || 'it'), get: (o) => 'pick up ' + (o || 'it'), grab: (o) => 'grab ' + (o || 'it'), purchase: (o) => 'pick up ' + (o || 'it'), order: (o) => 'order ' + (o || 'it'), pick: (o) => 'pick up ' + (o || 'it'),
    eat: (o) => o ? 'eat ' + o : 'eat something', lunch: () => 'have lunch', dinner: () => 'have dinner', breakfast: () => 'have breakfast', snack: () => 'have a snack', cook: (o) => 'cook ' + (o || 'dinner'),
    drink: (o) => 'have ' + (o || 'some water'), water: () => 'hydrate', hydrate: () => 'hydrate',
    gym: () => 'train', workout: () => 'train', train: () => 'train', exercise: () => 'train', run: () => 'go for your run', lift: () => 'train', stretch: () => 'stretch', walk: (o) => o ? 'walk ' + o : 'take a walk',
    study: (o) => o ? 'study ' + o : 'get studying', read: (o) => o ? 'read ' + o : 'get reading', review: (o) => 'review ' + (o || 'it'), learn: (o) => 'study ' + (o || 'it'), practice: (o) => 'practice ' + (o || ''),
    take: (o) => /med|pill|vitamin|tablet/i.test(o || '') ? 'take your ' + o : 'take ' + (o || 'it'), medication: () => 'take your medication', meds: () => 'take your meds', medicine: () => 'take your medicine',
    leave: (o) => o ? 'leave for ' + withArticle(o) : 'head out', depart: (o) => 'leave' + (o ? ' for ' + withArticle(o) : ''), head: (o) => o ? 'head ' + o : 'head out', go: (o) => o ? 'go ' + o : 'get going',
    work: (o) => o ? 'get to work on ' + o : 'get to work', build: (o) => 'work on ' + (o || 'it'), code: (o) => 'work on ' + (o || 'it'), write: (o) => 'write ' + (o || 'it'), finish: (o) => 'finish ' + (o || 'it'), complete: (o) => 'finish ' + (o || 'it'), start: (o) => 'start ' + (o || 'it'), prepare: (o) => 'prepare ' + (o || 'it'), prep: (o) => 'prep ' + (o || 'it'), book: (o) => 'book ' + (o || 'it'), schedule: (o) => 'schedule ' + (o || 'it'),
    post: (o) => 'post ' + (o || 'it'), publish: (o) => 'publish ' + (o || 'it'), record: (o) => 'record ' + (o || 'it'), edit: (o) => 'edit ' + (o || 'it'),
    clean: (o) => 'clean ' + (o || 'up'), wash: (o) => 'wash ' + (o || 'up'), laundry: () => 'do the laundry',
    feed: (o) => 'feed ' + (o || 'them'), check: (o) => 'check ' + (o || 'it'), pray: () => 'pray', meditate: () => 'meditate', journal: () => 'journal',
    begin: (o) => 'begin ' + (o || 'it'), continue: (o) => 'continue ' + (o || 'it'), resume: (o) => 'resume ' + (o || 'it'),
    revise: (o) => 'revise ' + (o || 'it'), attend: (o) => 'attend ' + (o || 'it'), join: (o) => 'join ' + (o || 'it'),
    watch: (o) => 'watch ' + (o || 'it'), plan: (o) => 'plan ' + (o || 'it'), organize: (o) => 'organise ' + (o || 'it'),
    renew: (o) => 'renew ' + (o || 'it'), apply: (o) => 'apply' + (o ? ' ' + o : ''), update: (o) => 'update ' + (o || 'it'), fix: (o) => 'fix ' + (o || 'it')
  };

  function extractIntent(rawTitle) {
    let title = (rawTitle || '').replace(/\s+/g, ' ').trim();
    // strip lead-in filler, repeatedly ("please remind me to go call…")
    const FILLER = /^(reminder\s+(to|for|:)\s*|remember\s+to\s+|remind\s+me\s+(to|about|for)\s+|don'?t\s+forget\s+(to\s+)?|i\s+(need|have|want|wanna|gotta|got)\s+(to\s+)?|i\s+should\s+|we\s+should\s+|need\s+to\s+|have\s+to\s+|gotta\s+|got\s+to\s+|make\s+sure\s+(i|to)\s+|please\s+|just\s+|quickly\s+|really\s+|to\s+|must\s+|go\s+and\s+)/i;
    let prev;
    do { prev = title; title = title.replace(FILLER, '').trim(); } while (title !== prev && title.length);
    // safety net: strip any trailing time/date that survived in the title
    title = title.replace(/\s+(at|by)?\s*\d{1,2}([:.]\d{2})?\s*([ap]\.?m\.?)?\s*$/i, ' $1').replace(/\s+(at|by)\s*$/i, '').trim();
    title = title.replace(/\s+(tonight|tomorrow|today|this\s+(morning|afternoon|evening)|next\s+week)$/i, '').trim();
    title = title.replace(/\s+(on\s+)?(mon|tue|wed|thu|fri|sat|sun)[a-z]*$/i, '').trim();
    // keep the first clause only
    title = title.split(/\s+(?:because|since|so that|and then|after that)\s+/i)[0];
    title = title.replace(/[.,;:!?]+$/, '').trim();
    const words = title.split(/\s+/);
    // scan the first 3 tokens for a known action verb ("go call mum" -> call)
    for (let i = 0; i < Math.min(3, words.length); i++) {
      const verb = (words[i] || '').toLowerCase().replace(/[^a-z]/g, '');
      const fn = VERB_PHRASES[verb];
      if (fn) {
        let object = words.slice(i + 1).join(' ').replace(/[.,;:!?]+$/, '').trim();
        // clamp long objects so speech stays tight
        const ow = object.split(/\s+/);
        if (ow.length > 7) object = ow.slice(0, 7).join(' ');
        // speak from JARVIS's side: "my course" -> "your course"
        object = object.replace(/\bmy\b/gi, 'your').replace(/\bour\b/gi, 'your')
                       .replace(/\bmyself\b/gi, 'yourself').replace(/\bi\b/g, 'you').replace(/\bme\b/gi, 'you');
        return { action: fn(object), matched: true };
      }
    }
    return { action: null, matched: false, title };
  }

  function clockOnly(d) {
    return d ? d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) : '';
  }

  // short line for desktop notifications
  function cue(task, kind) {
    const it = extractIntent(task.title);
    const subj = it.matched ? it.action : (task.title || 'your directive');
    if (kind === 'soon') return 'Coming up — ' + cap(subj);
    if (kind === 'overdue') return 'Still pending — ' + cap(subj);
    return 'Time to ' + subj;
  }

  // ---- public: acknowledge a newly captured task ---------------------------
  function acknowledge(task) {
    const due = task.due ? new Date(task.due) : null;
    const now = new Date();
    const it = extractIntent(task.title);
    const gist = shorten(task.title);
    const subject = it.matched ? it.action : `attend to "${gist}"`;
    const opener = ADDRESS[Math.floor(Math.random() * ADDRESS.length)]();
    let line;

    if (due) {
      const when = task.repeat ? humanRepeat(due, task.repeat) : humanWhen(due);
      const near = !task.repeat && (due - now) / 60000 < 90 && due > now;
      line = join(opener, pick([
        `I'll remind you to ${subject} ${when}.`,
        `noted — I'll prompt you to ${subject} ${when}.`,
        `consider it set. ${cap(subject)}, ${when}.`,
        `${cap(subject)} ${when} — I'll sound the alert.`
      ]), near ? cap(pick(DEADLINE_NEAR)) + '.' : '', maybe(0.3, pick(FLOURISH)));
    } else {
      line = join(opener, pick([
        `I'll keep "${gist}" on the board.`,
        `"${gist}" — logged. Add a time and I'll watch the clock.`,
        `noted. "${gist}" is tracked.`
      ]));
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
    const a = addr();
    const due = task.due ? new Date(task.due) : null;
    const it = extractIntent(task.title);
    const gist = shorten(task.title);
    const subject = it.matched ? it.action : `attend to "${gist}"`;
    const clock = clockOnly(due);

    const URGENCY = {
      red: ['This is critical.', 'Top priority.', 'This cannot slip.'],
      amber: ['Hard deadline.', 'The clock is up.', "Time's in."],
      cyan: ['Focus time.', 'Work block.', ''],
      green: ['For you, this one.', 'Look after yourself.', ''],
      violet: ['When you have a moment.', 'A thought to chase.', ''],
      gold: ['', '', '']
    };
    const tone = URGENCY[cat.color] || [''];

    let line;
    if (kind === 'soon') {
      const mins = due ? Math.max(1, Math.round((due - new Date()) / 60000)) : 10;
      const mlabel = `${mins} minute${mins === 1 ? '' : 's'}`;
      line = join(
        pick([a + ',', 'Heads up,', 'A moment,', 'Quick word,']),
        pick([
          `in about ${mlabel} it'll be time to ${subject}.`,
          `you'll want to ${subject} in roughly ${mlabel}.`,
          `${cap(subject)} comes up in ${mlabel}.`
        ]),
        maybe(0.5, pick(tone))
      );
    } else if (kind === 'overdue') {
      line = join(
        pick([a + ',', 'A reminder,', 'Still outstanding,', 'Circling back,']),
        pick([
          `you still need to ${subject}.`,
          `you've yet to ${subject}.`,
          `${cap(subject)} is still pending` + (clock ? ` — it was due at ${clock}.` : '.')
        ]),
        pick(tone),
        pick(['Shall I keep it live?', 'I recommend we clear it.', 'Your call on this one.', ''])
      );
    } else { // 'due' — the headline "it's time" event
      const head = pick([a + '.', a + ',', 'It is time,', 'Now,', a + ' —']);
      const body = it.matched ? pick([
        `it's time to ${subject}.`,
        `it is time to ${subject}.`,
        `you should ${subject} now.`,
        `let's ${subject}.`,
        `time to ${subject}.`
      ]) : pick([
        `it's time for "${gist}".`,
        `"${gist}" is up.`,
        `the moment for "${gist}" has arrived.`,
        `"${gist}" — now.`
      ]);
      const timeBit = clock ? pick([`It's ${clock}.`, `The time is ${clock}.`, `It's ${clock} now.`, '']) : '';
      line = join(head, body, pick(tone), timeBit,
        maybe(0.3, pick(['Shall I clear the way?', 'I have you covered.', "Let's see it done.", ''])));
    }
    return say(line, { force: kind === 'due' || cat.color === 'red', onText: settings.onText, pitch: cat.color === 'red' ? 0.86 : settings.pitch });
  }

  // ---- public: completion + misc reactions ---------------------------------
  function reactComplete(task) {
    const gist = shorten(task.title);
    return say(join(
      pick([addr() + ',', 'Done.', 'Excellent.', 'Cleared.']),
      pick([`"${gist}" is cleared.`, `"${gist}" — complete.`, `that's "${gist}" off the board.`, `marking "${gist}" done.`]),
      maybe(0.4, pick(['One less on the slate.', 'Onward.', 'Momentum is good.', '']))
    ));
  }

  // ---- text helpers --------------------------------------------------------
  function shorten(title) {
    // never speak a paragraph — clamp hard at a word boundary
    let t = (title || '').replace(/\s+/g, ' ').trim();
    const words = t.split(/\s+/);
    if (words.length > 8) t = words.slice(0, 8).join(' ') + '…';
    if (t.length > 48) t = t.slice(0, 48).replace(/\s+\S*$/, '') + '…';
    return t;
  }
  function humanRepeat(d, repeat) {
    const time = d ? d.toLocaleTimeString([], { hour: 'numeric', minute: d.getMinutes() ? '2-digit' : undefined }) : '';
    if (repeat && repeat.freq === 'weekly') {
      const day = d ? d.toLocaleDateString([], { weekday: 'long' }) : 'week';
      return 'every ' + day + (time ? ' at ' + time : '');
    }
    return 'every day' + (time ? ' at ' + time : '');
  }

  // ---- public: spoken agenda ("brief me" / "what's next") ------------------
  function brief(tasks) {
    const a = addr();
    const now = new Date();
    const active = (tasks || []).filter((t) => !t.done);
    if (!active.length) return say(pick([`${a}, the board is clear. Nothing scheduled.`, `Nothing on the slate, ${a}. Enjoy it.`]));

    const overdue = active.filter((t) => t.due && new Date(t.due) < now);
    const today = active.filter((t) => t.due && new Date(t.due) >= now && new Date(t.due).toDateString() === now.toDateString())
      .sort((x, y) => new Date(x.due) - new Date(y.due));
    const later = active.filter((t) => t.due && new Date(t.due) > now && new Date(t.due).toDateString() !== now.toDateString())
      .sort((x, y) => new Date(x.due) - new Date(y.due));

    const parts = [];
    if (today.length) {
      const list = today.slice(0, 3).map((t) => `${shorten(t.title)} at ${clockOnly(new Date(t.due))}`).join(', ');
      parts.push(`${today.length === 1 ? 'one directive today' : today.length + ' today'}: ${list}${today.length > 3 ? ', and more' : ''}`);
    }
    if (overdue.length) {
      parts.push(`${overdue.length} overdue` + (overdue.length <= 2 ? ' — ' + overdue.map((t) => shorten(t.title)).join(', and ') : ''));
    }
    if (!today.length && later.length) {
      const n = later[0];
      parts.push(`next up: ${shorten(n.title)}, ${humanWhen(new Date(n.due))}`);
    }
    if (!parts.length) parts.push(`${active.length} tracked, none time-boxed yet`);
    return say(join(pick([a + ',', 'Right then,', a + ' —', 'As it stands,']), parts.join('. ') + '.',
      maybe(0.35, pick(['Shall we begin?', 'I suggest we start at the top.', '']))));
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

  return { say, acknowledge, greeting, alert, reactComplete, cue, brief, configure, listVoices, hasGoodVoice, humanWhen, humanRepeat, partOfDay };
})();
