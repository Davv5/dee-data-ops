// Tag taxonomy — colour is meaning. Built-in tags plus user-defined custom
// tags (settable by hand in Settings or by voice: "Jarvis, make a tag called
// Finance in green"). A colour palette backs both.
window.FRIDAY_COLORS = (function () {
  // named palette -> hex + glow. Custom tags pick one of these.
  const PALETTE = {
    red: '#ff5d72', orange: '#ff8a3c', amber: '#ffc46a', gold: '#ffd9a0',
    lime: '#b6ff5d', green: '#5dffb0', teal: '#38e8c0', cyan: '#6ff0ff',
    blue: '#5db4ff', indigo: '#8a8cff', violet: '#c39bff', magenta: '#ff6fe0',
    pink: '#ff8ab0', white: '#dbeaff'
  };
  const hex = (c) => PALETTE[c] || PALETTE.gold;

  const BUILTIN = {
    critical: { label: 'Critical', color: 'red', meaning: 'Mission-critical. Drop everything else.', spoken: 'a critical priority',
      keywords: ['urgent', 'critical', 'asap', 'emergency', 'now', 'p0', 'blocker', '!!!'] },
    deadline: { label: 'Deadline', color: 'amber', meaning: 'Hard deadline. Time-boxed and unmovable.', spoken: 'a hard deadline',
      keywords: ['deadline', 'due', 'submit', 'deliver', 'ship', 'launch', 'invoice', 'pay', 'bill'] },
    work: { label: 'Work / Focus', color: 'cyan', meaning: 'Deep work, client, or build tasks.', spoken: 'work',
      keywords: ['work', 'build', 'code', 'client', 'meeting', 'call', 'email', 'review', 'report', 'design', 'write'] },
    personal: { label: 'Personal / Health', color: 'green', meaning: 'Personal wellbeing, health, errands, life.', spoken: 'a personal matter',
      keywords: ['gym', 'run', 'workout', 'health', 'doctor', 'meds', 'water', 'sleep', 'eat', 'family', 'home', 'shop', 'groceries', 'clean'] },
    idea: { label: 'Idea / Creative', color: 'violet', meaning: 'Sparks, research, things to explore.', spoken: 'an idea worth exploring',
      keywords: ['idea', 'maybe', 'explore', 'research', 'read', 'learn', 'sketch', 'brainstorm', 'someday'] },
    standard: { label: 'Standard', color: 'gold', meaning: 'General directive, no special class.', spoken: 'a directive', keywords: [] }
  };

  let custom = {};   // { key: {label,color,meaning,spoken,keywords} }

  function decorate(key, t) {
    return Object.assign({ key, hex: hex(t.color), glow: hex(t.color), spoken: t.spoken || (t.label || 'a directive').toLowerCase(), keywords: t.keywords || [] }, t, { key, hex: hex(t.color) });
  }
  function allRaw() { return Object.assign({}, BUILTIN, custom); }
  function byKey(k) {
    const raw = allRaw();
    const t = raw[k] || BUILTIN.standard;
    return decorate(raw[k] ? k : 'standard', t);
  }
  function infer(text) {
    const t = (text || '').toLowerCase();
    let best = 'standard', score = 0;
    for (const [key, c] of Object.entries(allRaw())) {
      let s = 0;
      for (const kw of (c.keywords || [])) if (kw && t.includes(kw)) s += kw.length > 3 ? 2 : 1;
      if (s > score) { score = s; best = key; }
    }
    return best;
  }
  function configure(customTags) { custom = customTags || {}; }
  function slug(name) { return (name || 'tag').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '').slice(0, 20) || 'tag'; }
  function makeTag(name, color, meaning) {
    const key = slug(name);
    const label = name.trim().replace(/\b\w/g, (c) => c.toUpperCase()).slice(0, 24);
    return {
      key,
      tag: {
        label, color: PALETTE[color] ? color : 'cyan',
        meaning: meaning || (name.trim() + ' tasks.'), spoken: name.trim().toLowerCase(),
        keywords: [slug(name).replace(/_/g, ' '), name.trim().toLowerCase()]
      }
    };
  }

  return {
    PALETTE,
    palette: Object.keys(PALETTE),
    hex,
    byKey,
    infer,
    configure,
    makeTag,
    isCustom: (k) => !!custom[k],
    // live getters so callers always see built-ins + current custom tags
    get categories() { return allRaw(); },
    get order() { return [...Object.keys(BUILTIN), ...Object.keys(custom)]; }
  };
})();
