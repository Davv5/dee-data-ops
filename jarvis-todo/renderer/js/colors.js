// Colour taxonomy — colour is meaning, not decoration.
// Each category carries a use-case, a hue, and the words JARVIS uses to refer
// to it when speaking. The quick-add parser maps natural keywords -> category.
window.JARVIS_COLORS = (function () {
  const CATEGORIES = {
    critical: {
      label: 'Critical',
      color: 'red',
      hex: '#ff4d5e',
      glow: 'rgba(255,77,94,0.55)',
      meaning: 'Mission-critical. Drop everything else.',
      spoken: 'a critical priority',
      keywords: ['urgent', 'critical', 'asap', 'emergency', 'now', 'p0', 'blocker', '!!!']
    },
    deadline: {
      label: 'Deadline',
      color: 'amber',
      hex: '#ffb648',
      glow: 'rgba(255,182,72,0.55)',
      meaning: 'Hard deadline. Time-boxed and unmovable.',
      spoken: 'a hard deadline',
      keywords: ['deadline', 'due', 'submit', 'deliver', 'ship', 'launch', 'invoice', 'pay', 'bill']
    },
    work: {
      label: 'Work / Focus',
      color: 'cyan',
      hex: '#39d7ff',
      glow: 'rgba(57,215,255,0.5)',
      meaning: 'Deep work, client, or build tasks.',
      spoken: 'work',
      keywords: ['work', 'build', 'code', 'client', 'meeting', 'call', 'email', 'review', 'report', 'design', 'write']
    },
    personal: {
      label: 'Personal / Health',
      color: 'green',
      hex: '#4dffa6',
      glow: 'rgba(77,255,166,0.5)',
      meaning: 'Personal wellbeing, health, errands, life.',
      spoken: 'a personal matter',
      keywords: ['gym', 'run', 'workout', 'health', 'doctor', 'meds', 'water', 'sleep', 'eat', 'family', 'home', 'shop', 'groceries', 'clean']
    },
    idea: {
      label: 'Idea / Creative',
      color: 'violet',
      hex: '#b58bff',
      glow: 'rgba(181,139,255,0.5)',
      meaning: 'Sparks, research, things to explore.',
      spoken: 'an idea worth exploring',
      keywords: ['idea', 'maybe', 'explore', 'research', 'read', 'learn', 'sketch', 'brainstorm', 'someday']
    },
    standard: {
      label: 'Standard',
      color: 'gold',
      hex: '#e9d8a6',
      glow: 'rgba(233,216,166,0.4)',
      meaning: 'General directive, no special class.',
      spoken: 'a directive',
      keywords: []
    }
  };

  function byKey(key) {
    return CATEGORIES[key] || CATEGORIES.standard;
  }

  // Infer a category from free text using keyword hits.
  function infer(text) {
    const t = (text || '').toLowerCase();
    let best = 'standard';
    let bestScore = 0;
    for (const [key, c] of Object.entries(CATEGORIES)) {
      let score = 0;
      for (const kw of c.keywords) {
        if (t.includes(kw)) score += kw.length > 3 ? 2 : 1;
      }
      if (score > bestScore) { bestScore = score; best = key; }
    }
    return best;
  }

  return {
    categories: CATEGORIES,
    order: ['critical', 'deadline', 'work', 'personal', 'idea', 'standard'],
    byKey,
    infer
  };
})();
