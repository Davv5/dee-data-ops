// Conversational command parser. Turns what you say/type into an action:
// small talk (thanks/greet), reschedule/snooze, complete, make-a-tag, or
// create a directive. Used by the quick-add HUD for both typed and spoken input.
window.JARVIS_COMMANDS = (function () {
  const RESPONSES = {
    thanks: ['A pleasure, {a}.', 'Anytime, {a}.', 'Of course, {a}.', 'Happy to help, {a}.', 'Think nothing of it, {a}.', 'At your service, always.', 'My pleasure, {a}.'],
    greet: ['Hello, {a}.', 'Yes, {a}?', "I'm here, {a}.", 'Always, {a}.', 'Go ahead, {a}.', 'How can I help, {a}?'],
    status: ['Fully operational, {a}.', 'All systems nominal, {a}.', 'Running at peak — and you, {a}?', 'Never better, {a}.'],
    dismiss: ['Very well, {a}.', 'Standing down.', 'As you wish, {a}.', 'Understood, {a}.'],
    wake: ['Yes, {a}?', "I'm listening.", 'At your service, {a}.', 'Go ahead.'],
    huh: ["I'm not certain I follow, {a}.", 'Could you rephrase that, {a}?', 'Say again, {a}?', "I didn't quite get that, {a}."]
  };

  function line(type, addr) {
    const arr = RESPONSES[type] || RESPONSES.huh;
    return arr[Math.floor(Math.random() * arr.length)].replace(/\{a\}/g, addr || 'Boss');
  }

  function classify(raw) {
    let t = (raw || '').trim();
    t = t.replace(/^\s*(hey\s+|ok\s+|okay\s+|wake up\s+)?(?:friday|jarvis)[\s,!.:-]*/i, '').trim(); // strip wake word
    const l = t.toLowerCase();
    if (!t) return { type: 'wake' };

    if (/^(thanks|thank you|thank u|thx|ty|cheers|much appreciated|appreciate (it|you)|good (job|work|boy|lad)|well done|nice( work| one)?|perfect|brilliant|excellent|amazing|legend|you'?re the best|love (you|it|ya))\b/.test(l)) return { type: 'thanks' };
    if (/^(hi|hey|hello|yo|hiya|sup|good (morning|afternoon|evening|day))\b/.test(l) || /\b(you there|are you there|you up)\b/.test(l)) return { type: 'greet' };
    if (/\bhow (are you|are things|is it going|'?s it going|you doing|you holding up)\b|\byou (good|ok|okay|alright)\b/.test(l)) return { type: 'status' };
    if (/^(never ?mind|cancel|dismiss|forget it|stop|that'?s all|nothing|no thanks|nah)\b/.test(l)) return { type: 'dismiss' };

    // make a custom tag: "make a tag called Finance in green"
    const tm = l.match(/\b(make|create|add|new|set up)\s+(a\s+|an\s+)?(tag|label|category|class|colou?r)\s+(called\s+|named\s+|for\s+|:\s*)?(.+)/);
    if (tm) {
      let rest = tm[5].trim(); let color = null;
      const PAL = window.JARVIS_COLORS.PALETTE;
      const cm = rest.match(/\b(in|as|with|coloured?|color)\s+([a-z]+)\b/);
      if (cm && PAL[cm[2]]) { color = cm[2]; rest = rest.replace(cm[0], '').trim(); }
      const words = rest.split(/\s+/);
      if (!color && PAL[words[words.length - 1]]) { color = words.pop(); rest = words.join(' '); }
      rest = rest.replace(/\b(tag|label|category)\b/g, '').replace(/[,.]+$/, '').trim();
      return { type: 'tag', name: rest || 'New Tag', color };
    }

    // reschedule / snooze
    if (/\b(reschedule|re-?schedule|resched|move|push|change|shift|snooze|delay|postpone|bump|put it (off|back)|do it later|later)\b/.test(l)) {
      const parsed = window.JARVIS_NLP.parse(t);
      let snoozeMins = null;
      const sm = l.match(/\b(snooze|delay|push|postpone|bump|later)\b[^0-9]*(\d+)\s*(min(?:ute)?s?|hours?|hrs?|h)\b/);
      if (sm) snoozeMins = parseInt(sm[2], 10) * (/^h/.test(sm[3]) ? 60 : 1);
      else { const s2 = l.match(/\b(\d+)\s*(min(?:ute)?s?|hours?|hrs?|h)\b/); if (/snooze|later|delay|push/.test(l) && s2) snoozeMins = parseInt(s2[1], 10) * (/^h/.test(s2[2]) ? 60 : 1); }
      return { type: 'reschedule', due: parsed.due, snoozeMins };
    }

    if (/\b(mark (it|that|this)?\s*(as\s+)?done|^done\b|complete[d]?|finished?|did it|i'?ve done it|handled|sorted|clear (it|that)|tick (it|that) off)\b/.test(l)) return { type: 'complete' };

    // explicit create
    const cm = t.match(/^\s*(set|add|create|new|log|note|make)\s+(a\s+|an\s+)?(directive|task|reminder|to-?do|todo|alarm|note)\s*(to\s+|for\s+|that\s+|:\s*)?(.+)/i)
      || t.match(/^\s*(remind me to|remind me|reminder to|reminder for|remember to|don'?t forget to|i need to|need to|i have to|have to)\s+(.+)/i);
    if (cm) return { type: 'create', title: cm[cm.length - 1].trim() };

    return { type: 'maybeCreate', title: t };
  }

  return { classify, line };
})();
