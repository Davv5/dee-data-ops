// Lightweight natural-language deadline parser. Zero dependencies.
// Extracts a Date from phrases like:
//   "submit report tomorrow 5pm"  /  "call mum friday"  /  "in 2 hours"
//   "gym tonight"  /  "pay invoice aug 3 9am"  /  "next monday noon"
// Returns { due: Date|null, cleanTitle: string } — cleanTitle has the time
// words stripped so the task title reads naturally.
window.JARVIS_NLP = (function () {
  const WEEKDAYS = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
  const MONTHS = ['january', 'february', 'march', 'april', 'may', 'june', 'july',
    'august', 'september', 'october', 'november', 'december'];
  const MONTH_ABBR = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];

  function atTime(date, h, m) {
    const d = new Date(date);
    d.setHours(h, m || 0, 0, 0);
    return d;
  }

  // Find an explicit clock time. Returns {h,m,matched} or null.
  function parseTime(text) {
    if (/\bnoon\b/.test(text)) return { h: 12, m: 0, matched: 'noon' };
    if (/\bmidnight\b/.test(text)) return { h: 0, m: 0, matched: 'midnight' };

    // 5pm / 5:30 pm / 17:00 / 9 am
    let m = text.match(/\b(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m\.?\b/);
    if (m) {
      let h = parseInt(m[1], 10) % 12;
      if (m[3] === 'p') h += 12;
      return { h, m: m[2] ? parseInt(m[2], 10) : 0, matched: m[0] };
    }
    m = text.match(/\b(\d{1,2}):(\d{2})\b/);
    if (m) return { h: parseInt(m[1], 10), m: parseInt(m[2], 10), matched: m[0] };
    return null;
  }

  function nextWeekday(targetIdx, forceNext) {
    const now = new Date();
    let delta = (targetIdx - now.getDay() + 7) % 7; // 0 == same weekday as today
    // A bare weekday that lands on today, or any "next <day>", rolls forward a
    // full week — you rarely mean "right now" when you name a weekday.
    if (delta === 0 || forceNext) delta = delta === 0 ? 7 : delta + 7;
    const d = new Date();
    d.setDate(now.getDate() + delta);
    return d;
  }

  function parse(input) {
    const text = ' ' + input.toLowerCase() + ' ';
    let baseDate = null;
    const consumed = [];          // substrings to strip from the title
    const time = parseTime(text);

    // ---- relative day words ----
    if (/\btomorrow\b/.test(text)) { baseDate = new Date(); baseDate.setDate(baseDate.getDate() + 1); consumed.push('tomorrow'); }
    else if (/\btonight\b/.test(text)) { baseDate = new Date(); if (!time) { return finalize(input, atTime(baseDate, 20, 0), consumed.concat('tonight')); } consumed.push('tonight'); }
    else if (/\btoday\b/.test(text)) { baseDate = new Date(); consumed.push('today'); }
    else if (/\bthis evening\b/.test(text)) { baseDate = new Date(); if (!time) return finalize(input, atTime(baseDate, 19, 0), consumed.concat('this evening')); consumed.push('this evening'); }
    else if (/\bthis afternoon\b/.test(text)) { baseDate = new Date(); if (!time) return finalize(input, atTime(baseDate, 15, 0), consumed.concat('this afternoon')); consumed.push('this afternoon'); }
    else if (/\bmorning\b/.test(text)) { baseDate = new Date(); if (/tomorrow/.test(text)) baseDate.setDate(baseDate.getDate() + 1); if (!time) return finalize(input, atTime(baseDate, 9, 0), consumed.concat('morning')); consumed.push('morning'); }

    // ---- "in N minutes/hours/days/weeks" ----
    const rel = text.match(/\bin\s+(\d+)\s*(min(?:ute)?s?|hours?|hrs?|days?|weeks?)\b/);
    if (rel && !baseDate) {
      const n = parseInt(rel[1], 10);
      const d = new Date();
      const unit = rel[2];
      if (/min/.test(unit)) d.setMinutes(d.getMinutes() + n);
      else if (/h/.test(unit)) d.setHours(d.getHours() + n);
      else if (/day/.test(unit)) d.setDate(d.getDate() + n);
      else if (/week/.test(unit)) d.setDate(d.getDate() + n * 7);
      return finalize(input, d, [rel[0].trim()]);
    }

    // ---- weekdays ("friday", "next monday") ----
    if (!baseDate) {
      const forceNext = /\bnext\s+/.test(text);
      for (let i = 0; i < WEEKDAYS.length; i++) {
        const re = new RegExp('\\b(next\\s+)?' + WEEKDAYS[i] + '\\b');
        const mm = text.match(re);
        if (mm) {
          baseDate = nextWeekday(i, forceNext);
          consumed.push(mm[0].trim());
          break;
        }
      }
    }

    // ---- explicit calendar date ("aug 3", "3 august", "august 3rd") ----
    if (!baseDate) {
      const allMonths = MONTHS.concat(MONTH_ABBR);
      for (let i = 0; i < allMonths.length; i++) {
        const mName = allMonths[i];
        const monthIdx = i % 12;
        let mm = text.match(new RegExp('\\b' + mName + '\\.?\\s+(\\d{1,2})(?:st|nd|rd|th)?\\b'));
        if (!mm) mm = text.match(new RegExp('\\b(\\d{1,2})(?:st|nd|rd|th)?\\s+' + mName + '\\b'));
        if (mm) {
          const day = parseInt(mm[1], 10);
          const d = new Date();
          d.setMonth(monthIdx, day);
          if (d < new Date() && !/\d{4}/.test(text)) d.setFullYear(d.getFullYear() + 1);
          baseDate = d;
          consumed.push(mm[0].trim());
          break;
        }
      }
    }

    // ---- next week / this week ----
    if (!baseDate && /\bnext week\b/.test(text)) { baseDate = new Date(); baseDate.setDate(baseDate.getDate() + 7); consumed.push('next week'); }

    if (!baseDate && !time) return { due: null, cleanTitle: input.trim() };

    if (!baseDate) baseDate = new Date(); // time only -> today
    const finalDate = time ? atTime(baseDate, time.h, time.m) : atTime(baseDate, 9, 0);
    if (time) consumed.push(time.matched);
    return finalize(input, finalDate, consumed);
  }

  function finalize(original, date, consumed) {
    let title = original;
    const fillers = ['by', 'on', 'at', 'due', 'this', 'next', 'in', '-'];
    consumed.filter(Boolean).forEach((c) => {
      title = title.replace(new RegExp(escape(c), 'ig'), ' ');
    });
    // tidy dangling prepositions + whitespace
    title = title.replace(/\s+/g, ' ').trim();
    title = title.replace(new RegExp('\\b(' + fillers.join('|') + ')\\b\\s*$', 'i'), '').trim();
    title = title.replace(/[,;:\s]+$/g, '').trim();
    return { due: date, cleanTitle: title || original.trim() };
  }

  function escape(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); }

  return { parse };
})();
