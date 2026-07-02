// Persistent task store (main-process side).
// Tasks live in a single JSON file under the OS user-data directory so the
// data survives app restarts and re-installs of the same build.

const { app } = require('electron');
const fs = require('fs');
const path = require('path');

const FILE = path.join(app.getPath('userData'), 'jarvis-tasks.json');

function load() {
  try {
    const raw = fs.readFileSync(FILE, 'utf8');
    const data = JSON.parse(raw);
    if (Array.isArray(data.tasks)) return data;
    return { tasks: [], settings: data.settings || {} };
  } catch (_) {
    return { tasks: [], settings: {} };
  }
}

function persist(data) {
  try {
    fs.mkdirSync(path.dirname(FILE), { recursive: true });
    fs.writeFileSync(FILE, JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    console.error('[store] failed to persist:', err);
  }
}

let state = load();

module.exports = {
  file: FILE,
  all() {
    return state.tasks;
  },
  settings() {
    return state.settings || {};
  },
  saveSettings(patch) {
    state.settings = { ...(state.settings || {}), ...patch };
    persist(state);
    return state.settings;
  },
  add(task) {
    const record = {
      id: `t_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`,
      title: task.title || 'Untitled directive',
      notes: task.notes || '',
      category: task.category || 'standard',
      color: task.color || 'cyan',
      due: task.due || null,          // ISO string or null
      repeat: task.repeat || null,    // { freq: 'daily' | 'weekly' } or null
      createdAt: new Date().toISOString(),
      done: false,
      announcedDue: false,            // spoken "it's time" fired
      announcedSoon: false            // spoken pre-warning fired
    };
    state.tasks.push(record);
    persist(state);
    return record;
  },
  update(id, patch) {
    const t = state.tasks.find((x) => x.id === id);
    if (!t) return null;
    Object.assign(t, patch);
    persist(state);
    return t;
  },
  remove(id) {
    state.tasks = state.tasks.filter((x) => x.id !== id);
    persist(state);
    return true;
  },
  replaceAll(tasks) {
    state.tasks = tasks;
    persist(state);
  }
};
