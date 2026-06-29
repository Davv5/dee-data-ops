// Secure bridge between the renderer and the main process.
const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('jarvis', {
  // store
  getTasks: () => ipcRenderer.invoke('tasks:all'),
  addTask: (task) => ipcRenderer.invoke('tasks:add', task),
  updateTask: (id, patch) => ipcRenderer.invoke('tasks:update', { id, patch }),
  removeTask: (id) => ipcRenderer.invoke('tasks:remove', id),

  // settings
  getSettings: () => ipcRenderer.invoke('settings:get'),
  saveSettings: (patch) => ipcRenderer.invoke('settings:save', patch),

  // brain (local LLM) — ask returns { ok, reply, actions } or { ok:false }
  askBrain: (text) => ipcRenderer.invoke('brain:ask', text),
  brainHealth: () => ipcRenderer.invoke('brain:health'),

  // window control
  closeQuickAdd: () => ipcRenderer.send('quickadd:close'),
  resizeQuickAdd: (h) => ipcRenderer.send('quickadd:resize', h),
  openDashboard: () => ipcRenderer.send('dashboard:open'),

  // events (main -> renderer)
  onTasksChanged: (cb) => ipcRenderer.on('tasks:changed', (_e, tasks) => cb(tasks)),
  onSettingsChanged: (cb) => ipcRenderer.on('settings:changed', (_e, s) => cb(s)),
  onSummon: (cb) => ipcRenderer.on('quickadd:summon', (_e, s) => cb(s)),
  onDismiss: (cb) => ipcRenderer.on('quickadd:dismiss', () => cb())
});
