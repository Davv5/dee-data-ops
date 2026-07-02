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

  // window control
  openQuickAdd: () => ipcRenderer.send('quickadd:open'),
  closeQuickAdd: () => ipcRenderer.send('quickadd:close'),
  resizeQuickAdd: (h) => ipcRenderer.send('quickadd:resize', h),
  openDashboard: () => ipcRenderer.send('dashboard:open'),

  // alert HUD (Jarvis-styled actionable reminder card)
  showAlert: (payload) => ipcRenderer.send('alert:show', payload),
  closeAlert: () => ipcRenderer.send('alert:close'),
  editFromAlert: (id) => ipcRenderer.send('alert:edit', id),
  onAlertData: (cb) => ipcRenderer.on('alert:data', (_e, p) => cb(p)),
  onEditorOpen: (cb) => ipcRenderer.on('editor:open', (_e, id) => cb(id)),

  // events (main -> renderer)
  onTasksChanged: (cb) => ipcRenderer.on('tasks:changed', (_e, tasks) => cb(tasks)),
  onSettingsChanged: (cb) => ipcRenderer.on('settings:changed', (_e, s) => cb(s)),
  onSummon: (cb) => ipcRenderer.on('quickadd:summon', (_e, s) => cb(s)),
  onDismiss: (cb) => ipcRenderer.on('quickadd:dismiss', () => cb())
});
