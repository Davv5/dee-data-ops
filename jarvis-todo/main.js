// JARVIS — main process.
// Owns the windows, the global hotkey, the tray, and the task store. Two
// windows exist: a frameless "quick-add" HUD that the hotkey summons, and the
// always-running dashboard that also hosts the deadline scheduler + speech.

const {
  app, BrowserWindow, globalShortcut, ipcMain, Tray, Menu, nativeImage, screen
} = require('electron');
const path = require('path');
const store = require('./src/store');

const isDev = process.argv.includes('--dev');
const DEFAULT_HOTKEY = 'CommandOrControl+Shift+Space';

// Safety net: a stray "object destroyed" or similar must never take the whole
// background app down. Log it and keep running so the menu bar stays alive.
process.on('uncaughtException', (err) => console.error('[jarvis] uncaught:', err));

let dashboardWin = null;
let quickWin = null;
let tray = null;

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

function createDashboard() {
  dashboardWin = new BrowserWindow({
    width: 1180,
    height: 760,
    minWidth: 880,
    minHeight: 580,
    show: false,
    title: 'JARVIS',
    backgroundColor: '#00000000',
    titleBarStyle: 'hiddenInset',
    trafficLightPosition: { x: 18, y: 22 },
    vibrancy: 'under-window',
    visualEffectState: 'active',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  dashboardWin.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  dashboardWin.once('ready-to-show', () => dashboardWin.show());

  // Keep the process alive in the tray rather than quitting on close.
  dashboardWin.on('close', (e) => {
    if (!app.isQuitting) {
      e.preventDefault();
      dashboardWin.hide();
    }
  });
}

function createQuickAdd() {
  const display = screen.getPrimaryDisplay();
  const { width } = display.workAreaSize;

  quickWin = new BrowserWindow({
    width: 760,
    height: 520,
    x: Math.round((width - 760) / 2),
    y: Math.round(display.workAreaSize.height * 0.18),
    show: false,
    frame: false,
    transparent: true,
    resizable: false,
    movable: true,
    skipTaskbar: true,
    alwaysOnTop: true,
    hasShadow: false,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  quickWin.loadFile(path.join(__dirname, 'renderer', 'quickadd.html'));
  quickWin.setVisibleOnAllWorkspaces(true, { visibleOnFullScreenScreens: true });

  // Dismiss when it loses focus (feels like a system HUD).
  quickWin.on('blur', () => {
    if (quickWin && quickWin.isVisible()) hideQuickAdd();
  });
}

function quickAlive() {
  return quickWin && !quickWin.isDestroyed();
}

function showQuickAdd() {
  if (!quickAlive()) createQuickAdd();
  const display = screen.getPrimaryDisplay();
  const { width } = display.workAreaSize;
  quickWin.setPosition(
    Math.round((width - 760) / 2),
    Math.round(display.workAreaSize.height * 0.18)
  );
  quickWin.show();
  quickWin.focus();
  quickWin.webContents.send('quickadd:summon', store.settings());
}

function hideQuickAdd() {
  if (quickAlive()) {
    quickWin.webContents.send('quickadd:dismiss');
    quickWin.hide();
  }
}

function toggleQuickAdd() {
  if (quickAlive() && quickWin.isVisible()) hideQuickAdd();
  else showQuickAdd();
}

// ---------------------------------------------------------------------------
// Tray
// ---------------------------------------------------------------------------

function trayIcon() {
  // A tiny gold reactor glyph drawn as a data-URI so we ship no binary asset.
  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22">
    <circle cx="11" cy="11" r="9" fill="none" stroke="#ffb648" stroke-width="1.6"/>
    <circle cx="11" cy="11" r="4" fill="#ffd27a"/>
  </svg>`;
  const img = nativeImage.createFromDataURL(
    'data:image/svg+xml;base64,' + Buffer.from(svg).toString('base64')
  );
  img.setTemplateImage(false);
  return img;
}

function createTray() {
  tray = new Tray(trayIcon());
  tray.setToolTip('JARVIS — at your service');
  const menu = Menu.buildFromTemplate([
    { label: 'New directive  (⇧⌘Space)', click: showQuickAdd },
    { label: 'Open dashboard', click: () => { if (dashboardWin) dashboardWin.show(); } },
    { type: 'separator' },
    { label: 'Quit JARVIS', click: () => { app.isQuitting = true; app.quit(); } }
  ]);
  tray.setContextMenu(menu);
  tray.on('click', showQuickAdd);
}

// ---------------------------------------------------------------------------
// Hotkey
// ---------------------------------------------------------------------------

function registerHotkey() {
  const accel = store.settings().hotkey || DEFAULT_HOTKEY;
  globalShortcut.unregisterAll();
  const ok = globalShortcut.register(accel, toggleQuickAdd);
  if (!ok) {
    // Fall back to the default if a custom binding is already taken.
    globalShortcut.register(DEFAULT_HOTKEY, toggleQuickAdd);
  }
}

// ---------------------------------------------------------------------------
// IPC — store + window plumbing
// ---------------------------------------------------------------------------

function broadcast(channel, payload) {
  [dashboardWin, quickWin].forEach((w) => {
    if (w && !w.isDestroyed()) w.webContents.send(channel, payload);
  });
}

ipcMain.handle('tasks:all', () => store.all());
ipcMain.handle('settings:get', () => store.settings());
ipcMain.handle('settings:save', (_e, patch) => {
  const s = store.saveSettings(patch);
  if (patch.hotkey) registerHotkey();
  broadcast('settings:changed', s);
  return s;
});

ipcMain.handle('tasks:add', (_e, task) => {
  const rec = store.add(task);
  broadcast('tasks:changed', store.all());
  return rec;
});

ipcMain.handle('tasks:update', (_e, { id, patch }) => {
  const rec = store.update(id, patch);
  broadcast('tasks:changed', store.all());
  return rec;
});

ipcMain.handle('tasks:remove', (_e, id) => {
  store.remove(id);
  broadcast('tasks:changed', store.all());
  return true;
});

ipcMain.on('quickadd:close', hideQuickAdd);
ipcMain.on('quickadd:resize', (_e, height) => {
  if (quickWin) {
    const [w] = quickWin.getSize();
    quickWin.setSize(w, Math.max(360, Math.round(height)));
  }
});
ipcMain.on('dashboard:open', () => { if (dashboardWin) dashboardWin.show(); });

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) {
  app.quit();
} else {
  app.on('second-instance', showQuickAdd);

  app.whenReady().then(() => {
    if (process.platform === 'darwin') app.dock.hide(); // live in the menu bar
    createDashboard();
    createQuickAdd();
    createTray();
    registerHotkey();

    app.on('activate', () => {
      if (BrowserWindow.getAllWindows().length === 0) createDashboard();
      else if (dashboardWin) dashboardWin.show();
    });
  });

  app.on('will-quit', () => globalShortcut.unregisterAll());
  // Don't quit when all windows are "closed" — we hide to the tray instead.
  app.on('window-all-closed', (e) => { if (!app.isQuitting) e.preventDefault?.(); });
}
