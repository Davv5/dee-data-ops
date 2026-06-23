import 'package:flutter/foundation.dart';

import '../models/reminder.dart';
import '../services/notification_service.dart';
import '../services/store.dart';
import '../services/sync_service.dart';

/// Single source of truth for the UI. Orchestrates persistence, local
/// notifications, and cloud sync.
class AppState extends ChangeNotifier {
  final Store store = Store();
  final NotificationService notifications = NotificationService();
  SyncService? _sync;

  List<Reminder> reminders = <Reminder>[];
  String dbUrl = '';
  String syncCode = '';
  bool syncing = false;

  /// Editable names for each crystal colour (acts like a category per colour).
  List<String> labels = <String>['Coral', 'Amber', 'Teal', 'Violet', 'Sky', 'Rose'];

  bool get cloudConfigured => dbUrl.isNotEmpty && syncCode.isNotEmpty;

  String labelFor(int colorIndex) =>
      labels[colorIndex % labels.length];

  Future<void> setLabel(int index, String name) async {
    final clean = name.trim();
    if (clean.isEmpty || index < 0 || index >= labels.length) return;
    labels[index] = clean;
    await store.saveLabels(labels);
    notifyListeners();
  }

  /// Reminders whose one-off time is already in the past (and not repeating).
  List<Reminder> get passed => reminders
      .where((r) => r.repeat == Repeat.none && r.dateTime.isBefore(DateTime.now()))
      .toList();

  Future<void> init() async {
    // 1) Local data first — the UI needs it and this is fast.
    try {
      reminders = await store.loadReminders();
      final (u, c) = await store.loadSync();
      dbUrl = u;
      syncCode = c;
      final savedLabels = await store.loadLabels();
      if (savedLabels != null && savedLabels.length == labels.length) {
        labels = savedLabels;
      }
      _sort();
      notifyListeners();
    } catch (_) {/* corrupt/empty store — start fresh */}

    // 2) Notifications — best effort. A failure here must never blank the app.
    try {
      await notifications.init();
      await notifications.requestBasicPermissions();
      await _rescheduleAll();
    } catch (_) {/* keep running; user can grant permission from Settings */}

    // 3) Cloud sync if configured.
    if (cloudConfigured) {
      try {
        _startSync();
      } catch (_) {/* offline — retries on next tick */}
    }
    notifyListeners();
  }

  void _sort() {
    reminders.sort((a, b) => a.dateTime.compareTo(b.dateTime));
  }

  Future<void> _rescheduleAll() async {
    await notifications.cancelAll();
    for (final r in reminders) {
      await notifications.schedule(r);
    }
  }

  Future<void> upsert(Reminder r) async {
    final updated =
        r.copyWith(updatedAt: DateTime.now().millisecondsSinceEpoch);
    final i = reminders.indexWhere((e) => e.id == updated.id);
    if (i >= 0) {
      reminders[i] = updated;
    } else {
      reminders.add(updated);
    }
    _sort();
    await store.saveReminders(reminders);
    await notifications.schedule(updated);
    notifyListeners();
    _sync?.pushItem(updated);
  }

  Future<void> remove(Reminder r) async {
    reminders.removeWhere((e) => e.id == r.id);
    await store.saveReminders(reminders);
    await notifications.cancel(r.notificationId);
    notifyListeners();
    _sync?.deleteItem(r.id);
  }

  /// Delete a set of reminders at once (selection delete).
  Future<void> removeMany(Set<String> ids) async {
    if (ids.isEmpty) return;
    final removed = reminders.where((e) => ids.contains(e.id)).toList();
    reminders.removeWhere((e) => ids.contains(e.id));
    await store.saveReminders(reminders);
    for (final r in removed) {
      await notifications.cancel(r.notificationId);
      _sync?.deleteItem(r.id);
    }
    notifyListeners();
  }

  /// Delete every one-off reminder whose time has already passed.
  Future<void> clearPassed() async {
    final ids = passed.map((e) => e.id).toSet();
    await removeMany(ids);
  }

  Future<void> toggle(Reminder r, bool enabled) =>
      upsert(r.copyWith(enabled: enabled));

  /// Merge the shared cloud state into local state.
  ///
  /// Conflict resolution: newest `updatedAt` wins. Items present locally but not
  /// remotely are treated as deleted-elsewhere once they age past a short grace
  /// window (so a just-created item isn't dropped before its push lands).
  Future<void> _applyRemote(Map<String, Reminder> remote) async {
    final now = DateTime.now().millisecondsSinceEpoch;
    const graceMs = 60 * 1000;

    final merged = <String, Reminder>{for (final r in reminders) r.id: r};
    remote.forEach((id, incoming) {
      final local = merged[id];
      if (local == null || incoming.updatedAt >= local.updatedAt) {
        merged[id] = incoming;
      }
    });
    merged.removeWhere(
      (id, r) => !remote.containsKey(id) && (now - r.updatedAt) > graceMs,
    );

    final next = merged.values.toList()
      ..sort((a, b) => a.dateTime.compareTo(b.dateTime));

    if (_sameAsCurrent(next)) return;
    reminders = next;
    await store.saveReminders(reminders);
    await _rescheduleAll();
    notifyListeners();
  }

  bool _sameAsCurrent(List<Reminder> next) {
    if (next.length != reminders.length) return false;
    for (var i = 0; i < next.length; i++) {
      if (next[i].id != reminders[i].id ||
          next[i].updatedAt != reminders[i].updatedAt) {
        return false;
      }
    }
    return true;
  }

  void _startSync() {
    _sync = SyncService(dbUrl: dbUrl, code: syncCode, onData: _applyRemote);
    _sync!.start();
    syncing = true;
  }

  Future<void> saveSyncSettings(String url, String code) async {
    dbUrl = url.trim();
    syncCode = code.trim();
    await store.saveSync(dbUrl, syncCode);
    _sync?.stop();
    _sync = null;
    syncing = false;
    if (cloudConfigured) {
      _startSync();
      await _sync?.pushAll(reminders);
    }
    notifyListeners();
  }

  Future<void> refreshNow() => _sync?.fetch() ?? Future<void>.value();

  @override
  void dispose() {
    _sync?.stop();
    super.dispose();
  }
}
