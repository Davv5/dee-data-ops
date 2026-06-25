import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;

import '../models/reminder.dart';

/// Cloud sync over the Firebase Realtime Database REST API.
///
/// No native Firebase SDK — just plain HTTPS — which keeps the build light and
/// identical on Android and macOS. Reminders live under `/rooms/<syncCode>` so
/// two devices sharing the same code share the same list. The sync code acts as
/// a shared secret/capability; use a long random one.
class SyncService {
  final String dbUrl;
  final String code;
  final Future<void> Function(Map<String, Reminder> remote) onData;

  Timer? _timer;
  bool _busy = false;

  SyncService({
    required this.dbUrl,
    required this.code,
    required this.onData,
  });

  String get _base {
    var u = dbUrl.trim();
    while (u.endsWith('/')) {
      u = u.substring(0, u.length - 1);
    }
    return u;
  }

  Uri get _collection => Uri.parse('$_base/rooms/$code/reminders.json');
  Uri _item(String id) => Uri.parse('$_base/rooms/$code/reminders/$id.json');

  void start() {
    stop();
    fetch();
    _timer = Timer.periodic(const Duration(seconds: 12), (_) => fetch());
  }

  void stop() {
    _timer?.cancel();
    _timer = null;
  }

  Future<void> fetch() async {
    if (_busy) return;
    _busy = true;
    try {
      final res =
          await http.get(_collection).timeout(const Duration(seconds: 15));
      if (res.statusCode != 200) return;
      final body = res.body.trim();
      if (body.isEmpty || body == 'null') {
        await onData(<String, Reminder>{});
        return;
      }
      final decoded = jsonDecode(body);
      final out = <String, Reminder>{};
      if (decoded is Map) {
        decoded.forEach((key, value) {
          if (value is Map) {
            try {
              out[key.toString()] =
                  Reminder.fromJson(Map<String, dynamic>.from(value));
            } catch (_) {/* skip malformed item */}
          }
        });
      }
      await onData(out);
    } catch (_) {
      // Offline / transient: ignore and retry on the next tick.
    } finally {
      _busy = false;
    }
  }

  Future<void> pushItem(Reminder r) async {
    try {
      await http
          .put(_item(r.id),
              headers: const {'Content-Type': 'application/json'},
              body: jsonEncode(r.toJson()))
          .timeout(const Duration(seconds: 15));
    } catch (_) {/* will reconcile on next sync */}
  }

  Future<void> pushAll(List<Reminder> items) async {
    for (final r in items) {
      await pushItem(r);
    }
  }

  Future<void> deleteItem(String id) async {
    try {
      await http.delete(_item(id)).timeout(const Duration(seconds: 15));
    } catch (_) {/* will reconcile on next sync */}
  }
}
