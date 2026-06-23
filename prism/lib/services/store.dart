import 'dart:convert';

import 'package:shared_preferences/shared_preferences.dart';

import '../models/reminder.dart';

/// Local persistence: reminders + sync settings, via shared_preferences.
class Store {
  static const String _kReminders = 'reminders_v1';
  static const String _kDbUrl = 'db_url';
  static const String _kSyncCode = 'sync_code';

  Future<List<Reminder>> loadReminders() async {
    final prefs = await SharedPreferences.getInstance();
    final raw = prefs.getString(_kReminders);
    if (raw == null || raw.isEmpty) return <Reminder>[];
    try {
      final decoded = jsonDecode(raw) as List<dynamic>;
      return decoded
          .map((e) => Reminder.fromJson(Map<String, dynamic>.from(e as Map)))
          .toList();
    } catch (_) {
      return <Reminder>[];
    }
  }

  Future<void> saveReminders(List<Reminder> reminders) async {
    final prefs = await SharedPreferences.getInstance();
    final raw = jsonEncode(reminders.map((e) => e.toJson()).toList());
    await prefs.setString(_kReminders, raw);
  }

  Future<(String, String)> loadSync() async {
    final prefs = await SharedPreferences.getInstance();
    return (prefs.getString(_kDbUrl) ?? '', prefs.getString(_kSyncCode) ?? '');
  }

  Future<void> saveSync(String dbUrl, String syncCode) async {
    final prefs = await SharedPreferences.getInstance();
    await prefs.setString(_kDbUrl, dbUrl);
    await prefs.setString(_kSyncCode, syncCode);
  }
}
