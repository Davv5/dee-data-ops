import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:flutter_timezone/flutter_timezone.dart';
import 'package:timezone/data/latest_all.dart' as tzdata;
import 'package:timezone/timezone.dart' as tz;

import '../models/reminder.dart';

/// Wraps local (on-device) scheduled notifications with sound, for Android + macOS.
///
/// Each device schedules independently against its own clock. Because phones and
/// Macs keep NTP-synced time, the same reminder fires within ~1s on both.
class NotificationService {
  final FlutterLocalNotificationsPlugin _plugin =
      FlutterLocalNotificationsPlugin();
  bool _ready = false;

  Future<void> init() async {
    if (_ready) return;

    tzdata.initializeTimeZones();
    try {
      final name = await FlutterTimezone.getLocalTimezone();
      tz.setLocalLocation(tz.getLocation(name));
    } catch (_) {
      tz.setLocalLocation(tz.getLocation('UTC'));
    }

    const android = AndroidInitializationSettings('@mipmap/ic_launcher');
    const darwin = DarwinInitializationSettings(
      requestAlertPermission: false,
      requestBadgePermission: false,
      requestSoundPermission: false,
    );
    const settings = InitializationSettings(android: android, macOS: darwin);
    await _plugin.initialize(settings);
    _ready = true;
  }

  /// Alerts + sound. Safe to call at startup — opens no settings pages.
  Future<void> requestBasicPermissions() async {
    await init();
    final android = _plugin.resolvePlatformSpecificImplementation<
        AndroidFlutterLocalNotificationsPlugin>();
    await android?.requestNotificationsPermission();

    final macos = _plugin.resolvePlatformSpecificImplementation<
        MacOSFlutterLocalNotificationsPlugin>();
    await macos?.requestPermissions(alert: true, badge: true, sound: true);
  }

  /// Exact-alarm grant. On Android this opens a system settings page, so only
  /// call it from an explicit button — never at startup.
  Future<void> requestExactAlarms() async {
    await init();
    final android = _plugin.resolvePlatformSpecificImplementation<
        AndroidFlutterLocalNotificationsPlugin>();
    await android?.requestExactAlarmsPermission();
  }

  /// Everything, for the explicit "Grant permission" button in Settings.
  Future<void> requestAllPermissions() async {
    await requestBasicPermissions();
    await requestExactAlarms();
  }

  NotificationDetails _details(bool sound) {
    final android = AndroidNotificationDetails(
      'prism_reminders',
      'Reminders',
      channelDescription: 'Prism scheduled reminders',
      importance: Importance.max,
      priority: Priority.high,
      playSound: sound,
      enableVibration: true,
    );
    final darwin = DarwinNotificationDetails(
      presentAlert: true,
      presentBadge: false,
      presentSound: sound,
    );
    return NotificationDetails(android: android, macOS: darwin);
  }

  tz.TZDateTime _nextInstance(Reminder r) {
    final now = tz.TZDateTime.now(tz.local);
    var scheduled = tz.TZDateTime.from(r.dateTime, tz.local);
    if (r.repeat == Repeat.daily) {
      while (!scheduled.isAfter(now)) {
        scheduled = scheduled.add(const Duration(days: 1));
      }
    } else if (r.repeat == Repeat.weekly) {
      while (!scheduled.isAfter(now)) {
        scheduled = scheduled.add(const Duration(days: 7));
      }
    }
    return scheduled;
  }

  Future<void> schedule(Reminder r) async {
    await init();
    await cancel(r.notificationId);
    if (!r.enabled) return;

    final when = _nextInstance(r);
    final now = tz.TZDateTime.now(tz.local);
    if (r.repeat == Repeat.none && !when.isAfter(now)) {
      return; // one-off in the past — nothing to schedule
    }

    DateTimeComponents? match;
    if (r.repeat == Repeat.daily) match = DateTimeComponents.time;
    if (r.repeat == Repeat.weekly) match = DateTimeComponents.dayOfWeekAndTime;

    try {
      await _plugin.zonedSchedule(
        r.notificationId,
        r.title.isEmpty ? 'Reminder' : r.title,
        _subtitle(r.repeat),
        when,
        _details(r.sound),
        androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
        uiLocalNotificationDateInterpretation:
            UILocalNotificationDateInterpretation.absoluteTime,
        matchDateTimeComponents: match,
      );
    } catch (_) {
      // Exact-alarm permission not granted — fall back to an inexact alarm so the
      // reminder still fires (a minute of slack) instead of throwing and dropping it.
      await _plugin.zonedSchedule(
        r.notificationId,
        r.title.isEmpty ? 'Reminder' : r.title,
        _subtitle(r.repeat),
        when,
        _details(r.sound),
        androidScheduleMode: AndroidScheduleMode.inexactAllowWhileIdle,
        uiLocalNotificationDateInterpretation:
            UILocalNotificationDateInterpretation.absoluteTime,
        matchDateTimeComponents: match,
      );
    }
  }

  String _subtitle(Repeat repeat) {
    switch (repeat) {
      case Repeat.none:
        return 'Prism reminder';
      case Repeat.daily:
        return 'Every day';
      case Repeat.weekly:
        return 'Every week';
    }
  }

  Future<void> showTest() async {
    await init();
    await _plugin.show(
      0x7ffffffe,
      'Prism is set up',
      'Notifications and sound are working 🎉',
      _details(true),
    );
  }

  Future<void> cancel(int id) => _plugin.cancel(id);

  Future<void> cancelAll() => _plugin.cancelAll();
}
