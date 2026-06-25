enum Repeat { none, daily, weekly }

extension RepeatLabel on Repeat {
  String get label {
    switch (this) {
      case Repeat.none:
        return 'Once';
      case Repeat.daily:
        return 'Daily';
      case Repeat.weekly:
        return 'Weekly';
    }
  }
}

int _asInt(dynamic v, [int fallback = 0]) {
  if (v is int) return v;
  if (v is double) return v.toInt();
  if (v is String) return int.tryParse(v) ?? fallback;
  return fallback;
}

bool _asBool(dynamic v, [bool fallback = true]) {
  if (v is bool) return v;
  if (v is String) return v.toLowerCase() == 'true';
  return fallback;
}

class Reminder {
  final String id;
  final String title;
  final DateTime dateTime;
  final Repeat repeat;
  final int colorIndex;
  final bool sound;
  final bool enabled;
  final int updatedAt; // millisecondsSinceEpoch (for sync conflict resolution)

  Reminder({
    required this.id,
    required this.title,
    required this.dateTime,
    this.repeat = Repeat.none,
    this.colorIndex = 0,
    this.sound = true,
    this.enabled = true,
    required this.updatedAt,
  });

  /// Stable, positive 31-bit id used by the OS notification scheduler.
  int get notificationId => id.hashCode & 0x7fffffff;

  Reminder copyWith({
    String? title,
    DateTime? dateTime,
    Repeat? repeat,
    int? colorIndex,
    bool? sound,
    bool? enabled,
    int? updatedAt,
  }) {
    return Reminder(
      id: id,
      title: title ?? this.title,
      dateTime: dateTime ?? this.dateTime,
      repeat: repeat ?? this.repeat,
      colorIndex: colorIndex ?? this.colorIndex,
      sound: sound ?? this.sound,
      enabled: enabled ?? this.enabled,
      updatedAt: updatedAt ?? this.updatedAt,
    );
  }

  Map<String, dynamic> toJson() => <String, dynamic>{
        'id': id,
        'title': title,
        'dateTime': dateTime.millisecondsSinceEpoch,
        'repeat': repeat.index,
        'colorIndex': colorIndex,
        'sound': sound,
        'enabled': enabled,
        'updatedAt': updatedAt,
      };

  factory Reminder.fromJson(Map<String, dynamic> json) {
    return Reminder(
      id: (json['id'] ?? '').toString(),
      title: (json['title'] ?? '').toString(),
      dateTime: DateTime.fromMillisecondsSinceEpoch(_asInt(json['dateTime'])),
      repeat: Repeat.values[_asInt(json['repeat']).clamp(0, Repeat.values.length - 1)],
      colorIndex: _asInt(json['colorIndex']),
      sound: _asBool(json['sound']),
      enabled: _asBool(json['enabled']),
      updatedAt: _asInt(json['updatedAt']),
    );
  }

  static String newId() {
    final now = DateTime.now();
    return '${now.microsecondsSinceEpoch}-${now.hashCode & 0xffff}';
  }
}
