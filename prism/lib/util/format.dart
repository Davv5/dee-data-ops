String _two(int n) => n.toString().padLeft(2, '0');

const List<String> _weekdays = <String>[
  'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun',
];
const List<String> _months = <String>[
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec',
];

/// Friendly absolute label, e.g. "Today · 14:30" or "Wed 24 Jun · 09:00".
String formatWhen(DateTime dt) {
  final now = DateTime.now();
  final today = DateTime(now.year, now.month, now.day);
  final day = DateTime(dt.year, dt.month, dt.day);
  final diff = day.difference(today).inDays;
  final time = '${_two(dt.hour)}:${_two(dt.minute)}';
  if (diff == 0) return 'Today · $time';
  if (diff == 1) return 'Tomorrow · $time';
  if (diff == -1) return 'Yesterday · $time';
  return '${_weekdays[dt.weekday - 1]} ${dt.day} ${_months[dt.month - 1]} · $time';
}

/// Compact relative label, e.g. "in 3h", "in 2d", "now", "passed".
String relativeTo(DateTime dt) {
  final d = dt.difference(DateTime.now());
  if (d.isNegative) return 'passed';
  if (d.inMinutes < 1) return 'now';
  if (d.inMinutes < 60) return 'in ${d.inMinutes}m';
  if (d.inHours < 24) return 'in ${d.inHours}h';
  return 'in ${d.inDays}d';
}
