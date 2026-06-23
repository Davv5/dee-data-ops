import 'package:flutter/material.dart';

import '../main.dart';
import '../models/reminder.dart';
import '../theme.dart';
import '../util/format.dart';
import '../widgets/glass.dart';
import '../widgets/prism_background.dart';

class EditScreen extends StatefulWidget {
  final Reminder? existing;
  const EditScreen({super.key, this.existing});

  @override
  State<EditScreen> createState() => _EditScreenState();
}

class _EditScreenState extends State<EditScreen> {
  late final TextEditingController _title;
  late DateTime _when;
  late Repeat _repeat;
  late int _colorIndex;
  late bool _sound;

  bool get _isEditing => widget.existing != null;

  @override
  void initState() {
    super.initState();
    final e = widget.existing;
    _title = TextEditingController(text: e?.title ?? '');
    _when = e?.dateTime ?? _defaultTime();
    _repeat = e?.repeat ?? Repeat.none;
    _colorIndex = e?.colorIndex ?? 0;
    _sound = e?.sound ?? true;
  }

  DateTime _defaultTime() {
    final n = DateTime.now().add(const Duration(hours: 1));
    return DateTime(n.year, n.month, n.day, n.hour, 0);
  }

  @override
  void dispose() {
    _title.dispose();
    super.dispose();
  }

  Future<void> _pickDate() async {
    final d = await showDatePicker(
      context: context,
      initialDate: _when,
      firstDate: DateTime.now().subtract(const Duration(days: 1)),
      lastDate: DateTime.now().add(const Duration(days: 365 * 5)),
      builder: _pickerTheme,
    );
    if (d != null) {
      setState(() =>
          _when = DateTime(d.year, d.month, d.day, _when.hour, _when.minute));
    }
  }

  Future<void> _pickTime() async {
    final t = await showTimePicker(
      context: context,
      initialTime: TimeOfDay.fromDateTime(_when),
      builder: _pickerTheme,
    );
    if (t != null) {
      setState(() => _when =
          DateTime(_when.year, _when.month, _when.day, t.hour, t.minute));
    }
  }

  Widget _pickerTheme(BuildContext context, Widget? child) {
    return Theme(
      data: Theme.of(context).copyWith(
        colorScheme: const ColorScheme.dark(
          primary: Palette.coral,
          onPrimary: Color(0xFF2A1209),
          surface: Palette.bg1,
          onSurface: Palette.ink,
        ),
      ),
      child: child!,
    );
  }

  void _save() {
    final r = (widget.existing ??
            Reminder(
              id: Reminder.newId(),
              title: '',
              dateTime: _when,
              updatedAt: 0,
            ))
        .copyWith(
      title: _title.text.trim(),
      dateTime: _when,
      repeat: _repeat,
      colorIndex: _colorIndex,
      sound: _sound,
      enabled: true,
    );
    appState.upsert(r);
    Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Palette.bg0,
      extendBodyBehindAppBar: true,
      appBar: AppBar(
        backgroundColor: Colors.transparent,
        elevation: 0,
        foregroundColor: Palette.ink,
        title: Text(_isEditing ? 'Edit reminder' : 'New reminder'),
        actions: [
          if (_isEditing)
            IconButton(
              icon: const Icon(Icons.delete_outline),
              onPressed: () {
                appState.remove(widget.existing!);
                Navigator.of(context).pop();
              },
            ),
        ],
      ),
      body: PrismBackground(
        child: SafeArea(
          child: ListView(
            padding: const EdgeInsets.fromLTRB(18, 8, 18, 120),
            children: [
              GlassCard(
                child: TextField(
                  controller: _title,
                  autofocus: !_isEditing,
                  style: const TextStyle(
                      color: Palette.ink,
                      fontSize: 18,
                      fontWeight: FontWeight.w600),
                  cursorColor: Palette.coral,
                  decoration: const InputDecoration(
                    border: InputBorder.none,
                    hintText: 'What should I remind you?',
                    hintStyle: TextStyle(color: Palette.inkFaint),
                  ),
                ),
              ),
              const SizedBox(height: 14),
              GlassCard(
                padding: EdgeInsets.zero,
                child: Column(
                  children: [
                    _row(
                      icon: Icons.calendar_today_outlined,
                      label: 'Date',
                      value: formatWhen(_when).split(' · ').first,
                      onTap: _pickDate,
                    ),
                    Divider(color: Glass.border, height: 1),
                    _row(
                      icon: Icons.access_time,
                      label: 'Time',
                      value: TimeOfDay.fromDateTime(_when).format(context),
                      onTap: _pickTime,
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 22),
              _sectionLabel('Repeat'),
              const SizedBox(height: 10),
              Wrap(
                spacing: 10,
                runSpacing: 10,
                children: [
                  for (final r in Repeat.values)
                    CrystalChip(
                      label: r.label,
                      color: Palette.crystal(_colorIndex),
                      selected: _repeat == r,
                      onTap: () => setState(() => _repeat = r),
                    ),
                ],
              ),
              const SizedBox(height: 22),
              _sectionLabel('Colour'),
              const SizedBox(height: 10),
              Wrap(
                spacing: 14,
                runSpacing: 12,
                children: [
                  for (var i = 0; i < Palette.crystals.length; i++)
                    CrystalSwatch(
                      color: Palette.crystals[i],
                      selected: _colorIndex == i,
                      onTap: () => setState(() => _colorIndex = i),
                    ),
                ],
              ),
              const SizedBox(height: 22),
              GlassCard(
                child: Row(
                  children: [
                    const Icon(Icons.volume_up_outlined,
                        color: Palette.inkSoft),
                    const SizedBox(width: 12),
                    const Expanded(
                      child: Text('Play sound',
                          style: TextStyle(
                              color: Palette.ink,
                              fontSize: 15,
                              fontWeight: FontWeight.w500)),
                    ),
                    Switch(
                      value: _sound,
                      activeColor: Palette.coral,
                      onChanged: (v) => setState(() => _sound = v),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 28),
              FilledButton(
                style: FilledButton.styleFrom(
                  backgroundColor: Palette.coral,
                  foregroundColor: const Color(0xFF2A1209),
                  minimumSize: const Size.fromHeight(54),
                  shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(18)),
                ),
                onPressed: _save,
                child: Text(_isEditing ? 'Save' : 'Schedule reminder',
                    style: const TextStyle(
                        fontSize: 16, fontWeight: FontWeight.w700)),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _sectionLabel(String text) => Padding(
        padding: const EdgeInsets.only(left: 4),
        child: Text(
          text.toUpperCase(),
          style: const TextStyle(
            color: Palette.inkFaint,
            fontSize: 12,
            fontWeight: FontWeight.w700,
            letterSpacing: 1.2,
          ),
        ),
      );

  Widget _row({
    required IconData icon,
    required String label,
    required String value,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 18),
        child: Row(
          children: [
            Icon(icon, color: Palette.inkSoft, size: 20),
            const SizedBox(width: 12),
            Text(label,
                style: const TextStyle(
                    color: Palette.ink,
                    fontSize: 15,
                    fontWeight: FontWeight.w500)),
            const Spacer(),
            Text(value,
                style: const TextStyle(
                    color: Palette.coralSoft,
                    fontSize: 15,
                    fontWeight: FontWeight.w600)),
            const SizedBox(width: 6),
            const Icon(Icons.chevron_right, color: Palette.inkFaint, size: 20),
          ],
        ),
      ),
    );
  }
}
