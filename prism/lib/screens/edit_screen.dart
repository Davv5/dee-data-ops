import 'dart:io' show Platform;

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
  double get _topInset => Platform.isMacOS ? 22 : 0;

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

  void _setWhen(DateTime dt) => setState(() => _when = dt);

  void _preset(Duration d) => _setWhen(DateTime.now().add(d));

  void _presetClock(int hour, {int addDays = 0}) {
    final now = DateTime.now();
    var dt = DateTime(now.year, now.month, now.day, hour, 0)
        .add(Duration(days: addDays));
    if (dt.isBefore(now)) dt = dt.add(const Duration(days: 1));
    _setWhen(dt);
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
      _setWhen(DateTime(d.year, d.month, d.day, _when.hour, _when.minute));
    }
  }

  Future<void> _pickTime() async {
    final t = await showTimePicker(
      context: context,
      initialTime: TimeOfDay.fromDateTime(_when),
      builder: _pickerTheme,
    );
    if (t != null) {
      _setWhen(DateTime(_when.year, _when.month, _when.day, t.hour, t.minute));
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

  Future<void> _editLabels() async {
    await showModalBottomSheet<void>(
      context: context,
      backgroundColor: Palette.bg1,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(22)),
      ),
      builder: (_) => const _LabelEditorSheet(),
    );
    if (mounted) setState(() {});
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Palette.bg0,
      extendBodyBehindAppBar: true,
      body: PrismBackground(
        child: SafeArea(
          bottom: false,
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              Padding(
                padding: EdgeInsets.only(top: _topInset),
                child: _TopBar(
                  title: _isEditing ? 'Edit reminder' : 'New reminder',
                  onDelete: _isEditing
                      ? () {
                          appState.remove(widget.existing!);
                          Navigator.of(context).pop();
                        }
                      : null,
                ),
              ),
              Expanded(
                child: ListView(
                  padding: const EdgeInsets.fromLTRB(18, 4, 18, 40),
                  children: [
                    GlassCard(
                      child: TextField(
                        controller: _title,
                        autofocus: !_isEditing,
                        textCapitalization: TextCapitalization.sentences,
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
                        onSubmitted: (_) => _save(),
                      ),
                    ),
                    const SizedBox(height: 18),
                    _sectionLabel('Quick set'),
                    const SizedBox(height: 10),
                    Wrap(
                      spacing: 9,
                      runSpacing: 9,
                      children: [
                        _preChip('+30 min', () => _preset(const Duration(minutes: 30))),
                        _preChip('+1 hour', () => _preset(const Duration(hours: 1))),
                        _preChip('+3 hours', () => _preset(const Duration(hours: 3))),
                        _preChip('Tonight 8pm', () => _presetClock(20)),
                        _preChip('Tomorrow 9am', () => _presetClock(9, addDays: 1)),
                      ],
                    ),
                    const SizedBox(height: 18),
                    GlassCard(
                      padding: EdgeInsets.zero,
                      child: Column(
                        children: [
                          _row(
                            icon: Icons.calendar_today_rounded,
                            label: 'Date',
                            value: formatWhen(_when).split(' · ').first,
                            onTap: _pickDate,
                          ),
                          Divider(color: Glass.border, height: 1),
                          _row(
                            icon: Icons.access_time_rounded,
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
                      spacing: 9,
                      runSpacing: 9,
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
                    Row(
                      children: [
                        Expanded(child: _sectionLabel('Label & colour')),
                        GestureDetector(
                          onTap: _editLabels,
                          child: const Padding(
                            padding: EdgeInsets.only(right: 4, bottom: 2),
                            child: Row(
                              children: [
                                Icon(Icons.edit_rounded,
                                    size: 13, color: Palette.coralSoft),
                                SizedBox(width: 5),
                                Text('Rename',
                                    style: TextStyle(
                                        color: Palette.coralSoft,
                                        fontSize: 12.5,
                                        fontWeight: FontWeight.w600)),
                              ],
                            ),
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 12),
                    Wrap(
                      spacing: 10,
                      runSpacing: 12,
                      children: [
                        for (var i = 0; i < Palette.crystals.length; i++)
                          _LabelSwatch(
                            color: Palette.crystals[i],
                            label: appState.labelFor(i),
                            selected: _colorIndex == i,
                            onTap: () => setState(() => _colorIndex = i),
                          ),
                      ],
                    ),
                    const SizedBox(height: 22),
                    GlassCard(
                      child: Row(
                        children: [
                          const Icon(Icons.graphic_eq_rounded,
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
                  ],
                ),
              ),
              Padding(
                padding: const EdgeInsets.fromLTRB(18, 6, 18, 16),
                child: FilledButton(
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
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _preChip(String label, VoidCallback onTap) {
    return CrystalChip(
      label: label,
      color: Palette.coral,
      selected: false,
      onTap: onTap,
    );
  }

  Widget _sectionLabel(String text) => Padding(
        padding: const EdgeInsets.only(left: 4),
        child: Text(
          text.toUpperCase(),
          style: const TextStyle(
            color: Palette.inkFaint,
            fontSize: 11.5,
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
        padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 17),
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
            const Icon(Icons.chevron_right_rounded,
                color: Palette.inkFaint, size: 20),
          ],
        ),
      ),
    );
  }
}

class _TopBar extends StatelessWidget {
  final String title;
  final VoidCallback? onDelete;
  const _TopBar({required this.title, this.onDelete});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(6, 8, 8, 6),
      child: Row(
        children: [
          IconButton(
            icon: const Icon(Icons.arrow_back_rounded, color: Palette.ink),
            onPressed: () => Navigator.of(context).pop(),
          ),
          Expanded(
            child: Text(
              title,
              style: const TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.w700,
                  color: Palette.ink),
            ),
          ),
          if (onDelete != null)
            IconButton(
              icon: const Icon(Icons.delete_outline_rounded,
                  color: Palette.inkSoft),
              onPressed: onDelete,
            ),
        ],
      ),
    );
  }
}

class _LabelSwatch extends StatelessWidget {
  final Color color;
  final String label;
  final bool selected;
  final VoidCallback onTap;
  const _LabelSwatch({
    required this.color,
    required this.label,
    required this.selected,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 160),
        padding: const EdgeInsets.fromLTRB(10, 8, 13, 8),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(30),
          color: color.withOpacity(selected ? 0.22 : 0.07),
          border: Border.all(
            color: color.withOpacity(selected ? 0.85 : 0.18),
            width: selected ? 1.6 : 1,
          ),
        ),
        child: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Container(
              width: 16,
              height: 16,
              decoration: BoxDecoration(
                shape: BoxShape.circle,
                gradient: RadialGradient(
                  colors: [color, color.withOpacity(0.55)],
                ),
              ),
            ),
            const SizedBox(width: 8),
            Text(
              label,
              style: TextStyle(
                color: selected ? Palette.ink : Palette.inkSoft,
                fontSize: 13,
                fontWeight: selected ? FontWeight.w700 : FontWeight.w500,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// Bottom sheet to rename the six colour labels.
class _LabelEditorSheet extends StatefulWidget {
  const _LabelEditorSheet();

  @override
  State<_LabelEditorSheet> createState() => _LabelEditorSheetState();
}

class _LabelEditorSheetState extends State<_LabelEditorSheet> {
  late final List<TextEditingController> _controllers;

  @override
  void initState() {
    super.initState();
    _controllers = [
      for (var i = 0; i < Palette.crystals.length; i++)
        TextEditingController(text: appState.labelFor(i)),
    ];
  }

  @override
  void dispose() {
    for (final c in _controllers) {
      c.dispose();
    }
    super.dispose();
  }

  void _saveAll() {
    for (var i = 0; i < _controllers.length; i++) {
      appState.setLabel(i, _controllers[i].text);
    }
    Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    final bottom = MediaQuery.of(context).viewInsets.bottom;
    return Padding(
      padding: EdgeInsets.only(bottom: bottom),
      child: SafeArea(
        top: false,
        child: Padding(
          padding: const EdgeInsets.fromLTRB(20, 16, 20, 16),
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Center(
                child: Container(
                  width: 40,
                  height: 4,
                  margin: const EdgeInsets.only(bottom: 16),
                  decoration: BoxDecoration(
                    color: Palette.inkFaint.withOpacity(0.4),
                    borderRadius: BorderRadius.circular(4),
                  ),
                ),
              ),
              const Text('Rename labels',
                  style: TextStyle(
                      color: Palette.ink,
                      fontSize: 18,
                      fontWeight: FontWeight.w700)),
              const SizedBox(height: 4),
              const Text('Give each colour a category that means something to you.',
                  style: TextStyle(color: Palette.inkSoft, fontSize: 13)),
              const SizedBox(height: 16),
              for (var i = 0; i < _controllers.length; i++)
                Padding(
                  padding: const EdgeInsets.only(bottom: 10),
                  child: Row(
                    children: [
                      Container(
                        width: 18,
                        height: 18,
                        decoration: BoxDecoration(
                          shape: BoxShape.circle,
                          gradient: RadialGradient(colors: [
                            Palette.crystals[i],
                            Palette.crystals[i].withOpacity(0.55),
                          ]),
                        ),
                      ),
                      const SizedBox(width: 12),
                      Expanded(
                        child: Container(
                          decoration: BoxDecoration(
                            borderRadius: BorderRadius.circular(12),
                            color: Colors.white.withOpacity(0.05),
                            border: Border.all(color: Glass.border),
                          ),
                          padding: const EdgeInsets.symmetric(horizontal: 12),
                          child: TextField(
                            controller: _controllers[i],
                            style: const TextStyle(
                                color: Palette.ink, fontSize: 14),
                            cursorColor: Palette.coral,
                            textCapitalization: TextCapitalization.words,
                            decoration: const InputDecoration(
                              border: InputBorder.none,
                              isDense: true,
                              contentPadding:
                                  EdgeInsets.symmetric(vertical: 12),
                            ),
                          ),
                        ),
                      ),
                    ],
                  ),
                ),
              const SizedBox(height: 8),
              FilledButton(
                style: FilledButton.styleFrom(
                  backgroundColor: Palette.coral,
                  foregroundColor: const Color(0xFF2A1209),
                  minimumSize: const Size.fromHeight(50),
                  shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(16)),
                ),
                onPressed: _saveAll,
                child: const Text('Done',
                    style: TextStyle(fontWeight: FontWeight.w700)),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
