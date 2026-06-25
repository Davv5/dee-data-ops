import 'dart:io' show Platform;

import 'package:flutter/material.dart';

import '../main.dart';
import '../models/reminder.dart';
import '../theme.dart';
import '../util/format.dart';
import '../widgets/glass.dart';
import '../widgets/prism_background.dart';
import 'edit_screen.dart';
import 'settings_screen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});

  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  bool _selecting = false;
  final Set<String> _selected = <String>{};

  double get _topInset => Platform.isMacOS ? 22 : 0;

  void _enterSelect([String? id]) {
    setState(() {
      _selecting = true;
      if (id != null) _selected.add(id);
    });
  }

  void _exitSelect() {
    setState(() {
      _selecting = false;
      _selected.clear();
    });
  }

  void _toggle(String id) {
    setState(() {
      if (!_selected.remove(id)) _selected.add(id);
      if (_selected.isEmpty) _selecting = false;
    });
  }

  Future<void> _deleteSelected() async {
    final ids = Set<String>.from(_selected);
    _exitSelect();
    await appState.removeMany(ids);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Palette.bg0,
      extendBodyBehindAppBar: true,
      body: PrismBackground(
        child: SafeArea(
          bottom: false,
          child: ListenableBuilder(
            listenable: appState,
            builder: (context, _) {
              final items = appState.reminders;
              // Drop selections that no longer exist.
              _selected.removeWhere((id) => !items.any((r) => r.id == id));
              return Column(
                children: [
                  Padding(
                    padding: EdgeInsets.only(top: _topInset),
                    child: _selecting
                        ? _SelectBar(
                            count: _selected.length,
                            total: items.length,
                            onCancel: _exitSelect,
                            onSelectAll: () => setState(() =>
                                _selected.addAll(items.map((e) => e.id))),
                          )
                        : _Header(onSelect: items.isEmpty ? null : _enterSelect),
                  ),
                  if (!_selecting && appState.passed.isNotEmpty)
                    _PassedBanner(
                      count: appState.passed.length,
                      onClear: appState.clearPassed,
                    ),
                  Expanded(
                    child: RefreshIndicator(
                      color: Palette.coral,
                      backgroundColor: Palette.bg1,
                      onRefresh: appState.refreshNow,
                      child: items.isEmpty
                          ? ListView(
                              physics: const AlwaysScrollableScrollPhysics(),
                              children: const [
                                SizedBox(height: 80),
                                _EmptyState(),
                              ],
                            )
                          : ListView.builder(
                              physics: const AlwaysScrollableScrollPhysics(),
                              padding: const EdgeInsets.fromLTRB(16, 6, 16, 120),
                              itemCount: items.length,
                              itemBuilder: (context, i) => Padding(
                                padding: const EdgeInsets.only(bottom: 10),
                                child: _ReminderTile(
                                  reminder: items[i],
                                  selecting: _selecting,
                                  selected: _selected.contains(items[i].id),
                                  onSelectToggle: () => _toggle(items[i].id),
                                  onLongPress: () => _enterSelect(items[i].id),
                                ),
                              ),
                            ),
                    ),
                  ),
                ],
              );
            },
          ),
        ),
      ),
      floatingActionButton: _selecting
          ? null
          : FloatingActionButton.extended(
              backgroundColor: Palette.coral,
              foregroundColor: const Color(0xFF2A1209),
              elevation: 4,
              icon: const Icon(Icons.add_rounded),
              label: const Text('New',
                  style: TextStyle(fontWeight: FontWeight.w700)),
              onPressed: () => Navigator.of(context).push(
                MaterialPageRoute(builder: (_) => const EditScreen()),
              ),
            ),
      bottomNavigationBar: _selecting
          ? _DeleteBar(
              count: _selected.length,
              onDelete: _selected.isEmpty ? null : _deleteSelected,
              onClearPassed: appState.passed.isEmpty
                  ? null
                  : () {
                      _exitSelect();
                      appState.clearPassed();
                    },
            )
          : null,
    );
  }
}

class _Header extends StatelessWidget {
  final VoidCallback? onSelect;
  const _Header({this.onSelect});

  @override
  Widget build(BuildContext context) {
    final synced = appState.cloudConfigured;
    return Padding(
      padding: const EdgeInsets.fromLTRB(22, 16, 12, 10),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.center,
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Prism',
                  style: TextStyle(
                    fontSize: 32,
                    fontWeight: FontWeight.w700,
                    letterSpacing: -0.5,
                    color: Palette.ink,
                  ),
                ),
                const SizedBox(height: 3),
                Row(
                  children: [
                    Container(
                      width: 7,
                      height: 7,
                      decoration: BoxDecoration(
                        shape: BoxShape.circle,
                        color:
                            synced ? Palette.crystals[2] : Palette.crystals[1],
                      ),
                    ),
                    const SizedBox(width: 6),
                    Text(
                      synced ? 'Synced across devices' : 'Local only',
                      style:
                          const TextStyle(color: Palette.inkSoft, fontSize: 12.5),
                    ),
                  ],
                ),
              ],
            ),
          ),
          if (onSelect != null)
            _RoundIcon(
              icon: Icons.checklist_rounded,
              tooltip: 'Select',
              onTap: onSelect!,
            ),
          _RoundIcon(
            icon: Icons.settings_outlined,
            tooltip: 'Settings',
            onTap: () => Navigator.of(context).push(
              MaterialPageRoute(builder: (_) => const SettingsScreen()),
            ),
          ),
        ],
      ),
    );
  }
}

class _RoundIcon extends StatelessWidget {
  final IconData icon;
  final String tooltip;
  final VoidCallback onTap;
  const _RoundIcon(
      {required this.icon, required this.tooltip, required this.onTap});

  @override
  Widget build(BuildContext context) {
    return IconButton(
      tooltip: tooltip,
      icon: Icon(icon, color: Palette.inkSoft, size: 22),
      onPressed: onTap,
    );
  }
}

class _SelectBar extends StatelessWidget {
  final int count;
  final int total;
  final VoidCallback onCancel;
  final VoidCallback onSelectAll;
  const _SelectBar({
    required this.count,
    required this.total,
    required this.onCancel,
    required this.onSelectAll,
  });

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(8, 14, 12, 10),
      child: Row(
        children: [
          IconButton(
            icon: const Icon(Icons.close_rounded, color: Palette.ink),
            onPressed: onCancel,
          ),
          Expanded(
            child: Text(
              count == 0 ? 'Select tasks' : '$count selected',
              style: const TextStyle(
                  fontSize: 18, fontWeight: FontWeight.w700, color: Palette.ink),
            ),
          ),
          TextButton(
            onPressed: onSelectAll,
            child: Text(
              count == total ? 'All' : 'Select all',
              style: const TextStyle(
                  color: Palette.coralSoft, fontWeight: FontWeight.w600),
            ),
          ),
        ],
      ),
    );
  }
}

class _PassedBanner extends StatelessWidget {
  final int count;
  final VoidCallback onClear;
  const _PassedBanner({required this.count, required this.onClear});

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.fromLTRB(16, 0, 16, 8),
      child: GestureDetector(
        onTap: onClear,
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(14),
            color: Palette.inkFaint.withOpacity(0.08),
            border: Border.all(color: Glass.border),
          ),
          child: Row(
            children: [
              const Icon(Icons.history_rounded,
                  size: 17, color: Palette.inkSoft),
              const SizedBox(width: 10),
              Expanded(
                child: Text(
                  '$count passed ${count == 1 ? "reminder" : "reminders"}',
                  style:
                      const TextStyle(color: Palette.inkSoft, fontSize: 13.5),
                ),
              ),
              const Text('Clear',
                  style: TextStyle(
                      color: Palette.coralSoft,
                      fontSize: 13.5,
                      fontWeight: FontWeight.w700)),
            ],
          ),
        ),
      ),
    );
  }
}

class _DeleteBar extends StatelessWidget {
  final int count;
  final VoidCallback? onDelete;
  final VoidCallback? onClearPassed;
  const _DeleteBar({
    required this.count,
    required this.onDelete,
    required this.onClearPassed,
  });

  @override
  Widget build(BuildContext context) {
    return SafeArea(
      top: false,
      child: Padding(
        padding: const EdgeInsets.fromLTRB(16, 8, 16, 12),
        child: Row(
          children: [
            if (onClearPassed != null) ...[
              Expanded(
                child: OutlinedButton.icon(
                  style: OutlinedButton.styleFrom(
                    foregroundColor: Palette.inkSoft,
                    side: BorderSide(color: Glass.border),
                    minimumSize: const Size.fromHeight(50),
                    shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(16)),
                  ),
                  icon: const Icon(Icons.history_rounded, size: 18),
                  label: const Text('Clear passed'),
                  onPressed: onClearPassed,
                ),
              ),
              const SizedBox(width: 10),
            ],
            Expanded(
              child: FilledButton.icon(
                style: FilledButton.styleFrom(
                  backgroundColor: Palette.crystals[5],
                  foregroundColor: Colors.white,
                  disabledBackgroundColor: Palette.crystals[5].withOpacity(0.25),
                  minimumSize: const Size.fromHeight(50),
                  shape: RoundedRectangleBorder(
                      borderRadius: BorderRadius.circular(16)),
                ),
                icon: const Icon(Icons.delete_outline_rounded, size: 19),
                label: Text(count == 0 ? 'Delete' : 'Delete $count'),
                onPressed: onDelete,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _EmptyState extends StatelessWidget {
  const _EmptyState();

  @override
  Widget build(BuildContext context) {
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(32),
        child: Column(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.notifications_active_outlined,
                size: 54, color: Palette.coral.withOpacity(0.8)),
            const SizedBox(height: 16),
            const Text(
              'No reminders yet',
              style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.w600,
                  color: Palette.ink),
            ),
            const SizedBox(height: 6),
            const Text(
              'Tap New to schedule one. It rings on every\ndevice signed in with the same sync code.',
              textAlign: TextAlign.center,
              style: TextStyle(color: Palette.inkSoft, height: 1.4),
            ),
          ],
        ),
      ),
    );
  }
}

class _ReminderTile extends StatelessWidget {
  final Reminder reminder;
  final bool selecting;
  final bool selected;
  final VoidCallback onSelectToggle;
  final VoidCallback onLongPress;

  const _ReminderTile({
    required this.reminder,
    required this.selecting,
    required this.selected,
    required this.onSelectToggle,
    required this.onLongPress,
  });

  @override
  Widget build(BuildContext context) {
    final color = Palette.crystal(reminder.colorIndex);
    final dim = !reminder.enabled;
    final passed = reminder.repeat == Repeat.none &&
        reminder.dateTime.isBefore(DateTime.now());

    final card = GlassCard(
      edgeTint: selected ? color : null,
      strong: selected,
      onTap: selecting
          ? onSelectToggle
          : () => Navigator.of(context).push(
                MaterialPageRoute(
                    builder: (_) => EditScreen(existing: reminder)),
              ),
      padding: const EdgeInsets.fromLTRB(14, 13, 10, 13),
      child: Row(
        children: [
          if (selecting)
            Padding(
              padding: const EdgeInsets.only(right: 12),
              child: _Check(selected: selected, color: color),
            )
          else
            Container(
              width: 4,
              height: 42,
              margin: const EdgeInsets.only(right: 14),
              decoration: BoxDecoration(
                borderRadius: BorderRadius.circular(4),
                gradient: LinearGradient(
                  begin: Alignment.topCenter,
                  end: Alignment.bottomCenter,
                  colors: [color, color.withOpacity(0.4)],
                ),
              ),
            ),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  reminder.title.isEmpty ? 'Reminder' : reminder.title,
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(
                    fontSize: 16,
                    fontWeight: FontWeight.w600,
                    color: Palette.ink,
                  ),
                ),
                const SizedBox(height: 5),
                Row(
                  children: [
                    Flexible(
                      child: Text(
                        formatWhen(reminder.dateTime),
                        maxLines: 1,
                        overflow: TextOverflow.ellipsis,
                        style: TextStyle(
                            color: passed
                                ? Palette.inkFaint
                                : Palette.inkSoft,
                            fontSize: 12.5),
                      ),
                    ),
                    const SizedBox(width: 7),
                    _Tag(appState.labelFor(reminder.colorIndex), color),
                    if (reminder.repeat != Repeat.none) ...[
                      const SizedBox(width: 6),
                      _Tag(reminder.repeat.label, Palette.inkSoft,
                          subtle: true),
                    ],
                    if (!reminder.sound) ...[
                      const SizedBox(width: 6),
                      const Icon(Icons.volume_off_rounded,
                          size: 13, color: Palette.inkFaint),
                    ],
                  ],
                ),
              ],
            ),
          ),
          if (!selecting)
            if (reminder.enabled && !passed && reminder.repeat == Repeat.none)
              Padding(
                padding: const EdgeInsets.only(left: 6, right: 2),
                child: Text(
                  relativeTo(reminder.dateTime),
                  style: TextStyle(
                      color: color.withOpacity(0.95),
                      fontSize: 12,
                      fontWeight: FontWeight.w700),
                ),
              ),
          if (!selecting)
            Transform.scale(
              scale: 0.85,
              child: Switch(
                value: reminder.enabled,
                activeColor: Palette.coral,
                onChanged: (v) => appState.toggle(reminder, v),
              ),
            ),
        ],
      ),
    );

    return Opacity(
      opacity: dim ? 0.5 : 1,
      child: GestureDetector(
        onLongPress: selecting ? null : onLongPress,
        child: card,
      ),
    );
  }
}

class _Check extends StatelessWidget {
  final bool selected;
  final Color color;
  const _Check({required this.selected, required this.color});

  @override
  Widget build(BuildContext context) {
    return AnimatedContainer(
      duration: const Duration(milliseconds: 150),
      width: 24,
      height: 24,
      decoration: BoxDecoration(
        shape: BoxShape.circle,
        color: selected ? color : Colors.transparent,
        border: Border.all(
          color: selected ? color : Palette.inkFaint,
          width: 2,
        ),
      ),
      child: selected
          ? const Icon(Icons.check_rounded, size: 16, color: Colors.white)
          : null,
    );
  }
}

class _Tag extends StatelessWidget {
  final String text;
  final Color color;
  final bool subtle;
  const _Tag(this.text, this.color, {this.subtle = false});

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 7, vertical: 2),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(7),
        color: color.withOpacity(subtle ? 0.10 : 0.15),
      ),
      child: Text(
        text,
        style: TextStyle(
            color: subtle ? Palette.inkSoft : color,
            fontSize: 10.5,
            fontWeight: FontWeight.w600,
            letterSpacing: 0.2),
      ),
    );
  }
}
