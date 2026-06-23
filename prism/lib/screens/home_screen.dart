import 'package:flutter/material.dart';

import '../main.dart';
import '../models/reminder.dart';
import '../theme.dart';
import '../util/format.dart';
import '../widgets/glass.dart';
import '../widgets/prism_background.dart';
import 'edit_screen.dart';
import 'settings_screen.dart';

class HomeScreen extends StatelessWidget {
  const HomeScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      backgroundColor: Palette.bg0,
      extendBodyBehindAppBar: true,
      body: PrismBackground(
        child: SafeArea(
          child: ListenableBuilder(
            listenable: appState,
            builder: (context, _) {
              final items = appState.reminders;
              return RefreshIndicator(
                color: Palette.coral,
                backgroundColor: Palette.bg1,
                onRefresh: appState.refreshNow,
                child: CustomScrollView(
                  physics: const AlwaysScrollableScrollPhysics(),
                  slivers: [
                    SliverToBoxAdapter(child: _Header()),
                    if (items.isEmpty)
                      const SliverFillRemaining(
                        hasScrollBody: false,
                        child: _EmptyState(),
                      )
                    else
                      SliverList.builder(
                        itemCount: items.length,
                        itemBuilder: (context, i) => Padding(
                          padding: EdgeInsets.fromLTRB(
                              18, 7, 18, i == items.length - 1 ? 120 : 7),
                          child: _ReminderTile(reminder: items[i]),
                        ),
                      ),
                  ],
                ),
              );
            },
          ),
        ),
      ),
      floatingActionButton: FloatingActionButton.extended(
        backgroundColor: Palette.coral,
        foregroundColor: const Color(0xFF2A1209),
        elevation: 6,
        icon: const Icon(Icons.add),
        label: const Text('New', style: TextStyle(fontWeight: FontWeight.w700)),
        onPressed: () => Navigator.of(context).push(
          MaterialPageRoute(builder: (_) => const EditScreen()),
        ),
      ),
    );
  }
}

class _Header extends StatelessWidget {
  @override
  Widget build(BuildContext context) {
    final synced = appState.cloudConfigured;
    return Padding(
      padding: const EdgeInsets.fromLTRB(20, 14, 14, 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  'Prism',
                  style: TextStyle(
                    fontSize: 34,
                    fontWeight: FontWeight.w700,
                    letterSpacing: -0.5,
                    color: Palette.ink,
                  ),
                ),
                const SizedBox(height: 2),
                Row(
                  children: [
                    Container(
                      width: 8,
                      height: 8,
                      decoration: BoxDecoration(
                        shape: BoxShape.circle,
                        color: synced
                            ? Palette.crystals[2]
                            : Palette.crystals[1],
                      ),
                    ),
                    const SizedBox(width: 6),
                    Text(
                      synced ? 'Synced across devices' : 'Local only · tap ⚙ to sync',
                      style: const TextStyle(
                          color: Palette.inkSoft, fontSize: 13),
                    ),
                  ],
                ),
              ],
            ),
          ),
          IconButton(
            icon: const Icon(Icons.settings_outlined, color: Palette.inkSoft),
            onPressed: () => Navigator.of(context).push(
              MaterialPageRoute(builder: (_) => const SettingsScreen()),
            ),
          ),
        ],
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
                size: 56, color: Palette.coral.withOpacity(0.8)),
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
  const _ReminderTile({required this.reminder});

  @override
  Widget build(BuildContext context) {
    final color = Palette.crystal(reminder.colorIndex);
    final dim = !reminder.enabled;
    return Dismissible(
      key: ValueKey(reminder.id),
      direction: DismissDirection.endToStart,
      background: Container(
        alignment: Alignment.centerRight,
        padding: const EdgeInsets.only(right: 28),
        decoration: BoxDecoration(
          borderRadius: BorderRadius.circular(Glass.radius),
          color: Palette.crystals[5].withOpacity(0.18),
        ),
        child: const Icon(Icons.delete_outline, color: Palette.ink),
      ),
      onDismissed: (_) => appState.remove(reminder),
      child: Opacity(
        opacity: dim ? 0.55 : 1,
        child: GlassCard(
          edgeTint: color,
          onTap: () => Navigator.of(context).push(
            MaterialPageRoute(builder: (_) => EditScreen(existing: reminder)),
          ),
          padding: const EdgeInsets.fromLTRB(14, 14, 8, 14),
          child: Row(
            children: [
              Container(
                width: 4,
                height: 44,
                decoration: BoxDecoration(
                  borderRadius: BorderRadius.circular(4),
                  gradient: LinearGradient(
                    begin: Alignment.topCenter,
                    end: Alignment.bottomCenter,
                    colors: [color, color.withOpacity(0.4)],
                  ),
                ),
              ),
              const SizedBox(width: 14),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      reminder.title.isEmpty ? 'Reminder' : reminder.title,
                      maxLines: 1,
                      overflow: TextOverflow.ellipsis,
                      style: const TextStyle(
                        fontSize: 16.5,
                        fontWeight: FontWeight.w600,
                        color: Palette.ink,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Row(
                      children: [
                        Text(
                          formatWhen(reminder.dateTime),
                          style: const TextStyle(
                              color: Palette.inkSoft, fontSize: 13),
                        ),
                        if (reminder.repeat != Repeat.none) ...[
                          const SizedBox(width: 8),
                          _Badge(reminder.repeat.label, color),
                        ],
                        if (!reminder.sound) ...[
                          const SizedBox(width: 6),
                          const Icon(Icons.volume_off,
                              size: 14, color: Palette.inkFaint),
                        ],
                      ],
                    ),
                  ],
                ),
              ),
              if (reminder.enabled && reminder.repeat == Repeat.none)
                Padding(
                  padding: const EdgeInsets.only(right: 4),
                  child: Text(
                    relativeTo(reminder.dateTime),
                    style: TextStyle(
                        color: color.withOpacity(0.95),
                        fontSize: 12.5,
                        fontWeight: FontWeight.w600),
                  ),
                ),
              Switch(
                value: reminder.enabled,
                activeColor: Palette.coral,
                onChanged: (v) => appState.toggle(reminder, v),
              ),
            ],
          ),
        ),
      ),
    );
  }
}

class _Badge extends StatelessWidget {
  final String text;
  final Color color;
  const _Badge(this.text, this.color);

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(20),
        color: color.withOpacity(0.16),
        border: Border.all(color: color.withOpacity(0.35)),
      ),
      child: Text(
        text,
        style: TextStyle(
            color: color, fontSize: 11, fontWeight: FontWeight.w600),
      ),
    );
  }
}
