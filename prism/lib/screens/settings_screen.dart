import 'dart:math';

import 'package:flutter/material.dart';

import '../main.dart';
import '../theme.dart';
import '../widgets/glass.dart';
import '../widgets/prism_background.dart';

class SettingsScreen extends StatefulWidget {
  const SettingsScreen({super.key});

  @override
  State<SettingsScreen> createState() => _SettingsScreenState();
}

class _SettingsScreenState extends State<SettingsScreen> {
  late final TextEditingController _url;
  late final TextEditingController _code;

  @override
  void initState() {
    super.initState();
    _url = TextEditingController(text: appState.dbUrl);
    _code = TextEditingController(text: appState.syncCode);
  }

  @override
  void dispose() {
    _url.dispose();
    _code.dispose();
    super.dispose();
  }

  void _generateCode() {
    const chars = 'abcdefghijkmnpqrstuvwxyz23456789';
    final rng = Random.secure();
    final code =
        List.generate(14, (_) => chars[rng.nextInt(chars.length)]).join();
    setState(() => _code.text = code);
  }

  Future<void> _saveSync() async {
    await appState.saveSyncSettings(_url.text, _code.text);
    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        backgroundColor: Palette.bg1,
        content: Text(
          appState.cloudConfigured
              ? 'Sync on. Use the same URL + code on your other device.'
              : 'Sync off — running local only.',
          style: const TextStyle(color: Palette.ink),
        ),
      ),
    );
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
        title: const Text('Settings'),
      ),
      body: PrismBackground(
        child: SafeArea(
          child: ListView(
            padding: const EdgeInsets.fromLTRB(18, 8, 18, 60),
            children: [
              _label('Cross-device sync'),
              const SizedBox(height: 10),
              GlassCard(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    const Text(
                      'Paste your Firebase Realtime Database URL, then set the '
                      'same sync code on both devices. See FIREBASE_SETUP.md.',
                      style: TextStyle(
                          color: Palette.inkSoft, fontSize: 13, height: 1.4),
                    ),
                    const SizedBox(height: 16),
                    _field(
                      controller: _url,
                      hint: 'https://your-app-default-rtdb.firebaseio.com',
                      icon: Icons.cloud_outlined,
                    ),
                    const SizedBox(height: 12),
                    _field(
                      controller: _code,
                      hint: 'sync code (same on both devices)',
                      icon: Icons.key_outlined,
                    ),
                    const SizedBox(height: 10),
                    Align(
                      alignment: Alignment.centerLeft,
                      child: TextButton.icon(
                        onPressed: _generateCode,
                        icon: const Icon(Icons.casino_outlined,
                            size: 18, color: Palette.coralSoft),
                        label: const Text('Generate code',
                            style: TextStyle(color: Palette.coralSoft)),
                      ),
                    ),
                    const SizedBox(height: 4),
                    FilledButton(
                      style: FilledButton.styleFrom(
                        backgroundColor: Palette.coral,
                        foregroundColor: const Color(0xFF2A1209),
                        minimumSize: const Size.fromHeight(50),
                        shape: RoundedRectangleBorder(
                            borderRadius: BorderRadius.circular(16)),
                      ),
                      onPressed: _saveSync,
                      child: const Text('Save sync settings',
                          style: TextStyle(fontWeight: FontWeight.w700)),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 24),
              _label('Permissions & test'),
              const SizedBox(height: 10),
              GlassCard(
                child: Column(
                  children: [
                    _action(
                      icon: Icons.notifications_active_outlined,
                      title: 'Grant notification permission',
                      subtitle: 'Allow alerts, sound and exact timing',
                      onTap: () => appState.notifications.requestPermissions(),
                    ),
                    Divider(color: Glass.border, height: 22),
                    _action(
                      icon: Icons.campaign_outlined,
                      title: 'Send a test notification',
                      subtitle: 'Check the sound plays on this device',
                      onTap: () => appState.notifications.showTest(),
                    ),
                  ],
                ),
              ),
              const SizedBox(height: 24),
              _label('Vivo V27 — keep reminders firing'),
              const SizedBox(height: 10),
              GlassCard(
                edgeTint: Palette.crystals[1],
                child: const Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Text(
                      'FunTouch OS aggressively closes background apps, which can '
                      'silence scheduled reminders. One-time fix on the Vivo:',
                      style: TextStyle(
                          color: Palette.inkSoft, fontSize: 13, height: 1.45),
                    ),
                    SizedBox(height: 12),
                    _Step('Settings → Battery → High background power usage → '
                        'enable Prism'),
                    _Step('Settings → Apps → Auto-start manager → allow Prism'),
                    _Step('Long-press Prism in Recents → lock it (padlock) so it '
                        'is not cleared'),
                  ],
                ),
              ),
              const SizedBox(height: 24),
              const Center(
                child: Text('Prism · local-first synced reminders',
                    style: TextStyle(color: Palette.inkFaint, fontSize: 12)),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _label(String text) => Padding(
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

  Widget _field({
    required TextEditingController controller,
    required String hint,
    required IconData icon,
  }) {
    return Container(
      decoration: BoxDecoration(
        borderRadius: BorderRadius.circular(14),
        color: Colors.white.withOpacity(0.05),
        border: Border.all(color: Glass.border),
      ),
      padding: const EdgeInsets.symmetric(horizontal: 12),
      child: Row(
        children: [
          Icon(icon, color: Palette.inkFaint, size: 18),
          const SizedBox(width: 10),
          Expanded(
            child: TextField(
              controller: controller,
              style: const TextStyle(color: Palette.ink, fontSize: 14),
              cursorColor: Palette.coral,
              decoration: InputDecoration(
                border: InputBorder.none,
                isDense: true,
                contentPadding: const EdgeInsets.symmetric(vertical: 14),
                hintText: hint,
                hintStyle:
                    const TextStyle(color: Palette.inkFaint, fontSize: 13),
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _action({
    required IconData icon,
    required String title,
    required String subtitle,
    required VoidCallback onTap,
  }) {
    return InkWell(
      onTap: onTap,
      borderRadius: BorderRadius.circular(12),
      child: Row(
        children: [
          Icon(icon, color: Palette.coralSoft),
          const SizedBox(width: 14),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(title,
                    style: const TextStyle(
                        color: Palette.ink,
                        fontSize: 15,
                        fontWeight: FontWeight.w600)),
                const SizedBox(height: 2),
                Text(subtitle,
                    style: const TextStyle(
                        color: Palette.inkSoft, fontSize: 12.5)),
              ],
            ),
          ),
          const Icon(Icons.chevron_right, color: Palette.inkFaint),
        ],
      ),
    );
  }
}

class _Step extends StatelessWidget {
  final String text;
  const _Step(this.text);

  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 8),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Padding(
            padding: EdgeInsets.only(top: 6, right: 10),
            child: Icon(Icons.check_circle_outline,
                size: 15, color: Palette.crystals[1]),
          ),
          Expanded(
            child: Text(text,
                style: const TextStyle(
                    color: Palette.ink, fontSize: 13.5, height: 1.4)),
          ),
        ],
      ),
    );
  }
}
