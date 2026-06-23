import 'package:flutter/material.dart';

import 'screens/home_screen.dart';
import 'state/app_state.dart';
import 'theme.dart';

/// Global app state. Simple and dependency-free (no provider package).
final AppState appState = AppState();

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // Paint the UI FIRST, then initialise. Loading data, requesting permissions,
  // or starting sync must never block startup — doing so (and awaiting it before
  // runApp) left Android on a black screen while the permission dialog waited.
  // init() is now fire-and-forget and self-healing.
  runApp(const PrismApp());
  WidgetsBinding.instance.addPostFrameCallback((_) => appState.init());
}

class PrismApp extends StatelessWidget {
  const PrismApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Prism',
      debugShowCheckedModeBanner: false,
      theme: PrismTheme.theme(),
      home: const HomeScreen(),
    );
  }
}
