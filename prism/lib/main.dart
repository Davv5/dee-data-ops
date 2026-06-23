import 'package:flutter/material.dart';

import 'screens/home_screen.dart';
import 'state/app_state.dart';
import 'theme.dart';

/// Global app state. Simple and dependency-free (no provider package).
final AppState appState = AppState();

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await appState.init();
  runApp(const PrismApp());
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
