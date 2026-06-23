import 'package:flutter/material.dart';

/// Claude-inspired warm palette + "crystal" translucent accents.
class Palette {
  // Warm dark canvas (deep clay charcoal).
  static const Color bg0 = Color(0xFF1A1714);
  static const Color bg1 = Color(0xFF241F1B);

  // Claude signature clay/coral.
  static const Color coral = Color(0xFFD97757);
  static const Color coralSoft = Color(0xFFE8A488);

  // Text.
  static const Color ink = Color(0xFFF4EEE6);
  static const Color inkSoft = Color(0xFFB7ADA1);
  static const Color inkFaint = Color(0xFF8A8076);

  /// Translucent "crystal" colors used for option chips / per-reminder tags.
  static const List<Color> crystals = <Color>[
    Color(0xFFD97757), // coral
    Color(0xFFE0A458), // amber
    Color(0xFF6FB7A6), // teal
    Color(0xFF8E86D6), // violet
    Color(0xFF6FA8DC), // sky
    Color(0xFFD98AB0), // rose
  ];

  static Color crystal(int i) => crystals[i % crystals.length];
}

/// Frosted-glass constants for the translucent UI.
class Glass {
  static const double blur = 18;
  static const double radius = 24;

  static Color get fill => Colors.white.withOpacity(0.06);
  static Color get fillStrong => Colors.white.withOpacity(0.11);
  static Color get border => Colors.white.withOpacity(0.13);
  static Color get highlight => Colors.white.withOpacity(0.05);
}

class PrismTheme {
  static ThemeData theme() {
    final base = ThemeData.dark(); // Material 3 is the default on modern Flutter.
    return base.copyWith(
      scaffoldBackgroundColor: Palette.bg0,
      colorScheme: const ColorScheme.dark(
        primary: Palette.coral,
        onPrimary: Color(0xFF2A1209),
        secondary: Palette.coralSoft,
        surface: Palette.bg1,
        onSurface: Palette.ink,
      ),
      textTheme: base.textTheme.apply(
        bodyColor: Palette.ink,
        displayColor: Palette.ink,
      ),
      splashColor: Palette.coral.withOpacity(0.10),
      highlightColor: Palette.coral.withOpacity(0.06),
      iconTheme: const IconThemeData(color: Palette.ink),
    );
  }
}
