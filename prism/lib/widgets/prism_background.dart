import 'package:flutter/material.dart';

import '../theme.dart';

/// Warm gradient canvas with soft "crystal" light blooms behind the glass.
class PrismBackground extends StatelessWidget {
  final Widget child;
  const PrismBackground({super.key, required this.child});

  @override
  Widget build(BuildContext context) {
    return DecoratedBox(
      decoration: const BoxDecoration(
        gradient: LinearGradient(
          begin: Alignment.topCenter,
          end: Alignment.bottomCenter,
          colors: [Palette.bg1, Palette.bg0],
        ),
      ),
      child: Stack(
        children: [
          Positioned(
            top: -90,
            left: -70,
            child: _bloom(Palette.coral, 280),
          ),
          Positioned(
            top: 180,
            right: -120,
            child: _bloom(Palette.crystals[2], 260),
          ),
          Positioned(
            bottom: -120,
            right: -60,
            child: _bloom(Palette.crystals[3], 320),
          ),
          Positioned(
            bottom: 60,
            left: -110,
            child: _bloom(Palette.crystals[4], 240),
          ),
          child,
        ],
      ),
    );
  }

  Widget _bloom(Color color, double size) {
    return IgnorePointer(
      child: Container(
        width: size,
        height: size,
        decoration: BoxDecoration(
          shape: BoxShape.circle,
          gradient: RadialGradient(
            colors: [color.withOpacity(0.30), color.withOpacity(0.0)],
          ),
        ),
      ),
    );
  }
}
