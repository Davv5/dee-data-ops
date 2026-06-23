import 'dart:ui';

import 'package:flutter/material.dart';

import '../theme.dart';

/// A frosted, translucent "crystal" card.
class GlassCard extends StatelessWidget {
  final Widget child;
  final EdgeInsetsGeometry padding;
  final double radius;
  final bool strong;
  final VoidCallback? onTap;
  final Color? edgeTint;

  const GlassCard({
    super.key,
    required this.child,
    this.padding = const EdgeInsets.all(16),
    this.radius = Glass.radius,
    this.strong = false,
    this.onTap,
    this.edgeTint,
  });

  @override
  Widget build(BuildContext context) {
    return ClipRRect(
      borderRadius: BorderRadius.circular(radius),
      child: BackdropFilter(
        filter: ImageFilter.blur(sigmaX: Glass.blur, sigmaY: Glass.blur),
        child: DecoratedBox(
          decoration: BoxDecoration(
            borderRadius: BorderRadius.circular(radius),
            color: strong ? Glass.fillStrong : Glass.fill,
            border: Border.all(
              color: edgeTint?.withOpacity(0.35) ?? Glass.border,
              width: 1,
            ),
            gradient: LinearGradient(
              begin: Alignment.topLeft,
              end: Alignment.bottomRight,
              colors: [Glass.highlight, Colors.white.withOpacity(0.0)],
            ),
          ),
          child: Material(
            color: Colors.transparent,
            child: InkWell(
              onTap: onTap,
              borderRadius: BorderRadius.circular(radius),
              child: Padding(padding: padding, child: child),
            ),
          ),
        ),
      ),
    );
  }
}

/// A translucent, selectable pill used for "crystal" option choices.
class CrystalChip extends StatelessWidget {
  final String label;
  final Color color;
  final bool selected;
  final VoidCallback onTap;

  const CrystalChip({
    super.key,
    required this.label,
    required this.color,
    required this.selected,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: ClipRRect(
        borderRadius: BorderRadius.circular(40),
        child: BackdropFilter(
          filter: ImageFilter.blur(sigmaX: 10, sigmaY: 10),
          child: AnimatedContainer(
            duration: const Duration(milliseconds: 180),
            padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 10),
            decoration: BoxDecoration(
              borderRadius: BorderRadius.circular(40),
              color: color.withOpacity(selected ? 0.28 : 0.08),
              border: Border.all(
                color: color.withOpacity(selected ? 0.85 : 0.22),
                width: 1.2,
              ),
            ),
            child: Text(
              label,
              style: TextStyle(
                color: selected ? Palette.ink : Palette.inkSoft,
                fontWeight: selected ? FontWeight.w600 : FontWeight.w500,
                fontSize: 13.5,
              ),
            ),
          ),
        ),
      ),
    );
  }
}

/// A small translucent color swatch for picking a reminder's accent.
class CrystalSwatch extends StatelessWidget {
  final Color color;
  final bool selected;
  final VoidCallback onTap;

  const CrystalSwatch({
    super.key,
    required this.color,
    required this.selected,
    required this.onTap,
  });

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: onTap,
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 180),
        width: 40,
        height: 40,
        decoration: BoxDecoration(
          shape: BoxShape.circle,
          gradient: RadialGradient(
            colors: [color.withOpacity(0.85), color.withOpacity(0.45)],
          ),
          border: Border.all(
            color: Colors.white.withOpacity(selected ? 0.9 : 0.15),
            width: selected ? 2.4 : 1,
          ),
          boxShadow: selected
              ? [BoxShadow(color: color.withOpacity(0.5), blurRadius: 14)]
              : null,
        ),
      ),
    );
  }
}
