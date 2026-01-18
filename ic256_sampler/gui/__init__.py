"""GUI package for IC256 data collection application.

This package provides a modular, reusable GUI architecture with:
- styles: Colors, fonts, and theming
- components: Reusable widgets (buttons, entries, tooltips)
- tabs: Tab implementations (main, settings, log)
- utils: Utilities (images, file operations, thread-safe updates)
"""

from .main import GUI

__all__ = ["GUI"]
