"""GUI styles package - colors, fonts, sizes, and theming."""

from .colors import COLORS
from .fonts import FONTS
from .sizes import STANDARD_WIDGET_HEIGHT, ENTRY_PADY, BUTTON_PADY, TAB_CONTENT_PADX
from .theme import apply_theme

__all__ = ["COLORS", "FONTS", "STANDARD_WIDGET_HEIGHT", "ENTRY_PADY", "BUTTON_PADY", "TAB_CONTENT_PADX", "apply_theme"]
