"""Font style configuration using standard system fonts."""

# Font style configuration - using standard sizes
FONTS = {
    "family_ui": "TkDefaultFont",  # System default font
    "family_mono": "TkFixedFont",  # System monospace font
    "size_tiny": 9,
    "size_small": 9,
    "size_normal": 10,
    "size_medium": 11,
    "size_large": 12,
    "size_huge": 24,  # Reduced from 48 for less flashy display
    # Predefined font tuples for common use cases
    "tooltip": ("TkDefaultFont", 9),
    "button": ("TkDefaultFont", 10),
    "button_small": ("TkDefaultFont", 9),
    "label": ("TkDefaultFont", 10),
    "label_small": ("TkDefaultFont", 9),
    "label_medium": ("TkDefaultFont", 10, "bold"),
    "entry": ("TkDefaultFont", 10),
    "entry_large": ("TkDefaultFont", 10),
    "heading": ("TkDefaultFont", 10, "bold"),
    "time_display": ("TkDefaultFont", 24, "bold"),  # Reduced from 48
    "date_time": ("TkDefaultFont", 9),
    "log": ("TkFixedFont", 9),
    "log_bold": ("TkFixedFont", 9, "bold"),
}
