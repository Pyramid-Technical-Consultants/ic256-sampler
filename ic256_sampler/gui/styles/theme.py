"""Theme configuration for native platform appearance."""

import sys
from tkinter import ttk


def apply_theme(root: ttk.Widget) -> None:
    """Apply native theme based on platform.
    
    Args:
        root: Root widget to apply theme to
    """
    style = ttk.Style()
    
    # Use native theme for the platform
    if sys.platform == "win32":
        if "vista" in style.theme_names():
            style.theme_use("vista")
        elif "xpnative" in style.theme_names():
            style.theme_use("xpnative")
        else:
            style.theme_use("default")
    elif sys.platform == "darwin":
        if "aqua" in style.theme_names():
            style.theme_use("aqua")
        else:
            style.theme_use("default")
    else:
        # Linux - use default
        style.theme_use("default")
    
    # Minimal custom styling to keep native look
    style.configure("TNotebook", borderwidth=1)
    style.configure("TNotebook.Tab", 
                   padding=(12, 6),  # Slightly reduced padding for native look
                   font=("TkDefaultFont", 9))
    
    return style
