"""Theme configuration for native platform appearance."""

import sys
from tkinter import ttk


def apply_theme(root: ttk.Widget) -> None:
    """Apply native theme based on platform."""
    style = ttk.Style()

    if sys.platform == "win32":
        style.theme_use("vista" if "vista" in style.theme_names() else "xpnative" if "xpnative" in style.theme_names() else "default")
    elif sys.platform == "darwin":
        style.theme_use("aqua" if "aqua" in style.theme_names() else "default")
    else:
        style.theme_use("default")

    style.configure("TNotebook", borderwidth=1)
    style.configure("TNotebook.Tab", padding=(12, 6), font=("TkDefaultFont", 9))

    return style
