"""File and directory utility functions for GUI operations."""

import os
import sys
import tkinter as tk


def open_directory(path: str) -> bool:
    """Open a directory in the system's default file manager."""
    if not path or not os.path.isdir(path):
        return False

    try:
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":
            os.system(f'open "{path}"')
        else:
            os.system(f'xdg-open "{path}"')
        return True
    except Exception:
        return False


def copy_to_clipboard(root: tk.Tk, text: str) -> bool:
    """Copy text to system clipboard."""
    try:
        root.clipboard_clear()
        root.clipboard_append(text)
        return True
    except Exception:
        return False
