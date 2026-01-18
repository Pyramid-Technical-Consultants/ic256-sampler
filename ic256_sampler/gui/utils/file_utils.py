"""File and directory utility functions for GUI operations."""

import os
import sys
from typing import Optional


def open_directory(path: str) -> bool:
    """Open a directory in the system's default file manager.
    
    Args:
        path: Directory path to open
        
    Returns:
        True if successful, False otherwise
    """
    if not path or not os.path.isdir(path):
        return False
    
    try:
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":  # macOS
            os.system(f'open "{path}"')
        else:  # Linux and other Unix-like systems
            os.system(f'xdg-open "{path}"')
        return True
    except Exception:
        return False


def copy_to_clipboard(root, text: str) -> bool:
    """Copy text to system clipboard.
    
    Args:
        root: Tk root window
        text: Text to copy
        
    Returns:
        True if successful, False otherwise
    """
    try:
        root.clipboard_clear()
        root.clipboard_append(text)
        return True
    except Exception:
        return False
