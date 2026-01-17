"""Window state management for saving/restoring window size and position."""

import os
import sys
import json
import pathlib
import tkinter as tk
from typing import Optional, Dict


class WindowStateManager:
    """Manages window state persistence (size and position)."""
    
    def __init__(self, root: tk.Tk):
        """Initialize window state manager.
        
        Args:
            root: Root window to manage
        """
        self.root = root
        self.state_file = self._get_state_file_path()
        self._close_handler_set = False
    
    def _get_state_file_path(self) -> str:
        """Get path to window state file.
        
        Returns:
            Path to state file
        """
        if hasattr(sys, "_MEIPASS"):
            # PyInstaller bundle - use user config directory
            config_dir = pathlib.Path.home() / ".ic256-sampler"
            config_dir.mkdir(parents=True, exist_ok=True)
            return str(config_dir / "window_state.json")
        else:
            # Development mode - use project root
            package_dir = pathlib.Path(__file__).parent.parent.parent
            return str(package_dir / "window_state.json")
    
    def load(self) -> None:
        """Load and apply window state - starts at minimum size by default."""
        try:
            # Start at minimum window size (600x500 as defined in main.py)
            self.root.geometry("600x500")
        except (tk.TclError, AttributeError):
            # If setting geometry fails, use default size
            self.root.geometry("1000x700")
    
    def save(self) -> None:
        """Save current window state - disabled (no-op)."""
        # Window state saving is disabled - do nothing
        pass
    
    def setup_close_handler(self, on_close: Optional[callable] = None) -> None:
        """Set up window close handler to save state.
        
        Args:
            on_close: Optional callback to call on close (before saving state)
        """
        if self._close_handler_set:
            return
        
        def close_handler():
            """Handle window close event - save state and call callback."""
            self.save()
            if on_close:
                on_close()
            else:
                self.root.quit()
        
        self.root.protocol("WM_DELETE_WINDOW", close_handler)
        self._close_handler_set = True
