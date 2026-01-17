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
        """Load and apply saved window state."""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    
                width = state.get('width', 1000)
                height = state.get('height', 700)
                x = state.get('x', None)
                y = state.get('y', None)
                
                # Validate dimensions
                width = max(800, min(width, self.root.winfo_screenwidth()))
                height = max(600, min(height, self.root.winfo_screenheight()))
                
                self.root.geometry(f"{width}x{height}")
                
                # Set position if valid
                if x is not None and y is not None:
                    # Ensure window is on screen
                    screen_width = self.root.winfo_screenwidth()
                    screen_height = self.root.winfo_screenheight()
                    x = max(0, min(x, screen_width - width))
                    y = max(0, min(y, screen_height - height))
                    self.root.geometry(f"{width}x{height}+{x}+{y}")
        except (json.JSONDecodeError, IOError, OSError, ValueError, tk.TclError):
            # If loading fails, use default size
            self.root.geometry("1000x700")
    
    def save(self) -> None:
        """Save current window state."""
        try:
            state = {
                'width': self.root.winfo_width(),
                'height': self.root.winfo_height(),
                'x': self.root.winfo_x(),
                'y': self.root.winfo_y()
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except (IOError, OSError, tk.TclError):
            # Ignore errors when saving state
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
