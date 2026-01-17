"""Main GUI class for IC256 data collection application.

This module provides the main GUI class that orchestrates all components,
tabs, and utilities into a cohesive application interface.
"""

import os
import sys
import tkinter as tk
from tkinter import ttk
import threading
from datetime import datetime
from typing import Optional, Dict

from .styles import COLORS, FONTS, apply_theme
from .components import ToolTip
from .tabs import MainTab, SettingsTab, LogTab
from .utils import ImageLoader, WindowStateManager
from ..utils import is_valid_device


class GUI:
    """Main GUI class for IC256 data collection application."""
    
    def __init__(self, name: str):
        """Initialize the GUI application.
        
        Args:
            name: Application name/title
        """
        self.root = tk.Tk()
        self.root.title(name)
        self.root.resizable(True, True)
        self.root.minsize(800, 600)
        
        # Set system background color
        self.root.configure(bg=COLORS["background"])
        
        # Initialize utilities
        self.image_loader = ImageLoader()
        self.window_state = WindowStateManager(self.root)
        
        # Set window icon
        self.image_loader.set_window_icon(self.root)
        
        # Load commonly used images
        self._load_images()
        
        # Bind keyboard shortcuts
        self._setup_keyboard_shortcuts()
        
        # Load and apply window state
        self.window_state.load()
        
        # Create data directory if needed
        self._ensure_data_directory()
    
    def _load_images(self):
        """Load commonly used images."""
        self.fail_image = self.image_loader.load_image("fail.png", (20, 20))
        self.loading_image = self.image_loader.load_image("loading.png", (15, 15))
        self.pass_image = self.image_loader.load_image("pass.png", (20, 20))
        self.search_image = self.image_loader.load_image("search.png", (13, 13))
        self.open_folder_image = self.image_loader.load_image("open_folder.png", (13, 13))
    
    def _ensure_data_directory(self):
        """Ensure data directory exists."""
        if hasattr(sys, "_MEIPASS"):
            data_collection_dir = os.path.join(sys._MEIPASS, "Data")
        else:
            package_dir = os.path.dirname(os.path.dirname(__file__))
            project_root = os.path.dirname(package_dir)
            data_collection_dir = os.path.join(project_root, "data")
        
        if not os.path.exists(data_collection_dir):
            os.makedirs(data_collection_dir)
    
    def _setup_keyboard_shortcuts(self):
        """Set up keyboard shortcuts for common actions."""
        # F9: Start collection
        self.root.bind('<F9>', lambda e: self.start() if hasattr(self, 'main_tab') and hasattr(self.main_tab, 'start_button') and self.main_tab.start_button['state'] == 'normal' else None)
        
        # F10 or Esc: Stop collection
        self.root.bind('<F10>', lambda e: self.stop() if hasattr(self, 'main_tab') and hasattr(self.main_tab, 'stop_button') and self.main_tab.stop_button['state'] == 'normal' else None)
        self.root.bind('<Escape>', lambda e: self.stop() if hasattr(self, 'main_tab') and hasattr(self.main_tab, 'stop_button') and self.main_tab.stop_button['state'] == 'normal' else None)
        
        # Ctrl+Shift+S: Export log
        self.root.bind('<Control-Shift-S>', lambda e: self.log_tab._export_log() if hasattr(self, 'log_tab') else None)
        
        # Ctrl+F: Focus search in log tab
        self.root.bind('<Control-f>', lambda e: self.log_tab._focus_log_search() if hasattr(self, 'log_tab') else None)
    
    # Placeholder methods - to be overridden by application
    def start(self):
        """Start data collection (override in application.py)."""
        print("GUI: Start")
    
    def stop(self):
        """Stop data collection (override in application.py)."""
        print("GUI: Stop")
    
    def set_up_device(self):
        """Set up device configuration (override in application.py)."""
        print("GUI: Set up device")
    
    def _update_icon(self, button: tk.Button, entry: tk.Entry, device_name: str):
        """Update device validation icon.
        
        Args:
            button: Button widget to update
            entry: Entry widget containing IP address
            device_name: Name of device type to validate
        """
        button.config(image=self.loading_image, state="disabled")
        if is_valid_device(entry.get(), device_name):
            button.config(state="normal", image=self.pass_image)
        else:
            button.config(state="normal", image=self.fail_image)
    
    def _update_icon_threaded(self, button: tk.Button, entry: tk.Entry, device_name: str):
        """Update device validation icon in background thread."""
        thread = threading.Thread(
            target=self._update_icon,
            args=(button, entry, device_name),
            name=f"update_{device_name.lower()}_icon",
            daemon=True,
        )
        thread.start()
    
    def render(self):
        """Render all GUI components and start the main loop."""
        # Configure root grid weights for proper resizing
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_rowconfigure(1, weight=0)
        self.root.grid_columnconfigure(0, weight=1)
        
        # Create tab frame
        tab_frame = tk.Frame(self.root, bg=COLORS["background"])
        tab_frame.grid(row=0, column=0, sticky="nsew")
        tab_frame.grid_rowconfigure(1, weight=1)  # Notebook row
        tab_frame.grid_columnconfigure(0, weight=1)
        
        # Connection status indicator in top left corner
        self._create_connection_status(tab_frame)
        
        # Create notebook
        self.tab = ttk.Notebook(tab_frame)
        self.tab.grid(row=1, column=0, sticky="nsew", padx=2, pady=2)
        
        # Apply theme
        apply_theme(self.tab)
        
        # Bind tab change event
        self.tab.bind("<<NotebookTabChanged>>", self._on_tab_click)
        
        # Create tabs
        main_tab_frame = ttk.Frame(self.tab)
        self.tab.add(main_tab_frame, text="Main")
        self.main_tab = MainTab(main_tab_frame, self.start, self.stop, self.image_loader)
        
        settings_tab_frame = ttk.Frame(self.tab)
        self.tab.add(settings_tab_frame, text="Settings")
        self.setting_tab = SettingsTab(
            settings_tab_frame,
            self.set_up_device,
            self.image_loader,
            self._update_icon_threaded
        )
        
        log_tab_frame = ttk.Frame(self.tab)
        self.tab.add(log_tab_frame, text="Log")
        self.log_tab = LogTab(log_tab_frame, self.show_message)
        
        # Create message frame
        self._create_message_frame()
        
        # Render date/time display
        self._render_date_time()
        
        # Set up window close handler
        self.window_state.setup_close_handler(self._on_window_close)
        
        self.root.update_idletasks()
        self.root.mainloop()
    
    def _create_connection_status(self, parent: tk.Widget):
        """Create connection status indicator."""
        connection_frame = tk.Frame(parent, bg=COLORS["background"])
        connection_frame.grid(row=0, column=0, sticky="nw", padx=10, pady=5)
        
        self.connection_status_label = tk.Label(
            connection_frame,
            font=FONTS["date_time"],
            fg=COLORS["text_secondary"],
            bg=COLORS["background"],
            text="●",
            cursor="hand2"
        )
        self.connection_status_label.pack(side=tk.LEFT, padx=(0, 5))
        ToolTip(self.connection_status_label, "Device connection status indicator", 0, 20)
        
        self.connection_status_text = tk.Label(
            connection_frame,
            font=("TkDefaultFont", 9),
            fg=COLORS["text_secondary"],
            bg=COLORS["background"],
            text="",
            cursor="hand2"
        )
        self.connection_status_text.pack(side=tk.LEFT)
        ToolTip(self.connection_status_text, "Click to view detailed connection status", 0, 20)
    
    def _create_message_frame(self):
        """Create message status bar."""
        message_frame = tk.Frame(
            self.root, 
            height=30,
            bg=COLORS["background"],
            relief="flat"
        )
        message_frame.grid(row=1, column=0, sticky="ew")
        message_frame.grid_propagate(False)
        message_frame.grid_columnconfigure(0, weight=1)
        
        self.message_text = tk.Label(
            message_frame,
            font=FONTS["label_small"],
            anchor="w",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            wraplength=580
        )
        self.message_text.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        self.message_frame = message_frame
    
    def _render_date_time(self):
        """Render date/time display in tab header."""
        header_frame = tk.Frame(self.tab, bg=COLORS["background"])
        header_frame.pack(side=tk.RIGHT, padx=10, pady=3)
        
        self.display_time = tk.Label(
            header_frame,
            font=FONTS["date_time"],
            fg=COLORS["text_primary"],
            bg=COLORS["background"]
        )
        self.display_time.pack(side=tk.LEFT)
        self.update_date_time()
    
    def _on_tab_click(self, event):
        """Handle tab click to prevent focus issues."""
        event.widget.master.focus_set()
    
    def _on_window_close(self):
        """Handle window close event."""
        if hasattr(self, 'on_close'):
            self.on_close()
        else:
            self.root.quit()
    
    def update_date_time(self):
        """Update the date/time display."""
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M:%S")
        self.display_time.config(text=f"{current_date} {current_time}")
        self.root.after(1000, self.update_date_time)
    
    def update_connection_status(self, status_dict: Dict[str, str]) -> None:
        """Update connection status display.
        
        Args:
            status_dict: Dictionary mapping device names to status strings
                        ("connected", "disconnected", "error")
        """
        if not hasattr(self, 'connection_status_label'):
            return
        
        try:
            if not status_dict:
                self.connection_status_label.config(text="●", fg=COLORS["text_secondary"])
                if hasattr(self, 'connection_status_text'):
                    self.connection_status_text.config(text="")
                return
            
            # Determine overall status
            all_connected = all(status == "connected" for status in status_dict.values())
            any_error = any(status == "error" for status in status_dict.values())
            any_disconnected = any(status == "disconnected" for status in status_dict.values())
            
            # Set indicator color
            if any_error:
                status_color = COLORS["error"]
            elif all_connected:
                status_color = COLORS["success"]
            elif any_disconnected:
                status_color = COLORS["warning"]
            else:
                status_color = COLORS["text_secondary"]
            
            self.connection_status_label.config(text="●", fg=status_color)
            
            # Build status text
            if hasattr(self, 'connection_status_text'):
                status_parts = []
                for device_name, status in status_dict.items():
                    if status == "connected":
                        status_parts.append(f"{device_name} ✓")
                    elif status == "error":
                        status_parts.append(f"{device_name} ✗")
                    else:
                        status_parts.append(f"{device_name} ○")
                
                status_text = " | ".join(status_parts)
                self.connection_status_text.config(text=status_text)
        except Exception as e:
            print(f"Error updating connection status: {e}")
    
    def show_message(self, message: str, fg_color: str = "black"):
        """Show a message in the status bar.
        
        Args:
            message: Message text to display
            fg_color: Foreground color (can be color name or level)
        """
        color_map = {
            "red": COLORS["error"],
            "green": COLORS["success"],
            "orange": COLORS["warning"],
            "blue": COLORS["primary"],
            "black": COLORS["text_primary"],
        }
        display_color = color_map.get(fg_color.lower(), fg_color)
        
        self.message_text.config(text=message, fg=display_color)
        
        # Update wraplength based on current window width
        try:
            self.root.update_idletasks()
            window_width = self.root.winfo_width()
            if window_width > 1:
                self.message_text.config(wraplength=max(100, window_width - 30))
        except (tk.TclError, AttributeError, RuntimeError):
            pass
        
        # Also log to log tab
        if hasattr(self, 'log_tab'):
            self.log_tab.log_message(message, fg_color)
    
    def hide_message(self):
        """Hide the status message."""
        self.message_text.config(text="")
    
    def log_message(self, message: str, level: str = "INFO"):
        """Add a message to the log tab.
        
        Args:
            message: The log message
            level: Log level - "INFO", "WARNING", "ERROR", or color string
        """
        if hasattr(self, 'log_tab'):
            self.log_tab.log_message(message, level)
    
    # Delegate methods to main_tab for backward compatibility
    def update_elapse_time(self, minute: str, second: str, ticks: str):
        """Update elapsed time display."""
        if hasattr(self, 'main_tab'):
            self.main_tab.update_elapse_time(minute, second, ticks)
    
    def reset_elapse_time(self):
        """Reset elapsed time display to zero."""
        if hasattr(self, 'main_tab'):
            self.main_tab.reset_elapse_time()
    
    def update_statistics(self, rows: int, file_size: str):
        """Update statistics display."""
        if hasattr(self, 'main_tab'):
            self.main_tab.update_statistics(rows, file_size)
    
    def reset_statistics(self):
        """Reset statistics display to zero."""
        if hasattr(self, 'main_tab'):
            self.main_tab.reset_statistics()
    
    def get_note_value(self) -> str:
        """Get note entry value, returning empty string if placeholder is present."""
        if hasattr(self, 'main_tab'):
            return self.main_tab.get_note_value()
        return ""
    
    # Expose settings tab entries for backward compatibility
    @property
    def ix256_a_entry(self):
        """Get IC256 entry widget."""
        return self.setting_tab.ix256_a_entry
    
    @property
    def tx2_entry(self):
        """Get TX2 entry widget."""
        return self.setting_tab.tx2_entry
    
    @property
    def path_entry(self):
        """Get path entry widget."""
        return self.setting_tab.path_entry
    
    @property
    def sampling_entry(self):
        """Get sampling rate entry widget."""
        return self.setting_tab.sampling_entry
    
    @property
    def note_entry(self):
        """Get note entry widget."""
        return self.main_tab.note_entry
    
    @property
    def start_button(self):
        """Get start button widget."""
        return self.main_tab.start_button
    
    @property
    def stop_button(self):
        """Get stop button widget."""
        return self.main_tab.stop_button
