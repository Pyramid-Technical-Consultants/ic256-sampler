"""GUI module for IC256 data collection application.

This module provides a modern, responsive Tkinter GUI with centered layouts,
beautiful styling, and comprehensive logging capabilities.
"""
import os
import sys
import tkinter as tk
from tkinter import ttk, filedialog
import threading
from datetime import datetime
from typing import Optional, Dict

from .utils import is_valid_device
from .config import update_file_json, init_ip
from PIL import Image, ImageTk


# Standard color scheme - using system colors where possible
COLORS = {
    "primary": "SystemButtonFace",  # Use system button color
    "success": "SystemButtonFace",  # Standard button color
    "error": "SystemButtonFace",    # Standard button color
    "warning": "SystemButtonFace",  # Standard button color
    "background": "SystemButtonFace",  # System background
    "surface": "SystemWindow",  # System window background
    "text_primary": "SystemWindowText",  # System text color
    "text_secondary": "SystemGrayText",  # System gray text
    "border": "SystemButtonShadow",  # System border
    "hover": "SystemButtonFace",  # System button face
    "disabled_bg": "SystemButtonFace",  # System disabled background
    "disabled_fg": "SystemGrayText",  # System disabled text
    "disabled_border": "SystemButtonShadow",  # System disabled border
}

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


class ToolTip:
    """Tooltip widget for showing helpful hints with delay to prevent flickering."""
    def __init__(self, widget, text: str, x: int, y: int, delay: int = 500):
        self.widget = widget
        self.text = text
        self.tooltip: Optional[tk.Toplevel] = None
        self.x = x
        self.y = y
        self.delay = delay  # Delay in milliseconds before showing tooltip
        self.after_id: Optional[str] = None

        # Use add=True to not override existing bindings
        self.widget.bind("<Enter>", self.on_enter, add="+")
        self.widget.bind("<Leave>", self.on_leave, add="+")
        self.widget.bind("<ButtonPress>", self.on_leave, add="+")  # Hide on click

    def show_tooltip(self, event=None):
        """Display tooltip on hover."""
        if not self.tooltip:
            try:
                # Get widget position
                x = self.widget.winfo_rootx() + self.x
                y = self.widget.winfo_rooty() + self.y + self.widget.winfo_height()
                
                # If bbox is available (for text widgets), use it
                try:
                    bbox = self.widget.bbox("insert")
                    if bbox:
                        x = self.widget.winfo_rootx() + bbox[0] + self.x
                        y = self.widget.winfo_rooty() + bbox[1] + bbox[3] + self.y
                except (tk.TclError, AttributeError):
                    pass

                self.tooltip = tk.Toplevel(self.widget)
                self.tooltip.wm_overrideredirect(True)
                self.tooltip.wm_geometry(f"+{x}+{y}")

                label = tk.Label(
                    self.tooltip,
                    text=self.text,
                    background="#FFFFE0",
                    relief="solid",
                    borderwidth=1,
                    font=FONTS["tooltip"],
                    padx=5,
                    pady=2,
                )
                label.pack()
            except (tk.TclError, AttributeError):
                # Widget may have been destroyed
                self.tooltip = None

    def hide_tooltip(self, event=None):
        """Hide tooltip."""
        # Cancel any pending tooltip display
        if self.after_id:
            self.widget.after_cancel(self.after_id)
            self.after_id = None
        
        if self.tooltip:
            try:
                self.tooltip.destroy()
            except (tk.TclError, AttributeError):
                pass
            self.tooltip = None

    def on_enter(self, event):
        """Handle mouse enter event with delay."""
        # Cancel any existing pending tooltip
        if self.after_id:
            self.widget.after_cancel(self.after_id)
        
        # Schedule tooltip to show after delay
        self.after_id = self.widget.after(self.delay, self.show_tooltip)

    def on_leave(self, event):
        """Handle mouse leave event."""
        self.hide_tooltip()


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
        self.root.minsize(800, 600)  # Larger minimum size for better layout
        
        # Set system background color
        self.root.configure(bg=COLORS["background"])

        # Determine the path to the executable's directory or package directory
        if hasattr(sys, "_MEIPASS"):
            # PyInstaller bundle
            base_dir = sys._MEIPASS
            images_dir = os.path.join(base_dir, "images")
            data_collection_dir = os.path.join(base_dir, "Data")
        else:
            # Development mode - use package directory
            package_dir = os.path.dirname(os.path.dirname(__file__))
            images_dir = os.path.join(package_dir, "ic256_sampler", "assets", "images")
            # Use project root data directory
            project_root = os.path.dirname(package_dir)
            data_collection_dir = os.path.join(project_root, "data")

        if not os.path.exists(data_collection_dir):
            os.makedirs(data_collection_dir)

        # Set the window icon
        icon = self.resize_image(os.path.join(images_dir, "logo.png"), (26, 26))
        self.root.iconphoto(True, icon)

        # Load images
        self.fail_image = self.resize_image(
            os.path.join(images_dir, "fail.png"), (20, 20)
        )
        self.loading_image = self.resize_image(
            os.path.join(images_dir, "loading.png"), (15, 15)
        )
        self.pass_image = self.resize_image(
            os.path.join(images_dir, "pass.png"), (20, 20)
        )
        self.search_image = self.resize_image(
            os.path.join(images_dir, "search.png"), (13, 13)
        )
        self.open_folder_image = self.resize_image(
            os.path.join(images_dir, "open_folder.png"), (13, 13)
        )
        
        # Bind keyboard shortcuts
        self._setup_keyboard_shortcuts()
        
        # Load and apply window state
        self._load_window_state()

    def resize_image(self, image_path: str, size: tuple) -> ImageTk.PhotoImage:
        """Resize an image to the specified size.
        
        Args:
            image_path: Path to the image file
            size: Tuple of (width, height)
            
        Returns:
            Resized PhotoImage object
        """
        with Image.open(image_path) as img:
            img = img.resize(size, Image.LANCZOS)
            return ImageTk.PhotoImage(img)
    
    def _setup_keyboard_shortcuts(self):
        """Set up keyboard shortcuts for common actions.
        
        Uses function keys and non-conflicting shortcuts to avoid conflicts
        with standard Windows/Mac shortcuts (Ctrl+S=Save, Ctrl+Q=Quit, etc.)
        """
        # F9: Start collection (function keys are safe, don't conflict)
        self.root.bind('<F9>', lambda e: self.start() if hasattr(self, 'start_button') and self.start_button['state'] == 'normal' else None)
        
        # F10 or Esc: Stop collection
        self.root.bind('<F10>', lambda e: self.stop() if hasattr(self, 'stop_button') and self.stop_button['state'] == 'normal' else None)
        # Esc only stops if stop button is enabled (not when editing text)
        self.root.bind('<Escape>', lambda e: self.stop() if hasattr(self, 'stop_button') and self.stop_button['state'] == 'normal' else None)
        
        # Ctrl+Shift+S: Export log (Save As pattern, doesn't conflict with Ctrl+S)
        self.root.bind('<Control-Shift-S>', lambda e: self._export_log() if hasattr(self, 'log_text') else None)
        
        # Ctrl+F: Focus search in log tab (standard Find shortcut, only when log tab visible)
        # This is fine as Ctrl+F is standard for Find/Search
        self.root.bind('<Control-f>', lambda e: self._focus_log_search() if hasattr(self, 'log_search_entry') else None)
        
        # Note: Ctrl+C for copy is handled natively by Text widget, no need to bind globally
        # This avoids conflicts with standard copy behavior
    
    def _get_window_state_file(self) -> str:
        """Get path to window state file."""
        import pathlib
        if hasattr(sys, "_MEIPASS"):
            # PyInstaller bundle - use user config directory
            config_dir = pathlib.Path.home() / ".ic256-sampler"
            config_dir.mkdir(parents=True, exist_ok=True)
            return str(config_dir / "window_state.json")
        else:
            # Development mode - use project root
            package_dir = pathlib.Path(__file__).parent.parent
            return str(package_dir / "window_state.json")
    
    def _load_window_state(self):
        """Load and apply saved window state (size and position)."""
        import json
        state_file = self._get_window_state_file()
        
        try:
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
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
        except (json.JSONDecodeError, IOError, OSError, ValueError):
            # If loading fails, use default size
            self.root.geometry("1000x700")
        
        # Save state on window close
        self.root.protocol("WM_DELETE_WINDOW", self._on_window_close)
    
    def _save_window_state(self):
        """Save current window state (size and position)."""
        import json
        try:
            state = {
                'width': self.root.winfo_width(),
                'height': self.root.winfo_height(),
                'x': self.root.winfo_x(),
                'y': self.root.winfo_y()
            }
            
            state_file = self._get_window_state_file()
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except (IOError, OSError, tk.TclError):
            # Ignore errors when saving state
            pass
    
    def _on_window_close(self):
        """Handle window close event - save state and call cleanup."""
        self._save_window_state()
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
            return  # GUI not fully initialized yet
        
        try:
            if not status_dict:
                # No connections
                self.connection_status_label.config(text="●", fg=COLORS["text_secondary"])
                if hasattr(self, 'connection_status_text'):
                    self.connection_status_text.config(text="")
                return
            
            # Determine overall status and device list
            all_connected = all(status == "connected" for status in status_dict.values())
            any_error = any(status == "error" for status in status_dict.values())
            any_disconnected = any(status == "disconnected" for status in status_dict.values())
            
            # Set indicator color and symbol
            if any_error:
                status_color = COLORS["error"]
                status_symbol = "●"
            elif all_connected:
                status_color = COLORS["success"]
                status_symbol = "●"
            elif any_disconnected:
                status_color = COLORS["warning"]
                status_symbol = "●"
            else:
                status_color = COLORS["text_secondary"]
                status_symbol = "●"
            
            self.connection_status_label.config(text=status_symbol, fg=status_color)
            
            # Build status text (device names with status)
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
            # Don't let status update errors break the GUI
            print(f"Error updating connection status: {e}")

    def update_icon(self, button: tk.Button, entry: tk.Entry, device_name: str):
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

    def update_ix256_a_icon(self):
        """Update IC256 device validation icon in background thread."""
        thread = threading.Thread(
            target=self.update_icon,
            args=(self.ix256_a_button, self.ix256_a_entry, "IC256"),
            name="update_ix256_a_icon",
            daemon=True,
        )
        thread.start()

    def update_tx2_icon(self):
        """Update TX2 device validation icon in background thread."""
        thread = threading.Thread(
            target=self.update_icon,
            args=(self.tx2_button, self.tx2_entry, "TX2"),
            name="update_tx2_icon",
            daemon=True,
        )
        thread.start()

    def create_tab(self, notebook: ttk.Notebook, tab_title: str) -> ttk.Frame:
        """Create a new tab in the notebook.
        
        Args:
            notebook: Notebook widget
            tab_title: Title for the tab
            
        Returns:
            Created frame widget
        """
        frame = ttk.Frame(notebook)
        notebook.add(frame, text=tab_title)
        return frame

    def on_tab_click(self, event):
        """Handle tab click to prevent focus issues."""
        event.widget.master.focus_set()

    def select_directory(self):
        """Open directory selection dialog."""
        directory_path = filedialog.askdirectory(
            initialdir=self.path_entry.get(), title="Select a Directory"
        )
        if directory_path:
            self.path_entry.config(state="normal")
            self.path_entry.delete(0, tk.END)
            self.path_entry.insert(0, directory_path)
            self.path_entry.config(state="readonly")

    def open_directory(self):
        """Open the directory in the system's default file manager."""
        path = self.path_entry.get()
        if path and os.path.isdir(path):
            try:
                if sys.platform == "win32":
                    os.startfile(path)
                elif sys.platform == "darwin":  # macOS
                    os.system(f'open "{path}"')
                else:  # Linux and other Unix-like systems
                    os.system(f'xdg-open "{path}"')
            except Exception as e:
                self.show_message(f"Failed to open directory: {str(e)}", "red")

    def update_elapse_time(self, minute: str, second: str, ticks: str):
        """Update elapsed time display.
        
        Args:
            minute: Minute string
            second: Second string
            ticks: Ticks/milliseconds string
        """
        self.minute.config(text=minute)
        self.second.config(text=second)
        self.ticks.config(text=ticks)

    def reset_elapse_time(self):
        """Reset elapsed time display to zero."""
        self.minute.config(text="00")
        self.second.config(text="00")
        self.ticks.config(text="000")
    
    def update_statistics(self, rows: int, file_size: str):
        """Update statistics display (rows and file size).
        
        Args:
            rows: Total number of rows captured
            file_size: Formatted file size string
        """
        if hasattr(self, 'rows_label'):
            self.rows_label.config(text=f"{rows:,}")
        if hasattr(self, 'file_size_label'):
            self.file_size_label.config(text=file_size)
    
    def reset_statistics(self):
        """Reset statistics display to zero."""
        if hasattr(self, 'rows_label'):
            self.rows_label.config(text="0")
        if hasattr(self, 'file_size_label'):
            self.file_size_label.config(text="0 B")

    # Can override by another function
    def start(self):
        """Start data collection (override in main.py)."""
        print("GUI: Start")

    # Can override by another function
    def stop(self):
        """Stop data collection (override in main.py)."""
        print("GUI: Stop")

    def set_up_device(self):
        """Set up device configuration (override in main.py)."""
        print("GUI: Set up device")

    def _create_modern_button(
        self,
        parent: tk.Widget,
        text: str,
        command,
        fg_color: str = COLORS["primary"],
        text_color: str = "white",
        **kwargs
    ) -> tk.Button:
        """Create a button with native appearance and subtle color customization.
        
        Uses native button styling with system colors, but allows color customization
        for important actions (start/stop) while maintaining native look and feel.
        
        Args:
            parent: Parent widget
            text: Button text
            command: Command to execute on click
            fg_color: Background color (for important buttons)
            text_color: Text color
            **kwargs: Additional button options
            
        Returns:
            Configured button widget with native styling
        """
        # Use native button styling - system relief and border
        button = tk.Button(
            parent,
            text=text,
            command=command,
            font=FONTS["button"],
            bg=fg_color,
            fg=text_color,
            activebackground=COLORS["hover"] if fg_color == COLORS["primary"] else fg_color,
            activeforeground=text_color,
            relief="raised",  # Native raised button appearance
            borderwidth=1,  # Standard native border
            padx=20,
            pady=8,
            cursor="hand2",
            highlightthickness=1,  # Native focus highlight
            **kwargs
        )
        
        # Store original colors for state management
        button._original_bg = fg_color
        button._original_fg = text_color
        
        # Create wrapper method to handle state changes with proper styling
        original_config = button.config
        
        def enhanced_config(**kw):
            result = original_config(**kw)
            if "state" in kw:
                state = kw["state"]
                if state == "disabled":
                    # Use native disabled appearance
                    try:
                        button.config(
                            relief="sunken",  # Native disabled appearance
                            cursor="arrow"
                        )
                    except tk.TclError:
                        pass
                elif state == "normal":
                    # Restore normal native appearance
                    button.config(
                        bg=button._original_bg,
                        fg=button._original_fg,
                        relief="raised",
                        cursor="hand2"
                    )
            return result
        
        # Replace config method with enhanced version
        button.config = enhanced_config
        
        return button

    def render_tab(self):
        """Render the main tab structure."""
        # Configure root grid weights for proper resizing
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_rowconfigure(1, weight=0)
        self.root.grid_columnconfigure(0, weight=1)
        
        tab_frame = tk.Frame(self.root, bg=COLORS["background"])
        tab_frame.grid(row=0, column=0, sticky="nsew")
        tab_frame.grid_rowconfigure(1, weight=1)  # Notebook row
        tab_frame.grid_columnconfigure(0, weight=1)

        # Connection status indicator in top left corner (outside tab control)
        connection_frame = tk.Frame(tab_frame, bg=COLORS["background"])
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
            font=("Segoe UI", 9),
            fg=COLORS["text_secondary"],
            bg=COLORS["background"],
            text="",
            cursor="hand2"
        )
        self.connection_status_text.pack(side=tk.LEFT)
        ToolTip(self.connection_status_text, "Click to view detailed connection status", 0, 20)

        # Create a Notebook (Tab Control)
        self.tab = ttk.Notebook(tab_frame)
        self.tab.grid(row=1, column=0, sticky="nsew", padx=2, pady=2)

        # Create tabs
        self.main_tab = self.create_tab(self.tab, "Main")
        self.setting_tab = self.create_tab(self.tab, "Settings")
        self.log_tab = self.create_tab(self.tab, "Log")

        # Configure native style - use system theme
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
                       font=FONTS["button_small"])

        # Bind tab change event
        self.tab.bind("<<NotebookTabChanged>>", self.on_tab_click)

        # Create message frame with fixed height
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

    def render_date_time(self):
        """Render date/time display in tab header."""
        # Create a frame for date/time in top right corner
        header_frame = tk.Frame(self.tab, bg=COLORS["background"])
        header_frame.pack(side=tk.RIGHT, padx=10, pady=3)
        
        # Date/time label
        self.display_time = tk.Label(
            header_frame,
            font=FONTS["date_time"],
            fg=COLORS["text_primary"],
            bg=COLORS["background"]
        )
        self.display_time.pack(side=tk.LEFT)
        self.update_date_time()

    def render_main_tab(self):
        """Render the main tab with centered, beautiful layout."""
        # Configure main tab for responsive resizing
        self.main_tab.grid_rowconfigure(0, weight=1)
        self.main_tab.grid_columnconfigure(0, weight=1)
        
        # Main container with centered content
        main_container = tk.Frame(self.main_tab, bg=COLORS["background"])
        main_container.grid(row=0, column=0, sticky="nsew")
        main_container.grid_rowconfigure(0, weight=1)
        main_container.grid_columnconfigure(0, weight=1)
        
        # Centered content frame
        content_frame = tk.Frame(main_container, bg=COLORS["background"])
        content_frame.grid(row=0, column=0)
        
        # Input section with standard styling
        input_section = tk.Frame(content_frame, bg=COLORS["background"], relief="flat")
        input_section.grid(row=0, column=0, pady=20, padx=20, sticky="ew")
        input_section.grid_columnconfigure(1, weight=1)
        
        # Note entry with modern styling
        note_label = tk.Label(
            input_section,
            font=FONTS["heading"],
            text="Note:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=12,
            anchor="w"
        )
        note_label.grid(row=0, column=0, padx=15, pady=15, sticky="w")

        self.note_entry = tk.Entry(
            input_section,
            font=FONTS["entry_large"],
            width=35,
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.note_entry.grid(row=0, column=1, padx=15, pady=15, sticky="ew")
        note_placeholder = "Enter a note for this data collection session..."
        self.note_entry.insert(0, note_placeholder)
        self.note_entry.config(fg=COLORS["text_secondary"])
        self.note_entry.bind('<FocusIn>', lambda e: self._on_entry_focus_in(self.note_entry, note_placeholder))
        self.note_entry.bind('<FocusOut>', lambda e: self._on_entry_focus_out(self.note_entry, note_placeholder))
        ToolTip(self.note_entry, "Optional note to include in the CSV file name and metadata", 0, 20)

        # Elapsed time section
        time_section = tk.Frame(content_frame, bg=COLORS["background"])
        time_section.grid(row=1, column=0, pady=30)
        
        time_label = tk.Label(
            time_section,
            font=FONTS["heading"],
            text="Elapsed Time:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        time_label.grid(row=0, column=0, pady=(0, 15))

        time_display_frame = tk.Frame(time_section, bg=COLORS["background"])
        time_display_frame.grid(row=1, column=0, pady=10)

        self.minute = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text="00",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=3
        )
        self.minute.grid(row=0, column=0, padx=2)

        colon_1 = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text=":",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        colon_1.grid(row=0, column=1, padx=2)

        self.second = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text="00",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=3
        )
        self.second.grid(row=0, column=2, padx=2)

        colon_2 = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text=":",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        colon_2.grid(row=0, column=3, padx=2)

        self.ticks = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text="000",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=4
        )
        self.ticks.grid(row=0, column=4, padx=2)

        # Statistics section (rows and file size) - separate row below elapsed time
        stats_section = tk.Frame(content_frame, bg=COLORS["background"], relief="flat")
        stats_section.grid(row=2, column=0, pady=15, padx=20, sticky="ew")
        stats_section.grid_columnconfigure(0, weight=1)
        stats_section.grid_columnconfigure(1, weight=1)
        
        # Statistics label
        stats_label = tk.Label(
            stats_section,
            font=FONTS["heading"],
            text="Statistics:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            anchor="w"
        )
        stats_label.grid(row=0, column=0, columnspan=2, padx=15, pady=(15, 10), sticky="w")
        
        # Rows display
        rows_frame = tk.Frame(stats_section, bg=COLORS["background"])
        rows_frame.grid(row=1, column=0, padx=15, pady=10, sticky="w")
        
        rows_title = tk.Label(
            rows_frame,
            font=FONTS["label_small"],
            text="Rows:",
            bg=COLORS["background"],
            fg=COLORS["text_secondary"]
        )
        rows_title.grid(row=0, column=0, padx=(0, 5))
        
        self.rows_label = tk.Label(
            rows_frame,
            font=FONTS["label_medium"],
            text="0",
            bg=COLORS["background"],
            fg=COLORS["primary"]
        )
        self.rows_label.grid(row=0, column=1)
        
        # File size display
        size_frame = tk.Frame(stats_section, bg=COLORS["background"])
        size_frame.grid(row=1, column=1, padx=15, pady=10, sticky="w")
        
        size_title = tk.Label(
            size_frame,
            font=FONTS["label_small"],
            text="File Size:",
            bg=COLORS["background"],
            fg=COLORS["text_secondary"]
        )
        size_title.grid(row=0, column=0, padx=(0, 5))
        
        self.file_size_label = tk.Label(
            size_frame,
            font=FONTS["label_medium"],
            text="0 B",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        self.file_size_label.grid(row=0, column=1)

        # Button section - centered
        button_section = tk.Frame(content_frame, bg=COLORS["background"])
        button_section.grid(row=3, column=0, pady=30)

        self.start_button = self._create_modern_button(
            button_section,
            "Start",
            self.start,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.start_button.grid(row=0, column=0, padx=15)

        self.stop_button = self._create_modern_button(
            button_section,
            "Stop",
            self.stop,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.stop_button.grid(row=0, column=1, padx=15)
        self.stop_button.config(state="disabled")

    def render_setting_tab(self):
        """Render the settings tab with modern, centered layout."""
        # Configure setting tab for resizing
        self.setting_tab.grid_rowconfigure(0, weight=1)
        self.setting_tab.grid_columnconfigure(0, weight=1)
        
        # Main container
        main_container = tk.Frame(self.setting_tab, bg=COLORS["background"])
        main_container.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
        main_container.grid_rowconfigure(0, weight=1)
        main_container.grid_columnconfigure(0, weight=1)
        
        # Centered content frame
        content_frame = tk.Frame(main_container, bg=COLORS["background"])
        content_frame.grid(row=0, column=0)
        content_frame.grid_columnconfigure(0, weight=1)
        
        # Device configuration section
        device_section = tk.LabelFrame(
            content_frame,
            text="Device Configuration",
            font=FONTS["heading"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="groove",
            padx=15,
            pady=10
        )
        device_section.grid(row=0, column=0, pady=15, padx=10, sticky="ew")
        device_section.grid_columnconfigure(1, weight=1)

        # IC256 device entry
        ic256_label = tk.Label(
            device_section,
            font=FONTS["label"],
            text="IC256-42/35:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=15,
            anchor="w"
        )
        ic256_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        self.ix256_a_entry = tk.Entry(
            device_section,
            width=30,
            font=FONTS["entry"],
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.ix256_a_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        self.ix256_a_entry.bind('<KeyRelease>', lambda e: self._validate_ip_entry(self.ix256_a_entry))
        ToolTip(self.ix256_a_entry, "IC256 device IP address (e.g., 192.168.1.100). Click search icon to validate.", 0, 20)

        self.ix256_a_button = tk.Button(
            device_section,
            image=self.search_image,
            command=self.update_ix256_a_icon,
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2"
        )
        self.ix256_a_button.grid(row=0, column=2, padx=5, pady=10)
        ToolTip(self.ix256_a_button, "Check IP address", 0, 20)

        # TX2 device entry
        tx2_label = tk.Label(
            device_section,
            font=FONTS["label"],
            text="TX2:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=15,
            anchor="w"
        )
        tx2_label.grid(row=1, column=0, padx=10, pady=10, sticky="w")

        self.tx2_entry = tk.Entry(
            device_section,
            width=30,
            font=FONTS["entry"],
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.tx2_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")
        self.tx2_entry.bind('<KeyRelease>', lambda e: self._validate_ip_entry(self.tx2_entry))
        ToolTip(self.tx2_entry, "TX2 device IP address (e.g., 192.168.1.101). Click search icon to validate.", 0, 20)

        self.tx2_button = tk.Button(
            device_section,
            image=self.search_image,
            command=self.update_tx2_icon,
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2"
        )
        self.tx2_button.grid(row=1, column=2, padx=5, pady=10)
        ToolTip(self.tx2_button, "Check IP address", 0, 20)

        # Sampling rate section
        sampling_section = tk.LabelFrame(
            content_frame,
            text="Sampling Configuration",
            font=FONTS["heading"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="groove",
            padx=15,
            pady=10
        )
        sampling_section.grid(row=1, column=0, pady=15, padx=10, sticky="ew")
        sampling_section.grid_columnconfigure(1, weight=1)

        sampling_label = tk.Label(
            sampling_section,
            font=FONTS["label"],
            text="Sampling Rate (1-6000 Hz):",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=20,
            anchor="w"
        )
        sampling_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        self.entry_var = tk.StringVar()
        self.entry_var.trace("w", self.sampling_change)

        self.sampling_entry = tk.Entry(
            sampling_section,
            width=20,
            font=FONTS["entry"],
            textvariable=self.entry_var,
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.sampling_entry.grid(row=0, column=1, padx=10, pady=10, sticky="w")
        ToolTip(self.sampling_entry, "Sampling rate in Hz (1-6000). Default: 500 Hz", 0, 20)

        self.set_up_button = self._create_modern_button(
            sampling_section,
            "Apply",
            self.set_up_device,
            fg_color=COLORS["primary"],
            text_color="white"
        )
        self.set_up_button.grid(row=0, column=2, padx=10, pady=10)
        ToolTip(self.set_up_button, "Apply sampling rate configuration to all devices", 20, -20)

        # Path configuration section
        path_section = tk.LabelFrame(
            content_frame,
            text="File Path Configuration",
            font=FONTS["heading"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="flat",
            padx=20,
            pady=15
        )
        path_section.grid(row=2, column=0, pady=15, padx=10, sticky="ew")
        path_section.grid_columnconfigure(1, weight=1)

        path_label = tk.Label(
            path_section,
            font=FONTS["label"],
            text="Save Path:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=15,
            anchor="w"
        )
        path_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        self.path_entry = tk.Entry(
            path_section,
            width=35,
            font=FONTS["entry"],
            state="readonly",
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            readonlybackground=COLORS["background"]  # Slightly different background for readonly
        )
        self.path_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        ToolTip(self.path_entry, "Directory where CSV files will be saved. Click Browse to select.", 0, 20)

        browse_button = self._create_modern_button(
            path_section,
            "Browse",
            self.select_directory,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        browse_button.grid(row=0, column=2, padx=5, pady=10)

        open_button = tk.Button(
            path_section,
            image=self.open_folder_image,
            command=self.open_directory,
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2"
        )
        open_button.grid(row=0, column=3, padx=5, pady=10)

        # Save configuration button - centered
        save_section = tk.Frame(content_frame, bg=COLORS["background"])
        save_section.grid(row=3, column=0, pady=20)

        save_setting = self._create_modern_button(
            save_section,
            "Update Configuration",
            lambda: update_file_json(
                self.ix256_a_entry,
                self.tx2_entry,
                self.path_entry,
                self.sampling_entry,
            ),
            fg_color=COLORS["primary"],
            text_color="white"
        )
        save_setting.pack()
        ToolTip(save_setting, "Save current settings to configuration file", 0, 20)

        # Initialize IP addresses
        init_ip(
            self.ix256_a_entry,
            self.tx2_entry,
            self.path_entry,
            self.sampling_entry,
        )

    def sampling_change(self, *args):
        """Validate and constrain sampling rate input."""
        current_value = self.entry_var.get()
        if current_value == "":
            return
        elif current_value.isdigit():
            try:
                if int(current_value) < 1:
                    self.sampling_entry.delete(0, tk.END)
                    self.sampling_entry.insert(0, "1")
                elif int(current_value) > 6000:
                    self.sampling_entry.delete(0, tk.END)
                    self.sampling_entry.insert(0, "6000")
            except ValueError:
                pass
        else:
            self.sampling_entry.delete(0, tk.END)
            self.sampling_entry.insert(0, "1")

    def show_message(self, message: str, fg_color: str = "black"):
        """Show a message in the status bar without changing window size.
        
        Args:
            message: Message text to display
            fg_color: Foreground color (can be color name or level)
        """
        # Map color names to actual colors
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
            # Window may be destroyed or widget not available - ignore
            pass
        
        # Also log to log tab
        self.log_message(message, fg_color)

    def hide_message(self):
        """Hide the status message."""
        self.message_text.config(text="")

    def log_message(self, message: str, level: str = "INFO"):
        """Add a message to the log tab with timestamp.
        
        Args:
            message: The log message
            level: Log level - "INFO", "WARNING", "ERROR", or color string
        """
        if not hasattr(self, 'log_text') or self.log_text is None:
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Map level/color to log level and tag
        level_upper = str(level).upper()
        message_lower = message.lower()
        
        if level_upper == "ERROR" or level == "red" or "error" in message_lower or "failed" in message_lower:
            log_level = "ERROR"
            tag = "error"
        elif level_upper == "WARNING" or level == "orange" or "warning" in message_lower:
            log_level = "WARNING"
            tag = "warning"
        elif level_upper == "INFO" or level == "green" or "success" in message_lower or "completed" in message_lower:
            log_level = "INFO"
            tag = "success"
        else:
            log_level = "INFO"
            tag = "info"
        
        log_entry = f"[{timestamp}] [{log_level}] {message}\n"
        
        try:
            # Store in log_content for filtering
            if not hasattr(self, 'log_content'):
                self.log_content = []
            self.log_content.append({'text': log_entry, 'tag': tag})
            
            # Limit log content size to prevent memory issues
            if len(self.log_content) > 1000:
                self.log_content = self.log_content[-1000:]
            
            # Insert into text widget (respecting current filter)
            if hasattr(self, 'log_search_entry') and self.log_search_entry.get():
                # Apply filter
                self._filter_log()
            else:
                self.log_text.insert(tk.END, log_entry, tag)
                self.log_text.see(tk.END)
            
            # Limit displayed log size to prevent memory issues
            lines = int(self.log_text.index('end-1c').split('.')[0])
            if lines > 1000:
                self.log_text.delete('1.0', f'{lines-1000}.0')
        except (tk.TclError, ValueError, AttributeError):
            # Text widget may be destroyed or invalid index - ignore
            pass

    def render_log_tab(self):
        """Render the log tab with scrollable text widget."""
        # Configure log tab for resizing
        self.log_tab.grid_rowconfigure(1, weight=1)
        self.log_tab.grid_columnconfigure(0, weight=1)
        
        # Create search frame at the top
        search_frame = tk.Frame(self.log_tab, bg=COLORS["background"])
        search_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        search_frame.grid_columnconfigure(1, weight=1)
        
        search_label = tk.Label(
            search_frame,
            text="Search:",
            font=FONTS["label_small"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        search_label.grid(row=0, column=0, padx=(0, 5), sticky="w")
        
        self.log_search_entry = tk.Entry(
            search_frame,
            font=FONTS["entry"],
            relief="sunken",
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"]
        )
        self.log_search_entry.grid(row=0, column=1, sticky="ew", padx=5)
        self.log_search_entry.bind('<KeyRelease>', self._filter_log)
        ToolTip(self.log_search_entry, "Search log entries (Ctrl+F to focus)", 0, 20)
        
        # Create a frame for the log
        log_frame = tk.Frame(self.log_tab, bg=COLORS["background"])
        log_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        log_frame.grid_rowconfigure(0, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)
        
        # Create scrollbar
        scrollbar = tk.Scrollbar(log_frame, bg=COLORS["background"])
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Create text widget for log
        self.log_text = tk.Text(
            log_frame,
            wrap=tk.WORD,
            yscrollcommand=scrollbar.set,
            font=FONTS["log"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="flat",
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            padx=10,
            pady=10
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar.config(command=self.log_text.yview)
        
        # Store original log content for filtering
        self.log_content = []
        
        # Configure text tags for different log levels
        self.log_text.tag_config("error", foreground=COLORS["error"], font=FONTS["log_bold"])
        self.log_text.tag_config("warning", foreground=COLORS["warning"], font=FONTS["log"])
        self.log_text.tag_config("success", foreground=COLORS["success"], font=FONTS["log"])
        self.log_text.tag_config("info", foreground=COLORS["primary"], font=FONTS["log"])
        self.log_text.tag_config("highlight", background="#FFFF00", foreground=COLORS["text_primary"])
        
        # Add initial log message
        self.log_message("Application started", "INFO")
        
        # Add button frame with multiple actions
        button_frame = tk.Frame(log_frame, bg=COLORS["background"])
        button_frame.grid(row=1, column=0, columnspan=2, pady=10)
        
        clear_button = self._create_modern_button(
            button_frame,
            "Clear Log",
            self.clear_log,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        clear_button.pack(side=tk.LEFT, padx=5)
        # No tooltip - button label is clear
        
        export_button = self._create_modern_button(
            button_frame,
            "Export Log",
            self._export_log,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        export_button.pack(side=tk.LEFT, padx=5)
        # No tooltip - button label is clear
        
        copy_button = self._create_modern_button(
            button_frame,
            "Copy Selected",
            self._copy_log_selection,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        copy_button.pack(side=tk.LEFT, padx=5)
        # No tooltip - button label is clear
    
    def _on_entry_focus_in(self, entry: tk.Entry, placeholder: str):
        """Handle entry focus in event - clear placeholder."""
        if entry.get() == placeholder:
            entry.delete(0, tk.END)
            entry.config(fg=COLORS["text_primary"])
    
    def _on_entry_focus_out(self, entry: tk.Entry, placeholder: str):
        """Handle entry focus out event - restore placeholder if empty."""
        if not entry.get().strip():
            entry.insert(0, placeholder)
            entry.config(fg=COLORS["text_secondary"])
    
    def get_note_value(self) -> str:
        """Get note entry value, returning empty string if placeholder is present.
        
        Returns:
            Note value or empty string if placeholder
        """
        if not hasattr(self, 'note_entry'):
            return ""
        note = self.note_entry.get().strip()
        placeholder = "Enter a note for this data collection session..."
        if note == placeholder:
            return ""
        return note
    
    def _validate_ip_entry(self, entry: tk.Entry):
        """Validate IP address entry and provide visual feedback."""
        ip = entry.get().strip()
        # Remove placeholder color if user is typing
        if entry.cget('fg') == COLORS["text_secondary"]:
            entry.config(fg=COLORS["text_primary"])
    
    def clear_log(self):
        """Clear the log text widget."""
        self.log_text.delete('1.0', tk.END)
        self.log_content = []
        self.log_message("Log cleared", "INFO")
    
    def _filter_log(self, event=None):
        """Filter log entries based on search text."""
        if not hasattr(self, 'log_search_entry'):
            return
        
        search_text = self.log_search_entry.get().lower()
        
        # Remove existing highlight tags
        self.log_text.tag_remove("highlight", "1.0", tk.END)
        
        if not search_text:
            # Show all content
            self.log_text.delete('1.0', tk.END)
            for entry in self.log_content:
                self.log_text.insert(tk.END, entry['text'], entry['tag'])
            return
        
        # Filter and highlight
        self.log_text.delete('1.0', tk.END)
        for entry in self.log_content:
            if search_text in entry['text'].lower():
                self.log_text.insert(tk.END, entry['text'], entry['tag'])
                # Highlight search matches
                start = "1.0"
                while True:
                    pos = self.log_text.search(search_text, start, tk.END, nocase=True)
                    if not pos:
                        break
                    end = f"{pos}+{len(search_text)}c"
                    self.log_text.tag_add("highlight", pos, end)
                    start = end
    
    def _focus_log_search(self, event=None):
        """Focus the log search entry."""
        if hasattr(self, 'log_search_entry'):
            self.log_search_entry.focus_set()
            self.log_search_entry.select_range(0, tk.END)
        return "break"
    
    def _export_log(self, event=None):
        """Export log to a text file."""
        if not hasattr(self, 'log_text'):
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Export Log"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    content = self.log_text.get('1.0', tk.END)
                    f.write(content)
                self.show_message(f"Log exported to {filename}", "green")
                self.log_message(f"Log exported to {filename}", "INFO")
            except Exception as e:
                error_msg = f"Failed to export log: {str(e)}"
                self.show_message(error_msg, "red")
                self.log_message(error_msg, "ERROR")
        return "break"
    
    def _copy_log_selection(self, event=None):
        """Copy selected text from log to clipboard."""
        if not hasattr(self, 'log_text'):
            return
        
        try:
            if self.log_text.tag_ranges(tk.SEL):
                # Copy selected text
                selected = self.log_text.get(tk.SEL_FIRST, tk.SEL_LAST)
                self.root.clipboard_clear()
                self.root.clipboard_append(selected)
                self.show_message("Text copied to clipboard", "green")
            else:
                # Copy all text if nothing selected
                all_text = self.log_text.get('1.0', tk.END)
                self.root.clipboard_clear()
                self.root.clipboard_append(all_text)
                self.show_message("All log text copied to clipboard", "green")
        except Exception as e:
            error_msg = f"Failed to copy: {str(e)}"
            self.show_message(error_msg, "red")
        return "break"

    def render(self):
        """Render all GUI components and start the main loop."""
        self.render_tab()
        self.render_main_tab()
        self.render_setting_tab()
        self.render_log_tab()
        self.render_date_time()
        self.root.update_idletasks()
        
        # Window close handler is already set in _load_window_state()
        # which calls _on_window_close() that handles saving state and cleanup
        
        self.root.mainloop()
