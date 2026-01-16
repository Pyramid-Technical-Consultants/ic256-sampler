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
from typing import Optional

from .utils import is_valid_device
from .config import update_file_json, init_ip
from PIL import Image, ImageTk


# Modern color scheme
COLORS = {
    "primary": "#2196F3",  # Blue
    "success": "#4CAF50",  # Green
    "error": "#F44336",    # Red
    "warning": "#FF9800",  # Orange
    "background": "#FAFAFA",  # Light gray
    "surface": "#FFFFFF",  # White
    "text_primary": "#212121",  # Dark gray
    "text_secondary": "#757575",  # Medium gray
    "border": "#E0E0E0",  # Light border
    "hover": "#1976D2",  # Darker blue for hover
    "disabled_bg": "#E0E0E0",  # Light gray for disabled buttons
    "disabled_fg": "#9E9E9E",  # Medium gray text for disabled buttons
    "disabled_border": "#BDBDBD",  # Border for disabled buttons
}

# Font style configuration
FONTS = {
    "family_ui": "Segoe UI",  # Primary UI font family
    "family_mono": "Consolas",  # Monospace font for logs/code
    "size_tiny": 9,
    "size_small": 10,
    "size_normal": 11,
    "size_medium": 12,
    "size_large": 14,
    "size_huge": 48,
    # Predefined font tuples for common use cases
    "tooltip": ("Segoe UI", 9),
    "button": ("Segoe UI", 11, "bold"),
    "button_small": ("Segoe UI", 10, "bold"),
    "label": ("Segoe UI", 11),
    "label_small": ("Segoe UI", 10),
    "label_medium": ("Segoe UI", 12, "bold"),
    "entry": ("Segoe UI", 10),
    "entry_large": ("Segoe UI", 11),
    "heading": ("Segoe UI", 12, "bold"),
    "time_display": ("Segoe UI", 48, "bold"),
    "date_time": ("Segoe UI", 10, "bold"),
    "log": ("Consolas", 10),
    "log_bold": ("Consolas", 10, "bold"),
}


class ToolTip:
    """Tooltip widget for showing helpful hints."""
    def __init__(self, widget, text: str, x: int, y: int):
        self.widget = widget
        self.text = text
        self.tooltip: Optional[tk.Toplevel] = None
        self.x = x
        self.y = y

        self.widget.bind("<Enter>", self.on_enter)
        self.widget.bind("<Leave>", self.on_leave)

    def show_tooltip(self, event=None):
        """Display tooltip on hover."""
        if not self.tooltip:
            x, y, _, _ = self.widget.bbox("insert")
            x += self.widget.winfo_rootx() + self.x
            y += self.widget.winfo_rooty() + self.y

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

    def hide_tooltip(self, event=None):
        """Hide tooltip."""
        if self.tooltip:
            self.tooltip.destroy()
            self.tooltip = None

    def on_enter(self, event):
        """Handle mouse enter event."""
        self.show_tooltip()

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
        
        # Set modern background color
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

    def update_date_time(self):
        """Update the date/time display."""
        now = datetime.now()
        current_date = now.strftime("%Y-%m-%d")
        current_time = now.strftime("%H:%M:%S")
        self.display_time.config(text=f"{current_date} {current_time}")
        self.root.after(1000, self.update_date_time)

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
            self.rows_label.config(text=f"Rows: {rows:,}")
        if hasattr(self, 'file_size_label'):
            self.file_size_label.config(text=f"File Size: {file_size}")
    
    def reset_statistics(self):
        """Reset statistics display to zero."""
        if hasattr(self, 'rows_label'):
            self.rows_label.config(text="Rows: 0")
        if hasattr(self, 'file_size_label'):
            self.file_size_label.config(text="File Size: 0 B")

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
        """Create a modern styled button with rounded corners (2px radius) and native Windows appearance.
        
        Args:
            parent: Parent widget
            text: Button text
            command: Command to execute on click
            fg_color: Background color
            text_color: Text color
            **kwargs: Additional button options
            
        Returns:
            Configured button widget (wrapped in container for rounded corners)
        """
        # Use native Windows button style with flat relief for rounded corner effect
        button = tk.Button(
            parent,
            text=text,
            command=command,
            font=FONTS["button"],
            bg=fg_color,
            fg=text_color,
            activebackground=COLORS["hover"] if fg_color == COLORS["primary"] else fg_color,
            activeforeground=text_color,
            relief="flat",  # Flat relief works better with rounded corners
            borderwidth=0,  # No border for cleaner rounded look
            padx=20,
            pady=10,
            cursor="hand2",
            highlightthickness=0,  # Remove focus highlight for cleaner look
            **kwargs
        )
        
        # Note: Rounded corners removed temporarily to fix button visibility issue
        # The canvas overlay was interfering with button display
        
        # Store original colors for state management
        button._original_bg = fg_color
        button._original_fg = text_color
        
        # Add hover effect (only when enabled)
        def on_enter(e):
            if button["state"] == "normal":
                if fg_color == COLORS["primary"]:
                    button.config(bg=COLORS["hover"])
                elif fg_color == COLORS["success"]:
                    button.config(bg="#45B049")  # Slightly darker green
                elif fg_color == COLORS["error"]:
                    button.config(bg="#E53935")  # Slightly darker red
        
        def on_leave(e):
            if button["state"] == "normal":
                button.config(bg=button._original_bg)
        
        def on_press(e):
            if button["state"] == "normal":
                # Visual feedback - slightly darker
                current_bg = button.cget("bg")
                if current_bg == COLORS["hover"]:
                    button.config(bg="#1565C0")  # Even darker blue
                elif current_bg == COLORS["success"]:
                    button.config(bg="#388E3C")  # Darker green
                elif current_bg == COLORS["error"]:
                    button.config(bg="#C62828")  # Darker red
        
        def on_release(e):
            if button["state"] == "normal":
                button.config(bg=button._original_bg if button._original_bg != COLORS["primary"] else COLORS["hover"])
        
        button.bind("<Enter>", on_enter)
        button.bind("<Leave>", on_leave)
        button.bind("<Button-1>", on_press)
        button.bind("<ButtonRelease-1>", on_release)
        
        # Create wrapper method to handle state changes with proper styling
        original_config = button.config
        
        def enhanced_config(**kw):
            result = original_config(**kw)
            if "state" in kw:
                state = kw["state"]
                if state == "disabled":
                    # Make disabled state very clear
                    try:
                        button.config(
                            bg=COLORS["disabled_bg"],
                            fg=COLORS["disabled_fg"],
                            cursor="arrow"  # Normal cursor when disabled
                        )
                        # Unbind hover events when disabled
                        button.unbind("<Enter>")
                        button.unbind("<Leave>")
                    except tk.TclError:
                        button.config(cursor="arrow")
                elif state == "normal":
                    # Restore normal appearance
                    button.config(
                        bg=button._original_bg,
                        fg=button._original_fg,
                        cursor="hand2",
                        activebackground=COLORS["hover"] if button._original_bg == COLORS["primary"] else button._original_bg,
                        activeforeground=button._original_fg
                    )
                    # Rebind hover events when enabled
                    button.bind("<Enter>", on_enter)
                    button.bind("<Leave>", on_leave)
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
        tab_frame.grid_rowconfigure(0, weight=1)
        tab_frame.grid_columnconfigure(0, weight=1)

        # Create a Notebook (Tab Control)
        self.tab = ttk.Notebook(tab_frame)
        self.tab.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)

        # Create tabs
        self.main_tab = self.create_tab(self.tab, "Main")
        self.setting_tab = self.create_tab(self.tab, "Settings")
        self.log_tab = self.create_tab(self.tab, "Log")

        # Configure modern style
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
        else:
            style.theme_use("default")
        
        # Modern tab styling
        style.configure("TNotebook", background=COLORS["background"], borderwidth=0)
        style.configure("TNotebook.Tab", 
                       padding=(15, 8), 
                       font=FONTS["button_small"],
                       foreground=COLORS["text_primary"])

        # Bind tab change event
        self.tab.bind("<<NotebookTabChanged>>", self.on_tab_click)

        # Create message frame with fixed height
        message_frame = tk.Frame(
            self.root, 
            height=30,
            bg=COLORS["surface"],
            relief="flat"
        )
        message_frame.grid(row=1, column=0, sticky="ew")
        message_frame.grid_propagate(False)
        message_frame.grid_columnconfigure(0, weight=1)

        self.message_text = tk.Label(
            message_frame,
            font=FONTS["label_small"],
            anchor="w",
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            wraplength=580
        )
        self.message_text.grid(row=0, column=0, sticky="ew", padx=10, pady=5)
        
        self.message_frame = message_frame

    def render_date_time(self):
        """Render date/time display in tab header."""
        # Date/time label
        self.display_time = tk.Label(
            self.tab,
            font=FONTS["date_time"],
            fg=COLORS["primary"],
            bg=COLORS["background"]
        )
        self.display_time.place(x=350, y=3)
        self.update_date_time()
        
        # Connection status indicator (next to clock)
        self.connection_status_label = tk.Label(
            self.tab,
            font=FONTS["date_time"],
            fg=COLORS["text_secondary"],
            bg=COLORS["background"],
            text="●"
        )
        # Position it to the right of the clock (we'll update position after first clock update)
        self.connection_status_label.place(x=550, y=3)
        
        # Connection status text (device names)
        self.connection_status_text = tk.Label(
            self.tab,
            font=("Segoe UI", 9),
            fg=COLORS["text_secondary"],
            bg=COLORS["background"],
            text=""
        )
        self.connection_status_text.place(x=570, y=3)

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
        
        # Input section with modern styling
        input_section = tk.Frame(content_frame, bg=COLORS["surface"], relief="flat")
        input_section.grid(row=0, column=0, pady=30, padx=40, sticky="ew")
        input_section.grid_columnconfigure(1, weight=1)
        
        # Note entry with modern styling
        note_label = tk.Label(
            input_section,
            font=FONTS["heading"],
            text="Note:",
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            width=12,
            anchor="w"
        )
        note_label.grid(row=0, column=0, padx=15, pady=15, sticky="w")

        self.note_entry = tk.Entry(
            input_section,
            font=FONTS["entry_large"],
            width=35,
            relief="sunken",  # Native Windows entry field appearance
            borderwidth=2,  # Standard Windows border width
            highlightthickness=0,  # Remove custom highlight for native look
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.note_entry.grid(row=0, column=1, padx=15, pady=15, sticky="ew")

        # Elapsed time section
        time_section = tk.Frame(content_frame, bg=COLORS["background"])
        time_section.grid(row=1, column=0, pady=20)
        
        time_label = tk.Label(
            time_section,
            font=FONTS["heading"],
            text="Elapsed Time:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        time_label.grid(row=0, column=0, pady=10)

        time_display_frame = tk.Frame(time_section, bg=COLORS["background"])
        time_display_frame.grid(row=1, column=0, pady=10)

        self.minute = tk.Label(
            time_display_frame,
            font=FONTS["time_display"],
            text="00",
            bg=COLORS["background"],
            fg=COLORS["primary"],
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
            fg=COLORS["primary"],
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
            fg=COLORS["primary"],
            width=4
        )
        self.ticks.grid(row=0, column=4, padx=2)

        # Statistics section (rows and file size)
        stats_section = tk.Frame(content_frame, bg=COLORS["background"])
        stats_section.grid(row=1, column=0, pady=15)
        
        self.rows_label = tk.Label(
            stats_section,
            font=FONTS["label"],
            text="Rows: 0",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        self.rows_label.grid(row=0, column=0, padx=20)
        
        self.file_size_label = tk.Label(
            stats_section,
            font=FONTS["label"],
            text="File Size: 0 B",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        self.file_size_label.grid(row=0, column=1, padx=20)

        # Button section - centered
        button_section = tk.Frame(content_frame, bg=COLORS["background"])
        button_section.grid(row=2, column=0, pady=30)

        self.start_button = self._create_modern_button(
            button_section,
            "START",
            self.start,
            fg_color=COLORS["success"],
            text_color="white"
        )
        self.start_button.grid(row=0, column=0, padx=15)

        self.stop_button = self._create_modern_button(
            button_section,
            "STOP",
            self.stop,
            fg_color=COLORS["error"],
            text_color="white"
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
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            relief="flat",
            padx=20,
            pady=15
        )
        device_section.grid(row=0, column=0, pady=15, padx=10, sticky="ew")
        device_section.grid_columnconfigure(1, weight=1)

        # IC256 device entry
        ic256_label = tk.Label(
            device_section,
            font=FONTS["label"],
            text="IC256-42/35:",
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            width=15,
            anchor="w"
        )
        ic256_label.grid(row=0, column=0, padx=10, pady=10, sticky="w")

        self.ix256_a_entry = tk.Entry(
            device_section,
            width=30,
            font=FONTS["entry"],
            relief="sunken",  # Native Windows entry field appearance
            borderwidth=2,  # Standard Windows border width
            highlightthickness=0,  # Remove custom highlight for native look
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.ix256_a_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")

        self.ix256_a_button = tk.Button(
            device_section,
            image=self.search_image,
            command=self.update_ix256_a_icon,
            relief="flat",
            bg=COLORS["surface"],
            cursor="hand2"
        )
        self.ix256_a_button.grid(row=0, column=2, padx=5, pady=10)
        ToolTip(self.ix256_a_button, "Check IP address", 0, 20)

        # TX2 device entry
        tx2_label = tk.Label(
            device_section,
            font=FONTS["label"],
            text="TX2:",
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            width=15,
            anchor="w"
        )
        tx2_label.grid(row=1, column=0, padx=10, pady=10, sticky="w")

        self.tx2_entry = tk.Entry(
            device_section,
            width=30,
            font=FONTS["entry"],
            relief="sunken",  # Native Windows entry field appearance
            borderwidth=2,  # Standard Windows border width
            highlightthickness=0,  # Remove custom highlight for native look
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.tx2_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")

        self.tx2_button = tk.Button(
            device_section,
            image=self.search_image,
            command=self.update_tx2_icon,
            relief="flat",
            bg=COLORS["surface"],
            cursor="hand2"
        )
        self.tx2_button.grid(row=1, column=2, padx=5, pady=10)
        ToolTip(self.tx2_button, "Check IP address", 0, 20)

        # Sampling rate section
        sampling_section = tk.LabelFrame(
            content_frame,
            text="Sampling Configuration",
            font=FONTS["heading"],
            bg=COLORS["surface"],
            fg=COLORS["text_primary"],
            relief="flat",
            padx=20,
            pady=15
        )
        sampling_section.grid(row=1, column=0, pady=15, padx=10, sticky="ew")
        sampling_section.grid_columnconfigure(1, weight=1)

        sampling_label = tk.Label(
            sampling_section,
            font=FONTS["label"],
            text="Sampling Rate (1-6000 Hz):",
            bg=COLORS["surface"],
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
            relief="sunken",  # Native Windows entry field appearance
            borderwidth=2,  # Standard Windows border width
            highlightthickness=0,  # Remove custom highlight for native look
            insertbackground=COLORS["text_primary"]  # Cursor color
        )
        self.sampling_entry.grid(row=0, column=1, padx=10, pady=10, sticky="w")

        self.set_up_button = self._create_modern_button(
            sampling_section,
            "Apply",
            self.set_up_device,
            fg_color=COLORS["primary"],
            text_color="white"
        )
        self.set_up_button.grid(row=0, column=2, padx=10, pady=10)
        ToolTip(self.set_up_button, "Set frequency configuration for all devices", 20, -20)

        # Path configuration section
        path_section = tk.LabelFrame(
            content_frame,
            text="File Path Configuration",
            font=FONTS["heading"],
            bg=COLORS["surface"],
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
            bg=COLORS["surface"],
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
            relief="sunken",  # Native Windows entry field appearance
            borderwidth=2,  # Standard Windows border width
            highlightthickness=0,  # Remove custom highlight for native look
            readonlybackground=COLORS["background"]  # Slightly different background for readonly
        )
        self.path_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")

        browse_button = self._create_modern_button(
            path_section,
            "Browse",
            self.select_directory,
            fg_color=COLORS["text_secondary"],
            text_color="white"
        )
        browse_button.grid(row=0, column=2, padx=5, pady=10)

        open_button = tk.Button(
            path_section,
            image=self.open_folder_image,
            command=self.open_directory,
            relief="flat",
            bg=COLORS["surface"],
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
            self.log_text.insert(tk.END, log_entry, tag)
            self.log_text.see(tk.END)
            
            # Limit log size to prevent memory issues
            lines = int(self.log_text.index('end-1c').split('.')[0])
            if lines > 1000:
                self.log_text.delete('1.0', f'{lines-1000}.0')
        except (tk.TclError, ValueError, AttributeError):
            # Text widget may be destroyed or invalid index - ignore
            pass

    def render_log_tab(self):
        """Render the log tab with scrollable text widget."""
        # Configure log tab for resizing
        self.log_tab.grid_rowconfigure(0, weight=1)
        self.log_tab.grid_columnconfigure(0, weight=1)
        
        # Create a frame for the log
        log_frame = tk.Frame(self.log_tab, bg=COLORS["background"])
        log_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
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
            bg=COLORS["surface"],
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
        
        # Configure text tags for different log levels
        self.log_text.tag_config("error", foreground=COLORS["error"], font=FONTS["log_bold"])
        self.log_text.tag_config("warning", foreground=COLORS["warning"], font=FONTS["log"])
        self.log_text.tag_config("success", foreground=COLORS["success"], font=FONTS["log"])
        self.log_text.tag_config("info", foreground=COLORS["primary"], font=FONTS["log"])
        
        # Add initial log message
        self.log_message("Application started", "INFO")
        
        # Add clear button - centered
        button_frame = tk.Frame(log_frame, bg=COLORS["background"])
        button_frame.grid(row=1, column=0, columnspan=2, pady=10)
        
        clear_button = self._create_modern_button(
            button_frame,
            "Clear Log",
            self.clear_log,
            fg_color=COLORS["text_secondary"],
            text_color="white"
        )
        clear_button.pack()
    
    def clear_log(self):
        """Clear the log text widget."""
        self.log_text.delete('1.0', tk.END)
        self.log_message("Log cleared", "INFO")

    def render(self):
        """Render all GUI components and start the main loop."""
        self.render_tab()
        self.render_main_tab()
        self.render_setting_tab()
        self.render_log_tab()
        self.render_date_time()
        self.root.update_idletasks()
        self.root.mainloop()
