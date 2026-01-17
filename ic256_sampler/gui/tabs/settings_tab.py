"""Settings tab implementation for device and path configuration."""

import tkinter as tk
from typing import Callable
from tkinter import filedialog

from ..styles import COLORS, FONTS
from ..components import StandardButton, StandardEntry, ToolTip
from ...config import update_file_json, init_ip
from ...utils import is_valid_device


class SettingsTab:
    """Settings tab for device and path configuration."""
    
    def __init__(
        self,
        parent: tk.Widget,
        setup_callback: Callable,
        image_loader,
        update_icon_callback: Callable
    ):
        """Initialize settings tab.
        
        Args:
            parent: Parent widget (ttk.Frame from notebook)
            setup_callback: Callback for setup button
            image_loader: ImageLoader instance
            update_icon_callback: Callback(button, entry, device_name) for icon updates
        """
        self.parent = parent
        self.setup_callback = setup_callback
        self.image_loader = image_loader
        self._update_icon_callback = update_icon_callback
        
        # Configure tab for resizing
        self.parent.grid_rowconfigure(0, weight=1)
        self.parent.grid_columnconfigure(0, weight=1)
        
        # Main container
        main_container = tk.Frame(self.parent, bg=COLORS["background"])
        main_container.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
        main_container.grid_rowconfigure(0, weight=1)
        main_container.grid_columnconfigure(0, weight=1)
        
        # Centered content frame
        content_frame = tk.Frame(main_container, bg=COLORS["background"])
        content_frame.grid(row=0, column=0)
        content_frame.grid_columnconfigure(0, weight=1)
        
        # Device configuration section
        self._create_device_section(content_frame)
        
        # Sampling rate section
        self._create_sampling_section(content_frame)
        
        # Path configuration section
        self._create_path_section(content_frame)
        
        # Save configuration button
        self._create_save_section(content_frame)
        
        # Initialize IP addresses from config
        init_ip(
            self.ix256_a_entry,
            self.tx2_entry,
            self.path_entry,
            self.sampling_entry,
        )
    
    def _create_device_section(self, parent: tk.Widget):
        """Create device configuration section."""
        device_section = tk.LabelFrame(
            parent,
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
        
        self.ix256_a_entry = StandardEntry.create(device_section, width=30)
        self.ix256_a_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        ToolTip(self.ix256_a_entry, "IC256 device IP address (e.g., 192.168.1.100). Click search icon to validate.", 0, 20)
        
        self.ix256_a_button = tk.Button(
            device_section,
            image=self.image_loader.load_image("search.png", (13, 13)),
            command=lambda: self._update_icon_callback(self.ix256_a_button, self.ix256_a_entry, "IC256"),
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
        
        self.tx2_entry = StandardEntry.create(device_section, width=30)
        self.tx2_entry.grid(row=1, column=1, padx=10, pady=10, sticky="ew")
        ToolTip(self.tx2_entry, "TX2 device IP address (e.g., 192.168.1.101). Click search icon to validate.", 0, 20)
        
        self.tx2_button = tk.Button(
            device_section,
            image=self.image_loader.load_image("search.png", (13, 13)),
            command=lambda: self._update_icon_callback(self.tx2_button, self.tx2_entry, "TX2"),
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2"
        )
        self.tx2_button.grid(row=1, column=2, padx=5, pady=10)
        ToolTip(self.tx2_button, "Check IP address", 0, 20)
    
    def _create_sampling_section(self, parent: tk.Widget):
        """Create sampling rate configuration section."""
        sampling_section = tk.LabelFrame(
            parent,
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
        self.entry_var.trace("w", self._sampling_change)
        
        self.sampling_entry = StandardEntry.create(
            sampling_section,
            width=20,
            textvariable=self.entry_var
        )
        self.sampling_entry.grid(row=0, column=1, padx=10, pady=10, sticky="w")
        ToolTip(self.sampling_entry, "Sampling rate in Hz (1-6000). Default: 500 Hz", 0, 20)
        
        self.set_up_button = StandardButton.create(
            sampling_section,
            "Apply",
            self.setup_callback,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.set_up_button.grid(row=0, column=2, padx=10, pady=10)
        ToolTip(self.set_up_button, "Apply sampling rate configuration to all devices", 20, -20)
    
    def _create_path_section(self, parent: tk.Widget):
        """Create file path configuration section."""
        path_section = tk.LabelFrame(
            parent,
            text="File Path Configuration",
            font=FONTS["heading"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="groove",
            padx=15,
            pady=10
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
        
        self.path_entry = StandardEntry.create(path_section, width=35, state="readonly")
        self.path_entry.grid(row=0, column=1, padx=10, pady=10, sticky="ew")
        ToolTip(self.path_entry, "Directory where CSV files will be saved. Click Browse to select.", 0, 20)
        
        browse_button = StandardButton.create(
            path_section,
            "Browse",
            self._select_directory,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        browse_button.grid(row=0, column=2, padx=5, pady=10)
        
        open_button = tk.Button(
            path_section,
            image=self.image_loader.load_image("open_folder.png", (13, 13)),
            command=self._open_directory,
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2"
        )
        open_button.grid(row=0, column=3, padx=5, pady=10)
    
    def _create_save_section(self, parent: tk.Widget):
        """Create save configuration section."""
        save_section = tk.Frame(parent, bg=COLORS["background"])
        save_section.grid(row=3, column=0, pady=20)
        
        save_setting = StandardButton.create(
            save_section,
            "Update Configuration",
            lambda: update_file_json(
                self.ix256_a_entry,
                self.tx2_entry,
                self.path_entry,
                self.sampling_entry,
            ),
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        save_setting.pack()
        ToolTip(save_setting, "Save current settings to configuration file", 0, 20)
    
    def _sampling_change(self, *args):
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
    
    def _select_directory(self):
        """Open directory selection dialog."""
        directory_path = filedialog.askdirectory(
            initialdir=self.path_entry.get(), title="Select a Directory"
        )
        if directory_path:
            self.path_entry.config(state="normal")
            self.path_entry.delete(0, tk.END)
            self.path_entry.insert(0, directory_path)
            self.path_entry.config(state="readonly")
    
    def _open_directory(self):
        """Open the directory in the system's default file manager."""
        import os
        import sys
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
                # Error handling would be done by GUI's show_message
                pass
