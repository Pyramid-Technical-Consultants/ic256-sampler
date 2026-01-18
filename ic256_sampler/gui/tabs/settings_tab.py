"""Settings tab implementation for device and path configuration."""

import tkinter as tk
from typing import Callable
from tkinter import filedialog

from ..styles import COLORS, FONTS
from ..styles.sizes import BUTTON_PADY, CONTENT_PAD
from ..components import (
    StandardButton,
    StandardSection,
    ScrollableFrame,
    FormField,
    FormFieldWithButton,
    IconButton,
    ToolTip,
)
from ..utils import create_scrollable_tab_content, open_directory
from ...config import update_file_json, init_ip
from ...utils import is_valid_device


class SettingsTab:
    """Settings tab for device and path configuration."""
    
    def __init__(
        self,
        parent: tk.Widget,
        setup_callback: Callable,
        image_loader,
        update_icon_callback: Callable,
        update_tab_title_callback: Callable = None
    ):
        """Initialize settings tab.
        
        Args:
            parent: Parent widget (ttk.Frame from notebook)
            setup_callback: Callback for setup button
            image_loader: ImageLoader instance
            update_icon_callback: Callback(button, entry, device_name) for icon updates
            update_tab_title_callback: Callback(text) to update tab title
        """
        self.parent = parent
        self.setup_callback = setup_callback
        self.image_loader = image_loader
        self._update_icon_callback = update_icon_callback
        self._update_tab_title = update_tab_title_callback
        self._has_unsaved_changes = False
        self._initial_values = {}
        
        # Configure tab for resizing
        self.parent.grid_rowconfigure(0, weight=1)
        self.parent.grid_columnconfigure(0, weight=1)
        
        # Create scrollable frame
        scrollable = ScrollableFrame(self.parent)
        scrollable_frame = scrollable.get_frame()
        
        # Create padded container for consistent spacing
        main_container = tk.Frame(scrollable_frame, bg=COLORS["background"])
        main_container.grid(row=0, column=0, padx=(CONTENT_PAD, CONTENT_PAD), pady=(CONTENT_PAD, CONTENT_PAD), sticky="nsew")
        # Configure grid for sections - column 0 should expand
        main_container.grid_columnconfigure(0, weight=1)
        scrollable_frame.grid_columnconfigure(0, weight=1)
        
        # Device configuration section
        self._create_device_section(main_container)
        
        # Sampling rate section
        self._create_sampling_section(main_container)
        
        # Path configuration section
        self._create_path_section(main_container)
        
        # Save configuration button
        self._create_save_section(main_container)
        
        # Initialize IP addresses from config
        init_ip(
            self.ix256_a_entry,
            self.tx2_entry,
            self.path_entry,
            self.sampling_entry,
        )
        
        # Store initial values for change detection
        self._store_initial_values()
        
        # Bind to entry changes to track unsaved changes
        self._setup_change_tracking()
    
    def _create_device_section(self, parent: tk.Widget):
        """Create device configuration section."""
        device_section = StandardSection.create(
            parent,
            "Device Configuration",
            row=0,
            column=0
        )
        device_section.grid_columnconfigure(1, weight=1)
        
        # IC256 device entry with button
        search_icon = self.image_loader.load_image("search.png", (20, 20))
        ic256_field = FormFieldWithButton(
            device_section,
            "IC256-42/35:",
            row=0,
            column=0,
            entry_width=30,
            button_image=search_icon,
            button_command=lambda: self._update_icon_callback(ic256_field.button, ic256_field.entry, "IC256"),
            button_tooltip="Check IP address",
            entry_tooltip="IC256 device IP address (e.g., 192.168.1.100). Click search icon to validate.",
            change_callback=self._on_setting_changed
        )
        self.ix256_a_entry = ic256_field.entry
        self.ix256_a_button = ic256_field.button
        
        # TX2 device entry with button
        tx2_field = FormFieldWithButton(
            device_section,
            "TX2:",
            row=1,
            column=0,
            entry_width=30,
            button_image=search_icon,
            button_command=lambda: self._update_icon_callback(tx2_field.button, tx2_field.entry, "TX2"),
            button_tooltip="Check IP address",
            entry_tooltip="TX2 device IP address (e.g., 192.168.1.101). Click search icon to validate.",
            change_callback=self._on_setting_changed
        )
        self.tx2_entry = tx2_field.entry
        self.tx2_button = tx2_field.button
    
    def _create_sampling_section(self, parent: tk.Widget):
        """Create sampling rate configuration section."""
        sampling_section = StandardSection.create(
            parent,
            "Sampling Configuration",
            row=1,
            column=0
        )
        sampling_section.grid_columnconfigure(1, weight=1)
        
        # Sampling rate entry
        self.entry_var = tk.StringVar()
        self.entry_var.trace("w", lambda *args: (self._sampling_change(*args), self._on_setting_changed()))
        
        sampling_field = FormField(
            sampling_section,
            "Sampling Rate (1-6000 Hz):",
            row=0,
            column=0,
            entry_width=15,
            entry_tooltip="Sampling rate in Hz (1-6000). Default: 500 Hz",
            change_callback=self._on_setting_changed,
            textvariable=self.entry_var
        )
        self.sampling_entry = sampling_field.entry
        
        # Apply button
        self.set_up_button = StandardButton.create(
            sampling_section,
            "Apply",
            self.setup_callback,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.set_up_button.grid(row=0, column=2, padx=(0, 5), pady=BUTTON_PADY)
        ToolTip(self.set_up_button, "Apply sampling rate configuration to all devices", 0, 20)
    
    def _create_path_section(self, parent: tk.Widget):
        """Create file path configuration section."""
        path_section = StandardSection.create(
            parent,
            "File Path Configuration",
            row=2,
            column=0
        )
        path_section.grid_columnconfigure(1, weight=1)
        
        # Path entry field
        path_field = FormField(
            path_section,
            "Save Path:",
            row=0,
            column=0,
            entry_width=30,
            entry_state="readonly",
            entry_tooltip="Directory where CSV files will be saved. Click Browse to select.",
            change_callback=self._on_setting_changed
        )
        self.path_entry = path_field.entry
        
        # Browse button
        browse_button = StandardButton.create(
            path_section,
            "Browse",
            self._select_directory,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        browse_button.grid(row=0, column=2, padx=(0, 5), pady=BUTTON_PADY)
        
        # Open folder button
        open_icon = self.image_loader.load_image("open_folder.png", (13, 13))
        open_button = IconButton.create(
            path_section,
            open_icon,
            command=self._open_directory,
            size=(20, 20)
        )
        open_button.grid(row=0, column=3, padx=(0, 5), pady=BUTTON_PADY)
    
    def _create_save_section(self, parent: tk.Widget):
        """Create save configuration section."""
        save_setting = StandardButton.create(
            parent,
            "Update Configuration",
            self._save_configuration,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        save_setting.grid(row=3, column=0, pady=(0, 0))
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
            self._on_setting_changed()
    
    def _open_directory(self):
        """Open the directory in the system's default file manager."""
        path = self.path_entry.get()
        open_directory(path)
    
    def _store_initial_values(self):
        """Store initial values for change detection."""
        self._initial_values = {
            'ix256_a': self.ix256_a_entry.get(),
            'tx2': self.tx2_entry.get(),
            'path': self.path_entry.get(),
            'sampling': self.sampling_entry.get(),
        }
    
    def _setup_change_tracking(self):
        """Set up change tracking for all settings."""
        # The entry bindings are already set up in _create_device_section and _create_sampling_section
        # Just need to check on initial load
        self._check_for_changes()
    
    def _check_for_changes(self):
        """Check if current values differ from initial values."""
        current_values = {
            'ix256_a': self.ix256_a_entry.get(),
            'tx2': self.tx2_entry.get(),
            'path': self.path_entry.get(),
            'sampling': self.sampling_entry.get(),
        }
        
        has_changes = current_values != self._initial_values
        if has_changes != self._has_unsaved_changes:
            self._has_unsaved_changes = has_changes
            self._update_tab_title_indicator()
    
    def _on_setting_changed(self):
        """Called when any setting changes."""
        self._check_for_changes()
    
    def _update_tab_title_indicator(self):
        """Update tab title to show/hide asterisk for unsaved changes."""
        if self._update_tab_title:
            if self._has_unsaved_changes:
                self._update_tab_title("Settings *")
            else:
                self._update_tab_title("Settings")
    
    def _save_configuration(self):
        """Save configuration and clear unsaved changes indicator."""
        update_file_json(
            self.ix256_a_entry,
            self.tx2_entry,
            self.path_entry,
            self.sampling_entry,
        )
        # Update initial values to current values
        self._store_initial_values()
        # Clear unsaved changes indicator
        self._has_unsaved_changes = False
        self._update_tab_title_indicator()
