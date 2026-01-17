"""Main tab implementation for data collection interface."""

import tkinter as tk
from typing import Callable, Optional

from ..styles import COLORS, FONTS
from ..components import StandardButton, ToolTip, EntryWithPlaceholder


class MainTab:
    """Main tab for data collection interface."""
    
    def __init__(
        self,
        parent: tk.Widget,
        start_callback: Callable,
        stop_callback: Callable,
        image_loader
    ):
        """Initialize main tab.
        
        Args:
            parent: Parent widget (ttk.Frame from notebook)
            start_callback: Callback for start button
            stop_callback: Callback for stop button
            image_loader: ImageLoader instance
        """
        self.parent = parent
        self.start_callback = start_callback
        self.stop_callback = stop_callback
        self.image_loader = image_loader
        
        # Configure tab for responsive resizing
        self.parent.grid_rowconfigure(0, weight=1)
        self.parent.grid_columnconfigure(0, weight=1)
        
        # Main container
        main_container = tk.Frame(self.parent, bg=COLORS["background"])
        main_container.grid(row=0, column=0, sticky="nsew", padx=20, pady=20)
        main_container.grid_columnconfigure(0, weight=1)
        
        # Note section
        self._create_note_section(main_container)
        
        # Elapsed time section
        self._create_time_section(main_container)
        
        # Statistics section
        self._create_statistics_section(main_container)
        
        # Button section
        self._create_button_section(main_container)
    
    def _create_note_section(self, parent: tk.Widget):
        """Create note input section."""
        note_label = tk.Label(
            parent,
            font=FONTS["label"],
            text="Note:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            anchor="w"
        )
        note_label.grid(row=0, column=0, sticky="w", pady=(0, 5))
        
        note_placeholder = EntryWithPlaceholder(
            parent,
            "Enter a note for this data collection session...",
            width=50,
            font=FONTS["entry_large"]
        )
        self.note_entry_wrapper = note_placeholder
        self.note_entry = note_placeholder.get_widget()
        self.note_entry.grid(row=1, column=0, sticky="ew", pady=(0, 15))
        parent.grid_columnconfigure(0, weight=1)
        ToolTip(self.note_entry, "Optional note to include in the CSV file name and metadata", 0, 20)
    
    def _create_time_section(self, parent: tk.Widget):
        """Create elapsed time display section."""
        time_label = tk.Label(
            parent,
            font=FONTS["label"],
            text="Elapsed Time:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        time_label.grid(row=2, column=0, sticky="w", pady=(0, 5))
        
        time_display_frame = tk.Frame(parent, bg=COLORS["background"])
        time_display_frame.grid(row=3, column=0, pady=(0, 15))
        
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
    
    def _create_statistics_section(self, parent: tk.Widget):
        """Create statistics display section."""
        stats_label = tk.Label(
            parent,
            font=FONTS["label"],
            text="Statistics:",
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            anchor="w"
        )
        stats_label.grid(row=4, column=0, sticky="w", pady=(0, 5))
        
        # Statistics container
        stats_container = tk.Frame(parent, bg=COLORS["background"])
        stats_container.grid(row=5, column=0, sticky="w", pady=(0, 15))
        
        # Rows display
        rows_frame = tk.Frame(stats_container, bg=COLORS["background"])
        rows_frame.grid(row=0, column=0, padx=(0, 30))
        
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
            fg=COLORS["text_primary"]
        )
        self.rows_label.grid(row=0, column=1)
        
        # File size display
        size_frame = tk.Frame(stats_container, bg=COLORS["background"])
        size_frame.grid(row=0, column=1)
        
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
    
    def _create_button_section(self, parent: tk.Widget):
        """Create button section."""
        button_container = tk.Frame(parent, bg=COLORS["background"])
        button_container.grid(row=6, column=0, pady=(10, 0))
        
        self.start_button = StandardButton.create(
            button_container,
            "Start",
            self.start_callback,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.start_button.grid(row=0, column=0, padx=(0, 10))
        
        self.stop_button = StandardButton.create(
            button_container,
            "Stop",
            self.stop_callback,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        self.stop_button.grid(row=0, column=1)
        self.stop_button.config(state="disabled")
    
    def get_note_value(self) -> str:
        """Get note entry value, returning empty string if placeholder is present."""
        if hasattr(self, 'note_entry_wrapper'):
            return self.note_entry_wrapper.get()
        # Fallback if wrapper not available
        note = self.note_entry.get().strip()
        placeholder = "Enter a note for this data collection session..."
        if note == placeholder:
            return ""
        return note
    
    def update_elapse_time(self, minute: str, second: str, ticks: str):
        """Update elapsed time display."""
        self.minute.config(text=minute)
        self.second.config(text=second)
        self.ticks.config(text=ticks)
    
    def reset_elapse_time(self):
        """Reset elapsed time display to zero."""
        self.minute.config(text="00")
        self.second.config(text="00")
        self.ticks.config(text="000")
    
    def update_statistics(self, rows: int, file_size: str):
        """Update statistics display."""
        self.rows_label.config(text=f"{rows:,}")
        self.file_size_label.config(text=file_size)
    
    def reset_statistics(self):
        """Reset statistics display to zero."""
        self.rows_label.config(text="0")
        self.file_size_label.config(text="0 B")
