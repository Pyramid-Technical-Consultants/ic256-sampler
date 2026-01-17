"""Main tab implementation for data collection interface."""

import tkinter as tk
from typing import Callable, Optional

from ..styles import COLORS, FONTS
from ..components import (
    StandardButton,
    StandardSection,
    ScrollableFrame,
    ButtonGroup,
    LabelValuePair,
    ToolTip,
    EntryWithPlaceholder,
)


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
        
        # Create scrollable frame
        scrollable = ScrollableFrame(self.parent)
        main_container = scrollable.get_frame()
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
        note_section = StandardSection.create(
            parent,
            "Session Note",
            row=0,
            column=0
        )
        note_section.grid_columnconfigure(0, weight=1)
        
        note_placeholder = EntryWithPlaceholder(
            note_section,
            "Enter a note for this data collection session...",
            width=50,
            font=FONTS["entry_large"]
        )
        self.note_entry_wrapper = note_placeholder
        self.note_entry = note_placeholder.get_widget()
        self.note_entry.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        ToolTip(self.note_entry, "Optional note to include in the CSV file name and metadata", 0, 20)
    
    def _create_time_section(self, parent: tk.Widget):
        """Create elapsed time display section."""
        time_section = StandardSection.create(
            parent,
            "Elapsed Time",
            row=1,
            column=0
        )
        
        time_display_frame = tk.Frame(time_section, bg=COLORS["background"])
        time_display_frame.grid(row=0, column=0, padx=5, pady=5)
        
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
        stats_section = StandardSection.create(
            parent,
            "Statistics",
            row=2,
            column=0
        )
        
        # Statistics container
        stats_container = tk.Frame(stats_section, bg=COLORS["background"])
        stats_container.grid(row=0, column=0, padx=5, pady=5, sticky="w")
        
        # Rows display
        self.rows_pair = LabelValuePair(
            stats_container,
            "Rows:",
            initial_value="0",
            row=0,
            column=0,
            spacing=5
        )
        self.rows_label = self.rows_pair.value
        
        # File size display
        self.size_pair = LabelValuePair(
            stats_container,
            "File Size:",
            initial_value="0 B",
            row=0,
            column=2,
            spacing=5
        )
        self.file_size_label = self.size_pair.value
    
    def _create_button_section(self, parent: tk.Widget):
        """Create button section."""
        button_group = ButtonGroup(
            parent,
            [("Start", self.start_callback), ("Stop", self.stop_callback)],
            row=3,
            column=0
        )
        self.start_button = button_group.get_button(0)
        self.stop_button = button_group.get_button(1)
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
