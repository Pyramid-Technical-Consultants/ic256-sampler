"""Main tab implementation for data collection interface."""

import tkinter as tk
from typing import Callable, Optional

from ..styles import COLORS, FONTS
from ..styles.sizes import ENTRY_PADY, TAB_CONTENT_PADX
from ..components import (
    StandardButton,
    StandardSection,
    ScrollableFrame,
    ButtonGroup,
    LabelValuePair,
    ToolTip,
    EntryWithPlaceholder,
    TimeDisplay,
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
        scrollable_frame = scrollable.get_frame()
        
        # Create padded container for consistent spacing
        main_container = tk.Frame(scrollable_frame, bg=COLORS["background"])
        main_container.grid(row=0, column=0, padx=(TAB_CONTENT_PADX, TAB_CONTENT_PADX), sticky="nsew")
        # Configure grid for sections - column 0 should expand
        main_container.grid_columnconfigure(0, weight=1)
        scrollable_frame.grid_columnconfigure(0, weight=1)
        
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
        # Entry is wrapped in frame for consistent height, place the frame
        note_placeholder.entry_frame.grid(row=0, column=0, sticky="ew", padx=5, pady=ENTRY_PADY)
        ToolTip(self.note_entry, "Optional note to include in the CSV file name and metadata", 0, 20)
    
    def _create_time_section(self, parent: tk.Widget):
        """Create elapsed time display section."""
        time_section = StandardSection.create(
            parent,
            "Elapsed Time",
            row=1,
            column=0
        )
        
        self.time_display = TimeDisplay(time_section)
    
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
        
        # File size display (with spacing from rows)
        self.size_pair = LabelValuePair(
            stats_container,
            "File Size:",
            initial_value="0 B",
            row=0,
            column=2,
            spacing=5
        )
        # Add spacing between the two pairs
        stats_container.grid_columnconfigure(1, minsize=30)
        self.file_size_label = self.size_pair.value
    
    def _create_button_section(self, parent: tk.Widget):
        """Create button section."""
        button_frame = tk.Frame(parent, bg=COLORS["background"])
        button_frame.grid(row=3, column=0, pady=(0, 0))
        button_group = ButtonGroup(
            button_frame,
            [("Start", self.start_callback), ("Stop", self.stop_callback)],
            row=0,
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
        self.time_display.update(minute, second, ticks)
    
    def reset_elapse_time(self):
        """Reset elapsed time display to zero."""
        self.time_display.reset()
    
    def update_statistics(self, rows: int, file_size: str):
        """Update statistics display."""
        self.rows_label.config(text=f"{rows:,}")
        self.file_size_label.config(text=file_size)
    
    def reset_statistics(self):
        """Reset statistics display to zero."""
        self.rows_label.config(text="0")
        self.file_size_label.config(text="0 B")
