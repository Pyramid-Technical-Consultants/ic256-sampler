"""Time display component for showing elapsed time."""

import tkinter as tk
from typing import Optional

from ..styles import COLORS, FONTS


class TimeDisplay:
    """A time display component showing minutes:seconds:ticks."""
    
    def __init__(
        self,
        parent: tk.Widget,
        row: int = 0,
        column: int = 0,
        initial_minute: str = "00",
        initial_second: str = "00",
        initial_ticks: str = "000"
    ):
        """Create a time display.
        
        Args:
            parent: Parent widget
            row: Grid row position
            column: Grid column position
            initial_minute: Initial minute value
            initial_second: Initial second value
            initial_ticks: Initial ticks value
        """
        self.container = tk.Frame(parent, bg=COLORS["background"])
        self.container.grid(row=row, column=column, padx=5, pady=5)
        
        # Minute label
        self.minute = tk.Label(
            self.container,
            font=FONTS["time_display"],
            text=initial_minute,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=3
        )
        self.minute.grid(row=0, column=0, padx=2)
        
        # First colon
        colon_1 = tk.Label(
            self.container,
            font=FONTS["time_display"],
            text=":",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        colon_1.grid(row=0, column=1, padx=2)
        
        # Second label
        self.second = tk.Label(
            self.container,
            font=FONTS["time_display"],
            text=initial_second,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=3
        )
        self.second.grid(row=0, column=2, padx=2)
        
        # Second colon
        colon_2 = tk.Label(
            self.container,
            font=FONTS["time_display"],
            text=":",
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        colon_2.grid(row=0, column=3, padx=2)
        
        # Ticks label
        self.ticks = tk.Label(
            self.container,
            font=FONTS["time_display"],
            text=initial_ticks,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=4
        )
        self.ticks.grid(row=0, column=4, padx=2)
    
    def update(self, minute: str, second: str, ticks: str):
        """Update the time display.
        
        Args:
            minute: Minute value
            second: Second value
            ticks: Ticks value
        """
        self.minute.config(text=minute)
        self.second.config(text=second)
        self.ticks.config(text=ticks)
    
    def reset(self):
        """Reset the time display to zero."""
        self.update("00", "00", "000")
