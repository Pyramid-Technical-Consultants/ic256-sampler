"""Time display component for showing elapsed time."""

import tkinter as tk

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
        initial_ticks: str = "000",
    ):
        """Create a time display."""
        self.container = tk.Frame(parent, bg=COLORS["background"])
        self.container.grid(row=row, column=column, padx=5, pady=5)

        font = FONTS["time_display"]
        label_config = {
            "font": font,
            "bg": COLORS["background"],
            "fg": COLORS["text_primary"],
        }

        self.minute = tk.Label(self.container, text=initial_minute, width=3, **label_config)
        self.minute.grid(row=0, column=0, padx=2)

        tk.Label(self.container, text=":", **label_config).grid(row=0, column=1, padx=2)

        self.second = tk.Label(self.container, text=initial_second, width=3, **label_config)
        self.second.grid(row=0, column=2, padx=2)

        tk.Label(self.container, text=":", **label_config).grid(row=0, column=3, padx=2)

        self.ticks = tk.Label(self.container, text=initial_ticks, width=4, **label_config)
        self.ticks.grid(row=0, column=4, padx=2)

    def update(self, minute: str, second: str, ticks: str):
        """Update the time display."""
        self.minute.config(text=minute)
        self.second.config(text=second)
        self.ticks.config(text=ticks)

    def reset(self):
        """Reset the time display to zero."""
        self.update("00", "00", "000")
