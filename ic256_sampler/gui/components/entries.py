"""Standard entry components with native styling."""

import tkinter as tk

from ..styles.colors import COLORS
from ..styles.fonts import FONTS
from ..styles.sizes import WIDGET_PADY


class StandardEntry:
    """Factory for creating standard entry fields with native appearance."""

    @staticmethod
    def create(
        parent: tk.Widget,
        width: int = 30,
        font: tuple = None,
        ipady: int = None,
        **kwargs,
    ) -> tk.Entry:
        """Create a standard entry field with native styling."""
        frame = tk.Frame(parent, bg=COLORS["background"])

        entry = tk.Entry(
            frame,
            width=width,
            font=font or FONTS["entry"],
            relief="sunken",
            borderwidth=1,
            highlightthickness=1,
            insertbackground=COLORS["text_primary"],
            **kwargs,
        )

        entry.pack(fill="both", expand=True, ipadx=10, ipady=ipady or WIDGET_PADY)
        entry._entry_frame = frame
        return entry


class EntryWithPlaceholder:
    """Entry field with placeholder text support."""

    def __init__(
        self,
        parent: tk.Widget,
        placeholder: str,
        width: int = 30,
        font: tuple = None,
        **kwargs,
    ):
        """Initialize entry with placeholder."""
        self.placeholder = placeholder
        self.entry = StandardEntry.create(parent, width=width, font=font, **kwargs)
        self.entry_frame = self.entry._entry_frame

        self.entry.insert(0, placeholder)
        self.entry.config(fg=COLORS["text_secondary"])
        self.entry.bind("<FocusIn>", self._on_focus_in)
        self.entry.bind("<FocusOut>", self._on_focus_out)

    def _on_focus_in(self, event):
        """Handle entry focus in event - clear placeholder."""
        if self.entry.get() == self.placeholder:
            self.entry.delete(0, tk.END)
            self.entry.config(fg=COLORS["text_primary"])

    def _on_focus_out(self, event):
        """Handle entry focus out event - restore placeholder if empty."""
        if not self.entry.get().strip():
            self.entry.insert(0, self.placeholder)
            self.entry.config(fg=COLORS["text_secondary"])

    def get(self) -> str:
        """Get entry value, returning empty string if placeholder is present."""
        value = self.entry.get().strip()
        return "" if value == self.placeholder else value

    def get_widget(self) -> tk.Entry:
        """Get the underlying entry widget."""
        return self.entry
