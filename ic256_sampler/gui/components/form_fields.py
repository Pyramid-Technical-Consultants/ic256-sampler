"""Form field components for label + entry + optional button patterns."""

import tkinter as tk
from typing import Optional, Callable

from ..styles import COLORS, FONTS
from ..styles.sizes import WIDGET_PADY
from .entries import StandardEntry
from .icon_buttons import IconButton
from .tooltip import ToolTip


class FormField:
    """A form field with label, entry, and optional action button."""

    def __init__(
        self,
        parent: tk.Widget,
        label_text: str,
        row: int = 0,
        column: int = 0,
        entry_width: int = 30,
        label_width: int = 18,
        entry_state: str = "normal",
        tooltip: Optional[str] = None,
        entry_tooltip: Optional[str] = None,
        change_callback: Optional[Callable] = None,
        **entry_kwargs,
    ):
        """Create a form field with label and entry."""
        tooltip_text = entry_tooltip or tooltip
        invalid_options = {"entry_tooltip", "entry_state", "tooltip"}
        filtered_kwargs = {k: v for k, v in entry_kwargs.items() if k not in invalid_options}

        self.label = tk.Label(
            parent,
            font=FONTS["label"],
            text=label_text,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=label_width,
            anchor="w",
        )
        self.label.grid(row=row, column=column, padx=(5, 10), pady=WIDGET_PADY, sticky="w")

        self.entry = StandardEntry.create(parent, width=entry_width, **filtered_kwargs)
        self.entry.config(state=entry_state)
        self.entry._entry_frame.grid(row=row, column=column + 1, padx=(0, 5), pady=WIDGET_PADY, sticky="ew")

        parent.grid_columnconfigure(column + 1, weight=1)

        if tooltip_text:
            ToolTip(self.entry, tooltip_text, 0, 20)

        if change_callback:
            self.entry.bind("<KeyRelease>", lambda e: change_callback())
            self.entry.bind("<FocusOut>", lambda e: change_callback())

    def get(self) -> str:
        """Get entry value."""
        return self.entry.get()

    def set(self, value: str):
        """Set entry value."""
        state = self.entry.cget("state")
        if state == "readonly":
            self.entry.config(state="normal")
            self.entry.delete(0, tk.END)
            self.entry.insert(0, value)
            self.entry.config(state="readonly")
        else:
            self.entry.delete(0, tk.END)
            self.entry.insert(0, value)


class FormFieldWithButton(FormField):
    """A form field with label, entry, and action button."""

    def __init__(
        self,
        parent: tk.Widget,
        label_text: str,
        row: int = 0,
        column: int = 0,
        entry_width: int = 30,
        label_width: int = 18,
        button_image: Optional[tk.PhotoImage] = None,
        button_command: Optional[Callable] = None,
        button_tooltip: Optional[str] = None,
        entry_state: str = "normal",
        entry_tooltip: Optional[str] = None,
        change_callback: Optional[Callable] = None,
        **entry_kwargs,
    ):
        """Create a form field with label, entry, and button."""
        super().__init__(
            parent,
            label_text,
            row,
            column,
            entry_width,
            label_width,
            entry_state,
            entry_tooltip=entry_tooltip,
            change_callback=change_callback,
            **entry_kwargs,
        )

        self.button = None
        if button_image and button_command:
            self.button = IconButton.create(
                parent,
                button_image,
                command=button_command,
                tooltip=button_tooltip,
            )
            self.button.grid(row=row, column=column + 2, padx=(0, 5), pady=WIDGET_PADY)
