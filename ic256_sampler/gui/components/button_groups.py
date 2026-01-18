"""Button group components for organizing multiple buttons."""

import tkinter as tk
from typing import List, Tuple, Optional, Callable

from ..styles import COLORS
from .buttons import StandardButton


class ButtonGroup:
    """A horizontal group of buttons with consistent spacing."""

    def __init__(
        self,
        parent: tk.Widget,
        buttons: List[Tuple[str, Optional[Callable]]],
        row: int = 0,
        column: int = 0,
        pady: tuple = (0, 0),
        spacing: int = 10,
    ):
        """Create a button group."""
        self.button_frame = tk.Frame(parent, bg=COLORS["background"])
        self.button_frame.grid(row=row, column=column, pady=pady)

        self.buttons = []
        for i, (text, command) in enumerate(buttons):
            button = StandardButton.create(
                self.button_frame,
                text,
                command,
                fg_color=COLORS["primary"],
                text_color=COLORS["text_primary"],
            )
            padx = (0, spacing) if i < len(buttons) - 1 else (0, 0)
            button.grid(row=0, column=i, padx=padx)
            self.buttons.append(button)

    def get_button(self, index: int) -> tk.Button:
        """Get button by index."""
        return self.buttons[index]
