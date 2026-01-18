"""Standard button components with native styling."""

import tkinter as tk
from typing import Callable, Optional

from ..styles.colors import COLORS
from ..styles.fonts import FONTS
from ..styles.sizes import WIDGET_PADY


def create_standard_button(
    parent: tk.Widget,
    text: str,
    command: Optional[Callable[[], None]] = None,
    bg_color: Optional[str] = None,
    fg_color: Optional[str] = None,
    padx: int = 20,
    pady: Optional[int] = None,
    **kwargs,
) -> tk.Button:
    """Create a standard button with native styling."""
    bg_color = bg_color or COLORS["primary"]
    fg_color = fg_color or COLORS["text_primary"]
    pady = pady or WIDGET_PADY

    button = tk.Button(
        parent,
        text=text,
        command=command,
        font=FONTS["button"],
        bg=bg_color,
        fg=fg_color,
        activebackground=COLORS["hover"] if bg_color == COLORS["primary"] else bg_color,
        activeforeground=fg_color,
        relief="raised",
        borderwidth=1,
        padx=padx,
        pady=pady,
        cursor="hand2",
        highlightthickness=1,
        **kwargs,
    )

    button._original_bg = bg_color
    button._original_fg = fg_color

    original_config = button.config

    def enhanced_config(**kw):
        result = original_config(**kw)
        if "state" in kw:
            state = kw["state"]
            if state == "disabled":
                button.config(relief="sunken", cursor="arrow")
            elif state == "normal":
                button.config(
                    bg=button._original_bg,
                    fg=button._original_fg,
                    relief="raised",
                    cursor="hand2",
                )
        return result

    button.config = enhanced_config
    return button


class StandardButton:
    """Factory class for creating standard buttons (backward compatibility)."""

    @staticmethod
    def create(
        parent: tk.Widget,
        text: str,
        command: Optional[Callable] = None,
        fg_color: Optional[str] = None,
        text_color: Optional[str] = None,
        **kwargs,
    ) -> tk.Button:
        """Create a button with native appearance."""
        return create_standard_button(
            parent,
            text,
            command,
            bg_color=fg_color,
            fg_color=text_color,
            **kwargs,
        )
