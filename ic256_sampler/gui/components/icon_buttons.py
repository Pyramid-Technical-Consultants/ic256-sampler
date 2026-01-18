"""Icon button components for action buttons with images."""

import tkinter as tk
import tkinter.font as tkfont
from typing import Optional, Callable

from ..styles import COLORS
from ..styles.fonts import FONTS
from ..styles.sizes import WIDGET_PADY
from .tooltip import ToolTip


def create_icon_button(
    parent: tk.Widget,
    image: tk.PhotoImage,
    command: Optional[Callable[[], None]] = None,
    tooltip: Optional[str] = None,
    width: int = 24,
    pady: Optional[int] = None,
    **kwargs,
) -> tk.Button:
    """Create an icon button with consistent sizing matching standard buttons."""
    if pady is None:
        pady = kwargs.pop("pady", WIDGET_PADY)
    else:
        kwargs.pop("pady", None)

    font = tkfont.Font(font=FONTS["button"])
    button_height = font.metrics("linespace") + (2 * pady) + 4

    button = tk.Button(
        parent,
        image=image,
        command=command,
        width=width,
        height=button_height,
        font=FONTS["button"],
        relief="raised",
        borderwidth=1,
        bg=COLORS["background"],
        cursor="hand2",
        highlightthickness=1,
        **kwargs,
    )

    if tooltip:
        ToolTip(button, tooltip, 0, 20)

    return button


class IconButton:
    """Factory class for creating icon buttons (backward compatibility)."""

    @staticmethod
    def create(
        parent: tk.Widget,
        image: tk.PhotoImage,
        command: Optional[Callable] = None,
        tooltip: Optional[str] = None,
        size: Optional[tuple[int, int]] = None,
        **kwargs,
    ) -> tk.Button:
        """Create an icon button with fixed size."""
        width = size[0] if size else 24
        return create_icon_button(parent, image, command, tooltip, width=width, **kwargs)
