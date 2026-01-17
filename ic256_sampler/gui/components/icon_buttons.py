"""Icon button components for action buttons with images."""

import tkinter as tk
from typing import Optional, Callable

from ..styles import COLORS
from .tooltip import ToolTip


class IconButton:
    """Factory for creating icon buttons with fixed size."""
    
    @staticmethod
    def create(
        parent: tk.Widget,
        image: tk.PhotoImage,
        command: Optional[Callable] = None,
        tooltip: Optional[str] = None,
        size: tuple = (24, 24),
        **kwargs
    ) -> tk.Button:
        """Create an icon button with fixed size.
        
        Args:
            parent: Parent widget
            image: Button image (PhotoImage)
            command: Optional command callback
            tooltip: Optional tooltip text
            size: Button size (width, height) in pixels
            **kwargs: Additional button options
            
        Returns:
            Configured button widget
        """
        button = tk.Button(
            parent,
            image=image,
            command=command,
            relief="raised",
            bg=COLORS["background"],
            cursor="hand2",
            width=size[0],
            height=size[1],
            **kwargs
        )
        
        if tooltip:
            ToolTip(button, tooltip, 0, 20)
        
        return button
