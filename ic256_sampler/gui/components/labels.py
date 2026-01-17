"""Label components for consistent text display."""

import tkinter as tk
from typing import Optional

from ..styles import COLORS, FONTS


class StandardLabel:
    """Factory for creating standard labels with consistent styling."""
    
    @staticmethod
    def create(
        parent: tk.Widget,
        text: str,
        font_key: str = "label",
        width: Optional[int] = None,
        anchor: str = "w",
        **kwargs
    ) -> tk.Label:
        """Create a standard label.
        
        Args:
            parent: Parent widget
            text: Label text
            font_key: Font key from FONTS dict (default: "label")
            width: Optional label width in characters
            anchor: Text anchor (default: "w" for left-aligned)
            **kwargs: Additional label options
            
        Returns:
            Configured label widget
        """
        label_kwargs = {
            "font": FONTS.get(font_key, FONTS["label"]),
            "bg": COLORS["background"],
            "fg": COLORS["text_primary"],
            "anchor": anchor,
            **kwargs
        }
        
        if width is not None:
            label_kwargs["width"] = width
        
        return tk.Label(parent, text=text, **label_kwargs)


class LabelValuePair:
    """A label-value pair for displaying information."""
    
    def __init__(
        self,
        parent: tk.Widget,
        label_text: str,
        initial_value: str = "",
        row: int = 0,
        column: int = 0,
        label_font: str = "label_small",
        value_font: str = "label_medium",
        label_color: str = "text_secondary",
        value_color: str = "text_primary",
        spacing: int = 5
    ):
        """Create a label-value pair.
        
        Args:
            parent: Parent widget
            label_text: Label text
            initial_value: Initial value text
            row: Grid row position
            column: Grid column position
            label_font: Font key for label
            value_font: Font key for value
            label_color: Color key for label
            value_color: Color key for value
            spacing: Spacing between label and value
        """
        # Label
        self.label = tk.Label(
            parent,
            text=label_text,
            font=FONTS[label_font],
            bg=COLORS["background"],
            fg=COLORS[label_color]
        )
        self.label.grid(row=row, column=column, padx=(0, spacing))
        
        # Value
        self.value = tk.Label(
            parent,
            text=initial_value,
            font=FONTS[value_font],
            bg=COLORS["background"],
            fg=COLORS[value_color]
        )
        self.value.grid(row=row, column=column + 1)
    
    def set_value(self, value: str):
        """Update the value text."""
        self.value.config(text=value)
    
    def get_value(self) -> str:
        """Get the current value text."""
        return self.value.cget("text")
