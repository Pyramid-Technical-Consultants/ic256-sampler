"""Section/fieldset components for grouping related controls."""

import tkinter as tk

from ..styles import COLORS, FONTS


class StandardSection:
    """Factory for creating standard LabelFrame sections with consistent styling."""
    
    @staticmethod
    def create(
        parent: tk.Widget,
        text: str,
        row: int = 0,
        column: int = 0,
        sticky: str = "ew",
        pady: tuple = (0, 15),
        **kwargs
    ) -> tk.LabelFrame:
        """Create a standard LabelFrame section.
        
        Args:
            parent: Parent widget
            text: Section title text
            row: Grid row position
            column: Grid column position
            sticky: Grid sticky option
            pady: Vertical padding tuple (top, bottom)
            **kwargs: Additional LabelFrame options
            
        Returns:
            Configured LabelFrame widget
        """
        section = tk.LabelFrame(
            parent,
            text=text,
            font=FONTS["heading"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="groove",
            padx=10,
            pady=10,
            **kwargs
        )
        section.grid(row=row, column=column, pady=pady, sticky=sticky)
        return section
