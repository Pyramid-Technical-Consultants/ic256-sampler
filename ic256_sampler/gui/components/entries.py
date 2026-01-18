"""Standard entry components with native styling."""

import tkinter as tk
from typing import Optional

from ..styles.colors import COLORS
from ..styles.fonts import FONTS
from ..styles.sizes import ENTRY_PADY, STANDARD_WIDGET_HEIGHT


class StandardEntry:
    """Factory for creating standard entry fields with native appearance."""
    
    @staticmethod
    def create(
        parent: tk.Widget,
        width: int = 30,
        font: tuple = None,
        **kwargs
    ) -> tk.Entry:
        """Create a standard entry field with native styling.
        
        Args:
            parent: Parent widget
            width: Entry width in characters
            font: Font tuple (defaults to standard entry font)
            **kwargs: Additional entry options
            
        Returns:
            Configured entry widget with native styling wrapped in a frame for consistent height
        """
        if font is None:
            font = FONTS["entry"]
        
        # Create a frame wrapper to control entry height
        entry_frame = tk.Frame(parent, bg=COLORS["background"], height=STANDARD_WIDGET_HEIGHT)
        entry_frame.pack_propagate(False)  # Prevent frame from shrinking to fit content
        
        entry = tk.Entry(
            entry_frame,
            width=width,
            font=font,
            relief="sunken",  # Native entry field appearance
            borderwidth=1,  # Standard native border width
            highlightthickness=1,  # Native focus highlight
            insertbackground=COLORS["text_primary"],  # Cursor color
            **kwargs
        )
        
        # Pack entry in frame with padding to center vertically
        entry.pack(fill="both", expand=True, padx=2, pady=2)
        
        # Store padding info and frame reference on entry
        entry._entry_pady = ENTRY_PADY
        entry._entry_frame = entry_frame
        
        # Return the entry (frame is accessible via _entry_frame for grid placement)
        return entry


class EntryWithPlaceholder:
    """Entry field with placeholder text support."""
    
    def __init__(
        self,
        parent: tk.Widget,
        placeholder: str,
        width: int = 30,
        font: tuple = None,
        **kwargs
    ):
        """Initialize entry with placeholder.
        
        Args:
            parent: Parent widget
            placeholder: Placeholder text to show when empty
            width: Entry width in characters
            font: Font tuple (defaults to standard entry font)
            **kwargs: Additional entry options
        """
        self.placeholder = placeholder
        self.entry = StandardEntry.create(parent, width=width, font=font, **kwargs)
        # Store frame reference for grid placement
        self.entry_frame = self.entry._entry_frame
        
        # Set up placeholder
        self.entry.insert(0, placeholder)
        self.entry.config(fg=COLORS["text_secondary"])
        self.entry.bind('<FocusIn>', self._on_focus_in)
        self.entry.bind('<FocusOut>', self._on_focus_out)
    
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
        """Get entry value, returning empty string if placeholder is present.
        
        Returns:
            Entry value or empty string if placeholder
        """
        value = self.entry.get().strip()
        if value == self.placeholder:
            return ""
        return value
    
    def get_widget(self) -> tk.Entry:
        """Get the underlying entry widget.
        
        Returns:
            The entry widget
        """
        return self.entry
