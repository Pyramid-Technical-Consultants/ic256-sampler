"""Form field components for label + entry + optional button patterns."""

import tkinter as tk
from typing import Optional, Callable

from ..styles import COLORS, FONTS
from ..styles.sizes import ENTRY_PADY
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
        entry_tooltip: Optional[str] = None,  # Alias for tooltip for consistency
        change_callback: Optional[Callable] = None,
        **entry_kwargs
    ):
        """Create a form field with label and entry.
        
        Args:
            parent: Parent widget
            label_text: Label text
            row: Grid row position
            column: Grid column position
            entry_width: Entry field width in characters
            label_width: Label width in characters
            entry_state: Entry state ("normal", "readonly", "disabled")
            tooltip: Optional tooltip text for the entry
            entry_tooltip: Optional tooltip text (alias for tooltip)
            change_callback: Optional callback for entry changes
            **entry_kwargs: Additional entry options (will be filtered to valid tk.Entry options)
        """
        # Use entry_tooltip if provided, otherwise use tooltip
        final_tooltip = entry_tooltip if entry_tooltip is not None else tooltip
        
        # Filter out non-entry options from kwargs
        # Common invalid options that might be passed
        invalid_options = {'entry_tooltip', 'entry_state', 'tooltip'}
        filtered_kwargs = {k: v for k, v in entry_kwargs.items() if k not in invalid_options}
        
        # Label
        self.label = tk.Label(
            parent,
            font=FONTS["label"],
            text=label_text,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=label_width,
            anchor="w"
        )
        self.label.grid(row=row, column=column, padx=(5, 10), pady=ENTRY_PADY, sticky="w")
        
        # Entry (wrapped in frame for consistent height)
        self.entry = StandardEntry.create(parent, width=entry_width, **filtered_kwargs)
        self.entry.config(state=entry_state)
        # Place the frame wrapper instead of entry directly
        entry_frame = self.entry._entry_frame
        entry_frame.grid(row=row, column=column + 1, padx=(0, 5), pady=ENTRY_PADY, sticky="ew")
        
        # Configure parent column for entry expansion
        parent.grid_columnconfigure(column + 1, weight=1)
        
        # Tooltip
        if final_tooltip:
            ToolTip(self.entry, final_tooltip, 0, 20)
        
        # Change callback
        if change_callback:
            self.entry.bind('<KeyRelease>', lambda e: change_callback())
            self.entry.bind('<FocusOut>', lambda e: change_callback())
    
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


class FormFieldWithButton:
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
        **entry_kwargs
    ):
        """Create a form field with label, entry, and button.
        
        Args:
            parent: Parent widget
            label_text: Label text
            row: Grid row position
            column: Grid column position
            entry_width: Entry field width in characters
            label_width: Label width in characters
            button_image: Optional button image
            button_command: Optional button command
            button_tooltip: Optional tooltip for button
            entry_state: Entry state ("normal", "readonly", "disabled")
            entry_tooltip: Optional tooltip text for the entry
            change_callback: Optional callback for entry changes
            **entry_kwargs: Additional entry options
        """
        # Label
        self.label = tk.Label(
            parent,
            font=FONTS["label"],
            text=label_text,
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            width=label_width,
            anchor="w"
        )
        self.label.grid(row=row, column=column, padx=(5, 10), pady=ENTRY_PADY, sticky="w")
        
        # Entry (wrapped in frame for consistent height)
        self.entry = StandardEntry.create(parent, width=entry_width, **entry_kwargs)
        self.entry.config(state=entry_state)
        # Place the frame wrapper instead of entry directly
        entry_frame = self.entry._entry_frame
        entry_frame.grid(row=row, column=column + 1, padx=(0, 5), pady=ENTRY_PADY, sticky="ew")
        
        # Configure parent column for entry expansion
        parent.grid_columnconfigure(column + 1, weight=1)
        
        # Entry tooltip
        if entry_tooltip:
            ToolTip(self.entry, entry_tooltip, 0, 20)
        
        # Change callback
        if change_callback:
            self.entry.bind('<KeyRelease>', lambda e: change_callback())
            self.entry.bind('<FocusOut>', lambda e: change_callback())
        
        # Button (if provided)
        self.button = None
        if button_image and button_command:
            self.button = IconButton.create(
                parent,
                button_image,
                command=button_command,
                tooltip=button_tooltip,
                size=(24, 24)
            )
            self.button.grid(row=row, column=column + 2, padx=(0, 5), pady=ENTRY_PADY)
    
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
