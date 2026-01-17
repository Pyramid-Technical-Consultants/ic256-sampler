"""Tooltip component for showing helpful hints."""

import tkinter as tk
from typing import Optional

from ..styles.fonts import FONTS


class ToolTip:
    """Tooltip widget for showing helpful hints with delay to prevent flickering."""
    
    def __init__(self, widget: tk.Widget, text: str, x: int = 0, y: int = 20, delay: int = 500):
        """Initialize tooltip.
        
        Args:
            widget: Widget to attach tooltip to
            text: Tooltip text to display
            x: X offset from widget
            y: Y offset from widget
            delay: Delay in milliseconds before showing tooltip
        """
        self.widget = widget
        self.text = text
        self.tooltip: Optional[tk.Toplevel] = None
        self.x = x
        self.y = y
        self.delay = delay
        self.after_id: Optional[str] = None

        # Use add=True to not override existing bindings
        self.widget.bind("<Enter>", self.on_enter, add="+")
        self.widget.bind("<Leave>", self.on_leave, add="+")
        self.widget.bind("<ButtonPress>", self.on_leave, add="+")  # Hide on click

    def show_tooltip(self, event=None):
        """Display tooltip on hover."""
        if not self.tooltip:
            try:
                # Get widget position
                x = self.widget.winfo_rootx() + self.x
                y = self.widget.winfo_rooty() + self.y + self.widget.winfo_height()
                
                # If bbox is available (for text widgets), use it
                try:
                    bbox = self.widget.bbox("insert")
                    if bbox:
                        x = self.widget.winfo_rootx() + bbox[0] + self.x
                        y = self.widget.winfo_rooty() + bbox[1] + bbox[3] + self.y
                except (tk.TclError, AttributeError):
                    pass

                self.tooltip = tk.Toplevel(self.widget)
                self.tooltip.wm_overrideredirect(True)
                self.tooltip.wm_geometry(f"+{x}+{y}")

                label = tk.Label(
                    self.tooltip,
                    text=self.text,
                    background="#FFFFE0",
                    relief="solid",
                    borderwidth=1,
                    font=FONTS["tooltip"],
                    padx=5,
                    pady=2,
                )
                label.pack()
            except (tk.TclError, AttributeError):
                # Widget may have been destroyed
                self.tooltip = None

    def hide_tooltip(self, event=None):
        """Hide tooltip."""
        # Cancel any pending tooltip display
        if self.after_id:
            self.widget.after_cancel(self.after_id)
            self.after_id = None
        
        if self.tooltip:
            try:
                self.tooltip.destroy()
            except (tk.TclError, AttributeError):
                pass
            self.tooltip = None

    def on_enter(self, event):
        """Handle mouse enter event with delay."""
        # Cancel any existing pending tooltip
        if self.after_id:
            self.widget.after_cancel(self.after_id)
        
        # Schedule tooltip to show after delay
        self.after_id = self.widget.after(self.delay, self.show_tooltip)

    def on_leave(self, event):
        """Handle mouse leave event."""
        self.hide_tooltip()
