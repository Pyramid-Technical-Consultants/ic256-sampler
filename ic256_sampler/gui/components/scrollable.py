"""Scrollable frame component for content that may exceed window size."""

import tkinter as tk
import platform

from ..styles import COLORS


class ScrollableFrame:
    """A scrollable frame using Canvas with automatic scrollbar."""
    
    def __init__(self, parent: tk.Widget):
        """Create a scrollable frame.
        
        Args:
            parent: Parent widget
        """
        # Create canvas frame
        canvas_frame = tk.Frame(parent, bg=COLORS["background"])
        canvas_frame.grid(row=0, column=0, sticky="nsew")
        canvas_frame.grid_rowconfigure(0, weight=1)
        canvas_frame.grid_columnconfigure(0, weight=1)
        
        # Create canvas
        self.canvas = tk.Canvas(
            canvas_frame,
            bg=COLORS["background"],
            highlightthickness=0
        )
        self.canvas.grid(row=0, column=0, sticky="nsew")
        
        # Create scrollbar
        scrollbar = tk.Scrollbar(
            canvas_frame,
            orient="vertical",
            command=self.canvas.yview
        )
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Configure canvas scrolling
        self.canvas.configure(yscrollcommand=scrollbar.set)
        
        # Create scrollable frame inside canvas
        self.scrollable_frame = tk.Frame(self.canvas, bg=COLORS["background"])
        self.canvas_window = self.canvas.create_window(
            (0, 0),
            window=self.scrollable_frame,
            anchor="nw"
        )
        
        # Update scroll region when frame size changes
        def configure_scroll_region(event):
            self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        
        self.scrollable_frame.bind("<Configure>", configure_scroll_region)
        
        # Bind canvas resize to adjust scrollable frame width
        def configure_canvas_width(event):
            canvas_width = event.width
            self.canvas.itemconfig(self.canvas_window, width=canvas_width)
        
        self.canvas.bind("<Configure>", configure_canvas_width)
        
        # Enable mousewheel scrolling
        self._setup_mousewheel()
    
    def _setup_mousewheel(self):
        """Set up mousewheel scrolling for the canvas."""
        def on_mousewheel(event):
            # Windows and Linux
            if platform.system() in ["Windows", "Linux"]:
                self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
            # macOS
            else:
                self.canvas.yview_scroll(int(-1 * event.delta), "units")
        
        # Bind to canvas and all child widgets
        self.canvas.bind_all("<MouseWheel>", on_mousewheel)
        self.canvas.bind_all("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"))
        self.canvas.bind_all("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"))
    
    def get_frame(self) -> tk.Frame:
        """Get the scrollable frame to add widgets to.
        
        Returns:
            The scrollable frame widget
        """
        return self.scrollable_frame
