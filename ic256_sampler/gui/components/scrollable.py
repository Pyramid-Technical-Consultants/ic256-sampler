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
        self.canvas_frame = tk.Frame(parent, bg=COLORS["background"])
        self.canvas_frame.grid(row=0, column=0, sticky="nsew")
        self.canvas_frame.grid_rowconfigure(0, weight=1)
        self.canvas_frame.grid_columnconfigure(0, weight=1)
        
        # Create canvas
        self.canvas = tk.Canvas(
            self.canvas_frame,
            bg=COLORS["background"],
            highlightthickness=0
        )
        self.canvas.grid(row=0, column=0, sticky="nsew")
        
        # Create scrollbar
        self.scrollbar = tk.Scrollbar(
            self.canvas_frame,
            orient="vertical",
            command=self.canvas.yview
        )
        self.scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Configure canvas scrolling
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        # Create scrollable frame inside canvas
        self.scrollable_frame = tk.Frame(self.canvas, bg=COLORS["background"])
        # Configure scrollable frame to expand horizontally
        self.scrollable_frame.grid_columnconfigure(0, weight=1)
        self.canvas_window = self.canvas.create_window(
            (0, 0),
            window=self.scrollable_frame,
            anchor="nw"
        )
        
        # Update scroll region when frame size changes
        def configure_scroll_region(event):
            # Update scroll region to match content
            bbox = self.canvas.bbox("all")
            if bbox:
                self.canvas.configure(scrollregion=bbox)
            # Update scrollable frame width to match canvas
            canvas_width = self.canvas.winfo_width()
            if canvas_width > 1:  # Only update if canvas has been rendered
                self.canvas.itemconfig(self.canvas_window, width=canvas_width)
        
        self.scrollable_frame.bind("<Configure>", configure_scroll_region)
        
        # Bind canvas resize to adjust scrollable frame width and update scroll region
        def configure_canvas_width(event):
            canvas_width = event.width
            if canvas_width > 1:  # Only update if canvas has been rendered
                self.canvas.itemconfig(self.canvas_window, width=canvas_width)
                # Update scroll region after width change
                self.canvas.update_idletasks()
                bbox = self.canvas.bbox("all")
                if bbox:
                    self.canvas.configure(scrollregion=bbox)
        
        self.canvas.bind("<Configure>", configure_canvas_width)
        
        # Also bind to parent resize to ensure responsiveness
        def on_parent_configure(event):
            # Update scroll region when parent resizes
            self.canvas.update_idletasks()
            bbox = self.canvas.bbox("all")
            if bbox:
                self.canvas.configure(scrollregion=bbox)
        
        parent.bind("<Configure>", on_parent_configure)
        
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
