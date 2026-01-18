"""Helper functions for tab initialization and setup."""

import tkinter as tk
from tkinter import ttk

from ..components import ScrollableFrame


def setup_tab_frame(parent: ttk.Frame) -> None:
    """Configure a tab frame for proper resizing.
    
    Args:
        parent: Tab frame to configure
    """
    parent.grid_rowconfigure(0, weight=1)
    parent.grid_columnconfigure(0, weight=1)


def create_scrollable_tab_content(parent: tk.Widget, padding: tuple = (20, 20)) -> tk.Frame:
    """Create a scrollable content area for a tab.
    
    Args:
        parent: Parent widget (tab frame)
        padding: Padding tuple (x, y) for the content container
        
    Returns:
        The scrollable frame to add content to
    """
    setup_tab_frame(parent)
    scrollable = ScrollableFrame(parent)
    main_container = scrollable.get_frame()
    main_container.pack(fill="both", expand=True, padx=padding[0], pady=padding[1])
    return main_container
