"""Helper functions for tab initialization and setup."""

from tkinter import ttk


def setup_tab_frame(parent: ttk.Frame) -> None:
    """Configure a tab frame for proper resizing."""
    parent.grid_rowconfigure(0, weight=1)
    parent.grid_columnconfigure(0, weight=1)
