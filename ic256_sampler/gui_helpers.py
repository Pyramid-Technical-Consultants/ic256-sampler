"""GUI helper utilities for thread-safe updates and common operations."""

from typing import Optional, Callable
from .gui import GUI


def safe_gui_update(
    window: Optional[GUI],
    callback: Callable[[], None],
) -> None:
    """Safely update GUI from any thread.
    
    Args:
        window: GUI window instance (may be None)
        callback: Function to call on GUI thread
    """
    if window:
        window.root.after(0, callback)


def log_message_safe(
    window: Optional[GUI],
    message: str,
    level: str = "INFO",
) -> None:
    """Safely log a message from any thread.
    
    Args:
        window: GUI window instance (may be None)
        message: Message to log
        level: Log level (INFO, WARNING, ERROR)
    """
    safe_gui_update(window, lambda: window.log_message(message, level))


def show_message_safe(
    window: Optional[GUI],
    message: str,
    color: str = "black",
) -> None:
    """Safely show a message from any thread.
    
    Args:
        window: GUI window instance (may be None)
        message: Message to show
        color: Message color
    """
    safe_gui_update(window, lambda: window.show_message(message, color))


def set_button_state_safe(
    window: Optional[GUI],
    button_name: str,
    state: str,
    image: Optional[Any] = None,
) -> None:
    """Safely set button state from any thread.
    
    Args:
        window: GUI window instance (may be None)
        button_name: Name of button attribute (e.g., "start_button")
        state: Button state ("normal", "disabled")
        image: Optional image to set
    """
    if not window:
        return
    
    def update_button():
        button = getattr(window, button_name, None)
        if button:
            if image is not None:
                button.config(state=state, image=image)
            else:
                button.config(state=state)
    
    safe_gui_update(window, update_button)
