"""Thread-safe GUI update utilities."""

from typing import Optional, Callable, Any


def safe_gui_update(window: Optional["GUI"], callback: Callable[[], None]) -> None:  # type: ignore
    """Safely update GUI from any thread."""
    if window:
        window.root.after(0, callback)


def log_message_safe(window: Optional["GUI"], message: str, level: str = "INFO") -> None:  # type: ignore
    """Safely log a message from any thread."""
    safe_gui_update(window, lambda: window.log_message(message, level))


def show_message_safe(window: Optional["GUI"], message: str, color: str = "black") -> None:  # type: ignore
    """Safely show a message from any thread."""
    safe_gui_update(window, lambda: window.show_message(message, color))


def set_button_state_safe(
    window: Optional["GUI"],  # type: ignore
    button_name: str,
    state: str,
    image: Optional[Any] = None,
) -> None:
    """Safely set button state from any thread."""
    if not window:
        return

    def update_button():
        button = getattr(window, button_name, None)
        if button:
            button.config(state=state, image=image if image is not None else button.cget("image"))

    safe_gui_update(window, update_button)
