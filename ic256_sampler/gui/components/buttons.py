"""Standard button components with native styling."""

import tkinter as tk
from typing import Callable, Optional

from ..styles.colors import COLORS
from ..styles.fonts import FONTS


class StandardButton:
    """Factory for creating standard buttons with native appearance."""
    
    @staticmethod
    def create(
        parent: tk.Widget,
        text: str,
        command: Optional[Callable] = None,
        fg_color: str = None,
        text_color: str = None,
        **kwargs
    ) -> tk.Button:
        """Create a button with native appearance.
        
        Args:
            parent: Parent widget
            text: Button text
            command: Command to execute on click
            fg_color: Background color (defaults to system button color)
            text_color: Text color (defaults to system text color)
            **kwargs: Additional button options
            
        Returns:
            Configured button widget with native styling
        """
        if fg_color is None:
            fg_color = COLORS["primary"]
        if text_color is None:
            text_color = COLORS["text_primary"]
        
        # Use native button styling - system relief and border
        button = tk.Button(
            parent,
            text=text,
            command=command,
            font=FONTS["button"],
            bg=fg_color,
            fg=text_color,
            activebackground=COLORS["hover"] if fg_color == COLORS["primary"] else fg_color,
            activeforeground=text_color,
            relief="raised",  # Native raised button appearance
            borderwidth=1,  # Standard native border
            padx=20,
            pady=8,
            cursor="hand2",
            highlightthickness=1,  # Native focus highlight
            **kwargs
        )
        
        # Store original colors for state management
        button._original_bg = fg_color
        button._original_fg = text_color
        
        # Create wrapper method to handle state changes with proper styling
        original_config = button.config
        
        def enhanced_config(**kw):
            result = original_config(**kw)
            if "state" in kw:
                state = kw["state"]
                if state == "disabled":
                    # Use native disabled appearance
                    try:
                        button.config(
                            relief="sunken",  # Native disabled appearance
                            cursor="arrow"
                        )
                    except tk.TclError:
                        pass
                elif state == "normal":
                    # Restore normal native appearance
                    button.config(
                        bg=button._original_bg,
                        fg=button._original_fg,
                        relief="raised",
                        cursor="hand2"
                    )
            return result
        
        # Replace config method with enhanced version
        button.config = enhanced_config
        
        return button
