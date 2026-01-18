"""Image loading and management utilities."""

import os
import sys
from typing import Dict
from PIL import Image, ImageTk
import tkinter as tk


class ImageLoader:
    """Utility class for loading and managing GUI images."""

    def __init__(self):
        """Initialize image loader and determine image directory."""
        if hasattr(sys, "_MEIPASS"):
            base_dir = sys._MEIPASS
            self.images_dir = os.path.join(base_dir, "images")
        else:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            self.images_dir = os.path.join(project_root, "ic256_sampler", "assets", "images")

        self._cache: Dict[str, ImageTk.PhotoImage] = {}

    def get_image_path(self, filename: str) -> str:
        """Get full path to an image file."""
        return os.path.join(self.images_dir, filename)

    def load_image(self, filename: str, size: tuple = None) -> ImageTk.PhotoImage:
        """Load and optionally resize an image."""
        cache_key = f"{filename}_{size}" if size else filename

        if cache_key in self._cache:
            return self._cache[cache_key]

        image_path = self.get_image_path(filename)

        with Image.open(image_path) as img:
            if size:
                img = img.resize(size, Image.LANCZOS)
            photo = ImageTk.PhotoImage(img)
            self._cache[cache_key] = photo
            return photo

    def set_window_icon(self, root: tk.Tk, filename: str = "logo.png", size: tuple = (26, 26)) -> None:
        """Set window icon."""
        icon = self.load_image(filename, size)
        root.iconphoto(True, icon)
