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
        # Determine the path to the executable's directory or package directory
        if hasattr(sys, "_MEIPASS"):
            # PyInstaller bundle
            base_dir = sys._MEIPASS
            self.images_dir = os.path.join(base_dir, "images")
        else:
            # Development mode - use package directory
            # __file__ is gui/utils/images.py, need to go up to project root
            # gui/utils/images.py -> gui/utils -> gui -> ic256_sampler -> project_root
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            self.images_dir = os.path.join(project_root, "ic256_sampler", "assets", "images")
        
        self._cache: Dict[str, ImageTk.PhotoImage] = {}
    
    def get_image_path(self, filename: str) -> str:
        """Get full path to an image file.
        
        Args:
            filename: Image filename
            
        Returns:
            Full path to image file
        """
        return os.path.join(self.images_dir, filename)
    
    def load_image(self, filename: str, size: tuple = None) -> ImageTk.PhotoImage:
        """Load and optionally resize an image.
        
        Args:
            filename: Image filename
            size: Optional tuple of (width, height) to resize
            
        Returns:
            PhotoImage object
        """
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
        """Set window icon.
        
        Args:
            root: Root window
            filename: Icon filename
            size: Icon size
        """
        icon = self.load_image(filename, size)
        root.iconphoto(True, icon)
