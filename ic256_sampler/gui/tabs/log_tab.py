"""Log tab implementation for application logging."""

import tkinter as tk
from datetime import datetime
from tkinter import filedialog
from typing import Optional

from ..styles import COLORS, FONTS
from ..components import StandardButton, StandardEntry, ToolTip


class LogTab:
    """Log tab for application logging."""
    
    def __init__(self, parent: tk.Widget, show_message_callback: Optional[callable] = None):
        """Initialize log tab.
        
        Args:
            parent: Parent widget (ttk.Frame from notebook)
            show_message_callback: Optional callback to show messages in status bar
        """
        self.parent = parent
        self.show_message_callback = show_message_callback
        self.log_content = []
        
        # Configure log tab for resizing
        self.parent.grid_rowconfigure(1, weight=1)
        self.parent.grid_columnconfigure(0, weight=1)
        
        # Create search frame at the top
        self._create_search_section()
        
        # Create log display
        self._create_log_display()
        
        # Create action buttons
        self._create_action_buttons()
        
        # Add initial log message
        self.log_message("Application started", "INFO")
    
    def _create_search_section(self):
        """Create search section."""
        search_frame = tk.Frame(self.parent, bg=COLORS["background"])
        search_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        search_frame.grid_columnconfigure(1, weight=1)
        
        search_label = tk.Label(
            search_frame,
            text="Search:",
            font=FONTS["label_small"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"]
        )
        search_label.grid(row=0, column=0, padx=(0, 5), sticky="w")
        
        self.log_search_entry = StandardEntry.create(search_frame)
        self.log_search_entry.grid(row=0, column=1, sticky="ew", padx=5)
        self.log_search_entry.bind('<KeyRelease>', self._filter_log)
        ToolTip(self.log_search_entry, "Search log entries (Ctrl+F to focus)", 0, 20)
    
    def _create_log_display(self):
        """Create log text display."""
        log_frame = tk.Frame(self.parent, bg=COLORS["background"])
        log_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
        log_frame.grid_rowconfigure(0, weight=1)
        log_frame.grid_columnconfigure(0, weight=1)
        
        # Create scrollbar
        scrollbar = tk.Scrollbar(log_frame, bg=COLORS["background"])
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Create text widget for log
        self.log_text = tk.Text(
            log_frame,
            wrap=tk.WORD,
            yscrollcommand=scrollbar.set,
            font=FONTS["log"],
            bg=COLORS["background"],
            fg=COLORS["text_primary"],
            relief="flat",
            borderwidth=1,
            highlightthickness=1,
            highlightbackground=COLORS["border"],
            padx=10,
            pady=10
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scrollbar.config(command=self.log_text.yview)
        
        # Configure text tags for different log levels
        self.log_text.tag_config("error", foreground=COLORS["error"], font=FONTS["log_bold"])
        self.log_text.tag_config("warning", foreground=COLORS["warning"], font=FONTS["log"])
        self.log_text.tag_config("success", foreground=COLORS["success"], font=FONTS["log"])
        self.log_text.tag_config("info", foreground=COLORS["primary"], font=FONTS["log"])
        self.log_text.tag_config("highlight", background="#FFFF00", foreground=COLORS["text_primary"])
    
    def _create_action_buttons(self):
        """Create action buttons."""
        button_frame = tk.Frame(self.parent, bg=COLORS["background"])
        button_frame.grid(row=2, column=0, pady=10)
        
        clear_button = StandardButton.create(
            button_frame,
            "Clear Log",
            self.clear_log,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        clear_button.pack(side=tk.LEFT, padx=5)
        
        export_button = StandardButton.create(
            button_frame,
            "Export Log",
            self._export_log,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        export_button.pack(side=tk.LEFT, padx=5)
        
        copy_button = StandardButton.create(
            button_frame,
            "Copy Selected",
            self._copy_log_selection,
            fg_color=COLORS["primary"],
            text_color=COLORS["text_primary"]
        )
        copy_button.pack(side=tk.LEFT, padx=5)
    
    def log_message(self, message: str, level: str = "INFO"):
        """Add a message to the log tab with timestamp.
        
        Args:
            message: The log message
            level: Log level - "INFO", "WARNING", "ERROR", or color string
        """
        if not hasattr(self, 'log_text') or self.log_text is None:
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Map level/color to log level and tag
        level_upper = str(level).upper()
        message_lower = message.lower()
        
        if level_upper == "ERROR" or level == "red" or "error" in message_lower or "failed" in message_lower:
            log_level = "ERROR"
            tag = "error"
        elif level_upper == "WARNING" or level == "orange" or "warning" in message_lower:
            log_level = "WARNING"
            tag = "warning"
        elif level_upper == "INFO" or level == "green" or "success" in message_lower or "completed" in message_lower:
            log_level = "INFO"
            tag = "success"
        else:
            log_level = "INFO"
            tag = "info"
        
        log_entry = f"[{timestamp}] [{log_level}] {message}\n"
        
        try:
            # Store in log_content for filtering
            self.log_content.append({'text': log_entry, 'tag': tag})
            
            # Limit log content size to prevent memory issues
            if len(self.log_content) > 1000:
                self.log_content = self.log_content[-1000:]
            
            # Insert into text widget (respecting current filter)
            if hasattr(self, 'log_search_entry') and self.log_search_entry.get():
                # Apply filter
                self._filter_log()
            else:
                self.log_text.insert(tk.END, log_entry, tag)
                self.log_text.see(tk.END)
            
            # Limit displayed log size to prevent memory issues
            lines = int(self.log_text.index('end-1c').split('.')[0])
            if lines > 1000:
                self.log_text.delete('1.0', f'{lines-1000}.0')
        except (tk.TclError, ValueError, AttributeError):
            # Text widget may be destroyed or invalid index - ignore
            pass
    
    def clear_log(self):
        """Clear the log text widget."""
        self.log_text.delete('1.0', tk.END)
        self.log_content = []
        self.log_message("Log cleared", "INFO")
    
    def _filter_log(self, event=None):
        """Filter log entries based on search text."""
        if not hasattr(self, 'log_search_entry'):
            return
        
        search_text = self.log_search_entry.get().lower()
        
        # Remove existing highlight tags
        self.log_text.tag_remove("highlight", "1.0", tk.END)
        
        if not search_text:
            # Show all content
            self.log_text.delete('1.0', tk.END)
            for entry in self.log_content:
                self.log_text.insert(tk.END, entry['text'], entry['tag'])
            return
        
        # Filter and highlight
        self.log_text.delete('1.0', tk.END)
        for entry in self.log_content:
            if search_text in entry['text'].lower():
                self.log_text.insert(tk.END, entry['text'], entry['tag'])
                # Highlight search matches
                start = "1.0"
                while True:
                    pos = self.log_text.search(search_text, start, tk.END, nocase=True)
                    if not pos:
                        break
                    end = f"{pos}+{len(search_text)}c"
                    self.log_text.tag_add("highlight", pos, end)
                    start = end
    
    def _focus_log_search(self, event=None):
        """Focus the log search entry."""
        if hasattr(self, 'log_search_entry'):
            self.log_search_entry.focus_set()
            self.log_search_entry.select_range(0, tk.END)
        return "break"
    
    def _export_log(self, event=None):
        """Export log to a text file."""
        if not hasattr(self, 'log_text'):
            return
        
        filename = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            title="Export Log"
        )
        
        if filename:
            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    content = self.log_text.get('1.0', tk.END)
                    f.write(content)
                if self.show_message_callback:
                    self.show_message_callback(f"Log exported to {filename}", "green")
                self.log_message(f"Log exported to {filename}", "INFO")
            except Exception as e:
                error_msg = f"Failed to export log: {str(e)}"
                if self.show_message_callback:
                    self.show_message_callback(error_msg, "red")
                self.log_message(error_msg, "ERROR")
        return "break"
    
    def _copy_log_selection(self, event=None):
        """Copy selected text from log to clipboard."""
        if not hasattr(self, 'log_text'):
            return
        
        try:
            root = self.log_text.winfo_toplevel()
            if self.log_text.tag_ranges(tk.SEL):
                # Copy selected text
                selected = self.log_text.get(tk.SEL_FIRST, tk.SEL_LAST)
                root.clipboard_clear()
                root.clipboard_append(selected)
                if self.show_message_callback:
                    self.show_message_callback("Text copied to clipboard", "green")
            else:
                # Copy all text if nothing selected
                all_text = self.log_text.get('1.0', tk.END)
                root.clipboard_clear()
                root.clipboard_append(all_text)
                if self.show_message_callback:
                    self.show_message_callback("All log text copied to clipboard", "green")
        except Exception as e:
            error_msg = f"Failed to copy: {str(e)}"
            if self.show_message_callback:
                self.show_message_callback(error_msg, "red")
        return "break"
