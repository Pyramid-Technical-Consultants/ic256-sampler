"""Unit tests for gui_helpers module."""

import pytest
import threading
from unittest.mock import Mock, patch
from ic256_sampler.gui_helpers import (
    safe_gui_update,
    log_message_safe,
    show_message_safe,
    set_button_state_safe,
)


class TestSafeGuiUpdate:
    """Tests for safe_gui_update function."""
    
    def test_safe_gui_update_with_window(self):
        """Test safe_gui_update calls callback when window exists."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        
        callback = Mock()
        safe_gui_update(mock_window, callback)
        
        mock_window.root.after.assert_called_once_with(0, callback)
    
    def test_safe_gui_update_no_window(self):
        """Test safe_gui_update does nothing when window is None."""
        callback = Mock()
        safe_gui_update(None, callback)
        
        # Should not raise
        callback.assert_not_called()
    
    def test_safe_gui_update_calls_callback(self):
        """Test safe_gui_update actually calls the callback."""
        mock_window = Mock()
        mock_window.root = Mock()
        callback_called = [False]
        
        def callback():
            callback_called[0] = True
        
        safe_gui_update(mock_window, callback)
        
        # Extract the callback passed to after()
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()  # Execute the scheduled callback
        
        assert callback_called[0] is True


class TestLogMessageSafe:
    """Tests for log_message_safe function."""
    
    def test_log_message_safe_with_window(self):
        """Test log_message_safe calls window method when window exists."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.log_message = Mock()
        
        log_message_safe(mock_window, "Test message", "INFO")
        
        # Should schedule the log_message call
        mock_window.root.after.assert_called_once()
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()
        
        mock_window.log_message.assert_called_once_with("Test message", "INFO")
    
    def test_log_message_safe_no_window(self):
        """Test log_message_safe does nothing when window is None."""
        # Should not raise
        log_message_safe(None, "Test message", "INFO")
    
    def test_log_message_safe_default_level(self):
        """Test log_message_safe uses default level when not specified."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.log_message = Mock()
        
        log_message_safe(mock_window, "Test message")
        
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()
        
        mock_window.log_message.assert_called_once_with("Test message", "INFO")


class TestShowMessageSafe:
    """Tests for show_message_safe function."""
    
    def test_show_message_safe_with_window(self):
        """Test show_message_safe calls window method when window exists."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.show_message = Mock()
        
        show_message_safe(mock_window, "Test message", "green")
        
        # Should schedule the show_message call
        mock_window.root.after.assert_called_once()
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()
        
        mock_window.show_message.assert_called_once_with("Test message", "green")
    
    def test_show_message_safe_no_window(self):
        """Test show_message_safe does nothing when window is None."""
        # Should not raise
        show_message_safe(None, "Test message", "green")
    
    def test_show_message_safe_various_colors(self):
        """Test show_message_safe with various color values."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_window.show_message = Mock()
        
        colors = ["green", "red", "blue", "yellow"]
        for color in colors:
            show_message_safe(mock_window, f"Message {color}", color)
            
            # Extract and call the scheduled callback
            call_args = mock_window.root.after.call_args
            scheduled_callback = call_args[0][1]
            scheduled_callback()
            
            mock_window.show_message.assert_called_with(f"Message {color}", color)


class TestSetButtonStateSafe:
    """Tests for set_button_state_safe function."""
    
    def test_set_button_state_safe_with_window(self):
        """Test set_button_state_safe updates button when window exists."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_button = Mock()
        mock_button.config = Mock()
        mock_window.start_button = mock_button
        
        set_button_state_safe(mock_window, "start_button", "normal")
        
        # Should schedule the update
        mock_window.root.after.assert_called_once()
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()
        
        mock_button.config.assert_called_once_with(state="normal")
    
    def test_set_button_state_safe_with_image(self):
        """Test set_button_state_safe with image parameter."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_button = Mock()
        mock_button.config = Mock()
        mock_window.start_button = mock_button
        mock_image = Mock()
        
        set_button_state_safe(mock_window, "start_button", "normal", mock_image)
        
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()
        
        mock_button.config.assert_called_once_with(state="normal", image=mock_image)
    
    def test_set_button_state_safe_no_window(self):
        """Test set_button_state_safe does nothing when window is None."""
        # Should not raise
        set_button_state_safe(None, "start_button", "normal")
    
    def test_set_button_state_safe_various_states(self):
        """Test set_button_state_safe with various button states."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        mock_button = Mock()
        mock_button.config = Mock()
        mock_window.start_button = mock_button
        
        states = ["normal", "disabled", "active"]
        for state in states:
            set_button_state_safe(mock_window, "start_button", state)
            
            # Extract and call the scheduled callback
            call_args = mock_window.root.after.call_args
            scheduled_callback = call_args[0][1]
            scheduled_callback()
            
            mock_button.config.assert_called_with(state=state)
    
    def test_set_button_state_safe_various_buttons(self):
        """Test set_button_state_safe with various button names."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        
        buttons = ["start_button", "stop_button", "set_up_button"]
        for button in buttons:
            mock_button = Mock()
            mock_button.config = Mock()
            setattr(mock_window, button, mock_button)
            
            set_button_state_safe(mock_window, button, "normal")
            
            # Extract and call the scheduled callback
            call_args = mock_window.root.after.call_args
            scheduled_callback = call_args[0][1]
            scheduled_callback()
            
            mock_button.config.assert_called_with(state="normal")
    
    def test_set_button_state_safe_no_button_attribute(self):
        """Test set_button_state_safe handles missing button attribute."""
        mock_window = Mock()
        mock_window.root = Mock()
        mock_window.root.after = Mock()
        # Don't set start_button attribute
        
        # Should not raise
        set_button_state_safe(mock_window, "start_button", "normal")
        
        # Extract and call the scheduled callback
        call_args = mock_window.root.after.call_args
        scheduled_callback = call_args[0][1]
        scheduled_callback()  # Should not raise
