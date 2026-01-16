"""Tests for MessagePack serialization in WebSocket communication."""

import pytest
import msgpack
import websocket
from unittest.mock import Mock, patch, MagicMock
from ic256_sampler.igx_client import IGXWebsocketClient


class TestMessagePackSerialization:
    """Test MessagePack serialization and deserialization."""
    
    def test_send_event_data_uses_msgpack(self):
        """Test that sendEventData uses MessagePack for serialization."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        client.ws.connected = True
        client.ws.send = Mock()
        
        # Send an event
        client.sendEventData("test_event", {"key": "value", "number": 42})
        
        # Verify send was called
        assert client.ws.send.called, "send should be called"
        
        # Get the arguments
        call_args = client.ws.send.call_args
        packed_data = call_args[0][0]
        
        # Verify it's binary (MessagePack)
        assert isinstance(packed_data, bytes), "Message should be bytes (MessagePack)"
        
        # Verify we can unpack it
        unpacked = msgpack.unpackb(packed_data, raw=False)
        assert unpacked["event"] == "test_event"
        assert unpacked["data"]["key"] == "value"
        assert unpacked["data"]["number"] == 42
        
        # Note: websocket-client automatically sends bytes as binary frames,
        # so we don't need to check for opcode parameter
    
    def test_wait_recv_deserializes_msgpack(self):
        """Test that waitRecv deserializes MessagePack messages."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        
        # Create a MessagePack message
        test_message = {"event": "response", "data": {"field": "value", "count": 123}}
        packed = msgpack.packb(test_message, use_bin_type=True)
        
        # Mock recv to return the packed message
        client.ws.recv = Mock(return_value=packed)
        
        # Receive and deserialize
        result = client.waitRecv()
        
        # Verify deserialization
        assert result == test_message
        assert result["event"] == "response"
        assert result["data"]["field"] == "value"
        assert result["data"]["count"] == 123
    
    def test_wait_recv_handles_empty_message(self):
        """Test that waitRecv handles empty messages gracefully."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        client.ws.recv = Mock(return_value=b'')
        
        result = client.waitRecv()
        
        # Should return empty dict on error
        assert result == {}
    
    def test_wait_recv_handles_invalid_msgpack(self):
        """Test that waitRecv handles invalid MessagePack gracefully."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        client.ws.recv = Mock(return_value=b'invalid msgpack data')
        
        result = client.waitRecv()
        
        # Should return empty dict on error
        assert result == {}
    
    def test_send_subscribe_fields_uses_msgpack(self):
        """Test that sendSubscribeFields uses MessagePack."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        client.ws.connected = True
        client.ws.send = Mock()
        
        # Create mock fields
        field1 = Mock()
        field1.getPath = Mock(return_value="/path/to/field1")
        field2 = Mock()
        field2.getPath = Mock(return_value="/path/to/field2")
        
        fields = {field1: True, field2: False}
        
        client.sendSubscribeFields(fields)
        
        # Verify send was called
        assert client.ws.send.called
        
        # Verify the message structure
        call_args = client.ws.send.call_args
        packed_data = call_args[0][0]
        unpacked = msgpack.unpackb(packed_data, raw=False)
        
        assert unpacked["event"] == "subscribe"
        assert "/path/to/field1" in unpacked["data"]
        assert "/path/to/field2" in unpacked["data"]
        assert unpacked["data"]["/path/to/field1"] is True
        assert unpacked["data"]["/path/to/field2"] is False
    
    def test_websocket_connection_tries_mpack_subprotocol(self):
        """Test that WebSocket connection tries MessagePack subprotocol ("mpack") first."""
        with patch('websocket.create_connection') as mock_create:
            mock_ws = Mock()
            mock_create.return_value = mock_ws
            
            client = IGXWebsocketClient("127.0.0.1")
            
            # Verify create_connection was called
            assert mock_create.called
            
            # Check if first call had "mpack" subprotocol (it should try it first)
            calls = mock_create.call_args_list
            if calls and len(calls) > 0:
                first_call = calls[0]
                if "subprotocols" in first_call.kwargs:
                    assert "mpack" in first_call.kwargs["subprotocols"]
    
    def test_reconnect_tries_mpack_subprotocol(self):
        """Test that reconnect tries MessagePack subprotocol ("mpack") first."""
        client = IGXWebsocketClient("")
        client.subscribedFields = {}
        
        with patch('websocket.create_connection') as mock_create:
            mock_ws = Mock()
            mock_create.return_value = mock_ws
            
            client.reconnect()
            
            # Verify create_connection was called
            assert mock_create.called
            
            # Check if first call had "mpack" subprotocol (it should try it first)
            calls = mock_create.call_args_list
            if calls and len(calls) > 0:
                first_call = calls[0]
                if "subprotocols" in first_call.kwargs:
                    assert "mpack" in first_call.kwargs["subprotocols"]
    
    def test_websocket_falls_back_to_regular_connection(self):
        """Test that WebSocket falls back to regular connection if MessagePack subprotocol fails."""
        with patch('websocket.create_connection') as mock_create:
            # First call fails (subprotocol not supported), second succeeds (regular connection)
            mock_ws = Mock()
            mock_create.side_effect = [
                websocket.WebSocketException("Invalid WebSocket Header"),  # First attempt fails
                mock_ws  # Second attempt succeeds
            ]
            
            client = IGXWebsocketClient("127.0.0.1")
            
            # Should have tried twice: once with subprotocol, once without
            assert mock_create.call_count == 2
            
            # First call should have "mpack" subprotocol
            first_call = mock_create.call_args_list[0]
            assert "subprotocols" in first_call.kwargs
            assert "mpack" in first_call.kwargs["subprotocols"]
            
            # Second call should not have subprotocols (fallback)
            second_call = mock_create.call_args_list[1]
            assert "subprotocols" not in second_call.kwargs or len(second_call.kwargs.get("subprotocols", [])) == 0
    
    def test_msgpack_preserves_data_types(self):
        """Test that MessagePack preserves Python data types correctly."""
        client = IGXWebsocketClient("")
        client.ws = Mock()
        client.ws.connected = True
        client.ws.send = Mock()
        
        # Test various data types
        test_data = {
            "string": "test",
            "integer": 42,
            "float": 3.14,
            "boolean": True,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
            "none": None
        }
        
        client.sendEventData("test", test_data)
        
        # Get and unpack the message
        call_args = client.ws.send.call_args
        packed_data = call_args[0][0]
        unpacked = msgpack.unpackb(packed_data, raw=False)
        
        # Verify all types are preserved
        assert unpacked["data"]["string"] == "test"
        assert unpacked["data"]["integer"] == 42
        assert unpacked["data"]["float"] == 3.14
        assert unpacked["data"]["boolean"] is True
        assert unpacked["data"]["list"] == [1, 2, 3]
        assert unpacked["data"]["dict"] == {"nested": "value"}
        assert unpacked["data"]["none"] is None
