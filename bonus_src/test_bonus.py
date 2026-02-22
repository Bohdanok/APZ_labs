import pytest
from unittest.mock import patch, MagicMock
from tenacity import RetryError

import logging_pb2
from logging_service_bonus import LoggingServiceServicer, message_map
from facade_service_bonus import send_grpc_log

def test_grpc_logging_service_deduplication():
    message_map.clear()
    servicer = LoggingServiceServicer()
    mock_context = MagicMock()
    
    uuid_test = "uuid-123"
    
    req1 = logging_pb2.LogRequest(uuid=uuid_test, msg="First attempt")
    servicer.LogMessage(req1, mock_context)
    
    assert len(message_map) == 1
    assert message_map[uuid_test] == "First attempt"
    
    req2 = logging_pb2.LogRequest(uuid=uuid_test, msg="Second attempt (duplicate)")
    servicer.LogMessage(req2, mock_context)
    
    assert len(message_map) == 1
    assert message_map[uuid_test] == "First attempt"

def test_grpc_logging_service_get_messages():
    message_map.clear()
    servicer = LoggingServiceServicer()
    mock_context = MagicMock()
    
    message_map["uuid-1"] = "Msg1"
    message_map["uuid-2"] = "Msg2"
    
    response = servicer.GetMessages(logging_pb2.EmptyRequest(), mock_context)
    assert "Msg1" in response.messages
    assert "Msg2" in response.messages

@patch('facade_service_bonus.grpc.insecure_channel')
def test_facade_retry_mechanism_success_after_failures(mock_channel):
    mock_stub = MagicMock()
    mock_channel.return_value.__enter__.return_value = MagicMock()
    
    mock_stub.LogMessage.side_effect = [
        Exception("Connection failed 1"),
        Exception("Connection failed 2"),
        logging_pb2.LogResponse(status="success")
    ]
    
    with patch('facade_service_bonus.logging_pb2_grpc.LoggingServiceStub', return_value=mock_stub):
        send_grpc_log.retry.statistics.clear()
        
        status = send_grpc_log("test-uuid", "test-msg")
        
        assert status == "success"
        assert mock_stub.LogMessage.call_count == 3

@patch('facade_service_bonus.grpc.insecure_channel')
def test_facade_retry_mechanism_fails_after_max_attempts(mock_channel):
    mock_stub = MagicMock()
    mock_channel.return_value.__enter__.return_value = MagicMock()
    
    mock_stub.LogMessage.side_effect = Exception("Persistent connection error")
    
    with patch('facade_service_bonus.logging_pb2_grpc.LoggingServiceStub', return_value=mock_stub):
        send_grpc_log.retry.statistics.clear()
        
        with pytest.raises(RetryError):
            send_grpc_log("test-uuid", "test-msg")
            
        assert mock_stub.LogMessage.call_count == 3
