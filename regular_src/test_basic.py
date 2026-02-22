import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock

from messages_service import app as messages_app
from logging_service import app as logging_app, message_map
from facade_service import app as facade_app

messages_client = TestClient(messages_app)
logging_client = TestClient(logging_app)
facade_client = TestClient(facade_app)

def test_messages_service_returns_static_text():
    response = messages_client.get("/message")
    assert response.status_code == 200
    assert response.text == "Hi from Bohdan!"

def test_logging_service_stores_and_returns_messages():
    message_map.clear()
    
    post_data = {"uuid": "123e4567-e89b-12d3-a456-426614174000", "msg": "Hello World"}
    response = logging_client.post("/log", json=post_data)
    assert response.status_code == 200
    assert response.json() == {"status": "success"}
    assert message_map[post_data["uuid"]] == "Hello World"
    
    get_response = logging_client.get("/log")
    assert get_response.status_code == 200
    assert get_response.text == "Hello World"

@pytest.mark.asyncio
@patch('facade_service.httpx.AsyncClient.post', new_callable=AsyncMock)
async def test_facade_post_generates_uuid_and_calls_logging(mock_post):
    mock_post.return_value.status_code = 200
    
    response = facade_client.post("/facade", json={"msg": "Test message"})
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "uuid" in data
    
    mock_post.assert_called_once()
    called_args = mock_post.call_args
    assert called_args[1]["json"]["msg"] == "Test message"

@pytest.mark.asyncio
@patch('facade_service.httpx.AsyncClient.get', new_callable=AsyncMock)
async def test_facade_get_concatenates_responses(mock_get):
    async def mock_get_side_effect(url):
        mock_response = AsyncMock()
        if "log" in url:
            mock_response.text = "Message 1, Message 2"
        elif "message" in url:
            mock_response.text = "not implemented yet"
        return mock_response
    
    mock_get.side_effect = mock_get_side_effect
    
    response = facade_client.get("/facade")
    
    assert response.status_code == 200
    assert response.text == "Message 1, Message 2: not implemented yet"
