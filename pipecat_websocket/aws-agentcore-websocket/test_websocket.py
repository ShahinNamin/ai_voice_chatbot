import asyncio
import websockets
import json
from urllib.parse import quote

async def local_websocket():
    local = False
    uri = "ws://localhost:8080/ws"
    if local == False:    
        # agent_runtime_arn = "arn:aws:bedrock-agentcore:ap-southeast-2:145770591129:runtime/pipecat_agent-6kzrHf2xwM"
        print(quote(agent_runtime_arn, safe=''))
        uri = f"wss://bedrock-agentcore.ap-southeast-2.amazonaws.com/runtimes/{quote(agent_runtime_arn, safe='')}/ws"

    try:
        async with websockets.connect(uri) as websocket:
            # Send a message
            await websocket.send(json.dumps({"inputText": "Hello WebSocket!"}))

            # Receive the echo response
            response = await websocket.recv()
            print(f"Received: {response}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(local_websocket())