from bedrock_agentcore.runtime import AgentCoreRuntimeClient
import websockets
import asyncio
import json
import os

async def main():
    # Get runtime ARN from environment variable
    # runtime_arn = os.getenv('AGENT_ARN')
    runtime_arn = "arn:aws:bedrock-agentcore:ap-southeast-2:145770591129:runtime/pipecat_agent-6kzrHf2xwM"
    if not runtime_arn:
        raise ValueError("AGENT_ARN environment variable is required")

    # Initialize client
    client = AgentCoreRuntimeClient(region="ap-southeast-2")

    # Generate WebSocket connection with authentication
    ws_url, headers = client.generate_ws_connection(
        runtime_arn=runtime_arn
    )

    try:
        async with websockets.connect(ws_url, additional_headers=headers) as ws:
            # Send message
            await ws.send(json.dumps({"inputText": "Hello!"}))

            # Receive response
            response = await ws.recv()
            print(f"Received: {response}")
    except websockets.exceptions.InvalidStatus as e:
        print(f"WebSocket handshake failed with status code: {e.response.status_code}")
        print(f"Response headers: {e.response.headers}")
        print(f"Response body: {e.response.body.decode()}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())