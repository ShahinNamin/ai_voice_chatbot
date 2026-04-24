# PBX Bridge — Chime Voice Connector → AgentCore

A standalone Python service that bridges Amazon Chime SDK Voice Connector telephone calls to an AWS Bedrock AgentCore Pipecat voice agent over WebSocket.

```
Phone call
   │
   ▼
Amazon Chime SDK Voice Connector
   │  SIP INVITE  (UDP 5060)
   │  RTP audio   (UDP 10000-20000)
   ▼
┌──────────────────────────────────┐
│          PBX Bridge              │
│                                  │
│  SipHandler                      │
│    • Parses INVITE / BYE         │
│    • Allocates RTP port          │
│    • Sends SIP 200 OK + SDP      │
│                                  │
│  RtpSession                      │
│    • Receives G.711 µ/A-law RTP  │
│    • Decodes → PCM16 @ 8 kHz    │
│    • Upsamples → PCM16 @ 16 kHz │
│    • Packs Pipecat binary frames │
│                                  │
│  AgentCoreBridge                 │
│    • SigV4-signed WSS URL        │
│    • Sends/receives binary audio │
│    • Downsamples 16→8 kHz        │
│    • Encodes G.711, sends RTP    │
└──────────────────────────────────┘
   │  WSS (SigV4-signed)
   ▼
AWS Bedrock AgentCore
(Pipecat voice agent)
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Python 3.10–3.12 | `audioop` built-in; Python 3.13 removes it |
| EC2 / ECS task role | Must have `bedrock-agentcore:InvokeRuntime` |
| Chime Voice Connector | Origination set to this host on port 5060 |
| AgentCore runtime ARN | Set `AGENT_RUNTIME_ARN` env var |

---

## Quick start (EC2)

```bash
# 1. Clone / copy files
git clone <repo> pbx-bridge && cd pbx-bridge

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure
cp .env.example .env
# Edit .env – set AGENT_RUNTIME_ARN and AWS_REGION at minimum

# 4. Run (needs root or CAP_NET_BIND_SERVICE for port 5060)
sudo python bridge.py
# or run on a non-privileged port:
SIP_PORT=5060 python bridge.py
```

---

## Docker / ECS

```bash
# Build
docker build -t pbx-bridge .

# Run locally with .env
docker run --rm --env-file .env \
  -p 5060:5060/udp \
  -p 10000-20000:10000-20000/udp \
  pbx-bridge

# On ECS the task execution role supplies AWS credentials automatically.
# No AWS_* env vars are required.
```

### ECS task definition snippet

```json
{
  "portMappings": [
    { "containerPort": 5060,  "protocol": "udp" },
    { "containerPort": 10000, "protocol": "udp" },
    ...
  ],
  "environment": [
    { "name": "AGENT_RUNTIME_ARN", "value": "arn:aws:bedrock-agentcore:..." },
    { "name": "AWS_REGION",        "value": "us-east-1" }
  ]
}
```

> **No AWS_ACCESS_KEY_ID / SECRET needed** — credentials are fetched from the
> task role via `boto3.Session().get_credentials()`.

---

## Amazon Chime Voice Connector configuration

1. In the Chime SDK console, open your Voice Connector.
2. Under **Origination**, add a route:
   - **Host**: public IP or DNS of this service
   - **Port**: 5060
   - **Protocol**: UDP
   - **Priority / Weight**: 1 / 1
3. Ensure inbound Security Group rules allow:
   - `UDP 5060` (SIP)
   - `UDP 10000–20000` (RTP) from Chime's IP ranges

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `AGENT_RUNTIME_ARN` | *(required)* | ARN of the Bedrock AgentCore runtime |
| `AWS_REGION` | `us-east-1` | Region of the AgentCore runtime |
| `SIGNED_URL_EXPIRY_SECONDS` | `300` | Lifetime of the signed WS URL |
| `SIP_HOST` | `0.0.0.0` | SIP listener bind address |
| `SIP_PORT` | `5060` | SIP listener UDP port |
| `RTP_PORT_MIN` | `10000` | Lower bound of ephemeral RTP port range |
| `RTP_PORT_MAX` | `20000` | Upper bound of ephemeral RTP port range |
| `LOCAL_IP` | *(auto-detect)* | Override the IP advertised in SDP |
| `LOCAL_AGENT` | `0` | Set `1` to skip AgentCore; connect to `LOCAL_AGENT_WS_URL` |
| `LOCAL_AGENT_WS_URL` | `ws://localhost:8080/ws` | Target when `LOCAL_AGENT=1` |

---

## Audio pipeline

```
Chime → bridge (inbound)
  G.711 µ-law  (8 kHz, 20 ms, ~90 kbps with IP overhead)
    │  audioop.ulaw2lin
    ▼
  PCM16 @ 8 kHz
    │  audioop.ratecv  8k→16k
    ▼
  PCM16 @ 16 kHz  → Pipecat binary frame → AgentCore WS

AgentCore → bridge → Chime (outbound)
  Pipecat binary frame  (PCM16 @ 16 kHz)
    │  audioop.ratecv  16k→8k
    ▼
  PCM16 @ 8 kHz
    │  audioop.lin2ulaw
    ▼
  G.711 µ-law  → RTP packet → Chime
```

---

## Concurrency model

- One `asyncio` event loop handles all SIP and RTP I/O.
- Each call spawns two coroutine tasks: `_rtp_to_ws` and `_ws_to_rtp`.
- RTP sockets are set non-blocking; `loop.sock_recv` yields to the event loop between packets.
- The design is intentionally single-process; for very high concurrency (>50 simultaneous calls) consider running multiple instances behind a load balancer.

---

## Logging

Set the `LOG_LEVEL` environment variable or edit `logging.basicConfig` in `bridge.py`:

```bash
LOG_LEVEL=DEBUG python bridge.py
```

---

## Local development / testing

```bash
# Start a local Pipecat agent on ws://localhost:8080/ws, then:
LOCAL_AGENT=1 python bridge.py

# Send a test SIP INVITE with SIPp:
sipp -sn uac localhost -p 5060 -m 1
```
