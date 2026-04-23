# Chime Voice Connector → AgentCore Bridge

A self-contained pure-Python asyncio service that bridges Amazon Chime Voice Connector calls directly to your Pipecat AI voice agent on AWS Bedrock AgentCore. No other services required.

```
PSTN caller
   │
   ▼
Amazon Chime Voice Connector
   │  SIP INVITE  (UDP 5060)
   ▼
┌─────────────────────────────────────────┐
│           bridge.py  (this service)     │
│                                         │
│  SipProtocol  ←─ SIP signaling          │
│       │                                 │
│  RtpProtocol  ←─ G.711 μ-law RTP        │
│       │  audioop decode/encode          │
│  SigV4-signed WebSocket ──► AgentCore   │
└─────────────────────────────────────────┘
```

## Deploy

### EC2

```bash
pip install -r requirements.txt

export AGENT_RUNTIME_ARN=arn:aws:bedrock-agentcore:us-east-1:123456789:runtime/xxxxx
export AWS_REGION=us-east-1
export PUBLIC_IP=<your-elastic-ip>

python bridge.py
```

### ECS / Docker

```bash
docker build -t chime-bridge .
docker run -d \
  --env-file .env \
  --network host \
  --name chime-bridge \
  chime-bridge
```

> **ECS launch type**: use EC2, not Fargate. Fargate does not support `host` networking, and mapping a large UDP port range (10000–20000) is impractical there.

## AWS Security Group rules

| Type | Protocol | Port range | Source |
|------|----------|------------|--------|
| SIP  | UDP | 5060 | Chime Voice Connector CIDR ranges* |
| RTP  | UDP | 10000–20000 | Same Chime CIDR ranges |

*Filter the [AWS IP ranges JSON](https://ip-ranges.amazonaws.com/ip-ranges.json) on `service: CHIME_VOICECONNECTOR` and `region: us-east-1`.

## Chime Voice Connector — Origination settings

In the Chime SDK console → Voice Connectors → your connector → **Origination**:

| Field | Value |
|-------|-------|
| Host | `<your Elastic Public IP>` |
| Port | `5060` |
| Protocol | `UDP` |
| Priority | `1` |
| Weight | `1` |

This tells Chime to forward inbound PSTN calls to your bridge.

## Audio format contract

| Direction | Format |
|-----------|--------|
| RTP → WebSocket | 16-bit signed PCM, 8 kHz mono, little-endian, 20 ms frames (320 bytes) |
| WebSocket → RTP | Same format — the bridge re-encodes to G.711 μ-law before sending |

Your Pipecat pipeline on AgentCore should use `InputAudioRawFrame` / `OutputAudioRawFrame` with `sample_rate=8000, num_channels=1`.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `AGENT_RUNTIME_ARN` | ✅ | ARN of your AgentCore runtime |
| `AWS_REGION` | ✅ | e.g. `us-east-1` |
| `SIGNED_URL_EXPIRY_SECONDS` | — | Signed URL TTL, default `300` |
| `PUBLIC_IP` | ✅ | Elastic IP announced in SDP |
| `SIP_BIND_HOST` | — | Bind address, default `0.0.0.0` |
| `RTP_PORT_MIN` | — | Start of RTP port range, default `10000` |
| `RTP_PORT_MAX` | — | End of RTP port range, default `20000` |
| `LOG_LEVEL` | — | `debug` / `info` / `warning`, default `info` |

## Troubleshooting

**No audio from agent back to caller**  
Check that your Pipecat agent emits `OutputAudioRawFrame` at 8 kHz. If the agent runs at 16 kHz or 24 kHz internally, add a resampling step in Pipecat before the output transport.

**SIP 488 Not Acceptable**  
The bridge couldn't parse the SDP from Chime. Enable `LOG_LEVEL=debug` and verify the INVITE contains a valid `c=IN IP4` and `m=audio` line.

**WebSocket connection fails**  
Confirm `AGENT_RUNTIME_ARN` and credentials are correct. The signed URL expires after `SIGNED_URL_EXPIRY_SECONDS` — a new URL is generated fresh for every call, so short calls are unaffected.

**Call drops after ~30 s**  
Chime sends SIP `OPTIONS` keep-alives; the bridge responds with `200 OK` automatically. If you see `re-INVITE` (codec renegotiation), add handling in `_handle_invite` for existing `call_id` sessions.
