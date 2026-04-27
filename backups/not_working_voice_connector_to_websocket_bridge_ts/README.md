# Chime Voice Connector → AgentCore WebSocket Bridge

A production-ready TypeScript service that acts as a **SIP/PBX server**, bridging phone calls received from **Amazon Chime SDK Voice Connector** directly to a **Pipecat voice agent running on AWS Bedrock AgentCore**.

```
Caller
  │
  │ PSTN
  ▼
Amazon Chime SDK Voice Connector
  │
  │ SIP (UDP 5060) + RTP audio
  ▼
THIS SERVICE  (chime-agentcore-bridge)
  ├── SIP UAS       – handles INVITE / ACK / BYE / CANCEL / OPTIONS
  ├── RTP handler   – receives & sends G.711 audio over UDP
  ├── G.711 codec   – decodes PCMU/PCMA → PCM, encodes PCM → PCMU/PCMA
  └── AgentCore WS  – Pipecat-compatible bidirectional WebSocket
        │
        │ WSS (SigV4 signed URL, obtained from EC2/ECS role)
        ▼
AWS Bedrock AgentCore (Pipecat voice agent)
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Node.js | ≥ 18 |
| AWS account | With Bedrock AgentCore enabled |
| AgentCore runtime | Deployed Pipecat agent |
| Chime Voice Connector | Configured in us-east-1 |
| IAM role | Attached to EC2/ECS with `bedrock-agentcore:InvokeRuntime` permission |

---

## Quick Start

### 1. Install dependencies

```bash
npm install
```

### 2. Build

```bash
npm run build
```

### 3. Set environment variables

```bash
export AGENT_RUNTIME_ARN="arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/your-runtime-id"
export AWS_REGION="us-east-1"

export PUBLIC_IP="public IP address of the instance"   # your EC2 public IP

# Optional overrides:
# export PUBLIC_IP="1.2.3.4"        # EC2 public IP (required if behind NAT)
# export SIP_PORT="5060"
# export RTP_MIN_PORT="10000"
# export RTP_MAX_PORT="20000"
# export HEALTH_PORT="8080"
# export LOG_LEVEL="info"
```

### 4. Run

```bash
npm start
```

Or for development (no build step):

```bash
npm run dev
```

---

## AWS Credentials

The bridge **never reads credentials from environment variables** on production.  
It resolves credentials in this order:

1. **ECS task role** – via `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` / `AWS_CONTAINER_CREDENTIALS_FULL_URI` (set automatically by ECS)
2. **EC2 instance profile** – via IMDSv2 (`http://169.254.169.254`)
3. **Environment variables** – `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (local dev fallback only)

This mirrors Python's `boto3.Session().get_credentials().get_frozen_credentials()`.

---

## IAM Policy

Attach the following inline policy to your EC2 instance role or ECS task role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AgentCoreInvoke",
      "Effect": "Allow",
      "Action": [
        "bedrock-agentcore:InvokeRuntime",
        "bedrock-agentcore:GetRuntime"
      ],
      "Resource": "arn:aws:bedrock-agentcore:us-east-1:*:runtime/*"
    }
  ]
}
```

---

## Amazon Chime Voice Connector Configuration

In the [AWS Chime console](https://console.aws.amazon.com/chime/) or via CLI:

1. **Create a Voice Connector** in `us-east-1`.
2. Under **Origination** (calls _from_ the PSTN _to_ your service):
   - Add a route with:
     - **Host**: Your EC2/ELB public IP or DNS
     - **Port**: `5060`
     - **Protocol**: `UDP`
     - **Priority**: `1`
     - **Weight**: `1`
3. Under **Streaming** (optional) – not required for this bridge.
4. Assign a phone number to the Voice Connector.

> **Important**: Chime Voice Connector sends SIP from the IP ranges documented at  
> https://docs.aws.amazon.com/chime-sdk/latest/ag/network-config.html  
> Your EC2 security group **must** allow inbound UDP 5060 from those ranges,  
> and UDP 10000–20000 for RTP.

---

## Security Group Rules

| Type | Protocol | Port Range | Source |
|---|---|---|---|
| Custom UDP | UDP | 5060 | Chime signalling IPs |
| Custom UDP | UDP | 10000 – 20000 | Chime media IPs |
| Custom TCP | TCP | 8080 | Your monitoring / load balancer |

Chime IP ranges (us-east-1): see AWS documentation for the current list.

---

## Docker / ECS Deployment

### Build image

```bash
docker build -t chime-agentcore-bridge .
```

### Run locally

```bash
docker run --rm \
  -e AGENT_RUNTIME_ARN="arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/abc" \
  -e AWS_REGION="us-east-1" \
  -e PUBLIC_IP="1.2.3.4" \
  -p 5060:5060/udp \
  -p 10000-20000:10000-20000/udp \
  -p 8080:8080 \
  chime-agentcore-bridge
```

### ECS Task Definition (excerpt)

```json
{
  "containerDefinitions": [
    {
      "name": "bridge",
      "image": "your-ecr-repo/chime-agentcore-bridge:latest",
      "portMappings": [
        { "containerPort": 5060,  "protocol": "udp" },
        { "containerPort": 8080,  "protocol": "tcp" }
      ],
      "environment": [
        { "name": "AGENT_RUNTIME_ARN", "value": "arn:aws:bedrock-agentcore:..." },
        { "name": "AWS_REGION",        "value": "us-east-1" },
        { "name": "PUBLIC_IP",         "value": "REPLACE_WITH_EIP" }
      ],
      "healthCheck": {
        "command": ["CMD-SHELL", "wget -qO- http://localhost:8080/health || exit 1"],
        "interval": 30,
        "timeout": 5,
        "retries": 3
      }
    }
  ],
  "taskRoleArn": "arn:aws:iam::123456789012:role/ChimeBridgeTaskRole",
  "networkMode": "host"
}
```

> Use `"networkMode": "host"` so that UDP port binding works correctly for both SIP and RTP.

---

## Project Structure

```
src/
├── index.ts            Entry point – starts BridgeServer, handles signals
├── bridge.ts           BridgeServer – top-level orchestrator + HTTP health
├── sipServer.ts        SIP UAS – UDP 5060, handles INVITE/ACK/BYE/OPTIONS
├── sip.ts              SIP message parser & response/BYE builder
├── sdp.ts              SDP offer parser & answer builder
├── rtp.ts              RTP UDP socket – send/receive audio packets
├── rtpPortManager.ts   Port pool allocator for RTP sessions
├── callSession.ts      Per-call state: RTP ↔ codec ↔ AgentCore WS
├── agentCoreClient.ts  Pipecat-compatible WebSocket client for AgentCore
├── awsAuth.ts          IMDSv2 / ECS credential resolver + SigV4 URL signer
├── codec.ts            G.711 μ-law/A-law ↔ 16-bit linear PCM codec
└── logger.ts           Structured timestamp logger
```

---

## Audio Flow Detail

```
Chime RTP packet (G.711 PCMU/PCMA, 160 bytes / 20ms)
  │
  ▼ rtp.ts: strip 12-byte RTP header
  │
  ▼ codec.ts: g711ToLinear16()  →  320 bytes of 16-bit LE PCM @ 8kHz
  │
  ▼ agentCoreClient.ts: wrap in Pipecat binary frame [type=0x01][len][pcm]
  │
  ▼ WebSocket → AgentCore → Pipecat voice agent
  
  (reverse path for agent speech output)
```

---

## Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `AGENT_RUNTIME_ARN` | ✅ | — | ARN of the AgentCore runtime |
| `AWS_REGION` | — | `us-east-1` | AWS region |
| `PUBLIC_IP` | Recommended | auto-detect | IP advertised in SDP (use EIP on EC2 or ELB IP) |
| `SIP_PORT` | — | `5060` | UDP port for SIP signalling |
| `RTP_MIN_PORT` | — | `10000` | Start of RTP port pool |
| `RTP_MAX_PORT` | — | `20000` | End of RTP port pool |
| `HEALTH_PORT` | — | `8080` | TCP port for HTTP health endpoint |
| `SIGNED_URL_EXPIRY_SECONDS` | — | `300` | Lifetime of the SigV4 signed WSS URL |
| `LOG_LEVEL` | — | `info` | `debug` \| `info` \| `warn` \| `error` |

---

## Health Endpoints

| Path | Method | Description |
|---|---|---|
| `GET /health` | HTTP | Returns `{"status":"ok",...}` – use for ALB/ECS health check |
| `GET /status` | HTTP | Active call count, port pool stats |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| No SIP messages arriving | Security group blocks UDP 5060 | Open UDP 5060 from Chime IP ranges |
| `401 Unauthorized` from AgentCore | IAM role missing permission | Add `bedrock-agentcore:InvokeRuntime` |
| One-way audio | Wrong IP in SDP answer | Set `PUBLIC_IP` env var to your public IP |
| `Missing AGENT_RUNTIME_ARN` | Env not set | Set `AGENT_RUNTIME_ARN` |
| Credentials error on ECS | Task role not attached | Attach IAM role to ECS task definition |
| RTP port pool exhausted | Too many simultaneous calls | Increase `RTP_MAX_PORT` |
