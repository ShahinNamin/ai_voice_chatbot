"""
PBX Bridge v3: Amazon Chime SDK Voice Connector -> AWS Bedrock AgentCore (WebRTC/KVS)
======================================================================================

Receives SIP signalling on UDP 5060 and RTP audio, bridges bidirectionally
to the Pipecat agent running on AgentCore via WebRTC (SmallWebRTCTransport)
with KVS managed TURN for NAT traversal.

Architecture:
  Chime Voice Connector
       |  SIP (UDP 5060)
       v
  SipHandler  --> deduplicates retransmits, allocates RTP port, sends SIP 200 OK
       |  RTP (dynamic UDP port)   ~90 kbps up/down (Chime spec)
       v
  RtpSession  --> decode u-law/PCMA (8 kHz, 20ms frames, 160 samples)
               -> upsample 8 kHz -> 16 kHz PCM16 (ElevenLabs STT input rate)
       |
       v
  AgentCoreWebRtcBridge
       |
       |  Step 1 – fetch KVS TURN credentials (boto3 KVS API)
       |           -> KVS TURN credentials
       |
       |  Step 2 – create aiortc RTCPeerConnection with TURN ICE servers
       |           attach RawAudioTrack (pipecat) to feed 16 kHz PCM16 to agent
       |           create SDP offer
       |
       |  Step 3 – POST /invocations  {"action":"offer", "sdp":..., "type":"offer"}
       |           -> SDP answer from Pipecat/ElevenLabs agent
       |
       |  Step 4 – trickle ICE candidates via POST /invocations
       |           {"action":"ice_candidate", "candidate":..., ...}
       |
       |  WebRTC media path (DTLS/SRTP via KVS TURN relay, TCP fallback)
       v
  AgentCore (Bedrock) – Pipecat + ElevenLabs STT/TTS
       |  ElevenLabs STT expects 16 kHz PCM16 (bridge upsamples 8k->16k)
       |  ElevenLabs TTS outputs 16 kHz PCM16 (bridge downsamples 16k->8k)
       |  audio returned over the same WebRTC peer connection
       v
  RtpSession  --> downsample 16 kHz -> 8 kHz
               -> encode u-law/PCMA -> 20ms/160-sample RTP frames -> Chime

Audio rate summary
------------------
  Chime VC  : G.711 u-law or a-law, 8 kHz, 20 ms frames, 160 samples/frame
  Bridge→Agent (WebRTC) : PCM16, 16 kHz, 20 ms frames, 320 samples/frame
  Agent→Bridge (WebRTC) : Opus (WebRTC), decoded by aiortc at 48 kHz fltp
                          (ElevenLabs TTS is 16 kHz before Opus encoding; aiortc always decodes Opus at 48 kHz)
                          bridge resamples dynamically: reads frame.sample_rate per frame (48 kHz → 8 kHz)
  Bridge→Chime: G.711 u-law or a-law, 8 kHz, 20 ms frames, 160 samples/frame

Key design decisions
--------------------
* The bridge INTRODUCES the call via HTTP signaling then steps aside —
  media flows over WebRTC through KVS TURN without a long-lived websocket.
* aiortc drives the local peer connection; audio I/O via custom
  aiortc is used directly for the bridge-side peer connection.
  pipecat's RawAudioTrack handles outbound audio to the agent.
* All SIP/RTP session management (keepalive, teardown, RTCP, re-INVITE)
  is unchanged from v2 — the Chime side is identical.
* A boto3 thread-pool executor handles all AgentCore invocation calls.

Required Python packages (add to requirements.txt):
  aiortc>=1.9.0
  av                 # pulled in by pipecat[webrtc] / aiortc
  pipecat-ai[webrtc] # provides RawAudioTrack, RTCIceServer, aiortc

Required environment variables:
  AGENT_RUNTIME_ARN  - full ARN of your AgentCore runtime  (REQUIRED)
  AWS_REGION         - AWS region                          (default: us-east-1)
  KVS_CHANNEL_NAME   - KVS signaling channel name that matches the Pipecat
                       agent's KVS_CHANNEL_NAME env var   (default: voice-agent-turn)

Optional environment variables (unchanged from v2 unless noted):
  SIP_HOST, SIP_PORT, RTP_PORT_MIN/MAX, HTTP_HEALTH_PORT,
  CHIME_VC_FQDN, OPTIONS_INTERVAL, SESSION_EXPIRES_S, MIN_SE_S,
  SIP_SESSION_REFRESH_S, RTP_SILENCE_TIMEOUT_S, WS_IDLE_TIMEOUT_S,
  RTP_SILENCE_HANGUP_S, DISCONNECT_LOG_FILE, LOCAL_AGENT
  ICE_TIMEOUT_S      - seconds to wait for ICE connected   (default: 15)
  LOCAL_AGENT        - set to "1" to skip KVS and use STUN only (local dev)

DISCONNECT DIAGNOSTICS (carried over from v2)
----------------------------------------------
Structured DISCONNECT REPORT logged on every call teardown.
"""

import asyncio
try:
    import audioop
except ImportError:
    try:
        import audioop_lts as audioop
    except ImportError:
        raise ImportError(
            "audioop is not available. On Python 3.13+ install: pip install audioop-lts"
        )
import json
import logging
import os
import random
import re
import socket
import struct
import time as _time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional

import boto3
from dotenv import load_dotenv

# aiortc — used directly for the bridge-side peer connection (we are the offerer;
# the agent-side uses SmallWebRTCConnection which is the answerer).
from aiortc import (
    RTCConfiguration,
    RTCIceServer,
    RTCPeerConnection,
    RTCSessionDescription,
)

# Pipecat's RawAudioTrack is the production-grade aiortc AudioStreamTrack that
# already handles 10ms chunking, internal queue, frame timing, and silence
# padding — exactly what our old ChimeAudioSenderTrack reimplemented by hand.
from pipecat.transports.smallwebrtc.transport import RawAudioTrack

load_dotenv(override=True)

# Verify audioop
try:
    audioop.ulaw2lin(b'\x7f' * 2, 2)
except Exception as _e:
    raise RuntimeError(f"audioop self-test failed: {_e}")

_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pbx-bridge")

_DISCONNECT_LOG_FILE = os.getenv("DISCONNECT_LOG_FILE", "")
if _DISCONNECT_LOG_FILE:
    _disc_handler = logging.FileHandler(_DISCONNECT_LOG_FILE)
    _disc_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _disc_log = logging.getLogger("pbx-bridge.disconnect")
    _disc_log.addHandler(_disc_handler)
    _disc_log.setLevel(logging.DEBUG)
else:
    _disc_log = log

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SIP_HOST          = os.getenv("SIP_HOST", "0.0.0.0")
SIP_PORT          = int(os.getenv("SIP_PORT", "5060"))
RTP_PORT_MIN      = int(os.getenv("RTP_PORT_MIN", "10000"))
RTP_PORT_MAX      = int(os.getenv("RTP_PORT_MAX", "20000"))
AGENT_RUNTIME_ARN = os.getenv("AGENT_RUNTIME_ARN", "")
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
HTTP_HEALTH_PORT  = int(os.getenv("HTTP_HEALTH_PORT", "8080"))
CHIME_VC_FQDN     = os.getenv("CHIME_VC_FQDN", "")
OPTIONS_INTERVAL  = int(os.getenv("OPTIONS_INTERVAL", "30"))

# Session timer (RFC 4028)
#
# SESSION_EXPIRES_S controls how often we send a SIP re-INVITE to Chime.
# Chime Voice Connector has a ~150s media inactivity timer: if the PSTN caller
# goes quiet (listening to the agent) for >150s, Chime kills its inbound RTP
# stream. A periodic re-INVITE resets this timer.
#
# Default: 120s  ->  refresh fires at ~50s (SE/2 - 10), safely before 150s.
# Do NOT raise this above 120 without also adjusting RTP_SILENCE_HANGUP_S.
SESSION_EXPIRES_S     = int(os.getenv("SESSION_EXPIRES_S", "120"))
MIN_SE_S              = int(os.getenv("MIN_SE_S", "90"))
SIP_SESSION_REFRESH_S = int(os.getenv("SIP_SESSION_REFRESH_S", "50"))

# Watchdog timeouts
RTP_SILENCE_TIMEOUT_S = int(os.getenv("RTP_SILENCE_TIMEOUT_S", "10"))
WS_IDLE_TIMEOUT_S     = int(os.getenv("WS_IDLE_TIMEOUT_S", "30"))
RTP_SILENCE_HANGUP_S  = int(os.getenv("RTP_SILENCE_HANGUP_S", "45"))

# WebRTC / KVS
KVS_CHANNEL_NAME  = os.getenv("KVS_CHANNEL_NAME", "voice-agent-turn")
ICE_TIMEOUT_S     = int(os.getenv("ICE_TIMEOUT_S", "30"))

# G.711 / RTP — Chime Voice Connector spec:
#   G.711 u-law or a-law, 8 kHz, 20 ms frames, ~90 kbps per call
CHIME_SAMPLE_RATE   = 8000
CHIME_FRAME_MS      = 20
CHIME_FRAME_SAMPLES = CHIME_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 160 samples
PCMU_SILENCE_FRAME  = bytes([0x7F] * 160)
PCMA_SILENCE_FRAME  = bytes([0xD5] * 160)

# ElevenLabs STT expects 16 kHz PCM16 input — this is what we send TO the agent.
# AGENT_SAMPLE_RATE controls the rate used for the outbound RawAudioTrack (bridge→agent).
# Default 16000 matches ElevenLabs STT input requirement.
AGENT_SAMPLE_RATE   = int(os.getenv("AGENT_SAMPLE_RATE", "16000"))
AGENT_FRAME_SAMPLES = AGENT_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 320 samples @ 16kHz

# FORCE_AGENT_RX_RATE: if set, override the sample rate read from incoming WebRTC
# audio frames (agent→bridge direction) before resampling to 8 kHz for Chime.
# Leave unset (default) to use frame.sample_rate as reported by aiortc.
# Useful values to try:
#   unset  — use aiortc's frame.sample_rate (typically 48000 for Opus)
#   48000  — force treat incoming audio as 48 kHz (same as unset for Opus)
#   16000  — force treat as 16 kHz (if agent sends raw PCM over WebRTC)
#   24000  — ElevenLabs Flash/Turbo models output 24 kHz
#   44100  — legacy / MP3 pipeline
FORCE_AGENT_RX_RATE = int(os.getenv("FORCE_AGENT_RX_RATE", "0"))  # 0 = auto from frame

RTP_PAYLOAD_PCMU    = 0
RTP_PAYLOAD_PCMA    = 8
RTP_VERSION         = 2

# ---------------------------------------------------------------------------
# Ringback tone (G.711 μ-law, 8 kHz, 440 Hz sine, 2s on / 4s off cycle)
#
# Played to the caller by the RTP keepalive task while the bridge is waiting
# for AgentCore to cold-start (which can take 15-30s on a fresh container).
# Without this the caller hears dead silence and often hangs up.
#
# Cycle: 100 × 20ms frames of 440 Hz tone, then 200 × 20ms frames of silence
# = 2s ring, 4s quiet, repeating — matches standard PSTN ringback cadence.
# ---------------------------------------------------------------------------
def _make_ringback_cycle() -> list:
    import struct as _s, audioop as _a, math as _m
    frames = []
    for i in range(100):          # 2 s of tone (100 × 20 ms)
        pcm = _s.pack('<160h', *[
            max(-32768, min(32767, int(0.7 * _m.sin(2 * _m.pi * 440 * (i * 160 + j) / 8000) * 32768)))
            for j in range(160)
        ])
        frames.append(_a.lin2ulaw(pcm, 2))
    sil = bytes([0x7F] * 160)
    for _ in range(200):          # 4 s of silence (200 × 20 ms)
        frames.append(sil)
    return frames                 # 300-frame / 6-second ring cycle

RINGBACK_CYCLE_PCMU = _make_ringback_cycle()

# Validate required env vars at import time so failures are obvious
if not os.getenv("LOCAL_AGENT") == "1":
    if not AGENT_RUNTIME_ARN:
        raise RuntimeError(
            "AGENT_RUNTIME_ARN environment variable is required.\n"
            "Set it to your AgentCore runtime ARN, e.g.:\n"
            "  arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/my-agent-XXXX\n"
            "For local development without AgentCore, set LOCAL_AGENT=1"
        )

SIP_TLS_PORT = int(os.getenv("SIP_TLS_PORT", "0"))
SIP_TLS_CERT = os.getenv("SIP_TLS_CERT", "")
SIP_TLS_KEY  = os.getenv("SIP_TLS_KEY",  "")


# ---------------------------------------------------------------------------
# AWS helpers
# ---------------------------------------------------------------------------

# Thread-pool executor for running synchronous boto3 calls without blocking
# the asyncio event loop.
import concurrent.futures
import urllib3
_boto3_executor = concurrent.futures.ThreadPoolExecutor(max_workers=16,
                                                        thread_name_prefix="agentcore")

# ---------------------------------------------------------------------------
# AgentCore client via boto3 invoke_agent_runtime.
# boto3 handles URL construction, endpoint resolution, and SigV4 signing.
#
# The StreamingBody returned by invoke_agent_runtime wraps a urllib3 response.
# We access the underlying socket via ._raw_stream to read SSE events as they
# arrive without any intermediate buffering by botocore.
# ---------------------------------------------------------------------------
from botocore.config import Config as _BotocoreConfig

_agentcore_client = None  # created lazily per-thread to avoid credential caching issues


def _get_agentcore_client():
    """Return a boto3 bedrock-agentcore client with streaming-friendly config."""
    return boto3.client(
        "bedrock-agentcore",
        region_name=AWS_REGION,
        config=_BotocoreConfig(
            connect_timeout=10,
            read_timeout=30,
            retries={"max_attempts": 0},
        ),
    )


def _invoke_agent_runtime_sync(session_id: str, payload: bytes) -> bytes:
    """
    Call InvokeAgentRuntime via boto3 and stream the SSE response.

    boto3 builds the correct URL and signs it. We read the StreamingBody
    via its underlying _raw_stream to get SSE events as they arrive.

    Returns the raw bytes of the first SSE data event containing '"sdp"'.
    """
    client = _get_agentcore_client()
    resp = client.invoke_agent_runtime(
        agentRuntimeArn=AGENT_RUNTIME_ARN,
        runtimeSessionId=session_id,
        payload=payload,
        contentType="application/json",
        accept="application/json",
    )

    status = resp.get("statusCode", 200)
    log.info("AgentCore HTTP status=%d  session=%s", status, session_id[:8])

    streaming_body = resp["response"]

    # Access the underlying urllib3 HTTPResponse for true chunk-by-chunk reading.
    # StreamingBody wraps a urllib3 response; ._raw_stream is the HTTPResponse.
    # Calling stream(amt) on it issues individual recv() calls on the socket,
    # returning each chunk immediately as it arrives in the TCP buffer.
    raw_stream = getattr(streaming_body, "_raw_stream", None)
    if raw_stream is None:
        # Fallback: use StreamingBody.read() in a loop
        raw_stream = streaming_body

    raw_buf = b""
    try:
        # Use iter_content on the underlying response if available
        if hasattr(raw_stream, "stream"):
            iterator = raw_stream.stream(4096)
        else:
            def _chunked_read():
                while True:
                    chunk = raw_stream.read(4096)
                    if not chunk:
                        break
                    yield chunk
            iterator = _chunked_read()

        for chunk in iterator:
            if not chunk:
                continue
            raw_buf += chunk
            log.debug("AgentCore recv: %d bytes (total %d)", len(chunk), len(raw_buf))

            text = raw_buf.decode("utf-8", errors="replace")
            events = [e.strip() for e in text.replace("\r\n", "\n").split("\n\n") if e.strip()]
            for event in events:
                for line in event.splitlines():
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    data_str = line[5:].strip()
                    if not data_str or data_str == "[DONE]":
                        continue
                    if '"answer"' in data_str or '"sdp"' in data_str:
                        log.info("AgentCore: answer received (%d bytes)", len(data_str))
                        # Agent yields {"answer": {"sdp":..., "type":..., "pc_id":...}}
                        # Unwrap the "answer" wrapper if present
                        try:
                            parsed = json.loads(data_str)
                            if "answer" in parsed:
                                return json.dumps(parsed["answer"]).encode()
                        except Exception:
                            pass
                        return data_str.encode()
                    if '"error"' in data_str or '"status": "error"' in data_str:
                        log.warning("AgentCore error event: %s", data_str[:200])
                        return data_str.encode()
                    log.info("AgentCore status event (waiting for SDP): %s", data_str[:100])

    except Exception as e:
        log.warning("AgentCore stream error: %s  buf=%r", e, raw_buf[:200])
    finally:
        try:
            streaming_body.close()
        except Exception:
            pass

    log.warning("AgentCore stream ended without SDP answer  total_bytes=%d", len(raw_buf))
    return b""


def _parse_agentcore_response(raw: bytes) -> dict:
    """
    Parse the raw AgentCore response body into a dict.

    AgentCore may return:
      1. Plain JSON:  {"sdp": "...", "type": "answer", "pc_id": "..."}
      2. SSE format:  data: {"sdp": "...", "type": "answer", "pc_id": "..."}\n\n
      3. Empty body:  b""  (cold-start, container not ready)
    """
    if not raw:
        return {}

    text = raw.decode("utf-8", errors="replace").strip()
    if not text:
        return {}

    # Try plain JSON first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try SSE: extract the last non-empty "data: ..." line
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("data:"):
            data_str = line[5:].strip()
            if data_str and data_str != "[DONE]":
                try:
                    return json.loads(data_str)
                except json.JSONDecodeError:
                    pass

    # Nothing parseable — log the full body for diagnosis
    log.error("AgentCore response not parseable as JSON or SSE: %r", raw[:500])
    return {}


async def _invoke_agentcore(session_id: str, payload: dict) -> dict:
    """
    Async wrapper around boto3 invoke_agent_runtime.
    Runs the synchronous boto3 call in a thread executor.

    Does NOT retry — retrying an offer creates a new peer connection and
    pipeline on the agent for every attempt, leaving stale connections that
    accumulate and time out. The urllib3 read timeout is sufficient to wait
    for the SDP answer after ANSWER:START.

    Returns the parsed JSON response body dict (empty on failure).
    """
    loop = asyncio.get_running_loop()
    body = json.dumps(payload).encode()
    raw = await loop.run_in_executor(
        _boto3_executor,
        _invoke_agent_runtime_sync,
        session_id,
        body,
    )
    return _parse_agentcore_response(raw)


def _get_bridge_ice_servers() -> list[RTCIceServer]:
    """
    ICE server configuration for the BRIDGE side of the WebRTC connection.

    The bridge EC2 is in the same AWS region as the agent VPC. They can reach
    each other via AWS internal routing using host/srflx candidates — no TURN
    relay is needed on the bridge side.

    Using KVS TURN on the bridge caused "403 Forbidden IP" errors: the agent's
    TURN allocation cannot channel-bind to the bridge's TURN relay address
    because each side allocates TURN independently with different credentials,
    and TURN servers reject channel-bind requests to peer addresses that aren't
    reachable through that specific allocation.

    STUN lets the bridge discover its public srflx candidate (13.54.x.x).
    The agent will reach the bridge on whichever candidate works — typically
    the public srflx via the agent's TURN relay (which CAN reach public IPs),
    or the private host candidate via VPC internal routing.
    """
    return [RTCIceServer(urls=["stun:stun.l.google.com:19302"])]


# ---------------------------------------------------------------------------
# RTP helpers
# ---------------------------------------------------------------------------

def make_rtp_header(payload_type, seq, timestamp, ssrc, marker=False) -> bytes:
    b0 = (RTP_VERSION << 6) & 0xFF
    b1 = (payload_type & 0x7F) | (0x80 if marker else 0x00)
    return struct.pack("!BBHII", b0, b1, seq & 0xFFFF, timestamp, ssrc)


def parse_rtp(data: bytes):
    if len(data) < 12:
        return None
    b0, b1 = data[0], data[1]
    if (b0 >> 6) != 2:
        return None
    cc           = b0 & 0x0F
    payload_type = b1 & 0x7F
    seq          = struct.unpack_from("!H", data, 2)[0]
    timestamp    = struct.unpack_from("!I", data, 4)[0]
    ssrc         = struct.unpack_from("!I", data, 8)[0]
    hdr_len      = 12 + cc * 4
    if b0 & 0x10:
        if len(data) < hdr_len + 4:
            return None
        ext_words = struct.unpack_from("!H", data, hdr_len + 2)[0]
        hdr_len  += 4 + ext_words * 4
    return payload_type, seq, timestamp, ssrc, data[hdr_len:]


# ---------------------------------------------------------------------------
# SIP helpers  (unchanged from v2)
# ---------------------------------------------------------------------------

def _is_private_ip(ip: str) -> bool:
    try:
        parts = list(map(int, ip.split('.')))
        if len(parts) != 4:
            return False
        a, b = parts[0], parts[1]
        return (
            a == 10 or a == 127 or
            (a == 172 and 16 <= b <= 31) or
            (a == 192 and b == 168) or
            (a == 169 and b == 254)
        )
    except Exception:
        return False


def parse_sip(raw: bytes) -> Optional[dict]:
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return None
    text  = text.replace("\r\n", "\n").replace("\r", "\n")
    blank = text.find("\n\n")
    if blank == -1:
        header_part, body = text, ""
    else:
        header_part, body = text[:blank], text[blank + 2:]
    header_lines = header_part.split("\n")
    if not header_lines:
        return None
    first_line = header_lines[0].strip()
    parts      = first_line.split(" ", 2)
    if len(parts) < 2:
        return None
    method = parts[0]
    headers: dict[str, str] = {}
    via_list: list[str] = []
    record_route_list: list[str] = []
    for line in header_lines[1:]:
        if not line:
            continue
        if line[0] in (" ", "\t") and headers:
            last = list(headers)[-1]
            headers[last] += " " + line.strip()
        elif ":" in line:
            k, _, v = line.partition(":")
            compact = {"v": "via", "f": "from", "t": "to", "i": "call-id", "m": "contact"}
            key = compact.get(k.strip().lower(), k.strip().lower())
            if key == "via":
                via_list.append(v.strip())
                headers["via"] = v.strip()
            elif key == "record-route":
                record_route_list.append(v.strip())
                headers["record-route"] = v.strip()
            else:
                headers[key] = v.strip()
    return {"method": method, "first_line": first_line, "headers": headers,
            "via_list": via_list, "record_route_list": record_route_list, "body": body}


def extract_sdp_rtp(body: str) -> tuple:
    host = port = None
    for line in body.splitlines():
        line = line.strip()
        if line.startswith("c=IN IP4 "):
            host = line.split()[-1]
        if line.startswith("m=audio "):
            try:
                port = int(line.split()[1])
            except (IndexError, ValueError):
                pass
    return host, port


def build_sdp_minimal(local_ip, local_rtp_port, codec_str="0 101", session_version=0) -> str:
    rtpmap_lines = ""
    for pt in codec_str.split():
        pt = pt.strip()
        if pt == "0":
            rtpmap_lines += "a=rtpmap:0 PCMU/8000\r\n"
        elif pt == "8":
            rtpmap_lines += "a=rtpmap:8 PCMA/8000\r\n"
        elif pt == "101":
            rtpmap_lines += "a=rtpmap:101 telephone-event/8000\r\n"
            rtpmap_lines += "a=fmtp:101 0-15\r\n"
    rtcp_port = local_rtp_port + 1
    return (
        "v=0\r\n"
        f"o=pbx-bridge 0 {session_version} IN IP4 {local_ip}\r\n"
        "s=PBX Bridge\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
        f"m=audio {local_rtp_port} RTP/AVP {codec_str}\r\n"
        f"{rtpmap_lines}"
        f"a=rtcp:{rtcp_port}\r\n"
        "a=sendrecv\r\n"
        "a=ptime:20\r\n"
    )


def build_sdp(local_ip, local_rtp_port, session_version=0) -> str:
    rtcp_port = local_rtp_port + 1
    return (
        "v=0\r\n"
        f"o=pbx-bridge 0 {session_version} IN IP4 {local_ip}\r\n"
        "s=PBX Bridge\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
        f"m=audio {local_rtp_port} RTP/AVP 0 8 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:8 PCMA/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=fmtp:101 0-15\r\n"
        f"a=rtcp:{rtcp_port}\r\n"
        "a=sendrecv\r\n"
        "a=ptime:20\r\n"
    )


def _transport_proto(transport) -> str:
    """Return 'TCP' if this is a SipTcpConn, 'UDP' otherwise."""
    return "TCP" if isinstance(transport, SipTcpConn) else "UDP"


def build_sip_response(
    code, reason, req, local_ip,
    to_tag="", local_rtp_port=0, include_sdp=False, session_expires=0,
    transport_proto: str = "UDP",
) -> bytes:
    h        = req["headers"]
    sdp      = build_sdp(local_ip, local_rtp_port) if include_sdp else ""
    ctype    = "Content-Type: application/sdp\r\n" if include_sdp else ""
    to_hdr   = h.get("to", "")
    if to_tag and "tag=" not in to_hdr:
        to_hdr = f"{to_hdr};tag={to_tag}"
    via_lines = "".join(f"Via: {v}\r\n" for v in req.get("via_list", [h.get("via", "")]))
    timer_hdrs = ""
    if session_expires > 0 and code == 200:
        timer_hdrs = (
            f"Session-Expires: {session_expires};refresher=uas\r\n"
            f"Min-SE: {MIN_SE_S}\r\n"
            f"Supported: timer\r\n"
            f"Require: timer\r\n"
        )
    # RFC 3261 §20.10: Contact URI transport param must match the transport
    # used for this dialog so in-dialog requests come back on the same channel.
    contact_transport = f";transport={transport_proto.lower()}" if transport_proto == "TCP" else ""
    resp = (
        f"SIP/2.0 {code} {reason}\r\n"
        f"{via_lines}"
        f"From: {h.get('from', '')}\r\n"
        f"To: {to_hdr}\r\n"
        f"Call-ID: {h.get('call-id', '')}\r\n"
        f"CSeq: {h.get('cseq', '')}\r\n"
        f"Contact: <sip:pbx@{local_ip}:{SIP_PORT}{contact_transport}>\r\n"
        f"{timer_hdrs}"
        f"{ctype}"
        f"Content-Length: {len(sdp.encode())}\r\n"
        f"\r\n"
        f"{sdp}"
    )
    return resp.encode("utf-8")


# ---------------------------------------------------------------------------
# Call session state machine
# ---------------------------------------------------------------------------

class CallState(Enum):
    CONNECTING    = auto()
    ESTABLISHED   = auto()
    TEARING_DOWN  = auto()


@dataclass
class CallSession:
    call_id:         str
    to_tag:          str
    remote_host:     str
    remote_rtp_port: int
    local_rtp_port:  int
    rtp_socket:      socket.socket
    state:           CallState = CallState.CONNECTING

    ssrc:         int = field(default_factory=lambda: random.randint(0, 0xFFFFFFFF))
    seq:          int = field(default_factory=lambda: random.randint(0, 0xFFFF))
    timestamp:    int = field(default_factory=lambda: random.randint(0, 0xFFFFFFFF))
    payload_type: int = RTP_PAYLOAD_PCMU

    cached_200:          Optional[bytes] = None
    sip_transport:       Optional[object] = None
    sip_remote_addr:     Optional[tuple] = None
    sip_from_hdr:        str = ""
    sip_to_hdr:          str = ""
    sip_cseq:            int = 1
    sip_invite_msg:      Optional[dict] = None
    sip_record_route:    list = field(default_factory=list)
    sip_remote_contact:  str = ""
    sip_chime_codecs:    str = "0 101"
    session_expires:     int = SESSION_EXPIRES_S
    sip_refresh_interval: int = 90
    sdp_version:         int = 0

    # WebRTC peer connection (aiortc) and outbound audio track
    pc:               Optional[RTCPeerConnection] = None
    sender_track:     Optional[RawAudioTrack] = None  # pipecat RawAudioTrack; set in _establish_webrtc
    # agentcore session id (used for all /invocations calls)
    agentcore_session_id: str = ""
    # SIP transport protocol for this call: "UDP" or "TCP"
    sip_proto:        str = "UDP"

    # Audio queue: WebRTC → RTP (agent audio → Chime)
    agent_to_rtp_queue: Optional[asyncio.Queue] = None  # encoded G.711 chunks

    _rs_in:  object = None
    _rs_out: object = None

    # Diagnostics (carried from v2)
    call_start_ts:        float = field(default_factory=_time.monotonic)
    webrtc_connect_ts:    Optional[float] = None
    rtvi_handshake_ok:    bool = False
    url_built_ts:         Optional[float] = None

    rtp_rx_count:         int = 0
    rtp_rx_last_ts:       Optional[float] = None
    rtp_tx_count:         int = 0
    rtp_tx_last_ts:       Optional[float] = None
    webrtc_frames_rx:     int = 0    # audio frames received from agent
    webrtc_frames_rx_last_ts: Optional[float] = None
    webrtc_frames_tx:     int = 0    # audio frames sent to agent

    disconnect_reason:    str = "unknown"
    webrtc_close_reason:  str = ""
    disconnect_ts:        Optional[float] = None

    def pcm8k_to_agent(self, pcm: bytes) -> bytes:
        if AGENT_SAMPLE_RATE == CHIME_SAMPLE_RATE:
            return pcm
        out, self._rs_in = audioop.ratecv(pcm, 2, 1, CHIME_SAMPLE_RATE, AGENT_SAMPLE_RATE, self._rs_in)
        return out

    def pcm_agent_to_8k(self, pcm: bytes, src_rate: int) -> bytes:
        if src_rate == CHIME_SAMPLE_RATE:
            return pcm
        out, self._rs_out = audioop.ratecv(pcm, 2, 1, src_rate, CHIME_SAMPLE_RATE, self._rs_out)
        return out

    def next_rtp_header(self, marker=False) -> bytes:
        hdr = make_rtp_header(self.payload_type, self.seq, self.timestamp, self.ssrc, marker)
        self.seq       = (self.seq       + 1)                   & 0xFFFF
        self.timestamp = (self.timestamp + CHIME_FRAME_SAMPLES) & 0xFFFFFFFF
        return hdr


# ---------------------------------------------------------------------------
# Disconnect reporting  (carried from v2)
# ---------------------------------------------------------------------------

def _emit_disconnect_report(session: CallSession, extra: str = ""):
    now      = _time.monotonic()
    call_dur = now - session.call_start_ts
    url_age  = (now - session.url_built_ts) if session.url_built_ts else None
    url_expiry_warn = url_age is not None and url_age > 240   # warn if >4 min
    rtp_rx_ago = (now - session.rtp_rx_last_ts) if session.rtp_rx_last_ts else None
    wx_rx_ago  = (now - session.webrtc_frames_rx_last_ts) if session.webrtc_frames_rx_last_ts else None
    wrtc_dur   = (now - session.webrtc_connect_ts) if session.webrtc_connect_ts else None

    lines = [
        "=" * 72,
        f"DISCONNECT REPORT  call-id={session.call_id}",
        f"  reason           : {session.disconnect_reason}",
        f"  call duration    : {call_dur:.1f}s",
        f"  WebRTC connected : {wrtc_dur:.1f}s" if wrtc_dur else "  WebRTC connected : never",
        f"  RTVI handshake   : {'[ok]' if session.rtvi_handshake_ok else '[x] NOT completed'}",
        "",
        "  RTP inbound  (Chime -> bridge):",
        f"    packets rx     : {session.rtp_rx_count}",
        f"    last pkt ago   : {rtp_rx_ago:.1f}s" if rtp_rx_ago else "    last pkt ago   : never",
        "",
        "  RTP outbound (bridge -> Chime):",
        f"    packets tx     : {session.rtp_tx_count}",
        "",
        "  WebRTC audio frames (bridge <-> agent):",
        f"    frames rx      : {session.webrtc_frames_rx}",
        f"    frames tx      : {session.webrtc_frames_tx}",
        f"    last rx ago    : {wx_rx_ago:.1f}s" if wx_rx_ago else "    last rx ago    : never",
    ]
    if session.webrtc_close_reason:
        lines += ["", f"  WebRTC close reason : '{session.webrtc_close_reason}'"]
    if extra:
        lines += ["", f"  extra              : {extra}"]
    lines += ["", "  DIAGNOSIS HINTS:"]
    hints = _diagnosis_hints(session, call_dur, rtp_rx_ago, wx_rx_ago, wrtc_dur)
    lines += hints
    lines.append("=" * 72)
    _disc_log.warning("\n".join(lines))


def _diagnosis_hints(session, call_dur, rtp_rx_ago, wx_rx_ago, wrtc_dur) -> list:
    hints = []
    if session.rtp_rx_count == 0:
        hints.append("  [x] Zero RTP packets from Chime — check SG inbound UDP rules")
    if rtp_rx_ago is not None and rtp_rx_ago > RTP_SILENCE_TIMEOUT_S:
        hints.append(f"  [x] No RTP from Chime for {rtp_rx_ago:.0f}s")
    if not session.rtvi_handshake_ok:
        hints.append("  [x] No audio from agent — WebRTC connected but agent sent nothing")
    if session.webrtc_frames_rx == 0:
        hints.append("  [x] Zero WebRTC frames from agent — check AgentCore logs")
    if wx_rx_ago is not None and wx_rx_ago > WS_IDLE_TIMEOUT_S:
        hints.append(f"  [x] No WebRTC frames from agent for {wx_rx_ago:.0f}s")
    if wrtc_dur is not None and wrtc_dur < 5:
        hints.append(f"  [x] WebRTC was only connected {wrtc_dur:.1f}s — check ICE/TURN config")
    if not session.webrtc_connect_ts:
        hints.append("  [x] WebRTC never connected — ICE failed, check KVS TURN credentials and VPC egress")
    if not hints:
        hints.append("  (no specific hint — check AgentCore CloudWatch logs)")
    return hints


# ---------------------------------------------------------------------------
# WebRTC audio track wrappers
# ---------------------------------------------------------------------------

def _make_sender_track(session: "CallSession") -> RawAudioTrack:
    """
    Create the outbound WebRTC audio track for one call.

    Uses pipecat's RawAudioTrack directly — it handles 10ms chunking,
    internal deque, frame timing, and silence generation, all tested
    against the same aiortc version we depend on.

    Audio flow:
      Chime RTP → decode G.711 → upsample 8k→16k → PCM16 bytes
      → add_audio_bytes() → RawAudioTrack.recv() → WebRTC DTLS/SRTP → agent

    RawAudioTrack requires audio in multiples of 10ms.
    One 20ms Chime frame = 320 samples @ 16kHz = 640 bytes — already a
    multiple of the 10ms constraint (320 bytes per 10ms chunk).
    """
    return RawAudioTrack(sample_rate=AGENT_SAMPLE_RATE)



class ChimeAudioReceiverSink:
    """
    Consumes audio frames arriving from the Pipecat/ElevenLabs agent over WebRTC
    and places encoded G.711 chunks on session.agent_to_rtp_queue for transmission
    to Chime.

    NOTE on sample rates: ElevenLabs TTS outputs 16 kHz PCM16 *on the agent side*,
    but once that audio passes through the WebRTC Opus codec, aiortc decodes it
    at Opus's native rate of 48000 Hz. The bridge reads frame.sample_rate on every
    frame (not the AGENT_SAMPLE_RATE constant) and resamples dynamically, so it
    handles 48000→8000 correctly. Do NOT assume the rate is 16 kHz here.

    ElevenLabs TTS voice output is mono. aiortc delivers Opus frames as 'fltp'
    (float32 planar). PyAV planes are alignment-padded — always slice to
    [:n_samples] before processing or you mix garbage into the audio.
    """

    def __init__(self, session: CallSession, stop: asyncio.Event):
        self._session = session
        self._stop    = stop

    async def run(self, track):  # track: aiortc MediaStreamTrack (RemoteStreamTrack)
        session = self._session
        log.info("WebRTC->RTP sink started  call-id=%.36s", session.call_id)
        resample_state = None
        last_logged_sr: Optional[int] = None
        import queue as _q_mod
        # G.711 accumulation buffer.
        # MP3 (mp3_44100_128) frames are 1152 samples each. After ratecv
        # 44100->8000 that gives ~208 G.711 bytes, which does not divide
        # evenly into 160-byte (20ms) Chime frames. The old code zero-padded
        # the 48-byte remainder to 160 bytes, injecting ~14ms of silence every
        # 26ms -> periodic crackling and distorted voice.
        # Fix: carry leftover bytes forward and only emit complete 160-byte frames.
        g711_buf = b""

        try:
            while not self._stop.is_set():
                try:
                    frame = await asyncio.wait_for(track.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log.warning("WebRTC audio track recv error: %s  call-id=%.36s",
                                e, session.call_id)
                    # Agent closed the WebRTC connection — mark it so _run_call
                    # knows to send a SIP BYE to Chime.
                    if session.disconnect_reason == "unknown":
                        session.disconnect_reason = "agent closed WebRTC"
                    break

                session.webrtc_frames_rx       += 1
                session.webrtc_frames_rx_last_ts = _time.monotonic()

                if not session.rtvi_handshake_ok:
                    session.rtvi_handshake_ok = True
                    session.webrtc_connect_ts = _time.monotonic()

                # Convert av.AudioFrame -> raw mono s16le PCM bytes.
                #
                # aiortc delivers frames as 'fltp' (32-bit float, planar).
                # Each plane is one channel of float32 samples.
                #
                # We avoid frame.reformat() entirely — it was added in PyAV 12
                # and is absent on older installs (AttributeError on Python 3.14
                # EC2 environments with system PyAV).
                #
                # Instead we manually mix down to mono and convert float32->s16:
                #   1. Average all channel planes (handles mono and stereo)
                #   2. Clamp to [-1, 1] and scale to int16 range
                #   3. Pack as little-endian s16 (what audioop expects)
                #
                # This is equivalent to reformat(format='s16p', layout='mono')
                # but works on any PyAV version.
                # Determine effective sample rate for this frame.
                # FORCE_AGENT_RX_RATE overrides the rate reported by aiortc,
                # useful when the frame header rate doesn't match the actual
                # PCM content (e.g. ElevenLabs sends 24 kHz audio but the
                # Opus container reports 48 kHz).
                # Leave FORCE_AGENT_RX_RATE=0 (default) for auto-detection.
                frame_sr = frame.sample_rate or AGENT_SAMPLE_RATE
                sr = FORCE_AGENT_RX_RATE if FORCE_AGENT_RX_RATE > 0 else frame_sr
                fmt = frame.format.name
                if fmt in ('fltp', 'flt', 's16p', 's16', 's32p', 's32',
                           'u8p', 'u8', 'dblp', 'dbl'):
                    import struct as _struct
                    n_planes = len(frame.planes)
                    n_samples = frame.samples
                    if fmt in ('fltp', 'flt'):
                        # float32 planar or interleaved — average planes for mono.
                        #
                        # BUG FIX: PyAV planes are alignment-padded, so
                        # len(bytes(frame.planes[p])) >= n_samples * 4.
                        # Slicing to [:n_samples] is critical — extra garbage
                        # floats beyond the real samples cause noise and
                        # pitch distortion (the single biggest audio quality bug).
                        import array as _array
                        plane_data = [
                            _array.array('f', bytes(frame.planes[p]))[:n_samples]
                            for p in range(n_planes)
                        ]
                        # mix to mono by averaging channels
                        if n_planes == 1:
                            # mono — avoid the averaging loop overhead
                            mixed = plane_data[0]
                        else:
                            mixed = [
                                sum(plane_data[p][i] for p in range(n_planes)) / n_planes
                                for i in range(n_samples)
                            ]
                        # BUG FIX: scale factor must be 32768.0, not 32767,
                        # so that -1.0 → -32768 (full negative range used).
                        # Using 32767 made negative-peak samples one LSB too quiet,
                        # causing a subtle asymmetric clipping that adds harmonic
                        # distortion audible as a buzzing undertone.
                        pcm16 = _struct.pack(
                            f'<{n_samples}h',
                            *[max(-32768, min(32767, int(s * 32768.0))) for s in mixed]
                        )
                    elif fmt in ('s16p', 's16'):
                        # THE ROOT CAUSE OF THE PITCH DISTORTION:
                        #
                        # aiortc delivers Opus audio as fmt='s16', layout='stereo',
                        # n_planes=1 (single interleaved plane), samples=960.
                        # The plane holds: 960 samples * 2 channels * 2 bytes = 3840 bytes
                        # BUT PyAV pads the plane buffer to an alignment boundary,
                        # so plane0_bytes=23040 (6x the real data).
                        #
                        # n_planes=1 for interleaved s16 — it does NOT mean mono.
                        # The number of audio channels must come from frame.layout.channels
                        # (or by counting layout channel descriptors), NOT from n_planes.
                        #
                        # Old buggy code did [:n_samples * 2] = [:1920] on a stereo
                        # interleaved frame, cutting halfway through the stereo pairs,
                        # then passed those 1920 bytes straight to audioop as if they
                        # were 960 mono samples. Real content: only 480 stereo pairs
                        # = half the audio at the declared rate = pitch halved =
                        # woman sounding like a deep man.
                        #
                        # Fix: derive n_channels from the layout, compute the exact
                        # byte slice for real audio data, then properly mix to mono
                        # by averaging left+right before passing to audioop.

                        # Number of channels from the frame layout (1=mono, 2=stereo, etc.)
                        try:
                            n_channels = len(frame.layout.channels)
                        except Exception:
                            n_channels = n_planes if n_planes > 1 else 1

                        if fmt == 's16':
                            # Interleaved: one plane, samples are L,R,L,R,...
                            # Exact real data = n_samples * n_channels * 2 bytes.
                            # Slice off PyAV padding before unpacking.
                            real_bytes = n_samples * n_channels * 2
                            raw = bytes(frame.planes[0])[:real_bytes]
                            total_shorts = n_samples * n_channels
                            shorts = _struct.unpack_from(f'<{total_shorts}h', raw)
                            if n_channels == 1:
                                pcm16 = raw  # already mono, no work needed
                            else:
                                # Average all channels to mono
                                pcm16 = _struct.pack(
                                    f'<{n_samples}h',
                                    *[
                                        max(-32768, min(32767,
                                            sum(shorts[i * n_channels + ch]
                                                for ch in range(n_channels)) // n_channels))
                                        for i in range(n_samples)
                                    ]
                                )
                        else:  # s16p — planar: each plane is one channel
                            # Slice each plane to n_samples * 2 bytes (strip padding)
                            planes_s16 = [
                                _struct.unpack_from(f'<{n_samples}h',
                                                    bytes(frame.planes[p])[:n_samples * 2])
                                for p in range(n_planes)
                            ]
                            if n_planes == 1:
                                pcm16 = bytes(frame.planes[0])[:n_samples * 2]
                            else:
                                pcm16 = _struct.pack(
                                    f'<{n_samples}h',
                                    *[
                                        max(-32768, min(32767,
                                            sum(planes_s16[p][i] for p in range(n_planes)) // n_planes))
                                        for i in range(n_samples)
                                    ]
                                )
                    else:
                        # s32p / dblp / u8p — convert via float
                        import array as _array
                        if fmt in ('s32p', 's32'):
                            raw = bytes(frame.planes[0])
                            ints = _array.array('i', raw)
                            mixed = [s / 2147483648.0 for s in ints[:n_samples]]
                        elif fmt in ('dblp', 'dbl'):
                            raw = bytes(frame.planes[0])
                            mixed = list(_struct.unpack_from(f'<{n_samples}d', raw))
                        else:  # u8p / u8
                            raw = bytes(frame.planes[0])
                            mixed = [(b - 128) / 128.0 for b in raw[:n_samples]]
                        pcm16 = _struct.pack(
                            f'<{n_samples}h',
                            *[max(-32768, min(32767, int(s * 32768.0))) for s in mixed]
                        )
                else:
                    # Unknown format — try planes[0] raw and hope for the best
                    log.warning("WebRTC->RTP: unknown frame format %s  call-id=%.36s",
                                fmt, session.call_id)
                    pcm16 = bytes(frame.planes[0])

                if sr != last_logged_sr:
                    if FORCE_AGENT_RX_RATE > 0 and frame_sr != FORCE_AGENT_RX_RATE:
                        log.debug("WebRTC->RTP: frame.sample_rate=%dHz overridden to %dHz  call-id=%.36s",
                                  frame_sr, sr, session.call_id)
                    last_logged_sr = sr
                    resample_state = None  # reset resampler on rate change
                    g711_buf = b""        # flush accumulator on rate change

                # Downsample agent rate (16 kHz) → 8 kHz for Chime G.711
                if sr != CHIME_SAMPLE_RATE:
                    pcm8, resample_state = audioop.ratecv(
                        pcm16, 2, 1, sr, CHIME_SAMPLE_RATE, resample_state
                    )
                else:
                    pcm8 = pcm16

                # Encode to the codec Chime negotiated (u-law or a-law)
                if session.payload_type == RTP_PAYLOAD_PCMA:
                    encoded = audioop.lin2alaw(pcm8, 2)
                else:
                    encoded = audioop.lin2ulaw(pcm8, 2)

                # Accumulate encoded bytes and emit only complete 160-byte
                # (20ms) Chime frames. Never pad partial frames with silence
                # -- carry the remainder into the next WebRTC frame instead.
                # This handles MP3 (1152 samples -> ~208 G.711 bytes) and any
                # other upstream codec whose frame size is not a multiple of 20ms.
                q = getattr(session, '_rtp_out_queue_sync', None)
                if q is None:
                    continue  # output loop not yet started
                g711_buf += encoded
                while len(g711_buf) >= CHIME_FRAME_SAMPLES:
                    chunk = g711_buf[:CHIME_FRAME_SAMPLES]
                    g711_buf = g711_buf[CHIME_FRAME_SAMPLES:]
                    try:
                        q.put_nowait(chunk)
                    except _q_mod.Full:
                        pass  # drop if backed up

        except Exception as e:
            log.error("WebRTC->RTP sink error: %s  call-id=%.36s", e, session.call_id)
        finally:
            self._stop.set()
            log.info("WebRTC->RTP sink ended  call-id=%.36s  frames_rx=%d",
                     session.call_id, session.webrtc_frames_rx)


# ---------------------------------------------------------------------------
# AgentCore WebRTC signaling
# ---------------------------------------------------------------------------

async def _establish_webrtc(session: CallSession, bridge: "PbxBridge" = None) -> bool:
    """
    Perform the WebRTC signaling dance with the Pipecat agent on AgentCore.

    Pipecat's SmallWebRTCTransport @app.entrypoint uses this protocol:
      POST  { "sdp": "<offer>", "type": "offer" }
            → returns { "sdp": "<answer>", "type": "answer", "pc_id": "..." }
      PATCH { "pc_id": "...", "candidates": [{ "candidate": "...",
              "sdp_mid": "...", "sdp_mline_index": 0 }] }
            → returns 200/204

    There is NO "action" field — that is the Nova Sonic sample agent, not Pipecat.
    ICE config / KVS TURN credentials are fetched directly from KVS, not via AgentCore.

    Returns True if WebRTC connected successfully, False on error.
    """
    call_id = session.call_id

    # --- Step 1: Get ICE servers for the bridge side (STUN only — no TURN needed)
    rtc_ice_servers = _get_bridge_ice_servers()
    log.info("WebRTC: bridge ICE config: %d server(s)  call-id=%.36s",
             len(rtc_ice_servers), call_id)

    # --- Step 2: Create peer connection with our audio sender track -----------
    config = RTCConfiguration(iceServers=rtc_ice_servers)
    pc     = RTCPeerConnection(configuration=config)
    session.pc = pc

    # Use pipecat's RawAudioTrack — handles chunking, queue, timing, silence.
    # Store on session so _rtp_to_webrtc can push audio via add_audio_bytes().
    sender_track = _make_sender_track(session)
    session.sender_track = sender_track
    pc.addTrack(sender_track)

    # Create a data channel so Pipecat's SmallWebRTCTransport sees one in the
    # offer and opens its own. Without this, the agent logs:
    #   "Data channel not established within 10s after connection"
    # on every call, and RTVI bot-ready / transcript events are silently dropped.
    #
    # CRITICAL: The bridge must send the RTVI "client-ready" message over this
    # channel as soon as it opens. Pipecat's on_client_ready handler (which
    # queues the opening LLMRunFrame and starts the conversation) only fires
    # when it receives this message. Without it, the agent sits silent until
    # the caller speaks and VAD eventually triggers — a ~55 second delay.
    #
    # RTVI message format (https://docs.pipecat.ai/client/rtvi-standard):
    #   { "label": "rtvi-ai", "type": "client-ready",
    #     "id": "<uuid>", "data": { "version": "1.0" } }
    dc = pc.createDataChannel("chat")

    @dc.on("open")
    def _on_dc_open():
        import uuid as _uuid
        import json as _json
        client_ready = _json.dumps({
            "label": "rtvi-ai",
            "type":  "client-ready",
            "id":    str(_uuid.uuid4()),
            "data":  {
                "version": "1.0",
                "about": {
                    "library":         "pbx-bridge",
                    "library_version": "3.0",
                    "platform":        "sip-rtp",
                },
            },
        })
        try:
            dc.send(client_ready)
            log.info("WebRTC: sent RTVI client-ready  call-id=%.36s", call_id)
        except Exception as _e:
            log.warning("WebRTC: failed to send client-ready: %s  call-id=%.36s",
                        _e, call_id)

    # Mutable cell so the dc.on("message") closure can reach the bridge instance
    # even though _establish_webrtc is a module-level function.
    _bridge_ref = [bridge]

    @dc.on("message")
    def _on_dc_message(msg):
        """
        Handle RTVI messages from the Pipecat agent over the data channel.

        When Pipecat decides to end the call it sends one of these RTVI events
        before (or while) closing the WebRTC peer connection:
          - { "label": "rtvi-ai", "type": "bot-stopped" }
          - { "label": "rtvi-ai", "type": "bot-disconnected" }

        As soon as we see one of those we send a SIP BYE to Chime immediately
        — this cuts the PSTN hangup latency from "wait for WebRTC teardown +
        Chime inactivity timer (~seconds)" down to sub-second.
        """
        import json as _json
        text = msg if isinstance(msg, str) else (msg.decode("utf-8", errors="replace") if isinstance(msg, (bytes, bytearray)) else repr(msg))
        log.debug("WebRTC: data channel msg: %.120s  call-id=%.36s", text, call_id)
        try:
            m = _json.loads(text)
            label = m.get("label", "")
            mtype = m.get("type", "")
            if label == "rtvi-ai" and mtype in ("bot-stopped", "bot-disconnected"):
                log.info("WebRTC: RTVI %s received — sending SIP BYE immediately  call-id=%.36s",
                         mtype, call_id)
                # Retrieve the live session; it may have already been torn down.
                # We access it from the enclosing _establish_webrtc scope via the
                # session variable that was closed over (session is in scope here).
                if session.state != CallState.TEARING_DOWN:
                    session.disconnect_reason = f"agent {mtype} (RTVI)"
                    session.state = CallState.TEARING_DOWN
                    # Schedule teardown on the event loop (dc callbacks are sync).
                    asyncio.get_event_loop().call_soon_threadsafe(
                        lambda: asyncio.ensure_future(
                            _bridge_ref[0]._teardown_with_bye(session)
                        )
                    )
        except Exception:
            pass

    ice_candidates_to_send: asyncio.Queue = asyncio.Queue()

    @pc.on("icecandidate")
    def on_ice_candidate(candidate):
        if candidate:
            log.debug("WebRTC: local ICE candidate: %s", candidate)
            ice_candidates_to_send.put_nowait(candidate)

    @pc.on("iceconnectionstatechange")
    async def on_ice_state_change():
        state = pc.iceConnectionState
        if state in ("connected", "completed", "failed", "closed"):
            log.info("WebRTC: ICE state=%s  call-id=%.36s", state, call_id)
        else:
            log.debug("WebRTC: ICE state=%s  call-id=%.36s", state, call_id)

    received_track = None
    track_event = asyncio.Event()

    @pc.on("track")
    def on_track(track):
        nonlocal received_track
        log.info("WebRTC: received remote track kind=%s  call-id=%.36s", track.kind, call_id)
        if track.kind == "audio":
            received_track = track
            track_event.set()

    # Build SDP offer and wait for ICE gathering to complete before sending.
    # This is critical: if we send the offer immediately after setLocalDescription,
    # the SDP only contains the host candidate (172.31.7.225 private IP).
    # The agent's TURN relay refuses to channel-bind to RFC1918 addresses (403).
    # We must wait for STUN to discover the public srflx candidate (13.54.x.x)
    # so the agent's TURN relay can reach us via the public internet.
    #
    # BUG FIX: Register the icegatheringstatechange handler BEFORE createOffer/
    # setLocalDescription (which triggers ICE gathering to start). Registering it
    # after setLocalDescription creates a race where gathering can complete before
    # the handler is attached and the event is never set. The old guard
    # (if pc.iceGatheringState == "complete") partially mitigated this but left
    # a window between handler registration and the check.
    gathering_complete = asyncio.Event()

    @pc.on("icegatheringstatechange")
    def on_gathering_state():
        log.debug("WebRTC: ICE gathering state=%s  call-id=%.36s",
                  pc.iceGatheringState, call_id)
        if pc.iceGatheringState == "complete":
            gathering_complete.set()

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)

    # Belt-and-suspenders: in case gathering completed synchronously
    if pc.iceGatheringState == "complete":
        gathering_complete.set()

    try:
        await asyncio.wait_for(gathering_complete.wait(), timeout=8.0)
        log.info("WebRTC: ICE gathering complete  call-id=%.36s", call_id)
    except asyncio.TimeoutError:
        log.warning("WebRTC: ICE gathering timed out after 8s — sending offer with available "
                    "candidates  call-id=%.36s", call_id)

    # Log the candidates we're about to advertise
    sdp = pc.localDescription.sdp
    candidates = [l for l in sdp.splitlines() if l.startswith("a=candidate")]
    log.info("WebRTC: offer SDP has %d candidate(s): %s  call-id=%.36s",
             len(candidates), [c[12:60] for c in candidates], call_id)

    # --- Step 3: POST offer to AgentCore, get SDP answer ----------------------
    log.info("WebRTC: sending SDP offer to AgentCore  call-id=%.36s", call_id)
    try:
        answer_resp = await _invoke_agentcore(session.agentcore_session_id, {
            "type": "offer",
            "data": {
                "sdp":  sdp,
                "type": "offer",
            },
        })
    except Exception as e:
        log.error("WebRTC: offer invocation exception: %s  call-id=%.36s", e, call_id)
        session.webrtc_close_reason = f"offer exception: {e}"
        return False

    if not answer_resp:
        log.error("WebRTC: no parseable response from agent after retries  call-id=%.36s",
                  call_id)
        session.webrtc_close_reason = "no answer from agent (cold-start timeout?)"
        return False

    answer_sdp  = answer_resp.get("sdp", "")
    answer_type = answer_resp.get("type", "answer")
    pc_id       = answer_resp.get("pc_id", "")
    if not answer_sdp:
        log.error("WebRTC: no SDP in answer response: %s  call-id=%.36s", answer_resp, call_id)
        session.webrtc_close_reason = "empty SDP answer from agent"
        return False

    await pc.setRemoteDescription(RTCSessionDescription(sdp=answer_sdp, type=answer_type))
    log.info("WebRTC: SDP answer applied  pc_id=%s  call-id=%.36s", pc_id, call_id)

    # --- Step 4: Trickle ICE candidates to AgentCore --------------------------
    async def _send_ice_candidates():
        pending = []
        while True:
            try:
                cand = await asyncio.wait_for(ice_candidates_to_send.get(), timeout=0.1)
                pending.append(cand)
                while not ice_candidates_to_send.empty():
                    pending.append(ice_candidates_to_send.get_nowait())
            except asyncio.TimeoutError:
                pass

            if pending and pc_id:
                candidates_payload = [
                    {
                        "candidate":       c.to_sdp(),
                        "sdp_mid":         c.sdpMid or "0",
                        "sdp_mline_index": c.sdpMLineIndex or 0,
                    }
                    for c in pending
                ]
                pending = []
                try:
                    await _invoke_agentcore(session.agentcore_session_id, {
                        "type": "ice-candidates",
                        "data": {
                            "pc_id":      pc_id,
                            "candidates": candidates_payload,
                        },
                    })
                    log.debug("WebRTC: sent %d ICE candidate(s)  call-id=%.36s",
                              len(candidates_payload), call_id)
                except Exception as e:
                    log.debug("WebRTC: ICE candidate send failed: %s", e)

            if pc.iceConnectionState in ("connected", "completed", "failed", "closed"):
                break

    # Start sending ICE candidates and wait for connection
    ice_task = asyncio.create_task(_send_ice_candidates())

    try:
        deadline = asyncio.get_event_loop().time() + ICE_TIMEOUT_S
        while True:
            state = pc.iceConnectionState
            if state in ("connected", "completed"):
                log.info("WebRTC: ICE connected!  call-id=%.36s", call_id)
                break
            if state in ("failed", "closed"):
                log.error("WebRTC: ICE failed (state=%s)  call-id=%.36s", state, call_id)
                session.webrtc_close_reason = f"ICE {state}"
                return False
            if asyncio.get_event_loop().time() > deadline:
                log.error("WebRTC: ICE timeout after %ds  call-id=%.36s", ICE_TIMEOUT_S, call_id)
                session.webrtc_close_reason = "ICE timeout"
                return False
            await asyncio.sleep(0.2)
    finally:
        ice_task.cancel()

    # Wait for the remote audio track
    try:
        await asyncio.wait_for(track_event.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        log.warning("WebRTC: no remote track received within 5s — continuing  call-id=%.36s",
                    call_id)

    session.url_built_ts = _time.monotonic()
    return True


# ---------------------------------------------------------------------------
# Core bridge per-call lifecycle
# ---------------------------------------------------------------------------

class PbxBridge:
    def __init__(self):
        self.sessions: dict[str, CallSession] = {}
        self._local_ip = self._detect_local_ip()
        log.info("Local IP advertised in SDP: %s", self._local_ip)

    def _detect_local_ip(self) -> str:
        env = os.getenv("LOCAL_IP")
        if env:
            if _is_private_ip(env):
                log.info("LOCAL_IP override: %s (private — correct for EC2/NAT)", env)
            else:
                log.warning("LOCAL_IP=%s looks like a public IP; on EC2 use the private IP", env)
            return env
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    def _alloc_rtp_port(self) -> tuple:
        for _ in range(300):
            port = random.randint(RTP_PORT_MIN, RTP_PORT_MAX - 1) & ~1
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(("0.0.0.0", port))
                rtcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                rtcp_sock.bind(("0.0.0.0", port + 1))
                rtcp_sock.close()
                sock.setblocking(False)
                return port, sock
            except OSError:
                try:
                    sock.close()
                except Exception:
                    pass
        raise RuntimeError("No free RTP port pair in range %d-%d" % (RTP_PORT_MIN, RTP_PORT_MAX))

    # -- SIP dispatch ----------------------------------------------------------

    async def handle_sip(self, transport, data: bytes, addr: tuple):
        first_line_raw = data.decode("utf-8", errors="replace").split("\n")[0].strip()
        proto = _transport_proto(transport)
        log.info("SIP RAW [%s] from %s:%d  >> %s", proto, addr[0], addr[1], first_line_raw[:120])
        log.debug("SIP FULL [%s] from %s:%d:\n%s", proto, addr[0], addr[1],
                  data.decode("utf-8", errors="replace")[:800])

        if data.lstrip()[:7] == b"SIP/2.0":
            first_line  = first_line_raw
            status_code = 0
            try:
                status_code = int(first_line.split()[1])
            except (IndexError, ValueError):
                pass

            if 100 <= status_code <= 199:
                return

            msg_parsed = parse_sip(data)
            cid   = msg_parsed["headers"].get("call-id", "") if msg_parsed else ""
            cseq  = msg_parsed["headers"].get("cseq", "")   if msg_parsed else ""

            if status_code == 200 and msg_parsed and "INVITE" in cseq:
                session = self.sessions.get(cid)
                if session:
                    await self._ack_reinvite_200(transport, msg_parsed, session, addr, cid, cseq)
                else:
                    log.info("SIP 200 OK (no active session)  call-id=%s", cid)
            elif status_code == 200 and msg_parsed and "BYE" in cseq:
                # Chime acknowledged our BYE — the PSTN leg is now terminated.
                # _retransmit_bye will stop because the session is being torn down.
                log.info("SIP 200 OK for BYE [%s]  call-id=%s — Chime confirmed hangup",
                         proto, cid)
                session = self.sessions.get(cid)
                if session and session.state != CallState.TEARING_DOWN:
                    session.state = CallState.TEARING_DOWN
                    asyncio.create_task(self._teardown(cid, send_bye=False))
            elif status_code == 481:
                log.warning("SIP 481 [%s] from %s:%d  call-id=%s — tearing down",
                            proto, addr[0], addr[1], cid)
                session = self.sessions.get(cid)
                if session and session.state != CallState.TEARING_DOWN:
                    session.disconnect_reason = "SIP 481 — Chime session ended"
                    session.state = CallState.TEARING_DOWN
                    asyncio.create_task(self._teardown(cid, send_bye=False))
            else:
                log.warning("SIP response %d [%s]  call-id=%s", status_code, proto, cid)
            return

        msg = parse_sip(data)
        if not msg:
            log.warning("Unparseable SIP [%s] from %s:%d", proto, addr[0], addr[1])
            return

        method  = msg["method"]
        headers = msg["headers"]
        call_id = headers.get("call-id", "")
        resp_addr = addr

        log.info("SIP %-8s [%s]  call-id=%.36s from %s:%d",
                 method, proto, call_id, addr[0], addr[1])

        # If this arrives over TCP, bind the call-id to this connection now so
        # all subsequent in-dialog responses (re-INVITE, BYE) go back on it.
        if isinstance(transport, SipTcpConn) and call_id:
            existing = SipTcpConn.lookup(call_id)
            if existing is None:
                SipTcpConn.register(call_id, transport)

        if method == "INVITE":
            await self._handle_invite(transport, msg, call_id, addr, resp_addr)
        elif method == "ACK":
            session = self.sessions.get(call_id)
            if session and not getattr(session, "ack_received", False):
                session.ack_received = True
                log.info("ACK received [%s]  call-id=%.36s — SIP dialog complete",
                         proto, call_id)
        elif method in ("BYE", "CANCEL"):
            log.info("SIP %s [%s] — tearing down  call-id=%.36s", method, proto, call_id)
            transport.sendto(
                build_sip_response(200, "OK", msg, self._local_ip,
                                   transport_proto=proto),
                resp_addr,
            )
            session = self.sessions.get(call_id)
            if session:
                session.disconnect_reason = f"SIP {method} from Chime"
            await self._teardown(call_id)
            SipTcpConn.unregister(call_id)
        elif method == "OPTIONS":
            transport.sendto(
                build_sip_response(200, "OK", msg, self._local_ip,
                                   transport_proto=proto),
                resp_addr,
            )
        else:
            log.warning("Unhandled SIP method=%s [%s]  call-id=%.36s", method, proto, call_id)

    async def _ack_reinvite_200(self, transport, msg_parsed, session, addr, cid, cseq):
        """Send ACK for a 200 OK response to our outbound re-INVITE.
        Uses the correct Via transport tag to match the session's transport."""
        proto = session.sip_proto  # TCP or UDP, set when session was created
        try:
            branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
            to_hdr = msg_parsed["headers"].get("to", "")
            routes = list(reversed(session.sip_record_route))
            route_hdrs = "".join(f"Route: {r}\r\n" for r in routes)
            contact_hdr = msg_parsed["headers"].get("contact", "")
            if "<" in contact_hdr:
                req_uri = contact_hdr[contact_hdr.index("<")+1:contact_hdr.index(">")]
            else:
                req_uri = f"sip:{session.remote_host}"
            if routes:
                first_route = routes[0]
                dest_uri = first_route[first_route.index("<")+1:first_route.index(">")] if "<" in first_route else first_route.split(";")[0]
                dest_part = dest_uri.replace("sip:", "").split(";")[0]
                if ":" in dest_part:
                    dhost, dport_s = dest_part.rsplit(":", 1)
                    dport = int(dport_s) if dport_s.isdigit() else 5060
                else:
                    dhost, dport = dest_part, 5060
                ack_addr = (dhost, dport)
            else:
                ack_addr = addr
            ack = (
                f"ACK {req_uri} SIP/2.0\r\n"
                f"Via: SIP/2.0/{proto} {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                f"From: {msg_parsed['headers'].get('from', '')}\r\n"
                f"To: {to_hdr}\r\n"
                f"Call-ID: {cid}\r\n"
                f"CSeq: {cseq.split()[0]} ACK\r\n"
                f"Max-Forwards: 70\r\n"
                f"{route_hdrs}"
                f"Content-Length: 0\r\n\r\n"
            )
            session.sip_transport.sendto(ack.encode(), ack_addr)
            log.info("ACK sent [%s] for re-INVITE 200 OK  call-id=%.36s  dest=%s:%d",
                     proto, cid, ack_addr[0], ack_addr[1])
        except Exception as e:
            log.warning("Failed to ACK re-INVITE 200 OK: %s", e)

    async def _handle_invite(self, transport, msg, call_id, addr, resp_addr):
        proto = _transport_proto(transport)
        existing = self.sessions.get(call_id)
        if existing:
            if existing.state == CallState.ESTABLISHED and existing.cached_200:
                transport.sendto(existing.cached_200, resp_addr)
            else:
                early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                           to_tag=existing.to_tag,
                                           local_rtp_port=existing.local_rtp_port,
                                           include_sdp=True,
                                           transport_proto=existing.sip_proto)
                transport.sendto(early, resp_addr)
            return

        transport.sendto(
            build_sip_response(100, "Trying", msg, self._local_ip, transport_proto=proto),
            resp_addr,
        )

        to_tag = "%08x" % random.randint(0, 0xFFFFFFFF)
        try:
            local_rtp_port, rtp_sock = self._alloc_rtp_port()
        except RuntimeError as e:
            log.error("%s", e)
            transport.sendto(
                build_sip_response(503, "Service Unavailable", msg, self._local_ip,
                                   transport_proto=proto), resp_addr)
            return

        early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                   to_tag=to_tag, local_rtp_port=local_rtp_port,
                                   include_sdp=True, transport_proto=proto)
        transport.sendto(early, resp_addr)
        log.info("Sent 183 [%s]  call-id=%.36s  rtp_port=%d", proto, call_id, local_rtp_port)

        chime_only = os.getenv("CHIME_ONLY", "1") == "1"
        if chime_only and not msg["body"].strip():
            log.warning("Rejecting INVITE with empty SDP  call-id=%.36s", call_id)
            transport.sendto(
                build_sip_response(488, "Not Acceptable Here", msg, self._local_ip,
                                   transport_proto=proto), resp_addr)
            return

        remote_host, remote_rtp_port = extract_sdp_rtp(msg["body"])
        if not remote_host:
            remote_host = addr[0]
        if not remote_rtp_port:
            remote_rtp_port = 5004

        if chime_only and _is_private_ip(remote_host):
            # BUG FIX: Chime Voice Connector often sends RTP from private VPC
            # addresses (e.g. 10.x.x.x). Rejecting private RTP hosts with 488
            # silently dropped valid calls. Changed: log a warning but allow the
            # call through. Set CHIME_ONLY=0 to suppress the warning entirely.
            log.warning("CHIME_ONLY: RTP host %s is private — allowing call through  call-id=%.36s",
                        remote_host, call_id)

        # Build session
        session = CallSession(
            call_id         = call_id,
            to_tag          = to_tag,
            remote_host     = remote_host,
            remote_rtp_port = remote_rtp_port,
            local_rtp_port  = local_rtp_port,
            rtp_socket      = rtp_sock,
        )
        session.sip_transport      = transport
        session.sip_remote_addr    = resp_addr
        session.sip_from_hdr       = msg["headers"].get("to", "")
        session.sip_to_hdr         = msg["headers"].get("from", "")
        session.sip_cseq           = 1
        session.sip_invite_msg     = msg
        session.sip_record_route   = msg.get("record_route_list", [])
        session.sip_remote_contact = msg["headers"].get("contact", f"sip:{remote_host}")
        session.sip_proto          = proto   # "TCP" or "UDP" — used in all outbound requests
        session.agent_to_rtp_queue = asyncio.Queue(maxsize=400)

        # Use call-id as the AgentCore session id (stable per call).
        # BUG FIX 1: SIP Call-IDs can contain '@' and other chars that AgentCore
        # rejects in runtimeSessionId (which allows only [a-zA-Z0-9_-]).
        # Strip anything outside that set and truncate to 128 chars.
        # BUG FIX 2: AgentCore requires runtimeSessionId to be at least 33 chars.
        # Some SIP stacks (e.g. FreeSWITCH, certain ATAs) generate purely numeric
        # call-IDs as short as 20-24 digits. After sanitization these remain too
        # short and AgentCore rejects the invocation with:
        #   "Invalid length for parameter runtimeSessionId, value: N, valid min length: 33"
        # Fix: if the sanitized ID is shorter than 33 chars, append a dash and a
        # zero-padded suffix derived from a random int so it reaches exactly 33.
        import re as _re
        safe_session_id = _re.sub(r"[^a-zA-Z0-9_\-]", "-", call_id)[:128]
        _AGENTCORE_MIN_SESSION_ID_LEN = 33
        if len(safe_session_id) < _AGENTCORE_MIN_SESSION_ID_LEN:
            pad_len = _AGENTCORE_MIN_SESSION_ID_LEN - len(safe_session_id) - 1  # -1 for dash
            safe_session_id = safe_session_id + "-" + ("%0*d" % (pad_len, random.randint(0, 10**pad_len - 1)))
        session.agentcore_session_id = safe_session_id

        chime_codecs = "0 101"
        for line in msg["body"].splitlines():
            if line.startswith("m=audio"):
                parts = line.split()
                if len(parts) > 3:
                    chime_codecs = " ".join(parts[3:])
                break
        session.sip_chime_codecs = chime_codecs

        # Lock the outbound payload type from SDP negotiation.
        # Chime US always uses PCMU (pt=0 / μ-law). We read it from the SDP
        # m=audio line so we never rely on the default and never need inbound
        # RTP packets to tell us what codec to use on the outbound path.
        # Priority: PCMU(0) first, then PCMA(8), then default to PCMU.
        negotiated_pts = [int(p) for p in chime_codecs.split() if p.isdigit()]
        if RTP_PAYLOAD_PCMU in negotiated_pts:
            session.payload_type = RTP_PAYLOAD_PCMU
        elif RTP_PAYLOAD_PCMA in negotiated_pts:
            session.payload_type = RTP_PAYLOAD_PCMA
        else:
            session.payload_type = RTP_PAYLOAD_PCMU  # safe default for US
        log.info("SDP codec negotiation: m=audio codecs=%s  outbound payload_type=%d (%s)  call-id=%.36s",
                 chime_codecs, session.payload_type,
                 "PCMU/μ-law" if session.payload_type == RTP_PAYLOAD_PCMU else "PCMA/A-law",
                 call_id)

        # Session timer negotiation
        se_hdr    = msg["headers"].get("session-expires", "")
        minse_hdr = msg["headers"].get("min-se", "")
        try:
            chime_se = int(se_hdr.split(";")[0].strip()) if se_hdr else 0
        except ValueError:
            chime_se = 0
        try:
            chime_minse = int(minse_hdr.strip()) if minse_hdr else MIN_SE_S
        except ValueError:
            chime_minse = MIN_SE_S
        # Negotiation: honour Chime's Min-SE floor but cap at SESSION_EXPIRES_S.
        # SESSION_EXPIRES_S is intentionally low (120s) to send re-INVITEs before
        # Chime's ~150s media inactivity timer kills the inbound RTP stream.
        # We take max(min-SE floors, our cap) rather than max(everything), so a
        # Chime header like "Session-Expires: 1800" cannot push us past our cap.
        se_floor      = max(chime_minse, MIN_SE_S, 90)  # must respect min-SE
        negotiated_se = max(se_floor, SESSION_EXPIRES_S)  # our cap is the ceiling
        session.session_expires     = negotiated_se
        # Refresh at SE/2 - 10s, but never below MIN_SE_S and never above 50s
        # (50s keeps us safely under Chime's 150s inactivity kill regardless of SE).
        session.sip_refresh_interval = min(
            max(MIN_SE_S, negotiated_se // 2 - 10),
            SIP_SESSION_REFRESH_S,
        )

        self.sessions[call_id] = session

        ok_bytes = build_sip_response(
            200, "OK", msg, self._local_ip,
            to_tag=to_tag, local_rtp_port=local_rtp_port,
            include_sdp=True, session_expires=negotiated_se,
            transport_proto=proto,
        )
        session.cached_200 = ok_bytes
        session.state = CallState.ESTABLISHED
        transport.sendto(ok_bytes, resp_addr)
        log.info("Sent 200 OK [%s]  call-id=%.36s  SDP: %s:%d <-> %s:%d",
                 proto, call_id, self._local_ip, local_rtp_port, remote_host, remote_rtp_port)

        asyncio.create_task(self._run_call(session, transport, msg, resp_addr))

    # -- Per-call lifecycle ----------------------------------------------------

    async def _run_call(self, session: CallSession, transport, invite_msg, resp_addr):
        call_id = session.call_id
        stop    = asyncio.Event()

        # BUG FIX: Do NOT start keepalive_task here. Starting it before WebRTC
        # establishment causes it to fire every 20ms during the ~1s KVS/SDP
        # exchange, consuming ~50 RTP sequence numbers with silence. Chime's
        # jitter buffer then sees a large sequence gap when real audio starts
        # and mutes/discards it. keepalive_task is started after WebRTC is up.
        keepalive_task = None
        watchdog_task  = asyncio.create_task(self._call_watchdog(session, stop))
        refresh_task   = asyncio.create_task(self._sip_session_refresh(session, stop))
        rtcp_task      = asyncio.create_task(self._rtcp_keepalive(session, stop))

        try:
            # --- Establish WebRTC connection to AgentCore ----------------------
            log.info("Establishing WebRTC connection to AgentCore  call-id=%.36s", call_id)
            ok = await _establish_webrtc(session, bridge=self)
            if not ok:
                session.disconnect_reason = f"WebRTC establishment failed: {session.webrtc_close_reason}"
                return

            # BUG FIX: Start keepalive AFTER WebRTC is established so it doesn't
            # corrupt the RTP sequence space during the signaling phase.
            keepalive_task = asyncio.create_task(self._rtp_keepalive(session, stop))

            # --- Start media bridges in parallel ------------------------------
            # 1. RTP->WebRTC: read from Chime RTP, feed the sender track queue
            # 2. WebRTC->RTP: pull frames from agent track, write to Chime RTP
            # 3. RTP output thread: drain agent_to_rtp_queue -> UDP packets

            pc = session.pc

            # Find the remote audio track (might have arrived in _establish_webrtc)
            remote_track = None
            for recv in pc.getReceivers():
                if recv.track and recv.track.kind == "audio":
                    remote_track = recv.track
                    break

            sink = ChimeAudioReceiverSink(session, stop)

            tasks = [
                asyncio.create_task(self._rtp_to_webrtc(session, stop)),
                asyncio.create_task(self._rtp_output_loop(session, stop)),
            ]
            if remote_track:
                tasks.append(asyncio.create_task(sink.run(remote_track)))
            else:
                log.warning("No remote audio track yet — will wait  call-id=%.36s", call_id)
                # Poll for the track
                tasks.append(asyncio.create_task(
                    self._wait_for_track_and_run_sink(session, pc, sink, stop)
                ))

            await asyncio.gather(*tasks, return_exceptions=True)
            log.info("All media tasks exited  call-id=%.36s", call_id)

        except Exception as e:
            session.disconnect_reason = f"unexpected: {type(e).__name__}: {e}"
            log.error("Call error  call-id=%.36s : %s", call_id, e, exc_info=True)
        finally:
            stop.set()
            for t in [keepalive_task, watchdog_task, refresh_task, rtcp_task]:
                if t is not None:
                    t.cancel()
            _emit_disconnect_report(session)
            silent_hangup  = "RTP silence" in session.disconnect_reason
            agent_hangup   = "agent" in session.disconnect_reason  # RTVI bot-stopped or WebRTC close
            chime_initiated = any(x in session.disconnect_reason
                                  for x in ("SIP BYE", "SIP CANCEL", "SIP 481", "RTP silence"))
            # Send BYE to Chime whenever WE are the side initiating the teardown
            # (agent hung up, or RTP silence timeout) — but NOT when Chime already
            # sent BYE/CANCEL to us (they know the call is over).
            # Also send BYE if WebRTC closed and the reason is still "unknown"
            # (agent closed without sending an RTVI event first).
            should_bye = (silent_hangup or agent_hangup or
                          (session.disconnect_reason == "unknown" and
                           session.state == CallState.TEARING_DOWN and
                           not chime_initiated))
            if should_bye and session.state != CallState.TEARING_DOWN:
                session.state = CallState.TEARING_DOWN
            bye_reason_str = ("RTP-timeout" if silent_hangup
                              else "agent-hangup" if agent_hangup
                              else "")
            await self._teardown(call_id, send_bye=should_bye,
                                 bye_reason=bye_reason_str)

    async def _wait_for_track_and_run_sink(self, session, pc, sink, stop):
        """Poll until a remote audio track appears then hand off to the sink."""
        deadline = _time.monotonic() + 10
        while not stop.is_set() and _time.monotonic() < deadline:
            for recv in pc.getReceivers():
                if recv.track and recv.track.kind == "audio":
                    await sink.run(recv.track)
                    return
            await asyncio.sleep(0.2)
        if not stop.is_set():
            log.warning("Remote audio track never appeared  call-id=%.36s", session.call_id)

    # -- RTP -> WebRTC ---------------------------------------------------------

    async def _rtp_to_webrtc(self, session: CallSession, stop: asyncio.Event):
        """
        Read RTP packets from Chime, decode G.711, resample 8k→16k, and push
        PCM16 bytes into session.sender_track (pipecat RawAudioTrack) via
        add_audio_bytes().  RawAudioTrack handles its own internal queue,
        frame timing, and silence generation.
        """
        loop  = asyncio.get_running_loop()
        sock  = session.rtp_socket
        queue: asyncio.Queue = asyncio.Queue()

        def _readable():
            try:
                data, _ = sock.recvfrom(4096)
                queue.put_nowait(data)
            except BlockingIOError:
                pass
            except Exception as exc:
                log.warning("RTP socket read error: %s", exc)

        if sock.fileno() == -1:
            stop.set()
            return

        # Clear any stale epoll entry on this fd before registering.
        # If a previous socket with the same fd number was closed without
        # remove_reader being called first, epoll retains a broken entry that
        # silently prevents new add_reader callbacks from firing on the reused fd.
        try:
            loop.remove_reader(sock.fileno())
        except Exception:
            pass
        loop.add_reader(sock.fileno(), _readable)
        log.info("RTP->WebRTC started  fd=%d  call-id=%.36s", sock.fileno(), session.call_id)
        no_rtp_warned = False

        try:
            while not stop.is_set() and session.state != CallState.TEARING_DOWN:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    if session.rtp_rx_count == 0 and not no_rtp_warned:
                        no_rtp_warned = True
                        log.warning(
                            "RTP->WebRTC: NO RTP from Chime yet after 5s  call-id=%.36s"
                            " — check SG UDP %d-%d, SDP IP=%s",
                            session.call_id, RTP_PORT_MIN, RTP_PORT_MAX, session.remote_host)
                    elif session.rtp_rx_count > 0:
                        dur_since = _time.monotonic() - session.rtp_rx_last_ts
                        if dur_since > RTP_SILENCE_HANGUP_S:
                            # BUG FIX: Previously gated on wx_idle (agent also silent),
                            # which meant if the agent kept talking (normal!) after the
                            # caller hung up without sending BYE, we'd never detect the
                            # hangup and the call would run forever.
                            # Chime RTP going silent IS the hangup signal — tear down
                            # regardless of whether the agent is still sending audio.
                            log.warning(
                                "RTP->WebRTC: RTP silent %.0fs — treating as caller hangup  call-id=%.36s",
                                dur_since, session.call_id)
                            session.disconnect_reason = (
                                f"RTP silence {dur_since:.0f}s — caller hangup without BYE"
                            )
                            stop.set()
                            break
                    continue

                parsed = parse_rtp(data)
                if parsed is None:
                    continue

                ptype, seq, _ts, _ssrc, payload = parsed
                session.rtp_rx_count   += 1
                session.rtp_rx_last_ts  = _time.monotonic()

                if ptype == RTP_PAYLOAD_PCMU:
                    # BUG FIX: Only update session.payload_type for real G.711
                    # audio packets. A single DTMF keypress (pt=101) used to
                    # overwrite session.payload_type, causing all subsequent
                    # outbound RTP packets to carry payload_type=101 in their
                    # header. Chime interprets pt=101 as telephone-event (DTMF),
                    # not audio, and discards/misplays it — heard as the pitched-
                    # down distortion (G.711 bytes decoded as wrong codec).
                    session.payload_type = ptype
                    if session.rtp_rx_count == 1:
                        log.info("RTP->WebRTC: first RTP ptype=%d (PCMU/u-law)  call-id=%.36s",
                                 ptype, session.call_id)
                    pcm8 = audioop.ulaw2lin(payload, 2)
                elif ptype == RTP_PAYLOAD_PCMA:
                    session.payload_type = ptype
                    if session.rtp_rx_count == 1:
                        log.info("RTP->WebRTC: first RTP ptype=%d (PCMA/a-law)  call-id=%.36s",
                                 ptype, session.call_id)
                    pcm8 = audioop.alaw2lin(payload, 2)
                else:
                    continue  # skip non-audio (DTMF pt=101, comfort noise, etc.)

                pcm16 = session.pcm8k_to_agent(pcm8)

                # Push directly into pipecat's RawAudioTrack.
                # add_audio_bytes() is non-blocking and handles its own internal
                # deque, so we never need a separate asyncio.Queue here.
                # One 20ms Chime frame = 640 bytes at 16kHz — exactly 2×10ms,
                # which satisfies RawAudioTrack's "multiple of 10ms" constraint.
                try:
                    session.sender_track.add_audio_bytes(pcm16)
                    session.webrtc_frames_tx += 1
                except Exception as e:
                    log.debug("RTP->WebRTC: add_audio_bytes failed: %s", e)

        finally:
            try:
                fd = sock.fileno()
                if fd != -1:
                    loop.remove_reader(fd)
                    log.debug("RTP->WebRTC: remove_reader fd=%d  call-id=%.36s", fd, session.call_id)
            except Exception:
                pass
            log.info("RTP->WebRTC ended  call-id=%.36s  rtp_rx=%d",
                     session.call_id, session.rtp_rx_count)
            stop.set()

    # -- RTP output loop (agent -> Chime) --------------------------------------

    async def _rtp_output_loop(self, session: CallSession, stop: asyncio.Event):
        """
        Drain session.agent_to_rtp_queue and send metered RTP to Chime.
        Uses a separate OS thread (same pattern as v2) for tight 20ms pacing.

        BUG FIX: asyncio.Queue is NOT thread-safe. The original code called
        session.agent_to_rtp_queue.get_nowait() from an OS thread while the
        asyncio event loop was running, causing data corruption and lost frames.
        Fix: use a stdlib threading.Queue for the OS thread path, exactly as
        ws_bridge_v2 does. ChimeAudioReceiverSink now puts into this sync queue.
        """
        import queue as _queue
        import threading as _threading

        # Thread-safe queue for the RTP output OS thread
        rtp_out_queue_sync: _queue.Queue = _queue.Queue(maxsize=400)
        session._rtp_out_queue_sync = rtp_out_queue_sync

        # Shim so keepalive suppression check still works
        class _QueueShim:
            def empty(self):
                return rtp_out_queue_sync.empty()
        session._rtp_out_queue = _QueueShim()

        PACKET_INTERVAL = CHIME_FRAME_MS / 1000.0
        stop_thread     = _threading.Event()

        def _rtp_output_thread():
            next_tx = _time.monotonic()
            sock    = session.rtp_socket
            dest    = (session.remote_host, session.remote_rtp_port)
            # Signal to _rtp_keepalive that we now own the RTP stream.
            # From this point on we are the sole caller of next_rtp_header().
            session._rtp_output_running = True
            log.info("RTP output thread started  call-id=%.36s", session.call_id)
            silence_frame = None  # computed lazily after payload_type is known
            while not stop_thread.is_set():
                now  = _time.monotonic()
                wait = next_tx - now
                if wait > 0.0005:
                    _time.sleep(wait)
                elif wait < -0.200:
                    next_tx = _time.monotonic()

                # Pull from the thread-safe sync queue.
                # BUG FIX: The original code used asyncio.Queue.get_nowait() from
                # an OS thread -- asyncio.Queue is NOT thread-safe and caused
                # silent data corruption. Now uses rtp_out_queue_sync (threading.Queue).
                # On empty queue, send silence to keep RTP timestamp/seq continuous.
                try:
                    chunk = rtp_out_queue_sync.get_nowait()
                except _queue.Empty:
                    # Queue empty: send a silence frame to keep the stream alive
                    if silence_frame is None or len(silence_frame) != CHIME_FRAME_SAMPLES:
                        silence_frame = (
                            PCMA_SILENCE_FRAME
                            if session.payload_type == RTP_PAYLOAD_PCMA
                            else PCMU_SILENCE_FRAME
                        )
                    pkt = session.next_rtp_header() + silence_frame
                    try:
                        sock.sendto(pkt, dest)
                        session.rtp_tx_count  += 1
                    except OSError:
                        break
                    next_tx += PACKET_INTERVAL
                    continue

                pkt = session.next_rtp_header() + chunk
                try:
                    sock.sendto(pkt, dest)
                    session.rtp_tx_count   += 1
                    session.rtp_tx_last_ts  = _time.monotonic()
                except OSError as e:
                    log.warning("RTP output thread: sendto failed: %s", e)
                    break

                next_tx += PACKET_INTERVAL

            session._rtp_output_running = False
            log.info("RTP output thread ended  call-id=%.36s  rtp_tx=%d",
                     session.call_id, session.rtp_tx_count)

        rtp_thread = _threading.Thread(
            target=_rtp_output_thread,
            name=f"rtp-out-{session.call_id[:8]}",
            daemon=True,
        )
        rtp_thread.start()

        try:
            while not stop.is_set():
                await asyncio.sleep(0.5)
        finally:
            stop_thread.set()
            rtp_thread.join(timeout=2.0)
            log.info("RTP output loop ended  call-id=%.36s", session.call_id)

    # -- Watchdog / keepalives (unchanged from v2) -----------------------------

    async def _call_watchdog(self, session: CallSession, stop: asyncio.Event):
        interval = 15.0
        while not stop.is_set():
            await asyncio.sleep(interval)
            if stop.is_set():
                break
            now = _time.monotonic()
            dur = now - session.call_start_ts
            rtp_ago = (now - session.rtp_rx_last_ts) if session.rtp_rx_last_ts else None
            wx_ago  = (now - session.webrtc_frames_rx_last_ts) if session.webrtc_frames_rx_last_ts else None
            rtp_warn  = rtp_ago is not None and rtp_ago > RTP_SILENCE_TIMEOUT_S
            wx_warn   = wx_ago  is not None and wx_ago  > WS_IDLE_TIMEOUT_S
            rtp_never = session.rtp_rx_count == 0 and dur > 10
            level = logging.WARNING if (rtp_warn or wx_warn or rtp_never) else logging.INFO
            log.log(level,
                "WATCHDOG  call-id=%.36s  dur=%.0fs  "
                "rtp_rx=%d(%s)  rtp_tx=%d  webrtc_rx=%d(%s)  webrtc_tx=%d  rtvi=%s",
                session.call_id, dur,
                session.rtp_rx_count,  f"{rtp_ago:.0f}s ago" if rtp_ago else "never",
                session.rtp_tx_count,
                session.webrtc_frames_rx, f"{wx_ago:.0f}s ago" if wx_ago else "never",
                session.webrtc_frames_tx,
                "ok" if session.rtvi_handshake_ok else "pending",
            )

    async def _sip_session_refresh(self, session: CallSession, stop: asyncio.Event):
        if SIP_SESSION_REFRESH_S <= 0:
            return
        interval = getattr(session, 'sip_refresh_interval', SIP_SESSION_REFRESH_S)
        log.info("SIP session refresh task started  call-id=%.36s  interval=%ds",
                 session.call_id, interval)
        await asyncio.sleep(interval)
        while not stop.is_set() and session.state == CallState.ESTABLISHED:
            try:
                session.sip_cseq += 1
                session.sdp_version += 1
                branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
                chime_codecs = getattr(session, 'sip_chime_codecs', '0 101')
                sdp = build_sdp_minimal(self._local_ip, session.local_rtp_port,
                                        chime_codecs, session_version=session.sdp_version)
                to_hdr = session.sip_from_hdr
                if session.to_tag and "tag=" not in to_hdr:
                    to_hdr = f"{to_hdr};tag={session.to_tag}"
                routes = list(reversed(session.sip_record_route))
                if session.sip_remote_contact:
                    c = session.sip_remote_contact
                    req_uri = c[c.index("<")+1:c.index(">")] if "<" in c else c.split(";")[0].strip()
                else:
                    req_uri = f"sip:{session.remote_host}"
                if routes:
                    first_route = routes[0]
                    dest_uri = first_route[first_route.index("<")+1:first_route.index(">")] if "<" in first_route else first_route.split(";")[0]
                    dest_part = dest_uri.replace("sip:", "").split(";")[0]
                    if ":" in dest_part:
                        dest_host, dest_port_str = dest_part.rsplit(":", 1)
                        dest_port = int(dest_port_str) if dest_port_str.isdigit() else 5060
                    else:
                        dest_host, dest_port = dest_part, 5060
                    send_addr = (dest_host, dest_port)
                else:
                    send_addr = session.sip_remote_addr
                route_hdrs = "".join(f"Route: {r}\r\n" for r in routes)
                se_val = getattr(session, 'session_expires', SESSION_EXPIRES_S)
                proto  = session.sip_proto
                contact_transport = f";transport={proto.lower()}" if proto == "TCP" else ""
                reinvite = (
                    f"INVITE {req_uri} SIP/2.0\r\n"
                    f"Via: SIP/2.0/{proto} {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                    f"From: {to_hdr}\r\n"
                    f"To: {session.sip_to_hdr}\r\n"
                    f"Call-ID: {session.call_id}\r\n"
                    f"CSeq: {session.sip_cseq} INVITE\r\n"
                    f"Contact: <sip:pbx@{self._local_ip}:{SIP_PORT}{contact_transport}>\r\n"
                    f"Max-Forwards: 70\r\n"
                    f"Session-Expires: {se_val};refresher=uas\r\n"
                    f"Min-SE: {MIN_SE_S}\r\n"
                    f"Supported: timer\r\n"
                    f"{route_hdrs}"
                    f"Content-Type: application/sdp\r\n"
                    f"Content-Length: {len(sdp.encode())}\r\n"
                    f"\r\n{sdp}"
                )
                session.sip_transport.sendto(reinvite.encode(), send_addr)
                log.info("SIP session refresh sent [%s]  call-id=%.36s  cseq=%d  dest=%s:%d",
                         proto, session.call_id, session.sip_cseq, send_addr[0], send_addr[1])
            except Exception as e:
                log.warning("SIP session refresh failed: %s  call-id=%.36s", e, session.call_id)
            await asyncio.sleep(interval)
        log.info("SIP session refresh ended  call-id=%.36s", session.call_id)

    async def _rtcp_keepalive(self, session, stop: asyncio.Event):
        import struct as _struct
        rtcp_port = session.remote_rtp_port + 1
        dest = (session.remote_host, rtcp_port)
        try:
            rtcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rtcp_sock.bind(("0.0.0.0", session.local_rtp_port + 1))
            rtcp_sock.setblocking(False)
        except OSError as e:
            log.warning("RTCP keepalive: cannot bind: %s  call-id=%.36s", e, session.call_id)
            return
        log.info("RTCP keepalive started  dest=%s:%d  call-id=%.36s",
                 dest[0], rtcp_port, session.call_id)
        ntp_offset = 2208988800

        # EC2 connection tracking keepalive interval.
        #
        # EC2 conntrack has two UDP timeouts:
        #   - UDP (single-direction or single req/resp): 30s default, max 60s
        #   - UDP stream (bidirectional, >1 req/resp):  180s default, max 180s
        #
        # Our call drops at ~180s because the Chime→bridge RTP inbound conntrack
        # entry expires when the caller is listening (not talking). The bridge→Chime
        # outbound direction stays alive via the continuous silence frames from
        # _rtp_output_loop, but EC2 conntrack tracks each direction independently.
        #
        # Fix: send a keepalive on the RTP socket (not just RTCP) toward Chime's
        # RTP source port every CONNTRACK_KEEPALIVE_S seconds. This refreshes the
        # conntrack entry for the Chime→bridge inbound flow by proving the bridge
        # is actively communicating on that 5-tuple.
        #
        # We send a minimal RTP comfort noise packet (payload type 13, 1 byte CN
        # payload per RFC 3389) rather than a raw empty datagram. This is valid
        # RTP that Chime will silently discard, and it does not affect the audio
        # stream because CN packets are not decoded as voice by G.711 codecs.
        # Interval is 20s — well under the 30s minimum UDP timeout.
        CONNTRACK_KEEPALIVE_S = 20
        _conntrack_ticks = 0

        rtp_dest = (session.remote_host, session.remote_rtp_port)

        try:
            while not stop.is_set() and session.state == CallState.ESTABLISHED:
                await asyncio.sleep(5.0)
                if stop.is_set():
                    break

                # --- RTCP sender report (every 5s, unchanged) ---
                try:
                    now = _time.time()
                    ntp_sec  = int(now) + ntp_offset
                    ntp_frac = int((now % 1) * (2**32))
                    sr = _struct.pack("!BBHIIIIII",
                        0x80, 200, 6, session.ssrc,
                        ntp_sec, ntp_frac, session.timestamp & 0xFFFFFFFF,
                        session.rtp_tx_count & 0xFFFFFFFF,
                        (session.rtp_tx_count * 160) & 0xFFFFFFFF,
                    )
                    rtcp_sock.sendto(sr, dest)
                except OSError as e:
                    log.debug("RTCP keepalive sendto: %s", e)

                # --- EC2 conntrack refresh on the RTP socket (every 20s) ---
                # Sends a minimal RTP Comfort Noise (PT=13) packet on the same
                # source port as the RTP stream so EC2 conntrack sees bidirectional
                # traffic on the Chime↔bridge RTP 5-tuple and resets its idle timer.
                # This prevents the inbound conntrack entry from expiring at 180s
                # when the caller is silent (listening to the agent).
                _conntrack_ticks += 1
                if _conntrack_ticks * 5 >= CONNTRACK_KEEPALIVE_S:
                    _conntrack_ticks = 0
                    try:
                        # RFC 3389 Comfort Noise: PT=13, 1-byte payload (noise level 0)
                        cn_hdr = make_rtp_header(13, session.seq, session.timestamp,
                                                 session.ssrc, marker=False)
                        session.rtp_socket.sendto(cn_hdr + b'\x00', rtp_dest)
                        log.debug("Conntrack RTP keepalive sent  dest=%s:%d  call-id=%.36s",
                                  rtp_dest[0], rtp_dest[1], session.call_id)
                    except OSError as e:
                        log.debug("Conntrack RTP keepalive sendto: %s  call-id=%.36s",
                                  e, session.call_id)
        finally:
            try:
                rtcp_sock.close()
            except Exception:
                pass

    async def _rtp_keepalive(self, session, stop: asyncio.Event):
        # Sends audio to Chime while WebRTC is being established.
        # Phase 1 (pre-WebRTC): plays a ringback tone (440 Hz, 2s on/4s off)
        #   so the caller hears ringing instead of dead silence during AgentCore
        #   cold-starts (which can take 15-30s on a fresh container).
        # Phase 2 (post-WebRTC): suppresses itself entirely — the RTP output
        #   thread takes over and sends silence when the agent queue is empty.
        log.info("RTP keepalive started  call-id=%.36s", session.call_id)

        # Convert PCMU ringback cycle to PCMA if needed (rare outside Europe)
        if session.payload_type == RTP_PAYLOAD_PCMA:
            import audioop as _ao
            ringback = [_ao.ulaw2alaw(f) for f in RINGBACK_CYCLE_PCMU]
        else:
            ringback = RINGBACK_CYCLE_PCMU

        ring_idx = 0
        interval = CHIME_FRAME_MS / 1000.0
        dest = (session.remote_host, session.remote_rtp_port)

        # Initial punch-through packet to open NAT pinhole
        try:
            session.rtp_socket.sendto(
                session.next_rtp_header() + ringback[0], dest)
            ring_idx = 1
        except OSError as e:
            log.warning("RTP punch-through error: %s", e)

        while not stop.is_set() and session.state != CallState.TEARING_DOWN:
            await asyncio.sleep(interval)
            if stop.is_set():
                break
            # Suppress entirely while the RTP output thread owns the stream
            if getattr(session, '_rtp_output_running', False):
                continue
            try:
                frame = ringback[ring_idx % len(ringback)]
                ring_idx += 1
                session.rtp_socket.sendto(session.next_rtp_header() + frame, dest)
            except OSError:
                break

        log.info("RTP keepalive ended  call-id=%.36s", session.call_id)

    async def _send_bye(self, session: CallSession, reason: str = ""):
        if not session.sip_transport or not session.sip_remote_addr:
            return
        proto = session.sip_proto
        try:
            session.sip_cseq += 1
            branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)

            # ----------------------------------------------------------------
            # From/To in a BYE must mirror the original INVITE dialog:
            #   From:  our UAS identity (what Chime sent as "To" in INVITE)
            #   To:    Chime's identity  (what Chime sent as "From" in INVITE)
            # sip_from_hdr was set from INVITE "To" header  (our side)
            # sip_to_hdr   was set from INVITE "From" header (Chime side)
            # ----------------------------------------------------------------
            from_hdr = session.sip_from_hdr
            if session.to_tag and "tag=" not in from_hdr:
                from_hdr = f"{from_hdr};tag={session.to_tag}"

            reason_hdr = f"Reason: SIP;cause=200;text=\"{reason}\"\r\n" if reason else ""
            contact_transport = f";transport={proto.lower()}" if proto == "TCP" else ""

            # ----------------------------------------------------------------
            # Request-URI: use remote Contact URI from the dialog, NOT the
            # bare remote host.  Chime sets Contact to its VC FQDN; sending
            # BYE to the raw IP often hits a different Chime node that has no
            # record of the dialog and silently drops or 481s the request.
            # ----------------------------------------------------------------
            contact_hdr = session.sip_remote_contact or ""
            if "<" in contact_hdr and ">" in contact_hdr:
                req_uri = contact_hdr[contact_hdr.index("<") + 1: contact_hdr.index(">")]
            else:
                req_uri = f"sip:{session.remote_host}"

            # ----------------------------------------------------------------
            # Route set: in-dialog requests MUST follow the Record-Route path
            # established during the INVITE (reversed, as per RFC 3261 §12.2).
            # Without this, the BYE can arrive at a Chime node that has no
            # dialog state and gets silently dropped — leaving the PSTN leg
            # active until Chime's inactivity timer fires (~30-60 s).
            # ----------------------------------------------------------------
            routes = list(reversed(session.sip_record_route))
            route_hdrs = "".join(f"Route: {r}\r\n" for r in routes)

            # Determine send address: first route hop if routes present,
            # otherwise fall back to the Contact URI host, then sip_remote_addr.
            if routes:
                first_route = routes[0]
                dest_uri = (first_route[first_route.index("<") + 1: first_route.index(">")]
                            if "<" in first_route else first_route.split(";")[0])
                dest_part = dest_uri.replace("sip:", "").split(";")[0]
                if ":" in dest_part:
                    d_host, d_port_s = dest_part.rsplit(":", 1)
                    d_port = int(d_port_s) if d_port_s.isdigit() else 5060
                else:
                    d_host, d_port = dest_part, 5060
                send_addr = (d_host, d_port)
            elif "<" in contact_hdr and ">" in contact_hdr:
                # No Route set — use Contact host directly (RFC 3261 §12.2.1.1)
                contact_uri = req_uri  # already extracted above
                c_part = contact_uri.replace("sip:", "").split(";")[0]
                if ":" in c_part:
                    c_host, c_port_s = c_part.rsplit(":", 1)
                    c_port = int(c_port_s) if c_port_s.isdigit() else 5060
                else:
                    c_host, c_port = c_part, 5060
                send_addr = (c_host, c_port)
            else:
                send_addr = session.sip_remote_addr

            bye = (
                f"BYE {req_uri} SIP/2.0\r\n"
                f"Via: SIP/2.0/{proto} {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                f"From: {from_hdr}\r\n"
                f"To: {session.sip_to_hdr}\r\n"
                f"Call-ID: {session.call_id}\r\n"
                f"CSeq: {session.sip_cseq} BYE\r\n"
                f"Contact: <sip:pbx@{self._local_ip}:{SIP_PORT}{contact_transport}>\r\n"
                f"Max-Forwards: 70\r\n"
                f"{route_hdrs}"
                f"{reason_hdr}"
                f"Content-Length: 0\r\n\r\n"
            )
            bye_bytes = bye.encode()
            session.sip_transport.sendto(bye_bytes, send_addr)
            log.info("Sent BYE [%s]  call-id=%.36s  dest=%s:%d  reason=%s",
                     proto, session.call_id, send_addr[0], send_addr[1], reason or "normal")

            # ----------------------------------------------------------------
            # RFC 3261 §17.1.2 retransmission for UDP (TCP is reliable).
            # Non-INVITE requests must be retransmitted with timer T1 doubling
            # up to T2 (4s) until a final response is received or 64*T1 elapses.
            # Without this, a single lost UDP packet silently leaves the PSTN
            # leg active.  We retransmit up to 6 times (covers ~32s) and stop
            # as soon as Chime replies 200 OK (which handle_sip already processes).
            # ----------------------------------------------------------------
            if proto == "UDP":
                asyncio.ensure_future(
                    self._retransmit_bye(bye_bytes, send_addr, session.call_id)
                )
        except Exception as e:
            log.warning("Failed to send BYE  call-id=%.36s : %s", session.call_id, e)

    async def _retransmit_bye(self, bye_bytes: bytes, send_addr: tuple, call_id: str):
        """
        Retransmit a BYE request per RFC 3261 §17.1.2 until Chime responds or
        the session is gone (meaning we received the 200 OK and tore down).

        Timer sequence: 500ms, 1s, 2s, 4s, 4s, 4s  (T1=500ms, cap at T2=4s).
        Stops early if the session has already been removed from self.sessions
        (i.e. teardown completed, which means we got a 200 OK for the BYE).
        """
        T1 = 0.5   # RFC 3261 T1 = 500 ms
        T2 = 4.0   # RFC 3261 T2 = 4 s
        t  = T1
        for attempt in range(1, 7):   # up to 6 retransmits (~15.5s total wait)
            await asyncio.sleep(t)
            if call_id not in self.sessions:
                log.debug("BYE retransmit: session gone after %d attempt(s)  call-id=%.36s",
                          attempt, call_id)
                return
            session = self.sessions.get(call_id)
            if session is None or session.state == CallState.TEARING_DOWN:
                return
            try:
                session.sip_transport.sendto(bye_bytes, send_addr)
                log.debug("BYE retransmit #%d  call-id=%.36s  dest=%s:%d",
                          attempt, call_id, send_addr[0], send_addr[1])
            except Exception as e:
                log.debug("BYE retransmit failed: %s  call-id=%.36s", e, call_id)
                return
            t = min(t * 2, T2)

    async def _teardown_with_bye(self, session: CallSession):
        """
        Convenience wrapper: send SIP BYE then tear down the session.
        Called when the agent initiates the hangup (RTVI bot-stopped or WebRTC
        closed while Chime side is still alive).
        """
        call_id = session.call_id
        await self._send_bye(session, reason="agent-hangup")
        await self._teardown(call_id, send_bye=False)  # BYE already sent above

    async def _teardown(self, call_id: str, send_bye: bool = False, bye_reason: str = ""):
        session = self.sessions.pop(call_id, None)
        if session is None:
            return
        session.state = CallState.TEARING_DOWN
        if send_bye:
            await self._send_bye(session, bye_reason)
        # Close WebRTC peer connection
        if session.pc:
            try:
                await session.pc.close()
            except Exception:
                pass
        try:
            # CRITICAL: remove_reader BEFORE close(). If close() runs first,
            # the fd becomes invalid (-1). The next socket allocation may reuse
            # that fd number, and add_reader() on the new fd will fail silently
            # because epoll has a broken/stale entry for it. Always remove first.
            try:
                fd = session.rtp_socket.fileno()
                if fd != -1:
                    loop = asyncio.get_event_loop()
                    loop.remove_reader(fd)
            except Exception:
                pass
            session.rtp_socket.close()
        except Exception:
            pass
        # Clean up TCP call-id registration so the connection slot is freed
        SipTcpConn.unregister(call_id)
        log.info("Session torn down  call-id=%.36s", call_id)


# ---------------------------------------------------------------------------
# SIP TCP listener
# ---------------------------------------------------------------------------

def _split_sip_message(buf: bytes):
    sep = buf.find(b"\r\n\r\n")
    if sep == -1:
        sep = buf.find(b"\n\n")
        if sep == -1:
            return None, buf
        eoh = sep + 2
    else:
        eoh = sep + 4
    header_part = buf[:eoh]
    cl_match = re.search(rb"[Cc]ontent-[Ll]ength\s*:\s*(\d+)", header_part)
    content_length = int(cl_match.group(1)) if cl_match else 0
    total = eoh + content_length
    if len(buf) < total:
        return None, buf
    return buf[:total], buf[total:]


class SipTcpConn:
    """
    Wraps a single TCP connection from Chime.

    Presents the same sendto(data, addr) interface as asyncio.DatagramTransport
    so handle_sip() works identically regardless of transport.

    Keeps a dict of call-id -> SipTcpConn so responses for a call always go
    back on the same TCP socket (important: Chime may have many parallel TCP
    connections and expects replies in-stream).

    RFC 5626 "outbound" keepalive: send CRLF-CRLF double-CRLF ping every
    TCP_KEEPALIVE_S seconds.  Chime sends a single CRLF pong back; we ignore it.
    """

    # Class-level registry: call_id -> SipTcpConn
    # Populated by handle_sip when an INVITE is matched to a TCP connection.
    _call_registry: dict[str, "SipTcpConn"] = {}

    TCP_KEEPALIVE_S = int(os.getenv("TCP_KEEPALIVE_S", "30"))

    def __init__(self, bridge: "PbxBridge", reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter):
        self.bridge   = bridge
        self.reader   = reader
        self.writer   = writer
        self._peer    = writer.get_extra_info("peername", ("?", 0))
        self._closed  = False

    # ------------------------------------------------------------------
    # Transport interface (matches asyncio.DatagramTransport.sendto)
    # ------------------------------------------------------------------

    def sendto(self, data: bytes, addr):
        """Write SIP message onto the TCP stream. addr is ignored (connection is 1:1)."""
        if self._closed:
            log.debug("SipTcpConn.sendto: connection already closed %s:%d", *self._peer)
            return
        try:
            self.writer.write(data)
            # Schedule drain; don't await here — sendto must be non-blocking
            # to stay consistent with the DatagramTransport interface.
            asyncio.ensure_future(self._drain())
        except Exception as e:
            log.debug("SIP TCP sendto error %s:%d: %s", *self._peer, e)

    async def _drain(self):
        try:
            await self.writer.drain()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def run(self):
        log.info("SIP TCP connection from %s:%d", *self._peer)
        buf        = b""
        keepalive  = asyncio.create_task(self._keepalive())
        try:
            while True:
                try:
                    chunk = await self.reader.read(4096)
                except Exception as e:
                    log.debug("SIP TCP read error %s:%d: %s", *self._peer, e)
                    break
                if not chunk:
                    break
                # Skip CRLF keepalive pongs (RFC 5626 §4.4.1)
                if chunk.strip(b"\r\n") == b"":
                    continue
                buf += chunk
                while True:
                    msg_bytes, buf = _split_sip_message(buf)
                    if msg_bytes is None:
                        break
                    asyncio.create_task(
                        self.bridge.handle_sip(self, msg_bytes, self._peer)
                    )
        finally:
            keepalive.cancel()
            self._closed = True
            # Unregister any call-ids this connection owns
            stale = [cid for cid, conn in SipTcpConn._call_registry.items()
                     if conn is self]
            for cid in stale:
                SipTcpConn._call_registry.pop(cid, None)
                log.info("SIP TCP: unregistered call-id=%.36s (connection closed)", cid)
            log.info("SIP TCP connection closed %s:%d", *self._peer)
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass

    async def _keepalive(self):
        """Send double-CRLF ping every TCP_KEEPALIVE_S seconds (RFC 5626 §4.4.1)."""
        interval = self.TCP_KEEPALIVE_S
        if interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            if self._closed:
                break
            try:
                self.writer.write(b"\r\n\r\n")
                await self.writer.drain()
                log.debug("SIP TCP keepalive ping -> %s:%d", *self._peer)
            except Exception as e:
                log.debug("SIP TCP keepalive failed %s:%d: %s", *self._peer, e)
                break

    # ------------------------------------------------------------------
    # Call-id registration helpers
    # ------------------------------------------------------------------

    @classmethod
    def register(cls, call_id: str, conn: "SipTcpConn"):
        cls._call_registry[call_id] = conn
        log.debug("SIP TCP: registered call-id=%.36s -> %s:%d",
                  call_id, *conn._peer)

    @classmethod
    def lookup(cls, call_id: str) -> Optional["SipTcpConn"]:
        return cls._call_registry.get(call_id)

    @classmethod
    def unregister(cls, call_id: str):
        cls._call_registry.pop(call_id, None)


async def _handle_tcp_conn(reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter,
                           bridge: "PbxBridge"):
    await SipTcpConn(bridge, reader, writer).run()


# ---------------------------------------------------------------------------
# SIP UDP protocol
# ---------------------------------------------------------------------------

class SipProtocol(asyncio.DatagramProtocol):
    """asyncio UDP protocol for SIP.  Presents the same sendto() interface as
    SipTcpConn so handle_sip() needs no transport-specific branching."""

    def __init__(self, bridge: "PbxBridge"):
        self.bridge    = bridge
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport):
        self.transport = transport
        log.info("SIP UDP listener ready on %s:%d", SIP_HOST, SIP_PORT)

    def datagram_received(self, data: bytes, addr: tuple):
        asyncio.create_task(self.bridge.handle_sip(self.transport, data, addr))

    def error_received(self, exc):
        log.error("SIP socket error: %s", exc)


# ---------------------------------------------------------------------------
# Health-check HTTP server
# ---------------------------------------------------------------------------

async def _health(reader, writer, bridge: PbxBridge):
    try:
        await reader.read(1024)
        sessions_info = []
        for cid, s in bridge.sessions.items():
            dur = _time.monotonic() - s.call_start_ts
            sessions_info.append({
                "call_id":      cid,
                "state":        s.state.name,
                "duration_s":   round(dur, 1),
                "rtp_rx":       s.rtp_rx_count,
                "rtp_tx":       s.rtp_tx_count,
                "webrtc_rx":    s.webrtc_frames_rx,
                "rtvi_ok":      s.rtvi_handshake_ok,
                "ice_state":    s.pc.iceConnectionState if s.pc else "none",
            })
        body = json.dumps({"status": "ok", "active_calls": len(bridge.sessions),
                           "sessions": sessions_info}) + "\n"
        resp = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Connection: close\r\n\r\n"
            + body
        )
        writer.write(resp.encode())
        await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


# ---------------------------------------------------------------------------
# Outbound SIP OPTIONS keepalive (unchanged from v2)
# ---------------------------------------------------------------------------

async def _send_options_ping(transport: asyncio.DatagramTransport, local_ip: str):
    if not CHIME_VC_FQDN:
        log.warning("CHIME_VC_FQDN not set — OPTIONS pings disabled")
        return
    log.info("OPTIONS keepalive target: %s:5060", CHIME_VC_FQDN)
    seq     = random.randint(1, 9999)
    call_id = "%08x@%s" % (random.randint(0, 0xFFFFFFFF), local_ip)
    while True:
        tag    = "%08x" % random.randint(0, 0xFFFFFFFF)
        branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
        msg = (
            f"OPTIONS sip:{CHIME_VC_FQDN} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {local_ip}:{SIP_PORT};branch={branch};rport\r\n"
            f"From: <sip:{local_ip}@{CHIME_VC_FQDN}>;tag={tag}\r\n"
            f"To: <sip:{CHIME_VC_FQDN}>\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {seq} OPTIONS\r\n"
            f"Contact: <sip:{local_ip}:{SIP_PORT}>\r\n"
            f"Max-Forwards: 70\r\n"
            f"Allow: INVITE, ACK, BYE, CANCEL, OPTIONS\r\n"
            f"Content-Length: 0\r\n\r\n"
        )
        try:
            loop   = asyncio.get_event_loop()
            infos  = await loop.getaddrinfo(CHIME_VC_FQDN, 5060,
                                            family=socket.AF_INET, type=socket.SOCK_DGRAM)
            dest_ip = infos[0][4][0]
            transport.sendto(msg.encode(), (dest_ip, 5060))
            log.debug("OPTIONS ping -> %s (%s)", CHIME_VC_FQDN, dest_ip)
        except Exception as e:
            log.warning("OPTIONS ping failed: %s", e)
        seq += 1
        await asyncio.sleep(OPTIONS_INTERVAL)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    log.info("Starting PBX Bridge v3 (WebRTC/KVS AgentCore transport)")
    log.info("  AGENT_RUNTIME_ARN : %s", AGENT_RUNTIME_ARN or "(LOCAL_AGENT mode)")
    log.info("  AWS_REGION        : %s", AWS_REGION)
    log.info("  KVS_CHANNEL_NAME : %s (agent-side TURN only)", KVS_CHANNEL_NAME)
    log.info("  ICE_TIMEOUT_S     : %ds", ICE_TIMEOUT_S)
    log.info("  Audio rates       : Chime=%dHz (G.711 20ms)  Agent TX=%dHz (AGENT_SAMPLE_RATE)",
             CHIME_SAMPLE_RATE, AGENT_SAMPLE_RATE)
    log.info("  Agent RX rate     : %s  (FORCE_AGENT_RX_RATE=%s)",
             f"forced={FORCE_AGENT_RX_RATE}Hz" if FORCE_AGENT_RX_RATE else "auto from frame.sample_rate",
             os.getenv("FORCE_AGENT_RX_RATE", "unset"))
    log.info("  SIP UDP          : udp://%s:%d", SIP_HOST, SIP_PORT)
    log.info("  RTP ports        : %d - %d", RTP_PORT_MIN, RTP_PORT_MAX)
    log.info("  Health port      : %d", HTTP_HEALTH_PORT)
    log.info("  TCP keepalive    : %ds (RFC 5626 CRLF ping)", SipTcpConn.TCP_KEEPALIVE_S)

    bridge = PbxBridge()
    loop   = asyncio.get_running_loop()

    # UDP SIP listener (primary — Chime default)
    sip_transport, _ = await loop.create_datagram_endpoint(
        lambda: SipProtocol(bridge),
        local_addr=(SIP_HOST, SIP_PORT),
    )

    servers = []

    # Plain TCP SIP listener on the same port 5060
    # RFC 3261 §18.2: a SIP element MUST listen for both UDP and TCP on the
    # same port.  Chime Voice Connector will use TCP when the message exceeds
    # the MTU or when it's explicitly configured to prefer TCP.
    tcp_sip_server = await asyncio.start_server(
        lambda r, w: _handle_tcp_conn(r, w, bridge),
        SIP_HOST, SIP_PORT,
    )
    servers.append(tcp_sip_server)
    log.info("  SIP TCP          : tcp://%s:%d", SIP_HOST, SIP_PORT)

    # Optional TLS SIP on a separate port (SIP_TLS_PORT)
    if SIP_TLS_PORT and SIP_TLS_CERT and SIP_TLS_KEY:
        import ssl
        tls_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_ctx.load_cert_chain(SIP_TLS_CERT, SIP_TLS_KEY)
        tls_ctx.verify_mode = ssl.CERT_NONE
        tls_server = await asyncio.start_server(
            lambda r, w: _handle_tcp_conn(r, w, bridge),
            "0.0.0.0", SIP_TLS_PORT, ssl=tls_ctx,
        )
        servers.append(tls_server)
        log.info("  SIP TLS          : tls://0.0.0.0:%d", SIP_TLS_PORT)
    elif SIP_TLS_PORT:
        log.warning("  SIP_TLS_PORT=%d set but SIP_TLS_CERT/KEY missing — TLS listener not started",
                    SIP_TLS_PORT)

    health_server = await asyncio.start_server(
        lambda r, w: _health(r, w, bridge),
        "0.0.0.0", HTTP_HEALTH_PORT,
    )
    servers.append(health_server)

    log.info("PBX Bridge v3 ready — UDP+TCP on port %d, TLS on port %d (if configured)",
             SIP_PORT, SIP_TLS_PORT or 0)

    tasks = [asyncio.create_task(srv.serve_forever()) for srv in servers]
    tasks.append(asyncio.create_task(_send_options_ping(sip_transport, bridge._local_ip)))
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        log.error("Server error: %s", e)
    finally:
        for t in tasks:
            t.cancel()
        sip_transport.close()
        for srv in servers:
            srv.close()
        _boto3_executor.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())