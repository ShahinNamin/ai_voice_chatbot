"""
Chime Voice Connector → AgentCore WebSocket Bridge
=====================================================
Listens on UDP 5060 for SIP INVITE from Amazon Chime Voice Connector.
For each call:
  1. Answers the SIP INVITE (100 Trying → 180 Ringing → 200 OK with SDP)
  2. Builds a SigV4-signed WebSocket URL for AWS Bedrock AgentCore
  3. Opens a persistent WebSocket to the AI agent
  4. Bridges RTP audio (G.711 μ-law, 8 kHz) ↔ WebSocket (raw PCM 16-bit LE, 8 kHz)
  5. Tears down cleanly on BYE or WebSocket close

Architecture
------------
                        ┌──────────────────────────────────────────┐
  PSTN caller           │           This bridge service            │
  ──────────────────►   │                                          │
  Chime Voice Connector │  SIP/UDP :5060  ──►  SipCallSession      │
  sends SIP INVITE  ──► │  RTP/UDP :10000-20000 (dynamic)          │
                        │       ↕  PCM decode/encode               │
                        │  WebSocket client ──►  AgentCore         │
                        └──────────────────────────────────────────┘

Audio path
----------
  Inbound:  RTP packet → strip 12-byte header → G.711 μ-law payload
            → audioop.ulaw2lin() → 16-bit PCM → send as binary WS frame
  Outbound: binary WS frame (16-bit PCM 8 kHz) → audioop.lin2ulaw()
            → G.711 μ-law → RTP packet → send UDP to caller

Dependencies
------------
  pip install websockets botocore
  (audioop is stdlib in Python ≤ 3.12; for 3.13+ install audioop-lts)

Environment variables
  AGENT_RUNTIME_ARN          ARN of the AgentCore runtime
  AWS_REGION                 AWS region (e.g. us-east-1)
  SIGNED_URL_EXPIRY_SECONDS  Signed URL TTL in seconds (default 300)
  SIP_BIND_HOST              IP to bind SIP/RTP sockets (default 0.0.0.0)
  PUBLIC_IP                  Elastic IP announced in SDP (required on EC2/ECS)
  RTP_PORT_MIN               RTP port range start (default 10000)
  RTP_PORT_MAX               RTP port range end   (default 20000)
  LOG_LEVEL                  debug | info | warning (default info)

  AWS credentials are resolved automatically from the EC2 instance profile
  or ECS task role. No static keys are read or required.
"""

import asyncio
import audioop
import logging
import os
import random
import re
import socket
import struct
import uuid
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

import boto3
import websockets
from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest


from dotenv import load_dotenv

# Load variables from .env
load_dotenv()


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# AWS / AgentCore — credentials come from the EC2 instance profile or
# ECS task role automatically; no keys are needed in the environment.
AGENT_RUNTIME_ARN  = os.environ["AGENT_RUNTIME_ARN"]
AWS_REGION         = os.environ["AWS_REGION"]
URL_EXPIRY_SECONDS = int(os.getenv("SIGNED_URL_EXPIRY_SECONDS", "300"))

# Network
BIND_HOST  = os.getenv("SIP_BIND_HOST", "0.0.0.0")
PUBLIC_IP  = os.getenv("PUBLIC_IP", "")   # filled at startup if not set
SIP_PORT   = 5060
RTP_MIN    = int(os.getenv("RTP_PORT_MIN", "10000"))
RTP_MAX    = int(os.getenv("RTP_PORT_MAX", "20000"))
LOG_LEVEL  = os.getenv("LOG_LEVEL", "info").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
log = logging.getLogger("bridge")

# RTP constants
RTP_HEADER_SIZE  = 12      # fixed RTP header bytes
PCMU_PAYLOAD     = 0       # G.711 μ-law
SAMPLE_RATE      = 8000    # Hz
PTIME_MS         = 20      # ms per RTP packet
SAMPLES_PER_PKT  = SAMPLE_RATE * PTIME_MS // 1000   # 160 samples
PCM_BYTES_PKT    = SAMPLES_PER_PKT * 2              # 320 bytes (16-bit)
ULAW_BYTES_PKT   = SAMPLES_PER_PKT                  # 160 bytes

# ---------------------------------------------------------------------------
# AgentCore — SigV4-signed WebSocket URL
# ---------------------------------------------------------------------------

def build_agentcore_ws_url() -> str:
    """
    Construct and sign a WebSocket URL for the AgentCore runtime endpoint.
    Credentials are resolved automatically from the EC2 instance profile or
    ECS task role via boto3's standard credential chain — no static keys.
    """
    ws_url = (
        f"wss://bedrock-agentcore.{AWS_REGION}.amazonaws.com"
        f"/runtimes/{quote(AGENT_RUNTIME_ARN, safe='')}/ws"
    )

    credentials = boto3.Session().get_credentials()

    aws_request = AWSRequest(method="GET", url=ws_url)
    SigV4QueryAuth(
        credentials, "bedrock-agentcore", AWS_REGION, expires=URL_EXPIRY_SECONDS
    ).add_auth(aws_request)

    return aws_request.url


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def detect_public_ip() -> str:
    """Best-effort: connect to a public address and read the local socket IP."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception as exc:
        log.error("Could not auto-detect public IP: %s", exc)
        return "127.0.0.1"


def _rtp_header(seq: int, ts: int, ssrc: int, payload_type: int = PCMU_PAYLOAD) -> bytes:
    """Build a minimal 12-byte RTP header."""
    return struct.pack(
        "!BBHII",
        0x80,          # V=2, P=0, X=0, CC=0
        payload_type & 0x7F,
        seq & 0xFFFF,
        ts & 0xFFFFFFFF,
        ssrc & 0xFFFFFFFF,
    )


def _parse_sdp_media(sdp: str):
    """Return (ip, port) of the remote RTP endpoint from SDP, or (None, None)."""
    ip = port = None
    for line in sdp.splitlines():
        m = re.match(r"c=IN IP4 (\S+)", line)
        if m:
            ip = m.group(1)
        m = re.match(r"m=audio (\d+)", line)
        if m:
            port = int(m.group(1))
    return ip, port


# ---------------------------------------------------------------------------
# RTP UDP transport (one per call)
# ---------------------------------------------------------------------------

class RtpProtocol(asyncio.DatagramProtocol):
    """asyncio UDP protocol that queues inbound RTP payloads."""

    def __init__(self):
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=200)
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.remote_addr: Optional[tuple] = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        if len(data) < RTP_HEADER_SIZE:
            return
        # Strip header; ignore RTCP (payload type 72-76)
        pt = data[1] & 0x7F
        if pt >= 72:
            return
        payload = data[RTP_HEADER_SIZE:]
        if not self.queue.full():
            self.queue.put_nowait(payload)

    def send_rtp(self, seq: int, ts: int, ssrc: int, ulaw_payload: bytes):
        if self.transport and self.remote_addr:
            hdr = _rtp_header(seq, ts, ssrc)
            self.transport.sendto(hdr + ulaw_payload, self.remote_addr)

    def error_received(self, exc):
        log.warning("RTP socket error: %s", exc)

    def connection_lost(self, exc):
        log.debug("RTP socket closed")


async def open_rtp_socket(bind_host: str) -> tuple[asyncio.DatagramTransport, RtpProtocol, int]:
    """Bind a UDP socket on a random port in [RTP_MIN, RTP_MAX]."""
    loop = asyncio.get_running_loop()
    for _ in range(200):
        port = random.randint(RTP_MIN, RTP_MAX)
        try:
            transport, protocol = await loop.create_datagram_endpoint(
                RtpProtocol,
                local_addr=(bind_host, port),
            )
            return transport, protocol, port
        except OSError as exc:
            log.error("OSerror: %s", exc )
            continue
    raise RuntimeError("Could not bind an RTP port in configured range")


# ---------------------------------------------------------------------------
# SIP message helpers
# ---------------------------------------------------------------------------

SIP_VERSION = "SIP/2.0"


def _extract_header(msg: str, name: str) -> str:
    pattern = re.compile(rf"^{re.escape(name)}\s*:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
    m = pattern.search(msg)
    return m.group(1).strip() if m else ""


def _build_response(
    request: str,
    status_code: int,
    reason: str,
    extra_headers: str = "",
    body: str = "",
) -> bytes:
    via     = _extract_header(request, "Via")
    frm     = _extract_header(request, "From")
    to      = _extract_header(request, "To")
    call_id = _extract_header(request, "Call-ID")
    cseq    = _extract_header(request, "CSeq")

    # Add tag to To for final responses
    if status_code >= 200 and ";tag=" not in to:
        to += f";tag={uuid.uuid4().hex[:8]}"

    content_type = ""
    if body:
        content_type = "Content-Type: application/sdp\r\n"

    resp = (
        f"{SIP_VERSION} {status_code} {reason}\r\n"
        f"Via: {via}\r\n"
        f"From: {frm}\r\n"
        f"To: {to}\r\n"
        f"Call-ID: {call_id}\r\n"
        f"CSeq: {cseq}\r\n"
        f"Server: ChimeAgentCoreBridge/1.0\r\n"
        f"{extra_headers}"
        f"{content_type}"
        f"Content-Length: {len(body.encode())}\r\n"
        f"\r\n"
        f"{body}"
    )
    return resp.encode()


def _build_sdp(local_ip: str, rtp_port: int) -> str:
    """Minimal SDP answer — G.711 μ-law only."""
    return (
        "v=0\r\n"
        f"o=bridge 0 0 IN IP4 {local_ip}\r\n"
        "s=AgentCore Bridge\r\n"
        f"c=IN IP4 {local_ip}\r\n"
        "t=0 0\r\n"
        f"m=audio {rtp_port} RTP/AVP 0 101\r\n"
        "a=rtpmap:0 PCMU/8000\r\n"
        "a=rtpmap:101 telephone-event/8000\r\n"
        "a=fmtp:101 0-15\r\n"
        "a=sendrecv\r\n"
        f"a=ptime:{PTIME_MS}\r\n"
    )


# ---------------------------------------------------------------------------
# Per-call session
# ---------------------------------------------------------------------------

@dataclass
class SipCallSession:
    call_id: str
    request: str           # original INVITE text
    remote_addr: tuple     # (ip, port) of Chime
    rtp_protocol: RtpProtocol
    rtp_transport: asyncio.DatagramTransport
    rtp_port: int
    ws: Optional[object] = field(default=None, repr=False)
    active: bool = True

    # RTP sequencing state
    seq:  int = field(default_factory=lambda: random.randint(0, 0xFFFF))
    ts:   int = field(default_factory=lambda: random.randint(0, 0xFFFFFFFF))
    ssrc: int = field(default_factory=lambda: random.randint(0, 0xFFFFFFFF))

    async def run(self, sip_transport, public_ip: str):
        """Main coroutine for a call: sign AgentCore URL, connect, bridge audio."""
        try:
            # --- 1. Build signed AgentCore WebSocket URL ---
            log.info("[%s] Signing AgentCore WebSocket URL", self.call_id)
            ws_url = build_agentcore_ws_url()
            log.info("[%s] Connecting to AgentCore: %s…", self.call_id, ws_url[:80])

            # --- 2. Connect WebSocket ---
            async with websockets.connect(
                ws_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                self.ws = ws
                log.info("[%s] WebSocket connected", self.call_id)

                # Run inbound (RTP→WS) and outbound (WS→RTP) concurrently
                await asyncio.gather(
                    self._rtp_to_ws(ws),
                    self._ws_to_rtp(ws),
                )

        except Exception as exc:
            log.error("error while waiting for rtp to ws or ws to rtp")
            log.error("[%s] Session error: %s", self.call_id, exc)
        finally:
            self.active = False
            self._hangup(sip_transport)
            log.info("[%s] Session ended", self.call_id)

    async def _rtp_to_ws(self, ws):
        """Read μ-law RTP payload → decode to PCM → send to agent over WS."""
        log.debug("[%s] rtp→ws bridge started", self.call_id)
        while self.active:
            try:
                ulaw = await asyncio.wait_for(self.rtp_protocol.queue.get(), timeout=5.0)
                pcm = audioop.ulaw2lin(ulaw, 2)   # → 16-bit signed PCM
                await ws.send(pcm)
            except asyncio.TimeoutError as exc:
                log.error("[%s] Timeout error: %s", self.call_id, exc)
                continue
            except websockets.exceptions.ConnectionClosed as exc:
                log.error("[%s] Connection closed error: %s", self.call_id, exc)
                log.info("[%s] WS closed (inbound side)", self.call_id)
                break
            except Exception as exc:
                log.error("[%s] rtp→ws error: %s", self.call_id, exc)
                break
        self.active = False

    async def _ws_to_rtp(self, ws):
        """Receive PCM frames from agent → encode to μ-law → send as RTP."""
        log.debug("[%s] ws→rtp bridge started", self.call_id)
        while self.active:
            try:
                data = await asyncio.wait_for(ws.recv(), timeout=5.0)
                if isinstance(data, bytes):
                    await self._send_pcm_as_rtp(data)
            except asyncio.TimeoutError as exc:
                log.error("[%s] ws to RTP Timeout error: %s", self.call_id, exc)
                continue
            except websockets.exceptions.ConnectionClosed as exc:
                log.error("[%s] ws to RTP Connection closed error: %s", self.call_id, exc)
                log.info("[%s] WS closed (outbound side)", self.call_id)
                break
            except Exception as exc:
                log.error("[%s] ws→rtp error: %s", self.call_id, exc)
                break
        self.active = False

    async def _send_pcm_as_rtp(self, pcm: bytes):
        """Chop PCM into 20ms packets, encode μ-law, send RTP."""
        offset = 0
        while offset < len(pcm):
            chunk = pcm[offset: offset + PCM_BYTES_PKT]
            offset += PCM_BYTES_PKT
            if len(chunk) < 2:
                break
            # Pad last chunk to full packet size if needed
            if len(chunk) < PCM_BYTES_PKT:
                chunk = chunk + b"\x00" * (PCM_BYTES_PKT - len(chunk))
            ulaw = audioop.lin2ulaw(chunk, 2)
            self.rtp_protocol.send_rtp(self.seq, self.ts, self.ssrc, ulaw)
            self.seq  = (self.seq + 1) & 0xFFFF
            self.ts   = (self.ts + SAMPLES_PER_PKT) & 0xFFFFFFFF
            await asyncio.sleep(0)   # yield to event loop

    def _hangup(self, sip_transport):
        """Close the RTP socket for this call."""
        try:
            self.rtp_transport.close()
        except Exception as  exc:
            log.error("[%s] Session error: %s", self.call_id, exc)
            pass


# ---------------------------------------------------------------------------
# SIP UDP server
# ---------------------------------------------------------------------------

class SipProtocol(asyncio.DatagramProtocol):
    """Minimal SIP UA — handles INVITE and BYE; ignores everything else."""

    def __init__(self, public_ip: str):
        self.public_ip = public_ip
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.sessions: dict[str, SipCallSession] = {}

    def connection_made(self, transport):
        self.transport = transport
        log.info("SIP socket listening on UDP %s:%d", BIND_HOST, SIP_PORT)

    def datagram_received(self, data: bytes, addr):
        try:
            msg = data.decode("utf-8", errors="replace")
        except Exception as exc:
            log.error("datagram received error: %s", exc )
            return
        asyncio.ensure_future(self._handle(msg, addr))

    async def _handle(self, msg: str, addr: tuple):
        first_line = msg.split("\r\n", 1)[0]
        log.debug("SIP ← %s  %s", addr, first_line)

        if msg.startswith("INVITE"):
            await self._handle_invite(msg, addr)
        elif msg.startswith("ACK"):
            call_id = _extract_header(msg, "Call-ID")
            log.debug("[%s] ACK received", call_id)
        elif msg.startswith("BYE"):
            await self._handle_bye(msg, addr)
        elif msg.startswith("CANCEL"):
            await self._handle_cancel(msg, addr)
        elif msg.startswith("OPTIONS"):
            self._send(addr, _build_response(msg, 200, "OK"))
        else:
            log.debug("Ignoring SIP method: %s", first_line)

    async def _handle_invite(self, msg: str, addr: tuple):
        call_id = _extract_header(msg, "Call-ID")
        if call_id in self.sessions:
            log.debug("[%s] Duplicate INVITE – ignoring", call_id)
            return

        log.info("[%s] INVITE from %s", call_id, addr)

        # 100 Trying — sent immediately to stop retransmits
        self._send(addr, _build_response(msg, 100, "Trying"))

        # Parse remote RTP from SDP
        body_start = msg.find("\r\n\r\n")
        sdp_body   = msg[body_start + 4:] if body_start != -1 else ""
        remote_rtp_ip, remote_rtp_port = _parse_sdp_media(sdp_body)

        if not remote_rtp_ip or not remote_rtp_port:
            log.error("[%s] Could not parse SDP; rejecting", call_id)
            self._send(addr, _build_response(msg, 488, "Not Acceptable Here"))
            return

        # Bind a local RTP socket
        try:
            rtp_transport, rtp_protocol, rtp_port = await open_rtp_socket(BIND_HOST)
        except RuntimeError as exc:
            log.error("[%s] %s", call_id, exc)
            self._send(addr, _build_response(msg, 503, "Service Unavailable"))
            return

        rtp_protocol.remote_addr = (remote_rtp_ip, remote_rtp_port)
        log.info(
            "[%s] RTP: local :%d ↔ remote %s:%d",
            call_id, rtp_port, remote_rtp_ip, remote_rtp_port,
        )

        # 180 Ringing
        self._send(addr, _build_response(msg, 180, "Ringing"))

        # 200 OK with SDP
        sdp_answer = _build_sdp(self.public_ip, rtp_port)
        contact    = f"Contact: <sip:{self.public_ip}:{SIP_PORT}>\r\n"
        ok_resp    = _build_response(msg, 200, "OK", extra_headers=contact, body=sdp_answer)
        self._send(addr, ok_resp)
        log.info("[%s] 200 OK sent", call_id)

        # Create and store session
        session = SipCallSession(
            call_id=call_id,
            request=msg,
            remote_addr=addr,
            rtp_protocol=rtp_protocol,
            rtp_transport=rtp_transport,
            rtp_port=rtp_port,
        )
        self.sessions[call_id] = session

        # Start the bridging coroutine (fire-and-forget)
        asyncio.ensure_future(self._run_session(session))

    async def _run_session(self, session: SipCallSession):
        await session.run(self, self.public_ip)
        self.sessions.pop(session.call_id, None)

    async def _handle_bye(self, msg: str, addr: tuple):
        call_id = _extract_header(msg, "Call-ID")
        log.info("[%s] BYE received", call_id)
        self._send(addr, _build_response(msg, 200, "OK"))
        session = self.sessions.pop(call_id, None)
        if session:
            session.active = False

    async def _handle_cancel(self, msg: str, addr: tuple):
        call_id = _extract_header(msg, "Call-ID")
        log.info("[%s] CANCEL received", call_id)
        self._send(addr, _build_response(msg, 200, "OK"))
        if call_id in self.sessions:
            invite = self.sessions[call_id].request
            self._send(addr, _build_response(invite, 487, "Request Terminated"))
            session = self.sessions.pop(call_id, None)
            if session:
                session.active = False

    def _send(self, addr: tuple, data: bytes):
        if self.transport:
            log.debug("SIP → %s  %s", addr, data.split(b"\r\n", 1)[0].decode())
            self.transport.sendto(data, addr)

    def error_received(self, exc):
        log.warning("SIP socket error: %s", exc)

    def connection_lost(self, exc):
        log.warning("SIP socket closed: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    global PUBLIC_IP

    if not PUBLIC_IP:
        PUBLIC_IP = detect_public_ip()
        log.info("Auto-detected public IP: %s", PUBLIC_IP)
    else:
        log.info("Using configured public IP: %s", PUBLIC_IP)

    loop = asyncio.get_running_loop()

    sip_protocol = SipProtocol(public_ip=PUBLIC_IP)
    transport, _ = await loop.create_datagram_endpoint(
        lambda: sip_protocol,
        local_addr=(BIND_HOST, SIP_PORT),
    )

    log.info(
        "Bridge ready — SIP UDP :%d | RTP %d-%d | AgentCore ARN: %s",
        SIP_PORT, RTP_MIN, RTP_MAX, AGENT_RUNTIME_ARN,
    )

    try:
        await asyncio.Event().wait()   # run forever
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())