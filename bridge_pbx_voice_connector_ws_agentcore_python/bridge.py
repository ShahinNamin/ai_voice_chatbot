"""
PBX Bridge: Amazon Chime SDK Voice Connector → AWS Bedrock AgentCore

Receives SIP signalling on UDP 5060 and RTP audio, bridges bidirectionally
to the Pipecat/RTVI WebSocket agent running on AgentCore.

Architecture:
  Chime Voice Connector
       │  SIP (UDP 5060)
       ▼
  SipHandler  ──► deduplicates retransmits, allocates RTP port, sends SIP 200 OK
       │  RTP (dynamic UDP port)
       ▼
  RtpSession  ──► decodes µ-law/PCMA → PCM16 → resample to 16 kHz
       │
       ▼
  AgentCoreBridge  ──► signed WSS URL (SigV4)
       │  WebSocket (pipecat-ai/websocket-transport protocol)
       ▼
  AgentCore (Bedrock)
       │  audio back
       ▼
  RtpSession  ──► resample → encode µ-law/PCMA → send RTP to Chime

Key SIP behaviours implemented
───────────────────────────────
* Retransmit guard  – a second INVITE with the same Call-ID that arrives while
  the first is still being set up gets a 100 Trying immediately; once the
  session is established every retransmit gets the cached 200 OK replayed.
* Correct response routing – the SIP response is sent to the address/port
  extracted from the top-most Via header (rport / received parameters) per
  RFC 3581, NOT blindly back to addr.
* To-tag is generated once per call and reused on all retransmit responses.
* SIP OPTIONS keep-alives answered with 200 OK.
"""

import asyncio
import audioop
import json
import logging
import os
import random
import re
import socket
import struct
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional
from urllib.parse import quote

import boto3
import websockets
from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from dotenv import load_dotenv

load_dotenv(override=True)

_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pbx-bridge")

# ── Configuration ─────────────────────────────────────────────────────────────

SIP_HOST          = os.getenv("SIP_HOST", "0.0.0.0")
SIP_PORT          = int(os.getenv("SIP_PORT", "5060"))
RTP_PORT_MIN      = int(os.getenv("RTP_PORT_MIN", "10000"))
RTP_PORT_MAX      = int(os.getenv("RTP_PORT_MAX", "20000"))
AGENT_RUNTIME_ARN = os.getenv("AGENT_RUNTIME_ARN", "")
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
SIGNED_URL_EXPIRY = int(os.getenv("SIGNED_URL_EXPIRY_SECONDS", "300"))
HTTP_HEALTH_PORT  = int(os.getenv("HTTP_HEALTH_PORT", "8080"))
# FQDN of your Chime Voice Connector — used for outbound OPTIONS pings.
# Find it in AWS Console → Chime SDK → Voice Connectors → your VC → General.
# Looks like: abcdef1ghij2klmno3pqr4.voiceconnector.chime.aws
CHIME_VC_FQDN     = os.getenv("CHIME_VC_FQDN", "")
OPTIONS_INTERVAL  = int(os.getenv("OPTIONS_INTERVAL", "30"))  # seconds
# Set CHIME_ONLY=0 to accept calls from any SIP source (disables scanner protection)
# Default=1: rejects INVITEs with empty SDP or private RTP IPs (scanner fingerprints)

# Chime Voice Connector sends G.711 µ-law (PCMU) at 8 kHz, 20 ms frames
CHIME_SAMPLE_RATE   = 8000
CHIME_FRAME_MS      = 20
CHIME_FRAME_SAMPLES = CHIME_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 160 samples

# G.711 silence for RTP keepalive: mu-law 0x7F, A-law 0xD5, 160 bytes = 20ms
PCMU_SILENCE_FRAME = bytes([0x7F] * 160)
PCMA_SILENCE_FRAME = bytes([0xD5] * 160)

# AgentCore / Pipecat expects PCM16 at 16 kHz
AGENT_SAMPLE_RATE   = 16000
AGENT_FRAME_SAMPLES = AGENT_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 320 samples

# RTP constants
RTP_PAYLOAD_PCMU = 0
RTP_PAYLOAD_PCMA = 8
RTP_VERSION      = 2


# ── AWS helpers ───────────────────────────────────────────────────────────────

def get_aws_credentials():
    """Fetch credentials from EC2/ECS execution role (or env fallback)."""
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    return creds.access_key, creds.secret_key, creds.token


def build_signed_ws_url() -> str:
    """
    Return a signed AgentCore WebSocket URL.
    Set LOCAL_AGENT=1 to connect to a local Pipecat agent instead.
    """
    if os.getenv("LOCAL_AGENT") == "1":
        url = os.getenv("LOCAL_AGENT_WS_URL", "ws://localhost:8080/ws")
        log.info("LOCAL_AGENT mode – connecting to %s", url)
        return url

    access_key, secret_key, session_token = get_aws_credentials()
    ws_url = (
        f"wss://bedrock-agentcore.{AWS_REGION}.amazonaws.com"
        f"/runtimes/{quote(AGENT_RUNTIME_ARN, safe='')}/ws"
    )
    creds = Credentials(access_key, secret_key, token=session_token)
    req   = AWSRequest(method="GET", url=ws_url)
    SigV4QueryAuth(creds, "bedrock-agentcore", AWS_REGION, expires=SIGNED_URL_EXPIRY).add_auth(req)
    return req.url


# ── RTP helpers ───────────────────────────────────────────────────────────────

def make_rtp_header(payload_type, seq, timestamp, ssrc, marker=False) -> bytes:
    b0 = (RTP_VERSION << 6) & 0xFF
    b1 = (payload_type & 0x7F) | (0x80 if marker else 0x00)
    return struct.pack("!BBHII", b0, b1, seq & 0xFFFF, timestamp, ssrc)


def parse_rtp(data: bytes):
    """Return (payload_type, seq, timestamp, ssrc, payload) or None."""
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
    if b0 & 0x10:                          # extension header present
        if len(data) < hdr_len + 4:
            return None
        ext_words = struct.unpack_from("!H", data, hdr_len + 2)[0]
        hdr_len  += 4 + ext_words * 4
    return payload_type, seq, timestamp, ssrc, data[hdr_len:]


# ── SIP helpers ───────────────────────────────────────────────────────────────

def _is_private_ip(ip: str) -> bool:
    """Return True if ip is RFC1918 private, loopback, or link-local."""
    try:
        parts = list(map(int, ip.split('.')))
        if len(parts) != 4:
            return False
        a, b = parts[0], parts[1]
        return (
            a == 10 or
            a == 127 or
            (a == 172 and 16 <= b <= 31) or
            (a == 192 and b == 168) or
            (a == 169 and b == 254)
        )
    except Exception:
        return False


def parse_sip(raw: bytes) -> Optional[dict]:
    """
    Minimal SIP parser.  Returns:
        method, first_line, headers (lower-case keys), body (str)
    """
    try:
        text = raw.decode("utf-8", errors="replace")
    except Exception:
        return None

    # Normalise line endings
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
    via_list: list[str] = []          # ordered list of all Via headers
    for line in header_lines[1:]:
        if not line:
            continue
        if line[0] in (" ", "\t") and headers:                # folded header
            last = list(headers)[-1]
            headers[last] += " " + line.strip()
        elif ":" in line:
            k, _, v = line.partition(":")
            # Compact forms: v → via, f → from, t → to, i → call-id, m → contact
            compact = {"v": "via", "f": "from", "t": "to", "i": "call-id", "m": "contact"}
            key = compact.get(k.strip().lower(), k.strip().lower())
            if key == "via":
                via_list.append(v.strip())
                headers["via"] = v.strip()   # keep last for compat; use via_list for responses
            else:
                headers[key] = v.strip()

    return {"method": method, "first_line": first_line, "headers": headers,
            "via_list": via_list, "body": body}


def via_response_addr(via_value: str, actual_src: tuple) -> tuple:
    """
    Determine where to send a SIP response.

    For Amazon Chime Voice Connector (and any server-side SIP proxy) the
    correct destination is simply the UDP source address of the packet that
    arrived — i.e. `actual_src`.  RFC 3581 `received`/`rport` reflection is
    designed for NAT traversal of *client* UAs; Chime fills those fields with
    the EC2-internal private IP which is unreachable from outside the instance.

    We still read the sent-by port from the Via as a fallback when the source
    port looks ephemeral (>1024) and no better information is available, but
    in practice Chime sends from port 5060 and `actual_src` is always correct.
    """
    # Always use the real source IP — ignore Via received= for server proxies.
    host = actual_src[0]

    # Use the real source port. If it is the standard SIP port or an ephemeral
    # port we just keep it. Only fall back to the Via sent-by port when the
    # source port is 0 (shouldn't happen, but be safe).
    port = actual_src[1] if actual_src[1] else 5060

    return host, port


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


def build_sdp(local_ip: str, local_rtp_port: int) -> str:
    rtcp_port = local_rtp_port + 1
    return (
        "v=0\r\n"
        f"o=pbx-bridge 0 0 IN IP4 {local_ip}\r\n"
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


def build_sip_response(
    code: int,
    reason: str,
    req: dict,
    local_ip: str,
    to_tag: str = "",
    local_rtp_port: int = 0,
    include_sdp: bool = False,
) -> bytes:
    h        = req["headers"]
    sdp      = build_sdp(local_ip, local_rtp_port) if include_sdp else ""
    ctype    = "Content-Type: application/sdp\r\n" if include_sdp else ""
    to_hdr   = h.get("to", "")
    if to_tag and "tag=" not in to_hdr:
        to_hdr = f"{to_hdr};tag={to_tag}"

    # Echo ALL Via headers in original order (RFC 3261 §8.2.6.1).
    # Chime sends two Via headers (proxy outer + internal node inner).
    # If we only echo one, the proxy cannot route the ACK back to us.
    via_lines = "".join(
        f"Via: {v}\r\n" for v in req.get("via_list", [h.get("via", "")])
    )

    resp = (
        f"SIP/2.0 {code} {reason}\r\n"
        f"{via_lines}"
        f"From: {h.get('from', '')}\r\n"
        f"To: {to_hdr}\r\n"
        f"Call-ID: {h.get('call-id', '')}\r\n"
        f"CSeq: {h.get('cseq', '')}\r\n"
        f"Contact: <sip:pbx@{local_ip}:{SIP_PORT}>\r\n"
        f"{ctype}"
        f"Content-Length: {len(sdp.encode())}\r\n"
        f"\r\n"
        f"{sdp}"
    )
    return resp.encode("utf-8")


# ── Call session state machine ────────────────────────────────────────────────

class CallState(Enum):
    CONNECTING   = auto()   # WS connect in progress
    ESTABLISHED  = auto()   # 200 OK sent, media flowing
    TEARING_DOWN = auto()   # BYE/error, cleaning up


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

    ws: Optional[object] = None    # websockets.WebSocketClientProtocol

    # Cached 200 OK to replay on INVITE retransmits
    cached_200: Optional[bytes] = None

    # audioop stateful resampler state
    _rs_in:  object = None
    _rs_out: object = None

    def pcm8k_to_16k(self, pcm: bytes) -> bytes:
        out, self._rs_in = audioop.ratecv(pcm, 2, 1, CHIME_SAMPLE_RATE, AGENT_SAMPLE_RATE, self._rs_in)
        return out

    def pcm16k_to_8k(self, pcm: bytes) -> bytes:
        out, self._rs_out = audioop.ratecv(pcm, 2, 1, AGENT_SAMPLE_RATE, CHIME_SAMPLE_RATE, self._rs_out)
        return out

    def next_rtp_header(self, marker=False) -> bytes:
        hdr = make_rtp_header(self.payload_type, self.seq, self.timestamp, self.ssrc, marker)
        self.seq       = (self.seq       + 1)                   & 0xFFFF
        self.timestamp = (self.timestamp + CHIME_FRAME_SAMPLES) & 0xFFFFFFFF
        return hdr


# ── Pipecat protobuf frame codec ─────────────────────────────────────────────
#
# AgentCore uses pipecat-ai's protobuf wire format (frames.proto).
# We hand-roll the minimal encode/decode needed to avoid a compiled-proto dep.
#
# Relevant proto schema (pipecat-ai/src/pipecat/frames/frames.proto):
#
#   message Frame {
#     oneof frame {
#       AudioRawFrame       audio_raw_frame       = 4;  // agent → bridge (outbound TTS)
#       InputAudioRawFrame  input_audio_raw_frame = 5;  // bridge → agent (inbound mic)
#     }
#   }
#   message AudioRawFrame      { bytes audio=1; uint32 sample_rate=2; uint32 num_channels=3; }
#   message InputAudioRawFrame { bytes audio=1; uint32 sample_rate=2; uint32 num_channels=3; }

def _encode_varint(value: int) -> bytes:
    out = []
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out.append(bits | 0x80)
        else:
            out.append(bits)
            break
    return bytes(out)

def _decode_varint(data: bytes, pos: int):
    result = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            break
        shift += 7
    return result, pos

def _len_field(field_num: int, payload: bytes) -> bytes:
    return _encode_varint((field_num << 3) | 2) + _encode_varint(len(payload)) + payload

def _varint_field(field_num: int, value: int) -> bytes:
    return _encode_varint((field_num << 3) | 0) + _encode_varint(value)


def pack_audio(pcm16: bytes, sample_rate: int = AGENT_SAMPLE_RATE) -> bytes:
    """Encode PCM16 as Frame { input_audio_raw_frame: InputAudioRawFrame{...} }."""
    inner = (
        _len_field(1, pcm16) +
        _varint_field(2, sample_rate) +
        _varint_field(3, 1)           # num_channels = 1
    )
    return _len_field(5, inner)       # Frame field 5 = input_audio_raw_frame


def unpack_audio(data: bytes):
    """
    Decode Frame { audio_raw_frame: AudioRawFrame{...} } → (pcm16, sample_rate).
    Returns None if the frame is not an AudioRawFrame (e.g. text/control frames).

    Pipecat proto field map (frames.proto):
      Frame.audio_raw_frame       = field 4   (agent TTS output → we send to RTP)
      Frame.input_audio_raw_frame = field 5   (our mic input → we send TO agent)
      Frame.text_frame            = field 2   (transcript/control JSON)
      Frame.llm_full_response     = field 14  (text control)
    """
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3; wt = tag & 7
        if wt == 2:
            ln, pos = _decode_varint(data, pos)
            payload  = data[pos:pos+ln]; pos += ln
            if fn == 4:                # audio_raw_frame — TTS audio from agent
                return _decode_audio_inner(payload)
            if fn == 2:                # text_frame — log as ctrl
                try:
                    log.debug("WS frame: text_frame(field2) = %s", payload.decode("utf-8", errors="replace")[:200])
                except Exception:
                    pass
            # other length-delimited fields: skip
        elif wt == 0:
            _, pos = _decode_varint(data, pos)
        elif wt == 5:
            pos += 4
        elif wt == 1:
            pos += 8
        else:
            break
    return None


def _decode_audio_inner(data: bytes):
    """Decode AudioRawFrame → (audio_bytes, sample_rate)."""
    audio = b""; sr = AGENT_SAMPLE_RATE
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3; wt = tag & 7
        if wt == 2:
            ln, pos = _decode_varint(data, pos)
            val = data[pos:pos+ln]; pos += ln
            if fn == 1: audio = val
        elif wt == 0:
            v, pos = _decode_varint(data, pos)
            if fn == 2: sr = v
        elif wt == 5: pos += 4
        elif wt == 1: pos += 8
        else: break
    return audio, sr


# ── Core bridge ───────────────────────────────────────────────────────────────

class PbxBridge:
    def __init__(self):
        self.sessions: dict[str, CallSession] = {}
        self._local_ip = self._detect_local_ip()
        log.info("Local IP advertised in SDP: %s", self._local_ip)

    # ── IP detection ──────────────────────────────────────────────────────────

    def _detect_local_ip(self) -> str:
        """
        Return the IP address to advertise in SDP and to bind RTP sockets to.

        On EC2 (and any host behind NAT) this MUST be the private/interface IP,
        NOT the public/elastic IP.  The OS always sources UDP packets from the
        private interface address; if the SDP advertises the public IP instead,
        Chime's symmetric-RTP filter sees a source-IP mismatch and drops every
        inbound RTP packet silently.

        LOCAL_IP env-var is kept as an escape hatch (e.g. non-NAT bare-metal
        deployments) but on EC2 you should leave it unset and let the socket
        probe below discover the correct private IP automatically.
        """
        env = os.getenv("LOCAL_IP")
        if env:
            if _is_private_ip(env):
                log.info("LOCAL_IP override: %s (private — correct for EC2/NAT)", env)
            else:
                log.warning(
                    "LOCAL_IP=%s looks like a public IP. On EC2 behind NAT this will "
                    "cause RTP source-IP mismatches and Chime will drop all inbound RTP. "
                    "Unset LOCAL_IP to auto-detect the correct private interface IP.", env
                )
            return env
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"

    # ── RTP port allocation ───────────────────────────────────────────────────

    def _alloc_rtp_port(self) -> tuple:
        """Allocate an even RTP port with port+1 free for RTCP (RFC 3550).

        On EC2 with a directly-attached Elastic IP, the EIP↔private-IP
        translation is performed at the AWS hypervisor *outside* the instance.
        The instance's network stack only ever sees the private IP (172.31.x.x).

        We must bind to 0.0.0.0 (not the private IP) so the socket accepts
        inbound packets — binding to the specific private IP causes the kernel
        to reject packets with ICMP "port unreachable" because the UDP demux
        does not find a matching socket.

        Outbound packets sourced from a 0.0.0.0-bound socket use the private
        IP as source address, which is correct — AWS transparently rewrites
        it to the EIP before the packet leaves the hypervisor.  Chime sees
        RTP arriving from the public EIP, which matches the SDP c= line
        (also set to the private IP so it matches what the OS will use).

        The SDP must therefore advertise the PRIVATE IP, not the EIP.
        _detect_local_ip() returns the private IP via the routing-socket
        probe, which is correct for both SDP and this bind.
        """
        for _ in range(300):
            port = random.randint(RTP_PORT_MIN, RTP_PORT_MAX - 1) & ~1   # even
            try:
                # Bind to 0.0.0.0 so the socket receives packets regardless of
                # which destination IP the kernel sees after EIP translation.
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind(("0.0.0.0", port))
                # Verify RTCP port+1 is also free (bind then release)
                rtcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                rtcp_sock.bind(("0.0.0.0", port + 1))
                rtcp_sock.close()  # We don't use RTCP ourselves, just keep it free
                sock.setblocking(False)
                return port, sock
            except OSError:
                try:
                    sock.close()
                except Exception:
                    pass
        raise RuntimeError("No free RTP port pair in range %d-%d" % (RTP_PORT_MIN, RTP_PORT_MAX))

    # ── SIP dispatch ──────────────────────────────────────────────────────────

    async def handle_sip(self, transport: asyncio.DatagramTransport, data: bytes, addr: tuple):
        # Log every raw SIP datagram at DEBUG so we can see ACK/etc
        log.debug("SIP RAW from %s:%d:\n%s", addr[0], addr[1],
                  data.decode("utf-8", errors="replace")[:800])

        # SIP responses start with "SIP/2.0" — handle separately from requests
        if data.lstrip()[:7] == b"SIP/2.0":
            first_line = data.decode("utf-8", errors="replace").split("\n")[0].strip()
            # Log at INFO so OPTIONS 200/403 is always visible without DEBUG mode
            if "200" in first_line:
                log.info("SIP response %s from %s:%d  ← OPTIONS ping accepted",
                         first_line.strip(), addr[0], addr[1])
            else:
                log.warning("SIP response %s from %s:%d  ← OPTIONS ping REJECTED",
                            first_line.strip(), addr[0], addr[1])
            return  # responses to our OPTIONS pings — nothing to do

        msg = parse_sip(data)
        if not msg:
            log.warning("Unparseable SIP from %s:%d  raw=%s",
                        addr[0], addr[1], data[:200])
            return

        method  = msg["method"]
        headers = msg["headers"]
        call_id = headers.get("call-id", "")
        via     = headers.get("via", "")

        # For Chime Voice Connector (a server-side SIP proxy) the correct
        # response destination is always the actual UDP source of the packet.
        # Do NOT follow Via received=/rport= — those reflect the EC2 private IP.
        resp_addr = addr

        log.info("SIP %-8s call-id=%.36s from %s:%d  reply→%s:%d",
                 method, call_id, addr[0], addr[1], resp_addr[0], resp_addr[1])

        if method == "INVITE":
            await self._handle_invite(transport, msg, call_id, addr, resp_addr)
        elif method == "ACK":
            session = self.sessions.get(call_id)
            if session:
                if not getattr(session, "ack_received", False):
                    session.ack_received = True
                    log.info("ACK received  call-id=%.36s  from %s:%d  "
                             "-- SIP dialog complete, media should flow",
                             call_id, addr[0], addr[1])
            else:
                log.info("ACK received (no session)  call-id=%.36s  from %s:%d",
                         call_id, addr[0], addr[1])
        elif method in ("BYE", "CANCEL"):
            resp = build_sip_response(200, "OK", msg, self._local_ip)
            transport.sendto(resp, resp_addr)
            await self._teardown(call_id)
        elif method == "OPTIONS":
            resp = build_sip_response(200, "OK", msg, self._local_ip)
            transport.sendto(resp, resp_addr)
        else:
            log.warning("Unhandled SIP method=%s  call-id=%.36s  from %s:%d",
                        method, call_id, addr[0], addr[1])

    async def _handle_invite(self, transport, msg, call_id, addr, resp_addr):
        # ── Retransmit guard ──────────────────────────────────────────────────
        existing = self.sessions.get(call_id)
        if existing:
            if existing.state == CallState.ESTABLISHED and existing.cached_200:
                log.info("INVITE retransmit (established) – replaying 200 OK  call-id=%.36s", call_id)
                transport.sendto(existing.cached_200, resp_addr)
            else:
                log.info("INVITE retransmit (connecting)  – resending 183 call-id=%.36s", call_id)
                early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                           to_tag=existing.to_tag,
                                           local_rtp_port=existing.local_rtp_port,
                                           include_sdp=True)
                transport.sendto(early, resp_addr)
            return

        # ── Fresh INVITE ──────────────────────────────────────────────────────
        trying = build_sip_response(100, "Trying", msg, self._local_ip)
        transport.sendto(trying, resp_addr)

        # Generate to_tag now so it is consistent across all provisional
        # and final responses for this dialog.
        to_tag  = "%08x" % random.randint(0, 0xFFFFFFFF)

        # Allocate the RTP port early so we can include it in 183.
        # This must happen before session creation below.
        try:
            local_rtp_port_early, rtp_sock_early = self._alloc_rtp_port()
        except RuntimeError as e:
            log.error("%s", e)
            transport.sendto(
                build_sip_response(503, "Service Unavailable", msg, self._local_ip),
                resp_addr,
            )
            return

        # 183 Session Progress with SDP = "early media".
        # FreeSWITCH opens its RTP path immediately on receiving this,
        # without waiting for the ACK to complete the dialog.
        early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                   to_tag=to_tag,
                                   local_rtp_port=local_rtp_port_early,
                                   include_sdp=True)
        transport.sendto(early, resp_addr)
        log.info("Sent 183 Session Progress (early media)  call-id=%.36s  "
                 "to_tag=%s  rtp_port=%d", call_id, to_tag, local_rtp_port_early)

        log.info("INVITE SDP body: %.500s", msg["body"].strip().replace("\n", " | "))

        # Reject SIP scanner probes: empty SDP or private/bogon RTP addresses
        # (Chime always sends a valid SDP with a public IP)
        chime_only = os.getenv("CHIME_ONLY", "1") == "1"
        if chime_only and not msg["body"].strip():
            log.warning("Rejecting INVITE with empty SDP (scanner?) from %s  call-id=%.36s",
                        addr[0], call_id)
            transport.sendto(
                build_sip_response(488, "Not Acceptable Here", msg, self._local_ip),
                resp_addr,
            )
            return

        remote_host, remote_rtp_port = extract_sdp_rtp(msg["body"])
        if not remote_host:
            remote_host = addr[0]
        if not remote_rtp_port:
            log.warning("Could not parse RTP port from SDP; defaulting to 5004")
            remote_rtp_port = 5004

        # Reject calls where RTP would go to a private/RFC1918 address
        # (Chime always uses public IPs; private addresses = scanners or misconfigured)
        if chime_only and _is_private_ip(remote_host):
            log.warning("Rejecting INVITE: RTP host %s is private/RFC1918  call-id=%.36s",
                        remote_host, call_id)
            transport.sendto(
                build_sip_response(488, "Not Acceptable Here", msg, self._local_ip),
                resp_addr,
            )
            return

        # Reuse the RTP port allocated above for the 183 early media response.
        local_rtp_port, rtp_sock = local_rtp_port_early, rtp_sock_early

        # to_tag already generated above (with 180 Ringing)
        session = CallSession(
            call_id         = call_id,
            to_tag          = to_tag,
            remote_host     = remote_host,
            remote_rtp_port = remote_rtp_port,
            local_rtp_port  = local_rtp_port,
            rtp_socket      = rtp_sock,
        )
        self.sessions[call_id] = session

        log.info(
            "New call  call-id=%.36s  remote_rtp=%s:%d  local_rtp=%d",
            call_id, remote_host, remote_rtp_port, local_rtp_port,
        )

        # Send 200 OK immediately — do NOT wait for AgentCore WS connection.
        # FreeSWITCH (Chime B2BUA) will not send ACK or flow RTP until it
        # receives 200 OK. Delaying 200 OK until WS is ready creates a deadlock.
        ok_bytes = build_sip_response(
            200, "OK", msg, self._local_ip,
            to_tag         = to_tag,
            local_rtp_port = local_rtp_port,
            include_sdp    = True,
        )
        session.cached_200 = ok_bytes
        session.state = CallState.ESTABLISHED  # allow keepalive loop to run
        transport.sendto(ok_bytes, resp_addr)
        log.info("Sent 200 OK  call-id=%.36s  SDP: %s:%d <-> %s:%d",
                 call_id, self._local_ip, local_rtp_port, remote_host, remote_rtp_port)

        asyncio.create_task(self._run_call(session, transport, msg, resp_addr))

    # ── Per-call lifecycle ────────────────────────────────────────────────────

    async def _run_call(self, session: CallSession, transport, invite_msg, resp_addr):
        call_id = session.call_id
        # 200 OK already sent in _handle_invite. Connect to AgentCore now.
        # RTP keepalive is running immediately so FreeSWITCH sees media flow.
        try:
            signed_url = build_signed_ws_url()
        except Exception as e:
            log.error("Failed to build signed URL: %s", e)
            await self._teardown(call_id)
            return

        # Start the RTP keepalive immediately, before WS is ready, so
        # FreeSWITCH sees RTP arriving right after it sends ACK.
        stop = asyncio.Event()
        keepalive_task = asyncio.create_task(self._rtp_keepalive(session, stop))

        try:
            log.info("Connecting to AgentCore WS  call-id=%.36s", call_id)
            async with websockets.connect(
                signed_url,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
                additional_headers={"User-Agent": "pbx-bridge/1.0"},
            ) as ws:
                session.ws = ws
                log.info("AgentCore WS connected  call-id=%.36s", call_id)

                # ── RTVI handshake ────────────────────────────────────────────
                # FastAPIWebsocketTransport with ProtobufFrameSerializer uses
                # the RTVI protocol. On connect the server sends a bot-ready
                # TransportMessageFrame. The client must respond with a
                # client-ready TransportMessageFrame before the pipeline
                # activates and audio starts flowing.
                #
                # Frame { transport_message_frame(field4) {
                #   message(field1): '{"label":"rtvi-ai","type":"client-ready","id":"1"}'
                # }}
                try:
                    client_ready_json = json.dumps({
                        "label": "rtvi-ai",
                        "type":  "client-ready",
                        "id":    "1",
                    }).encode()
                    inner = _len_field(1, client_ready_json)
                    client_ready_frame = _len_field(4, inner)
                    await ws.send(client_ready_frame)
                    log.info("RTVI client-ready sent  call-id=%.36s", call_id)
                except Exception as e:
                    log.warning("RTVI client-ready failed: %s  call-id=%.36s", e, call_id)

                await asyncio.gather(
                    self._rtp_to_ws(session, stop),
                    self._ws_to_rtp(session, stop),
                )
                log.info("Both media loops exited  call-id=%.36s", call_id)

        except websockets.exceptions.InvalidStatusCode as e:
            log.error("WS handshake rejected HTTP %d  call-id=%.36s", e.status_code, call_id)
        except websockets.exceptions.ConnectionClosedError as e:
            log.error("WS closed with error  code=%s reason='%s'  call-id=%.36s",
                      e.code, e.reason, call_id)
        except websockets.exceptions.ConnectionClosedOK:
            log.info("WS closed cleanly  call-id=%.36s", call_id)
        except Exception as e:
            log.error("Call error  call-id=%.36s : %s", call_id, e, exc_info=True)
        finally:
            stop.set()
            keepalive_task.cancel()
            await self._teardown(call_id)

    async def _teardown(self, call_id: str):
        session = self.sessions.pop(call_id, None)
        if session is None:
            return
        session.state = CallState.TEARING_DOWN
        try:
            if session.ws and not session.ws.closed:
                await session.ws.close()
        except Exception:
            pass
        try:
            session.rtp_socket.close()
        except Exception:
            pass
        log.info("Session torn down  call-id=%.36s", call_id)

    # ── RTP keepalive / NAT punch-through ──────────────────────────────────────

    async def _rtp_keepalive(self, session, stop):
        """Send RTP silence every 20 ms to punch through Chime's symmetric-RTP gate.

        Chime will not send media back until it receives at least one RTP packet
        from our advertised IP:port. We also keep sending during silence so the
        UDP path stays open between TTS phrases.
        """
        log.info("RTP keepalive started  call-id=%.36s", session.call_id)

        def _pkt():
            sil = PCMA_SILENCE_FRAME if session.payload_type == RTP_PAYLOAD_PCMA else PCMU_SILENCE_FRAME
            return session.next_rtp_header() + sil

        try:
            session.rtp_socket.sendto(_pkt(), (session.remote_host, session.remote_rtp_port))
            log.info("RTP punch-through sent  dest=%s:%d  call-id=%.36s",
                     session.remote_host, session.remote_rtp_port, session.call_id)
        except OSError as e:
            log.warning("RTP punch-through error: %s", e)

        interval = CHIME_FRAME_MS / 1000.0
        while not stop.is_set() and session.state != CallState.TEARING_DOWN:
            await asyncio.sleep(interval)
            if stop.is_set():
                break
            if getattr(session, 'suppress_keepalive', False):
                session.suppress_keepalive = False
                continue
            try:
                session.rtp_socket.sendto(_pkt(), (session.remote_host, session.remote_rtp_port))
            except OSError:
                break

        log.info("RTP keepalive ended  call-id=%.36s", session.call_id)

    # ── RTP → WebSocket ───────────────────────────────────────────────────────

    async def _rtp_to_ws(self, session: CallSession, stop: asyncio.Event):
        """
        Read RTP datagrams from the bound UDP socket using add_reader so the
        event loop wakes up only when data actually arrives — no busy-polling.
        """
        loop   = asyncio.get_running_loop()
        sock   = session.rtp_socket
        queue: asyncio.Queue = asyncio.Queue()
        rtp_packets_rx = 0

        def _readable():
            try:
                data, _ = sock.recvfrom(4096)
                queue.put_nowait(data)
            except BlockingIOError:
                pass
            except Exception as exc:
                log.warning("RTP socket read error: %s", exc)

        # Guard against socket already closed by CANCEL/BYE before we started
        if sock.fileno() == -1 or session.state == CallState.TEARING_DOWN:
            log.info("RTP→WS: socket already closed, skipping  call-id=%.36s", session.call_id)
            stop.set()
            return

        loop.add_reader(sock.fileno(), _readable)
        log.info("RTP→WS started  call-id=%.36s", session.call_id)

        try:
            while not stop.is_set() and session.state != CallState.TEARING_DOWN:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    if rtp_packets_rx == 0:
                        log.warning("RTP→WS: NO RTP from Chime yet after 5s  call-id=%.36s"
                                    " -- check: (1) security group UDP %d-%d open inbound,"
                                    " (2) SDP advertises local IP %s -- on EC2 this must be"
                                    " the PRIVATE interface IP (e.g. 172.31.x.x), NOT the"
                                    " public/elastic IP. Unset LOCAL_IP if it is set to a public IP."
                                    " (3) tcpdump -i any udp port %d shows inbound packets",
                                    session.call_id, RTP_PORT_MIN, RTP_PORT_MAX,
                                    session.remote_host, session.local_rtp_port)
                    else:
                        log.debug("RTP→WS: no RTP for 5s  packets_so_far=%d", rtp_packets_rx)
                    continue

                parsed = parse_rtp(data)
                if parsed is None:
                    log.debug("RTP→WS: unparseable packet (%d bytes) dropped", len(data))
                    continue

                ptype, seq, _ts, _ssrc, payload = parsed
                session.payload_type = ptype
                rtp_packets_rx += 1

                if rtp_packets_rx == 1:
                    log.info("RTP→WS: first RTP packet  ptype=%d seq=%d payload=%d bytes  call-id=%.36s",
                             ptype, seq, len(payload), session.call_id)

                if ptype == RTP_PAYLOAD_PCMU:
                    pcm8 = audioop.ulaw2lin(payload, 2)
                elif ptype == RTP_PAYLOAD_PCMA:
                    pcm8 = audioop.alaw2lin(payload, 2)
                else:
                    log.debug("RTP→WS: unsupported payload type %d — skipping", ptype)
                    continue

                pcm16 = session.pcm8k_to_16k(pcm8)

                try:
                    await session.ws.send(pack_audio(pcm16))
                except websockets.exceptions.ConnectionClosed as e:
                    log.warning("RTP→WS: WS closed while sending  code=%s reason='%s'  call-id=%.36s",
                                e.code, e.reason, session.call_id)
                    break
                except Exception as e:
                    log.warning("RTP→WS: WS send error  call-id=%.36s : %s", session.call_id, e)
                    break

        finally:
            loop.remove_reader(sock.fileno())
            log.info("RTP→WS ended  call-id=%.36s  total_packets=%d",
                     session.call_id, rtp_packets_rx)
            stop.set()   # signal _ws_to_rtp to exit too

    # ── WebSocket → RTP ───────────────────────────────────────────────────────

    async def _ws_to_rtp(self, session: CallSession, stop: asyncio.Event):
        log.info("WS→RTP started  call-id=%.36s", session.call_id)
        ws_frames_rx = 0
        rtp_packets_tx = 0

        try:
            async for message in session.ws:
                if stop.is_set() or session.state != CallState.ESTABLISHED:
                    break

                if isinstance(message, bytes):
                    ws_frames_rx += 1
                    if ws_frames_rx == 1:
                        log.info("WS→RTP: first binary frame  %d bytes  header=%s  call-id=%.36s",
                                 len(message), message[:12].hex(), session.call_id)
                    elif ws_frames_rx <= 5:
                        log.debug("WS→RTP: frame #%d  %d bytes  header=%s  call-id=%.36s",
                                  ws_frames_rx, len(message), message[:12].hex(), session.call_id)

                    result = unpack_audio(message)
                    if result is None:
                        # Not an AudioRawFrame — log first few occurrences then silence
                        if ws_frames_rx <= 5:
                            log.debug("WS→RTP: non-audio binary frame #%d  %d bytes  header=%s  call-id=%.36s",
                                      ws_frames_rx, len(message), message[:16].hex(), session.call_id)
                        continue

                    pcm16, agent_sr = result
                    if not pcm16:
                        continue

                    # audioop.ratecv requires an even byte count (2 bytes per PCM16 sample).
                    # AgentCore may return an odd-length buffer — drop the stray byte.
                    if len(pcm16) % 2:
                        pcm16 = pcm16[:-1]
                    if not pcm16:
                        continue

                    if agent_sr and agent_sr != AGENT_SAMPLE_RATE:
                        log.debug("WS→RTP: agent sr=%d (expected %d)", agent_sr, AGENT_SAMPLE_RATE)
                    pcm8 = session.pcm16k_to_8k(pcm16)
                    encoded = (
                        audioop.lin2alaw(pcm8, 2)
                        if session.payload_type == RTP_PAYLOAD_PCMA
                        else audioop.lin2ulaw(pcm8, 2)
                    )

                    for i in range(0, len(encoded), CHIME_FRAME_SAMPLES):
                        chunk = encoded[i : i + CHIME_FRAME_SAMPLES]
                        if not chunk:
                            break
                        pkt = session.next_rtp_header() + chunk
                        try:
                            session.rtp_socket.sendto(
                                pkt, (session.remote_host, session.remote_rtp_port)
                            )
                            rtp_packets_tx += 1
                            session.suppress_keepalive = True
                        except OSError as e:
                            log.warning("WS→RTP: RTP sendto failed: %s", e)

                elif isinstance(message, str):
                    # Control / transcript frames — always log at INFO so we see agent events
                    log.info("WS ctrl  call-id=%.36s : %.300s", session.call_id, message)

        except websockets.exceptions.ConnectionClosedError as e:
            log.warning("WS→RTP: WS closed with error  code=%s reason='%s'  call-id=%.36s",
                        e.code, e.reason, session.call_id)
        except websockets.exceptions.ConnectionClosedOK:
            log.info("WS→RTP: WS closed cleanly  call-id=%.36s", session.call_id)
        except Exception as e:
            log.error("WS→RTP: unexpected error  call-id=%.36s : %s", session.call_id, e, exc_info=True)
        finally:
            log.info("WS→RTP ended  call-id=%.36s  ws_frames_rx=%d  rtp_packets_tx=%d  "
                     "(if rtp_packets_tx=0: agent sent no AudioRawFrame field-4 frames)",
                     session.call_id, ws_frames_rx, rtp_packets_tx)
            stop.set()   # signal _rtp_to_ws to exit too


# ── Outbound SIP OPTIONS keepalive ───────────────────────────────────────────
# Chime Voice Connector requires periodic OPTIONS pings from your endpoint to
# mark your origination host as "reachable". Without them, Chime will accept
# the SIP signalling but withhold RTP media.
# Set CHIME_VC_FQDN to your Voice Connector FQDN in the environment.

async def _send_options_ping(transport: asyncio.DatagramTransport, local_ip: str):
    """Send SIP OPTIONS to the Chime Voice Connector every OPTIONS_INTERVAL seconds.

    The From URI must use the VC FQDN as the domain — Chime returns 403 if
    the From domain is an IP address or unrecognised hostname.
    Correct format:  From: <sip:{local_ip}@{vc_fqdn}>;tag=...
    """
    if not CHIME_VC_FQDN:
        log.warning("CHIME_VC_FQDN not set — OPTIONS pings disabled.\n"
                    "  Set CHIME_VC_FQDN=<your-vc>.voiceconnector.chime.aws")
        return

    log.info("OPTIONS keepalive target: %s:5060", CHIME_VC_FQDN)
    seq      = random.randint(1, 9999)
    call_id  = "%08x@%s" % (random.randint(0, 0xFFFFFFFF), local_ip)

    while True:
        tag    = "%08x" % random.randint(0, 0xFFFFFFFF)
        branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
        # From domain must be the VC FQDN so Chime recognises the trunk
        msg = (
            f"OPTIONS sip:{CHIME_VC_FQDN} SIP/2.0\r\n"
            f"Via: SIP/2.0/UDP {local_ip}:{SIP_PORT};branch={branch}\r\n"
            f"From: <sip:{local_ip}@{CHIME_VC_FQDN}>;tag={tag}\r\n"
            f"To: <sip:{CHIME_VC_FQDN}>\r\n"
            f"Call-ID: {call_id}\r\n"
            f"CSeq: {seq} OPTIONS\r\n"
            f"Contact: <sip:{local_ip}:{SIP_PORT}>\r\n"
            f"Max-Forwards: 70\r\n"
            f"Allow: INVITE, ACK, BYE, CANCEL, OPTIONS\r\n"
            f"Content-Length: 0\r\n"
            f"\r\n"
        )
        try:
            # Resolve FQDN each time in case Chime rotates IPs
            loop   = asyncio.get_event_loop()
            infos  = await loop.getaddrinfo(
                CHIME_VC_FQDN, 5060,
                family=socket.AF_INET, type=socket.SOCK_DGRAM
            )
            dest_ip = infos[0][4][0]
            transport.sendto(msg.encode(), (dest_ip, 5060))
            log.debug("OPTIONS ping → %s (%s)", CHIME_VC_FQDN, dest_ip)
        except Exception as e:
            log.warning("OPTIONS ping failed: %s", e)
        seq += 1
        await asyncio.sleep(OPTIONS_INTERVAL)


# ── asyncio UDP / SIP protocol ────────────────────────────────────────────────

class SipProtocol(asyncio.DatagramProtocol):
    def __init__(self, bridge: PbxBridge):
        self.bridge    = bridge
        self.transport: Optional[asyncio.DatagramTransport] = None

    def connection_made(self, transport):
        self.transport = transport
        log.info("SIP UDP listener ready on %s:%d", SIP_HOST, SIP_PORT)

    def datagram_received(self, data: bytes, addr: tuple):
        asyncio.create_task(self.bridge.handle_sip(self.transport, data, addr))

    def error_received(self, exc):
        log.error("SIP socket error: %s", exc)



# ── TCP/TLS SIP listener ──────────────────────────────────────────────────────
# Used when the bridge connects directly to Chime Voice Connector (TLS 5061)
# or when FreePBX uses TCP transport toward this bridge.
#
# FreePBX note: with "from-pstn" context + P-Asserted-Identity, FreePBX acts as
# a B2BUA and sends plain UDP SIP to this bridge. The TCP/TLS listener is an
# optional extra for direct-to-Chime deployments.

SIP_TLS_PORT = int(os.getenv("SIP_TLS_PORT", "0"))   # 0 = disabled
SIP_TLS_CERT = os.getenv("SIP_TLS_CERT", "")         # path to PEM cert
SIP_TLS_KEY  = os.getenv("SIP_TLS_KEY",  "")         # path to PEM key


def _split_sip_message(buf: bytes):
    """
    Extract one complete SIP message from a TCP stream buffer.
    Returns (message_bytes, remaining_buf) or (None, buf) if incomplete.
    """
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
    """Wraps one TCP/TLS SIP connection; presents the same sendto() interface as
    asyncio.DatagramTransport so handle_sip() works unchanged."""

    def __init__(self, bridge: PbxBridge, reader, writer):
        self.bridge = bridge
        self.reader = reader
        self.writer = writer
        self._peer  = writer.get_extra_info("peername", ("?", 0))

    async def run(self):
        log.info("SIP TCP connection from %s:%d", *self._peer)
        buf = b""
        try:
            while True:
                chunk = await self.reader.read(4096)
                if not chunk:
                    break
                buf += chunk
                while True:
                    msg_bytes, buf = _split_sip_message(buf)
                    if msg_bytes is None:
                        break
                    asyncio.create_task(
                        self.bridge.handle_sip(self, msg_bytes, self._peer)
                    )
        except Exception as e:
            log.debug("SIP TCP conn error %s:%d : %s", *self._peer, e)
        finally:
            log.info("SIP TCP connection closed %s:%d", *self._peer)
            try:
                self.writer.close()
            except Exception:
                pass

    def sendto(self, data: bytes, addr):
        try:
            self.writer.write(data)
            asyncio.create_task(self.writer.drain())
        except Exception as e:
            log.debug("SIP TCP sendto error: %s", e)


async def _handle_tcp_conn(reader, writer, bridge: PbxBridge):
    await SipTcpConn(bridge, reader, writer).run()


# ── Health-check HTTP server ──────────────────────────────────────────────────

async def _health(reader, writer, bridge: PbxBridge):
    try:
        await reader.read(1024)
        body = json.dumps({"status": "ok", "active_calls": len(bridge.sessions)}) + "\n"
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


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    log.info("Starting PBX Bridge")
    log.info("  AGENT_RUNTIME_ARN : %s", AGENT_RUNTIME_ARN or "(not set – LOCAL_AGENT mode)")
    log.info("  AWS_REGION        : %s", AWS_REGION)
    log.info("")
    log.info("  Deployment notes:")
    log.info("  - If using FreePBX/Asterisk as B2BUA: configure a SIP peer/trunk pointing")
    log.info("    to this bridge on UDP %d.  Set context=from-pstn on the FreePBX trunk.", SIP_PORT)
    log.info("  - If connecting Chime Voice Connector directly: set SIP_TLS_PORT=5061 and")
    log.info("    provide SIP_TLS_CERT / SIP_TLS_KEY paths for mutual TLS.")
    log.info("  - Security group must allow UDP %d (SIP) + UDP %d-%d (RTP) inbound.",
             SIP_PORT, RTP_PORT_MIN, RTP_PORT_MAX)

    bridge = PbxBridge()
    loop   = asyncio.get_running_loop()

    # UDP SIP listener (FreePBX B2BUA → bridge, or direct Chime plain UDP)
    sip_transport, _ = await loop.create_datagram_endpoint(
        lambda: SipProtocol(bridge),
        local_addr=(SIP_HOST, SIP_PORT),
    )

    servers = []

    # Optional TLS SIP listener (direct Chime Voice Connector without FreePBX)
    if SIP_TLS_PORT and SIP_TLS_CERT and SIP_TLS_KEY:
        import ssl
        tls_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_ctx.load_cert_chain(SIP_TLS_CERT, SIP_TLS_KEY)
        tls_ctx.verify_mode = ssl.CERT_NONE   # Chime uses wildcard; match FreePBX "Verify Server=No"
        tls_server = await asyncio.start_server(
            lambda r, w: _handle_tcp_conn(r, w, bridge),
            "0.0.0.0", SIP_TLS_PORT, ssl=tls_ctx,
        )
        servers.append(tls_server)
        log.info("  SIP TLS listener: tls://0.0.0.0:%d", SIP_TLS_PORT)
    elif SIP_TLS_PORT:
        log.warning("  SIP_TLS_PORT=%d set but SIP_TLS_CERT/KEY missing — TLS listener not started",
                    SIP_TLS_PORT)

    health_server = await asyncio.start_server(
        lambda r, w: _health(r, w, bridge),
        "0.0.0.0", HTTP_HEALTH_PORT,
    )
    servers.append(health_server)

    log.info("PBX Bridge ready.")
    log.info("  SIP UDP  udp://%s:%d", SIP_HOST, SIP_PORT)
    log.info("  RTP      %d – %d", RTP_PORT_MIN, RTP_PORT_MAX)
    log.info("  Health   http://0.0.0.0:%d/", HTTP_HEALTH_PORT)

    # asyncio.TaskGroup and except* require Python 3.11+; use gather for 3.9 compat.
    tasks = [asyncio.create_task(srv.serve_forever()) for srv in servers]
    # Outbound OPTIONS keepalive — tells Chime our endpoint is reachable
    tasks.append(asyncio.create_task(
        _send_options_ping(sip_transport, bridge._local_ip)
    ))
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


if __name__ == "__main__":
    asyncio.run(main())