"""
PBX Bridge: Amazon Chime SDK Voice Connector -> AWS Bedrock AgentCore
v2  -  Enhanced disconnect diagnostics

Receives SIP signalling on UDP 5060 and RTP audio, bridges bidirectionally
to the Pipecat/RTVI WebSocket agent running on AgentCore.

Architecture:
  Chime Voice Connector
       |  SIP (UDP 5060)
       v
  SipHandler  --> deduplicates retransmits, allocates RTP port, sends SIP 200 OK
       |  RTP (dynamic UDP port)
       v
  RtpSession  --> decodes u-law/PCMA -> PCM16 -> resample to 16 kHz
       |
       v
  AgentCoreBridge  --> signed WSS URL (SigV4)
       |  WebSocket (pipecat-ai/websocket-transport protocol)
       v
  AgentCore (Bedrock)
       |  audio back
       v
  RtpSession  --> resample -> encode u-law/PCMA -> send RTP to Chime

DISCONNECT DIAGNOSTICS ADDED (v2)
----------------------------------
Every disconnect now logs a structured DISCONNECT REPORT with:
  - Which side closed first (WS closed / RTP timeout / SIP BYE / error)
  - WebSocket close code + reason string
  - RTP stats: total packets rx/tx, last-packet-ago seconds
  - WS stats: total frames rx/tx, last-frame-ago seconds
  - Call duration at time of disconnect
  - Whether the RTVI handshake completed
  - Whether the signed URL was about to expire
  - asyncio task exception tracebacks

New env vars:
  RTP_SILENCE_TIMEOUT_S   -  seconds of RTP silence before we log a warning (default 10)
  WS_IDLE_TIMEOUT_S       -  seconds of WS silence before we log a warning (default 30)
  DISCONNECT_LOG_FILE     -  if set, also write DISCONNECT REPORTs to this file path
"""

import asyncio
try:
    import audioop
except ImportError:
    try:
        import audioop_lts as audioop  # pip install audioop-lts (Python 3.13+)
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
import traceback
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

# Verify audioop works (removed in Python 3.13 - needs audioop-lts)
try:
    audioop.ulaw2lin(b'\x7f' * 2, 2)
except Exception as _e:
    raise RuntimeError(
        f"audioop self-test failed: {_e}\n"
        "On Python 3.13+ run: pip install audioop-lts"
    )

_log_level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("pbx-bridge")

# Optional separate file handler for disconnect reports
_DISCONNECT_LOG_FILE = os.getenv("DISCONNECT_LOG_FILE", "")
if _DISCONNECT_LOG_FILE:
    _disc_handler = logging.FileHandler(_DISCONNECT_LOG_FILE)
    _disc_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _disc_log = logging.getLogger("pbx-bridge.disconnect")
    _disc_log.addHandler(_disc_handler)
    _disc_log.setLevel(logging.DEBUG)
else:
    _disc_log = log

# -- Configuration -------------------------------------------------------------

SIP_HOST          = os.getenv("SIP_HOST", "0.0.0.0")
SIP_PORT          = int(os.getenv("SIP_PORT", "5060"))
RTP_PORT_MIN      = int(os.getenv("RTP_PORT_MIN", "10000"))
RTP_PORT_MAX      = int(os.getenv("RTP_PORT_MAX", "20000"))
AGENT_RUNTIME_ARN = os.getenv("AGENT_RUNTIME_ARN", "")
AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
SIGNED_URL_EXPIRY = int(os.getenv("SIGNED_URL_EXPIRY_SECONDS", "300"))
HTTP_HEALTH_PORT  = int(os.getenv("HTTP_HEALTH_PORT", "8080"))
CHIME_VC_FQDN     = os.getenv("CHIME_VC_FQDN", "")
OPTIONS_INTERVAL  = int(os.getenv("OPTIONS_INTERVAL", "30"))

# Disconnect watchdog timeouts
RTP_SILENCE_TIMEOUT_S  = int(os.getenv("RTP_SILENCE_TIMEOUT_S", "10"))
WS_IDLE_TIMEOUT_S      = int(os.getenv("WS_IDLE_TIMEOUT_S", "30"))
# How long RTP can be silent before treating it as a hangup (both RTP AND WS must be idle).
RTP_SILENCE_HANGUP_S   = int(os.getenv("RTP_SILENCE_HANGUP_S", "45"))
# RFC 4028 Session-Expires value advertised in our 200 OK.
# Using 1800s (30 min) so refresh fires at ~890s, well clear of any
# Chime internal media timeouts. Chime will negotiate down if needed.
SESSION_EXPIRES_S      = int(os.getenv("SESSION_EXPIRES_S", "1800"))

# RFC 4028 Min-SE: minimum session expiry we will accept.
MIN_SE_S               = int(os.getenv("MIN_SE_S", "90"))

# How often to send a SIP re-INVITE (seconds).
# Computed per-call as (negotiated_session_expires / 2) - 10.
# This default is used only if negotiation fails.
SIP_SESSION_REFRESH_S  = int(os.getenv("SIP_SESSION_REFRESH_S", "890"))

# Chime Voice Connector sends G.711 u-law (PCMU) at 8 kHz, 20 ms frames
CHIME_SAMPLE_RATE   = 8000
CHIME_FRAME_MS      = 20
CHIME_FRAME_SAMPLES = CHIME_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 160 samples

# G.711 silence
PCMU_SILENCE_FRAME = bytes([0x7F] * 160)
PCMA_SILENCE_FRAME = bytes([0xD5] * 160)

# AgentCore / Pipecat expects PCM16 at 16 kHz
AGENT_SAMPLE_RATE   = 16000
AGENT_FRAME_SAMPLES = AGENT_SAMPLE_RATE * CHIME_FRAME_MS // 1000   # 320 samples

RTP_PAYLOAD_PCMU = 0
RTP_PAYLOAD_PCMA = 8
RTP_VERSION      = 2


# -- AWS helpers ---------------------------------------------------------------

def get_aws_credentials():
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()
    return creds.access_key, creds.secret_key, creds.token


def build_signed_ws_url() -> str:
    if os.getenv("LOCAL_AGENT") == "1":
        url = os.getenv("LOCAL_AGENT_WS_URL", "ws://localhost:8080/ws")
        log.info("LOCAL_AGENT mode - connecting to %s", url)
        return url

    access_key, secret_key, session_token = get_aws_credentials()
    ws_url = (
        f"wss://bedrock-agentcore.{AWS_REGION}.amazonaws.com"
        f"/runtimes/{quote(AGENT_RUNTIME_ARN, safe='')}/ws"
    )
    creds = Credentials(access_key, secret_key, token=session_token)
    req   = AWSRequest(method="GET", url=ws_url)
    SigV4QueryAuth(creds, "bedrock-agentcore", AWS_REGION, expires=SIGNED_URL_EXPIRY).add_auth(req)
    log.debug("Signed WS URL built (expires in %ds)", SIGNED_URL_EXPIRY)
    return req.url


# -- RTP helpers ---------------------------------------------------------------

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


# -- SIP helpers ---------------------------------------------------------------

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


def via_response_addr(via_value: str, actual_src: tuple) -> tuple:
    host = actual_src[0]
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


def build_sdp_minimal(local_ip: str, local_rtp_port: int, codec_str: str = "0 101",
                      session_version: int = 0) -> str:
    """
    Build an SDP that exactly mirrors Chime's original offer codec list.
    Used for re-INVITE session refreshes to avoid media renegotiation.
    Only include the codecs Chime originally offered -- adding extras (like PCMA)
    can cause Chime to renegotiate the session and stop sending RTP.
    The session_version is bumped on each refresh to signal a new media session
    and reset Chime's internal RTP activity timer.
    """
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


def build_sdp(local_ip: str, local_rtp_port: int, session_version: int = 0) -> str:
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


def build_sip_response(
    code: int,
    reason: str,
    req: dict,
    local_ip: str,
    to_tag: str = "",
    local_rtp_port: int = 0,
    include_sdp: bool = False,
    session_expires: int = 0,
) -> bytes:
    h        = req["headers"]
    sdp      = build_sdp(local_ip, local_rtp_port) if include_sdp else ""
    ctype    = "Content-Type: application/sdp\r\n" if include_sdp else ""
    to_hdr   = h.get("to", "")
    if to_tag and "tag=" not in to_hdr:
        to_hdr = f"{to_hdr};tag={to_tag}"

    via_lines = "".join(
        f"Via: {v}\r\n" for v in req.get("via_list", [h.get("via", "")])
    )

    # RFC 4028 session timer headers -- declare we are the refresher
    timer_hdrs = ""
    if session_expires > 0 and code == 200:
        timer_hdrs = (
            f"Session-Expires: {session_expires};refresher=uas\r\n"
            f"Min-SE: {MIN_SE_S}\r\n"
            f"Supported: timer\r\n"
            f"Require: timer\r\n"
        )

    resp = (
        f"SIP/2.0 {code} {reason}\r\n"
        f"{via_lines}"
        f"From: {h.get('from', '')}\r\n"
        f"To: {to_hdr}\r\n"
        f"Call-ID: {h.get('call-id', '')}\r\n"
        f"CSeq: {h.get('cseq', '')}\r\n"
        f"Contact: <sip:pbx@{local_ip}:{SIP_PORT}>\r\n"
        f"{timer_hdrs}"
        f"{ctype}"
        f"Content-Length: {len(sdp.encode())}\r\n"
        f"\r\n"
        f"{sdp}"
    )
    return resp.encode("utf-8")


# -- Call session state machine ------------------------------------------------

class CallState(Enum):
    CONNECTING   = auto()
    ESTABLISHED  = auto()
    TEARING_DOWN = auto()


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

    ws: Optional[object] = None

    cached_200: Optional[bytes] = None

    # Stored so _teardown can send an outbound BYE if Chime never sent one
    sip_transport: Optional[object] = None
    sip_remote_addr: Optional[tuple] = None   # (host, port) to send BYE to
    sip_from_hdr:  str = ""
    sip_to_hdr:    str = ""
    sip_cseq:      int = 1
    sip_invite_msg: Optional[dict] = None
    sip_record_route: list = field(default_factory=list)
    sip_remote_contact: str = ""
    sip_chime_codecs: str = "0 101"
    session_expires: int = SESSION_EXPIRES_S
    sip_refresh_interval: int = 90
    sdp_version: int = 0                      # bumped on each re-INVITE to reset Chime media

    _rs_in:  object = None
    _rs_out: object = None

    # -- Disconnect diagnostic counters ----------------------------------------
    # Populated by the media loops; read by _emit_disconnect_report()
    call_start_ts:      float = field(default_factory=_time.monotonic)
    ws_connect_ts:      Optional[float] = None
    rtvi_handshake_ok:  bool = False
    url_built_ts:       Optional[float] = None   # when signed URL was generated

    rtp_rx_count:       int = 0
    rtp_rx_last_ts:     Optional[float] = None
    rtp_tx_count:       int = 0
    rtp_tx_last_ts:     Optional[float] = None

    ws_frames_rx:       int = 0
    ws_frames_rx_last_ts: Optional[float] = None
    ws_frames_tx:       int = 0

    # How the disconnect was triggered
    disconnect_reason:  str = "unknown"
    ws_close_code:      Optional[int] = None
    ws_close_reason:    str = ""
    disconnect_ts:      Optional[float] = None

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


# -- Disconnect report ---------------------------------------------------------

def _emit_disconnect_report(session: CallSession, extra: str = ""):
    """
    Emit a structured DISCONNECT REPORT to the log (and optionally DISCONNECT_LOG_FILE).
    Call this just before tearing down a session so all counters are populated.
    """
    now = _time.monotonic()
    call_dur = now - session.call_start_ts

    url_age = (
        (now - session.url_built_ts) if session.url_built_ts else None
    )
    url_expiry_warn = (
        url_age is not None and url_age > (SIGNED_URL_EXPIRY * 0.8)
    )

    rtp_rx_ago = (
        (now - session.rtp_rx_last_ts) if session.rtp_rx_last_ts else None
    )
    ws_rx_ago = (
        (now - session.ws_frames_rx_last_ts) if session.ws_frames_rx_last_ts else None
    )
    ws_connected_for = (
        (now - session.ws_connect_ts) if session.ws_connect_ts else None
    )

    lines = [
        "=" * 72,
        f"DISCONNECT REPORT  call-id={session.call_id}",
        f"  reason          : {session.disconnect_reason}",
        f"  call duration   : {call_dur:.1f}s",
        f"  WS connected for: {ws_connected_for:.1f}s" if ws_connected_for else "  WS connected for: never",
        f"  RTVI handshake  : {'[ok] complete' if session.rtvi_handshake_ok else '[x] NOT completed'}",
        "",
        "  RTP inbound  (Chime -> bridge):",
        f"    packets rx    : {session.rtp_rx_count}",
        f"    last pkt ago  : {rtp_rx_ago:.1f}s" if rtp_rx_ago else "    last pkt ago  : never",
        "",
        "  RTP outbound (bridge -> Chime):",
        f"    packets tx    : {session.rtp_tx_count}",
        f"    last pkt ago  : {(now - session.rtp_tx_last_ts):.1f}s" if session.rtp_tx_last_ts else "    last pkt ago  : never",
        "",
        "  WebSocket frames:",
        f"    frames rx     : {session.ws_frames_rx}",
        f"    frames tx     : {session.ws_frames_tx}",
        f"    last rx ago   : {ws_rx_ago:.1f}s" if ws_rx_ago else "    last rx ago   : never",
    ]

    if session.ws_close_code is not None:
        lines += [
            "",
            f"  WS close code   : {session.ws_close_code}  ({_ws_close_meaning(session.ws_close_code)})",
            f"  WS close reason : '{session.ws_close_reason}'",
        ]

    if url_age is not None:
        lines += [
            "",
            f"  Signed URL age  : {url_age:.0f}s / {SIGNED_URL_EXPIRY}s expiry"
            + ("  [!] NEAR EXPIRY" if url_expiry_warn else ""),
        ]

    if extra:
        lines += ["", f"  extra           : {extra}"]

    lines += [
        "",
        "  DIAGNOSIS HINTS:",
    ]
    lines += _diagnosis_hints(session, call_dur, rtp_rx_ago, ws_rx_ago, ws_connected_for, url_expiry_warn)
    lines.append("=" * 72)

    report = "\n".join(lines)
    _disc_log.warning(report)


def _ws_close_meaning(code: Optional[int]) -> str:
    meanings = {
        1000: "Normal closure",
        1001: "Going away (server shutdown / navigation)",
        1002: "Protocol error",
        1003: "Unsupported data",
        1006: "Abnormal closure (no close frame  -  TCP reset or timeout)",
        1007: "Invalid frame payload data",
        1008: "Policy violation",
        1009: "Message too large",
        1011: "Internal server error",
        1012: "Service restart",
        1013: "Try again later",
        4000: "AgentCore: session ended",
        4001: "AgentCore: auth error",
        4008: "AgentCore: timeout / idle",
    }
    return meanings.get(code, "unknown")


def _diagnosis_hints(session, call_dur, rtp_rx_ago, ws_rx_ago,
                     ws_connected_for, url_expiry_warn) -> list:
    hints = []

    if session.ws_close_code == 1006:
        hints.append("  [x] WS code 1006 = no clean close frame. Likely causes:")
        hints.append("    - AgentCore crashed / was killed (check AgentCore logs)")
        hints.append("    - Network interruption (EC2->AgentCore path)")
        hints.append("    - AgentCore idle timeout (check AgentCore timeout settings)")

    if session.ws_close_code in (4008,):
        hints.append("  [x] AgentCore sent timeout/idle close. The agent likely stopped")
        hints.append("    receiving audio (RTP->WS pipeline stalled) or the agent pipeline")
        hints.append("    completed its task and ended the session.")

    if not session.rtvi_handshake_ok:
        hints.append("  [x] RTVI handshake never completed  -  bot-ready was never received.")
        hints.append("    The agent pipeline may not have started up correctly.")

    if session.rtp_rx_count == 0:
        hints.append("  [x] Zero RTP packets received from Chime  -  check SG inbound UDP rules")
        hints.append(f"    and that SDP c= line advertised private IP, not public IP.")

    if rtp_rx_ago is not None and rtp_rx_ago > RTP_SILENCE_TIMEOUT_S:
        hints.append(f"  [x] No RTP from Chime for {rtp_rx_ago:.0f}s before disconnect.")
        hints.append("    Chime may have sent a BYE that the bridge missed, or the call")
        hints.append("    was silent (hold/mute) longer than Chime's RTP timeout.")

    if session.ws_frames_rx == 0:
        hints.append("  [x] Zero WS frames received from AgentCore  -  agent sent nothing.")
        hints.append("    Check AgentCore logs for pipeline startup errors.")
    elif ws_rx_ago is not None and ws_rx_ago > WS_IDLE_TIMEOUT_S:
        hints.append(f"  [x] No WS frames from AgentCore for {ws_rx_ago:.0f}s before disconnect.")
        hints.append("    The agent may have become idle / stopped producing audio.")

    if url_expiry_warn:
        hints.append(f"  [!] Signed URL was >80% through its {SIGNED_URL_EXPIRY}s expiry window.")
        hints.append("    If calls consistently drop near this window, increase SIGNED_URL_EXPIRY_SECONDS.")

    if ws_connected_for is not None and ws_connected_for < 5:
        hints.append(f"  [x] WS was only connected for {ws_connected_for:.1f}s  -  very short session.")
        hints.append("    Check AgentCore auth and startup errors.")

    if not hints:
        hints.append("  (no specific hint  -  check AgentCore logs for the server-side view)")

    return hints


# -- Pipecat protobuf frame codec ---------------------------------------------

_PIPECAT_AVAILABLE = False

def _encode_varint(value: int) -> bytes:
    out = []
    while True:
        bits = value & 0x7F
        value >>= 7
        out.append(bits | 0x80 if value else bits)
        if not value:
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
    safe_sr = sample_rate if sample_rate and sample_rate > 0 else AGENT_SAMPLE_RATE
    inner = (
        _len_field(3, pcm16) +
        _varint_field(4, safe_sr) +
        _varint_field(5, 1)
    )
    return _len_field(2, inner)


async def pack_audio_async(pcm16: bytes, sample_rate: int = AGENT_SAMPLE_RATE) -> bytes:
    return pack_audio(pcm16, sample_rate)


async def unpack_audio_async(data: bytes):
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3; wt = tag & 7
        if wt == 2:
            ln, pos = _decode_varint(data, pos)
            payload = data[pos:pos+ln]; pos += ln
            if fn == 2:
                return _decode_pipecat_audio_frame(payload)
        elif wt == 0:
            _, pos = _decode_varint(data, pos)
        elif wt == 5:
            pos += 4
        elif wt == 1:
            pos += 8
        else:
            break
    return None


def _decode_pipecat_audio_frame(data: bytes):
    audio = b""; sr = AGENT_SAMPLE_RATE
    pos = 0
    while pos < len(data):
        tag, pos = _decode_varint(data, pos)
        fn = tag >> 3; wt = tag & 7
        if wt == 2:
            ln, pos = _decode_varint(data, pos)
            val = data[pos:pos+ln]; pos += ln
            if fn == 3: audio = val
        elif wt == 0:
            v, pos = _decode_varint(data, pos)
            if fn == 4: sr = v
        elif wt == 5: pos += 4
        elif wt == 1: pos += 8
        else: break
    return audio, sr


def _decode_audio_inner(data: bytes):
    return _decode_pipecat_audio_frame(data)


# -- Core bridge ---------------------------------------------------------------

class PbxBridge:
    def __init__(self):
        self.sessions: dict[str, CallSession] = {}
        self._local_ip = self._detect_local_ip()
        log.info("Local IP advertised in SDP: %s", self._local_ip)

    def _detect_local_ip(self) -> str:
        env = os.getenv("LOCAL_IP")
        if env:
            if _is_private_ip(env):
                log.info("LOCAL_IP override: %s (private  -  correct for EC2/NAT)", env)
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

    async def handle_sip(self, transport: asyncio.DatagramTransport, data: bytes, addr: tuple):
        # Log every SIP datagram first line at INFO so we never miss a BYE/CANCEL
        # arriving around the time RTP drops.
        first_line_raw = data.decode("utf-8", errors="replace").split("\n")[0].strip()
        log.info("SIP RAW from %s:%d  >> %s", addr[0], addr[1], first_line_raw[:120])
        log.debug("SIP FULL from %s:%d:\n%s", addr[0], addr[1],
                  data.decode("utf-8", errors="replace")[:800])

        if data.lstrip()[:7] == b"SIP/2.0":
            first_line = first_line_raw
            status_code = 0
            try:
                status_code = int(first_line.split()[1])
            except (IndexError, ValueError):
                pass

            if 100 <= status_code <= 199:
                # Provisional response (100 Trying, 180 Ringing etc.) -- ignore
                log.debug("SIP provisional %d from %s:%d", status_code, addr[0], addr[1])
                return

            msg_parsed = parse_sip(data)
            cid = msg_parsed["headers"].get("call-id", "") if msg_parsed else ""
            cseq = msg_parsed["headers"].get("cseq", "") if msg_parsed else ""

            if status_code == 200:
                session = self.sessions.get(cid)
                if session and "INVITE" in cseq:
                    # ACK the 200 OK to our re-INVITE.
                    # Must be sent through the Record-Route path, not directly
                    # to the remote host -- otherwise Chime never receives it
                    # and retransmits the 200 OK, eventually resetting the session.
                    try:
                        branch = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
                        to_hdr = msg_parsed["headers"].get("to", "")

                        # Use reversed Record-Route for routing (same as re-INVITE)
                        routes = list(reversed(session.sip_record_route))
                        route_hdrs = "".join(f"Route: {r}\r\n" for r in routes)

                        # Request-URI from Contact in the 200 OK response
                        contact_hdr = msg_parsed["headers"].get("contact", "")
                        if "<" in contact_hdr:
                            req_uri = contact_hdr[contact_hdr.index("<")+1:contact_hdr.index(">")]
                        else:
                            req_uri = f"sip:{session.remote_host}"

                        # Send to first Route hop
                        if routes:
                            first_route = routes[0]
                            dest_uri = first_route[first_route.index("<")+1:first_route.index(">")] if "<" in first_route else first_route.split(";")[0]
                            dest_part = dest_uri.replace("sip:", "").split(";")[0]
                            if ":" in dest_part:
                                dhost, dport_s = dest_part.rsplit(":", 1)
                                try:
                                    dport = int(dport_s)
                                except ValueError:
                                    dport = 5060
                            else:
                                dhost, dport = dest_part, 5060
                            ack_addr = (dhost, dport)
                        else:
                            ack_addr = addr

                        ack = (
                            f"ACK {req_uri} SIP/2.0\r\n"
                            f"Via: SIP/2.0/UDP {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                            f"From: {msg_parsed['headers'].get('from', '')}\r\n"
                            f"To: {to_hdr}\r\n"
                            f"Call-ID: {cid}\r\n"
                            f"CSeq: {cseq.split()[0]} ACK\r\n"
                            f"Max-Forwards: 70\r\n"
                            f"{route_hdrs}"
                            f"Content-Length: 0\r\n"
                            f"\r\n"
                        )
                        transport.sendto(ack.encode(), ack_addr)
                        log.info("ACK sent for re-INVITE 200 OK  call-id=%.36s  dest=%s:%d",
                                 cid, ack_addr[0], ack_addr[1])
                    except Exception as e:
                        log.warning("Failed to ACK re-INVITE 200 OK: %s", e)
                else:
                    log.info("SIP 200 OK from %s:%d  call-id=%s",
                             addr[0], addr[1], cid or "(OPTIONS)")

            elif status_code == 481:
                # 481 = Chime has already ended this session internally.
                # Tear down our side immediately -- but only once.
                log.warning(
                    "SIP 481 Call/Transaction Does Not Exist from %s:%d  "
                    "call-id=%s -- Chime session ended, tearing down",
                    addr[0], addr[1], cid
                )
                session = self.sessions.get(cid)
                if session and session.state != CallState.TEARING_DOWN:
                    session.disconnect_reason = (
                        "SIP 481 -- Chime session ended between refreshes"
                    )
                    session.state = CallState.TEARING_DOWN
                    asyncio.create_task(self._teardown(cid, send_bye=False))

            else:
                log.warning("SIP response %d from %s:%d  call-id=%s",
                            status_code, addr[0], addr[1], cid)
            return

        msg = parse_sip(data)
        if not msg:
            log.warning("Unparseable SIP from %s:%d  raw=%s",
                        addr[0], addr[1], data[:200])
            return

        method  = msg["method"]
        headers = msg["headers"]
        call_id = headers.get("call-id", "")
        resp_addr = addr

        log.info("SIP %-8s call-id=%.36s from %s:%d  reply->%s:%d",
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
            log.info("SIP %s received  -  tearing down  call-id=%.36s", method, call_id)
            resp = build_sip_response(200, "OK", msg, self._local_ip)
            transport.sendto(resp, resp_addr)
            session = self.sessions.get(call_id)
            if session:
                session.disconnect_reason = f"SIP {method} from Chime"
            await self._teardown(call_id)
        elif method == "OPTIONS":
            resp = build_sip_response(200, "OK", msg, self._local_ip)
            transport.sendto(resp, resp_addr)
        else:
            log.warning("Unhandled SIP method=%s  call-id=%.36s  from %s:%d",
                        method, call_id, addr[0], addr[1])

    async def _handle_invite(self, transport, msg, call_id, addr, resp_addr):
        existing = self.sessions.get(call_id)
        if existing:
            if existing.state == CallState.ESTABLISHED and existing.cached_200:
                log.info("INVITE retransmit (established) - replaying 200 OK  call-id=%.36s", call_id)
                transport.sendto(existing.cached_200, resp_addr)
            else:
                log.info("INVITE retransmit (connecting)  - resending 183 call-id=%.36s", call_id)
                early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                           to_tag=existing.to_tag,
                                           local_rtp_port=existing.local_rtp_port,
                                           include_sdp=True)
                transport.sendto(early, resp_addr)
            return

        trying = build_sip_response(100, "Trying", msg, self._local_ip)
        transport.sendto(trying, resp_addr)

        to_tag  = "%08x" % random.randint(0, 0xFFFFFFFF)

        try:
            local_rtp_port_early, rtp_sock_early = self._alloc_rtp_port()
        except RuntimeError as e:
            log.error("%s", e)
            transport.sendto(
                build_sip_response(503, "Service Unavailable", msg, self._local_ip),
                resp_addr,
            )
            return

        early = build_sip_response(183, "Session Progress", msg, self._local_ip,
                                   to_tag=to_tag,
                                   local_rtp_port=local_rtp_port_early,
                                   include_sdp=True)
        transport.sendto(early, resp_addr)
        log.info("Sent 183 Session Progress (early media)  call-id=%.36s  "
                 "to_tag=%s  rtp_port=%d", call_id, to_tag, local_rtp_port_early)

        log.info("INVITE SDP body: %.500s", msg["body"].strip().replace("\n", " | "))

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

        if chime_only and _is_private_ip(remote_host):
            log.warning("Rejecting INVITE: RTP host %s is private/RFC1918  call-id=%.36s",
                        remote_host, call_id)
            transport.sendto(
                build_sip_response(488, "Not Acceptable Here", msg, self._local_ip),
                resp_addr,
            )
            return

        local_rtp_port, rtp_sock = local_rtp_port_early, rtp_sock_early

        session = CallSession(
            call_id         = call_id,
            to_tag          = to_tag,
            remote_host     = remote_host,
            remote_rtp_port = remote_rtp_port,
            local_rtp_port  = local_rtp_port,
            rtp_socket      = rtp_sock,
        )
        # Store SIP context so we can send an outbound BYE if Chime never sends one
        session.sip_transport   = transport
        session.sip_remote_addr = resp_addr
        session.sip_from_hdr    = msg["headers"].get("to", "")   # our To becomes their From
        session.sip_to_hdr      = msg["headers"].get("from", "")
        session.sip_cseq        = 1
        session.sip_invite_msg  = msg   # kept for re-INVITE session refresh
        # Store Record-Route (reversed for re-INVITE) and remote Contact
        session.sip_record_route = msg.get("record_route_list", [])
        session.sip_remote_contact = msg["headers"].get("contact", f"sip:{remote_host}")
        # Store the codec payload types Chime offered so we mirror them in re-INVITEs
        # Parse from SDP m= line e.g. "m=audio 33794 RTP/AVP 0 101"
        chime_codecs = "0 101"  # default
        for line in msg["body"].splitlines():
            if line.startswith("m=audio"):
                parts = line.split()
                if len(parts) > 3:
                    chime_codecs = " ".join(parts[3:])
                break
        session.sip_chime_codecs = chime_codecs
        self.sessions[call_id] = session

        log.info(
            "New call  call-id=%.36s  remote_rtp=%s:%d  local_rtp=%d",
            call_id, remote_host, remote_rtp_port, local_rtp_port,
        )

        # RFC 4028 session timer negotiation.
        # Parse what Chime offers; we take the max of our preference and theirs,
        # but must respect their Min-SE if present.
        se_hdr   = msg["headers"].get("session-expires", "")
        minse_hdr = msg["headers"].get("min-se", "")

        # Parse Chime's offered Session-Expires
        try:
            chime_se = int(se_hdr.split(";")[0].strip()) if se_hdr else 0
        except ValueError:
            chime_se = 0

        # Parse Chime's Min-SE (minimum we must respect)
        try:
            chime_minse = int(minse_hdr.strip()) if minse_hdr else MIN_SE_S
        except ValueError:
            chime_minse = MIN_SE_S

        # Use our preferred value (1800s) but respect Chime's minimum
        # If Chime offers a lower value, use the higher of the two
        negotiated_se = max(SESSION_EXPIRES_S, chime_se, chime_minse, 90)

        log.info("Session timer negotiation  call-id=%.36s  "
                 "our_pref=%ds  chime_offer=%ds  chime_min=%ds  negotiated=%ds",
                 call_id, SESSION_EXPIRES_S, chime_se, chime_minse, negotiated_se)

        session.session_expires = negotiated_se
        session.sip_refresh_interval = max(MIN_SE_S, negotiated_se // 2 - 10)

        ok_bytes = build_sip_response(
            200, "OK", msg, self._local_ip,
            to_tag         = to_tag,
            local_rtp_port = local_rtp_port,
            include_sdp    = True,
            session_expires = negotiated_se,
        )
        session.cached_200 = ok_bytes
        session.state = CallState.ESTABLISHED
        transport.sendto(ok_bytes, resp_addr)
        log.info("Sent 200 OK  call-id=%.36s  SDP: %s:%d <-> %s:%d  "
                 "Session-Expires: %ds  refresh_interval: %ds",
                 call_id, self._local_ip, local_rtp_port, remote_host, remote_rtp_port,
                 negotiated_se, session.sip_refresh_interval)

        asyncio.create_task(self._run_call(session, transport, msg, resp_addr))

    async def _sip_session_refresh(self, session: CallSession, stop: asyncio.Event):
        """
        Send a SIP re-INVITE every SIP_SESSION_REFRESH_S seconds to tell Chime
        the session is still active. Without this, Chime's media layer drops the
        call after ~2 minutes.

        The re-INVITE must follow the Record-Route path from the original INVITE
        (reversed, as per RFC 3261 section 12.2). Sending directly to the remote
        host without routing causes Chime to respond 404 Not here.
        """
        if SIP_SESSION_REFRESH_S <= 0:
            return

        interval = getattr(session, 'sip_refresh_interval', SIP_SESSION_REFRESH_S)
        log.info("SIP session refresh task started  call-id=%.36s  "
                 "session_expires=%ds  refresh_interval=%ds",
                 session.call_id, getattr(session, 'session_expires', SESSION_EXPIRES_S),
                 interval)
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

                # Record-Route from INVITE must be REVERSED for requests we send
                # within the dialog (RFC 3261 s12.2.1.1).
                # The first Route header determines where we send the request.
                routes = list(reversed(session.sip_record_route))

                # Request-URI: use the remote contact if we have one,
                # otherwise fall back to the first Route entry stripped of lr param
                if session.sip_remote_contact:
                    # Extract bare URI from Contact header
                    c = session.sip_remote_contact
                    if "<" in c:
                        req_uri = c[c.index("<")+1:c.index(">")]
                    else:
                        req_uri = c.split(";")[0].strip()
                else:
                    req_uri = f"sip:{session.remote_host}"

                # Send to first Route hop (the Chime proxy that sent us the INVITE)
                if routes:
                    first_route = routes[0]
                    if "<" in first_route:
                        dest_uri = first_route[first_route.index("<")+1:first_route.index(">")]
                    else:
                        dest_uri = first_route.split(";")[0].strip()
                    # Parse host:port from dest_uri
                    dest_part = dest_uri.replace("sip:", "").split(";")[0]
                    if ":" in dest_part:
                        dest_host, dest_port_str = dest_part.rsplit(":", 1)
                        try:
                            dest_port = int(dest_port_str)
                        except ValueError:
                            dest_port = 5060
                    else:
                        dest_host = dest_part
                        dest_port = 5060
                    send_addr = (dest_host, dest_port)
                else:
                    send_addr = session.sip_remote_addr

                route_hdrs = "".join(f"Route: {r}\r\n" for r in routes)

                se_val = getattr(session, 'session_expires', SESSION_EXPIRES_S)
                reinvite = (
                    f"INVITE {req_uri} SIP/2.0\r\n"
                    f"Via: SIP/2.0/UDP {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                    f"From: {to_hdr}\r\n"
                    f"To: {session.sip_to_hdr}\r\n"
                    f"Call-ID: {session.call_id}\r\n"
                    f"CSeq: {session.sip_cseq} INVITE\r\n"
                    f"Contact: <sip:pbx@{self._local_ip}:{SIP_PORT}>\r\n"
                    f"Max-Forwards: 70\r\n"
                    f"Session-Expires: {se_val};refresher=uas\r\n"
                    f"Min-SE: {MIN_SE_S}\r\n"
                    f"Supported: timer\r\n"
                    f"{route_hdrs}"
                    f"Content-Type: application/sdp\r\n"
                    f"Content-Length: {len(sdp.encode())}\r\n"
                    f"\r\n"
                    f"{sdp}"
                )
                session.sip_transport.sendto(reinvite.encode(), send_addr)
                log.info(
                    "SIP session refresh (re-INVITE)  call-id=%.36s  cseq=%d  "
                    "dur=%.0fs  dest=%s:%d",
                    session.call_id, session.sip_cseq,
                    _time.monotonic() - session.call_start_ts,
                    send_addr[0], send_addr[1],
                )
            except Exception as e:
                log.warning("SIP session refresh failed: %s  call-id=%.36s",
                            e, session.call_id)

            await asyncio.sleep(interval)

        log.info("SIP session refresh task ended  call-id=%.36s", session.call_id)

    # -- Per-call lifecycle ----------------------------------------------------

    async def _run_call(self, session: CallSession, transport, invite_msg, resp_addr):
        call_id = session.call_id
        try:
            signed_url = build_signed_ws_url()
            session.url_built_ts = _time.monotonic()
        except Exception as e:
            log.error("Failed to build signed URL: %s", e)
            session.disconnect_reason = f"signed URL build failed: {e}"
            _emit_disconnect_report(session)
            await self._teardown(call_id)
            return

        stop = asyncio.Event()
        keepalive_task = None
        watchdog_task  = None
        refresh_task   = None
        rtcp_task      = None
        keepalive_task = asyncio.create_task(self._rtp_keepalive(session, stop))
        watchdog_task  = asyncio.create_task(self._call_watchdog(session, stop))
        refresh_task   = asyncio.create_task(self._sip_session_refresh(session, stop))
        rtcp_task      = asyncio.create_task(self._rtcp_keepalive(session, stop))

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
                session.ws_connect_ts = _time.monotonic()
                log.info("AgentCore WS connected  call-id=%.36s", call_id)

                _cr_json = json.dumps({
                    "label":   "rtvi-ai",
                    "type":    "client-ready",
                    "id":      "1",
                    "data":    {"version": "1.2.0"},
                })
                _cr_inner = _len_field(1, _cr_json.encode())
                _client_ready_frame = _len_field(4, _cr_inner)

                await asyncio.gather(
                    self._rtp_to_ws(session, stop),
                    self._ws_to_rtp(session, stop,
                                    client_ready_frame=_client_ready_frame),
                )
                log.info("Both media loops exited  call-id=%.36s", call_id)

        except websockets.exceptions.InvalidStatusCode as e:
            session.disconnect_reason = f"WS handshake rejected HTTP {e.status_code}"
            session.ws_close_code = e.status_code
            log.error("WS handshake rejected HTTP %d  call-id=%.36s", e.status_code, call_id)
        except websockets.exceptions.ConnectionClosedError as e:
            session.disconnect_reason = f"WS closed with error code={e.code}"
            session.ws_close_code = e.code
            session.ws_close_reason = e.reason or ""
            log.error("WS closed with error  code=%s reason='%s'  call-id=%.36s",
                      e.code, e.reason, call_id)
        except websockets.exceptions.ConnectionClosedOK as e:
            session.disconnect_reason = "WS closed cleanly (server initiated)"
            session.ws_close_code = getattr(e, 'code', 1000)
            session.ws_close_reason = getattr(e, 'reason', '') or ""
            log.info("WS closed cleanly  code=%s reason='%s'  call-id=%.36s",
                     session.ws_close_code, session.ws_close_reason, call_id)
        except Exception as e:
            session.disconnect_reason = f"unexpected error: {type(e).__name__}: {e}"
            log.error("Call error  call-id=%.36s : %s", call_id, e, exc_info=True)
        finally:
            session.disconnect_ts = _time.monotonic()
            stop.set()
            if keepalive_task is not None:
                keepalive_task.cancel()
            if watchdog_task is not None:
                watchdog_task.cancel()
            if refresh_task is not None:
                refresh_task.cancel()
            if rtcp_task is not None:
                rtcp_task.cancel()
            _emit_disconnect_report(session)
            # Send BYE if Connect dropped the call without sending one
            silent_hangup = "RTP silence" in session.disconnect_reason
            await self._teardown(call_id,
                                 send_bye=silent_hangup,
                                 bye_reason="RTP-timeout" if silent_hangup else "")

    async def _call_watchdog(self, session: CallSession, stop: asyncio.Event):
        """Log a heartbeat every 15s so we can confirm media is still flowing."""
        interval = 15.0
        while not stop.is_set():
            await asyncio.sleep(interval)
            if stop.is_set():
                break
            now = _time.monotonic()
            dur = now - session.call_start_ts
            rtp_ago = (now - session.rtp_rx_last_ts) if session.rtp_rx_last_ts else None
            ws_ago  = (now - session.ws_frames_rx_last_ts) if session.ws_frames_rx_last_ts else None

            # Warn if either direction has gone quiet unexpectedly
            rtp_warn = rtp_ago is not None and rtp_ago > RTP_SILENCE_TIMEOUT_S
            ws_warn  = ws_ago  is not None and ws_ago  > WS_IDLE_TIMEOUT_S
            rtp_never = session.rtp_rx_count == 0 and dur > 10

            level = logging.WARNING if (rtp_warn or ws_warn or rtp_never) else logging.INFO
            log.log(
                level,
                "WATCHDOG  call-id=%.36s  dur=%.0fs  "
                "rtp_rx=%d(%s)  rtp_tx=%d  ws_rx=%d(%s)  ws_tx=%d  rtvi=%s",
                session.call_id, dur,
                session.rtp_rx_count,
                f"{rtp_ago:.0f}s ago" if rtp_ago else "never",
                session.rtp_tx_count,
                session.ws_frames_rx,
                f"{ws_ago:.0f}s ago" if ws_ago else "never",
                session.ws_frames_tx,
                "ok" if session.rtvi_handshake_ok else "pending",
            )
            if rtp_never:
                log.warning(
                    "WATCHDOG [x] No RTP received after %.0fs  -  "
                    "check SG inbound UDP %d-%d and SDP IP  call-id=%.36s",
                    dur, RTP_PORT_MIN, RTP_PORT_MAX, session.call_id
                )
            if rtp_warn:
                log.warning(
                    "WATCHDOG [x] RTP from Chime silent for %.0fs  call-id=%.36s",
                    rtp_ago, session.call_id
                )
            if ws_warn:
                log.warning(
                    "WATCHDOG [x] No WS frames from AgentCore for %.0fs  call-id=%.36s",
                    ws_ago, session.call_id
                )

            # Check if signed URL is nearing expiry
            if session.url_built_ts:
                url_age = now - session.url_built_ts
                if url_age > SIGNED_URL_EXPIRY * 0.8:
                    log.warning(
                        "WATCHDOG [!] Signed URL is %.0fs old (expiry=%ds)  -  "
                        "call may disconnect soon  call-id=%.36s",
                        url_age, SIGNED_URL_EXPIRY, session.call_id
                    )

    async def _send_bye(self, session: CallSession, reason: str = ""):
        """Send an outbound SIP BYE to Chime if we have transport context."""
        if not session.sip_transport or not session.sip_remote_addr:
            return
        try:
            branch   = "z9hG4bK%08x" % random.randint(0, 0xFFFFFFFF)
            to_hdr   = session.sip_from_hdr
            if session.to_tag and "tag=" not in to_hdr:
                to_hdr = f"{to_hdr};tag={session.to_tag}"
            reason_hdr = f"Reason: SIP;cause=200;text=\"{reason}\"\r\n" if reason else ""
            bye = (
                f"BYE sip:{session.remote_host} SIP/2.0\r\n"
                f"Via: SIP/2.0/UDP {self._local_ip}:{SIP_PORT};branch={branch};rport\r\n"
                f"From: {to_hdr}\r\n"
                f"To: {session.sip_to_hdr}\r\n"
                f"Call-ID: {session.call_id}\r\n"
                f"CSeq: {session.sip_cseq} BYE\r\n"
                f"Contact: <sip:pbx@{self._local_ip}:{SIP_PORT}>\r\n"
                f"Max-Forwards: 70\r\n"
                f"{reason_hdr}"
                f"Content-Length: 0\r\n"
                f"\r\n"
            )
            session.sip_transport.sendto(bye.encode(), session.sip_remote_addr)
            log.info("Sent BYE  call-id=%.36s  reason=%s  dest=%s:%d",
                     session.call_id, reason or "normal",
                     session.sip_remote_addr[0], session.sip_remote_addr[1])
        except Exception as e:
            log.warning("Failed to send BYE  call-id=%.36s : %s", session.call_id, e)

    async def _teardown(self, call_id: str, send_bye: bool = False, bye_reason: str = ""):
        session = self.sessions.pop(call_id, None)
        if session is None:
            return
        session.state = CallState.TEARING_DOWN
        if send_bye:
            await self._send_bye(session, bye_reason)
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

    # -- RTP keepalive / NAT punch-through --------------------------------------

    async def _rtcp_keepalive(self, session, stop: asyncio.Event):
        """
        Send RTCP Sender Reports to Chime's RTCP port every 5 seconds.
        Chime's media plane monitors RTCP to determine if a session is active.
        Without RTCP SR packets, Chime treats the session as idle after ~150s
        even if RTP is flowing, and stops forwarding media.
        """
        import struct as _struct
        rtcp_port = session.remote_rtp_port + 1
        dest = (session.remote_host, rtcp_port)

        # Open a separate UDP socket for RTCP (port+1 of our RTP socket)
        try:
            rtcp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rtcp_sock.bind(("0.0.0.0", session.local_rtp_port + 1))
            rtcp_sock.setblocking(False)
        except OSError as e:
            log.warning("RTCP keepalive: could not bind RTCP socket: %s  call-id=%.36s",
                        e, session.call_id)
            return

        log.info("RTCP keepalive started  dest=%s:%d  call-id=%.36s",
                 dest[0], rtcp_port, session.call_id)

        interval = 5.0
        ntp_offset = 2208988800  # seconds between 1900 and 1970

        try:
            while not stop.is_set() and session.state == CallState.ESTABLISHED:
                await asyncio.sleep(interval)
                if stop.is_set():
                    break
                try:
                    # Build RTCP Sender Report (RFC 3550)
                    now = _time.time()
                    ntp_sec  = int(now) + ntp_offset
                    ntp_frac = int((now % 1) * (2**32))
                    rtp_ts   = session.timestamp & 0xFFFFFFFF
                    pkt_count = session.rtp_tx_count & 0xFFFFFFFF
                    byte_count = (pkt_count * 160) & 0xFFFFFFFF

                    # RTCP SR: V=2, P=0, RC=0, PT=200, length=6 (28 bytes)
                    sr = _struct.pack("!BBHIIIIII",
                        0x80,       # V=2, P=0, RC=0
                        200,        # PT=SR
                        6,          # length in 32-bit words minus 1 (28 bytes = 7 words - 1)
                        session.ssrc,
                        ntp_sec,
                        ntp_frac,
                        rtp_ts,
                        pkt_count,
                        byte_count,
                    )
                    rtcp_sock.sendto(sr, dest)
                    log.debug("RTCP SR sent  dest=%s:%d  ssrc=%08x  pkts=%d  call-id=%.36s",
                              dest[0], rtcp_port, session.ssrc,
                              pkt_count, session.call_id)
                except OSError as e:
                    log.debug("RTCP keepalive sendto: %s", e)
        finally:
            try:
                rtcp_sock.close()
            except Exception:
                pass
            log.info("RTCP keepalive ended  call-id=%.36s", session.call_id)

    async def _rtp_keepalive(self, session, stop):
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
            rtp_queue = getattr(session, '_rtp_out_queue', None)
            if rtp_queue is not None and not rtp_queue.empty():
                continue
            if getattr(session, 'suppress_keepalive', False):
                session.suppress_keepalive = False
                continue
            try:
                session.rtp_socket.sendto(_pkt(), (session.remote_host, session.remote_rtp_port))
            except OSError:
                break

        log.info("RTP keepalive ended  call-id=%.36s", session.call_id)

    # -- RTP -> WebSocket -------------------------------------------------------

    async def _rtp_to_ws(self, session: CallSession, stop: asyncio.Event):
        loop   = asyncio.get_running_loop()
        sock   = session.rtp_socket
        queue: asyncio.Queue = asyncio.Queue()

        def _readable():
            try:
                data, _ = sock.recvfrom(4096)
                queue.put_nowait(data)
            except BlockingIOError:
                pass
            except Exception as exc:
                log.warning("RTP socket read error: %s", exc)

        if sock.fileno() == -1 or session.state == CallState.TEARING_DOWN:
            log.info("RTP->WS: socket already closed, skipping  call-id=%.36s", session.call_id)
            stop.set()
            return

        loop.add_reader(sock.fileno(), _readable)
        log.info("RTP->WS started  call-id=%.36s", session.call_id)

        no_rtp_warned = False

        try:
            while not stop.is_set() and session.state != CallState.TEARING_DOWN:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=5.0)
                except asyncio.TimeoutError:
                    if session.rtp_rx_count == 0:
                        if not no_rtp_warned:
                            no_rtp_warned = True
                            log.warning(
                                "RTP->WS: NO RTP from Chime yet after 5s  call-id=%.36s"
                                " -- check: (1) security group UDP %d-%d open inbound,"
                                " (2) SDP advertises local IP %s -- on EC2 this must be"
                                " the PRIVATE interface IP (e.g. 172.31.x.x), NOT the"
                                " public/elastic IP. Unset LOCAL_IP if it is set to a public IP."
                                " (3) tcpdump -i any udp port %d shows inbound packets",
                                session.call_id, RTP_PORT_MIN, RTP_PORT_MAX,
                                session.remote_host, session.local_rtp_port
                            )
                    else:
                        dur_since = _time.monotonic() - session.rtp_rx_last_ts
                        if dur_since > RTP_SILENCE_TIMEOUT_S:
                            log.warning(
                                "RTP->WS: RTP silence for %.0fs  total_rx=%d  call-id=%.36s",
                                dur_since, session.rtp_rx_count, session.call_id
                            )
                        if RTP_SILENCE_HANGUP_S > 0 and dur_since > RTP_SILENCE_HANGUP_S:
                            # Only treat as hangup if the WS is ALSO idle.
                            # If AgentCore is still sending frames the user is
                            # just listening quietly (bot speaking, function call
                            # running, etc.) -- do NOT cut the call.
                            ws_idle = (
                                session.ws_frames_rx_last_ts is None or
                                (_time.monotonic() - session.ws_frames_rx_last_ts) > RTP_SILENCE_HANGUP_S
                            )
                            if ws_idle:
                                log.warning(
                                    "RTP->WS: RTP silent for %.0fs AND WS idle "
                                    ">= hangup threshold %ds "
                                    "-- treating as remote hangup, tearing down  call-id=%.36s",
                                    dur_since, RTP_SILENCE_HANGUP_S, session.call_id
                                )
                                session.disconnect_reason = (
                                    f"RTP silence {dur_since:.0f}s -- Connect hangup without BYE"
                                )
                                stop.set()
                                break
                            else:
                                ws_ago = _time.monotonic() - session.ws_frames_rx_last_ts
                                log.debug(
                                    "RTP->WS: RTP silent for %.0fs but WS active "
                                    "(last frame %.0fs ago) -- not a hangup  call-id=%.36s",
                                    dur_since, ws_ago, session.call_id
                                )
                    continue

                parsed = parse_rtp(data)
                if parsed is None:
                    log.debug("RTP->WS: unparseable packet (%d bytes) dropped", len(data))
                    continue

                ptype, seq, _ts, _ssrc, payload = parsed
                session.payload_type = ptype
                session.rtp_rx_count += 1
                session.rtp_rx_last_ts = _time.monotonic()

                if session.rtp_rx_count == 1:
                    log.info("RTP->WS: first RTP packet  ptype=%d seq=%d payload=%d bytes  call-id=%.36s",
                             ptype, seq, len(payload), session.call_id)

                # Log every 500 packets so we can see the stream is alive
                if session.rtp_rx_count % 500 == 0:
                    log.debug("RTP->WS: %d packets received  call-id=%.36s",
                              session.rtp_rx_count, session.call_id)

                if ptype == RTP_PAYLOAD_PCMU:
                    pcm8 = audioop.ulaw2lin(payload, 2)
                elif ptype == RTP_PAYLOAD_PCMA:
                    pcm8 = audioop.alaw2lin(payload, 2)
                else:
                    log.debug("RTP->WS: unsupported payload type %d  -  skipping", ptype)
                    continue

                pcm16 = session.pcm8k_to_16k(pcm8)

                try:
                    frame = await pack_audio_async(pcm16)
                    await session.ws.send(frame)
                    session.ws_frames_tx += 1
                except websockets.exceptions.ConnectionClosed as e:
                    session.ws_close_code = e.code
                    session.ws_close_reason = e.reason or ""
                    if not session.disconnect_reason or session.disconnect_reason == "unknown":
                        session.disconnect_reason = f"WS closed while sending RTP audio code={e.code}"
                    log.warning(
                        "RTP->WS: WS closed while sending audio  code=%s reason='%s'  "
                        "rtp_rx=%d  call-id=%.36s",
                        e.code, e.reason, session.rtp_rx_count, session.call_id
                    )
                    break
                except Exception as e:
                    if not session.disconnect_reason or session.disconnect_reason == "unknown":
                        session.disconnect_reason = f"WS send error: {e}"
                    log.warning("RTP->WS: WS send error  call-id=%.36s : %s", session.call_id, e)
                    break

        finally:
            try:
                fd = sock.fileno()
                if fd != -1:
                    loop.remove_reader(fd)
            except Exception:
                pass
            log.info("RTP->WS ended  call-id=%.36s  total_rtp_rx=%d  ws_tx=%d",
                     session.call_id, session.rtp_rx_count, session.ws_frames_tx)
            stop.set()

    # -- WebSocket -> RTP -------------------------------------------------------

    async def _ws_to_rtp(self, session: CallSession, stop: asyncio.Event,
                          client_ready_frame: bytes = b""):
        log.info("WS->RTP started  call-id=%.36s", session.call_id)
        client_ready_sent = False

        # Use a thread-safe queue so the RTP output thread can drain it
        # independently of the asyncio event loop. This is critical --
        # the previous asyncio-based output task shared the event loop with
        # WS frame processing and starved when the agent sent lots of audio,
        # causing rtp_tx to fall far behind rtp_rx and Chime to drop the call.
        import queue as _queue
        import threading as _threading

        rtp_out_queue_sync: _queue.Queue = _queue.Queue(maxsize=400)

        # Keep asyncio queue interface for the keepalive suppression check
        class _QueueShim:
            def empty(self):
                return rtp_out_queue_sync.empty()
        session._rtp_out_queue = _QueueShim()

        PACKET_INTERVAL = CHIME_FRAME_MS / 1000.0
        stop_thread = _threading.Event()

        def _rtp_output_thread():
            """Dedicated OS thread for metered RTP output. Uses time.sleep
            which yields to the OS scheduler, giving much tighter 20ms pacing
            than asyncio.sleep which can drift under event loop load."""
            next_tx = _time.monotonic()
            sock   = session.rtp_socket
            dest   = (session.remote_host, session.remote_rtp_port)
            log.info("RTP output thread started  call-id=%.36s", session.call_id)
            while not stop_thread.is_set():
                now  = _time.monotonic()
                wait = next_tx - now
                if wait > 0.0005:
                    _time.sleep(wait)
                elif wait < -0.200:
                    # Fell badly behind -- reset clock to avoid burst
                    next_tx = _time.monotonic()

                try:
                    chunk = rtp_out_queue_sync.get_nowait()
                except _queue.Empty:
                    next_tx += PACKET_INTERVAL
                    continue

                pkt = session.next_rtp_header() + chunk
                try:
                    sock.sendto(pkt, dest)
                    session.rtp_tx_count += 1
                    session.rtp_tx_last_ts = _time.monotonic()
                except OSError as e:
                    log.warning("RTP output thread: sendto failed: %s", e)
                    break

                next_tx += PACKET_INTERVAL

            log.info("RTP output thread ended  call-id=%.36s  rtp_tx=%d",
                     session.call_id, session.rtp_tx_count)

        rtp_thread = _threading.Thread(
            target=_rtp_output_thread,
            name=f"rtp-out-{session.call_id[:8]}",
            daemon=True,
        )
        rtp_thread.start()

        try:
            async for message in session.ws:
                if stop.is_set() or session.state != CallState.ESTABLISHED:
                    break

                if isinstance(message, bytes):
                    session.ws_frames_rx += 1
                    session.ws_frames_rx_last_ts = _time.monotonic()

                    outer_field = (message[0] >> 3) if message else 0
                    if outer_field == 2:
                        audio_frame_count = getattr(session, '_audio_frame_count', 0) + 1
                        session._audio_frame_count = audio_frame_count
                        if audio_frame_count <= 3:
                            log.info("WS AUDIO FRAME #%d  %d bytes  call-id=%.36s",
                                     audio_frame_count, len(message), session.call_id)
                    elif session.ws_frames_rx <= 10:
                        log.info("WS CTRL FRAME #%d  %d bytes  hex=%s  call-id=%.36s",
                                 session.ws_frames_rx, len(message),
                                 message[:32].hex(), session.call_id)

                    # -- RTVI handshake ----------------------------------------
                    if not client_ready_sent and client_ready_frame:
                        try:
                            await session.ws.send(client_ready_frame)
                            client_ready_sent = True
                            session.ws_frames_tx += 1
                            log.info("RTVI client-ready sent (after bot-ready)  call-id=%.36s",
                                     session.call_id)
                        except Exception as e:
                            log.warning("RTVI client-ready send failed: %s  call-id=%.36s",
                                        e, session.call_id)

                    result = await unpack_audio_async(message)
                    if result is None:
                        # Log what we can about unrecognised frames to help debug
                        if session.ws_frames_rx <= 20:
                            # Try to decode as JSON / RTVI text
                            try:
                                text_attempt = message.decode("utf-8", errors="replace")
                                if "{" in text_attempt:
                                    log.info(
                                        "WS->RTP: non-audio binary frame #%d (possible RTVI JSON)  "
                                        "%d bytes  text=%.200s  call-id=%.36s",
                                        session.ws_frames_rx, len(message),
                                        text_attempt, session.call_id
                                    )
                                else:
                                    log.debug(
                                        "WS->RTP: non-audio binary frame #%d  %d bytes  "
                                        "header=%s  call-id=%.36s",
                                        session.ws_frames_rx, len(message),
                                        message[:16].hex(), session.call_id
                                    )
                            except Exception:
                                pass
                        continue

                    pcm16, agent_sr = result

                    # Mark RTVI as complete once we receive real audio
                    if not session.rtvi_handshake_ok and pcm16:
                        session.rtvi_handshake_ok = True
                        log.info("RTVI handshake complete  -  first audio from agent  call-id=%.36s",
                                 session.call_id)

                    if not pcm16:
                        continue

                    if len(pcm16) < 200:
                        log.debug("WS->RTP: skipping tiny pseudo-audio frame  "
                                  "pcm_bytes=%d  sr=%d  call-id=%.36s",
                                  len(pcm16), agent_sr, session.call_id)
                        continue

                    printable = sum(32 <= b < 127 for b in pcm16[:32])
                    if printable > 24:
                        log.debug("WS->RTP: skipping text-like pseudo-audio frame  "
                                  "pcm_bytes=%d  call-id=%.36s", len(pcm16), session.call_id)
                        continue

                    if session.rtp_tx_count == 0:
                        log.info("WS->RTP: first REAL audio frame  pcm_bytes=%d  sample_rate=%d  call-id=%.36s",
                                 len(pcm16), agent_sr, session.call_id)

                    if len(pcm16) % 2:
                        pcm16 = pcm16[:-1]
                    if not pcm16:
                        continue

                    src_rate = agent_sr if agent_sr and agent_sr > 0 else AGENT_SAMPLE_RATE
                    if not hasattr(session, '_ws_src_rate') or session._ws_src_rate != src_rate:
                        session._ws_src_rate = src_rate
                        session._ws_rs_state = None
                        log.info("WS->RTP: resampler reset  src=%dHz->8kHz  call-id=%.36s",
                                 src_rate, session.call_id)
                    if src_rate != CHIME_SAMPLE_RATE:
                        pcm8, session._ws_rs_state = audioop.ratecv(
                            pcm16, 2, 1, src_rate, CHIME_SAMPLE_RATE, session._ws_rs_state
                        )
                    else:
                        pcm8 = pcm16

                    encoded = (
                        audioop.lin2alaw(pcm8, 2)
                        if session.payload_type == RTP_PAYLOAD_PCMA
                        else audioop.lin2ulaw(pcm8, 2)
                    )

                    for i in range(0, len(encoded), CHIME_FRAME_SAMPLES):
                        chunk = encoded[i : i + CHIME_FRAME_SAMPLES]
                        if not chunk:
                            break
                        if len(chunk) < CHIME_FRAME_SAMPLES:
                            chunk = chunk + bytes(CHIME_FRAME_SAMPLES - len(chunk))
                        try:
                            rtp_out_queue_sync.put_nowait(chunk)
                        except _queue.Full:
                            pass  # drop if output is backed up
                        session.suppress_keepalive = True

                elif isinstance(message, str):
                    session.ws_frames_rx += 1
                    session.ws_frames_rx_last_ts = _time.monotonic()
                    log.info("WS ctrl (text)  call-id=%.36s : %.300s", session.call_id, message)
                    # Try to parse as JSON for richer logging
                    try:
                        ctrl = json.loads(message)
                        msg_type = ctrl.get("type", "?")
                        log.info("WS ctrl parsed  type=%s  call-id=%.36s  data=%.200s",
                                 msg_type, session.call_id, str(ctrl.get("data", "")))
                        if msg_type in ("bot-ready", "client-ready"):
                            log.info("RTVI event: %s  call-id=%.36s", msg_type, session.call_id)
                    except Exception:
                        pass

        except websockets.exceptions.ConnectionClosedError as e:
            if not session.disconnect_reason or session.disconnect_reason == "unknown":
                session.disconnect_reason = f"WS closed error in ws_to_rtp code={e.code}"
            session.ws_close_code = e.code
            session.ws_close_reason = e.reason or ""
            log.warning("WS->RTP: WS closed with error  code=%s reason='%s'  call-id=%.36s",
                        e.code, e.reason, session.call_id)
        except websockets.exceptions.ConnectionClosedOK as e:
            if not session.disconnect_reason or session.disconnect_reason == "unknown":
                session.disconnect_reason = "WS closed cleanly (server)"
            session.ws_close_code = getattr(e, 'code', 1000)
            session.ws_close_reason = getattr(e, 'reason', '') or ""
            log.info("WS->RTP: WS closed cleanly  code=%s  call-id=%.36s",
                     session.ws_close_code, session.call_id)
        except Exception as e:
            if not session.disconnect_reason or session.disconnect_reason == "unknown":
                session.disconnect_reason = f"WS->RTP error: {type(e).__name__}: {e}"
            log.error("WS->RTP: unexpected error  call-id=%.36s : %s",
                      session.call_id, e, exc_info=True)
        finally:
            stop_thread.set()
            rtp_thread.join(timeout=1.0)
            log.info("WS->RTP ended  call-id=%.36s  ws_frames_rx=%d  rtp_packets_tx=%d",
                     session.call_id, session.ws_frames_rx, session.rtp_tx_count)
            stop.set()


# -- Outbound SIP OPTIONS keepalive -------------------------------------------

async def _send_options_ping(transport: asyncio.DatagramTransport, local_ip: str):
    if not CHIME_VC_FQDN:
        log.warning("CHIME_VC_FQDN not set  -  OPTIONS pings disabled.\n"
                    "  Set CHIME_VC_FQDN=<your-vc>.voiceconnector.chime.aws")
        return

    log.info("OPTIONS keepalive target: %s:5060", CHIME_VC_FQDN)
    seq      = random.randint(1, 9999)
    call_id  = "%08x@%s" % (random.randint(0, 0xFFFFFFFF), local_ip)

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
            f"Content-Length: 0\r\n"
            f"\r\n"
        )
        try:
            loop   = asyncio.get_event_loop()
            infos  = await loop.getaddrinfo(
                CHIME_VC_FQDN, 5060,
                family=socket.AF_INET, type=socket.SOCK_DGRAM
            )
            dest_ip = infos[0][4][0]
            transport.sendto(msg.encode(), (dest_ip, 5060))
            log.debug("OPTIONS ping -> %s (%s)", CHIME_VC_FQDN, dest_ip)
        except Exception as e:
            log.warning("OPTIONS ping failed: %s", e)
        seq += 1
        await asyncio.sleep(OPTIONS_INTERVAL)


# -- asyncio UDP / SIP protocol ------------------------------------------------

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


# -- TCP/TLS SIP listener ------------------------------------------------------

SIP_TLS_PORT = int(os.getenv("SIP_TLS_PORT", "0"))
SIP_TLS_CERT = os.getenv("SIP_TLS_CERT", "")
SIP_TLS_KEY  = os.getenv("SIP_TLS_KEY",  "")


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


# -- Health-check HTTP server --------------------------------------------------

async def _health(reader, writer, bridge: PbxBridge):
    try:
        await reader.read(1024)
        sessions_info = []
        for cid, s in bridge.sessions.items():
            dur = _time.monotonic() - s.call_start_ts
            sessions_info.append({
                "call_id": cid,
                "state": s.state.name,
                "duration_s": round(dur, 1),
                "rtp_rx": s.rtp_rx_count,
                "rtp_tx": s.rtp_tx_count,
                "ws_rx": s.ws_frames_rx,
                "rtvi_ok": s.rtvi_handshake_ok,
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


# -- Entry point ---------------------------------------------------------------

async def main():
    log.info("Starting PBX Bridge v2 (disconnect diagnostics enabled)")
    log.info("  AGENT_RUNTIME_ARN      : %s", AGENT_RUNTIME_ARN or "(not set - LOCAL_AGENT mode)")
    log.info("  AWS_REGION             : %s", AWS_REGION)
    log.info("  SIGNED_URL_EXPIRY      : %ds", SIGNED_URL_EXPIRY)
    log.info("  RTP_SILENCE_TIMEOUT_S  : %ds", RTP_SILENCE_TIMEOUT_S)
    log.info("  WS_IDLE_TIMEOUT_S      : %ds", WS_IDLE_TIMEOUT_S)
    log.info("  DISCONNECT_LOG_FILE    : %s", _DISCONNECT_LOG_FILE or "(stdout only)")
    log.info("")
    log.info("  Security group must allow UDP %d (SIP) + UDP %d-%d (RTP) inbound.",
             SIP_PORT, RTP_PORT_MIN, RTP_PORT_MAX)

    bridge = PbxBridge()
    loop   = asyncio.get_running_loop()

    sip_transport, _ = await loop.create_datagram_endpoint(
        lambda: SipProtocol(bridge),
        local_addr=(SIP_HOST, SIP_PORT),
    )

    servers = []

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
        log.info("  SIP TLS listener: tls://0.0.0.0:%d", SIP_TLS_PORT)
    elif SIP_TLS_PORT:
        log.warning("  SIP_TLS_PORT=%d set but SIP_TLS_CERT/KEY missing  -  TLS listener not started",
                    SIP_TLS_PORT)

    health_server = await asyncio.start_server(
        lambda r, w: _health(r, w, bridge),
        "0.0.0.0", HTTP_HEALTH_PORT,
    )
    servers.append(health_server)

    log.info("PBX Bridge ready.")
    log.info("  SIP UDP  udp://%s:%d", SIP_HOST, SIP_PORT)
    log.info("  RTP      %d - %d", RTP_PORT_MIN, RTP_PORT_MAX)
    log.info("  Health   http://0.0.0.0:%d/", HTTP_HEALTH_PORT)

    tasks = [asyncio.create_task(srv.serve_forever()) for srv in servers]
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