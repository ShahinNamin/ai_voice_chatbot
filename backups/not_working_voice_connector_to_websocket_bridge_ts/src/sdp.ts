/**
 * Minimal SDP (Session Description Protocol) parser and builder.
 *
 * Used to:
 *  - Parse the SDP body in a SIP INVITE to find the caller's RTP IP/port and codec.
 *  - Build a SDP answer advertising our local RTP port.
 *
 * We support the codecs Chime Voice Connector sends: PCMU (0), PCMA (8).
 */

export interface SdpMediaDescription {
  port: number;
  protocols: string;
  payloadTypes: number[];
}

export interface ParsedSdp {
  sessionLines: string[];
  originIp: string;         // o= field IP
  connectionIp: string;     // c= field IP (remote RTP destination)
  media: SdpMediaDescription;
  payloadType: number;      // chosen codec payload type (0=PCMU, 8=PCMA)
  codecName: string;        // "PCMU" or "PCMA"
  sampleRate: number;
  rtcpMux: boolean;
}

/**
 * Parse a raw SDP string into a structured object.
 */
export function parseSdp(sdpText: string): ParsedSdp {
  const lines = sdpText.split(/\r?\n/).map((l) => l.trim()).filter(Boolean);

  let originIp = "";
  let connectionIp = "";
  let mediaPort = 0;
  let mediaProtocols = "RTP/AVP";
  let payloadTypes: number[] = [];
  let chosenPayload = 0;
  let codecName = "PCMU";
  let sampleRate = 8000;

  for (const line of lines) {
    // o= (origin)
    if (line.startsWith("o=")) {
      const parts = line.slice(2).split(" ");
      originIp = parts[5] ?? "";
    }

    // c= (connection / remote RTP IP)
    if (line.startsWith("c=")) {
      const parts = line.slice(2).split(" ");
      connectionIp = parts[2] ?? "";
    }

    // m= (media)
    if (line.startsWith("m=audio")) {
      const parts = line.slice(2).split(" ");
      mediaPort = parseInt(parts[1], 10);
      mediaProtocols = parts[2];
      payloadTypes = parts.slice(3).map(Number);
    }

    // a=rtpmap:<pt> <codec>/<rate>
    if (line.startsWith("a=rtpmap:")) {
      const m = line.match(/^a=rtpmap:(\d+)\s+(\w+)\/(\d+)/);
      if (m) {
        const pt = parseInt(m[1], 10);
        const cn = m[2].toUpperCase();
        const sr = parseInt(m[3], 10);
        if ((cn === "PCMU" || cn === "PCMA") && payloadTypes.includes(pt)) {
          chosenPayload = pt;
          codecName = cn;
          sampleRate = sr;
        }
      }
    }
  }

  // Fall-back: if no rtpmap line, derive from static payload type
  if (!chosenPayload && payloadTypes.length > 0) {
    // PT 0 = PCMU, PT 8 = PCMA per RFC 3551
    if (payloadTypes.includes(0)) {
      chosenPayload = 0; codecName = "PCMU"; sampleRate = 8000;
    } else if (payloadTypes.includes(8)) {
      chosenPayload = 8; codecName = "PCMA"; sampleRate = 8000;
    } else {
      chosenPayload = payloadTypes[0];
    }
  }

  // Chime sometimes puts 0.0.0.0 in the c= line and the real IP in o=
  // Fall back to the origin IP in that case
  const resolvedIp = (!connectionIp || connectionIp === "0.0.0.0")
    ? originIp
    : connectionIp;

  // Detect rtcp-mux — Chime requires we echo this back in our SDP answer
  const rtcpMux = lines.some((l: string) => l.trim() === "a=rtcp-mux");

  return {
    sessionLines: lines,
    originIp,
    connectionIp: resolvedIp,
    media: { port: mediaPort, protocols: mediaProtocols, payloadTypes },
    payloadType: chosenPayload,
    codecName,
    sampleRate,
    rtcpMux,
  };
}

/**
 * Build an SDP answer for a 200 OK response.
 *
 * @param localIp      Public/local IP of this bridge server.
 * @param localRtpPort RTP port we will listen on for incoming audio.
 * @param offer        Parsed offer SDP (so we can mirror codec selection).
 */
export function buildSdpAnswer(
  localIp: string,
  localRtpPort: number,
  offer: ParsedSdp
): string {
  const ts = Math.floor(Date.now() / 1000);
  const lines = [
    "v=0",
    `o=bridge ${ts} ${ts} IN IP4 ${localIp}`,
    "s=Chime-AgentCore Bridge",
    `c=IN IP4 ${localIp}`,
    "t=0 0",
    // Include both PCMU (0) and telephone-event (101) to match Chime's offer
    `m=audio ${localRtpPort} RTP/AVP ${offer.payloadType} 101`,
    `a=rtpmap:${offer.payloadType} ${offer.codecName}/${offer.sampleRate}`,
    "a=rtpmap:101 telephone-event/8000",
    "a=fmtp:101 0-15",
    "a=sendrecv",
    "a=ptime:20",
    // Echo rtcp-mux if offered — required for Chime to start sending RTP
    ...(offer.rtcpMux ? ["a=rtcp-mux"] : [`a=rtcp:${localRtpPort + 1}`]),
    "",
  ];
  return lines.join("\r\n");
}