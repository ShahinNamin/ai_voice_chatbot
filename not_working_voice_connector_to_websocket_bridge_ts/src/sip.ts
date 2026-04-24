/**
 * Minimal SIP message parser and response builder.
 *
 * Handles the subset of SIP messages that Amazon Chime Voice Connector sends:
 *   INVITE  – incoming call, contains SDP offer
 *   ACK     – acknowledgement of 200 OK
 *   BYE     – call termination
 *   OPTIONS – keep-alive / health check
 *   CANCEL  – call cancellation
 */

// ─── Types ───────────────────────────────────────────────────────────────────

export type SipMethod = "INVITE" | "ACK" | "BYE" | "CANCEL" | "OPTIONS" | "REGISTER";

export interface SipMessage {
  raw: string;
  isRequest: boolean;
  method?: SipMethod;
  requestUri?: string;
  statusCode?: number;
  reasonPhrase?: string;
  headers: Map<string, string>;
  body: string;
  callId: string;
  from: string;
  to: string;
  via: string;
  cseq: string;
  contact?: string;
  contentType?: string;
  contentLength: number;
  recordRoutes: string[]; // all Record-Route headers in order
}

// ─── Parser ──────────────────────────────────────────────────────────────────

export function parseSipMessage(raw: string): SipMessage | null {
  try {
    const crlfIndex = raw.indexOf("\r\n\r\n");
    const headerSection = crlfIndex >= 0 ? raw.slice(0, crlfIndex) : raw;
    const body = crlfIndex >= 0 ? raw.slice(crlfIndex + 4) : "";

    const rawLines = headerSection.split("\r\n");
    if (rawLines.length === 0) return null;

    const firstLine = rawLines[0].trim();
    let isRequest = false;
    let method: SipMethod | undefined;
    let requestUri: string | undefined;
    let statusCode: number | undefined;
    let reasonPhrase: string | undefined;

    if (firstLine.startsWith("SIP/")) {
      // Response: SIP/2.0 200 OK
      const m = firstLine.match(/^SIP\/2\.0\s+(\d+)\s+(.*)/);
      if (!m) return null;
      statusCode = parseInt(m[1], 10);
      reasonPhrase = m[2];
    } else {
      // Request: INVITE sip:... SIP/2.0
      const m = firstLine.match(/^(\w+)\s+(\S+)\s+SIP\/2\.0/);
      if (!m) return null;
      isRequest = true;
      method = m[1] as SipMethod;
      requestUri = m[2];
    }

    // Parse headers (handle multi-line / compact forms)
    // Use set-first semantics for Via: SIP stacks add Via headers in order,
    // so the first Via is the topmost (most recent hop) — that is the one
    // we must echo back in responses. All other duplicate headers use last-wins.
    const headers = new Map<string, string>();
    const recordRoutes: string[] = [];
    for (let i = 1; i < rawLines.length; i++) {
      const line = rawLines[i];
      if (!line) continue;
      const colonIdx = line.indexOf(":");
      if (colonIdx < 0) continue;
      const name = normalizeHeaderName(line.slice(0, colonIdx).trim().toLowerCase());
      const value = line.slice(colonIdx + 1).trim();
      // Collect ALL Record-Route headers in order
      if (name === "record-route") {
        recordRoutes.push(value);
        continue;
      }
      // For Via: only keep the first occurrence (topmost hop)
      if (name === "via" && headers.has("via")) continue;
      headers.set(name, value);
    }

    const contentLength = parseInt(headers.get("content-length") ?? "0", 10);

    return {
      raw,
      isRequest,
      method,
      requestUri,
      statusCode,
      reasonPhrase,
      headers,
      body,
      callId: headers.get("call-id") ?? "",
      from: headers.get("from") ?? "",
      to: headers.get("to") ?? "",
      via: headers.get("via") ?? "",
      cseq: headers.get("cseq") ?? "",
      contact: headers.get("contact"),
      contentType: headers.get("content-type"),
      contentLength,
      recordRoutes,
    };
  } catch {
    return null;
  }
}

// Compact SIP header name → long form
function normalizeHeaderName(name: string): string {
  const compact: Record<string, string> = {
    f: "from",
    t: "to",
    v: "via",
    i: "call-id",
    m: "contact",
    c: "content-type",
    l: "content-length",
    s: "subject",
    e: "content-encoding",
  };
  return compact[name] ?? name;
}

// ─── Response builder ────────────────────────────────────────────────────────

export interface SipResponseOptions {
  statusCode: number;
  reasonPhrase: string;
  request: SipMessage;
  localIp: string;
  localPort: number;
  remoteIp?: string;   // actual source IP of the request — used to fix Via header
  remotePort?: number; // actual source port of the request
  body?: string;
  contentType?: string;
  toTag?: string;
  contact?: string;
}

/**
 * Build a SIP response message as a string ready to send over UDP.
 */
export function buildSipResponse(opts: SipResponseOptions): string {
  const {
    statusCode,
    reasonPhrase,
    request,
    localIp,
    localPort,
    remoteIp,
    remotePort,
    body = "",
    contentType,
    toTag,
    contact,
  } = opts;

  // Add tag to To header for dialog establishment (required by RFC 3261 for non-100 responses)
  let toHeader = request.to;
  if (toTag && !toHeader.includes(";tag=")) {
    toHeader = `${toHeader};tag=${toTag}`;
  }

  // Leave the Via header exactly as received — RFC 3261 says responses
  // must echo the Via unchanged. The ACK will be routed using our Contact
  // header (which uses PUBLIC_IP), not the Via.
  const viaHeader = request.via;

  const lines: string[] = [
    `SIP/2.0 ${statusCode} ${reasonPhrase}`,
    `Via: ${viaHeader}`,
  ];

  // Echo Record-Route headers back in the same order — required by RFC 3261
  // so the proxy (Chime) can route the ACK back through its own proxy chain.
  for (const rr of request.recordRoutes) {
    lines.push(`Record-Route: ${rr}`);
  }

  lines.push(
    `From: ${request.from}`,
    `To: ${toHeader}`,
    `Call-ID: ${request.callId}`,
    `CSeq: ${request.cseq}`,
  );

  if (contact) {
    // Include the request-URI user part and transport=UDP in Contact
    // so Chime knows exactly where to send the ACK
    const toMatch = request.requestUri?.match(/sip:([^@;>]+)/);
    const userPart = toMatch ? toMatch[1] + "@" : "";
    lines.push(`Contact: <sip:${userPart}${localIp}:${localPort};transport=UDP>`);
  }

  if (body && contentType) {
    lines.push(`Content-Type: ${contentType}`);
    lines.push(`Content-Length: ${Buffer.byteLength(body, "utf8")}`);
  } else {
    lines.push("Content-Length: 0");
  }

  lines.push("");
  if (body) lines.push(body);

  return lines.join("\r\n");
}

/**
 * Build a SIP BYE request (to terminate a call).
 */
export function buildSipBye(opts: {
  callId: string;
  from: string;
  to: string;
  cseqNum: number;
  targetUri: string;
  localIp: string;
  localPort: number;
}): string {
  const branch = `z9hG4bK${Math.random().toString(36).slice(2)}`;
  const lines = [
    `BYE ${opts.targetUri} SIP/2.0`,
    `Via: SIP/2.0/UDP ${opts.localIp}:${opts.localPort};branch=${branch}`,
    `From: ${opts.from}`,
    `To: ${opts.to}`,
    `Call-ID: ${opts.callId}`,
    `CSeq: ${opts.cseqNum} BYE`,
    "Content-Length: 0",
    "",
    "",
  ];
  return lines.join("\r\n");
}

/**
 * Generate a random SIP tag (8 hex chars).
 */
export function generateTag(): string {
  return Math.random().toString(16).slice(2, 10);
}