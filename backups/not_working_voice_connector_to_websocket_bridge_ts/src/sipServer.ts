/**
 * SIP Server (User Agent Server)
 *
 * Listens on UDP port 5060 (configurable) for SIP messages from
 * Amazon Chime SDK Voice Connector.
 *
 * Handles:
 *  - INVITE  → responds with 100 Trying, allocates RTP port, starts AgentCore
 *              WS, responds 200 OK with SDP answer, waits for ACK
 *  - ACK     → call is now fully established, audio flows
 *  - BYE     → tears down call session
 *  - CANCEL  → cancels a pending INVITE
 *  - OPTIONS → responds 200 OK (keep-alive)
 */

import * as dgram from "dgram";

import * as os from "os";
import EventEmitter = require("events");
import {
  parseSipMessage,
  buildSipResponse,
  buildSipBye,
  generateTag,
  SipMessage,
} from "./sip";
import { parseSdp, buildSdpAnswer } from "./sdp";
import { CallSession } from "./callSession";
import { RtpPortManager } from "./rtpPortManager";
import { logger } from "./logger";

// Declare require for CommonJS interop (minimal @types/node doesn't include it)
declare const require: (mod: string) => any; // eslint-disable-line @typescript-eslint/no-explicit-any

// ─── Internal call state ─────────────────────────────────────────────────────

interface PendingCall {
  invite: SipMessage;
  toTag: string;
  localRtpPort: number;
  remoteIp: string;
  remotePort: number;
}

// ─── SIP Server ──────────────────────────────────────────────────────────────

export class SipServer extends EventEmitter {
  private socket: dgram.Socket;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private tcpServer: any = null;
  private readonly sipPort: number;
  private localIp: string;
  private readonly portManager: RtpPortManager;

  // callId → active CallSession
  private activeSessions = new Map<string, CallSession>();

  // callId → pending (INVITE received, ACK not yet received)
  private pendingCalls = new Map<string, PendingCall>();

  constructor(sipPort: number, localIp: string, portManager: RtpPortManager) {
    super();
    this.sipPort = sipPort;
    this.localIp = localIp;
    this.portManager = portManager;
    this.socket = dgram.createSocket("udp4");
  }

  updateLocalIp(ip: string): void {
    this.localIp = ip;
    logger.debug(`SipServer local IP updated to ${ip}`);
  }

  async start(): Promise<void> {
    await this.startUdp();
    await this.startTcp();
  }

  private startUdp(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket.on("error", (err) => {
        logger.error("SIP UDP socket error", err);
        reject(err);
      });

      this.socket.on("message", (msg, rinfo) => {
        const raw = msg.toString("utf8");
        logger.debug(`SIP UDP ← ${rinfo.address}:${rinfo.port}\n${raw.slice(0, 300)}`);
        this.handleMessage(raw, rinfo.address, rinfo.port);
      });

      this.socket.bind(this.sipPort, () => {
        logger.info(`SIP server listening on UDP port ${this.sipPort}`);
        resolve();
      });
    });
  }

  private async startTcp(): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any, @typescript-eslint/no-var-requires
    const { startTcpSipServer } = require("./tcpServer") as any;
    this.tcpServer = await startTcpSipServer(
      this.sipPort,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      (msg: string, remoteIp: string, remotePort: number, tcpSend: (data: string) => void) => {
        logger.debug(`SIP TCP ← ${remoteIp}:${remotePort}\n${msg.slice(0, 300)}`);
        // Pass tcpSend so responses go back on the same TCP socket
        this.handleMessage(msg, remoteIp, remotePort, tcpSend);
      },
      (err: Error) => logger.error("SIP TCP server error", err)
    );
    logger.info(`SIP server listening on TCP port ${this.sipPort}`);
  }


  stop(): void {
    try {
      this.socket.close();
    } catch {
      // ignore
    }
    // End all active sessions
    for (const [callId, session] of this.activeSessions) {
      session.end().catch((e) => logger.warn(`Error ending session ${callId}`, e));
    }
  }

  // ─── Message dispatch ───────────────────────────────────────────────────

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private handleMessage(raw: string, remoteIp: string, remotePort: number, tcpSend?: (data: string) => void): void {
    const msg = parseSipMessage(raw);
    if (!msg) {
      logger.warn(`Could not parse SIP message from ${remoteIp}:${remotePort}`);
      return;
    }

    if (!msg.isRequest) {
      // We might receive responses to our BYE requests – ignore them
      logger.debug(`Ignoring SIP response ${msg.statusCode} for call ${msg.callId}`);
      return;
    }

    switch (msg.method) {
      case "INVITE":
        this.handleInvite(msg, remoteIp, remotePort, tcpSend).catch((e) =>
          logger.error("Error handling INVITE", e)
        );
        break;
      case "ACK":
        this.handleAck(msg, remoteIp, remotePort).catch((e) =>
          logger.error("Error handling ACK", e)
        );
        break;
      case "BYE":
        this.handleBye(msg, remoteIp, remotePort).catch((e) =>
          logger.error("Error handling BYE", e)
        );
        break;
      case "CANCEL":
        this.handleCancel(msg, remoteIp, remotePort);
        break;
      case "OPTIONS":
        this.handleOptions(msg, remoteIp, remotePort);
        break;
      case "REGISTER":
        // Respond 200 OK to keep-alive REGISTER messages from Chime
        this.sendResponse(
          buildSipResponse({
            statusCode: 200,
            reasonPhrase: "OK",
            request: msg,
            localIp: this.localIp,
            localPort: this.sipPort,
          remoteIp,
          remotePort,
          }),
          remoteIp,
          remotePort
        );
        break;
      default:
        logger.warn(`Unhandled SIP method: ${msg.method}`);
        this.sendResponse(
          buildSipResponse({
            statusCode: 501,
            reasonPhrase: "Not Implemented",
            request: msg,
            localIp: this.localIp,
            localPort: this.sipPort,
          remoteIp,
          remotePort,
          }),
          remoteIp,
          remotePort
        );
    }
  }

  // ─── INVITE ─────────────────────────────────────────────────────────────

  private async handleInvite(
    msg: SipMessage,
    remoteIp: string,
    remotePort: number,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    tcpSend?: (data: string) => void
  ): Promise<void> {
    const callId = msg.callId;
    logger.info(`Incoming INVITE call-id=${callId} from=${msg.from}`);

    // Send 100 Trying immediately
    this.sendResponse(
      buildSipResponse({
        statusCode: 100,
        reasonPhrase: "Trying",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
          remoteIp,
          remotePort,
      }),
      remoteIp,
      remotePort
    );

    // Check for duplicate
    if (this.activeSessions.has(callId) || this.pendingCalls.has(callId)) {
      logger.warn(`Duplicate INVITE for call ${callId}, ignoring`);
      return;
    }

    // Parse the SDP offer
    if (!msg.body || !msg.body.trim()) {
      logger.warn(`INVITE for ${callId} has no SDP body`);
      this.sendResponse(
        buildSipResponse({
          statusCode: 400,
          reasonPhrase: "Bad Request – Missing SDP",
          request: msg,
          localIp: this.localIp,
          localPort: this.sipPort,
          remoteIp,
          remotePort,
        }),
        remoteIp,
        remotePort
      );
      return;
    }

    const sdpOffer = parseSdp(msg.body);
    logger.info(
      `SDP offer: remoteRTP=${sdpOffer.connectionIp}:${sdpOffer.media.port} ` +
        `codec=${sdpOffer.codecName}(PT=${sdpOffer.payloadType})`
    );

    // Allocate a local RTP port
    const localRtpPort = this.portManager.allocate();
    if (!localRtpPort) {
      this.sendResponse(
        buildSipResponse({
          statusCode: 503,
          reasonPhrase: "Service Unavailable – No RTP ports",
          request: msg,
          localIp: this.localIp,
          localPort: this.sipPort,
          remoteIp,
          remotePort,
        }),
        remoteIp,
        remotePort
      );
      return;
    }

    // Send 180 Ringing
    const toTag = generateTag();
    this.sendResponse(
      buildSipResponse({
        statusCode: 180,
        reasonPhrase: "Ringing",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
          remoteIp,
          remotePort,
        toTag,
      }),
      remoteIp,
      remotePort
    );

    // Create and start the call session (connects to AgentCore)
    try {
      const session = new CallSession({
        callId,
        callerIp: sdpOffer.connectionIp || remoteIp,
        callerRtpPort: sdpOffer.media.port,
        localRtpPort,
        payloadType: sdpOffer.payloadType,
        sampleRate: sdpOffer.sampleRate,
      });

      await session.start();

      // Store as pending until we receive ACK
      this.pendingCalls.set(callId, {
        invite: msg,
        toTag,
        localRtpPort,
        remoteIp,
        remotePort,
      });
      this.activeSessions.set(callId, session);

      // Build SDP answer and send 200 OK
      const sdpAnswer = buildSdpAnswer(this.localIp, localRtpPort, sdpOffer);
      logger.info(`[${callId}] SDP answer advertising RTP at ${this.localIp}:${localRtpPort}`);
      const ok200 = buildSipResponse({
        statusCode: 200,
        reasonPhrase: "OK",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
        toTag,
        contact: `sip:${this.localIp}:${this.sipPort}`,
        body: sdpAnswer,
        contentType: "application/sdp",
      });

      this.sendResponse(ok200, remoteIp, remotePort, tcpSend);
      logger.info(`[${callId}] Sent 200 OK with SDP answer, RTP port ${localRtpPort}`);

    } catch (err) {
      logger.error(`[${callId}] Failed to start call session`, err);
      this.portManager.release(localRtpPort);
      this.sendResponse(
        buildSipResponse({
          statusCode: 500,
          reasonPhrase: "Internal Server Error",
          request: msg,
          localIp: this.localIp,
          localPort: this.sipPort,
          remoteIp,
          remotePort,
          toTag,
        }),
        remoteIp,
        remotePort
      );
    }
  }

  // ─── ACK ────────────────────────────────────────────────────────────────

  private async handleAck(
    msg: SipMessage,
    _remoteIp: string,
    _remotePort: number
  ): Promise<void> {
    const callId = msg.callId;
    const pending = this.pendingCalls.get(callId);
    if (!pending) {
      logger.debug(`ACK for unknown/non-pending call ${callId}`);
      return;
    }

    this.pendingCalls.delete(callId);
    logger.info(`[${callId}] ACK received – call fully established`);
  }

  // ─── BYE ────────────────────────────────────────────────────────────────

  private async handleBye(
    msg: SipMessage,
    remoteIp: string,
    remotePort: number
  ): Promise<void> {
    const callId = msg.callId;
    logger.info(`[${callId}] BYE received`);

    // Send 200 OK immediately
    this.sendResponse(
      buildSipResponse({
        statusCode: 200,
        reasonPhrase: "OK",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
          remoteIp,
          remotePort,
      }),
      remoteIp,
      remotePort
    );

    await this.tearDownCall(callId);
  }

  // ─── CANCEL ─────────────────────────────────────────────────────────────

  private handleCancel(
    msg: SipMessage,
    remoteIp: string,
    remotePort: number
  ): void {
    const callId = msg.callId;
    logger.info(`[${callId}] CANCEL received`);

    this.sendResponse(
      buildSipResponse({
        statusCode: 200,
        reasonPhrase: "OK",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
          remoteIp,
          remotePort,
      }),
      remoteIp,
      remotePort
    );

    // If the INVITE was still pending, also send 487 Request Terminated
    const pending = this.pendingCalls.get(callId);
    if (pending) {
      this.sendResponse(
        buildSipResponse({
          statusCode: 487,
          reasonPhrase: "Request Terminated",
          request: pending.invite,
          localIp: this.localIp,
          localPort: this.sipPort,
          remoteIp,
          remotePort,
          toTag: pending.toTag,
        }),
        pending.remoteIp,
        pending.remotePort
      );
      this.pendingCalls.delete(callId);
    }

    this.tearDownCall(callId).catch((e) => logger.warn("teardown error", e));
  }

  // ─── OPTIONS ────────────────────────────────────────────────────────────

  private handleOptions(
    msg: SipMessage,
    remoteIp: string,
    remotePort: number
  ): void {
    this.sendResponse(
      buildSipResponse({
        statusCode: 200,
        reasonPhrase: "OK",
        request: msg,
        localIp: this.localIp,
        localPort: this.sipPort,
          remoteIp,
          remotePort,
      }),
      remoteIp,
      remotePort
    );
  }

  // ─── Helpers ─────────────────────────────────────────────────────────────

  private async tearDownCall(callId: string): Promise<void> {
    const session = this.activeSessions.get(callId);
    if (session) {
      await session.end();
      this.activeSessions.delete(callId);
      // Release the RTP port
      const pending = this.pendingCalls.get(callId);
      if (pending) {
        this.portManager.release(pending.localRtpPort);
        this.pendingCalls.delete(callId);
      }
      logger.info(`[${callId}] Call session torn down. Active calls: ${this.activeSessions.size}`);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private sendResponse(message: string, ip: string, port: number, tcpSend?: (data: string) => void): void {
    logger.debug(`SIP → ${ip}:${port}\n${message.slice(0, 300)}`);
    if (tcpSend) {
      tcpSend(message);
    } else {
      const buf = Buffer.from(message, "utf8");
      this.socket.send(buf, port, ip, (err) => {
        if (err) logger.warn(`Failed to send SIP response to ${ip}:${port}`, err);
      });
    }
  }

  /**
   * Send periodic SIP OPTIONS to Chime Voice Connector to signal we are alive.
   * Equivalent to Asterisk pjsip.conf qualify_frequency=30.
   * Without this, Chime may not complete the SIP handshake with our endpoint.
   */
  /**
   * Send SIP REGISTER to Chime Voice Connector.
   * Chime requires registration before it will complete the ACK handshake
   * for outbound calls. This is equivalent to what chan_pjsip does automatically.
   */
  startRegistration(chimeVcHost: string, intervalSeconds = 60): void {
    let cseq = 1;

    const sendRegister = () => {
      const callId   = `reg-${this.localIp}-${Date.now()}`;
      const branch   = "z9hG4bK" + Math.random().toString(36).slice(2);
      const tag      = Math.random().toString(36).slice(2);
      const expiry   = intervalSeconds + 30;

      const register = [
        `REGISTER sip:${chimeVcHost} SIP/2.0`,
        `Via: SIP/2.0/UDP ${this.localIp}:${this.sipPort};branch=${branch};rport`,
        `From: <sip:${this.localIp}>;tag=${tag}`,
        `To: <sip:${this.localIp}@${chimeVcHost}>`,
        `Call-ID: ${callId}`,
        `CSeq: ${cseq++} REGISTER`,
        `Contact: <sip:${this.localIp}:${this.sipPort};transport=UDP>;expires=${expiry}`,
        `Max-Forwards: 70`,
        `Expires: ${expiry}`,
        `Content-Length: 0`,
        ``,
        ``,
      ].join("\r\n");

      const buf = Buffer.from(register, "utf8");
      this.socket.send(buf, 5060, chimeVcHost, (err) => {
        if (err) logger.debug(`REGISTER error: ${err.message}`);
        else logger.info(`REGISTER sent to ${chimeVcHost}`);
      });
    };

    sendRegister();
    setInterval(sendRegister, intervalSeconds * 1000);
    logger.info(`SIP registration started → ${chimeVcHost} every ${intervalSeconds}s`);
  }

  startOptionsKeepalive(chimeVcHost: string, intervalSeconds = 30): void {
    const sendOptions = () => {
      const callId = Math.random().toString(36).slice(2);
      const branch = "z9hG4bK" + Math.random().toString(36).slice(2);
      const tag    = Math.random().toString(36).slice(2);
      const options = [
        `OPTIONS sip:${chimeVcHost} SIP/2.0`,
        `Via: SIP/2.0/UDP ${this.localIp}:${this.sipPort};branch=${branch};rport`,
        `From: <sip:${this.localIp}:${this.sipPort}>;tag=${tag}`,
        `To: <sip:${chimeVcHost}>`,
        `Call-ID: ${callId}@${this.localIp}`,
        `CSeq: 1 OPTIONS`,
        `Contact: <sip:${this.localIp}:${this.sipPort}>`,
        `Max-Forwards: 70`,
        `Content-Length: 0`,
        ``,
        ``,
      ].join("\r\n");

      const buf = Buffer.from(options, "utf8");
      // Send to Chime VC host on port 5060
      this.socket.send(buf, 5060, chimeVcHost, (err) => {
        if (err) logger.debug(`OPTIONS keepalive error: ${err.message}`);
        else logger.debug(`OPTIONS keepalive sent to ${chimeVcHost}`);
      });
    };

    // Send immediately then on interval
    sendOptions();
    setInterval(sendOptions, intervalSeconds * 1000);
    logger.info(`OPTIONS keepalive started → ${chimeVcHost} every ${intervalSeconds}s`);
  }

  getActiveSessions(): number {
    return this.activeSessions.size;
  }
}

// ─── Local IP helper ─────────────────────────────────────────────────────────

/**
 * Determine the best local IP to advertise in SDP and SIP Contact headers.
 * Prefers the first non-loopback IPv4 address.
 * Falls back to the PUBLIC_IP env var (useful when behind an ECS/EC2 NAT).
 */
export function getLocalIp(): string {
  // Allow override via environment (important for containers behind NAT)
  if (process.env.PUBLIC_IP) return process.env.PUBLIC_IP;

  const interfaces = os.networkInterfaces();
  for (const iface of Object.values(interfaces)) {
    if (!iface) continue;
    for (const info of iface) {
      if (!info.internal && info.family === "IPv4") {
        return info.address;
      }
    }
  }
  return "127.0.0.1";
}

/**
 * Fetch the public IP from EC2 instance metadata (IMDSv2).
 * Falls back to getLocalIp() if not on EC2 or if PUBLIC_IP is already set.
 * Call this once at startup and store the result.
 */
export async function resolvePublicIp(): Promise<string> {
  // Explicit override always wins
  if (process.env.PUBLIC_IP) {
    logger.info(`Using PUBLIC_IP from environment: ${process.env.PUBLIC_IP}`);
    return process.env.PUBLIC_IP;
  }

  try {
    // IMDSv2: get token first
    const tokenRes = await fetch("http://169.254.169.254/latest/api/token", {
      method: "PUT",
      headers: { "X-aws-ec2-metadata-token-ttl-seconds": "21600" },
      signal: AbortSignal.timeout(2000),
    });
    if (!tokenRes.ok) throw new Error("token request failed");
    const token = await tokenRes.text();

    // Fetch public IPv4
    const ipRes = await fetch(
      "http://169.254.169.254/latest/meta-data/public-ipv4",
      {
        headers: { "X-aws-ec2-metadata-token": token },
        signal: AbortSignal.timeout(2000),
      }
    );
    if (!ipRes.ok) throw new Error(`metadata fetch failed: ${ipRes.status}`);
    const ip = (await ipRes.text()).trim();
    logger.info(`Resolved public IP from EC2 metadata: ${ip}`);
    return ip;
  } catch (err) {
    const fallback = getLocalIp();
    logger.warn(`Could not resolve public IP from metadata, using ${fallback}`, err);
    return fallback;
  }
}