/**
 * RTP (Real-time Transport Protocol) socket handler.
 *
 * Responsibilities:
 *  - Open a UDP socket on a negotiated local port.
 *  - Receive RTP packets from Chime Voice Connector, strip the 12-byte header,
 *    and emit raw PCM payload buffers.
 *  - Accept raw PCM payload buffers and wrap them in RTP headers for transmission
 *    back to the Voice Connector.
 *
 * Chime Voice Connector uses:
 *   - PCMU (G.711 μ-law, PT=0) or PCMA (G.711 A-law, PT=8)
 *   - 8000 Hz, 1 channel
 *   - 20 ms packetization → 160 samples → 160 bytes per packet
 */

import * as dgram from "dgram";
import { EventEmitter } from "events";
import { logger } from "./logger";

export interface RtpPacketInfo {
  payloadType: number;
  sequenceNumber: number;
  timestamp: number;
  ssrc: number;
  payload: Buffer;
}

export class RtpHandler extends EventEmitter {
  private socket: dgram.Socket;
  private localPort: number;
  private remoteIp: string = "";
  private remotePort: number = 0;
  private ssrc: number;
  private sequenceNumber: number = 0;
  private timestamp: number = 0;
  private payloadType: number;
  private running = false;

  constructor(localPort: number, payloadType: number = 0) {
    super();
    this.localPort = localPort;
    this.payloadType = payloadType;
    this.ssrc = Math.floor(Math.random() * 0xffffffff);
    this.sequenceNumber = Math.floor(Math.random() * 0xffff);
    this.timestamp = Math.floor(Math.random() * 0xffffffff);
    this.socket = dgram.createSocket("udp4");
  }

  /**
   * Bind the UDP socket and start receiving RTP packets.
   */
  async start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.socket.on("error", (err) => {
        logger.error("RTP socket error", err);
        this.emit("error", err);
        if (!this.running) reject(err);
      });

      this.socket.on("message", (msg, rinfo) => {
        // Latch the remote endpoint from the first packet received
        if (!this.remoteIp) {
          this.remoteIp = rinfo.address;
          this.remotePort = rinfo.port;
          logger.info(`RTP: latched remote endpoint ${rinfo.address}:${rinfo.port}`);
        }

        const parsed = this.parseRtp(msg);
        if (parsed) {
          this.emit("audioIn", parsed.payload, parsed);
        }
      });

      this.socket.bind(this.localPort, () => {
        this.running = true;
        logger.info(`RTP socket bound on port ${this.localPort}`);
        resolve();
      });
    });
  }

  /**
   * Set the remote endpoint explicitly (from SDP negotiation).
   */
  setRemoteEndpoint(ip: string, port: number): void {
    this.remoteIp = ip;
    this.remotePort = port;
    logger.debug(`RTP remote endpoint set to ${ip}:${port}`);
  }

  /**
   * Send a raw payload buffer as an RTP packet to the remote endpoint.
   */
  sendAudio(payload: Buffer): void {
    if (!this.remoteIp || !this.remotePort) return;

    const packet = this.buildRtp(payload);
    this.socket.send(packet, this.remotePort, this.remoteIp, (err) => {
      if (err) logger.warn("RTP send error", err);
    });

    // Advance sequence number and timestamp (160 samples @ 8kHz = 20ms)
    this.sequenceNumber = (this.sequenceNumber + 1) & 0xffff;
    this.timestamp = (this.timestamp + 160) >>> 0;
  }

  /**
   * Parse an RTP packet according to RFC 3550.
   * Returns null if the packet is malformed.
   */
  private parseRtp(buf: Buffer): RtpPacketInfo | null {
    if (buf.length < 12) return null;

    const firstByte = buf[0];
    const version = (firstByte >> 6) & 0x3;
    if (version !== 2) return null;

    const hasExtension = (firstByte >> 4) & 0x1;
    const csrcCount = firstByte & 0xf;

    const payloadType = buf[1] & 0x7f;
    const sequenceNumber = buf.readUInt16BE(2);
    const timestamp = buf.readUInt32BE(4);
    const ssrc = buf.readUInt32BE(8);

    let headerLen = 12 + csrcCount * 4;

    if (hasExtension) {
      if (buf.length < headerLen + 4) return null;
      const extLen = buf.readUInt16BE(headerLen + 2);
      headerLen += 4 + extLen * 4;
    }

    if (buf.length <= headerLen) return null;

    return {
      payloadType,
      sequenceNumber,
      timestamp,
      ssrc,
      payload: buf.slice(headerLen),
    };
  }

  /**
   * Build an RTP packet from a raw payload buffer.
   */
  private buildRtp(payload: Buffer): Buffer {
    const header = Buffer.alloc(12);
    header[0] = 0x80; // V=2, P=0, X=0, CC=0
    header[1] = this.payloadType & 0x7f; // M=0
    header.writeUInt16BE(this.sequenceNumber, 2);
    header.writeUInt32BE(this.timestamp, 4);
    header.writeUInt32BE(this.ssrc, 8);
    return Buffer.concat([header, payload]);
  }

  /**
   * Close the UDP socket.
   */
  stop(): void {
    this.running = false;
    try {
      this.socket.close();
    } catch {
      // ignore
    }
    logger.debug(`RTP socket on port ${this.localPort} closed`);
  }

  getLocalPort(): number {
    return this.localPort;
  }
}
