/**
 * AgentCore WebSocket Client
 *
 * Opens a Pipecat-compatible WebSocket connection to AWS Bedrock AgentCore.
 *
 * === Wire Protocol ===
 * The Pipecat WebSocketTransport (pipecat-ai/client-js) uses
 * ProtobufFrameSerializer by default.  The protobuf schema is:
 *
 *   message Frame {
 *     oneof frame {
 *       AudioRawFrame    audio    = 1;
 *       TranscriptionFrame transcription = 6;
 *       ...
 *     }
 *   }
 *   message AudioRawFrame {
 *     bytes  audio        = 1;
 *     uint32 sample_rate  = 2;
 *     uint32 num_channels = 3;
 *   }
 *
 * Each websocket binary message is a raw protobuf-encoded Frame.
 * We hand-encode/decode the minimal subset we need (AudioRawFrame)
 * without pulling in a full protobuf library, using protobuf
 * binary encoding rules directly.
 *
 * In addition the transport exchanges JSON RTVI control messages as text frames:
 *   → client-ready  (sent after connection, so the pipeline starts)
 *   ← bot-ready     (agent confirms it is listening)
 *   ← transcript    (agent / user transcription events)
 *
 * See also:
 *  - https://docs.pipecat.ai/client/js/transports/websocket
 *  - https://github.com/pipecat-ai/pipecat/blob/main/src/pipecat/frames/protobufs/frames.proto
 */

import WebSocket from "ws";
import EventEmitter = require("events");
import { getSignedAgentCoreUrl } from "./awsAuth";
import { logger } from "./logger";

// ─── Minimal protobuf encode/decode ─────────────────────────────────────────

function encodeVarint(value: number): Buffer {
  const bytes: number[] = [];
  while (value > 0x7f) {
    bytes.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  bytes.push(value & 0x7f);
  return Buffer.from(bytes);
}

function encodeField(fieldNumber: number, wireType: 0 | 2, value: Buffer | number): Buffer {
  const tag = encodeVarint((fieldNumber << 3) | wireType);
  if (wireType === 0) {
    return Buffer.concat([tag, encodeVarint(value as number)]);
  }
  const data = value as Buffer;
  return Buffer.concat([tag, encodeVarint(data.length), data]);
}

/**
 * Encode an AudioRawFrame wrapped in a Frame (protobuf).
 *
 * frames.proto layout:
 *   Frame.audio (field 1) -> AudioRawFrame {
 *     audio        field 1 (bytes)
 *     sample_rate  field 2 (uint32)
 *     num_channels field 3 (uint32)
 *   }
 */
export function encodeAudioFrame(
  pcm16: Buffer,
  sampleRate: number,
  numChannels: number
): Buffer {
  const innerMsg = Buffer.concat([
    encodeField(1, 2, pcm16),
    encodeField(2, 0, sampleRate),
    encodeField(3, 0, numChannels),
  ]);
  // Outer Frame wraps the AudioRawFrame at field 1
  return encodeField(1, 2, innerMsg);
}

/**
 * Decode a protobuf Frame binary message.
 * Returns audio payload when the frame is an AudioRawFrame, else null.
 */
export function decodeFrame(buf: Buffer): {
  audio?: Buffer;
  sampleRate?: number;
  numChannels?: number;
} | null {
  let offset = 0;

  function readVarint(): number {
    let result = 0, shift = 0;
    while (offset < buf.length) {
      const b = buf[offset++];
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
    }
    return result;
  }

  if (offset >= buf.length) return null;
  const outerTag  = readVarint();
  const outerField = outerTag >> 3;
  const outerWire  = outerTag & 0x07;

  // We only handle the audio oneof field (field 1)
  if (outerField !== 1 || outerWire !== 2) return null;

  const innerLen = readVarint();
  const innerBuf = buf.slice(offset, offset + innerLen);
  offset += innerLen;

  // Decode AudioRawFrame fields
  let innerOff = 0;
  let audio: Buffer | undefined;
  let sampleRate: number | undefined;
  let numChannels: number | undefined;

  function innerVarint(): number {
    let result = 0, shift = 0;
    while (innerOff < innerBuf.length) {
      const b = innerBuf[innerOff++];
      result |= (b & 0x7f) << shift;
      if ((b & 0x80) === 0) break;
      shift += 7;
    }
    return result;
  }

  while (innerOff < innerBuf.length) {
    const tag   = innerVarint();
    const field = tag >> 3;
    const wire  = tag & 0x07;
    if (wire === 2) {
      const len = innerVarint();
      const val = innerBuf.slice(innerOff, innerOff + len);
      innerOff += len;
      if (field === 1) audio = val;
    } else if (wire === 0) {
      const val = innerVarint();
      if (field === 2) sampleRate  = val;
      if (field === 3) numChannels = val;
    } else {
      break;
    }
  }

  return { audio, sampleRate, numChannels };
}

// ─── RTVI control message helpers ────────────────────────────────────────────

// No client-ready message needed — AgentCore's Pipecat server starts
// the pipeline automatically when the WebSocket connection is established.

// ─── AgentCoreClient ─────────────────────────────────────────────────────────

export interface AgentCoreClientOptions {
  maxRetries?:   number;
  retryDelayMs?: number;
  sampleRate?:   number;
  numChannels?:  number;
}

export class AgentCoreClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private connected    = false;
  private connecting   = false;
  private shouldReconnect = true;
  private retryCount   = 0;
  private readonly options: Required<AgentCoreClientOptions>;
  private readonly callId: string;

  constructor(callId: string, options: AgentCoreClientOptions = {}) {
    super();
    this.callId  = callId;
    this.options = {
      maxRetries:   options.maxRetries   ?? 5,
      retryDelayMs: options.retryDelayMs ?? 2000,
      sampleRate:   options.sampleRate   ?? 8000,
      numChannels:  options.numChannels  ?? 1,
    };
  }

  // ── Public API ────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    if (this.connecting || this.connected) return;
    this.connecting = true;

    try {
      logger.info(`[${this.callId}] Connecting to AgentCore...`);
      const signedUrl = await getSignedAgentCoreUrl();

      const ws = new WebSocket(signedUrl, {
        headers: { Origin: "https://bridge.local" },
      });
      this.ws = ws;

      ws.on("open", () => {
        this.connected  = true;
        this.connecting = false;
        this.retryCount = 0;
        logger.info(`[${this.callId}] AgentCore WebSocket connected`);
        this.emit("connected");
      });

      ws.on("message", (data: WebSocket.RawData) => {
        const isBinary = Buffer.isBuffer(data) || data instanceof ArrayBuffer;
        this.handleMessage(data, isBinary);
      });

      ws.on("close", (code, reason) => {
        this.connected  = false;
        this.connecting = false;
        logger.warn(`[${this.callId}] AgentCore WS closed`, { code, reason: reason.toString() });
        this.emit("disconnected", code);

        // Code 1000 = normal closure (AgentCore ended the session cleanly,
        // e.g. another client connected or the pipeline finished).
        // Code 1001 = going away. Do NOT reconnect on these — reconnecting
        // would just keep fighting with the next incoming call.
        const isCleanClosure = code === 1000 || code === 1001;
        if (isCleanClosure) {
          logger.info(`[${this.callId}] AgentCore closed cleanly (${code}), not reconnecting`);
          return;
        }

        // Only reconnect on unexpected network errors (1006, 1011, etc.)
        if (this.shouldReconnect && this.retryCount < this.options.maxRetries) {
          this.retryCount++;
          const delay = this.options.retryDelayMs * this.retryCount;
          logger.info(`[${this.callId}] Reconnecting in ${delay}ms (attempt ${this.retryCount})`);
          setTimeout(() => this.connect(), delay);
        } else if (this.shouldReconnect) {
          logger.error(`[${this.callId}] Max reconnect attempts reached`);
          this.emit("error", new Error("AgentCore: max reconnect attempts reached"));
        }
      });

      ws.on("error", (err) => {
        this.connecting = false;
        logger.error(`[${this.callId}] AgentCore WS error: ${err.message}`);
        this.emit("error", err);
      });

    } catch (err) {
      this.connecting = false;
      logger.error(`[${this.callId}] Failed to connect to AgentCore`, err);
      throw err;
    }
  }

  /**
   * Send a buffer of 16-bit signed LE PCM audio to AgentCore.
   * The buffer is protobuf-serialised as Frame { audio: AudioRawFrame }.
   */
  sendAudio(pcm16: Buffer): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    const frame = encodeAudioFrame(pcm16, this.options.sampleRate, this.options.numChannels);
    this.ws.send(frame, { binary: true }, (err) => {
      if (err) logger.warn(`[${this.callId}] Error sending audio frame: ${err.message}`);
    });
  }

  async disconnect(): Promise<void> {
    this.shouldReconnect = false;
    if (this.ws) {
      if (this.ws.readyState === WebSocket.OPEN) {
        try { this.sendJson(JSON.stringify({ label: "rtvi-ai", type: "disconnect" })); }
        catch { /* ignore */ }
      }
      this.ws.close(1000, "Call ended");
      this.ws = null;
    }
    this.connected = false;
    logger.info(`[${this.callId}] AgentCore client disconnected`);
  }

  isConnected(): boolean {
    return this.connected && this.ws?.readyState === WebSocket.OPEN;
  }

  // ── Private helpers ───────────────────────────────────────────────────────

  private sendJson(msg: string): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    this.ws.send(msg, (err) => {
      if (err) logger.warn(`[${this.callId}] Error sending JSON: ${err.message}`);
    });
  }

  private handleMessage(data: WebSocket.RawData, isBinary: boolean): void {
    try {
      // ── Text / RTVI JSON control frames ────────────────────────────────
      if (!isBinary) {
        const text = data.toString("utf8");
        let msg: Record<string, unknown>;
        try { msg = JSON.parse(text); }
        catch {
          logger.debug(`[${this.callId}] Non-JSON text: ${text.slice(0, 100)}`);
          return;
        }

        const type = (msg["type"] ?? msg["action"]) as string | undefined;
        logger.debug(`[${this.callId}] AgentCore JSON: type=${type}`);

        switch (type) {
          case "bot-ready":
          case "pipecat-ready":
            logger.info(`[${this.callId}] AgentCore bot is ready`);
            this.emit("botReady", msg);
            break;

          case "transcript":
          case "bot-transcription":
          case "user-transcription": {
            const t = (msg["text"] ?? msg["transcript"] ?? "") as string;
            const role = type === "user-transcription" ? "User" : "Bot";
            logger.info(`[${this.callId}] ${role}: ${t}`);
            this.emit("transcript", t, role);
            break;
          }

          case "error":
            logger.warn(`[${this.callId}] AgentCore reported error`, msg);
            break;
        }
        return;
      }

      // ── Binary protobuf audio frames ────────────────────────────────────
      const buf = Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer);
      if (buf.length < 2) return;

      const decoded = decodeFrame(buf);
      if (decoded?.audio && decoded.audio.length > 0) {
        this.emit("audioOut", decoded.audio);
      }

    } catch (err) {
      logger.warn(`[${this.callId}] Error parsing AgentCore message`, err);
    }
  }
}