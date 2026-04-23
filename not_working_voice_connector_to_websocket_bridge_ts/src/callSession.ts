/**
 * CallSession
 *
 * Represents one active phone call. Owns:
 *  - An RTP socket (for audio to/from Chime)
 *  - An AgentCore WebSocket client
 *  - The codec state for G.711 ↔ PCM conversion
 *
 * Data flow:
 *   Chime RTP (G.711) → decode → PCM → AgentCore WS
 *   AgentCore WS → PCM → encode → G.711 → Chime RTP
 */

import { RtpHandler } from "./rtp";
import { AgentCoreClient } from "./agentCoreClient";
import { g711ToLinear16, linear16ToG711 } from "./codec";
import { upsample8to16, downsample16to8 } from "./resampler";
import { logger } from "./logger";

export interface CallSessionOptions {
  callId: string;
  callerIp: string;
  callerRtpPort: number;
  localRtpPort: number;
  payloadType: number; // 0=PCMU, 8=PCMA
  sampleRate?: number;
}

export class CallSession {
  readonly callId: string;
  private rtp: RtpHandler;
  private agentClient: AgentCoreClient;
  private payloadType: number;
  private active = false;
  private startTime: Date;

  constructor(opts: CallSessionOptions) {
    this.callId = opts.callId;
    this.payloadType = opts.payloadType;
    this.startTime = new Date();

    // Set up RTP handler
    this.rtp = new RtpHandler(opts.localRtpPort, opts.payloadType);
    this.rtp.setRemoteEndpoint(opts.callerIp, opts.callerRtpPort);

    // Set up AgentCore client — always tell it 16 kHz so it matches
    // Pipecat's default pipeline. We resample from/to the 8 kHz RTP world.
    this.agentClient = new AgentCoreClient(opts.callId, {
      sampleRate: 16000,
      numChannels: 1,
    });
  }

  /**
   * Start the call session:
   *  1. Bind RTP socket
   *  2. Connect to AgentCore
   *  3. Wire up audio pipelines
   */
  async start(): Promise<void> {
    logger.info(`[${this.callId}] Starting call session`);

    // Start RTP first so we can receive audio
    await this.rtp.start();

    // Connect to AgentCore
    await this.agentClient.connect();

    // ── Phone → Agent pipeline ────────────────────────────────────────────
    let rtpPacketsReceived = 0;
    let audioSentToAgent = 0;
    this.rtp.on("audioIn", (payload: Buffer) => {
      if (!this.active) return;
      rtpPacketsReceived++;
      if (rtpPacketsReceived === 1) {
        logger.info(`[${this.callId}] ✅ First RTP packet received from phone (${payload.length} bytes)`);
      }
      if (rtpPacketsReceived % 500 === 0) {
        logger.info(`[${this.callId}] 📞 RTP packets received: ${rtpPacketsReceived}, sent to agent: ${audioSentToAgent}`);
      }
      // Decode G.711 (8 kHz) → linear 16-bit PCM at 8 kHz
      const pcm8k = g711ToLinear16(payload, this.payloadType);
      // Upsample 8 kHz → 16 kHz to match Pipecat's default pipeline
      const pcm16k = upsample8to16(pcm8k);
      if (this.agentClient.isConnected()) {
        this.agentClient.sendAudio(pcm16k);
        audioSentToAgent++;
      } else if (rtpPacketsReceived === 1) {
        logger.warn(`[${this.callId}] ⚠️  RTP arriving but AgentCore not connected yet`);
      }
    });

    // ── Agent → Phone pipeline ────────────────────────────────────────────
    let audioFromAgent = 0;
    this.agentClient.on("audioOut", (pcm16k: Buffer) => {
      if (!this.active) return;
      audioFromAgent++;
      if (audioFromAgent === 1) {
        logger.info(`[${this.callId}] ✅ First audio received FROM agent (${pcm16k.length} bytes) — sending to phone`);
      }
      // Downsample 16 kHz → 8 kHz to match G.711 / RTP
      const pcm8k = downsample16to8(pcm16k);
      // Encode linear PCM → G.711 and send as RTP to Chime
      const encoded = linear16ToG711(pcm8k, this.payloadType);
      this.rtp.sendAudio(encoded);
    });

    // ── Logging ───────────────────────────────────────────────────────────
    this.agentClient.on("transcript", (text: string) => {
      logger.info(`[${this.callId}] Agent: ${text}`);
    });

    this.agentClient.on("botReady", () => {
      logger.info(`[${this.callId}] Agent bot is ready, audio flowing`);
    });

    this.agentClient.on("error", (err: Error) => {
      logger.error(`[${this.callId}] AgentCore error: ${err.message}`);
    });

    this.rtp.on("error", (err: Error) => {
      logger.error(`[${this.callId}] RTP error: ${err.message}`);
    });

    this.active = true;
    logger.info(`[${this.callId}] Call session active`);
  }

  /**
   * Tear down the session, releasing all resources.
   */
  async end(): Promise<void> {
    if (!this.active) return;
    this.active = false;

    const duration = Math.round((Date.now() - this.startTime.getTime()) / 1000);
    logger.info(`[${this.callId}] Ending call session (duration: ${duration}s)`);

    await this.agentClient.disconnect();
    this.rtp.stop();
  }

  isActive(): boolean {
    return this.active;
  }

  getDuration(): number {
    return Math.round((Date.now() - this.startTime.getTime()) / 1000);
  }
}