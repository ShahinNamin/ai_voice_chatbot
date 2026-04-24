/**
 * BridgeServer
 *
 * Top-level orchestrator. Wires together:
 *   - SIP server (UDP 5060)
 *   - RTP port pool
 *   - HTTP health/status endpoint
 */

import * as http from "http";
import { SipServer, getLocalIp, resolvePublicIp } from "./sipServer";
import { RtpPortManager } from "./rtpPortManager";
import { logger } from "./logger";

export class BridgeServer {
  private sipServer: SipServer;
  private portManager: RtpPortManager;
  private httpServer: http.Server;
  private readonly sipPort: number;
  private readonly httpPort: number;
  private localIp: string;
  private startTime: Date;

  constructor() {
    this.sipPort = parseInt(process.env.SIP_PORT ?? "5060", 10);
    this.httpPort = parseInt(process.env.HEALTH_PORT ?? "8080", 10);

    const rtpMinPort = parseInt(process.env.RTP_MIN_PORT ?? "10000", 10);
    const rtpMaxPort = parseInt(process.env.RTP_MAX_PORT ?? "20000", 10);

    // Use private IP as placeholder — resolvePublicIp() will update this at startup
    this.localIp = getLocalIp();
    this.startTime = new Date();

    this.portManager = new RtpPortManager(rtpMinPort, rtpMaxPort);
    this.sipServer = new SipServer(this.sipPort, this.localIp, this.portManager);
    this.httpServer = this.createHttpServer();
  }

  async start(): Promise<void> {
    // Validate required environment
    if (!process.env.AGENT_RUNTIME_ARN) {
      throw new Error("AGENT_RUNTIME_ARN environment variable must be set");
    }

    // Resolve public IP before starting SIP server so SDP answers are correct
    this.localIp = await resolvePublicIp();
    this.sipServer.updateLocalIp(this.localIp);

    logger.info(`Bridge server initializing`, {
      localIp: this.localIp,
      sipPort: this.sipPort,
    });

    // Start SIP server
    await this.sipServer.start();

    // Register with and send OPTIONS keepalives to Chime Voice Connector.
    // Chime requires REGISTER before it will complete ACK handshakes (same as Asterisk).
    const chimeVcHost = process.env.CHIME_VC_HOST;
    if (chimeVcHost) {
      this.sipServer.startRegistration(chimeVcHost);
      this.sipServer.startOptionsKeepalive(chimeVcHost);
    } else {
      logger.warn("CHIME_VC_HOST not set — SIP registration and keepalives disabled");
    }

    // Start HTTP health server
    await this.startHttpServer();

    logger.info("=".repeat(60));
    logger.info("Chime → AgentCore Bridge is RUNNING");
    logger.info(`  Local IP         : ${this.localIp}`);
    logger.info(`  SIP port (UDP)   : ${this.sipPort}`);
    logger.info(`  Health HTTP      : ${this.httpPort}`);
    logger.info(`  AgentCore ARN    : ${process.env.AGENT_RUNTIME_ARN}`);
    logger.info(`  AWS Region       : ${process.env.AWS_REGION ?? "us-east-1"}`);
    logger.info("=".repeat(60));
  }

  async stop(): Promise<void> {
    logger.info("Stopping bridge server...");
    this.sipServer.stop();
    await new Promise<void>((resolve) => this.httpServer.close(() => resolve()));
    logger.info("Bridge server stopped");
  }

  // ─── HTTP health & status server ─────────────────────────────────────────

  private createHttpServer(): http.Server {
    return http.createServer((req, res) => {
      const url = new URL(req.url ?? "/", `http://localhost`);

      res.setHeader("Content-Type", "application/json");

      if (url.pathname === "/health" || url.pathname === "/") {
        const payload = JSON.stringify({
          status: "ok",
          service: "chime-agentcore-bridge",
          uptime: Math.round((Date.now() - this.startTime.getTime()) / 1000),
          activeCalls: this.sipServer.getActiveSessions(),
          localIp: this.localIp,
          sipPort: this.sipPort,
        });
        res.writeHead(200);
        res.end(payload);
        return;
      }

      if (url.pathname === "/status") {
        const payload = JSON.stringify({
          activeCalls: this.sipServer.getActiveSessions(),
          portPoolAvailable: this.portManager.availableCount(),
          agentRuntimeArn: process.env.AGENT_RUNTIME_ARN,
          region: process.env.AWS_REGION ?? "us-east-1",
        });
        res.writeHead(200);
        res.end(payload);
        return;
      }

      res.writeHead(404);
      res.end(JSON.stringify({ error: "Not found" }));
    });
  }

  private startHttpServer(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.httpServer.on("error", reject);
      this.httpServer.listen(this.httpPort, () => {
        logger.info(`Health endpoint listening on http://0.0.0.0:${this.httpPort}/health`);
        resolve();
      });
    });
  }
}