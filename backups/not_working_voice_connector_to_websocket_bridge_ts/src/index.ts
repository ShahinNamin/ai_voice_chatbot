/**
 * Chime Voice Connector → AgentCore WebSocket Bridge
 */

import { BridgeServer } from "./bridge";
import { logger } from "./logger";

const server = new BridgeServer();

process.on("SIGINT", async () => {
  logger.info("Received SIGINT – shutting down gracefully");
  await server.stop();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  logger.info("Received SIGTERM – shutting down gracefully");
  await server.stop();
  process.exit(0);
});

server.start().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  const stack   = err instanceof Error ? err.stack   : undefined;
  logger.error(`Fatal error starting bridge server: ${message}`);
  if (stack) console.error(stack);
  process.exit(1);
});