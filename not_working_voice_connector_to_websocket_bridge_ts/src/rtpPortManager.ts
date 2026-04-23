/**
 * RTP Port Manager
 *
 * Allocates local UDP port numbers for RTP sessions.
 * Ports are drawn from a configurable pool (default 10000–20000)
 * and returned to the pool when a call ends.
 */

import { logger } from "./logger";

export class RtpPortManager {
  private readonly minPort: number;
  private readonly maxPort: number;
  private readonly availablePorts: Set<number>;

  constructor(minPort = 10000, maxPort = 20000) {
    this.minPort = minPort;
    this.maxPort = maxPort;
    this.availablePorts = new Set();

    // Pre-fill the pool with even-numbered ports (RTP convention)
    for (let port = minPort; port <= maxPort; port += 2) {
      this.availablePorts.add(port);
    }

    logger.debug(`RTP port pool initialized: ${minPort}–${maxPort} (${this.availablePorts.size} ports)`);
  }

  /**
   * Allocate the next available RTP port.
   * Returns null if the pool is exhausted.
   */
  allocate(): number | null {
    const iter = this.availablePorts.values();
    const next = iter.next();
    if (next.done) {
      logger.warn("RTP port pool exhausted");
      return null;
    }
    this.availablePorts.delete(next.value);
    logger.debug(`Allocated RTP port ${next.value} (${this.availablePorts.size} remaining)`);
    return next.value;
  }

  /**
   * Return a port to the pool after a call ends.
   */
  release(port: number): void {
    if (port >= this.minPort && port <= this.maxPort) {
      this.availablePorts.add(port);
      logger.debug(`Released RTP port ${port} (${this.availablePorts.size} available)`);
    }
  }

  availableCount(): number {
    return this.availablePorts.size;
  }
}
