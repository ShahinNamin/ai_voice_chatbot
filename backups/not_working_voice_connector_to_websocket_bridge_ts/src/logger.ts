/**
 * Simple structured logger with timestamps and levels.
 */

type LogLevel = "debug" | "info" | "warn" | "error";

const LEVELS: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

const currentLevel: LogLevel =
  (process.env.LOG_LEVEL as LogLevel | undefined) ?? "info";

function log(level: LogLevel, message: string, meta?: unknown): void {
  if (LEVELS[level] < LEVELS[currentLevel]) return;
  const ts = new Date().toISOString();
  const metaStr = meta !== undefined ? " " + JSON.stringify(meta) : "";
  const line = `[${ts}] [${level.toUpperCase()}] ${message}${metaStr}`;
  if (level === "error") {
    console.error(line);
  } else {
    console.log(line);
  }
}

export const logger = {
  debug: (msg: string, meta?: unknown) => log("debug", msg, meta),
  info:  (msg: string, meta?: unknown) => log("info",  msg, meta),
  warn:  (msg: string, meta?: unknown) => log("warn",  msg, meta),
  error: (msg: string, meta?: unknown) => log("error", msg, meta),
};
