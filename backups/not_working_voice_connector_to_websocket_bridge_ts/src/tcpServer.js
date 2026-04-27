/**
 * tcpServer.js — plain JS TCP SIP listener.
 * Keeps each TCP socket alive and provides a way to send responses
 * back over the same connection the request came in on.
 */
"use strict";
const net = require("net");

/**
 * @param {number} port
 * @param {(msg: string, remoteIp: string, remotePort: number, send: (data: string) => void) => void} onMessage
 * @param {(err: Error) => void} onError
 * @returns {Promise<import("net").Server>}
 */
function startTcpSipServer(port, onMessage, onError) {
  return new Promise((resolve, reject) => {
    const server = net.createServer((socket) => {
      const remoteIp   = (socket.remoteAddress || "").replace(/^::ffff:/, "");
      const remotePort = socket.remotePort || 5060;

      // Keep socket alive so we can send responses back on it
      socket.setKeepAlive(true, 5000);
      socket.setTimeout(120000); // 2 min idle timeout

      // Helper to send a SIP response back over this socket
      const send = (data) => {
        if (!socket.destroyed) {
          socket.write(data);
        }
      };

      let buffer = "";
      socket.on("data", (data) => {
        buffer += data.toString("utf8");
        let idx;
        while ((idx = buffer.indexOf("\r\n\r\n")) >= 0) {
          const headerPart = buffer.slice(0, idx + 4);
          const clMatch = headerPart.match(/Content-Length:\s*(\d+)/i);
          const cl = clMatch ? parseInt(clMatch[1], 10) : 0;
          const total = idx + 4 + cl;
          if (buffer.length < total) break;
          const fullMsg = buffer.slice(0, total);
          buffer = buffer.slice(total);
          // Pass send function so sipServer can reply on this socket
          onMessage(fullMsg, remoteIp, remotePort, send);
        }
      });

      socket.on("timeout", () => socket.destroy());
      socket.on("error", () => {}); // ignore individual socket errors
      socket.on("close", () => { buffer = ""; });
    });

    server.on("error", (err) => {
      onError(err);
      reject(err);
    });

    server.listen(port, "0.0.0.0", () => {
      resolve(server);
    });
  });
}

module.exports = { startTcpSipServer };