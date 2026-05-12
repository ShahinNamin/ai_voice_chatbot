# ─── Build stage ─────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

# Install dependencies
COPY package.json ./
RUN npm install

# Compile TypeScript
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ─── Runtime stage ────────────────────────────────────────────────────────────
FROM node:20-alpine AS runtime

WORKDIR /app

# Only production dependencies
COPY package.json ./
RUN npm install --omit=dev

# Copy compiled output
COPY --from=builder /app/dist ./dist

# ── Ports ─────────────────────────────────────────────────────────────────────
# SIP signalling (UDP)
EXPOSE 5060/udp
# RTP audio (UDP range – configure your security group / task def accordingly)
EXPOSE 10000-20000/udp
# HTTP health check
EXPOSE 8080/tcp

# ── Environment (set at runtime via ECS task definition / EC2 userdata) ───────
# Required:
#   AGENT_RUNTIME_ARN   – ARN of the AgentCore runtime, e.g.
#                         arn:aws:bedrock-agentcore:us-east-1:123456789:runtime/abc
# Optional:
#   AWS_REGION          – defaults to us-east-1
#   SIP_PORT            – defaults to 5060
#   RTP_MIN_PORT        – defaults to 10000
#   RTP_MAX_PORT        – defaults to 20000
#   HEALTH_PORT         – HTTP health port, defaults to 8080
#   SIGNED_URL_EXPIRY_SECONDS – WSS URL expiry, defaults to 300
#   PUBLIC_IP           – override the IP advertised in SDP (needed if behind NAT)
#   LOG_LEVEL           – debug | info | warn | error  (default: info)

ENV NODE_ENV=production

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:8080/health || exit 1

CMD ["node", "dist/index.js"]
