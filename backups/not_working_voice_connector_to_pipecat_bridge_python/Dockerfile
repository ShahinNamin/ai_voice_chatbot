FROM python:3.11-slim

WORKDIR /app

# System deps (none needed beyond Python stdlib for audioop in 3.11)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .
COPY .env.example .env

# SIP signaling port (UDP)
EXPOSE 5060/udp

# RTP media ports (UDP) — keep in sync with RTP_PORT_MIN/MAX in .env
EXPOSE 10000-20000/udp

CMD ["python", "bridge.py"]