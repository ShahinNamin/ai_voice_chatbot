FROM python:3.12-slim

WORKDIR /app

# System deps for audioop (Python 3.13 removes it; 3.12 still has it built-in)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

# SIP UDP port
EXPOSE 5060/udp

# RTP port range
EXPOSE 10000-20000/udp

# Credentials come from the ECS task execution role (no env vars needed)
CMD ["python", "-u", "bridge.py"]
