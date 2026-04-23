/**
 * G.711 codec: μ-law (PCMU) and A-law (PCMA) ↔ Linear PCM (16-bit signed)
 *
 * Chime Voice Connector sends G.711 encoded audio.
 * The Pipecat / AgentCore WebSocket expects raw 16-bit linear PCM at 8000 Hz.
 *
 * Reference: ITU-T G.711
 */

// ─── μ-law (PCMU, payload type 0) ────────────────────────────────────────────

/**
 * Decode a single μ-law byte to a 16-bit signed linear PCM sample.
 */
export function ulawToLinear(ulaw: number): number {
  ulaw = ~ulaw & 0xff;
  const sign = ulaw & 0x80 ? -1 : 1;
  const exponent = (ulaw >> 4) & 0x07;
  const mantissa = ulaw & 0x0f;
  const magnitude = ((mantissa << 1) | 0x21) << exponent;
  return sign * (magnitude - 33);
}

/**
 * Encode a 16-bit signed linear PCM sample to a μ-law byte.
 */
export function linearToUlaw(sample: number): number {
  const MU = 255;
  const MAX = 32767;

  // Clamp
  sample = Math.max(-MAX, Math.min(MAX, sample));

  const sign = sample < 0 ? 0x80 : 0;
  if (sample < 0) sample = -sample;

  // Bias
  sample = Math.round(sample + 33);
  if (sample > 32767) sample = 32767;

  // Segment
  let exponent = 7;
  for (let mask = 0x4000; exponent > 0; exponent--, mask >>= 1) {
    if (sample & mask) break;
  }
  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  const ulaw = ~(sign | (exponent << 4) | mantissa) & 0xff;
  return ulaw;
}

// ─── A-law (PCMA, payload type 8) ────────────────────────────────────────────

/**
 * Decode a single A-law byte to a 16-bit signed linear PCM sample.
 */
export function alawToLinear(alaw: number): number {
  alaw ^= 0x55; // A-law is XORed with 0x55
  const sign = alaw & 0x80 ? 1 : -1;
  alaw &= 0x7f;

  let linear: number;
  if (alaw >= 0x10) {
    const exponent = (alaw >> 4) - 1;
    const mantissa = alaw & 0x0f;
    linear = (mantissa << 1 | 0x21) << exponent;
  } else {
    linear = (alaw << 1) | 1;
  }

  linear <<= 3; // scale to 16-bit
  return sign * linear;
}

/**
 * Encode a 16-bit signed linear PCM sample to an A-law byte.
 */
export function linearToAlaw(sample: number): number {
  const MAX = 32767;
  sample = Math.max(-MAX, Math.min(MAX, sample));

  const sign = sample >= 0 ? 0x80 : 0;
  if (sample < 0) sample = -sample;

  let alaw: number;
  if (sample >= 2048) {
    let exponent = 7;
    for (let mask = 16384; exponent > 1; exponent--, mask >>= 1) {
      if (sample & mask) break;
    }
    const mantissa = (sample >> exponent) & 0x0f;
    alaw = (sign | ((exponent + 1) << 4) | mantissa) ^ 0x55;
  } else if (sample >= 256) {
    alaw = (sign | (sample >> 4 << 4) | ((sample >> 1) & 0x0f)) ^ 0x55;
  } else {
    alaw = (sign | (sample >> 1)) ^ 0x55;
  }

  return alaw & 0xff;
}

// ─── Buffer-level converters ──────────────────────────────────────────────────

/**
 * Decode a μ-law encoded buffer to 16-bit LE linear PCM buffer.
 */
export function ulawBufferToLinear16(ulaw: Buffer): Buffer {
  const out = Buffer.alloc(ulaw.length * 2);
  for (let i = 0; i < ulaw.length; i++) {
    const sample = ulawToLinear(ulaw[i]);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

/**
 * Encode a 16-bit LE linear PCM buffer to μ-law.
 */
export function linear16ToUlawBuffer(pcm: Buffer): Buffer {
  const out = Buffer.alloc(pcm.length / 2);
  for (let i = 0; i < out.length; i++) {
    const sample = pcm.readInt16LE(i * 2);
    out[i] = linearToUlaw(sample);
  }
  return out;
}

/**
 * Decode an A-law encoded buffer to 16-bit LE linear PCM buffer.
 */
export function alawBufferToLinear16(alaw: Buffer): Buffer {
  const out = Buffer.alloc(alaw.length * 2);
  for (let i = 0; i < alaw.length; i++) {
    const sample = alawToLinear(alaw[i]);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

/**
 * Encode a 16-bit LE linear PCM buffer to A-law.
 */
export function linear16ToAlawBuffer(pcm: Buffer): Buffer {
  const out = Buffer.alloc(pcm.length / 2);
  for (let i = 0; i < out.length; i++) {
    const sample = pcm.readInt16LE(i * 2);
    out[i] = linearToAlaw(sample);
  }
  return out;
}

/**
 * Decode a G.711 buffer (μ-law or A-law) to linear PCM based on payload type.
 *
 * @param encoded Raw G.711 bytes from RTP payload
 * @param payloadType 0 = PCMU (μ-law), 8 = PCMA (A-law)
 */
export function g711ToLinear16(encoded: Buffer, payloadType: number): Buffer {
  if (payloadType === 8) {
    return alawBufferToLinear16(encoded);
  }
  return ulawBufferToLinear16(encoded);
}

/**
 * Encode linear PCM to G.711 based on payload type.
 *
 * @param pcm Raw 16-bit LE PCM bytes
 * @param payloadType 0 = PCMU (μ-law), 8 = PCMA (A-law)
 */
export function linear16ToG711(pcm: Buffer, payloadType: number): Buffer {
  if (payloadType === 8) {
    return linear16ToAlawBuffer(pcm);
  }
  return linear16ToUlawBuffer(pcm);
}
