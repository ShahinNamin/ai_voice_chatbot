/**
 * Real-time PCM resampler — linear interpolation, zero lookahead.
 *
 * Used to bridge the 8 kHz phone world (Chime RTP / G.711) with the
 * 16 kHz default Pipecat pipeline.
 *
 * Why linear interpolation?
 *   - Zero added latency: each input chunk produces output immediately,
 *     with no buffering or look-ahead window.
 *   - Negligible CPU cost: a simple lerp per output sample.
 *   - Sufficient quality for voice at 2x ratios. The only artefact is a
 *     gentle low-pass effect above 4 kHz, which is inaudible for speech.
 *
 * For non-integer ratios (not needed here) a polyphase FIR would be used,
 * but 8 kHz ↔ 16 kHz is an exact 2× relationship so lerp is perfect.
 */

/**
 * Upsample a 16-bit LE PCM buffer from 8 kHz to 16 kHz (2× ratio).
 *
 * Each pair of consecutive input samples produces 3 output samples:
 *   out[2n]   = in[n]
 *   out[2n+1] = midpoint(in[n], in[n+1])   ← linear interpolation
 *
 * The last input sample is repeated at the end to avoid a DC glitch
 * on chunk boundaries (stateless — no inter-chunk state needed at 2×).
 *
 * @param pcm8k   Buffer of 16-bit signed LE PCM at 8000 Hz
 * @returns       Buffer of 16-bit signed LE PCM at 16000 Hz (2× length)
 */
export function upsample8to16(pcm8k: Buffer): Buffer {
  const inSamples  = pcm8k.length >> 1;          // number of 16-bit samples
  const outSamples = inSamples * 2;
  const out        = Buffer.alloc(outSamples * 2);

  for (let i = 0; i < inSamples; i++) {
    const s0 = pcm8k.readInt16LE(i * 2);
    const s1 = i + 1 < inSamples
      ? pcm8k.readInt16LE((i + 1) * 2)
      : s0;                                       // repeat last sample

    out.writeInt16LE(s0,              i * 4);     // original sample
    out.writeInt16LE((s0 + s1) >> 1,  i * 4 + 2); // interpolated midpoint
  }

  return out;
}

/**
 * Downsample a 16-bit LE PCM buffer from 16 kHz to 8 kHz (2× decimation).
 *
 * Takes every other sample (simple decimation). Because Pipecat's output
 * has already been band-limited by the TTS service to well below 8 kHz,
 * no anti-aliasing filter is needed in practice.
 *
 * If you ever encounter aliasing artefacts (rare with modern TTS), a
 * simple 2-tap averaging filter can be swapped in:
 *   out[i] = (in[2i] + in[2i+1]) >> 1
 *
 * @param pcm16k  Buffer of 16-bit signed LE PCM at 16000 Hz
 * @returns       Buffer of 16-bit signed LE PCM at 8000 Hz (½ length)
 */
export function downsample16to8(pcm16k: Buffer): Buffer {
  const inSamples  = pcm16k.length >> 1;
  const outSamples = inSamples >> 1;              // keep every other sample
  const out        = Buffer.alloc(outSamples * 2);

  for (let i = 0; i < outSamples; i++) {
    // Average adjacent pair — removes high-frequency aliases cheaply
    const s0 = pcm16k.readInt16LE(i * 4);
    const s1 = pcm16k.readInt16LE(i * 4 + 2);
    out.writeInt16LE((s0 + s1) >> 1, i * 2);
  }

  return out;
}
