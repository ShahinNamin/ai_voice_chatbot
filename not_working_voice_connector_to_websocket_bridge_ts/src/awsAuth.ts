/**
 * AWS Credentials & Signed WebSocket URL
 *
 * Uses the official AWS SDK v3 for credential resolution and SigV4 signing.
 * This is the TypeScript equivalent of server.py's botocore approach and
 * guarantees correct signing without any hand-rolled crypto.
 */

import { SignatureV4 } from "@smithy/signature-v4";
import { Sha256 } from "@aws-crypto/sha256-js";
import { fromNodeProviderChain } from "@aws-sdk/credential-providers";
import { logger } from "./logger";

/**
 * Resolve credentials and return a signed WSS URL for AgentCore.
 * fromNodeProviderChain() mirrors boto3.Session().get_credentials() exactly:
 *   1. Environment variables
 *   2. ~/.aws/credentials
 *   3. ECS task role
 *   4. EC2 instance profile (IMDSv2)
 */
export async function getSignedAgentCoreUrl(): Promise<string> {
  const agentRuntimeArn = process.env.AGENT_RUNTIME_ARN;
  const region          = process.env.AWS_REGION ?? "us-east-1";
  const expiresSeconds  = parseInt(process.env.SIGNED_URL_EXPIRY_SECONDS ?? "300", 10);

  if (!agentRuntimeArn) {
    throw new Error("AGENT_RUNTIME_ARN environment variable is required");
  }

  // Build the base URL with the ARN encoded in the path
  const encodedArn = encodeURIComponent(agentRuntimeArn);

  // Resolve credentials via the standard AWS provider chain
  const credentials = await fromNodeProviderChain()();

  // Sign with SigV4 using pre-signed URL (query string) mode
  const signer = new SignatureV4({
    service:     "bedrock-agentcore",
    region,
    credentials,
    sha256:      Sha256,
  });

  const request = {
    method:   "GET",
    hostname: `bedrock-agentcore.${region}.amazonaws.com`,
    path:     `/runtimes/${encodedArn}/ws`,
    protocol: "https:",
    headers:  { host: `bedrock-agentcore.${region}.amazonaws.com` },
    query:    {} as Record<string, string>,
  };

  const signed = await signer.presign(request, { expiresIn: expiresSeconds });

  // Reconstruct the signed URL as wss://
  const queryString = Object.entries(signed.query ?? {})
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v as string)}`)
    .join("&");

  const signedUrl = `wss://bedrock-agentcore.${region}.amazonaws.com${signed.path}?${queryString}`;

  logger.debug("Generated signed AgentCore URL");
  return signedUrl;
}