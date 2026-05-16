// Web Crypto so this works in both Edge (middleware) and Node (route handlers / server actions).

export const SESSION_COOKIE_NAME = "dee_dashboard_session";
export const SESSION_TTL_SECONDS = 60 * 60 * 12; // 12 hours
export const SMOKE_TOKEN_HEADER = "x-dashboard-smoke-token";

// Minimum lengths. Anything below these means "not configured" so the gate stays
// dormant rather than becoming half-configured (would-be authenticated but with
// no working password or unsignable cookie).
const MIN_PASSWORD_LENGTH = 8;
const MIN_COOKIE_SECRET_LENGTH = 16;
const MIN_SMOKE_TOKEN_LENGTH = 16;

type SessionPayload = {
  iat: number;
  exp: number;
};

function getCookieSecret(): string | null {
  const secret = process.env.DASHBOARD_GATE_COOKIE_SECRET;
  if (!secret || secret.length < MIN_COOKIE_SECRET_LENGTH) return null;
  return secret;
}

function getConfiguredPassword(): string | null {
  const expected = process.env.DASHBOARD_GATE_PASSWORD;
  if (!expected || expected.length < MIN_PASSWORD_LENGTH) return null;
  return expected;
}

function base64UrlEncode(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function base64UrlEncodeString(value: string): string {
  return base64UrlEncode(new TextEncoder().encode(value));
}

function base64UrlDecodeToBytes(value: string): Uint8Array {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/").padEnd(
    value.length + ((4 - (value.length % 4)) % 4),
    "=",
  );
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function base64UrlDecodeToString(value: string): string {
  return new TextDecoder().decode(base64UrlDecodeToBytes(value));
}

async function hmacKey(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let mismatch = 0;
  for (let i = 0; i < a.length; i++) mismatch |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return mismatch === 0;
}

export async function issueSessionCookieValue(now: number = Date.now()): Promise<string | null> {
  const secret = getCookieSecret();
  if (!secret) return null;

  const payload: SessionPayload = {
    iat: Math.floor(now / 1000),
    exp: Math.floor(now / 1000) + SESSION_TTL_SECONDS,
  };

  const encodedPayload = base64UrlEncodeString(JSON.stringify(payload));
  const key = await hmacKey(secret);
  const signature = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(encodedPayload)),
  );

  return `${encodedPayload}.${base64UrlEncode(signature)}`;
}

export async function verifySessionCookieValue(
  value: string | undefined | null,
  now: number = Date.now(),
): Promise<boolean> {
  if (!value) return false;
  const secret = getCookieSecret();
  if (!secret) return false;

  const parts = value.split(".");
  if (parts.length !== 2) return false;
  const [encodedPayload, providedSignature] = parts;
  if (!encodedPayload || !providedSignature) return false;

  let payload: SessionPayload;
  try {
    payload = JSON.parse(base64UrlDecodeToString(encodedPayload)) as SessionPayload;
  } catch {
    return false;
  }

  if (typeof payload.exp !== "number" || typeof payload.iat !== "number") return false;
  if (Math.floor(now / 1000) >= payload.exp) return false;

  const key = await hmacKey(secret);
  const expectedSignature = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(encodedPayload)),
  );
  const expectedEncoded = base64UrlEncode(expectedSignature);

  return timingSafeEqual(expectedEncoded, providedSignature);
}

export function smokeTokenMatches(headerValue: string | null | undefined): boolean {
  const expected = process.env.DASHBOARD_SMOKE_TOKEN;
  if (!expected || expected.length < MIN_SMOKE_TOKEN_LENGTH) return false;
  if (!headerValue) return false;
  if (headerValue.length !== expected.length) return false;
  return timingSafeEqual(headerValue, expected);
}

export function checkSubmittedPassword(submitted: string | undefined | null): boolean {
  const expected = getConfiguredPassword();
  if (!expected) return false;
  if (typeof submitted !== "string" || submitted.length === 0) return false;
  if (submitted.length !== expected.length) return false;
  return timingSafeEqual(submitted, expected);
}

export function isGateConfigured(): boolean {
  return Boolean(getCookieSecret()) && Boolean(getConfiguredPassword());
}
