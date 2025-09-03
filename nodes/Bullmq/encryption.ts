import * as crypto from 'crypto';

type EncHeader = {
  v: 1;
  alg: 'aes-256-gcm';
  iv: string; // base64
  tag: string; // base64
};

export type EncryptedPayload = {
  __enc: EncHeader;
  data: string; // base64 ciphertext
};

function parseKey(key: string | Buffer): Buffer {
  if (Buffer.isBuffer(key)) {
    if (key.length !== 32) throw new Error('Encryption key must be 32 bytes');
    return key;
  }
  const k = key.trim();
  // hex (64 chars)
  if (/^[0-9a-fA-F]{64}$/.test(k)) return Buffer.from(k, 'hex');
  // base64 (32 bytes)
  try {
    const b = Buffer.from(k, 'base64');
    if (b.length === 32) return b;
  } catch {
    // ignore
  }
  throw new Error('Key must be 32-byte base64 or 64-char hex');
}

export function encryptPayload(payload: unknown, key: string | Buffer): EncryptedPayload {
  const k = parseKey(key);
  const iv = crypto.randomBytes(12);
  const plaintext = Buffer.from(JSON.stringify(payload), 'utf8');
  const cipher = crypto.createCipheriv('aes-256-gcm', k, iv);
  const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  const tag = cipher.getAuthTag();
  return {
    __enc: { v: 1, alg: 'aes-256-gcm', iv: iv.toString('base64'), tag: tag.toString('base64') },
    data: ciphertext.toString('base64'),
  };
}

export function isEncryptedPayload(x: unknown): x is EncryptedPayload {
  return Boolean(
    x &&
      typeof x === 'object' &&
      x.__enc &&
      x.__enc.alg === 'aes-256-gcm' &&
      typeof x.data === 'string',
  );
}

export function decryptPayload(input: EncryptedPayload, key: string | Buffer): unknown {
  const k = parseKey(key);
  const iv = Buffer.from(input.__enc.iv, 'base64');
  const tag = Buffer.from(input.__enc.tag, 'base64');
  const ciphertext = Buffer.from(input.data, 'base64');
  const decipher = crypto.createDecipheriv('aes-256-gcm', k, iv);
  decipher.setAuthTag(tag);
  const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  return JSON.parse(plaintext.toString('utf8'));
}
