"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.encryptPayload = encryptPayload;
exports.isEncryptedPayload = isEncryptedPayload;
exports.decryptPayload = decryptPayload;
const crypto = __importStar(require("crypto"));
function parseKey(key) {
    if (Buffer.isBuffer(key)) {
        if (key.length !== 32)
            throw new Error('Encryption key must be 32 bytes');
        return key;
    }
    const k = key.trim();
    if (/^[0-9a-fA-F]{64}$/.test(k))
        return Buffer.from(k, 'hex');
    try {
        const b = Buffer.from(k, 'base64');
        if (b.length === 32)
            return b;
    }
    catch {
    }
    throw new Error('Key must be 32-byte base64 or 64-char hex');
}
function encryptPayload(payload, key) {
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
function isEncryptedPayload(x) {
    if (!x || typeof x !== 'object')
        return false;
    const rec = x;
    const enc = rec['__enc'];
    if (!enc || typeof enc !== 'object')
        return false;
    const alg = enc['alg'];
    const data = rec['data'];
    return alg === 'aes-256-gcm' && typeof data === 'string';
}
function decryptPayload(input, key) {
    const k = parseKey(key);
    const iv = Buffer.from(input.__enc.iv, 'base64');
    const tag = Buffer.from(input.__enc.tag, 'base64');
    const ciphertext = Buffer.from(input.data, 'base64');
    const decipher = crypto.createDecipheriv('aes-256-gcm', k, iv);
    decipher.setAuthTag(tag);
    const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    return JSON.parse(plaintext.toString('utf8'));
}
//# sourceMappingURL=encryption.js.map