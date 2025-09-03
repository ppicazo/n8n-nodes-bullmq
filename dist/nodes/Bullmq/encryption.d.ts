type EncHeader = {
    v: 1;
    alg: 'aes-256-gcm';
    iv: string;
    tag: string;
};
export type EncryptedPayload = {
    __enc: EncHeader;
    data: string;
};
export declare function encryptPayload(payload: unknown, key: string | Buffer): EncryptedPayload;
export declare function isEncryptedPayload(x: unknown): x is EncryptedPayload;
export declare function decryptPayload(input: EncryptedPayload, key: string | Buffer): unknown;
export {};
