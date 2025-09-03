"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class BullmqEncryptionKeyApi {
    constructor() {
        this.name = 'bullMqEncryptionKeyApi';
        this.displayName = 'BullMQ Encryption Key API';
        this.properties = [
            {
                displayName: 'Key',
                name: 'key',
                type: 'string',
                typeOptions: { password: true },
                default: '',
                description: '32-byte key in base64 or 64-char hex',
                required: true,
            },
        ];
    }
}
exports.default = BullmqEncryptionKeyApi;
//# sourceMappingURL=BullmqEncryptionKeyApi.credentials.js.map