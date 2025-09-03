import type { ICredentialType, INodeProperties } from 'n8n-workflow';
export default class BullmqEncryptionKeyApi implements ICredentialType {
    name: string;
    displayName: string;
    documentationUrl: string;
    properties: INodeProperties[];
}
