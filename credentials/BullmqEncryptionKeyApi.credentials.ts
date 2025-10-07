import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export default class BullmqEncryptionKeyApi implements ICredentialType {
	name = 'bullMqEncryptionKeyApi';
	displayName = 'BullMQ Encryption Key API';
	properties: INodeProperties[] = [
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
