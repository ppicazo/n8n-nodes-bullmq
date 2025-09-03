import type { IExecuteFunctions, INodeExecutionData, INodeTypeDescription, IWebhookDescription, IWebhookFunctions, IWebhookResponseData, Node } from 'n8n-workflow';
import { redisConnectionTest } from './GenericFuntions';
export declare const getResponseCode: (parameters: WebhookParameters) => number;
export type WebhookParameters = {
    httpMethod: string | string[];
    responseMode: string;
    responseData: string;
    responseCode?: number;
    options?: {
        responseData?: string;
        responseCode?: {
            values?: {
                responseCode: number;
                customCode?: number;
            };
        };
        noResponseBody?: boolean;
    };
};
export declare const getResponseData: (parameters: WebhookParameters) => string | undefined;
export declare const defaultWebhookDescription: IWebhookDescription;
export declare class BullmqWait implements Node {
    description: INodeTypeDescription;
    methods: {
        credentialTest: {
            redisConnectionTest: typeof redisConnectionTest;
        };
    };
    webhook(context: IWebhookFunctions): Promise<IWebhookResponseData>;
    execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]>;
}
