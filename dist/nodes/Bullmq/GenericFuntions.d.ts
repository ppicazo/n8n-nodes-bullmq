import { Queue, QueueOptions } from 'bullmq';
import type { ICredentialsDecrypted, ICredentialTestFunctions, IExecuteFunctions, IExecuteWorkflowInfo, INodeCredentialTestResult } from 'n8n-workflow';
export declare function getWorkflowInfo(this: IExecuteFunctions, source: 'workflows' | 'custom', itemIndex?: number): Promise<IExecuteWorkflowInfo>;
export declare function getQueue(this: IExecuteFunctions, queueName: string, options: QueueOptions): Promise<Queue>;
export declare function redisConnectionTest(this: ICredentialTestFunctions, credential: ICredentialsDecrypted): Promise<INodeCredentialTestResult>;
