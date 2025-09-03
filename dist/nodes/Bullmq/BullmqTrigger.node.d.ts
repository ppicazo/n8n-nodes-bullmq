import type { ITriggerFunctions, INodeType, INodeTypeDescription, ITriggerResponse } from 'n8n-workflow';
import { redisConnectionTest } from './GenericFuntions';
export declare class BullmqTrigger implements INodeType {
    description: INodeTypeDescription;
    methods: {
        credentialTest: {
            redisConnectionTest: typeof redisConnectionTest;
        };
    };
    trigger(this: ITriggerFunctions): Promise<ITriggerResponse>;
}
