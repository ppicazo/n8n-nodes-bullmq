"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getWorkflowInfo = getWorkflowInfo;
exports.getQueue = getQueue;
exports.redisConnectionTest = redisConnectionTest;
const bullmq_1 = require("bullmq");
const utils_1 = require("./utils");
async function getWorkflowInfo(source, itemIndex = 0) {
    const workflowInfo = {};
    if (source === 'workflows') {
        const { value } = this.getNodeParameter('workflowId', itemIndex, {});
        workflowInfo.id = value;
    }
    else if (source === 'custom') {
        const queueName = this.getNodeParameter('queueName', itemIndex);
        workflowInfo.id = queueName;
    }
    return workflowInfo;
}
async function getQueue(queueName, options) {
    const queue = new bullmq_1.Queue(queueName, options);
    return queue;
}
async function redisConnectionTest(credential) {
    const credentials = credential.data;
    try {
        const client = (0, utils_1.setupRedisClient)(credentials);
        await client.ping();
        return {
            status: 'OK',
            message: 'Connection successful!',
        };
    }
    catch (error) {
        return {
            status: 'Error',
            message: error.message,
        };
    }
}
//# sourceMappingURL=GenericFuntions.js.map