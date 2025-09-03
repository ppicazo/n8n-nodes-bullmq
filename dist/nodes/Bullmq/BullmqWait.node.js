"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BullmqWait = exports.defaultWebhookDescription = exports.getResponseData = exports.getResponseCode = void 0;
const n8n_workflow_1 = require("n8n-workflow");
const GenericFuntions_1 = require("./GenericFuntions");
const utils_1 = require("./utils");
const getResponseCode = (parameters) => {
    var _a;
    if (parameters.responseCode) {
        return parameters.responseCode;
    }
    const responseCodeOptions = parameters.options;
    if ((_a = responseCodeOptions === null || responseCodeOptions === void 0 ? void 0 : responseCodeOptions.responseCode) === null || _a === void 0 ? void 0 : _a.values) {
        const { responseCode, customCode } = responseCodeOptions.responseCode.values;
        if (customCode) {
            return customCode;
        }
        return responseCode;
    }
    return 200;
};
exports.getResponseCode = getResponseCode;
const getResponseData = (parameters) => {
    const { responseData, responseMode, options } = parameters;
    if (responseData)
        return responseData;
    if (responseMode === 'onReceived') {
        const data = options === null || options === void 0 ? void 0 : options.responseData;
        if (data)
            return data;
    }
    if (options === null || options === void 0 ? void 0 : options.noResponseBody)
        return 'noData';
    return undefined;
};
exports.getResponseData = getResponseData;
exports.defaultWebhookDescription = {
    name: 'default',
    httpMethod: '={{$parameter["httpMethod"] || "GET"}}',
    isFullPath: true,
    responseCode: `={{(${exports.getResponseCode})($parameter)}}`,
    responseMode: '={{$parameter["responseMode"]}}',
    responseData: `={{(${exports.getResponseData})($parameter)}}`,
    responseBinaryPropertyName: '={{$parameter["responseBinaryPropertyName"]}}',
    responseContentType: '={{$parameter["options"]["responseContentType"]}}',
    responsePropertyName: '={{$parameter["options"]["responsePropertyName"]}}',
    responseHeaders: '={{$parameter["options"]["responseHeaders"]}}',
    path: '={{$parameter["path"]}}',
};
const webhookPath = 'bullmq-wait';
class BullmqWait {
    constructor() {
        this.description = {
            displayName: 'Wait for Job',
            name: 'bullmqWait',
            icon: 'file:bullmq.svg',
            group: ['input'],
            version: 1,
            description: 'Wait for a job in BullMQ',
            defaults: {
                name: 'Wait for Job',
            },
            inputs: ['main'],
            outputs: ['main'],
            webhooks: [
                {
                    ...exports.defaultWebhookDescription,
                    httpMethod: 'GET',
                    responseData: 'noData',
                    responseMode: 'onReceived',
                    responseContentType: 'text/plain',
                    path: webhookPath,
                    restartWebhook: true,
                },
                {
                    name: 'default',
                    httpMethod: 'GET',
                    responseMode: 'onReceived',
                    path: webhookPath,
                    restartWebhook: true,
                    isFullPath: true,
                    isForm: true,
                },
                {
                    name: 'default',
                    httpMethod: 'POST',
                    responseMode: '={{$parameter["responseMode"]}}',
                    responseData: '={{$parameter["responseMode"] === "lastNode" ? "noData" : undefined}}',
                    path: webhookPath,
                    restartWebhook: true,
                    isFullPath: true,
                    isForm: true,
                },
            ],
            credentials: [
                {
                    name: 'redis',
                    required: true,
                    testedBy: 'redisConnectionTest',
                },
            ],
            properties: [
                {
                    displayName: 'Operation',
                    name: 'operation',
                    type: 'options',
                    noDataExpression: true,
                    options: [
                        {
                            name: 'Wait',
                            value: 'wait',
                            description: 'Wait for a job',
                            action: 'Wait for a job',
                        },
                    ],
                    default: 'wait',
                },
                {
                    displayName: 'Queue Name',
                    name: 'queueName',
                    type: 'string',
                    displayOptions: {
                        show: {
                            operation: ['wait'],
                        },
                    },
                    default: '',
                    required: true,
                    description: 'Queue name to wait for the job',
                },
                {
                    displayName: 'Job ID',
                    name: 'jobId',
                    type: 'string',
                    displayOptions: {
                        show: {
                            operation: ['wait'],
                        },
                    },
                    default: '',
                    required: true,
                    description: 'ID of the job to wait for',
                },
            ],
        };
        this.methods = {
            credentialTest: { redisConnectionTest: GenericFuntions_1.redisConnectionTest },
        };
    }
    async webhook(context) {
        return {
            webhookResponse: { ok: true },
            workflowData: [[{ json: {} }]],
            noWebhookResponse: false,
        };
    }
    async execute() {
        const credentials = await this.getCredentials('redis');
        const connection = (0, utils_1.setupRedisClient)(credentials);
        const operation = this.getNodeParameter('operation', 0);
        const returnItems = [];
        try {
            const items = this.getInputData();
            for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
                const item = { json: {}, pairedItem: { item: itemIndex } };
                if (operation === 'wait') {
                    const queueName = this.getNodeParameter('queueName', itemIndex);
                    const jobId = this.getNodeParameter('jobId', itemIndex);
                    const queue = await GenericFuntions_1.getQueue.call(this, queueName, { connection });
                    const job = await queue.getJob(jobId);
                    if (!job) {
                        throw new n8n_workflow_1.NodeOperationError(this.getNode(), `Job with ID "${jobId}" does not exist!`);
                    }
                    const context = this.getWorkflowDataProxy(0);
                    const execution = context.$execution;
                    const resumeUrl = execution.resumeUrl;
                    await job.updateData({
                        ...job.data,
                        resumeUrl,
                        step: 'waiting',
                    });
                    item.json = job.toJSON();
                    items[itemIndex] = item;
                    returnItems.push(items[itemIndex]);
                }
                else {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The operation "${operation}" is not supported!`);
                }
            }
        }
        catch (error) {
            throw error;
        }
        const waitTill = new Date(n8n_workflow_1.WAIT_TIME_UNLIMITED);
        return putToWait(this, waitTill);
    }
}
exports.BullmqWait = BullmqWait;
async function putToWait(context, waitTill) {
    await context.putExecutionToWait(waitTill);
    return [context.getInputData()];
}
//# sourceMappingURL=BullmqWait.node.js.map