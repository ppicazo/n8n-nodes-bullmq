"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Bullmq = void 0;
const n8n_workflow_1 = require("n8n-workflow");
const utils_1 = require("./utils");
const encryption_1 = require("./encryption");
const bullmq_1 = require("bullmq");
const GenericFuntions_1 = require("./GenericFuntions");
class Bullmq {
    constructor() {
        this.description = {
            displayName: 'BullMQ',
            name: 'bullmq',
            icon: 'file:bullmq.svg',
            group: ['input'],
            version: 1,
            description: 'Get, send and update data in Redis',
            defaults: {
                name: 'BullMQ',
            },
            inputs: ['main'],
            outputs: ['main'],
            credentials: [
                {
                    name: 'redis',
                    required: true,
                    testedBy: 'redisConnectionTest',
                },
                {
                    name: 'bullMqEncryptionKeyApi',
                    required: false,
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
                            displayName: 'Job ID',
                            name: 'jobId',
                            type: 'string',
                            default: '',
                            description: 'Custom job ID to assign to the job (optional)',
                        },
                        {
                            name: 'Add',
                            value: 'add',
                            description: 'Add a job to a queue',
                            action: 'Add a job',
                        },
                    ],
                    default: 'add',
                },
                {
                    displayName: 'Queue Source',
                    name: 'queueSource',
                    type: 'options',
                    options: [
                        { name: 'Workflows', value: 'workflows' },
                        { name: 'Custom', value: 'custom' },
                    ],
                    default: 'workflows',
                    displayOptions: { show: { operation: ['add'] } },
                },
                {
                    displayName: 'Queue Name',
                    name: 'queueName',
                    type: 'string',
                    displayOptions: { show: { operation: ['add'], queueSource: ['custom'] } },
                    default: '',
                    required: true,
                    description: 'Queue name to add the job to',
                },
                {
                    displayName: 'Workflow',
                    name: 'workflowId',
                    type: 'workflowSelector',
                    default: '',
                    required: true,
                    displayOptions: { show: { operation: ['add'], queueSource: ['workflows'] } },
                },
                {
                    displayName: 'Job Name',
                    name: 'jobName',
                    type: 'string',
                    displayOptions: { show: { operation: ['add'] } },
                    default: '',
                    required: true,
                    description: 'Job name to publish',
                },
                {
                    displayName: 'Data Source',
                    name: 'dataSource',
                    type: 'options',
                    displayOptions: { show: { operation: ['add'] } },
                    options: [
                        { name: 'Previous Node', value: 'previousNode', description: 'Get data from previous node' },
                        { name: 'Input', value: 'input', description: 'Use data from the input' },
                    ],
                    default: 'previousNode',
                },
                {
                    displayName: 'Job Data',
                    name: 'jobData',
                    type: 'assignmentCollection',
                    displayOptions: { show: { operation: ['add'], dataSource: ['input'] } },
                    default: {},
                },
                {
                    displayName: 'Wait Until Finished',
                    name: 'waitUntilFinished',
                    type: 'boolean',
                    displayOptions: { show: { operation: ['add'] } },
                    default: false,
                    description: 'Whether to wait until the job is finished, Don\'t use this option for long running jobs',
                },
                {
                    displayName: 'Encryption',
                    name: 'encryption',
                    type: 'collection',
                    placeholder: 'Encryption options',
                    displayOptions: { show: { operation: ['add'] } },
                    default: {},
                    options: [
                        { displayName: 'Encrypt Payload', name: 'enabled', type: 'boolean', default: false },
                        {
                            displayName: 'Key Source',
                            name: 'keySource',
                            type: 'options',
                            options: [
                                { name: 'Credential', value: 'credential' },
                                { name: 'Inline', value: 'inline' },
                            ],
                            default: 'credential',
                            displayOptions: { show: { enabled: [true] } },
                        },
                        {
                            displayName: 'Inline Key',
                            name: 'inlineKey',
                            type: 'string',
                            typeOptions: { password: true },
                            description: '32-byte key in base64 or 64-char hex',
                            default: '',
                            displayOptions: { show: { enabled: [true], keySource: ['inline'] } },
                        },
                    ],
                },
                {
                    displayName: 'Options',
                    name: 'options',
                    type: 'collection',
                    placeholder: 'Add option',
                    displayOptions: { show: { operation: ['add'] } },
                    default: {},
                    options: [
                        {
                            displayName: 'timeToLive',
                            name: 'timeToLive',
                            type: 'number',
                            default: 0,
                            description: 'Time in milliseconds before the job should be failed',
                        },
                        {
                            displayName: 'Return Value',
                            name: 'returnValue',
                            type: 'boolean',
                            default: false,
                            description: 'Whether to return the value of the job',
                        },
                        {
                            displayName: 'Delay',
                            name: 'delay',
                            type: 'number',
                            default: 0,
                            description: 'Delay in milliseconds before the job should be processed',
                        },
                        {
                            displayName: 'Priority',
                            name: 'priority',
                            type: 'number',
                            default: 0,
                            description: 'Priority of the job, from 1 to any, higher is higher priority',
                        },
                        {
                            displayName: 'Attempts',
                            name: 'attempts',
                            type: 'number',
                            default: 0,
                            description: 'Number of attempts to run the job',
                        },
                        {
                            displayName: 'Backoff',
                            name: 'backoff',
                            type: 'number',
                            default: 0,
                            description: 'Backoff time in milliseconds',
                        },
                        {
                            displayName: 'Lifo',
                            name: 'lifo',
                            type: 'boolean',
                            default: false,
                            description: 'Whether to process the job in LIFO order, otherwise FIFO',
                        },
                        {
                            displayName: 'Remove On Complete',
                            name: 'removeOnComplete',
                            type: 'boolean',
                            default: false,
                            description: 'Whether to remove the job from the queue when it is completed',
                        },
                        {
                            displayName: 'Remove On Fail',
                            name: 'removeOnFail',
                            type: 'boolean',
                            default: false,
                            description: 'Whether to remove the job from the queue when it fails',
                        }
                    ]
                },
            ],
        };
        this.methods = {
            credentialTest: { redisConnectionTest: GenericFuntions_1.redisConnectionTest },
        };
    }
    async execute() {
        var _a;
        const credentials = await this.getCredentials('redis');
        const source = this.getNodeParameter('queueSource', 0, '');
        const connection = (0, utils_1.setupRedisClient)(credentials);
        const operation = this.getNodeParameter('operation', 0);
        const returnItems = [];
        const items = this.getInputData();
        for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
            const inputItem = items[itemIndex];
            const item = { json: {}, pairedItem: { item: itemIndex } };
            try {
                if (operation === 'add') {
                    const workflowInfo = await GenericFuntions_1.getWorkflowInfo.call(this, source, itemIndex);
                    if (!workflowInfo.id) {
                        throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The workflow did not return an id!`);
                    }
                    const queueName = workflowInfo.id;
                    const jobName = this.getNodeParameter('jobName', itemIndex);
                    const dataSource = this.getNodeParameter('dataSource', itemIndex);
                    const messageData = this.getNodeParameter('jobData', itemIndex, {});
                    const options = this.getNodeParameter('options', itemIndex);
                    const { timeToLive, delay = 0, priority = 1, attempts = 1, backoff = 0, lifo = false, removeOnComplete = false, removeOnFail = false, returnValue = false, jobId, } = options;
                    const queue = await GenericFuntions_1.getQueue.call(this, queueName, { connection });
                    const cleanup = async () => {
                        try {
                            queue.close();
                            queue.disconnect();
                        }
                        catch (error) {
                            console.log(error);
                        }
                    };
                    const jsonPayload = dataSource === 'previousNode' ?
                        (0, utils_1.parseJson)(inputItem.json, {}) :
                        (0, utils_1.parseAssignmentsCollection)(messageData, {});
                    const encryption = this.getNodeParameter('encryption', itemIndex, {}) || {};
                    const shouldEncrypt = Boolean(encryption.enabled);
                    let key;
                    if (shouldEncrypt) {
                        if (encryption.keySource === 'inline') {
                            key = encryption.inlineKey;
                        }
                        else {
                            try {
                                const creds = await this.getCredentials('bullMqEncryptionKeyApi');
                                key = creds === null || creds === void 0 ? void 0 : creds.key;
                            }
                            catch (error) {
                                console.debug('bullMqEncryptionKeyApi credential not available:', (_a = error === null || error === void 0 ? void 0 : error.message) !== null && _a !== void 0 ? _a : error);
                            }
                        }
                        if (!key) {
                            throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Encryption enabled but no key provided (credential missing or inline key empty).', { itemIndex });
                        }
                    }
                    const payloadToSend = shouldEncrypt ? (0, encryption_1.encryptPayload)(jsonPayload, key) : jsonPayload;
                    const job = await queue.add(jobName, payloadToSend, {
                        delay,
                        priority,
                        attempts,
                        backoff,
                        lifo,
                        removeOnComplete,
                        removeOnFail,
                        ...(jobId ? { jobId } : {}),
                    });
                    job.log(`Job added from executionId ${this.getExecutionId()}`);
                    const waitUntilFinished = this.getNodeParameter('waitUntilFinished', itemIndex);
                    if (waitUntilFinished) {
                        const queueEvents = new bullmq_1.QueueEvents(queueName, { connection });
                        const cleanupQueueEvents = async () => {
                            try {
                                queueEvents.close();
                                queueEvents.disconnect();
                            }
                            catch (error) {
                                console.log(error);
                            }
                        };
                        await job.waitUntilFinished(queueEvents, +timeToLive);
                        if (!job.id) {
                            cleanupQueueEvents();
                            cleanup();
                            throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The job did not return an id!`);
                        }
                        const updatedJob = await queue.getJob(job.id);
                        if (updatedJob) {
                            item.json = updatedJob.toJSON();
                        }
                        else {
                            item.json = job.toJSON();
                        }
                        if (returnValue) {
                            item.json = (0, utils_1.craftJobReturnValue)(item.json.returnvalue);
                        }
                        items[itemIndex] = item;
                        returnItems.push(items[itemIndex]);
                        cleanupQueueEvents();
                    }
                    else {
                        item.json = job.toJSON();
                        items[itemIndex] = item;
                        returnItems.push(items[itemIndex]);
                    }
                    cleanup();
                }
                else {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The operation "${operation}" is not supported!`, { itemIndex });
                }
                ;
            }
            catch (error) {
                if (this.continueOnFail()) {
                    items[itemIndex] = {
                        json: inputItem.json,
                        error,
                        pairedItem: { item: itemIndex },
                    };
                    returnItems.push(items[itemIndex]);
                }
                else {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The operation "${error.message}" is not supported!`, { itemIndex });
                }
            }
        }
        return [returnItems];
    }
}
exports.Bullmq = Bullmq;
//# sourceMappingURL=Bullmq.node.js.map