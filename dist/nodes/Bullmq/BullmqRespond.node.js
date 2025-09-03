"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BullmqRespond = void 0;
const n8n_workflow_1 = require("n8n-workflow");
const utils_1 = require("./utils");
const GenericFuntions_1 = require("./GenericFuntions");
class BullmqRespond {
    constructor() {
        this.description = {
            displayName: 'BullMQ Respond',
            name: 'bullmqRespond',
            icon: 'file:bullmq.svg',
            group: ['output'],
            version: 1,
            description: 'Respond to Bullmq jobs',
            defaults: {
                name: 'BullMQ Respond',
            },
            inputs: ['main'],
            outputs: ['main'],
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
                            name: 'Respond',
                            value: 'respond',
                            description: 'Respond to a job',
                            action: 'Respond to a job',
                        },
                    ],
                    default: 'respond',
                },
                {
                    displayName: 'Data Source',
                    name: 'dataSource',
                    type: 'options',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                        },
                    },
                    options: [
                        {
                            name: 'Previous Node',
                            value: 'previousNode',
                            description: 'Get data from previous node',
                        },
                        {
                            name: 'Input',
                            value: 'input',
                            description: 'Use data from the input',
                        },
                    ],
                    default: 'previousNode',
                },
                {
                    displayName: 'Job Data',
                    name: 'jobData',
                    type: 'assignmentCollection',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                            dataSource: ['input'],
                        },
                    },
                    default: {},
                },
                {
                    displayName: "Use Current Job's Data",
                    name: 'useCurrentJobData',
                    type: 'boolean',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                        },
                    },
                    default: true,
                    description: 'Whether to use the current job data to respond to the job. If set to false, the data from the input will be used.',
                },
                {
                    displayName: 'Queue Name',
                    name: 'queueName',
                    type: 'string',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                            useCurrentJobData: [false],
                        },
                    },
                    default: '',
                    required: true,
                    description: 'Queue name to add the job to',
                },
                {
                    displayName: 'Job ID',
                    name: 'jobId',
                    type: 'string',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                            useCurrentJobData: [false],
                        },
                    },
                    default: '',
                    required: true,
                    description: 'Job ID to respond to',
                },
                {
                    displayName: 'Lock Token',
                    name: 'lockToken',
                    type: 'string',
                    displayOptions: {
                        show: {
                            operation: ['respond'],
                            useCurrentJobData: [false],
                        },
                    },
                    default: '',
                    description: 'Lock token of the job, if the job is locked then get the lock from Bullmq Trigger node',
                },
            ],
        };
        this.methods = {
            credentialTest: { redisConnectionTest: GenericFuntions_1.redisConnectionTest },
        };
    }
    async execute() {
        const credentials = await this.getCredentials('redis');
        const connection = (0, utils_1.setupRedisClient)(credentials);
        const operation = this.getNodeParameter('operation', 0);
        const returnItems = [];
        try {
            if (['respond'].includes(operation)) {
                const items = this.getInputData();
                for (let itemIndex = 0; itemIndex < items.length; itemIndex++) {
                    const item = { json: {}, pairedItem: { item: itemIndex } };
                    switch (operation) {
                        case 'respond':
                            const data = this.getNodeParameter('jobData', itemIndex, {});
                            const dataSource = this.getNodeParameter('dataSource', itemIndex, 'previousNode');
                            const { queueName, jobId, lockToken } = await getJobInfo.call(this, itemIndex);
                            if (!queueName || !jobId || !lockToken) {
                                throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Queue Name, Job ID and Lock Token must be provided!');
                            }
                            const queue = await GenericFuntions_1.getQueue.call(this, queueName, { connection });
                            const cleanup = async () => {
                                queue.close();
                                queue.disconnect();
                            };
                            const job = await queue.getJob(jobId);
                            if (!job) {
                                cleanup();
                                throw new n8n_workflow_1.NodeOperationError(this.getNode(), `Job with ID "${jobId}" does not exist!`);
                            }
                            job.log(`Job is about to be responded from executionId ${this.getExecutionId()}`);
                            const jobData = dataSource === 'input'
                                ? (0, utils_1.parseAssignmentsCollection)(data, {})
                                : (0, utils_1.parseJson)(items[itemIndex].json, {});
                            await job.moveToCompleted(jobData, lockToken);
                            item.json = job.toJSON();
                            items[itemIndex] = item;
                            returnItems.push(items[itemIndex]);
                            cleanup();
                            break;
                        default:
                            throw new n8n_workflow_1.NodeOperationError(this.getNode(), `The operation "${operation}" is not supported!`);
                    }
                }
            }
        }
        catch (error) {
            throw error;
        }
        return [returnItems];
    }
}
exports.BullmqRespond = BullmqRespond;
function getJobInfo(itemIndex) {
    const useCurrentJobData = this.getNodeParameter('useCurrentJobData', itemIndex, true);
    if (useCurrentJobData) {
        throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Using current job data is not yet supported. Please disable "Use Current Job Data" option.');
    }
    const queueName = this.getNodeParameter('queueName', itemIndex, '');
    const jobId = this.getNodeParameter('jobId', itemIndex, '');
    const lockToken = this.getNodeParameter('lockToken', itemIndex, '');
    return { queueName, jobId, lockToken };
}
//# sourceMappingURL=BullmqRespond.node.js.map