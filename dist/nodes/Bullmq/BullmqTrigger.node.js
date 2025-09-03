"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BullmqTrigger = void 0;
const n8n_workflow_1 = require("n8n-workflow");
const utils_1 = require("./utils");
const bullmq_1 = require("bullmq");
const GenericFuntions_1 = require("./GenericFuntions");
class BullmqTrigger {
    constructor() {
        this.description = {
            displayName: 'BullMQ Trigger',
            name: 'bullmqTrigger',
            icon: 'file:bullmq.svg',
            group: ['trigger'],
            version: 1,
            description: 'Register new workers for Bullmq',
            defaults: {
                name: 'BullMQ Trigger',
            },
            inputs: [],
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
                    displayName: "Queue Source",
                    name: "queueSource",
                    type: "options",
                    options: [
                        {
                            name: "Workflows",
                            value: "workflows",
                        },
                        {
                            name: "Custom",
                            value: "custom",
                        },
                    ],
                    default: "workflows",
                },
                {
                    displayName: 'Queue Name',
                    name: 'queueName',
                    type: 'string',
                    default: '',
                    required: true,
                    description: 'Queue name to add the job to',
                    displayOptions: {
                        show: {
                            queueSource: [
                                "custom"
                            ]
                        },
                    },
                },
                {
                    displayName: 'Job Name',
                    name: 'jobName',
                    type: 'string',
                    default: '',
                    required: true,
                    description: 'Job name to publish',
                },
                {
                    displayName: 'Respond Type',
                    name: 'respondType',
                    type: 'options',
                    default: 'useLastNode',
                    options: [
                        {
                            name: 'Use Last Node',
                            value: 'useLastNode',
                        },
                        {
                            name: 'Immediate',
                            value: 'immediate',
                        },
                        {
                            name: 'Use Respond Node',
                            value: 'useRespondNode',
                        },
                    ],
                },
                {
                    displayName: 'Options',
                    name: 'options',
                    type: 'collection',
                    placeholder: 'Add option',
                    default: {},
                    options: [
                        {
                            displayName: 'Lock Duration',
                            name: 'lockDuration',
                            type: 'number',
                            default: 60000,
                            description: 'The duration in milliseconds for which the lock should be held',
                        },
                        {
                            displayName: 'Concurrency',
                            name: 'concurrency',
                            type: 'number',
                            default: 1,
                            description: 'The number of jobs that can be processed concurrently',
                        },
                        {
                            displayName: 'Run Retry Delay',
                            name: 'runRetryDelay',
                            type: 'number',
                            default: 5000,
                            description: 'The duration in milliseconds to wait before retrying a job',
                        },
                        {
                            displayName: 'Only Data',
                            name: 'onlyData',
                            type: 'boolean',
                            default: true,
                            description: 'Whether to only return the data of the job',
                        },
                    ],
                },
            ],
        };
        this.methods = {
            credentialTest: { redisConnectionTest: GenericFuntions_1.redisConnectionTest },
        };
    }
    async trigger() {
        const credentials = await this.getCredentials('redis');
        const workflowId = this.getWorkflow().id;
        const queueNameParam = this.getNodeParameter('queueName', '');
        const respondType = this.getNodeParameter('respondType');
        const options = this.getNodeParameter('options');
        const { lockDuration = 60000, concurrency = 1, onlyData = false, runRetryDelay = 0 } = options;
        const queueName = queueNameParam || workflowId;
        if (!queueName) {
            throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Queue Name must be set');
        }
        const processJob = async (job, token, donePromise) => {
            var _a;
            const payload = onlyData ? { data: job.data } : job.toJSON();
            if (((_a = job === null || job === void 0 ? void 0 : job.data) === null || _a === void 0 ? void 0 : _a.step) === 'waiting') {
                throw new bullmq_1.DelayedError('Job is waiting');
            }
            job.log(`Job received, executionId ${this.getExecutionId()}, responding type ${respondType}`);
            const payloadWithToken = { ...payload, lockToken: token };
            if (respondType === 'immediate') {
                return {
                    respondType: 'immediate',
                };
            }
            if (!token) {
                throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Token is missing');
            }
            const dataItems = this.helpers.returnJsonArray(payloadWithToken);
            this.emit([dataItems], undefined, donePromise);
        };
        const connection = (0, utils_1.setupRedisClient)(credentials);
        const workerName = this.getWorkflow().name;
        let worker;
        const manualTriggerFunction = () => {
            return new Promise((resolve) => {
                worker = (0, utils_1.createWorker)(queueName, async (job, token) => {
                    processJob(job, token);
                    resolve();
                }, {
                    lockDuration: 60000,
                    connection,
                    autorun: false,
                    name: workerName,
                    concurrency: 1,
                });
                worker.run();
            });
        };
        if (this.getMode() === 'trigger') {
            worker = (0, utils_1.createWorker)(queueName, async (job, token) => {
                const donePromise = respondType === 'useLastNode'
                    ? await this.helpers.createDeferredPromise()
                    : undefined;
                await processJob(job, token, donePromise);
                if (respondType === 'immediate') {
                    return {
                        respondType: 'immediate',
                        executionId: this.getExecutionId(),
                    };
                }
                if (!token) {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Token is missing');
                }
                if (respondType === 'useRespondNode') {
                    await job.extendLock(token, lockDuration);
                    console.log('Job is deleted and being released from the response node');
                    throw new bullmq_1.DelayedError('Job was triggered');
                }
                if (!donePromise) {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Done promise is missing');
                }
                const result = await donePromise.promise();
                const lastNodeResult = result.data.resultData;
                const executionStatus = result.status;
                job.log(`Job is about to be released, executionId ${this.getExecutionId()}, status ${executionStatus}`);
                if (lastNodeResult.error) {
                    const lastNodeError = lastNodeResult.error;
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), lastNodeError);
                }
                const lastNodeExecuted = lastNodeResult.lastNodeExecuted;
                if (!lastNodeExecuted) {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Last node executed is missing');
                }
                const lastNodeExecutionResult = lastNodeResult.runData[lastNodeExecuted];
                if (!lastNodeExecutionResult) {
                    throw new n8n_workflow_1.NodeOperationError(this.getNode(), 'Last node execution data is missing');
                }
                const lastNodeExecutionResultData = (0, utils_1.extractNodeExecutionResultData)(lastNodeExecutionResult);
                return lastNodeExecutionResultData;
            }, {
                lockDuration,
                connection,
                autorun: false,
                name: workerName,
                concurrency,
                runRetryDelay,
            });
            worker.run();
        }
        async function closeFunction() {
            if (worker) {
                await worker.close();
            }
        }
        return {
            closeFunction,
            manualTriggerFunction,
        };
    }
}
exports.BullmqTrigger = BullmqTrigger;
//# sourceMappingURL=BullmqTrigger.node.js.map