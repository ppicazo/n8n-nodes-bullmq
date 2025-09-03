"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupRedisClient = setupRedisClient;
exports.convertInfoToObject = convertInfoToObject;
exports.createWorker = createWorker;
exports.parseJson = parseJson;
exports.craftJobReturnValue = craftJobReturnValue;
exports.parseAssignmentsCollection = parseAssignmentsCollection;
exports.extractNodeExecutionResultData = extractNodeExecutionResultData;
const ioredis_1 = __importDefault(require("ioredis"));
const bullmq_1 = require("bullmq");
function setupRedisClient(credentials) {
    const redisOptions = {
        host: credentials.host,
        port: credentials.port,
        db: credentials.database,
        password: credentials.password || undefined,
        maxRetriesPerRequest: null,
    };
    const connection = new ioredis_1.default(redisOptions);
    return connection;
}
function getParsedValue(value) {
    if (value.match(/^[\d\.]+$/) === null) {
        return value;
    }
    else {
        return parseFloat(value);
    }
}
function convertInfoToObject(stringData) {
    const returnData = {};
    let key, value;
    for (const line of stringData.split('\n')) {
        if (['#', ''].includes(line.charAt(0))) {
            continue;
        }
        [key, value] = line.split(':');
        if (key === undefined || value === undefined) {
            continue;
        }
        value = value.trim();
        if (value.includes('=')) {
            returnData[key] = {};
            let key2, value2;
            for (const keyValuePair of value.split(',')) {
                [key2, value2] = keyValuePair.split('=');
                returnData[key][key2] = getParsedValue(value2);
            }
        }
        else {
            returnData[key] = getParsedValue(value);
        }
    }
    return returnData;
}
function createWorker(queueName, handler, extraOptions) {
    const worker = new bullmq_1.Worker(queueName, handler, extraOptions);
    return worker;
}
function parseJson(jsonString, fallback) {
    try {
        if (typeof jsonString === 'object') {
            return jsonString;
        }
        return JSON.parse(jsonString);
    }
    catch (error) {
        return fallback;
    }
}
function craftJobReturnValue(json) {
    if (typeof json !== 'object') {
        return json;
    }
    return {
        data: json.data,
    };
}
function parseAssignmentsCollection(collection, fallback) {
    try {
        const returnData = Object.fromEntries(collection.assignments.map(({ name, value }) => {
            return [name, value];
        }));
        return returnData;
    }
    catch (error) {
        return fallback;
    }
}
function extractNodeExecutionResultData(executionResult) {
    if (!executionResult) {
        return {};
    }
    if (executionResult.length === 1) {
        return extractItemExecutionResultData(executionResult[0]);
    }
    return {
        items: executionResult.map((res) => extractItemExecutionResultData(res, 'main')),
    };
}
function extractItemExecutionResultData(executionResult, output = 'main') {
    if (!executionResult) {
        return {};
    }
    if ((executionResult === null || executionResult === void 0 ? void 0 : executionResult.data) === undefined) {
        return {};
    }
    const mainData = executionResult === null || executionResult === void 0 ? void 0 : executionResult.data[output];
    if (mainData === undefined) {
        return {};
    }
    const returnData = extractConnectionExecutionData(mainData);
    return returnData;
}
function extractConnectionExecutionData(data) {
    if (!data) {
        return {};
    }
    if (data.length === 1) {
        return extractOutputDataExecutionData(data[0]);
    }
    return {
        items: data.map(extractOutputDataExecutionData),
    };
}
function extractOutputDataExecutionData(data) {
    if (!data || data.length === 0) {
        return {};
    }
    if (data.length === 1) {
        return extractSingleOutputDataExecutionData(data[0]);
    }
    return {
        items: data.map(extractSingleOutputDataExecutionData),
    };
}
function extractSingleOutputDataExecutionData(data) {
    if (data.json === undefined) {
        return {};
    }
    return data.json;
}
//# sourceMappingURL=utils.js.map