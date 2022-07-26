"use strict";
/// <reference path="../../adonis-typings/bull.ts" />
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const japa_1 = __importDefault(require("japa"));
const fold_1 = require("@adonisjs/fold");
const BullManager_1 = require("../../src/BullManager");
const logger_1 = require("@adonisjs/logger");
const BullExceptionHandler_1 = require("../../src/BullExceptionHandler");
const test_helpers_1 = require("../../test-helpers");
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const CONNECTION_CONFIG = {
    connection: 'local',
    connections: {
        local: {
            host: process.env.REDIS_HOST || '127.0.0.1',
            port: process.env.REDIS_PORT || 6379,
            password: process.env.REDIS_PASSWORD || '',
        },
    },
};
japa_1.default.group('Bull', (group) => {
    group.beforeEach(async () => {
        await test_helpers_1.fs.cleanup();
    });
    japa_1.default('should add a new job', async (assert) => {
        const ioc = new fold_1.Ioc();
        ioc.bind('App/Jobs/TestBull', () => ({
            key: 'TestBull-name',
            concurrency: 2,
            async handle() { },
        }));
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const queue = bull.getByKey(jobDefinition.key);
        const job = await bull.add(jobDefinition.key, data);
        assert.isDefined(job);
        assert.equal(jobDefinition.key, job.name);
        assert.deepEqual(data, job.data);
        assert.equal(queue.concurrency, 2);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should add a new job with events inside Job class', async (assert) => {
        assert.plan(1);
        const ioc = new fold_1.Ioc();
        ioc.singleton('App/Jobs/TestBull', () => {
            return new (class Job {
                constructor() {
                    this.key = 'TestBull-name';
                }
                async handle() { }
                onCompleted() {
                    assert.isOk(true);
                }
            })();
        });
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        bull.getByKey(jobDefinition.key);
        bull.process();
        const job = await bull.add(jobDefinition.key, data);
        await delay(500);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should call boot method when starting job instance', async (assert) => {
        const ioc = new fold_1.Ioc();
        assert.plan(1);
        ioc.bind('App/Jobs/TestBull', () => ({
            key: 'TestBull-name',
            concurrency: 2,
            async handle() { },
            boot(queue) {
                assert.isOk(queue);
            },
        }));
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const job = await bull.add(jobDefinition.key, data);
        bull.process();
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should execute the job handler inside Job class', async (assert) => {
        const ioc = new fold_1.Ioc();
        const expectedResponse = Date.now();
        ioc.singleton('App/Jobs/TestBull', () => {
            return new (class Job {
                constructor() {
                    this.key = 'TestBull-name';
                }
                async handle() {
                    return expectedResponse;
                }
            })();
        });
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const queue = bull.getByKey(jobDefinition.key);
        bull.process();
        let job = await bull.add(jobDefinition.key, data);
        await delay(500);
        job = (await queue.bull.getJob(job.id));
        assert.deepEqual(job.data, data);
        assert.deepEqual(job.returnvalue, expectedResponse);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should handle the exception raised by the handler inside Job class', async (assert) => {
        assert.plan(4);
        const ioc = new fold_1.Ioc();
        ioc.singleton('App/Jobs/TestBull', () => {
            return new (class Job {
                constructor() {
                    this.key = 'TestBull-name';
                }
                async handle() {
                    throw new Error('Error with the current job');
                }
            })();
        });
        const logger = new logger_1.FakeLogger({});
        class FakeExceptionHandler extends BullExceptionHandler_1.BullExceptionHandler {
            constructor() {
                super(logger);
            }
            async handle(error, job) {
                assert.equal(error.message, 'Error with the current job');
                assert.isDefined(job);
            }
        }
        ioc.bind('App/Exceptions/BullHandler', () => new FakeExceptionHandler());
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const queue = bull.getByKey(jobDefinition.key);
        bull.process();
        let job = await bull.add(jobDefinition.key, data);
        await delay(500);
        job = (await queue.bull.getJob(job.id));
        assert.deepEqual(job.data, data);
        assert.isNull(job.returnvalue);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should schedule a new job', async (assert) => {
        const ioc = new fold_1.Ioc();
        ioc.bind('App/Jobs/TestBull', () => new (class Job {
            constructor() {
                this.key = 'TestBull-name';
            }
            async handle() { }
        })());
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const job = await bull.schedule(jobDefinition.key, data, 1000);
        assert.equal(jobDefinition.key, job.name);
        assert.equal(job.opts.delay, 1000);
        assert.deepEqual(data, job.data);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should schedule a new job with Date', async (assert) => {
        const ioc = new fold_1.Ioc();
        ioc.bind('App/Jobs/TestBull', () => new (class Job {
            constructor() {
                this.key = 'TestBull-name';
            }
            async handle() { }
        })());
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        const job = await bull.schedule(jobDefinition.key, data, new Date(Date.now() + 1000));
        assert.equal(jobDefinition.key, job.name);
        assert.equal(job.opts.delay, 1000);
        assert.deepEqual(data, job.data);
        await job.remove();
        await bull.shutdown();
    });
    japa_1.default('should not schedule when time is invalid', async (assert) => {
        const ioc = new fold_1.Ioc();
        ioc.bind('App/Jobs/TestBull', () => ({
            key: 'TestBull-name',
            async handle() { },
        }));
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        assert.throw(() => {
            bull.schedule(jobDefinition.key, data, -100);
        }, 'Invalid schedule time');
        await bull.shutdown();
    });
    japa_1.default('should remove a scheduled job', async (assert) => {
        const ioc = new fold_1.Ioc();
        ioc.bind('App/Jobs/TestBull', () => ({
            key: 'TestBull-name',
            async handle() { },
        }));
        const logger = new logger_1.FakeLogger({});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        await bull.schedule(jobDefinition.key, data, 1000, { jobId: '1' });
        await bull.remove(jobDefinition.key, '1');
        const job = await bull.getByKey(jobDefinition.key).bull.getJob('1');
        assert.isUndefined(job);
        await bull.shutdown();
    });
    japa_1.default('should call the logger when exception handler is not defined', async (assert) => {
        assert.plan(2);
        const ioc = new fold_1.Ioc();
        ioc.singleton('App/Jobs/TestBull', () => {
            return new (class Job {
                constructor() {
                    this.key = 'TestBull-name';
                }
                async handle() {
                    throw new Error('Error with the current job');
                }
            })();
        });
        const logger = new test_helpers_1.MyFakeLogger(assert, {});
        const bull = new BullManager_1.BullManager(ioc, logger, CONNECTION_CONFIG, [
            'App/Jobs/TestBull',
        ]);
        const jobDefinition = ioc.use('App/Jobs/TestBull');
        const data = { test: 'data' };
        bull.process();
        const job = await bull.add(jobDefinition.key, data);
        await delay(500);
        assert.isNull(job.returnvalue);
        await job.remove();
        await bull.shutdown();
    });
});
