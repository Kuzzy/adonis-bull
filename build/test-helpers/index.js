"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.setupApplication = exports.MyFakeLogger = exports.fs = void 0;
const path_1 = require("path");
const dev_utils_1 = require("@poppinss/dev-utils");
const application_1 = require("@adonisjs/application");
const logger_1 = require("@adonisjs/logger");
exports.fs = new dev_utils_1.Filesystem(path_1.join(__dirname, 'app'));
class MyFakeLogger extends logger_1.FakeLogger {
    constructor(assert, config, pino) {
        super(config, pino);
        this.assert = assert;
    }
    error(message) {
        this.assert.isTrue(message.includes('name=TestBull-name id='));
    }
}
exports.MyFakeLogger = MyFakeLogger;
async function setupApplication(environment = 'test') {
    await exports.fs.add('.env', '');
    await exports.fs.add('config/app.ts', `
    export const appKey = 'averylong32charsrandomsecretkey'
    export const http = {
        cookie: {},
        trustProxy: () => true,
    }
    `);
    await exports.fs.add('app/Jobs/SomeJob.ts', `
    export default class SomeJob {
      public key = 'SomeJob-key'

      public async handle () {
        return 'good luck'
      }
    }
  `);
    await exports.fs.add('start/jobs.ts', `
    export default ['App/SomeJob']
  `);
    await exports.fs.add('config/bull.ts', `
    import { BullConfig } from '@ioc:Rocketseat/Bull'

    const bullConfig  = {
        connection: 'local',

        connections: {
            local: {
              host: 'localhost',
              port: 6379,
              password: '',
              db: 0,
              keyPrefix: '',
            },
        },
    } as unknown as BullConfig

    export default bullConfig
    `);
    const app = new application_1.Application(exports.fs.basePath, environment, {
        providers: ['@adonisjs/core', '../../providers/BullProvider'],
    });
    await app.setup();
    await app.registerProviders();
    await app.bootProviders();
    return app;
}
exports.setupApplication = setupApplication;
