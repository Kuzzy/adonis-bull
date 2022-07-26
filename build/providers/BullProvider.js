"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/**
 * Provider to bind bull to the container
 */
class BullProvider {
    constructor(app) {
        this.app = app;
    }
    async register() {
        this.app.container.bind('Rocketseat/Bull/BullExceptionHandler', () => {
            const { BullExceptionHandler } = require('../src/BullExceptionHandler');
            return BullExceptionHandler;
        });
        this.app.container.singleton('Rocketseat/Bull', () => {
            const app = this.app.container.use('Adonis/Core/Application');
            const config = this.app.container
                .use('Adonis/Core/Config')
                .get('bull', {});
            const Logger = this.app.container.use('Adonis/Core/Logger');
            const jobs = require(app.startPath('jobs'))?.default || [];
            const { BullManager } = require('../src/BullManager');
            return new BullManager(this.app.container, Logger, config, jobs);
        });
        this.app.container.alias('Rocketseat/Bull', 'Bull');
    }
    async shutdown() {
        await this.app.container
            .use('Rocketseat/Bull')
            .shutdown();
    }
}
exports.default = BullProvider;
