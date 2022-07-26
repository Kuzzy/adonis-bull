"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BullExceptionHandler = void 0;
/**
 * Bull exception handler serves as the base exception handler
 * to handle all exceptions occured during the BULL queue execution
 * lifecycle and makes appropriate response for them.
 */
class BullExceptionHandler {
    constructor(logger) {
        this.logger = logger;
    }
    async handle(error, job) {
        if (typeof error.handle === 'function') {
            return error.handle(error, job);
        }
        this.logger.error(error.message);
    }
}
exports.BullExceptionHandler = BullExceptionHandler;
