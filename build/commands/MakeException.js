"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = require("path");
const standalone_1 = require("@adonisjs/core/build/standalone");
class MakeException extends standalone_1.BaseCommand {
    /**
     * Execute command
     */
    async run() {
        const stub = path_1.join(__dirname, '..', 'templates', 'handler.txt');
        const path = this.application.resolveNamespaceDirectory('exceptions');
        const rootDir = this.application.cliCwd || this.application.appRoot;
        this.generator
            .addFile('BullHandler.ts')
            .stub(stub)
            .destinationDir(path || 'app/Exceptions')
            .useMustache()
            .appRoot(rootDir);
        await this.generator.run();
    }
}
exports.default = MakeException;
MakeException.commandName = 'bull:exception';
MakeException.description = 'Make a Bull exception handle file';
