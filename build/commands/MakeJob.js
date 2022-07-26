"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const path_1 = require("path");
const fs_1 = __importDefault(require("fs"));
const standalone_1 = require("@adonisjs/core/build/standalone");
const StringTransformer_1 = require("@adonisjs/ace/build/src/Generator/StringTransformer");
class MakeJob extends standalone_1.BaseCommand {
    /**
     * Execute command
     */
    async run() {
        const stub = path_1.join(__dirname, '..', 'templates', 'job.txt');
        const jobName = new StringTransformer_1.StringTransformer(this.name)
            .changeCase('pascalcase')
            .changeForm('singular')
            .toValue();
        const path = this.application.resolveNamespaceDirectory('jobs');
        const rootDir = this.application.cliCwd || this.application.appRoot;
        const jobPath = path_1.join(path || 'app/Jobs', `${jobName}.ts`);
        const exist = fs_1.default.existsSync(jobPath);
        if (exist) {
            this.logger.action('create').skipped(jobPath, 'File already exists');
            return;
        }
        this.generator
            .addFile(jobName)
            .stub(stub)
            .destinationDir(path || 'app/Jobs')
            .useMustache()
            .appRoot(rootDir);
        await this.generator.run();
        const startFolder = this.application.resolveNamespaceDirectory('start');
        const jobsPath = path_1.join(startFolder || 'start', 'jobs.ts');
        const jobsAlreadyExists = fs_1.default.existsSync(jobsPath);
        this.generator.clear();
        const stubStart = path_1.join(__dirname, '..', 'templates', 'start.txt');
        let startJobs = [`App/Jobs/${jobName}`];
        if (jobsAlreadyExists) {
            const currentJobs = (await Promise.resolve().then(() => __importStar(require(path_1.join(rootDir, jobsPath))))).default;
            startJobs = currentJobs.concat(startJobs);
            fs_1.default.unlinkSync(jobsPath);
        }
        this.generator
            .addFile('jobs.ts')
            .stub(stubStart)
            .destinationDir(startFolder || 'start')
            .useMustache()
            .apply({ startJobs })
            .appRoot(this.application.cliCwd || this.application.appRoot);
        await this.generator.run();
    }
}
MakeJob.commandName = 'make:job';
MakeJob.description = 'Make a new Bull job';
__decorate([
    standalone_1.args.string({ description: 'Name of the job class' }),
    __metadata("design:type", String)
], MakeJob.prototype, "name", void 0);
exports.default = MakeJob;
