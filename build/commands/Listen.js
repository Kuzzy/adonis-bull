"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
Object.defineProperty(exports, "__esModule", { value: true });
const standalone_1 = require("@adonisjs/core/build/standalone");
class Listen extends standalone_1.BaseCommand {
    /**
     * Execute command
     */
    async run() {
        const bull = this.application.container.use('Rocketseat/Bull');
        console.log({ board: this.board });
        bull.process();
        if (this.board ||
            (typeof this.board === 'undefined' && typeof this.port !== 'undefined')) {
            bull.ui(this.port);
        }
    }
}
Listen.commandName = 'bull:listen';
Listen.description = 'Start the Bull listener';
Listen.settings = {
    loadApp: true,
    stayAlive: true,
};
__decorate([
    standalone_1.flags.boolean({ description: "Run bull's dashboard", alias: 'b' }),
    __metadata("design:type", Boolean)
], Listen.prototype, "board", void 0);
__decorate([
    standalone_1.flags.number({
        description: "Run bull's dashboard in the provided port",
        alias: 'p',
    }),
    __metadata("design:type", Number)
], Listen.prototype, "port", void 0);
exports.default = Listen;
