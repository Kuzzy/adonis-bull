"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const japa_1 = __importDefault(require("japa"));
const test_helpers_1 = require("../../test-helpers");
const BullManager_1 = require("../../src/BullManager");
japa_1.default.group('Bull Provider', (group) => {
    group.afterEach(async () => {
        await test_helpers_1.fs.cleanup();
    });
    japa_1.default('register bull provider', async (assert) => {
        const app = await test_helpers_1.setupApplication();
        assert.instanceOf(app.container.use('Rocketseat/Bull'), BullManager_1.BullManager);
    });
});
