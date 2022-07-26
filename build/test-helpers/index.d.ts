import { Filesystem } from '@poppinss/dev-utils'
import { Application } from '@adonisjs/application'
import { FakeLogger } from '@adonisjs/logger'
export declare const fs: Filesystem
export declare class MyFakeLogger extends FakeLogger {
  assert: any
  constructor(assert: any, config: any, pino?: any)
  error(message: string): void
}
export declare function setupApplication(
  environment?: 'web' | 'repl' | 'test'
): Promise<Application>
