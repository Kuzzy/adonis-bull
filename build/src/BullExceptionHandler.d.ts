/// <reference types="@adonisjs/logger/build/adonis-typings/logger" />
import { LoggerContract } from '@ioc:Adonis/Core/Logger'
import { Job } from 'bullmq'
/**
 * Bull exception handler serves as the base exception handler
 * to handle all exceptions occured during the BULL queue execution
 * lifecycle and makes appropriate response for them.
 */
export declare abstract class BullExceptionHandler {
  protected logger: LoggerContract
  constructor(logger: LoggerContract)
  handle(error: any, job: Job): Promise<any>
}
