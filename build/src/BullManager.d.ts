/// <reference types="@adonisjs/logger/build/adonis-typings/logger" />
import { IocContract } from '@adonisjs/fold'
import { LoggerContract } from '@ioc:Adonis/Core/Logger'
import {
  BullManagerContract,
  QueueContract,
  BullConfig,
} from '@ioc:Rocketseat/Bull'
import { JobsOptions, Job as BullJob } from 'bullmq'
export declare class BullManager implements BullManagerContract {
  protected container: IocContract
  protected Logger: LoggerContract
  protected config: BullConfig
  protected jobs: string[]
  constructor(
    container: IocContract,
    Logger: LoggerContract,
    config: BullConfig,
    jobs: string[]
  )

  private _queues
  private _shutdowns
  get queues(): any
  private _getEventListener
  getByKey(key: string): QueueContract
  add<T>(
    key: string,
    data: T,
    jobOptions?: JobsOptions
  ): Promise<BullJob<any, any>>

  schedule<T = any>(
    key: string,
    data: T,
    date: number | Date,
    options?: JobsOptions
  ): Promise<BullJob<any, any, string>>

  remove(key: string, jobId: string): Promise<void>
  ui(port?: number): void
  process(): this
  private handleException
  shutdown(): Promise<void>
}
