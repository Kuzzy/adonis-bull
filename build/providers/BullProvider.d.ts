/// <reference types="@adonisjs/application/build/adonis-typings" />
import { ApplicationContract } from '@ioc:Adonis/Core/Application'
/**
 * Provider to bind bull to the container
 */
export default class BullProvider {
  protected app: ApplicationContract
  constructor(app: ApplicationContract)
  register(): Promise<void>
  shutdown(): Promise<void>
}
