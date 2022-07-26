import { BaseCommand } from '@adonisjs/core/build/standalone'
export default class MakeException extends BaseCommand {
  static commandName: string
  static description: string
  /**
   * Execute command
   */
  run(): Promise<void>
}
