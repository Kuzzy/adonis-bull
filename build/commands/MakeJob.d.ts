import { BaseCommand } from '@adonisjs/core/build/standalone'
export default class MakeJob extends BaseCommand {
  static commandName: string
  static description: string
  /**
   * The name of the job file.
   */
  name: string
  /**
   * Execute command
   */
  run(): Promise<void>
}
