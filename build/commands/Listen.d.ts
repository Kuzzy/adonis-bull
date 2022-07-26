import { BaseCommand } from '@adonisjs/core/build/standalone'
export default class Listen extends BaseCommand {
  static commandName: string
  static description: string
  static settings: {
    loadApp: boolean
    stayAlive: boolean
  }

  /**
   * Start the bull-board
   */
  board: boolean
  /**
   * Custom port for the bull-board
   */
  port: number
  /**
   * Execute command
   */
  run(): Promise<void>
}
