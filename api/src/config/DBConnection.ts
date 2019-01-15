import { Pool, PoolClient, PoolConfig } from 'pg'
import { Logger } from 'winston'

export default class DBConnection {

  private pool: Pool
  private client: PoolClient
  private CONNECTION_RETRY = 1000 * 5
  private retrycount = 0
  private promise: Promise<any>
  private resolver

  constructor(
    private config: PoolConfig,
    private logger: Logger,
  ) {
    this.logger.info('DBConnection: Instance')

    this.promise = new Promise((resolve, reject) => {
      this.resolver = resolve
    })
    this.makeNewConnection()
  }

  public getConnection() {
    return this.promise
  }

  private async makeNewConnection() {

    this.logger.info('DBConnection: Connecting...')

    try {

      this.pool = new Pool(this.config)

      this.pool.on('error', (err: Error) => {
        this.logger.error('DBConnection:', err)
        this.retry()
      })

      this.client = await this.pool.connect()

      this.client.on('error', (err: Error) => {
        this.logger.error('DBConnection:', err)
      })

      this.resolver(this.client)
      this.logger.info('DBConnection: Connected')
      this.retrycount = 0

    } catch (e) {
      this.logger.error('DBConnection:', e)
      this.retry()
    }
  }

  private retry() {
    this.retrycount += 1
    const interval = Math.min(2 * 60 * 1000, this.retrycount * this.CONNECTION_RETRY)
    this.logger.error(`DBConnection: Attemping reconnection in ${interval / 1000} seconds...`)
    setTimeout(() => this.getConnection(), interval)
  }

  public async release() {
    this.logger.info('DBConnection: Releasing...')
    this.client.release()
    await this.pool.end()
    this.logger.info('DBConnection: Released')
  }

}
