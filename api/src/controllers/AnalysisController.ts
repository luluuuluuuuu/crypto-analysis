import { Router, Request, Response } from 'express'
import { Logger } from 'winston'
import DBConnection from '../config/DBConnection'

export default class AnalysisContoller {

  private router: Router

  constructor(
    private dbConnection: DBConnection,
    private logger: Logger,
  ) {
    this.router = Router()
    this.init()
  }

  private init() {
    this.router.get('/hello', (req: Request, res: Response) => res.send('Hello World!'))
  }

  public getRouter() {
    return this.router
  }
}
