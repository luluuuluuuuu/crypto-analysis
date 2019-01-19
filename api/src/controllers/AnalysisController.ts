import { Router, Request, Response } from 'express'
import { Logger } from 'winston'
import DBConnection from '../config/DBConnection'
import Dal from '../dal'

export default class AnalysisContoller {

  private dal: Dal
  private router: Router

  constructor(
    private dbConnection: DBConnection,
    private logger: Logger,
  ) {
    this.dal = new Dal(dbConnection, logger)
    this.router = Router()
    this.init()
  }

  private init() {
    this.router.get('/correlations', async (req: Request, res: Response) => {
      const data = await this.dal.getCorrelations().getAll()
      res.send(data)
    })
  }

  public getRouter() {
    return this.router
  }
}
