import { Request, Response, Router } from 'express';
import { Logger } from 'winston';
import { DBConnection } from '../config';
import Dal from '../Dal';

export default class AnalysisContoller {
  private dal: Dal;
  private router: Router;

  constructor(private dbConnection: DBConnection, private logger: Logger) {
    this.dal = new Dal(dbConnection, logger);
    this.router = Router();
    this.init();
  }

  public getRouter = () => {
    return this.router;
  }

  private init = () => {
    this.router.get('/correlations', async (req: Request, res: Response) => {
      const data = await this.dal.getCorrelations().getAll();
      res.send(data);
    });
  }
}
