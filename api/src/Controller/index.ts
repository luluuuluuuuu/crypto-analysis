import bodyParser from 'body-parser';
import { Express, Router } from 'express';
import { Logger } from 'winston';
import DBConnection from '../config/DBConnection';
import AnalysisController from './AnalysisController';

export default (dbConnection: DBConnection, logger: Logger) => (
  app: Express,
): Express => {
  const router = Router();
  const analysisController = new AnalysisController(dbConnection, logger);

  router.use('/analysis', analysisController.getRouter());
  app.use('/api', router);
  app.use(bodyParser.json());
  app.use(
    bodyParser.urlencoded({
      extended: false,
    }),
  );

  return app;
};
