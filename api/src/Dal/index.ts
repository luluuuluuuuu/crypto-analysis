import { Logger } from 'winston';
import { DBConnection } from '../Config';
import Correlations from './Correlations';

export default class Dal {
  private correlations: Correlations;

  constructor(private dbconnection: DBConnection, private logger: Logger) {
    this.correlations = new Correlations(dbconnection, logger);
  }

  public getCorrelations() {
    return this.correlations;
  }
}
