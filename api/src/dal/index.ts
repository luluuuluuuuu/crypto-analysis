import { Logger } from 'winston'
import Correlations from './Correlations'
import DBConnection from '../config/DBConnection'

export default class Dal {

  private correlations: Correlations

  constructor(private dbconnection: DBConnection, private logger: Logger) {
    this.correlations = new Correlations(dbconnection, logger)
  }

  public getCorrelations() {
    return this.correlations
  }
}
