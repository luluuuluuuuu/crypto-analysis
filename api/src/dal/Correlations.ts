import { Logger } from 'winston';
import { DBConnection } from '../config';

export default class Correlations {
  constructor(private db: DBConnection, private logger: Logger) {}

  public getAll = async (): Promise<CorrelationsDAO[]> => {
    const sql = `SELECT * FROM output.crypto_correlation`;
    try {
      const connection = await this.db.getConnection();
      const result = await connection
        .query(sql)
        .catch(e => this.logger.error(e));
      const correlations = result.rows.map(x => x as CorrelationsDAO);
      return correlations;
    } catch (e) {
      this.logger.error(`Failed to load correlations: ${e}`);
    }
  }
}

export interface CorrelationsDAO {
  readonly crypto: string;
  readonly AEON: number;
  readonly BAY: number;
  readonly BTC: number;
  readonly BTS: number;
  readonly BURST: number;
  readonly CRW: number;
  readonly DASH: number;
  readonly DGB: number;
  readonly DOGE: number;
  readonly EMC2: number;
  readonly ETH: number;
  readonly FCT: number;
  readonly FTC: number;
  readonly GAME: number;
  readonly GRS: number;
  readonly LTC: number;
  readonly MONA: number;
  readonly NAV: number;
  readonly NLG: number;
  readonly POT: number;
  readonly PPC: number;
  readonly RDD: number;
  readonly SYS: number;
  readonly VIA: number;
  readonly VTC: number;
  readonly XCP: number;
  readonly XMR: number;
  readonly XRP: number;
}
