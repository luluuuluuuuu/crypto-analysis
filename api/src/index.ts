import { AddressInfo } from 'net';
import App from './App';
import { DBConnection } from './Config';
import Controller from './Controller';
import Logger from './Logger';

const PORT = process.env.SERVER_PORT || 8090;
const DB_SETTINGS = {
  user: process.env.POSTGRES_USER,
  host: process.env.DB_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: parseInt(process.env.DB_PORT),
};

const init = async () => {
  const dbConnection = new DBConnection(DB_SETTINGS, Logger);
  await dbConnection.getConnection();

  const app = await App(Controller(dbConnection, Logger));
  const server = app.listen(PORT, () => {
    const addressInfo = server.address() as AddressInfo;
    const host =
      (addressInfo.address !== '::' && addressInfo.address) || 'localhost';
    const port = addressInfo.port;
    Logger.info(`Listening on http://${host}:${port}`);
  });

  process.on('exit', async () => {
    Logger.warn('SVC: Shutting down');
    await dbConnection.release();
    Logger.warn('SVC: Exited');
  });
};

init();
