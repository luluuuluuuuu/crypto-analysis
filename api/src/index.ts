import { AddressInfo } from 'net'
import Controller from './controllers'
import App from './App'
import logger from './logger'
import DBConnection from './config/DBConnection'

const PORT = process.env.SERVER_PORT || 8090
const DB_SETTINGS = {
  user: process.env.POSTGRES_USER,
  host: process.env.DB_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: parseInt(process.env.DB_PORT),
}

const init = async () => {
  const dbConnection = new DBConnection(DB_SETTINGS, logger)
  await dbConnection.getConnection()

  const app = await App(Controller(dbConnection, logger))
  const server = app.listen(PORT, () => {
    const addressInfo = server.address() as AddressInfo
    const host = (addressInfo.address !== '::' && addressInfo.address) || 'localhost'
    const port = addressInfo.port
    logger.info(`Listening on http://${host}:${port}`)
  })

  process.on('exit', async () => {
    logger.warn('SVC: Shutting down')
    await dbConnection.release()
    logger.warn('SVC: Exited')
  })
}

init()
