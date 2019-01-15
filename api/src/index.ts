import { AddressInfo } from 'net'
import Controller from './controllers'
import App from './App'
import logger from './logger'

const app = App(Controller)

const PORT = process.env.SERVER_PORT || 8090

const server = app.listen(PORT, () => {
  const addressInfo = server.address() as AddressInfo
  const host = (addressInfo.address !== '::' && addressInfo.address) || 'localhost'
  const port = addressInfo.port
  logger.info(`Listening on http://${host}:${port}`)
})
