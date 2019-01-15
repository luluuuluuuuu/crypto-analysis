import express from 'express'
import bodyParser from 'body-parser'
import { AddressInfo } from 'net'

const PORT = process.env.SERVER_PORT || 8090

const app = express()

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({
  extended: false,
}))

const server = app.listen(PORT, () => {
  const addressInfo = server.address() as AddressInfo
  const host = (addressInfo.address !== '::' && addressInfo.address) || 'localhost'
  const port = addressInfo.port
  console.log('Listening on https://%s:%s', host, port)
})
