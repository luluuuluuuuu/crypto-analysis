import bodyParser from 'body-parser'
import { Router, Express, Request, Response } from 'express'

export default (app: Express): Express => {
  const router = Router()
  router.get('/hello', (req: Request, res: Response) => res.send('Hello World!'))
  app.use('/api', router)
  app.use(bodyParser.json())
  app.use(bodyParser.urlencoded({
    extended: false,
  }))

  return app
}
