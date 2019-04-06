import errorhandler from 'errorhandler';
import express, { Express } from 'express';

declare type Provider = (app: Express) => Express;

const app = express();

if (process.env.NODE_ENV !== 'production') {
  app.use(errorhandler());
}

export default (...providers: Provider[]): Express =>
  providers.reduce((acc: Express, current: Function) => current(acc), app) ||
  app;
