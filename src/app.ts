import { json } from 'body-parser';
import { merge } from 'config-plus';
import dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import { connectToDb } from 'mongodb-extension';
import { config } from './config';
import { createContext } from './context';

dotenv.config();
const conf = merge(config, process.env);

const app = express();
app.use(json());

connectToDb(`${conf.mongo.uri}`, `${conf.mongo.db}`).then(db => {
  const ctx = createContext(db, conf);
  ctx.subscribe(ctx.handle);
  app.get('/health', ctx.health.check);
  http.createServer(app).listen(conf.port, () => {
    console.log('Start server at port ' + conf.port);
  });
});
