import { json } from 'body-parser';
import { merge } from 'config-plus';
import dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import { connectToDb } from 'mongodb-extension';
import { config, env } from './config';
import { useContext } from './context';

dotenv.config();
const conf = merge(config, process.env, env, process.env.ENV);

const app = express();
app.use(json());

connectToDb(`${conf.mongo.uri}`, `${conf.mongo.db}`).then(db => {
  const ctx = useContext(db, conf);
  ctx.consume(ctx.handle);
  app.get('/health', ctx.health.check);
  app.patch('/log', ctx.log.config);
  http.createServer(app).listen(conf.port, () => {
    console.log('Start server at port ' + conf.port);
  });
});
