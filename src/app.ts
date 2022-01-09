import { merge } from 'config-plus';
import dotenv from 'dotenv';
import express, { json } from 'express';
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
  const produce = ctx.produce;
  if (produce) {
    app.post('/send', (req, res) => {
      produce(req.body).then(r => res.json({ message: 'message was produced'}))
        .catch(err => res.json({ error: err }));
    });
  }
  http.createServer(app).listen(conf.port, () => {
    console.log('Start server at port ' + conf.port);
  });
});
