import {json} from 'body-parser';
import dotenv from 'dotenv';
import express from 'express';
import http from 'http';
import { connectToDb } from 'mongodb-extension';
import {createContext} from './init';
import {route} from './route';

dotenv.config();

const app = express();

const port = process.env.PORT;
const mongoURI = process.env.MONGO_URI;
const mongoDB = process.env.MONGO_DB;

app.use(json());

connectToDb(`${mongoURI}`, `${mongoDB}`).then(db => {
  const ctx = createContext(db);
  ctx.subscribe(ctx.handle);
  route(app, ctx);
  http.createServer(app).listen(port, () => {
    console.log('Start server at port ' + port);
  });
});
