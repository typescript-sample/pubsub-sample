import { HealthController } from 'express-ext';
import { JSONLogger, LogConfig } from 'logger-core';
import { Db } from 'mongodb';
import { MongoChecker, MongoInserter } from 'mongodb-extension';
import { createRetry, ErrorHandler, Handler, NumberMap, RetryConfig, RetryService, StringMap } from 'mq-one';
import { Attributes, Validator } from 'xvalidators';
import { createPublisher, createPubSubChecker, createSubscriber, PubConfig, SubConfig } from './pubsub';

export interface User {
  id: string;
  username: string;
  email?: string;
  phone?: string;
  dateOfBirth?: Date;
}
export const user: Attributes = {
  id: {
    length: 40
  },
  username: {
    required: true,
    length: 255
  },
  email: {
    format: 'email',
    required: true,
    length: 120
  },
  phone: {
    format: 'phone',
    required: true,
    length: 14
  },
  dateOfBirth: {
    type: 'datetime'
  }
};

export interface Config {
  port?: number;
  log: LogConfig;
  retries: NumberMap;
  sub: SubConfig;
  retry: RetryConfig;
  pub?: PubConfig;
}
export interface ApplicationContext {
  health: HealthController;
  handle: (data: User, header?: StringMap) => Promise<number>;
  subscribe: (handle: (data: User, header?: StringMap) => Promise<number>) => void;
}

export function createContext(db: Db, conf: Config): ApplicationContext {
  const retries = createRetry(conf.retries);
  const logger = new JSONLogger(conf.log.level, conf.log.map);
  const mongoChecker = new MongoChecker(db);
  const pubsubChecker = createPubSubChecker(conf.sub.projectId, conf.sub.credentials, conf.sub.subscriptionName);
  const health = new HealthController([mongoChecker, pubsubChecker]);

  const subscriber = createSubscriber<User>(conf.sub.projectId, conf.sub.credentials, conf.sub.subscriptionName);

  const validator = new Validator<User>(user, true);
  const writer = new MongoInserter(db.collection('users'), 'id');
  // const retryWriter = new RetryWriter(writer.write, retries, writeUser, log);
  const errorHandler = new ErrorHandler(logger.error);
  let handler: Handler<User, string>;
  if (conf.pub) {
    const publisher = createPublisher<User>(conf.pub.topicName, conf.pub.projectId, conf.pub.credentials, logger.info);
    const retryService = new RetryService<User, string>(publisher.publish, logger.error, logger.info);
    handler = new Handler<User, string>(writer.write, validator.validate, [], errorHandler.error, logger.error, logger.info, retryService.retry, conf.retry.limit, conf.retry.name);
  } else {
    handler = new Handler<User, string>(writer.write, validator.validate, retries, errorHandler.error, logger.error, logger.info);
  }
  const ctx: ApplicationContext = { health, subscribe: subscriber.subscribe, handle: handler.handle };
  return ctx;
}
export function writeUser(msg: User): Promise<number> {
  console.log('Error: ' + JSON.stringify(msg));
  return Promise.resolve(1);
}
