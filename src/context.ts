import { HealthController, LogController } from 'express-ext';
import { Config, createLogger, map } from 'logger-core';
import { Db } from 'mongodb';
import { Attributes, MongoChecker, MongoUpserter } from 'mongodb-extension';
import { Consume, createRetry, ErrorHandler, Handle, Handler, NumberMap, RetryConfig, RetryService } from 'mq-one';
import { Validator } from 'xvalidators';
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
    type: 'datetime',
    column: 'date_of_birth'
  }
};

export interface Conf {
  log: Config;
  retries: NumberMap;
  sub: SubConfig;
  retry: RetryConfig;
  pub?: PubConfig;
}
export interface ApplicationContext {
  health: HealthController;
  log: LogController;
  publish?: (data: User) => Promise<[string]>;
  subscribe: Consume<User>;
  handle: Handle<User>;
}
export function useContext(db: Db, conf: Conf): ApplicationContext {
  const retries = createRetry(conf.retries);
  const logger = createLogger(conf.log);
  const log = new LogController(logger, map);
  const mongoChecker = new MongoChecker(db);
  const pubsubChecker = createPubSubChecker(conf.sub.projectId, conf.sub.credentials, conf.sub.subscriptionName);
  const health = new HealthController([mongoChecker, pubsubChecker]);

  const subscriber = createSubscriber<User>(conf.sub.projectId, conf.sub.credentials, conf.sub.subscriptionName);
  const validator = new Validator<User>(user, true);
  const writer = new MongoUpserter(db.collection('users'), 'id');
  // const retryWriter = new RetryWriter(writer.write, retries, writeUser, log);
  const errorHandler = new ErrorHandler(logger.error);
  let handler: Handler<User, [string]>;
  let publish: ((data: User) => Promise<[string]>)|undefined;
  if (conf.pub) {
    const publisher = createPublisher<User>(conf.pub.topicName, conf.pub.projectId, conf.pub.credentials, logger.info);
    const retryService = new RetryService<User, [string]>(publisher.publish, logger.error, logger.info);
    handler = new Handler<User, [string]>(writer.write, validator.validate, [], errorHandler.error, logger.error, logger.info, retryService.retry, conf.retry.limit, conf.retry.name);
    publish = publisher.publish;
  } else {
    handler = new Handler<User, [string]>(writer.write, validator.validate, retries, errorHandler.error, logger.error, logger.info);
  }
  return { health, log, publish, subscribe: subscriber.subscribe, handle: handler.handle };
}
export function writeUser(msg: User): Promise<number> {
  console.log('Error: ' + JSON.stringify(msg));
  return Promise.resolve(1);
}
