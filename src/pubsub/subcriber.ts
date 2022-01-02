import { Message, PubSub, Subscription } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, StringMap, toString } from './core';

export interface SubConfig {
  projectId: string;
  subscriptionName: string;
  credentials: CredentialBody | ExternalAccountClientOptions;
}
export function createSubscription(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, subscriptionName: string, logInfo?: (msg: string) => void): Subscription {
  const s = new PubSub({ projectId, credentials }).subscription(subscriptionName);
  checkPermission(s.iam, ['pubsub.subscriptions.consume'], logInfo);
  return s;
}
export function createSubscriber<T>(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, subscriptionName: string, logError?: (msg: string) => void, logInfo?: (msg: string) => void, json?: boolean): Subscriber<T> {
  const s = createSubscription(projectId, credentials, subscriptionName, logInfo);
  return new Subscriber<T>(s, logError, json);
}
export const createConsumer = createSubscriber;
export type Hanlde<T> = (data: T, attributes?: StringMap, raw?: Message) => Promise<number>;
export class Subscriber<T> {
  ack: boolean;
  constructor(
      public subscription: Subscription,
      public logError?: (msg: string) => void,
      public json?: boolean,
      ack?: boolean) {
    this.ack = (ack === false ? false : true);
    this.subscribe = this.subscribe.bind(this);
    this.get = this.get.bind(this);
    this.receive = this.receive.bind(this);
    this.read = this.read.bind(this);
    this.consume = this.consume.bind(this);
  }
  get(handle: Hanlde<T>) {
    return this.subscribe(handle);
  }
  receive(handle: Hanlde<T>) {
    return this.subscribe(handle);
  }
  read(handle: Hanlde<T>) {
    return this.subscribe(handle);
  }
  consume(handle: Hanlde<T>) {
    return this.subscribe(handle);
  }
  subscribe(handle: Hanlde<T>) {
    this.subscription.on('message', (message: Message) => {
      if (this.ack) {
        message.ack();
      }
      const data = (this.json ? JSON.parse(message.data.toString()) : message.data.toString());
      try {
        handle(data, message.attributes, message);
      } catch (err) {
        if (err && this.logError) {
          this.logError('Fail to consume message: ' + toString(err));
        }
      }
    });
    this.subscription.on('error', (err: any) => {
      if (err && this.logError) {
        this.logError('Error: ' + toString(err));
      }
    });
  }
}
export const Consumer = Subscriber;
