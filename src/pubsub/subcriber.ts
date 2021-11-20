import { Message, PubSub, Subscription } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, StringMap, toString } from './core';

export function createSubscription(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, subscriptionName: string, logInfo?: (msg: any) => void): Subscription {
  const s = new PubSub({ projectId, credentials }).subscription(subscriptionName);
  checkPermission(s.iam, ['pubsub.subscriptions.consume'], logInfo);
  return s;
}
export function createSubscriber<T>(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, subscriptionName: string, logError?: (msg: any) => void, logInfo?: (msg: any) => void, json?: boolean): Subscriber<T> {
  const s = createSubscription(projectId, credentials, subscriptionName, logInfo);
  return new Subscriber<T>(s, logError, json);
}
export class Subscriber<T> {
  constructor(
      public subscription: Subscription,
      public logError?: (msg: any) => void,
      public json?: boolean) {
    this.subscribe = this.subscribe.bind(this);
  }
  subscribe(handle: (data: T, attributes?: StringMap, raw?: Message) => Promise<number>): void {
    this.subscription.on('message', (message: Message) => {
      message.ack();
      // console.log(message);
      const data = (this.json ? JSON.parse(message.data.toString()) : message.data.toString());
      try {
        handle(data, message.attributes, message);
      } catch (err) {
        if (err && this.logError) {
          this.logError('Fail to consume message: ' + toString(err));
        }
      }
    });
    this.subscription.on('error', (err) => {
      if (err && this.logError) {
        this.logError('Error: ' + toString(err));
      }
    });
  }
}
