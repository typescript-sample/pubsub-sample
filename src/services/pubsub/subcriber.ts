import { PubSub, Subscription } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, StringMap, toString } from './core';

export class SimpleSubscriber<T> {
  subscription: Subscription;
  constructor(
      projectId: string,
      credentials: CredentialBody | ExternalAccountClientOptions,
      subscriptionName: string,
      public logError?: (msg: any) => void,
      public logInfo?: (msg: any) => void,
      public json?: boolean) {
    this.subscription = new PubSub({ projectId, credentials }).subscription(subscriptionName);
    checkPermission(this.subscription.iam, ['pubsub.subscriptions.consume'], this.logInfo);
    this.subscribe = this.subscribe.bind(this);
  }
  subscribe(handle: (data: T, attributes?: StringMap) => Promise<number>): void {
    this.subscription.on('message', (message: any) => {
      message.ack();
      // console.log(message);
      const data = (this.json ? JSON.parse(message.data.toString()) : message.data.toString());
      try {
        handle(data, message.attributes);
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
