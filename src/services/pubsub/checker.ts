import { PubSub, Subscription } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission } from './core';

export interface AnyMap {
  [key: string]: any;
}
export interface HealthChecker {
  name(): string;
  build(data: AnyMap, error: any): AnyMap;
  check(): Promise<AnyMap>;
}

export class PubSubChecker {
  subscription: Subscription;
  constructor(
    projectId: string,
    credentials: CredentialBody | ExternalAccountClientOptions,
    subscriptionName: string,
    public service?: string, private timeout?: number
  ) {
    this.subscription = new PubSub({ projectId, credentials }).subscription(subscriptionName);
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  check(): Promise<AnyMap> {
    const obj = {} as AnyMap;
    const promise = new Promise<any>(async (resolve, reject) => {
      try {
        await checkPermission(this.subscription.iam, ['pubsub.subscriptions.consume'], undefined);
        resolve(obj);
      } catch (err) {
        reject(`pubsub is down`);
      }
    });
    if (!this.timeout) {
      this.timeout = 4200;
    }
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    } else {
      return promise;
    }
  }
  name(): string {
    if (!this.service) {
      this.service = 'kafka';
    }
    return this.service;
  }
  build(data: AnyMap, err: any): AnyMap {
    if (err) {
      if (!data) {
        data = {} as AnyMap;
      }
      data['error'] = err;
    }
    return data;
  }
}

function promiseTimeOut(timeoutInMilliseconds: number, promise: Promise<any>): Promise<any> {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    })
  ]);
}
