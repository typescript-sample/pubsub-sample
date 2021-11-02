import { PubSub, Topic } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, StringMap } from './core';

export class Publisher<T> {
  private topic: Topic;
  constructor(
    private topicName: string,
    private projectId: string,
    private credentials: CredentialBody | ExternalAccountClientOptions,
    private log?: (msg: any) => void) {
    this.topic = new PubSub({ projectId: this.projectId, credentials: this.credentials }).topic(this.topicName);
    checkPermission(this.topic.iam, ['pubsub.topics.publish'], this.log);
    this.publish = this.publish.bind(this);
  }
  publish(data: T, attributes?: StringMap): Promise<string> {
    return new Promise((resolve, reject) => {
      this.topic.publishJSON(data as any, attributes).then(messageId => {
        resolve(messageId);
      }).catch(err => {
        reject(err);
      });
    });
  }
}
export class SimplePublisher<T> {
  pubsub: PubSub;
  constructor(
    private projectId: string,
    private credentials: CredentialBody | ExternalAccountClientOptions) {
    this.pubsub = new PubSub({ projectId: this.projectId, credentials: this.credentials });
    this.publish = this.publish.bind(this);
  }
  publish(topicName: string, data: T, attributes?: StringMap): Promise<string> {
    return new Promise((resolve, reject) => {
      const topic = this.pubsub.topic(topicName);
      topic.publishJSON(data as any, attributes).then(messageId => {
        resolve(messageId);
      }).catch(err => {
        reject(err);
      });
    });
  }
}
