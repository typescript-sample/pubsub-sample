import { PubSub, Topic } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, createPubSub, StringMap } from './core';

export interface PubConfig {
  projectId: string;
  topicName: string;
  credentials: CredentialBody | ExternalAccountClientOptions;
}
export function createTopic(topicName: string, projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, log?: (msg: string) => void): Topic {
  const t = new PubSub({ projectId, credentials }).topic(topicName);
  checkPermission(t.iam, ['pubsub.topics.publish'], log);
  return t;
}
export function createPublisher<T>(topicName: string, projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, log?: (msg: string) => void): Publisher<T> {
  const t = createTopic(topicName, projectId, credentials, log);
  return new Publisher<T>(t);
}
export class Publisher<T> {
  constructor(public topic: Topic) {
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
export function createSimplePublisher<T>(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions): SimplePublisher<T> {
  const p = createPubSub(projectId, credentials);
  return new SimplePublisher<T>(p);
}
export class SimplePublisher<T> {
  constructor(public pubsub: PubSub) {
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
