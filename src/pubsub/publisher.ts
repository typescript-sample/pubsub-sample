import { PubSub, Topic } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';
import { checkPermission, createPubSub, StringMap } from './core';

export function createTopic(topicName: string, projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, log?: (msg: any) => void): Topic {
  const topic = new PubSub({ projectId, credentials }).topic(topicName);
  checkPermission(topic.iam, ['pubsub.topics.publish'], log);
  return topic;
}
export function createPublisher<T>(topicName: string, projectId: string, credentials: CredentialBody | ExternalAccountClientOptions, log?: (msg: any) => void): Publisher<T> {
  const topic = createTopic(topicName, projectId, credentials, log);
  return new Publisher<T>(topic);
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
  const pubsub = createPubSub(projectId, credentials);
  return new SimplePublisher<T>(pubsub);
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
