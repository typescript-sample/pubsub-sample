import { PubSub, Topic } from '@google-cloud/pubsub';
import { MessageOptions } from '@google-cloud/pubsub/build/src/topic';
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
export const createProducer = createPublisher;
export const createWriter = createPublisher;
export const createSender = createPublisher;
export class Publisher<T> {
  constructor(public topic: Topic) {
    this.publish = this.publish.bind(this);
    this.send = this.send.bind(this);
    this.put = this.put.bind(this);
    this.write = this.write.bind(this);
    this.produce = this.produce.bind(this);
  }
  send(data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(data, attributes);
  }
  put(data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(data, attributes);
  }
  write(data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(data, attributes);
  }
  produce(data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(data, attributes);
  }
  publish(data: T, attributes?: StringMap): Promise<[string]> {
    return new Promise((resolve, reject) => {
      const dataBuffer = toBuffer(data);
      const message: MessageOptions = {
        data: dataBuffer,
        attributes
      };
      this.topic.publishMessage(message).then(messageId => {
        resolve(messageId);
      }).catch(err => {
        reject(err);
      });
    });
  }
}
export const Producer = Publisher;
export const Sender = Publisher;
export const Writer = Publisher;

export function toBuffer(d: any): Buffer {
  return (typeof d === 'string' ? Buffer.from(d) : Buffer.from(JSON.stringify(d)));
}
export function createSimplePublisher<T>(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions): SimplePublisher<T> {
  const p = createPubSub(projectId, credentials);
  return new SimplePublisher<T>(p);
}
export const createSimpleProducer = createSimplePublisher;
export const createSimpleWriter = createSimplePublisher;
export const createSimpleSender = createSimplePublisher;
// tslint:disable-next-line:max-classes-per-file
export class SimplePublisher<T> {
  constructor(public pubsub: PubSub) {
    this.publish = this.publish.bind(this);
    this.send = this.send.bind(this);
    this.put = this.put.bind(this);
    this.write = this.write.bind(this);
    this.produce = this.produce.bind(this);
  }
  send(topicName: string, data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(topicName, data, attributes);
  }
  put(topicName: string, data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(topicName, data, attributes);
  }
  write(topicName: string, data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(topicName, data, attributes);
  }
  produce(topicName: string, data: T, attributes?: StringMap): Promise<[string]> {
    return this.publish(topicName, data, attributes);
  }
  publish(topicName: string, data: T, attributes?: StringMap): Promise<[string]> {
    return new Promise((resolve, reject) => {
      const topic = this.pubsub.topic(topicName);
      const dataBuffer = toBuffer(data);
      const message: MessageOptions = {
        data: dataBuffer,
        attributes
      };
      topic.publishMessage(message).then(messageId => {
        resolve(messageId);
      }).catch(err => {
        reject(err);
      });
    });
  }
}
export const SimpleProducer = SimplePublisher;
export const SimpleSender = SimplePublisher;
export const SimpleWriter = SimplePublisher;
