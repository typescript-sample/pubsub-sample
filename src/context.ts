import { StringMap } from 'mongodb-extension';
import { User } from './models/User';

export interface ApplicationContext {
  handle: (data: User, header?: StringMap) => Promise<number>;
  subscribe: (handle: (data: User, attributes?: StringMap) => Promise<number>) => void;
}
