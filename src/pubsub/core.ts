import { CallOptions, IAM, PubSub } from '@google-cloud/pubsub';
import { CredentialBody, ExternalAccountClientOptions } from 'google-auth-library';

export function createPubSub(projectId: string, credentials: CredentialBody | ExternalAccountClientOptions): PubSub {
  const pubsub = new PubSub({ projectId, credentials });
  return pubsub;
}
export interface StringMap {
  [key: string]: string;
}
export function toString(v: any, attributes?: StringMap): string {
  if (attributes) {
    const ks = Object.keys(attributes);
    if (ks.length > 0) {
      if (typeof v === 'string') {
        return v + JSON.stringify(attributes);
      } else {
        return JSON.stringify(v) + ' ' + JSON.stringify(attributes);
      }
    } else {
      return ts(v);
    }
  } else {
    return ts(v);
  }
}
function ts(v: any): string {
  if (typeof v === 'string') {
    return v;
  } else {
    return JSON.stringify(v);
  }
}
export async function checkPermission(iam: IAM, permissionString: string[], log?: (msg: string) => void) {
  if (!log) {
    log = console.log;
  }
  const opt: CallOptions = {
    timeout: 30000,
  };
  log(`Checking permission: ${JSON.stringify(permissionString)}`);
  try {
    const permissions = await iam.testPermissions(permissionString, opt);
    if (permissions.length > 0) {
      log(`Permission ${JSON.stringify(permissions)} valid`);
    } else {
      log(`Permission ${JSON.stringify(permissions)} invalid`);
    }
  } catch (err) {
    log(`Cannot check permission ${permissionString}: ${toString(err)}`);
  }
}
