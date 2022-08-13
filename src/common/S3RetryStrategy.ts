import { StandardRetryStrategy } from '@aws-sdk/middleware-retry';
import config from './Config';
export function getRetryStrategy () {
  const maxRetryCount: number = config.get<number>('s3.max_retry_count');
  return new StandardRetryStrategy(
    () => Promise.resolve(maxRetryCount)
  );
}
