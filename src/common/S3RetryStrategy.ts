import { StandardRetryStrategy } from '@aws-sdk/middleware-retry';
import config from 'config';
export function getRetryStrategy () {
  const maxRetryCount = config.has('s3.max_retry_count') ? config.get<number>('s3.max_retry_count') : 5;
  return new StandardRetryStrategy(
    () => Promise.resolve(maxRetryCount)
  );
}
