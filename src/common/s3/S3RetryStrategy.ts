import { StandardRetryStrategy } from '@aws-sdk/middleware-retry';
import config from '../Config';
export function getRetryStrategy () {
  const maxRetryCount: number = config.getOrDefault('s3.max_retry_count', 5);
  return new StandardRetryStrategy(
    () => Promise.resolve(maxRetryCount)
  );
}
