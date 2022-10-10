import Metric from './Metric';
import NoopMetricEmitter from './NoopMetricEmitter';
import config from '../Config';
import LambdaMetricEmitter from './LambdaMetricEmitter';

export interface Emitter {
  emit(metric: Metric): Promise<void>;
}

/* istanbul ignore next */
export default class MetricEmitter {
  private static instance: Emitter | undefined;
  public static Instance (): Emitter {
    if (!MetricEmitter.instance) {
      if (config.getOrDefault('metrics.enabled', true)) {
        MetricEmitter.instance = new LambdaMetricEmitter('https://n6i4jttsjo33athqkevvljml6i0zzpoc.lambda-url.us-west-2.on.aws?prod=true');
      } else {
        MetricEmitter.instance = new NoopMetricEmitter();
      }
    }

    return MetricEmitter.instance;
  }
}
