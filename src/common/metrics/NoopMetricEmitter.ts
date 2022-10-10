import Metric from './Metric';
import { Emitter } from './MetricEmitter';

export default class NoopMetricEmitter implements Emitter {
  public emit (_metric: Metric): Promise<void> {
    return Promise.resolve();
  }
}
