import Metric from './Metric';
import MetricEmitter from './MetricEmitter';

export default class NoopMetricEmitter implements MetricEmitter {
  public emit (_metric: Metric): Promise<void> {
    return Promise.resolve();
  }
}
