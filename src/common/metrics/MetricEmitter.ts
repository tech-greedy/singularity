import Metric from './Metric';
import NoopMetricEmitter from './NoopMetricEmitter';

export default interface MetricEmitter {
  emit(metric: Metric): Promise<void>;
}

export function getMetricEmitter (): MetricEmitter {
  return new NoopMetricEmitter();
}
