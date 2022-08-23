import winston from 'winston';
import Logger, { Category } from './Logger';
import config from './Config';
import MetricEmitter, { getMetricEmitter } from './metrics/MetricEmitter';

export default abstract class BaseService {
  public metricEmitter: MetricEmitter;
  public logger: winston.Logger;
  protected enabled: boolean;

  protected constructor (category: Category) {
    this.logger = Logger.getLogger(category);
    this.enabled = config.get(`${category}.enabled`);
    this.metricEmitter = getMetricEmitter();
  }

  public abstract start (): void;
}
