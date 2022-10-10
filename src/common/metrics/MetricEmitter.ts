import Metric, { MetricEvent } from './Metric';
import NoopMetricEmitter from './NoopMetricEmitter';
import config, { ConfigInitializer } from '../Config';
import { sleep } from '../Util';
import axios from 'axios';
import Logger, { Category } from '../Logger';
import { compress } from '@xingrz/cppzst';

export interface Emitter {
  emit(metric: Metric): Promise<void>;
}

export class LambdaMetricEmitter implements Emitter {
  private url: string;
  private logger;
  private events: MetricEvent[] = [];

  constructor (url: string) {
    this.url = url;
    this.logger = Logger.getLogger(Category.MetricEmitter);
    this.run();
  }

  private async run () :Promise<void> {
    this.logger.info('Starting metric emitter');
    while (true) {
      if (this.events.length === 0) {
        await sleep(5 * 1000);
        continue;
      }
      try {
        this.logger.debug(`Publishing ${this.events.length} events to ${this.url}`);
        const json = JSON.stringify(this.events);
        const compressed = await compress(Buffer.from(json, 'utf8'));
        await axios.post(this.url, compressed);
        this.events = [];
      } catch (e) {
        this.logger.warn(`Failed to publish ${this.events.length} events`, e);
      }

      await sleep(60 * 1000);
    }
  }

  public async emit (metric: Metric): Promise<void> {
    const timestamp = Math.floor(Date.now() / 1000);
    const instance = ConfigInitializer.instanceId;
    const ip = ConfigInitializer.publicIp;
    this.events.push({
      timestamp,
      ip,
      instance,
      type: metric.type,
      values: metric.values
    });
  }
}

export default class MetricEmitter {
  private static instance: Emitter | undefined;
  public static Instance (): Emitter {
    if (!MetricEmitter.instance) {
      if (config.getOrDefault('metrics.enabled', true)) {
        MetricEmitter.instance = new LambdaMetricEmitter('https://n6i4jttsjo33athqkevvljml6i0zzpoc.lambda-url.us-west-2.on.aws');
      } else {
        MetricEmitter.instance = new NoopMetricEmitter();
      }
    }

    return MetricEmitter.instance;
  }
}
