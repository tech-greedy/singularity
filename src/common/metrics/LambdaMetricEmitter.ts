import Metric, { MetricEvent } from './Metric';
import Logger, { Category } from '../Logger';
import { Mutex } from 'async-mutex';
import { sleep } from '../Util';
import { compress } from '@xingrz/cppzst';
import axios from 'axios';
import { ConfigInitializer } from '../Config';
import { Emitter } from './MetricEmitter';

export default class LambdaMetricEmitter implements Emitter {
  private url: string;
  private logger;
  private events: MetricEvent[] = [];
  private mutex;

  /* istanbul ignore next */
  constructor (url: string, start = true) {
    this.url = url;
    this.logger = Logger.getLogger(Category.MetricEmitter);
    this.mutex = new Mutex();
    if (start) {
      this.run();
    }
    process.on('SIGINT', async () => {
      console.error('SIGINT received. Flushing metrics...');
      await this.flush();
      process.exit();
    });
    process.on('SIGTERM', async () => {
      console.error('SIGTERM received. Flushing metrics...');
      await this.flush();
      process.exit();
    });
  }

  private async run () :Promise<void> {
    this.logger.info('Starting metric emitter');
    while (true) {
      await this.flush();
      await sleep(60 * 1000);
    }
  }

  public async flush (): Promise<void> {
    if (this.events.length === 0) {
      return;
    }
    const release = await this.mutex.acquire();
    try {
      this.logger.debug(`Publishing ${this.events.length} events to ${this.url}`);
      const json = JSON.stringify(this.events);
      const compressed = await compress(Buffer.from(json, 'utf8'));
      await axios.post(this.url, compressed.toString('base64'), {
        headers: {
          'Content-Type': 'text/plain'
        }
      });
      this.events = [];
    } catch (e) {
      this.logger.warn(`Failed to publish ${this.events.length} events`, e);
    } finally {
      release();
    }
  }

  public async emit (metric: Metric): Promise<void> {
    const timestamp = Math.floor(Date.now() / 1000);
    const instance = ConfigInitializer.instanceId;
    if (instance === 'unknown') {
      this.logger.warn('Instance ID is unknown. Not emitting metric');
      return;
    }
    this.events.push({
      timestamp,
      instance,
      type: metric.type,
      values: metric.values
    });
  }
}
