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
      this.run.bind(this)();
    }
    process.on('SIGINT', async () => {
      console.error('SIGINT received. Flushing metrics...');
      await this.flushAll();
      process.exit();
    });
    process.on('SIGTERM', async () => {
      console.error('SIGTERM received. Flushing metrics...');
      await this.flushAll();
      process.exit();
    });
  }

  private async run () :Promise<void> {
    this.logger.info('Starting metric emitter');
    while (true) {
      await this.flushAll();
      await sleep(60 * 1000);
    }
  }

  public async flush (): Promise<void> {
    if (this.events.length === 0) {
      return;
    }
    const release = await this.mutex.acquire();
    try {
      const toPublish = this.events.slice(0, 1000);
      this.logger.debug(`Publishing ${toPublish.length} events`);
      const json = JSON.stringify(toPublish);
      const compressed = await compress(Buffer.from(json, 'utf8'));
      await axios.post(this.url, compressed.toString('base64'), {
        headers: {
          'Content-Type': 'text/plain'
        }
      });
      this.events.splice(0, toPublish.length);
    } catch (e) {
      this.logger.warn(`Failed to publish ${this.events.length} events`);
    } finally {
      release();
    }
  }

  public async flushAll (): Promise<void> {
    while (this.events.length > 0) {
      await this.flush();
    }
  }

  public async emit (metric: Metric, timeOverride?: Date): Promise<void> {
    const date = timeOverride?.getTime() || Date.now();
    const timestamp = Math.floor(date / 1000);
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
