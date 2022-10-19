import LambdaMetricEmitter from '../../../src/common/metrics/LambdaMetricEmitter';
import axios from 'axios';
import { ConfigInitializer } from '../../../src/common/Config';
import { MetricEvent } from '../../../src/common/metrics/Metric';
import { compress } from '@xingrz/cppzst';
import { randomUUID } from 'crypto';

describe('LambdaMetricEmitter', () => {
  beforeAll(() => {
    ConfigInitializer.instanceId = 'test';
  })
  afterAll(() => {
    ConfigInitializer.instanceId = 'unknown';
  })
  describe('testdata', () => {
    it('should return some test data', async () => {
      const events: MetricEvent[] = [];
      for (let i = 0; i < 1000; i++) {
        events.push({
          type: 'test',
          timestamp: Math.floor(new Date().getTime() / 1000),
          instance: 'test',
          values: {
            i: i,
            random: randomUUID()
          }
        });
      }
      const json = JSON.stringify(events);
      const compressed = await compress(Buffer.from(json, 'utf8'));
      const data = compressed.toString('base64');
      console.log(data);
    })
  })
  describe('emit', () => {
    it('should push new metrics to the queue', () => {
      const emitter = new LambdaMetricEmitter('http://localhost:8080', false);
      emitter.emit({
        type: 'test',
        values: {
          test: 1
        }
      });
      expect(emitter['events'].length).toBe(1);
    });
  })
  describe('flush', () => {
    it('should post all metrics to the API', async () => {
      const emitter = new LambdaMetricEmitter('http://localhost:8080', false);
      emitter.emit({
        type: 'test1',
        values: {
          test: 1
        }
      });
      emitter.emit({
        type: 'test2',
        values: {
          test: 2
        }
      });
      const axiosSpy = spyOn<any>(axios, 'post').and.callFake((_: any, data: string) => {
        console.log(data);
        return Promise.resolve();
      });
      await emitter.flushAll();
      expect(emitter['events'].length).toBe(0);
      expect(axiosSpy).toHaveBeenCalledWith('http://localhost:8080', jasmine.any(String),
        { headers: { 'Content-Type': 'text/plain' } });
    })
  });
})
