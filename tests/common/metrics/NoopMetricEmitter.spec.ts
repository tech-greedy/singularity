import NoopMetricEmitter from '../../../src/common/metrics/NoopMetricEmitter';

describe('NoopMetricEmitter', () => {
  it('should emit metrics', async () => {
    const emitter = new NoopMetricEmitter();
    await emitter.emit({
      type: 'type',
      values: {
        key: 'value'
      }
    });
  })
})
