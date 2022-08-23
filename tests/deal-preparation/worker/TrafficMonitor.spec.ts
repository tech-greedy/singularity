import TrafficMonitor from '../../../src/deal-preparation/worker/TrafficMonitor';
import { sleep } from '../../../src/common/Util';

describe('TrafficMonitor', () => {
  it('should count the download speed', async () => {
    const monitor = new TrafficMonitor(1000);
    monitor.countNewChunk(new Int8Array(888));
    expect(monitor.downloaded).toEqual(888);
    monitor.countNewChunk(new Int8Array(888 ));
    expect(monitor.downloaded).toEqual(888 * 2);
    await sleep(100);
    monitor.countNewChunk(new Int8Array(888 ));
    expect(monitor.downloaded).toEqual(888 * 3);
    await sleep(1000);
    monitor.countNewChunk(new Int8Array(888));
    expect(monitor.downloaded).toEqual(888);
  })
})
