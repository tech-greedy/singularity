import BaseService from '../src/common/BaseService';
import { Category } from '../src/common/Logger';
import Utils from './Utils';
import Datastore from '../src/common/Datastore';
import any = jasmine.any;
import { sleep } from '../src/common/Util';
import { GenerationProcessor } from '../src/deal-preparation/worker/GenerationProcessor';

class DummyService extends BaseService {
  constructor () {
    super(Category.DealPreparationService);
  }

  start (): void {
  }
}

describe('BaseService', () => {
  let service: DummyService;
  let defaultTimeout: number;

  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DummyService();
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
  });

  afterAll(async () => {
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  })

  beforeEach(async () => {
    await Datastore.HealthCheckModel.deleteMany();
  });

  describe('initialize', () => {
    it('should initialize without error', async () => {
      await service.initialize(() => Promise.resolve(false));
      const found = await Datastore.HealthCheckModel.findOne();
      expect(found).toEqual(jasmine.objectContaining({
        type: 'deal_preparation_service',
        state: 'idle',
        downloadSpeed: 0,
        pid: any(Number),
        workerId: any(String),
      }));
    });
    it('should update the database with usage information', async () => {
      let aborted = false;
      GenerationProcessor.childProcessPid = process.pid;
      await service.initialize(() => Promise.resolve(aborted));
      await sleep(1000);
      const found = await Datastore.HealthCheckModel.findOne();
      expect(found).toEqual(jasmine.objectContaining({
        type: 'deal_preparation_service',
        state: 'idle',
        downloadSpeed: 0,
        pid: any(Number),
        workerId: any(String),
        cpuUsage: any(Number),
        memoryUsage: any(Number),
        childPid: any(Number),
        childCpuUsage: any(Number),
        childMemoryUsage: any(Number)
      }));
      aborted = true;
    })
  })
})
