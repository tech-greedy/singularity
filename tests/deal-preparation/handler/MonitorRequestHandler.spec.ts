import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';

describe('MonitorRequestHandler', () => {
  let service: DealPreparationService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealPreparationService();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
    await Datastore.HealthCheckModel.deleteMany();
  });
  describe('GET /monitor', () => {
    it('should return worker stats', async () => {
      await Datastore.HealthCheckModel.create({
        workerId: 'workerId',
        downloadSpeed: 100,
        state: 'idle',
        pid: 1,
        cpuUsage: 2,
        memoryUsage: 3,
        childPid: 4,
        childCpuUsage: 5,
        childMemoryUsage: 6,
        type: 'type'
      });
      const response = await (supertest(service['app'])).get('/monitor');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual([jasmine.objectContaining({
        downloadSpeed: 100,
        workerId: 'workerId',
        state: 'idle',
        pid: 1,
        cpuUsage: 2,
        memoryUsage: 3,
        childPid: 4,
        childCpuUsage: 5,
        childMemoryUsage: 6,
        type: 'type'
      })]);
    })
  });
})
