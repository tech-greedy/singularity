import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';

describe('ListPreparationRequestHandler', () => {
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
  });

  describe('GET /preparations', () => {
    it('should return even if some status has zero requests', async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'path',
        status: 'completed',
        minSize: 0,
        maxSize: 10
      });
      const response = await (supertest(service['app']))
        .get('/preparations');
      expect(response.status).toEqual(200);
      expect(response.body.length).toEqual(1);
      expect(response.body[0]).toEqual(jasmine.objectContaining({
        name: 'name',
        path: 'path',
        minSize: 0,
        maxSize: 10,
        scanningStatus: 'completed',
        generationTotal: 0,
        generationActive: 0,
        generationPaused: 0,
        generationCompleted: 0,
        generationError: 0
      }));
    });
    it('should return all scanning requests with aggregated stats', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'path',
        status: 'completed',
        minSize: 0,
        maxSize: 10
      });
      for (let i = 0; i <= 3; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'active'
        });
      }
      for (let i = 0; i <= 4; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'paused'
        });
      }
      for (let i = 0; i <= 5; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'completed'
        });
      }
      for (let i = 0; i <= 6; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'error'
        });
      }
      const response = await (supertest(service['app']))
        .get('/preparations');
      expect(response.status).toEqual(200);
      expect(response.body.length).toEqual(1);
      expect(response.body[0]).toEqual(jasmine.objectContaining({
        name: 'name',
        path: 'path',
        minSize: 0,
        maxSize: 10,
        scanningStatus: 'completed',
        generationTotal: 22,
        generationActive: 4,
        generationPaused: 5,
        generationCompleted: 6,
        generationError: 7
      }));
    });
  });
})
