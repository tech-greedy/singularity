import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';

describe('GetPreparationRequestHandler', () => {
  let service: DealPreparationService;
  const fakeId = '62429da5002efca9dd13d380';
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
  describe('GET /preparation/:id', () => {
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get(`/preparation/${fakeId}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get('/preparation/fakeid');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return all generation requests', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'path',
        status: 'completed',
        minSize: 0,
        maxSize: 10
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'active',
        index: 0,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10,
        fileList: [{
          path: '/data/file1.mp4',
          start: 0,
          end: 0,
          size: 100
        }]
      });
      const response = await (supertest(service['app']))
        .get('/preparation/' + scanning.id);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        name: 'name',
        path: 'path',
        scanningStatus: 'completed',
        minSize: 0,
        maxSize: 10,
        generationTotal: 1,
        generationActive: 1,
        generationPaused: 0,
        generationCompleted: 0,
        generationError: 0
      }));
      expect(response.body.generationRequests.length).toEqual(1);
      expect(response.body.generationRequests[0]).toEqual(jasmine.objectContaining({
        index: 0,
        status: 'active',
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10
      }));
    });
  });
})
