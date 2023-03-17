import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';

describe('PostPreparationAppendRequetHandler', () => {
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


  describe('POST /preparation/:id/append', () => {
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .post(`/preparation/${fakeId}/append`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return error if the request is not completed', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/tmp/path',
        outDir: './test-ipld',
        status: 'active',
        maxSize: 1048576,
      })
      const response = await (supertest(service['app']))
        .post(`/preparation/${scanning.id}/append`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.SCANNING_ACTIVE,
        message: ErrorMessage[ErrorCode.SCANNING_ACTIVE]})
    })
    it('should update the underlying scanning request', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/tmp/path',
        outDir: './test-ipld',
        status: 'completed',
        maxSize: 1048576,
        rescanInitiated: false,
        dagGenerationAttempted: true,
      })
      const response = await (supertest(service['app']))
        .post(`/preparation/${scanning.id}/append`).send({ path: '/tmp/path2' });
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        status: 'active',
        id: scanning.id,
        path: '/tmp/path2'
      }))
      const found = await Datastore.ScanningRequestModel.findById(scanning.id);
      expect(found?.status).toEqual('active');
      expect(found?.rescanInitiated).toBeTrue();
      expect(found?.dagGenerationAttempted).toBeFalse();
    })
  });
})
