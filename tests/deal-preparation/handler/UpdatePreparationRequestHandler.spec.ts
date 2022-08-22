import supertest from 'supertest';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';

describe('UpdatePreparationRequestHandler', () => {
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

  describe('POST /preparation/:id', () => {
    it('should change the state of the scanning request', async () => {
      const request = await Datastore.ScanningRequestModel.create({
        status: 'active',
        workerId: 'workerId',
      });
      let response = await supertest(service['app'])
        .post(`/preparation/${request.id}`).send({ action: 'pause' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(request.id)).toEqual(jasmine.objectContaining({
        status: 'paused',
        workerId: null
      }));

      response = await supertest(service['app'])
        .post(`/preparation/${request.id}`).send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(request.id)).toEqual(jasmine.objectContaining({
        status: 'active',
        workerId: null
      }));

      await Datastore.ScanningRequestModel.findByIdAndUpdate(request.id, { status: 'error', errorMessage: 'error message' });
      response = await supertest(service['app'])
        .post(`/preparation/${request.id}`).send({ action: 'retry' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(request.id)).toEqual(jasmine.objectContaining({
        status: 'active',
        workerId: null,
        errorMessage: undefined,
      }));
    })
    it('should return error if database does not exist', async () => {
      const response = await supertest(service['app'])
        .post(`/preparation/${fakeId}`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    });
  });
})
