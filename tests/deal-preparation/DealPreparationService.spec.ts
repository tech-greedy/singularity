import supertest from 'supertest';
import Datastore from '../../src/common/Datastore';
import DealPreparationService from '../../src/deal-preparation/DealPreparationService';
import ErrorCode from '../../src/deal-preparation/ErrorCode';
import Utils from '../Utils';

describe('DealPreparationService', () => {
  let service : DealPreparationService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealPreparationService();
  })
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.remove();
    await Datastore.GenerationRequestModel.remove();
  })
  describe('POST /preparation', () => {
    it('should return error if deal size is not allowed', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '123GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DEAL_SIZE_NOT_ALLOWED
      });
    })
    it('should return error if path is not accessible', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '/probably/does/not/exist',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.PATH_NOT_ACCESSIBLE
      });
    })
    it('should create scanning request', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const found = await Datastore.ScanningRequestModel.findOne({name: 'name'});
      expect(found).not.toBeNull();
      expect(found).toEqual(jasmine.objectContaining({
        name: 'name',
        path: '.',
        minSize: 18897856102,
        maxSize: 32641751449,
      }));
      expect(found?.id).toBeDefined();
    })
  })
})
