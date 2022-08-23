import supertest from 'supertest';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';

describe('CreatePreparationRequestHandler', () => {
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
  describe('POST /preparation', () => {
    it('should return error if min ratio too small', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB',
          minRatio: 0.4,
          maxRatio: 0.6,
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.MIN_RATIO_INVALID,
        message: ErrorMessage[ErrorCode.MIN_RATIO_INVALID],
      });
    });
    it('should return error if max ratio too large', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB',
          minRatio: 0.6,
          maxRatio: 1.2,
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.MAX_RATIO_INVALID,
        message: ErrorMessage[ErrorCode.MAX_RATIO_INVALID],
      });
    });
    it('should return error if max ratio too small', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB',
          minRatio: 0.6,
          maxRatio: 0.55,
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.MAX_RATIO_INVALID,
        message: ErrorMessage[ErrorCode.MAX_RATIO_INVALID],
      });
    });
    it('should use min and max ratio if specified', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB',
          minRatio: 0.7,
          maxRatio: 0.8,
          outDir: '.',
          tmpDir: '.',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        minSize: 24051816858,
        maxSize:27487790694
      }));
    });
    it('should return error if deal size is not allowed', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '123GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DEAL_SIZE_NOT_ALLOWED,
        message: ErrorMessage[ErrorCode.DEAL_SIZE_NOT_ALLOWED],
      });
    });
    it('should return error if path is not accessible', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '/probably/does/not/exist',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.PATH_NOT_ACCESSIBLE,
        message: ErrorMessage[ErrorCode.PATH_NOT_ACCESSIBLE],
      });
    });
    it('should return error if tmpDir is not accessible', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          tmpDir: '/probably/does/not/exist',
          path: '.',
          outDir: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.PATH_NOT_ACCESSIBLE,
        message: ErrorMessage[ErrorCode.PATH_NOT_ACCESSIBLE],
      });
    });
    it('should return error if s3 path is used but tmpdir is not specified', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: 's3://dummy',
          outDir: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.TMPDIR_MISSING_FOR_S3,
        message: ErrorMessage[ErrorCode.TMPDIR_MISSING_FOR_S3],
      });
    });
    it('should return error if the dataset name is already taken', async () => {
      let response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name2',
          path: '.',
          outDir: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name2',
          path: '.',
          outDir: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NAME_CONFLICT,
        message: ErrorMessage[ErrorCode.DATASET_NAME_CONFLICT],
      });
    });
    it('should create scanning request', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          outDir: '.',
          tmpDir: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const found = await Datastore.ScanningRequestModel.findOne({ name: 'name' });
      expect(found).not.toBeNull();
      expect(found).toEqual(jasmine.objectContaining({
        name: 'name',
        path: '.',
        tmpDir: '.',
        minSize: 18897856102,
        maxSize: 32641751449,
      }));
      expect(found?.id).toBeDefined();
    });
  });
})
