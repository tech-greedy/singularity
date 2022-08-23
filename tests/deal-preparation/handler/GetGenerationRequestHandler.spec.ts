import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';

describe('GetGenerationRequestHandler', () => {
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
  describe('GET /generation/:id', () => {
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/${fakeId}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND],
      });
    })
    it('should return error if the id is not valid', async () => {
      const response = await (supertest(service['app']))
        .get('/generation/fakeid');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND],
      });
    })
    it('should return file list of a specific generation request', async () => {
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: 'datasetId',
        status: 'active',
        index: 0,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10,
      });
      await Datastore.InputFileListModel.create({
        generationId: generationRequest.id,
        index: 0,
        fileList: [{
          path: '/data/file1.mp4',
          start: 0,
          end: 0,
          size: 100
        }]
      })
      await Datastore.OutputFileListModel.create({
        generationId: generationRequest.id,
        index: 0,
        generatedFileList: [{
          path: '/data/file1.mp4',
          start: 0,
          end: 0,
          size: 100,
          cid: 'cid',
          dir: false
        }]
      })
      const response = await (supertest(service['app']))
        .get('/generation/' + generationRequest.id);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        datasetId: 'datasetId',
        status: 'active',
        index: 0,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10
      }));
      expect(response.body.fileList.length).toEqual(1);
      expect(response.body.fileList[0]).toEqual(jasmine.objectContaining({
        path: '/data/file1.mp4',
        start: 0,
        end: 0,
        size: 100
      }));
      expect(response.body.generatedFileList.length).toEqual(1);
      expect(response.body.generatedFileList[0]).toEqual(jasmine.objectContaining({
        path: '/data/file1.mp4',
        start: 0,
        end: 0,
        size: 100,
        cid: 'cid',
        dir: false
      }));
    });
  });
  describe('GET /generation/:dataset/:id', () => {
    it('should return error if the id is not integer', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/name/not_number`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND],
      });
    })
    it('should return error if the id is not found', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/name/10`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND],
      });
    })
    it('should return found generation request by dataset name and index', async () => {
      await Datastore.GenerationRequestModel.create({
        datasetId: fakeId,
        datasetName: 'datasetName',
        status: 'active',
        index: 10,
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
      let response = await (supertest(service['app']))
        .get(`/generation/datasetName/10`);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        datasetId: fakeId,
        datasetName: 'datasetName',
        status: 'active',
        index: 10,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10
      }));
      response = await (supertest(service['app']))
        .get(`/generation/${fakeId}/10`);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        datasetId: fakeId,
        datasetName: 'datasetName',
        status: 'active',
        index: 10,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10
      }));
    })
  })
})
