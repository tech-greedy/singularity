import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';

describe('GetGenerationManifestRequestHandler', () => {
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
  describe('GET /generation-manifest/:id', () => {
    it('should return 400 if generation request is not found', async () => {
      const response = await (supertest(service['app']))
        .get('/generation-manifest/' + fakeId);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND],
      });
    })
    it('should throw if generation is not complete', async () => {
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: 'datasetId',
        status: 'active',
        index: 0,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10,
      });
      const response = await (supertest(service['app']))
        .get('/generation-manifest/' + generationRequest.id);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.GENERATION_NOT_COMPLETED,
        message: ErrorMessage[ErrorCode.GENERATION_NOT_COMPLETED],
      });
    })
    it('should return slingshot compliant manifest', async () => {
      const generationRequest = await Datastore.GenerationRequestModel.create({
        datasetId: 'datasetId',
        status: 'completed',
        index: 0,
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10,
      });
      await Datastore.OutputFileListModel.create({
        generationId: generationRequest.id,
        index: 0,
        generatedFileList: [{
          path: '/data/file1.mp4',
          start: 50,
          end: 60,
          size: 100,
          cid: 'file_cid',
          dir: false
        }, {
          path: '/data',
          cid: 'dir_cid',
          dir: true
        },{
          path: '',
          cid: 'root_cid',
          dir: true
        }]
      })
      const response = await (supertest(service['app']))
        .get('/generation-manifest/' + generationRequest.id);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual({
          piece_cid: 'pieceCid',
          payload_cid: 'dataCid',
          contents: {
            '/data/file1.mp4': {
              CID: 'file_cid',
              filesize: 100,
              chunkoffset: 50,
              chunklength: 10
            }
          },
          groupings: { '/data': 'dir_cid' }
        }
      );
    })
  })
})
