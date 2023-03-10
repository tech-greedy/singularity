import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';
import fs from 'fs/promises';

describe('PostGenerateDagRequestHandler', () => {
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

  afterEach(async () => {
    await fs.rm('./test-ipld', { recursive: true, force: true });
  })

  describe('POST /preparation/:id/generate-dag', () => {
    it('should return 500 if the underlying binary failed with whatever reason', async() => {
      await fs.mkdir('./test-ipld', { recursive: true });
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/tmp/path',
        outDir: './test-ipld',
        status: 'completed',
        maxSize: 1048576,
      })
      const generation = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: scanning.name,
        path: scanning.path,
        outDir: scanning.outDir,
        index: 0,
        status: 'completed',
      })
      await Datastore.OutputFileListModel.create({
        generationId: generation.id,
        index: 0,
        generatedFileList: [{
          path: 'file1.mp4',
          size: 100,
          dir: false,
          cid: 'invalid'
        }]
      })
      const response = await (supertest(service['app']))
        .post(`/preparation/${scanning.id}/generate-dag`);
      expect(response.status).toEqual(500);
      console.log(response.body);
      expect(response.body).toEqual({error: jasmine.stringContaining('failed to parse cid')});
    })

    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .post(`/preparation/${fakeId}/generate-dag`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return success for a valid request for local directory', async () => {
      await fs.mkdir('./test-ipld', { recursive: true });
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/tmp/path',
        outDir: './test-ipld',
        status: 'completed',
        maxSize: 1048576,
      })
      const generation = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: scanning.name,
        path: scanning.path,
        outDir: scanning.outDir,
        index: 0,
        status: 'completed',
      })
      await Datastore.OutputFileListModel.create({
        generationId: generation.id,
        index: 0,
        generatedFileList: [{
          path: '',
          dir: true,
          cid: 'bafy'
        },{
            path: 'file1.mp4',
          size: 100,
          dir: false,
          cid: 'bafy2bzaceadigy5httv7utqjspcfcvejbhb6dir5dhmo6h4yjyc2gibisq7lm'
        }]
      })
      const response = await (supertest(service['app']))
        .post(`/preparation/${scanning.id}/generate-dag`);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        datasetName: scanning.name,
        datasetId: scanning.id,
        path: scanning.path,
        outDir: scanning.outDir,
        status: 'dag',
        dataCid: 'bafybeiduglswzploozrqikkkzsko33soyh4adngvtdp7o62bdqq653bgfa',
        carSize: 155,
        pieceCid: 'baga6ea4seaqdrsvczersp2wuakfnvvu5tlsn6bmnbutxsvmoe3hlozclaqjkeea',
        pieceSize: 1048576
      }))
    })
  });
})
