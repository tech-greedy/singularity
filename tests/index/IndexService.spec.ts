import IndexService from '../../src/index/IndexService';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import supertest from 'supertest';
import ErrorCode from '../../src/index/ErrorCode';

describe('IndexService', () => {
  let service: IndexService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new IndexService();
  });
  beforeEach(async () => {
    await Datastore.DatasetFileMappingModel.remove();
    await Datastore.DatasetFileMappingModel.create({
      datasetId: 'id',
      datasetName: 'name',
      index: 0,
      filePath: 'path1',
      rootCid: 'cid1',
      selector: [0]
    });
    await Datastore.DatasetFileMappingModel.create({
      datasetId: 'id',
      datasetName: 'name',
      index: 1,
      filePath: 'path2',
      rootCid: 'cid2',
      selector: [1]
    });
    await Datastore.DatasetFileMappingModel.create({
      datasetId: 'id',
      datasetName: 'name',
      index: 2,
      filePath: 'path2',
      rootCid: 'cid3',
      selector: [2]
    });
  });
  describe('POST /lookup', () => {
    it('should throw error if no dataset is provided', async () => {
      const response = await (supertest(service['app']))
        .post(`/lookup`)
        .send({ path: 'path' });
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_EMPTY
      });
    })

    it('should return entries that match the dataset id', async () => {
      let response = await (supertest(service['app']))
        .post(`/lookup`)
        .send({ datasetId: 'id' });
      expect(response.status).toEqual(200);
      expect(response.text).toEqual(
        '{"datasetId":"id","datasetName":"name","path":"path1","selector":[0]}\n' +
        '{"datasetId":"id","datasetName":"name","path":"path2","selector":[1]}\n' +
        '{"datasetId":"id","datasetName":"name","path":"path2","selector":[2]}\n');
    })

    it('should return entries that match the path', async () => {
      let response = await (supertest(service['app']))
        .post(`/lookup`)
        .send({ datasetId: 'id', path: 'path2' });
      expect(response.status).toEqual(200);
      expect(response.text).toEqual(
        '{"datasetId":"id","datasetName":"name","path":"path2","selector":[1]}\n' +
        '{"datasetId":"id","datasetName":"name","path":"path2","selector":[2]}\n');
    })

    it('should return empty entries if nothing matches', async () => {
      let response = await (supertest(service['app']))
        .post(`/lookup`)
        .send({ datasetId: 'id2', path: 'path2' });
      expect(response.status).toEqual(200);
      expect(response.text).toEqual('');
    })
  })
})
