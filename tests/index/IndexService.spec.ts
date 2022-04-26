import IndexService from '../../src/index/IndexService';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import supertest from 'supertest';
import ErrorCode from '../../src/index/ErrorCode';
import { CID } from 'ipfs-core';
import * as IPFS from 'ipfs-core'

describe('IndexService', () => {
  let service: IndexService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new IndexService();
    const ipfs = await IPFS.create();
    service['ipfsClient'] = ipfs;
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.remove();
    await Datastore.GenerationRequestModel.remove();
  });
  describe('start', () => {
    it('should start the service', () => {
      const listen = spyOn(service['app'], 'listen');
      service.start();
      expect<any>(listen).toHaveBeenCalledWith(7003, '0.0.0.0', jasmine.anything());
    })
  })
  describe('POST /create/:id', () => {
    it('should throw error if no dataset is provided', async () => {
      const response = await (supertest(service['app']))
        .post(`/create`);
      expect(response.status).toEqual(404);
    })
    it('should throw error if dataset id is invalid', async () => {
      const response = await (supertest(service['app']))
        .post(`/create/invalid_id`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_OBJECT_ID
      });
    })
    it('should throw error if dataset id does not exist', async () => {
      const response = await (supertest(service['app']))
        .post(`/create/507f1f77bcf86cd799439011`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    })
    it('should return rootCid', async () => {
      const scanningRequest = await Datastore.ScanningRequestModel.create({
        status: 'completed',
        path: 'base/path'
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanningRequest.id,
        status: 'completed',
        pieceCid: 'piece1',
        dataCid: 'data1',
        fileList: [
          {
            path: 'base/path/a/b.mp4',
            selector: [0, 0],
            size: 100,
            start: 0,
            end: 100
          },
          {
            path: 'base/path/a/c.mp4',
            selector: [0, 1],
            size: 100,
            start: 0,
            end: 50
          },
        ]
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanningRequest.id,
        status: 'completed',
        pieceCid: 'piece2',
        dataCid: 'data2',
        fileList: [
          {
            path: 'base/path/a/c.mp4',
            selector: [0, 0],
            size: 100,
            start: 50,
            end: 100
          },
          {
            path: 'base/path/d/e.mp4',
            selector: [1, 0],
            size: 100,
            start: 0,
            end: 100
          },
        ]
      });
      const response = await (supertest(service['app']))
        .post(`/create/${scanningRequest.id}`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const expectCid = 'bafyreihjxxy4ujk7xqa5ict5abni2djdxt4mfmfnzs63dizzcydzwfokvu';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
      const result = await service['ipfsClient'].dag.get(CID.parse(expectCid));
      expect(result.value).toEqual({"entries":{"a":{"entries":{"b.mp4":{"name":"b.mp4","size":100,"sources":{"data1":{"dataCid":"data1","from":0,"pieceCid":"piece1","selector":[0,0],"to":100}},"type":"file"},"c.mp4":{"name":"c.mp4","size":100,"sources":{"data1":{"dataCid":"data1","from":0,"pieceCid":"piece1","selector":[0,1],"to":50},"data2":{"dataCid":"data2","from":50,"pieceCid":"piece2","selector":[0,0],"to":100}},"type":"file"}},"name":"a","sources":{"data1":{"dataCid":"data1","pieceCid":"piece1","selector":[0]},"data2":{"dataCid":"data2","pieceCid":"piece2","selector":[0]}},"type":"dir"},"d":{"entries":{"e.mp4":{"name":"e.mp4","size":100,"sources":{"data2":{"dataCid":"data2","from":0,"pieceCid":"piece2","selector":[1,0],"to":100}},"type":"file"}},"name":"d","sources":{"data2":{"dataCid":"data2","pieceCid":"piece2","selector":[1]}},"type":"dir"}},"name":"","sources":{"data1":{"dataCid":"data1","pieceCid":"piece1","selector":[]},"data2":{"dataCid":"data2","pieceCid":"piece2","selector":[]}},"type":"dir"});
    })
  })
})
