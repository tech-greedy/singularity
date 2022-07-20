import path from 'path';
import IndexService from '../../src/index/IndexService';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import supertest from 'supertest';
import ErrorCode from '../../src/index/ErrorCode';
import { CID } from 'ipfs-core';
import * as IPFS from 'ipfs-core';

describe('IndexService', () => {
  let service: IndexService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new IndexService();
    const ipfs = await IPFS.create();
    service['ipfsClient'] = ipfs;
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
  });
  describe('start', () => {
    it('should start the service', () => {
      const listen = spyOn(service['app'], 'listen');
      service.start();
      expect<any>(listen).toHaveBeenCalledWith(7003, '0.0.0.0', jasmine.anything());
    })
  })
  describe('POST /create/:id', () => {
    it('should throw error if no dataset has not completed scanning', async () => {
      const scanningRequest = await Datastore.ScanningRequestModel.create({
        status: 'active',
        path: 'base/path'
      });
      const response = await (supertest(service['app']))
        .get(`/create/${scanningRequest.id}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.SCANNING_INCOMPLETE
      });
    })
    it('should throw error if no dataset is provided', async () => {
      const response = await (supertest(service['app']))
        .get(`/create`);
      expect(response.status).toEqual(404);
    })
    it('should throw error if dataset id is invalid', async () => {
      const response = await (supertest(service['app']))
        .get(`/create/invalid_id`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    })
    it('should throw error if dataset id does not exist', async () => {
      const response = await (supertest(service['app']))
        .get(`/create/507f1f77bcf86cd799439011`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    })
    fit('should return rootCid', async () => {
      const scanningRequest = await Datastore.ScanningRequestModel.create({
        status: 'completed',
        path: path.join('base', 'path')
      });
      const generation1 = await Datastore.GenerationRequestModel.create({
        datasetId: scanningRequest.id,
        status: 'completed',
        pieceCid: 'piece1',
        dataCid: 'data1',
      });
      await Datastore.OutputFileListModel.create({
        generationId: generation1.id,
        index: 0,
        generatedFileList: [
          {
            path: '',
            dir: true,
            cid: 'data1'
          },
          {
            path: path.join('a'),
            dir: true,
            cid: 'cid2'
          },
          {
            path: path.join('a', 'b.mp4'),
            size: 100,
            start: 0,
            end: 100,
            dir: false,
            cid: 'cid3'
          },
          {
            path: path.join('a', 'c.mp4'),
            size: 100,
            start: 0,
            end: 50,
            dir: false,
            cid: 'cid4'
          },
        ]
      })
      const generation2 = await Datastore.GenerationRequestModel.create({
        datasetId: scanningRequest.id,
        status: 'completed',
        pieceCid: 'piece2',
        dataCid: 'data2',
      });
      await Datastore.OutputFileListModel.create({
        generationId: generation2.id,
        generatedFileList: [
          {
            path: '',
            dir: true,
            cid: 'data2'
          },
          {
            path: path.join('a'),
            dir: true,
            cid: 'cid6'
          },
          {
            path: path.join('a', 'c.mp4'),
            size: 100,
            start: 50,
            end: 100,
            dir: false,
            cid: 'cid7'
          },
          {
            path: path.join('d'),
            dir: true,
            cid: 'cid8'
          },
          {
            path: path.join('d', 'e.mp4'),
            size: 100,
            dir: false,
            cid: 'cid9'
          },
        ]
      });
      const response = await (supertest(service['app']))
        .get(`/create/${scanningRequest.id}`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const expectCid = 'bafyreierxb23v5bfuopejyuofp5tp75ywjwttflq5rvky7x2zutok7gusm';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
      const result = await service['ipfsClient'].dag.get(CID.parse(expectCid));
      const expectedCidA = CID.parse('bafyreiawgqtrxyps4w34nzpnv42ry32kaiuafqvam4e5ou55kxjf2yyn5a');
      const expectedCidD = CID.parse('bafyreidvdz5gffokecbuazmlritxmwntk3oucng4auc7oxdqdwd2pgttdu');
      const expectedResult = {
        name: '',
        type: 'dir',
        entries: {
          a: expectedCidA,
          d: expectedCidD
        },
        sources: ['data1', 'data2']
      };
      expect(result.value).toEqual(expectedResult);
      const resultA = await service['ipfsClient'].dag.get(expectedCidA);
      const resultD = await service['ipfsClient'].dag.get(expectedCidD);
      const expectedResultA = {
        name: 'a',
        type: 'dir',
        entries: {
          'b.mp4': {
            name: 'b.mp4',
            size: 100,
            type: 'file',
            sources: [
              {
                to: 100,
                from: 0,
                cid: "cid3"
              }]
          },
          'c.mp4': {
            name: 'c.mp4',
            size: 100,
            type: 'file',
            sources: [{
                to: 50,
                from: 0,
                cid: "cid4"
              },
              {
                to: 100,
                from: 50,
                cid: "cid7"
              }]
          }
        },
        sources: ['cid2', 'cid6']
      };
      const expectedResultD = {
        name: 'd',
        type: 'dir',
        entries: {
          'e.mp4': {
            name: 'e.mp4',
            size: 100,
            type: 'file',
            sources: [{
                to: 100,
                from: 0,
                cid: 'cid9'
              }]
          }
        },
        sources: ['cid8']
      }
      expect(resultA.value).toEqual(expectedResultA);
      expect(resultD.value).toEqual(expectedResultD);
    })
  })
})
