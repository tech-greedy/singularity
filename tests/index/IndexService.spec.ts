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
      const expectCid = 'bafyreihah4flyu6ex4tsdkg3ngnz6dvnfjv6tzgex6nsxrme6uulbubwtm';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
      const result = await service['ipfsClient'].dag.get(CID.parse(expectCid));
      const expectedCidA = CID.parse('bafyreigf573nwgfleglbyb3e43agibk3hxddnzhq4l3q2jsjbn2i6drkn4');
      const expectedCidD = CID.parse('bafyreibbnaqs6y63rkmpwikrziqn5wldcxe2jx3s7u5bzg4mnjkvvymucm');
      const expectedResult = {
        name: '',
        type: 'dir',
        entries: {
          a: expectedCidA,
          d: expectedCidD
        },
        sources: {
          data1: {
            dataCid: 'data1',
            pieceCid: 'piece1',
            selector: []
          },
          data2: {
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: []
          }
        }
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
            sources: {
              data1: {
                to: 100,
                from: 0,
                dataCid: 'data1',
                pieceCid: 'piece1',
                selector: [0, 0]
              }
            }
          },
          'c.mp4': {
            name: 'c.mp4',
            size: 100,
            type: 'file',
            sources: {
              data1: {
                to: 50,
                from: 0,
                dataCid: 'data1',
                pieceCid: 'piece1',
                selector: [0, 1]
              },
              data2: {
                to: 100,
                from: 50,
                dataCid: 'data2',
                pieceCid: 'piece2',
                selector: [0, 0]
              }
            }
          }
        },
        sources: {
          data1: {
            dataCid: 'data1',
            pieceCid: 'piece1',
            selector: [0]
          },
          data2: {
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: [0]
          }
        }
      };
      const expectedResultD = {
        name: 'd',
        type: 'dir',
        entries: {
          'e.mp4': {
            name: 'e.mp4',
            size: 100,
            type: 'file',
            sources: {
              data2: {
                to: 100,
                from: 0,
                dataCid: 'data2',
                pieceCid: 'piece2',
                selector: [1, 0]
              }
            }
          }
        },
        sources: {
          data2: {
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: [1]
          }
        }
      }
      expect(resultA.value).toEqual(expectedResultA);
      expect(resultD.value).toEqual(expectedResultD);
    })
  })
})
