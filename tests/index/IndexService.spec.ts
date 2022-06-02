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
    it('should return rootCid', async () => {
      const scanningRequest = await Datastore.ScanningRequestModel.create({
        status: 'completed',
        path: path.join('base', 'path')
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanningRequest.id,
        status: 'completed',
        pieceCid: 'piece1',
        dataCid: 'data1',
        generatedFileList: [
          {
            path: path.join('a', 'b.mp4'),
            selector: [0, 0],
            size: 100,
            start: 0,
            end: 100
          },
          {
            path: path.join('a', 'c.mp4'),
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
        generatedFileList: [
          {
            path: path.join('a', 'c.mp4'),
            selector: [0, 0],
            size: 100,
            start: 50,
            end: 100
          },
          {
            path: path.join('d', 'e.mp4'),
            selector: [1, 0],
            size: 100,
            start: 0,
            end: 100
          },
        ]
      });
      const response = await (supertest(service['app']))
        .get(`/create/${scanningRequest.id}`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const expectCid = 'bafyreigk3f2znurdgrmtjxemcegd5m7662aymxtzpkx7msobsb75f6xkya';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
      const result = await service['ipfsClient'].dag.get(CID.parse(expectCid));
      const expectedCidA = CID.parse('bafyreidn23unhic76og5u3khw76i4fz7hhn5hdtvb7tmhzy3zqxkluw7tm');
      const expectedCidD = CID.parse('bafyreigs4ma6yormosommvf44yrv2cniqtzaskln3zejyy5kvd4sexdgom');
      const expectedResult = {
        name: '',
        type: 'dir',
        entries: {
          a: expectedCidA,
          d: expectedCidD
        },
        sourcesMap: null,
        sources: [{
            dataCid: 'data1',
            pieceCid: 'piece1',
            selector: []
          },{
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: []
          }]
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
            sourcesMap: null,
            sources: [
              {
                to: 100,
                from: 0,
                dataCid: 'data1',
                pieceCid: 'piece1',
                selector: [0, 0]
              }]
          },
          'c.mp4': {
            name: 'c.mp4',
            size: 100,
            type: 'file',
            sourcesMap: null,
            sources: [{
                to: 50,
                from: 0,
                dataCid: 'data1',
                pieceCid: 'piece1',
                selector: [0, 1]
              },
              {
                to: 100,
                from: 50,
                dataCid: 'data2',
                pieceCid: 'piece2',
                selector: [0, 0]
              }]
          }
        },
        sourcesMap: null,
        sources: [{
            dataCid: 'data1',
            pieceCid: 'piece1',
            selector: [0]
          },{
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: [0]
          }]
      };
      const expectedResultD = {
        name: 'd',
        type: 'dir',
        entries: {
          'e.mp4': {
            name: 'e.mp4',
            size: 100,
            type: 'file',
            sourcesMap: null,
            sources: [{
                to: 100,
                from: 0,
                dataCid: 'data2',
                pieceCid: 'piece2',
                selector: [1, 0]
              }]
          }
        },
        sourcesMap: null,
        sources: [{
            dataCid: 'data2',
            pieceCid: 'piece2',
            selector: [1]
          }]
      }
      expect(resultA.value).toEqual(expectedResultA);
      expect(resultD.value).toEqual(expectedResultD);
    })
  })
})
