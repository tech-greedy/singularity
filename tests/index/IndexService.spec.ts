import path from 'path';
import IndexService from '../../src/index/IndexService';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import supertest from 'supertest';
import ErrorCode from '../../src/index/ErrorCode';
import { CID } from 'ipfs-core';
import * as IPFS from 'ipfs-core';
import { DirNode, DynamicArray, DynamicMap, FileNode, LayeredArray, LayeredMap } from '../../src/index/FsDag';

function replacer(_key: any, value: any) {
  if(value instanceof Map) {
    return Object.fromEntries(value.entries());
  } else {
    return value;
  }
}

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
  describe('pinEntries', () => {
    it('should handle less entries', async () => {
      const entries: DynamicMap<FileNode | DirNode | CID> = new Map();
      entries.set('k1', { type: 'file', size: 1, name: 'k1', realSources: []});
      entries.set('k2', { type: 'file', size: 1, name: 'k2', realSources: []});
      const [node, count] = await service['pinEntries'](entries, 100);
      expect(count).toEqual(21);
      expect(node).toEqual(entries);
    })
    it('should handle more entries', async () => {
      const entries: DynamicMap<FileNode | DirNode | CID> = new Map();
      entries.set('k1', { type: 'file', size: 1, name: 'k1', realSources: []});
      entries.set('k2', { type: 'file', size: 1, name: 'k2', realSources: []});
      const [node, count] = await service['pinEntries'](entries, 10);
      expect(count).toEqual(1);
      expect(<any>node).toEqual(CID.parse('bafyreic2qccr5ps4dxh3lqjpovcw23zfy26oj2lblt6sy5cpjz4sqhdhui'));
      const result = await service['ipfsClient'].dag.get(<CID>node);
      expect(result.value).toEqual({
          k1: { name: 'k1', size: 1, type: 'file', realSources: [] },
          k2: { name: 'k2', size: 1, type: 'file', realSources: [] }
        }
      )
    })
    it('should handle layered map', async () => {
      const map = new Map();
      map.set('k1', { type: 'file', size: 1, name: 'k1', realSources: []});
      map.set('k2', { type: 'file', size: 1, name: 'k2', realSources: []});
      const entries: LayeredMap<FileNode | DirNode | CID>[] = [{
        from: 'k1',
        to: 'k2',
        map: map
      }];
      const [node, count] = await service['pinEntries'](entries, 10);
      expect(count).toEqual(7);
      expect(node).toEqual([{from: 'k1', to: 'k2', map: CID.parse('bafyreic2qccr5ps4dxh3lqjpovcw23zfy26oj2lblt6sy5cpjz4sqhdhui')}])
    })
  })
  describe('pinDynamicArray', () => {
    it('should handle empty array', async() => {
      const array: DynamicArray<string> = [];
      const [node, count] = await service['pinDynamicArray'](array, 1, 10);
      expect(node).toEqual([]);
      expect(count).toEqual(1);
    })
    it('should handle short array', async() => {
      const array: DynamicArray<string> = ['a', 'b'];
      const [node, count] = await service['pinDynamicArray'](array, 1, 10);
      expect(node).toEqual(['a', 'b']);
      expect(count).toEqual(3);
    })
    it('should handle large array', async() => {
      const array: DynamicArray<string> = ['a', 'b', 'c', 'd', 'e'];
      const [node, count] = await service['pinDynamicArray'](array, 1, 4);
      expect(node).toEqual(CID.parse('bafyreihnb4ffkxpva3r5qetbr33htpk6wpspked7rqxlzfxs4zwbv5dp3e'));
      expect(count).toEqual(1);
    })
    it ('should handle layered array', async() => {
      const array: LayeredArray<string>[] = [
        {index: 0, array: ['a', 'b', 'c']},
        {index: 3, array: ['d', 'e', 'f']},
        {index: 6, array: ['g', 'h', 'i']},
      ];
      const [node, count] = await service['pinDynamicArray'](array, 1, 3);
      expect(node).toEqual(CID.parse('bafyreigeygdkvytmehe337jy7ywlthsgjxscsolior3v4vyrueksejpvu4'));
      expect(count).toEqual(1);
      const result = await service['ipfsClient'].dag.get(<CID>node);
      expect(result.value).toEqual([
        {index: 0, array: CID.parse('bafyreigaaygv3yxbsif2volst5c67cxw2glbcorkbt3ndy4emeuvys3woy')},
        {index: 3, array: CID.parse('bafyreih5j5f25oizur4v2osm2jbc77g4cercmfgs2qa4biqds5sopr3joy')},
        {index: 6, array: CID.parse('bafyreid6bfb4aos3wbpzlb7teft4ohfm5hr3popvdredahlpyabf2y2cva')},
      ])
    });
  })
  describe('dynamizeDirNode', () => {
    it('should be able to dynamize dirNode with small arrays', async () => {
      const dirNode: DirNode = {
        name: 'name',
        type: 'dir',
        sources: [
          'cid1', 'cid2'
        ],
        entries: new Map<string, FileNode | DirNode | CID>(),
      };
      dirNode.entries!.set('file1', {
        name: 'file1',
        type: 'file',
        sources: [],
        size: 0,
      });
      dirNode.entries!.set('dir1', {
        name: 'dir1',
        type: 'dir',
        sources: [],
        entries: new Map<string, FileNode | DirNode | CID>(),
      })
      service['dynamizeDirNode'](dirNode, 10);
      expect(JSON.stringify(dirNode, replacer, 2)).toEqual(`{
  "name": "name",
  "type": "dir",
  "realSources": [
    "cid1",
    "cid2"
  ],
  "realEntries": {
    "file1": {
      "name": "file1",
      "type": "file",
      "size": 0,
      "realSources": []
    },
    "dir1": {
      "name": "dir1",
      "type": "dir",
      "realSources": [],
      "realEntries": {}
    }
  }
}`);
    });
  });
  describe('dynamizeFileNode', () => {
    it('should be able to dynamize file node with small arrays', () => {
      const file: FileNode = {
        name: 'name',
        type: 'file',
        size: 100,
        sources: [{
          from: 0,
          to: 50,
          cid: 'cid1',
        },{
          from: 0,
          to: 50,
          cid: 'cid2',
        }]
      };
      service['dynamizeFileNode'](file, 100);
      expect(file).toEqual({
        name: 'name',
        type: 'file',
        size: 100,
        realSources: [{
          from: 0,
          to: 50,
          cid: 'cid1',
        },{
          from: 0,
          to: 50,
          cid: 'cid2',
        }]
      })
    })

    it('should be able to dynamize file node with large arrays', () => {
      const file: FileNode = {
        name: 'name',
        type: 'file',
        size: 100,
        sources: [{
          from: 0,
          to: 0,
          cid: 'cid1',
        },{
          from: 0,
          to: 0,
          cid: 'cid2',
        },{
          from: 0,
          to: 0,
          cid: 'cid3',
        },{
          from: 0,
          to: 0,
          cid: 'cid4',
        },{
          from: 0,
          to: 0,
          cid: 'cid5',
        }]
      };
      service['dynamizeFileNode'](file, 2);
      expect(file).toEqual({
        "name": "name",
        "type": "file",
        "size": 100,
        "realSources": [
          {
            "index": 0,
            "array": [
              {
                "index": 0,
                "array": [
                  {
                    "from": 0,
                    "to": 0,
                    "cid": "cid1"
                  },
                  {
                    "from": 0,
                    "to": 0,
                    "cid": "cid2"
                  }
                ]
              },
              {
                "index": 2,
                "array": [
                  {
                    "from": 0,
                    "to": 0,
                    "cid": "cid3"
                  },
                  {
                    "from": 0,
                    "to": 0,
                    "cid": "cid4"
                  }
                ]
              }
            ]
          },
          {
            "index": 4,
            "array": [
              {
                "from": 0,
                "to": 0,
                "cid": "cid5"
              }
            ]
          }
        ]
      })
    })

  })
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
        .post(`/create/${scanningRequest.id}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.SCANNING_INCOMPLETE
      });
    })
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
        error: ErrorCode.DATASET_NOT_FOUND
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
      let response = await (supertest(service['app']))
        .post(`/create/${scanningRequest.id}`)
        .set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      let expectCid = 'bafyreidadk6lzfbmzuz5snvnzl6zc6chk26uw6o3k5kap6kodr3vo7etee';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
      const result = await service['ipfsClient'].dag.get(CID.parse(expectCid));
      expect(result.value).toEqual({
        name: '',
        type: 'dir',
        realSources: ['data1', 'data2'],
        realEntries: {
          a: jasmine.anything(),
          d: jasmine.anything()
        }
      })


      response = await (supertest(service['app']))
        .post(`/create/${scanningRequest.id}`)
        .set('Accept', 'application/json')
        .send({
          maxNodes: 2,
          maxLinks: 2
        });
      expect(response.status).toEqual(200);
      expectCid = 'bafyreiehlptanmikfgf2kcwzomji6u7ksjuoi5xwyhdgw4c35blq6rwgeu';
      expect(response.body).toEqual({
        rootCid: expectCid
      });
    })
  })
})
