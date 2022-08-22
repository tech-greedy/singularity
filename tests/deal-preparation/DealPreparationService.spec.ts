import supertest from 'supertest';
import Datastore from '../../src/common/Datastore';
import DealPreparationService from '../../src/deal-preparation/DealPreparationService';
import ErrorCode from '../../src/deal-preparation/model/ErrorCode';
import Utils from '../Utils';
import fs from 'fs/promises';

describe('DealPreparationService', () => {
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
  describe('cleanupHealthCheck', () => {
    it('should clean up scanningrequest and generationrequest table', async () => {
      await Datastore.ScanningRequestModel.create({
        workerId: 'a',
        name: 'a',
      })
      await Datastore.ScanningRequestModel.create({
        workerId: 'b',
        name: 'b',
      })
      await Datastore.GenerationRequestModel.create({
        workerId: 'c'
      })
      await Datastore.GenerationRequestModel.create({
        workerId: 'd'
      })
      await Datastore.HealthCheckModel.create({
        workerId: 'a'
      })
      await Datastore.HealthCheckModel.create({
        workerId: 'c'
      })
      await service['cleanupHealthCheck']();
      expect(await Datastore.ScanningRequestModel.findOne({ workerId: 'a' })).not.toBeNull();
      expect(await Datastore.ScanningRequestModel.findOne({ workerId: 'b' })).toBeNull();
      expect(await Datastore.GenerationRequestModel.findOne({ workerId: 'c' })).not.toBeNull();
      expect(await Datastore.GenerationRequestModel.findOne({ workerId: 'd' })).toBeNull();
    })
  })
  describe('GET /generation/:dataset/:id', () => {
    it('should return error if the id is not integer', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/name/not_number`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_OBJECT_ID
      });
    })
    it('should return error if the id is not found', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/name/10`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND
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
  describe('GET /generation-manifest/:id', () => {
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
        error: ErrorCode.GENERATION_NOT_COMPLETED
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
  describe('GET /generation/:id', () => {
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get(`/generation/${fakeId}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND
      });
    })
    it('should return error if the id is not valid', async () => {
      const response = await (supertest(service['app']))
        .get('/generation/fakeid');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_OBJECT_ID
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
  describe('GET /preparation/:id', () => {
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get(`/preparation/${fakeId}`);
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    })
    it('should return error if the id cannot be found', async () => {
      const response = await (supertest(service['app']))
        .get('/preparation/fakeid');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    })
    it('should return all generation requests', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'path',
        status: 'completed',
        minSize: 0,
        maxSize: 10
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'active',
        index: 0,
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
      const response = await (supertest(service['app']))
        .get('/preparation/' + scanning.id);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        name: 'name',
        path: 'path',
        scanningStatus: 'completed',
        minSize: 0,
        maxSize: 10
      }));
      expect(response.body.generationRequests.length).toEqual(1);
      expect(response.body.generationRequests[0]).toEqual(jasmine.objectContaining({
        index: 0,
        status: 'active',
        dataCid: 'dataCid',
        pieceCid: 'pieceCid',
        pieceSize: 10
      }));
    });
  });
  describe('GET /preparations', () => {
    it('should return all scanning requests with aggregated stats', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'path',
        status: 'completed',
        minSize: 0,
        maxSize: 10
      });
      for (let i = 0; i <= 3; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'active'
        });
      }
      for (let i = 0; i <= 4; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'paused'
        });
      }
      for (let i = 0; i <= 5; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'completed'
        });
      }
      for (let i = 0; i <= 6; ++i) {
        await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'error'
        });
      }
      const response = await (supertest(service['app']))
        .get('/preparations');
      expect(response.status).toEqual(200);
      expect(response.body.length).toEqual(1);
      expect(response.body[0]).toEqual(jasmine.objectContaining({
        name: 'name',
        path: 'path',
        minSize: 0,
        maxSize: 10,
        scanningStatus: 'completed',
        generationTotal: 22,
        generationActive: 4,
        generationPaused: 5,
        generationCompleted: 6,
        generationError: 7
      }));
    });
  });
  describe('POST /generation/:dataset', () => {
    it('should change status for all generation requests if generation is not given', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const r1 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'active'
      });
      const r2 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'completed'
      });
      const response = await supertest(service['app'])
        .post('/generation/' + scanning.id)
        .send({ action: 'pause' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
        status: 'active'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        status: 'paused'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        status: 'completed'
      }));
      expect(response.body).toEqual({
        generationRequestsChanged: 1
      });
    });
    it('should return 0 changed if generation does not exist', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const response = await supertest(service['app'])
        .post(`/generation/${scanning.id}/${fakeId}`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual({
        generationRequestsChanged: 0
      });
    });
    it('should return error if generation is malformed', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const response = await supertest(service['app'])
        .post(`/generation/${scanning.id}/fffff`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_OBJECT_ID
      });
    });
    it('should change status for specific generation request', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const r1 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'active'
      });
      const r2 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        index: 5,
        status: 'active'
      });
      const response = await supertest(service['app'])
        .post(`/generation/${scanning.id}/${r1.id}`)
        .send({ action: 'pause' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
        status: 'active'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        status: 'paused'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        status: 'active'
      }));
      expect(response.body).toEqual({
        generationRequestsChanged: 1
      });
    });
  });
  it('should change status for specific generation request by index', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'active'
    });
    const r1 = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      status: 'active'
    });
    const r2 = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      index: 5,
      status: 'active'
    });
    const response = await supertest(service['app'])
      .post(`/generation/${scanning.id}/${r2.index}`)
      .send({ action: 'pause' }).set('Accept', 'application/json');
    expect(response.status).toEqual(200);
    expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
      status: 'paused'
    }));
    expect(response.body).toEqual({
      generationRequestsChanged: 1
    });
  });
  it('should change status for all generation requests for a scanning request', async () => {
    const scanning1 = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'active'
    });
    const scanning2 = await Datastore.ScanningRequestModel.create({
      name: 'name2',
      status: 'active'
    });
    const g1 = await Datastore.GenerationRequestModel.create({
      datasetId: scanning1.id,
      status: 'active'
    });
    const g2 = await Datastore.GenerationRequestModel.create({
      datasetId: scanning2.id,
      status: 'active'
    });
    const g3 = await Datastore.GenerationRequestModel.create({
      datasetId: scanning2.id,
      status: 'active'
    });
    const response = await supertest(service['app'])
      .post(`/generation/${scanning2.id}`)
      .send({ action: 'pause' }).set('Accept', 'application/json');
    expect(response.status).toEqual(200);
    expect(await Datastore.ScanningRequestModel.findById(scanning1.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.ScanningRequestModel.findById(scanning2.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.GenerationRequestModel.findById(g1.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.GenerationRequestModel.findById(g2.id)).toEqual(jasmine.objectContaining({
      status: 'paused'
    }));
    expect(await Datastore.GenerationRequestModel.findById(g3.id)).toEqual(jasmine.objectContaining({
      status: 'paused'
    }));
  })
  describe('cleanupIncompleteFiles', () => {
    it('should delete the incomplete files', async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'test-deletion',
        outDir: '.',
        tmpDir: './tmp'
      });
      await fs.mkdir('./tmp/d715461e-8d42-4a53-9b33-e17ed4247304', { recursive: true });
      await fs.writeFile('./d715461e-8d42-4a53-9b33-e17ed4247304.car', 'something');
      await DealPreparationService.cleanupIncompleteFiles();
      await expectAsync(fs.access('./d715461e-8d42-4a53-9b33-e17ed4247304.car')).toBeRejected();
      await expectAsync(fs.access('./d715461e-8d42-4a53-9b33-e17ed4247304')).toBeRejected();
    })
    it('should skip nonexisting folders', async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'test-deletion',
        outDir: './non-existing',
        tmpDir: './non-existing',
      });
      await expectAsync(DealPreparationService.cleanupIncompleteFiles()).toBeResolved();
    })
  })
});
