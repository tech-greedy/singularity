import supertest from 'supertest';
import Datastore from '../../src/common/Datastore';
import DealPreparationService from '../../src/deal-preparation/DealPreparationService';
import ErrorCode from '../../src/deal-preparation/ErrorCode';
import Utils from '../Utils';

describe('DealPreparationService', () => {
  let service: DealPreparationService;
  const fakeId = '62429da5002efca9dd13d380';
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealPreparationService();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.remove();
    await Datastore.GenerationRequestModel.remove();
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
        fileList: [{
          path: '/data/file1.mp4',
          start: 0,
          end: 0,
          size: 100
        }]
      });
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
  describe('POST /preparation/:id', () => {
    it('should return error if action is not valid', async () => {
      const response = await supertest(service['app'])
        .post(`/preparation/${fakeId}`)
        .send({ action: 'invalid' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.ACTION_INVALID
      });
    });
    it('should return error if database does not exist', async () => {
      const response = await supertest(service['app'])
        .post(`/preparation/${fakeId}`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND
      });
    });
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
        .post('/preparation/' + scanning.id)
        .send({ action: 'pause' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
        status: 'paused'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        status: 'paused'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        status: 'completed'
      }));
    });
    it('should return error if generation does not exist', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const response = await supertest(service['app'])
        .post(`/preparation/${scanning.id}/${fakeId}`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND
      });
    });
    it('should return error if generation is malformed', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active'
      });
      const response = await supertest(service['app'])
        .post(`/preparation/${scanning.id}/fffff`)
        .send({ action: 'resume' }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_OBJECT_ID
      });
    });
    it('should change status for specific generation request', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'completed'
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
        .post(`/preparation/${scanning.id}/${r1.id}`)
        .send({ action: 'pause' }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
        status: 'completed'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        status: 'paused'
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        status: 'active'
      }));
    });
  });
  it('should change status for specific generation request by index', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
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
      .post(`/preparation/${scanning.id}/${r2.index}`)
      .send({ action: 'pause' }).set('Accept', 'application/json');
    expect(response.status).toEqual(200);
    expect(await Datastore.ScanningRequestModel.findById(scanning.id)).toEqual(jasmine.objectContaining({
      status: 'completed'
    }));
    expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
      status: 'active'
    }));
    expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
      status: 'paused'
    }));
  });
  describe('POST /preparation', () => {
    it('should return error if deal size is not allowed', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '123GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DEAL_SIZE_NOT_ALLOWED
      });
    });
    it('should return error if path is not accessible', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '/probably/does/not/exist',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.PATH_NOT_ACCESSIBLE
      });
    });
    it('should return error if the dataset name is already taken', async () => {
      let response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NAME_CONFLICT
      });
    });
    it('should create scanning request', async () => {
      const response = await supertest(service['app'])
        .post('/preparation').send({
          name: 'name',
          path: '.',
          dealSize: '32GiB'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const found = await Datastore.ScanningRequestModel.findOne({ name: 'name' });
      expect(found).not.toBeNull();
      expect(found).toEqual(jasmine.objectContaining({
        name: 'name',
        path: '.',
        minSize: 18897856102,
        maxSize: 32641751449,
      }));
      expect(found?.id).toBeDefined();
    });
  });
});
