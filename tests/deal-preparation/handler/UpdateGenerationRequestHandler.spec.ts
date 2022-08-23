import supertest from 'supertest';
import ErrorCode, { ErrorMessage } from '../../../src/deal-preparation/model/ErrorCode';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';

describe('UpdateGenerationRequestHandler', () => {
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

  describe('POST /generation/:dataset', () => {
    it('should return error if dataset is not found', async () => {
        const response = await (supertest(service['app']))
            .post(`/generation/${fakeId}`);
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
            error: ErrorCode.DATASET_NOT_FOUND,
            message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
        });
    })
    it('should change tmpdir and outdir of all generation requests of a dataset', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
        status: 'active',
      });
      const r1 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'active',
        tmpDir: 'tmpdir1',
        outDir: 'outdir1',
      });
      const r2 = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'completed'
      });
      let response = await supertest(service['app'])
        .post('/generation/' + scanning.id)
        .send({ tmpDir: 'tmpdir2', outDir: 'outdir2' }).set('Accept', 'application/json');
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        tmpDir: 'tmpdir2',
        outDir: 'outdir2',
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        tmpDir: 'tmpdir2',
        outDir: 'outdir2',
      }));
      expect(response.body).toEqual({
        success: true
      });
      response = await supertest(service['app'])
        .post('/generation/' + scanning.id)
        .send({ tmpDir: null, outDir: undefined }).set('Accept', 'application/json');
      expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
        tmpDir: null,
        outDir: 'outdir2',
      }));
      expect(await Datastore.GenerationRequestModel.findById(r2.id)).toEqual(jasmine.objectContaining({
        tmpDir: null,
        outDir: 'outdir2',
      }));
    })
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
        success: true
      });
    });

    describe('POST /generation/:dataset/:id', () => {
      it('should change tmpdir and outdir of a specified generation request', async () => {
        const scanning = await Datastore.ScanningRequestModel.create({
          name: 'name',
          status: 'active',
        });
        const r1 = await Datastore.GenerationRequestModel.create({
          datasetId: scanning.id,
          status: 'active',
          tmpDir: 'tmpdir1',
          outDir: 'outdir1',
        });
        let response = await supertest(service['app'])
          .post(`/generation/${scanning.id}/${r1.id}`)
          .send({ tmpDir: 'tmpdir2', outDir: 'outdir2' }).set('Accept', 'application/json');
        expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
          tmpDir: 'tmpdir2',
          outDir: 'outdir2',
        }));
        expect(response.body).toEqual({
          success: true
        });
        response = await supertest(service['app'])
          .post(`/generation/${scanning.id}/${r1.id}`)
          .send({ tmpDir: null, outDir: undefined }).set('Accept', 'application/json');
        expect(await Datastore.GenerationRequestModel.findById(r1.id)).toEqual(jasmine.objectContaining({
          tmpDir: null,
          outDir: 'outdir2',
        }));
      })
      it('should return 400 if generation does not exist', async () => {
        const scanning = await Datastore.ScanningRequestModel.create({
          name: 'name',
          status: 'active'
        });
        const response = await supertest(service['app'])
          .post(`/generation/${scanning.id}/${fakeId}`)
          .send({ action: 'resume' }).set('Accept', 'application/json');
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
          error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
          message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND]
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
          error: ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND,
          message: ErrorMessage[ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND]
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
          success: true
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
        success: true
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
  });
})
