import Datastore from '../../src/common/Datastore';
import Utils from '../Utils';
import {sleep} from '../../src/common/Util';
import DealReplicationService from "../../src/replication/DealReplicationService";
import supertest from "supertest";
import ErrorCode, {ErrorMessage} from "../../src/replication/model/ErrorCode";

xdescribe('DealReplicationService', () => {
  let service: DealReplicationService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealReplicationService();
  });
  beforeEach(async () => {
    await Datastore.HealthCheckModel.deleteMany();
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
    await Datastore.ReplicationRequestModel.deleteMany();
    await Datastore.DealStateModel.deleteMany();
    await Datastore.DealTrackingStateModel.deleteMany();
  });
  describe('POST /replication', () => {
    it('should return error if the dataset is not found', async () => {
      const response = await supertest(service['app'])
        .post('/replication')
        .send({
          datasetId: 'non-existent-dataset'
        });
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return error if no generation requests are found', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'dataset-1',
      })
      const response = await supertest(service['app'])
        .post('/replication')
        .send({
          datasetId: scanning.id
        });
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.GENERATION_NOT_FOUND,
        message: ErrorMessage[ErrorCode.GENERATION_NOT_FOUND],
      });
    });
    it('should create the replication request and deal tracking state', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'dataset-1',
      })
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        status: 'completed',
      })
      const response = await supertest(service['app'])
        .post('/replication')
        .send({
          datasetId: scanning.id,
          client: 'client'
        });
      expect(response.status).toEqual(200);
      const replicationId = response.body.id;
      const replication = await Datastore.ReplicationRequestModel.findById(replicationId);
      expect(replication).toEqual(jasmine.objectContaining({
        client: 'client',
        status: 'active'
      }));
      const trackingState = await Datastore.DealTrackingStateModel.findOne({
        stateKey: 'client'
      });
      expect(trackingState).toEqual(jasmine.objectContaining({
        stateKey: 'client',
        stateType: 'client',
        stateValue: 'track'
      }));
    })
  })
  describe('POST /replication/:id', () => {
    it('should return error if the replication request is not found', async () => {
      const response = await supertest(service['app']).post('/replication/123');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.REPLICATION_NOT_FOUND,
        message: ErrorMessage[ErrorCode.REPLICATION_NOT_FOUND],
      })
    });
    it('should return error if trying to change a replication request without cronschedule', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        status: 'active',
      });
      const response = await supertest(service['app']).post(`/replication/${request.id}`).send(
        {
        cronSchedule: '0 0 0 * * *',
        cronMaxDeals: 10,
        cronMaxPendingDeals: 10,
      });
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.NOT_CRON_JOB,
        message: ErrorMessage[ErrorCode.NOT_CRON_JOB],
      });
    })
    it('should be able to pause the replication request', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        status: 'active',
      });
      const response = await supertest(service['app']).post(`/replication/${request.id}`).send(
        {
        status: 'paused',
      });
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        status: 'paused',
      }));
      const updatedRequest = await Datastore.ReplicationRequestModel.findById(request.id);
      expect(updatedRequest?.status).toEqual('paused');
    })
    it('should be able to change the cronSchedule of the request', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        status: 'active',
        cronSchedule: '0 0 0 * * *',
        cronMaxDeals: 10,
        cronMaxPendingDeals: 10,
      });
      const response = await supertest(service['app']).post(`/replication/${request.id}`).send(
        {
        cronSchedule: '20 0 0 * * *',
        cronMaxDeals: 20,
        cronMaxPendingDeals: 20,
      });
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        cronSchedule: '20 0 0 * * *',
        cronMaxDeals: 20,
        cronMaxPendingDeals: 20,
      }));
      const updatedRequest = await Datastore.ReplicationRequestModel.findById(request.id);
      expect(updatedRequest?.cronSchedule).toEqual('20 0 0 * * *');
      expect(updatedRequest?.cronMaxDeals).toEqual(20);
      expect(updatedRequest?.cronMaxPendingDeals).toEqual(20);
    })
  });
  describe('GET /replications', () => {
    it('should return list of replication requests', async () => {
      const request = {
        datasetId: 'datasetId',
        maxReplicas: 1,
        storageProviders: 'storageProviders',
        client: 'client',
        urlPrefix: 'urlPrefix',
        maxPrice: 1,
        maxNumberOfDeals: 1,
        isVerified: true,
        startDelay: 1,
        duration: 1,
        isOffline: true,
        status: 'active',
        cronSchedule: 'cronSchedule',
        cronMaxDeals: 1,
        cronMaxPendingDeals: 1,
        fileListPath: 'fileListPath',
        notes: 'notes',
        errorMessage: 'errorMessage',
      };
      await Datastore.ReplicationRequestModel.create(request);
      const response = await supertest(service['app']).get('/replications');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual([jasmine.objectContaining({
        datasetId: 'datasetId',
        replica: 1,
        storageProviders: 'storageProviders',
        client: 'client',
        maxNumberOfDeals: 1,
        status: 'active',
        errorMessage: 'errorMessage',
      })]);
    })
  })
  describe('GET /replication/:id', () => {
    it('should return error if not found', async () => {
      const response = await supertest(service['app']).get('/replication/123');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.REPLICATION_NOT_FOUND,
        message: ErrorMessage[ErrorCode.REPLICATION_NOT_FOUND]
      });
    })
    it('should return aggregated stats for a replication request', async () => {
      const countMap: any = {
        'proposed': 1,
        'published': 2,
        'active': 3,
        'proposal_expired': 4,
        'expired': 5,
        'slashed': 6,
        'error': 7
      }
      const request = await Datastore.ReplicationRequestModel.create({
        datasetId: '123',
        maxReplicas: 10,
        storageProviders: 'f01234,f01235',
        client: 'f01236',
        urlPrefix: 'http://localhost:3000',
        maxPrice: 100,
        maxNumberOfDeals: 10,
        isVerfied: false,
        startDelay: 0,
        duration: 100,
        isOffline: false,
        status: 'active',
        cronSchedule: 'cron_schedule',
        cronMaxDeals: 100,
        cronMaxPendingDeals: 100,
        fileListPath: 'file_list_path',
        notes: 'notes'
      });
      for (const state of Object.keys(countMap)) {
        for (let i = 0; i < countMap[state]; i++) {
          await Datastore.DealStateModel.create({
            replicationRequestId: request.id,
            state
          })
        }
      }
      const response = await supertest(service['app']).get(`/replication/${request.id}`);
      expect(response.status).toEqual(200);
      expect(response.body).toEqual(jasmine.objectContaining({
        datasetId: '123',
        replica: 10,
        storageProviders: 'f01234,f01235',
        client: 'f01236',
        urlPrefix: 'http://localhost:3000',
        maxPrice: 100,
        maxNumberOfDeals: 10,
        isVerfied: 'false',
        startDelay: 0,
        duration: 100,
        isOffline: 'false',
        status: 'active',
        cronSchedule: 'cron_schedule',
        cronMaxDeals: 100,
        cronMaxPendingDeals: 100,
        fileListPath: 'file_list_path',
        notes: 'notes',
        dealsProposed: 1,
        dealsPublished: 2,
        dealsActive: 3,
        dealsProposalExpired: 4,
        dealsExpired: 5,
        dealsSlashed: 6,
        dealsError: 7,
        dealsTotal: 28,
      }));
    })
  })
  describe('cleanupHealthCheck', () => {
    it('should clean up replicationrequest table', async () => {
      await Datastore.ReplicationRequestModel.create({
        workerId: 'a',
        name: 'a',
      })
      await Datastore.ReplicationRequestModel.create({
        workerId: 'b',
        name: 'b',
      })
      await Datastore.HealthCheckModel.create({
        workerId: 'a'
      })
      await service['cleanupHealthCheck']();
      expect(await Datastore.ReplicationRequestModel.findOne({workerId: 'a'})).not.toBeNull();
      expect(await Datastore.ReplicationRequestModel.findOne({workerId: 'b'})).toBeNull();
    })
    it('should be called every interval time', async () => {
      const spy = spyOn<any>(service, 'cleanupHealthCheck');
      let abort = false;
      service['startCleanupHealthCheck'](() => Promise.resolve(abort));
      await sleep(7000);
      expect(spy).toHaveBeenCalledTimes(2);
      abort = true;
      await sleep(5000);
      expect(spy).toHaveBeenCalledTimes(2);
    })
  })
  it('should not throw when start', () => {
    new DealReplicationService().start();
  })
});
