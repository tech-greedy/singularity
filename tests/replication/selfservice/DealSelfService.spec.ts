import DealSelfService from "../../../src/replication/selfservice/DealSelfService"
import Utils from '../../Utils';
import Datastore from '../../../src/common/Datastore';
import supertest from 'supertest';
import ErrorCode, { ErrorMessage } from '../../../src/replication/selfservice/model/ErrorCode';

describe('DealSelfService', () => {
  let service: DealSelfService
  const defaultPolicy = {
    client: 'client',
    provider: 'provider',
    dataset: 'name',
    minStartDays: 10,
    maxStartDays: 10,
    verified: true,
    price: 0,
    minDurationDays: 180,
    maxDurationDays: 520,
  }
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealSelfService();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.DealStateModel.deleteMany();
    await Datastore.DealSelfServicePolicyModel.deleteMany();
  });

  it('should not throw when start', () => {
    new DealSelfService().start();
  })

  describe('GET /propose', () => {
    it('should return error if provider is empty', async () => {
      const response = await supertest(service['app'])
        .get('/propose').query({
          dataset: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_PROVIDER,
        message: ErrorMessage[ErrorCode.INVALID_PROVIDER],
      });
    })
    it('should return error if dataset is empty', async () => {
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_DATASET,
        message: ErrorMessage[ErrorCode.INVALID_DATASET],
      });
    })
    it('should return error if dataset cannot be found', async () => {
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.DATASET_NOT_FOUND,
        message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
      });
    })
    it('should return error if there is no matching policy', async () => {
      await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create({provider: 'provider', datasetName: 'name2'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', datasetName: 'name', client: 'client1'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', datasetName: 'name', client: 'client', minStartDays: 5, maxStartDays: 5});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', datasetName: 'name', client: 'client', minStartDays: 0, maxStartDays: 20, minDurationDays: 300, maxDurationDays: 300});
      const response = await supertest(service['app'])
        .get('/propose').query({
            provider: 'provider',
            dataset: 'name',
            client: 'client',
            startDays: 10,
          durationDays: 200,
        }).set('Accept', 'application/json');
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
            error: ErrorCode.NO_MATCHING_POLICY,
            message: ErrorMessage[ErrorCode.NO_MATCHING_POLICY],
        });
    })
    it('should return error if pieceCid has already been proposed', async () => {
      await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', client: 'client',
          minStartDays: 0, maxStartDays: 20, minDurationDays: 200, maxDurationDays: 500,
        verified: true, price: 0, datasetName: 'name'});
      await Datastore.DealStateModel.create({provider: 'provider', pieceCid: 'pieceCid', state:'active'})
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
          pieceCid: 'pieceCid'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.ALREADY_PROPOSED,
        message: ErrorMessage[ErrorCode.ALREADY_PROPOSED],
      });
    })
    it('should return error if pieceCid is not found', async () => {
      await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', client: 'client',
          minStartDays: 0, maxStartDays: 20, minDurationDays: 200, maxDurationDays: 500,
          verified: true, price: 0, datasetName: 'name'});
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
          pieceCid: 'pieceCid'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.PIECE_NOT_FOUND,
        message: ErrorMessage[ErrorCode.PIECE_NOT_FOUND],
      });
    })
    it('should propose deal if pieceCid is provided', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', client: 'client',
          minStartDays: 0, maxStartDays: 20, minDurationDays: 200, maxDurationDays: 500,
          verified: true, price: 0, datasetName: 'name'});
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        pieceCid: 'pieceCid',
        pieceSize: 100,
        dataCid: 'dataCid',
        carSize: 100,
      })
      const dealSpy = spyOn<any>(service, 'proposeDeal').and.resolveTo({dealCid: 'dealCid', state: 'proposed', errorMsg: ''});
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
          pieceCid: 'pieceCid'
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual({
        proposalId: 'dealCid',
        status: 'proposed',
        errorMessage: '',
        pieceCid: 'pieceCid',
        pieceSize: 100,
        dataCid: 'dataCid',
        carSize: 100,
        client: 'client',
        provider: 'provider'});
      expect(dealSpy).toHaveBeenCalledWith(scanning.id, 'client', 'provider', jasmine.objectContaining({
        dataCid: 'dataCid',
        carSize: 100,
        pieceCid: 'pieceCid',
        pieceSize: 100,
      }), 20, 200, true, 0);
    })
    it('should return error if pieceCid is not specified and there is no piece to propose', async () => {
      await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', client: 'client',
          minStartDays: 0, maxStartDays: 20, minDurationDays: 200, maxDurationDays: 500,
          verified: true, price: 0, datasetName: 'name'});
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.NO_PIECE_TO_PROPOSE,
        message: ErrorMessage[ErrorCode.NO_PIECE_TO_PROPOSE],
      });
    })
    it('should propose deal if pieceCid is not provided', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({name: 'name'});
      await Datastore.DealSelfServicePolicyModel.create(
        {provider: 'provider', client: 'client',
          minStartDays: 0, maxStartDays: 20, minDurationDays: 200, maxDurationDays: 500,
          verified: true, price: 0, datasetName: 'name'});
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        pieceCid: 'pieceCid',
        pieceSize: 100,
        dataCid: 'dataCid',
        carSize: 100,
      })
      const dealSpy = spyOn<any>(service, 'proposeDeal').and.resolveTo({dealCid: 'dealCid', state: 'proposed', errorMsg: ''});
      const response = await supertest(service['app'])
        .get('/propose').query({
          provider: 'provider',
          dataset: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      expect(response.body).toEqual({
        proposalId: 'dealCid',
        status: 'proposed',
        pieceCid: 'pieceCid',
        pieceSize: 100,
        dataCid: 'dataCid',
        carSize: 100,
        client: 'client',
        provider: 'provider',
        errorMessage: ''
      });
      expect(dealSpy).toHaveBeenCalledWith(scanning.id, 'client', 'provider', jasmine.objectContaining({
        dataCid: 'dataCid',
        carSize: 100,
        pieceCid: 'pieceCid',
        pieceSize: 100,
      }), 20, 200, true, 0);
    })
  })

  describe('GET /pieceCids', () => {
    it('should return error if provider is empty', async () => {
      const response = await supertest(service['app'])
        .get('/pieceCids').query({
          dataset: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_PROVIDER,
        message: ErrorMessage[ErrorCode.INVALID_PROVIDER],
      });
    })
    it('should return error if dataset is empty', async () => {
      const response = await supertest(service['app'])
        .get('/pieceCids').query({
          provider: 'name',
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_DATASET,
        message: ErrorMessage[ErrorCode.INVALID_DATASET],
      });
    })
    it('should return pieceCids', async () => {
      await Datastore.ScanningRequestModel.create({name: 'name'});
      spyOn<any>(service, 'getPieceCidsToPropose').and.resolveTo([{
        pieceCid: 'pieceCid',
        dataCid: 'dataCid',
        pieceSize: 1,
        carSize: 1,
      }])
        const response = await supertest(service['app'])
            .get('/pieceCids').query({
                provider: 'provider',
                dataset: 'name',
            }).set('Accept', 'application/json');
        expect(response.status).toEqual(200);
        expect(response.body).toEqual(['pieceCid']);
    })
  })

  describe('getPieceCidsToPropose', () => {
    it ('should return cids to propose', async ()=> {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'name',
      });
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: scanning.name,
        pieceCid: 'pieceCid1',
        pieceSize: 1,
        carSize: 1,
        dataCid: 'dataCid1',
      })
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: scanning.name,
        pieceCid: 'pieceCid2',
        pieceSize: 1,
        carSize: 1,
        dataCid: 'dataCid2',
      })
      await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        datasetName: scanning.name,
        pieceCid: 'pieceCid3',
        pieceSize: 1,
        carSize: 1,
        dataCid: 'dataCid3',
      })
      await Datastore.GenerationRequestModel.create({
        datasetId: 'otherId',
        datasetName: 'otherName',
        pieceCid: 'pieceCid0',
        pieceSize: 1,
        carSize: 1,
        dataCid: 'dataCid0',
      })
      await Datastore.DealStateModel.create({
        datasetId: scanning.id,
        client: 'client',
        provider: 'provider',
        pieceCid: 'pieceCid1',
        state: 'proposed'
      })
      await Datastore.DealStateModel.create({
        datasetId: scanning.id,
        client: 'client',
        provider: 'provider',
        pieceCid: 'pieceCid2',
        state: 'proposed_expired'
      })
      let result = await service['getPieceCidsToPropose']('provider', scanning.id, undefined);
      expect(result).toEqual([jasmine.objectContaining({
        dataCid: 'dataCid2', pieceCid: 'pieceCid2', pieceSize: 1, carSize: 1
      }),jasmine.objectContaining({
        dataCid: 'dataCid3', pieceCid: 'pieceCid3', pieceSize: 1, carSize: 1
      })]);
      result = await service['getPieceCidsToPropose']('provider', scanning.id, 1);
      expect(result).toEqual([jasmine.objectContaining({
        dataCid: 'dataCid2', pieceCid: 'pieceCid2', pieceSize: 1, carSize: 1
      })]);
    })
  })

  describe('POST /policy', () => {
    beforeEach(async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'name',
      });
    })
    it('should return error if the dataset cannot be found', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
            dataset: 'dataset'
            }).set('Accept', 'application/json');
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
            error: ErrorCode.DATASET_NOT_FOUND,
            message: ErrorMessage[ErrorCode.DATASET_NOT_FOUND],
        });
    })

    it('should return error if the minStartDays is invalid', async () => {
      const response = await supertest(service['app'])
        .post('/policy').send({
          ...defaultPolicy,
            minStartDays: 40,
        }).set('Accept', 'application/json');
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
            error: ErrorCode.INVALID_MIN_START_DAYS,
            message: ErrorMessage[ErrorCode.INVALID_MIN_START_DAYS],
        });
    })

    it ('should return error if the maxStartDays is invalid', async () => {
      const response = await supertest(service['app'])
        .post('/policy').send({
          ...defaultPolicy,
            maxStartDays: 40,
        }).set('Accept', 'application/json');
        expect(response.status).toEqual(400);
        expect(response.body).toEqual({
            error: ErrorCode.INVALID_MAX_START_DAYS,
            message: ErrorMessage[ErrorCode.INVALID_MAX_START_DAYS],
        });
    })

    it ('should return error if the minStartDays is greater than maxStartDays', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
                minStartDays: 25,
                maxStartDays: 20,
            }).set('Accept', 'application/json');
            expect(response.status).toEqual(400);
            expect(response.body).toEqual({
                error: ErrorCode.INVALID_MIN_MAX_START_DAYS,
                message: ErrorMessage[ErrorCode.INVALID_MIN_MAX_START_DAYS],
            });
    })

    it ('should return error if the minDurationDays is invalid', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
                minDurationDays: 100,
            }).set('Accept', 'application/json');
            expect(response.status).toEqual(400);
            expect(response.body).toEqual({
                error: ErrorCode.INVALID_MIN_DURATION_DAYS,
                message: ErrorMessage[ErrorCode.INVALID_MIN_DURATION_DAYS],
            });
    });

    it ('should return error if the maxDurationDays is invalid', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
                maxDurationDays: 100,
            }).set('Accept', 'application/json');
            expect(response.status).toEqual(400);
            expect(response.body).toEqual({
                error: ErrorCode.INVALID_MAX_DURATION_DAYS,
                message: ErrorMessage[ErrorCode.INVALID_MAX_DURATION_DAYS],
            });
    })

    it ('should return error if the minDurationDays is greater than maxDurationDays', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
                minDurationDays: 250,
                maxDurationDays: 200,
            }).set('Accept', 'application/json');
            expect(response.status).toEqual(400);
            expect(response.body).toEqual({
                error: ErrorCode.INVALID_MIN_MAX_DURATION_DAYS,
                message: ErrorMessage[ErrorCode.INVALID_MIN_MAX_DURATION_DAYS],
            });
    })

    it('should return error if the price is invalid', async () => {
        const response = await supertest(service['app'])
            .post('/policy').send({
            ...defaultPolicy,
                price: -1,
            }).set('Accept', 'application/json');
            expect(response.status).toEqual(400);
            expect(response.body).toEqual({
                error: ErrorCode.INVALID_PRICE,
                message: ErrorMessage[ErrorCode.INVALID_PRICE],
            });
    })

    it('should return error if the total duration >= 540', async () => {
      const response = await supertest(service['app'])
        .post('/policy').send({
          ...defaultPolicy,
          maxDurationDays: 530,
          maxStartDays: 10,
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(400);
      expect(response.body).toEqual({
        error: ErrorCode.INVALID_MAX_DAYS,
        message: ErrorMessage[ErrorCode.INVALID_MAX_DAYS],
      });
    })

    it('should create the policy model if the request is valid, then list and delete', async () => {
      await Datastore.DealSelfServicePolicyModel.deleteMany();
      const response = await supertest(service['app'])
        .post('/policy').send({
          ...defaultPolicy
        }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      const listResponse = await supertest(service['app'])
        .get('/policy').set('Accept', 'application/json');
      expect(listResponse.status).toEqual(200);
      expect(listResponse.body).toEqual([jasmine.objectContaining({
        client: defaultPolicy.client,
        datasetName: defaultPolicy.dataset,
        minStartDays: defaultPolicy.minStartDays,
        maxStartDays: defaultPolicy.maxStartDays,
        minDurationDays: defaultPolicy.minDurationDays,
        maxDurationDays: defaultPolicy.maxDurationDays,
        price: defaultPolicy.price,
        verified: defaultPolicy.verified,
        provider: defaultPolicy.provider,
      })]);
      const id = listResponse.body[0].id;
      const deleteResponse = await supertest(service['app'])
        .delete(`/policy/${id}`).set('Accept', 'application/json');
      expect(deleteResponse.status).toEqual(200);
      expect((await Datastore.DealSelfServicePolicyModel.find()).length).toEqual(0);
    })
  })
})
