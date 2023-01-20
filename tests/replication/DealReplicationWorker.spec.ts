import DealReplicationWorker from '../../src/replication/DealReplicationWorker';
import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import { HeightFromCurrentTime } from '../../src/common/ChainHeight';
import { sleep } from '../../src/common/Util';
import createSpyObj = jasmine.createSpyObj;
import cron from 'node-cron';
import * as childprocess from 'promisify-child-process';
import axios from 'axios';

describe('DealReplicationWorker', () => {
  let worker: DealReplicationWorker;
  let defaultTimeout: number;

  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealReplicationWorker();
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
    await worker.initialize(() => Promise.resolve(true));
  });

  afterAll(async () => {
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  });

  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.ReplicationRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
    for (const cron of worker['cronRefArray']) {
      cron[1][1].stop();
    }
    worker['cronRefArray'].clear();
  });

  describe('startPollWork', () => {
    it('should immediately start next job if work finishes', async () => {
      const spy = spyOn(global, 'setTimeout');
      const spyWork = spyOn<any>(worker, 'pollWork').and.resolveTo(true);
      await worker['startPollWork']();
      expect(spyWork).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['ImmediatePollInterval']);
    })
    it('should poll for next job after 5s if no work found', async () => {
      const spy = spyOn(global, 'setTimeout');
      const spyWork = spyOn<any>(worker, 'pollWork').and.resolveTo(false);
      await worker['startPollWork']();
      expect(spyWork).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['PollInterval']);
    })
  });

  describe('healthCheck', () => {
    it('should create an entry in HealthCheck table', async () => {
      await worker['healthCheck']();
      const found = await Datastore.HealthCheckModel.findOne({ workerId: worker['workerId'] });
      expect(found).not.toBeNull();
      expect(found!.workerId).toEqual(worker['workerId']);
      expect(found!.updatedAt).not.toBeNull();
    })
    it('should be called every interval time', async () => {
      const spy = spyOn<any>(worker, 'healthCheck');
      let abort = false;
      worker['startHealthCheck'](() => Promise.resolve(abort));
      await sleep(7000);
      expect(spy).toHaveBeenCalledTimes(2);
      abort = true;
      await sleep(5000);
      expect(spy).toHaveBeenCalledTimes(2);
    })
  })

  describe('checkCronChange', () => {
    it('should update cron if it has changed', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        cronSchedule: 'new',
        workerId: 'workerId',
        status: 'active'
      })
      const task = createSpyObj('task', ['stop']);
      worker['cronRefArray'].set(request.id, ['old', <any>task]);
      await worker['checkCronChange']();
      expect(worker['cronRefArray'].size).toEqual(0);
    })
  })

  describe('pollReplicationWork', () => {
    it('should start a new cron if new request is found and has cron schedule', async () => {
      await Datastore.ReplicationRequestModel.create({
        cronSchedule: 'new',
        workerId: null,
        status: 'active'
      })
      const replicateSpy = spyOn<any>(worker, 'replicate');
      const cronSpy = spyOn(cron, 'schedule').and.returnValue(<any>{
        stop: () => {
        }
      });
      await worker['pollReplicationWork']();
      expect(worker['cronRefArray'].size).toEqual(1);
      expect(replicateSpy).not.toHaveBeenCalled();
      expect(cronSpy).toHaveBeenCalled();
      expect(worker['cronRefArray'].size).toEqual(1);
    })
    it('should directly start replicate if new request is found and has no cron schedule', async () => {
      await Datastore.ReplicationRequestModel.create({
        cronSchedule: undefined,
        workerId: null,
        status: 'active'
      })
      const replicateSpy = spyOn<any>(worker, 'replicate');
      const cronSpy = spyOn(cron, 'schedule').and.returnValue(<any>{
        stop: () => {
        }
      });
      await worker['pollReplicationWork']();
      expect(worker['cronRefArray'].size).toEqual(0);
      expect(replicateSpy).toHaveBeenCalled();
      expect(cronSpy).not.toHaveBeenCalled();
    })
  })
  describe('generateProviderList', () => {
    it('should return list of providers splitted by comma', () => {
      const result = DealReplicationWorker['generateProvidersList']('provider1,provider2,provider3');
      expect(result).toEqual(['provider1', 'provider2', 'provider3']);
    })
  })

  describe('isUsingLotus', () => {
    it('should return true if using lotus', async () => {
      const cmdSpy = spyOn<any>(childprocess, 'exec').and.resolveTo({ stdout: '/fil/storage/mk/1.1.0' });
      await expectAsync(worker['isUsingLotus']('provider')).toBeResolvedTo(true);
      expect(cmdSpy).toHaveBeenCalledWith('boost provider libp2p-info provider');
    })
    it('should return false if using boost', async () => {
      const cmdSpy = spyOn<any>(childprocess, 'exec').and.resolveTo({ stdout: '/fil/storage/mk/1.2.0' });
      await expectAsync(worker['isUsingLotus']('provider')).toBeResolvedTo(false);
      expect(cmdSpy).toHaveBeenCalledWith('boost provider libp2p-info provider');
    })
    it('should throw error if command fails', async () => {
      spyOn<any>(childprocess, 'exec').and.resolveTo({ stdout: 'unknown' });
      await expectAsync(worker['isUsingLotus']('provider')).toBeRejectedWithError('{"stdout":"unknown"}');
    })
  })

  // TODO START of Devnet tests

  describe('checkIsMainnet', () => {
    it('should return false, i.e. not mainnet, if lotus height different from computed height by wide margin.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({status: 200, data: { result: { Height: 12345 }}})
      )
      await expectAsync(worker['checkIsMainnet']()).toBeResolvedTo(false); // Expected a promise to be resolved to false but it was resolved to true.?
    })
    it('should return default true isMainnet, if lotus height and computed height are within close range.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({status: 200, data: { result: { Height: HeightFromCurrentTime() }}})
      )
      await expectAsync(worker['checkIsMainnet']()).toBeResolvedTo(true);
    })
    it('should return default true isMainnet, if lotus height NaN.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({status: 200, data: { result: { Height: "NOT A NUMBER" }}})
      )
      await expectAsync(worker['checkIsMainnet']()).toBeResolvedTo(true);
    })
    it('should return default true isMainnet, if lotus height missing.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({status: 200, data: { result: "nothing" }})
      )
      await expectAsync(worker['checkIsMainnet']()).toBeResolvedTo(true);
    })
        // TODO process.env.FULLNODE_API_INFO

  })

  describe('lotusBlockHeightAPI', () => {
    it('should return lotus chain height, when computed and lotus heights differ widely.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({
              status: 200, data: { result: { Height: 12345 }}
        })
      );
      await expectAsync(worker['lotusBlockHeightAPI']()).toBeResolvedTo(12345);
    })
    it('should return computed chain height, when computed height differs from lotus height.', async () => {
      spyOn<any>(axios, 'post').and.resolveTo(
        Promise.resolve({
              status: 200, data: { result: { Height: 12345 }}
        })
      );
      await expectAsync(worker['lotusBlockHeightAPI']()).toBeResolvedTo(12345);
    })
  })

  // TODO END of Devnet tests

  describe('calculatePriceWithSize', () => {
    it('should calculate price with size', () => {
      const result = DealReplicationWorker['calculatePriceWithSize'](100, 100);
      expect(result).toEqual('0.000009313225746154785');
    })
    it('should calculate price with zero price', () => {
      const result = DealReplicationWorker['calculatePriceWithSize'](0, 100);
      expect(result).toEqual('0');
    })
  })

  describe('assembleDownloadUrl', () => {
    it('should assemble download url', () => {
      const result = DealReplicationWorker['assembleDownloadUrl']('https://example.com', 'cid');
      expect(result).toEqual('https://example.com/cid.car');
    })
    it('should assemble download url with filename override', () => {
      const result = DealReplicationWorker['assembleDownloadUrl']('https://example.com', 'cid', 'filename');
      expect(result).toEqual('https://example.com/filename.car');
    })
  })

  describe('isUrlReachable', () => {
    it('should return true if url is reachable', async () => {
      spyOn<any>(axios, 'head').and.resolveTo({ status: 200 });
      await expectAsync(worker['isUrlReachable']('https://example.com')).toBeResolvedTo(true);
    })
    it('should return false if url is not reachable', async () => {
      spyOn<any>(axios, 'head').and.throwError('error');
      await expectAsync(worker['isUrlReachable']('https://example.com')).toBeResolvedTo(false);
    })
  })

  describe('createDealCmd', () => {
    it('should throw exception if trying to create deal cmd with lotus online deal', async () => {
      await expectAsync(worker['createDealCmd'](true, 'f0xxxx', <any>{
        isOffline: false,
      }, <any>{}, 10000)).toBeRejectedWithError(/for lotus we only support offline deals/);
    })
    it('should generate command when using lotus', async () => {
      const cmd = await worker['createDealCmd'](true, 'f0xxxx', <any>{
        isOffline: true,
        maxPrice: 100,
        isVerfied: true,
        client: 'f1xxxx',
        duration: 10000,
      }, <any>{
        pieceCid: 'piece',
        dataCid: 'data',
        pieceSize: 4096
      }, 10000);
      expect(cmd).toEqual('lotus client deal --manual-piece-cid=piece --manual-piece-size=4064  --from=f1xxxx --verified-deal=true --start-epoch=10000 data f0xxxx 0.0003814697265625 10000');
    })
    it('should throw exception if using boost and url is not reachable', async () => {
      spyOn<any>(worker, 'isUrlReachable').and.resolveTo(false);
      await expectAsync(worker['createDealCmd'](false, 'f0xxxx', <any>{
        isOffline: false,
        urlPrefix: 'https://example.com',
      }, <any>{
        pieceCid: 'piece',
        filenameOverride: 'filename'
      }, 10000)).toBeRejectedWithError('https://example.com/filename.car is not reachable');
    })
    it('should generate command when using boost offline deal', async () => {
      spyOn<any>(worker, 'isUrlReachable').and.resolveTo(true);
      const cmd = await worker['createDealCmd'](false, 'f0xxxx', <any>{
        isOffline: true,
        urlPrefix: 'https://example.com',
        maxPrice: 100,
        isVerfied: true,
        client: 'f1xxxx',
        duration: 10000,
      }, <any>{
        pieceCid: 'piece',
        dataCid: 'data',
        pieceSize: 4096,
        filenameOverride: 'filename'
      }, 10000);
      expect(cmd).toEqual('boost offline-deal --provider=f0xxxx  --commp=piece --car-size=undefined --piece-size=4096 --payload-cid=data --storage-price=100000000000000000000 --start-epoch=10000 --verified=true --wallet=f1xxxx --duration=10000');
    })
    it('should generate command when using boost online deal', async () => {
      spyOn<any>(worker, 'isUrlReachable').and.resolveTo(true);
      const cmd = await worker['createDealCmd'](false, 'f0xxxx', <any>{
        isOffline: false,
        urlPrefix: 'https://example.com',
        maxPrice: 100,
        isVerfied: true,
        client: 'f1xxxx',
        duration: 10000,
      }, <any>{
        pieceCid: 'piece',
        dataCid: 'data',
        pieceSize: 4096,
        filenameOverride: 'filename'
      }, 10000);
      expect(cmd).toEqual('boost deal --http-url=https://example.com/filename.car --provider=f0xxxx  --commp=piece --car-size=undefined --piece-size=4096 --payload-cid=data --storage-price=100000000000000000000 --start-epoch=10000 --verified=true --wallet=f1xxxx --duration=10000');
    })
  })
  describe('stopCronIfExist', () => {
    it('should stop cron if exist', () => {
      const task = createSpyObj('task', ['stop']);
      worker['cronRefArray'].set('id', ['schedule', task]);
      worker['stopCronIfExist']('id');
      expect(task.stop).toHaveBeenCalled();
      expect(worker['cronRefArray'].size).toEqual(0);
    })
    it('should not stop cron if not exist', () => {
      expect(() => worker['stopCronIfExist']('')).not.toThrow();
    })
  })
  describe('checkAndMarkCompletion', () => {
    it('should mark completion if request does not have cron schedule', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        storageProviders: 'f0xxxx,f1xxxx',
        cronSchedule: undefined,
        status: 'active',
      })
      const result = await worker['checkAndMarkCompletion'](request, 10);
      expect(result).toBeTrue();
      expect((await Datastore.ReplicationRequestModel.findById(request.id))!.status).toEqual('completed');
    })
    it('should mark completion if deals made is more than max number of deals', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        storageProviders: 'f0xxxx,f1xxxx',
        cronSchedule: '* * * * *',
        status: 'active',
        cronMaxDeals: 100,
        maxNumberOfDeals: 10,
      })
      spyOn(Datastore.DealStateModel, 'count').and.resolveTo(250)
      const result = await worker['checkAndMarkCompletion'](request, 1000);
      expect(result).toBeTrue();
      expect((await Datastore.ReplicationRequestModel.findById(request.id))!.status).toEqual('completed');
    })
    it('should mark completion if deals made is more maxReplica', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        storageProviders: 'f0xxxx,f1xxxx',
        cronSchedule: '* * * * *',
        status: 'active',
        cronMaxDeals: 1000,
        maxNumberOfDeals: 10,
        maxReplicas: 2
      })
      spyOn(Datastore.DealStateModel, 'count').and.resolveTo(250)
      const result = await worker['checkAndMarkCompletion'](request, 100);
      expect(result).toBeTrue();
      expect((await Datastore.ReplicationRequestModel.findById(request.id))!.status).toEqual('completed');
    })
    it('should not mark completion if the deal is not completed', async () => {
      const request = await Datastore.ReplicationRequestModel.create({
        storageProviders: 'f0xxxx,f1xxxx',
        cronSchedule: '* * * * *',
        status: 'active',
        cronMaxDeals: 1000,
        maxNumberOfDeals: 10,
        maxReplicas: 2
      })
      spyOn(Datastore.DealStateModel, 'count').and.resolveTo(10)
      const result = await worker['checkAndMarkCompletion'](request, 100);
      expect(result).toBeFalse();
      expect((await Datastore.ReplicationRequestModel.findById(request.id))!.status).toEqual('active');
    })
  })
  describe('makeDeal', () => {
    it('should throw error if lotus command failed', async () => {
      spyOn(childprocess, 'exec').and.resolveTo({
        stdout: '',
        stderr: 'error'
      })
      const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
          10, true, 10);
      expect(dealCid).toEqual('unknown');
      expect(errorMsg).toEqual('error');
      expect(state).toEqual('error');
      expect(retryTimeout).toEqual(80);
    })

    it ('should make deal if lotus command succeeds', async () => {
        spyOn(childprocess, 'exec').and.resolveTo({
            stdout: 'bafy',
            stderr: ''
        })
        const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
            10, true, 10);
        expect(dealCid).toEqual('bafy');
        expect(errorMsg).toEqual('');
        expect(state).toEqual('proposed');
        expect(retryTimeout).toEqual(10);
    })

    it('should throw error if lotus command failed with provider collateral warning', async () => {
        spyOn(childprocess, 'exec').and.resolveTo({
            stdout: '',
            stderr: 'proposed provider collateral below minimum'
        })
        const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
            10, true, 10);
        expect(dealCid).toEqual('unknown');
        expect(errorMsg).toEqual('proposed provider collateral below minimum');
        expect(state).toEqual('error');
        expect(retryTimeout).toEqual(10);
    })

    it('should throw error if boost command failed', async () => {
        spyOn(childprocess, 'exec').and.resolveTo({
            stdout: '',
            stderr: 'error'
        })
        const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
            10, false, 10);
        expect(dealCid).toEqual('unknown');
        expect(errorMsg).toEqual('error');
        expect(state).toEqual('error');
        expect(retryTimeout).toEqual(80);
    })

    it('should make deal if boost command succeeds', async () => {
        spyOn(childprocess, 'exec').and.resolveTo({
            stdout: 'deal uuid: bafy',
            stderr: ''
        })
        const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
            10, false, 10);
        expect(dealCid).toEqual('bafy');
        expect(errorMsg).toEqual('');
        expect(state).toEqual('proposed');
        expect(retryTimeout).toEqual(10);
    })

    it('should throw error if exec throws error', async () => {
      spyOn(childprocess, 'exec').and.throwError('error')
      const {dealCid, errorMsg, state, retryTimeout} = await worker['makeDeal']('cmd', 'piece_cid', 'provider',
          10, false, 10);
      expect(dealCid).toEqual('unknown');
      expect(errorMsg).toEqual('Error: error');
      expect(state).toEqual('error');
      expect(retryTimeout).toEqual(80);
    })
  })
})
