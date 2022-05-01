import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import DealPreparationWorker from '../../src/deal-preparation/DealPreparationWorker';
import Scanner from '../../src/deal-preparation/Scanner';
import { FileList } from '../../src/common/model/GenerationRequest';
import { spawn } from 'child_process';
import { onExit, readableToString, streamEnd, streamWrite } from '@rauschma/stringio';

describe('DealPreparationWorker', () => {
  let worker: DealPreparationWorker;
  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealPreparationWorker();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.remove();
    await Datastore.GenerationRequestModel.remove();
  });
  describe('startPollWork', () => {
    it('should immediately start next job if Scan work finishes', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(true);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(false);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).not.toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['ImmediatePollInterval']);
    })
    it('should immediately start next job if Generation work finishes', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(false);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(true);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['ImmediatePollInterval']);
    })
    it('should poll for next job after 5s if no work found', async () => {
      const spy = spyOn(global,'setTimeout');
      const spyScanning = spyOn<any>(worker, 'pollScanningWork').and.resolveTo(false);
      const spyGeneration = spyOn<any>(worker, 'pollGenerationWork').and.resolveTo(false);
      await worker['startPollWork']();
      expect(spyScanning).toHaveBeenCalled();
      expect(spyGeneration).toHaveBeenCalled();
      expect(spy).toHaveBeenCalledWith(jasmine.anything(), worker['PollInterval']);
    })
  })
  describe('healthCheck', () => {
    it('should create an entry in HealthCheck table', async () => {
      await worker['healthCheck']();
      const found = await Datastore.HealthCheckModel.findOne({ workerId: worker['workerId'] });
      expect(found).not.toBeNull();
      expect(found!.workerId).toEqual(worker['workerId']);
      expect(found!.updatedAt).not.toBeNull();
    })
  })
  describe('pollWork', () => {
    it('should update with error if file no longer exists', async () => {
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        status: 'active',
        fileList: [
          {
            path: 'tests/test_folder/not_exists.txt',
            name: '1.txt',
            size: 3,
            start: 0,
            end: 0
          }
        ]
      });
      await worker['pollWork']();
      const found = await Datastore.GenerationRequestModel.findById(created.id);
      expect<any>(found).toEqual(jasmine.objectContaining({
        status: 'error',
        errorMessage: jasmine.stringContaining('no such file or directory')
      }));
    })
    it('should generate commp, car files', async () => {
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        status: 'active',
        fileList: [
          {
            path: 'tests/test_folder/a/1.txt',
            name: '1.txt',
            size: 3,
            start: 0,
            end: 0
          }, {
            path: 'tests/test_folder/b/2.txt',
            name: '2.txt',
            size: 27,
            start: 0,
            end: 9
          }
        ]
      });
      await worker['pollWork']();
      const found = await Datastore.GenerationRequestModel.findById(created.id);
      expect(found).toEqual(jasmine.objectContaining({
        status: 'completed',
        dataCid: 'bafybeih2nwd66s7rstnbj4grzjw7re4lyhmx3auvphibbz7nalo4ygfypq',
        pieceCid: 'baga6ea4seaqoqixvkneyg6tzwfoqsmw33xdva3aywkawp6n5jd5tffjdmqrn6gy',
        pieceSize: 512
      }));
    })
    it('should insert the database with fileLists', async () => {
      const created = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: 'tests/test_folder',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      expect(await worker['pollWork']()).toEqual(true);
      const found = await Datastore.ScanningRequestModel.findById(created.id);
      expect(found!.status).toEqual('completed');
      expect(await Datastore.GenerationRequestModel.find({ datasetId: created.id })).toHaveSize(4);
    })
    it('should update the database with error message if it counter any error', async () => {
      const created = await Datastore.ScanningRequestModel.create({
        name: 'name',
        path: '/home/shane/test_folder_not_exist',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      expect(await worker['pollWork']()).toEqual(true);
      expect(await Datastore.ScanningRequestModel.findById(created.id)).toEqual(jasmine.objectContaining({
        status: 'error',
        errorMessage: jasmine.stringContaining('ENOENT')
      }))
    })
  })
  describe('buildSelector', () => {
    it('should return correct selector', () => {
      const fileList : FileList = ['base/path/a.txt', 'base/path/b/c/d.txt', 'base/path/c/d.txt', 'base/path/c/d2.txt', 'base/path/e.txt'].map((path) => ({
        path, selector: [], start: 0, end: 0, size: 0, name: 'dummy'
      }));
      Scanner.buildSelector("base/path", fileList);
      expect(fileList[0].selector).toEqual([0]);
      expect(fileList[1].selector).toEqual([1,0,0]);
      expect(fileList[2].selector).toEqual([2,0]);
      expect(fileList[3].selector).toEqual([2,1]);
      expect(fileList[4].selector).toEqual([3]);
    })
  })
  describe('scan', () => {
    it('should get the correct fileList', async () => {
      await worker['scan']({
        id: 'id',
        name: 'name',
        path: 'tests/test_folder',
        minSize: 12,
        maxSize: 16,
        status: 'active'
      });
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 } });
      /**
       * a/1.txt -> 3 bytes
       * b/2.txt -> 27 bytes
       * c/3.txt -> 9 bytes
       * d.txt   -> 9 bytes (symlink)
       * 0. a/1.txt (3) + b/2.txt (9) = 12
       * 1. b/2.txt(12) = 12
       * 2. b/2.txt(6) + c/3.txt(9) = 15
       * 3. d.txt(9) = 9
       */
      expect(requests.length).toEqual(4);
      expect(requests[0]).toEqual(jasmine.objectContaining({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        fileList: [jasmine.objectContaining({
          path: 'tests/test_folder/a/1.txt',
          name: '1.txt',
          size: 3,
          start: 0,
          end: 0,
          selector: [0, 0]
        }), jasmine.objectContaining({
          path: 'tests/test_folder/b/2.txt',
          name: '2.txt',
          size: 27,
          start: 0,
          end: 9,
          selector: [1, 0]
        })],
        status: 'active',
      }));
      expect(requests[1]).toEqual(jasmine.objectContaining({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 1,
        fileList: [jasmine.objectContaining({
          path: 'tests/test_folder/b/2.txt',
          name: '2.txt',
          size: 27,
          start: 9,
          end: 21,
          selector: [0, 0]
        })],
        status: 'active',
      }));
      expect(requests[2]).toEqual(jasmine.objectContaining({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 2,
        fileList: [jasmine.objectContaining({
          path: 'tests/test_folder/b/2.txt',
          name: '2.txt',
          size: 27,
          start: 21,
          end: 27,
          selector: [0, 0]
        }), jasmine.objectContaining({
          path: 'tests/test_folder/c/3.txt',
          name: '3.txt',
          size: 9,
          start: 0,
          end: 0,
          selector: [1, 0]
        })],
        status: 'active',
      }));
      expect(requests[3]).toEqual(jasmine.objectContaining({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 3,
        fileList: [jasmine.objectContaining({
          path: 'tests/test_folder/d.txt',
          name: 'd.txt',
          size: 9,
          start: 0,
          end: 0,
          selector: [0]
        })],
        status: 'active',
      }));
    })
  })
})
