import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import DealPreparationWorker from '../../src/deal-preparation/DealPreparationWorker';

describe("DealPreparationWorker", () => {
  let worker : DealPreparationWorker;
  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealPreparationWorker();
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.remove();
    await Datastore.GenerationRequestModel.remove();
  });
  describe("healthCheck", () => {
    it('should create an entry in HealthCheck table', async () => {
      await worker['healthCheck']();
      const found = await Datastore.HealthCheckModel.findOne({workerId: worker['workerId']});
      expect(found).not.toBeNull();
      expect(found!.workerId).toEqual(worker['workerId']);
      expect(found!.updatedAt).not.toBeNull();
    })
  })
  describe('pollWork', () => {
    it ('should insert the database with fileLists', async () => {
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
      expect(await Datastore.GenerationRequestModel.find({datasetId: created.id})).toHaveSize(4);
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
      const requests = await Datastore.GenerationRequestModel.find({}, null, { sort: { index: 1 }});
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
          end: 0
        }),jasmine.objectContaining({
          path: 'tests/test_folder/b/2.txt',
          name: '2.txt',
          size: 27,
          start: 0,
          end: 9
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
          end: 21
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
          end: 27
        }),jasmine.objectContaining({
          path: 'tests/test_folder/c/3.txt',
          name: '3.txt',
          size: 9,
          start: 0,
          end: 0
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
          end: 0
        })],
        status: 'active',
      }));
    })
  })
})
