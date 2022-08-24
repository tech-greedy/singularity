import Utils from '../Utils';
import Datastore from '../../src/common/Datastore';
import DealPreparationWorker from '../../src/deal-preparation/DealPreparationWorker';
import fs from 'fs-extra';
import GenerateCar from '../../src/common/GenerateCar';
import * as GenerationProcessor from '../../src/deal-preparation/worker/GenerationProcessor';
import { sleep } from '../../src/common/Util';
describe('DealPreparationWorker', () => {
  let worker: DealPreparationWorker;
  let defaultTimeout: number;
  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealPreparationWorker();
    GenerateCar.initialize();
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
  });
  afterAll(async () => {
    for (const file of await fs.readdir('.')) {
      if (file.endsWith('.car')) {
        await fs.rm(file);
      }
    }
    await fs.rm('tests/subfolder1', { recursive: true, force: true });
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  })
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
  describe('pollWork', () => {
    it('should update with error if file no longer exists', async () => {
      const created = await Datastore.GenerationRequestModel.create({
        datasetId: 'id',
        datasetName: 'name',
        path: 'tests/test_folder',
        index: 0,
        status: 'active',
        outDir: '.',
      });
      await Datastore.InputFileListModel.create({
        generationId: created.id,
        index: 0,
        fileList: [
          {
            path: 'tests/test_folder/not_exists.txt',
            size: 3,
            start: 0,
            end: 0
          }
        ]
      })
      await worker['pollWork']();
      const found = await Datastore.GenerationRequestModel.findById(created.id);
      expect<any>(found).toEqual(jasmine.objectContaining({
        status: 'error',
        errorMessage: jasmine.stringMatching(/no such file or directory|cannot find the file/)
      }));
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
  it('should update the database with error message if processGeneration throws any error', async () => {
    spyOn(GenerationProcessor, 'processGeneration').and.throwError('custom error');
    const created = await Datastore.GenerationRequestModel.create({
        datasetId: 'id',
        datasetName: 'name',
        status: 'active',
    });
    await worker['pollGenerationWork']();
    expect(await Datastore.GenerationRequestModel.findById(created.id)).toEqual(jasmine.objectContaining({
      status: 'error',
      errorMessage: 'custom error'
    }));
  });
  it('should remove the tmpdir after processGeneration', async () => {
    const tmpDir = './tmpdir/test_tmpdir_deletion'
    await fs.mkdirp(tmpDir);
    spyOn(GenerationProcessor, 'processGeneration').and.resolveTo({
      tmpDir, finished: true
    });
    await Datastore.GenerationRequestModel.create({
      datasetId: 'id',
      datasetName: 'name',
      status: 'active',
    });
    await worker['pollGenerationWork']();
    expect(await fs.pathExists(tmpDir)).toBe(false);
  })
  it('should not throw when start', () => {
    new DealPreparationWorker().start();
  })
})
