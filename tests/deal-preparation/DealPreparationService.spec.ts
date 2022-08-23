import Datastore from '../../src/common/Datastore';
import DealPreparationService from '../../src/deal-preparation/DealPreparationService';
import Utils from '../Utils';
import fs from 'fs/promises';
import { sleep } from '../../src/common/Util';

describe('DealPreparationService', () => {
  let service: DealPreparationService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealPreparationService();
  });
  beforeEach(async () => {
    await Datastore.HealthCheckModel.deleteMany();
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
  describe('cleanupIncompleteFiles', () => {
    it('should delete the incomplete files', async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'test-deletion',
        outDir: '.',
        tmpDir: './tmp'
      });
      await fs.mkdir('./tmp/d715461e-8d42-4a53-9b33-e17ed4247304', { recursive: true });
      await fs.writeFile('./d715461e-8d42-4a53-9b33-e17ed4247304.car', 'something');
      await DealPreparationService.cleanupIncompleteFiles(service['logger']);
      await expectAsync(fs.access('./d715461e-8d42-4a53-9b33-e17ed4247304.car')).toBeRejected();
      await expectAsync(fs.access('./d715461e-8d42-4a53-9b33-e17ed4247304')).toBeRejected();
    })
    it('should skip nonexisting folders', async () => {
      await Datastore.ScanningRequestModel.create({
        name: 'test-deletion',
        outDir: './non-existing',
        tmpDir: './non-existing',
      });
      await expectAsync(DealPreparationService.cleanupIncompleteFiles(service['logger'])).toBeResolved();
    })
  })
});
