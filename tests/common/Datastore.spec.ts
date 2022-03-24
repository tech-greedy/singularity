import { randomUUID } from 'crypto';
import Datastore from '../../src/common/Datastore';
import Utils from '../Utils';

describe('Datastore', () => {
  beforeAll(Utils.initDatabase)

  describe('HealthCheckModel', () => {
    it('should be able to create and fetch entries', async () => {
      const model = new Datastore.HealthCheckModel();
      model.workerId = randomUUID();
      await model.save();

      const found = await Datastore.HealthCheckModel.findOne({ workerId: model.workerId });
      expect(found).not.toBeNull();
      expect(found!.workerId).toEqual(model.workerId);
    })
  })

  describe('GenerationRequestModel', () => {
    it('should be able to create and fetch entries', async () => {
      const model = new Datastore.GenerationRequestModel();
      model.datasetName = 'name';
      const fileInfo1 = {name: 'name2', path: 'path', start: 0, end: 0 , size: 1024}
      const fileInfo2 = {name: 'name1', path: 'path', start: 0, end: 0 , size: 1024}
      const fileInfo3 = {name: 'name3', path: 'path', start: 0, end: 0 , size: 1024}
      model.fileList = [fileInfo1, fileInfo2, fileInfo3];
      await model.save();

      const found = await Datastore.GenerationRequestModel.findOne({ name: model.datasetName });
      expect(found).not.toBeNull();
      expect(found!.datasetName).toEqual(model.datasetName);
      expect(found!.fileList.length).toEqual(3);
      // Make sure the order is preserved
      expect(found!.fileList[0]).toEqual(jasmine.objectContaining(fileInfo1));
      expect(found!.fileList[1]).toEqual(jasmine.objectContaining(fileInfo2));
      expect(found!.fileList[2]).toEqual(jasmine.objectContaining(fileInfo3));
    })
  })

  describe('ScanningRequestModel', () => {
    it('should be able to create and fetch entries', async () => {
      const model = new Datastore.ScanningRequestModel();
      model.name = 'name'
      await model.save();

      const found = await Datastore.ScanningRequestModel.findOne({ name: model.name });
      expect(found).not.toBeNull();
      expect(found!.name).toEqual(model.name);

      const model2 = new Datastore.ScanningRequestModel();
      model2.name = 'name'
      await expectAsync(model2.save()).toBeRejectedWithError(/E11000 duplicate key error collection/);
    })
  })
});
