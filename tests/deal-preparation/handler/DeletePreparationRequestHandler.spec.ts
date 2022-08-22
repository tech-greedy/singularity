import supertest from 'supertest';
import Datastore from '../../../src/common/Datastore';
import DealPreparationService from '../../../src/deal-preparation/DealPreparationService';
import Utils from '../../Utils';
import path from 'path';
import fs from 'fs/promises';

describe('DeletePreparationRequestHandler', () => {
  let service: DealPreparationService;
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

  describe('DELETE /preparation/:id', () => {
    it('should delete the entries and car files', async () => {
      const scanning = await Datastore.ScanningRequestModel.create({
        name: 'test-deletion',
        outDir: '.',
      });
      const generation = await Datastore.GenerationRequestModel.create({
        datasetId: scanning.id,
        pieceCid: 'bafy',
        outDir: '.',
      });
      const inputList = await Datastore.InputFileListModel.create({
        generationId: generation.id
      });
      const outputList = await Datastore.OutputFileListModel.create({
        generationId: generation.id
      });
      const filePath = path.resolve('./bafy.car');
      await fs.mkdir(path.dirname(filePath), {recursive: true});
      await fs.writeFile(filePath, 'some data');
      const response = await supertest(service['app'])
        .delete('/preparation/test-deletion').send({ purge: true }).set('Accept', 'application/json');
      expect(response.status).toEqual(200);
      await expectAsync(Datastore.ScanningRequestModel.findById(scanning.id)).toBeResolvedTo(null);
      await expectAsync(Datastore.GenerationRequestModel.findById(generation.id)).toBeResolvedTo(null);
      await expectAsync(Datastore.GenerationRequestModel.findById(inputList.id)).toBeResolvedTo(null);
      await expectAsync(Datastore.GenerationRequestModel.findById(outputList.id)).toBeResolvedTo(null);
      await expectAsync(fs.access(filePath)).toBeRejected();
    })
  })
})
