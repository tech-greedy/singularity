import Datastore from '../../src/common/Datastore';
import GenerateCsv from '../../src/common/GenerateCsv';
import Utils from '../Utils';

describe('GenerateCsv', () => {
  beforeAll(async () => {
    await Utils.initDatabase();
  });
  beforeEach(async () => {
    await Datastore.ReplicationRequestModel.deleteMany();
    await Datastore.DealStateModel.deleteMany();
  });
  it ('should fail export with no deals', async () => {
    const request = await Datastore.ReplicationRequestModel.create({
      storageProviders: 'f01001',
      status: 'completed',
      urlPrefix: 'http://localhost:3000'
    });
    const runCSV = await GenerateCsv.generate(request.id, '/tmp');
    await expect(runCSV).toEqual(`No deal found to export in ${request.id}\n`)
  }) 
  it ('should be able to gen with valid id', async () => {
    const request = await Datastore.ReplicationRequestModel.create({
      storageProviders: 'f01001',
      status: 'completed',
      urlPrefix: 'http://localhost:3000/'
    });
    await Datastore.DealStateModel.create({
      dealId: 2,
      client: 'f01001',
      provider: 'f01001',
      state: 'proposed',
      replicationRequestId: request.id
    });
    const runCSV = await GenerateCsv.generate(request.id, '/tmp');
    await expect(runCSV).toEqual(`CSV saved to /tmp/f01001_${request.id}.csv\n`)
  })

  it ('should be able to gen with valid id with file list path', async () => {
    const request = await Datastore.ReplicationRequestModel.create({
      storageProviders: 'f01001',
      fileListPath: '-path-',
      status: 'completed',
      urlPrefix: 'http://localhost:3000'
    });
    await Datastore.DealStateModel.create({
      dealId: 2,
      client: 'f01001',
      provider: 'f01001',
      state: 'proposed',
      replicationRequestId: request.id
    });
    const runCSV = await GenerateCsv.generate(request.id, '/tmp');
    await expect(runCSV).toEqual(`CSV saved to /tmp/f01001_-path-_${request.id}.csv\n`)
  })

  it ('should not be able to gen without valid id', async () => {
    const runCSV = await GenerateCsv.generate('634b71ada25b9be2a58e434c', '/tmp');
    await expect(runCSV).toEqual(`Replication request not found 634b71ada25b9be2a58e434c`)
  })
});
