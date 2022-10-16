import Datastore from '../../src/common/Datastore';
import GenerateCsv from '../../src/common/GenerateCsv';

describe('GenerateCsv', () => {
  it ('should be able to gen with valid id', async () => {
    const request = await Datastore.ReplicationRequestModel.create({
      storageProviders: 'f01001',
      status: 'completed',
      urlPrefix: 'http://localhost:3000'
    });
    await Datastore.DealStateModel.create({
      dealId: 2,
      client: 'f01001',
      provider: 'f01001',
      state: 'active',
      replicationRequestId: request.id
    });
    await GenerateCsv.generate(request.id, '/tmp');
  })
  it ('should not be able to gen without valid id', async () => {
    await GenerateCsv.generate('634b71ada25b9be2a58e434c', '/tmp');
  })
});
