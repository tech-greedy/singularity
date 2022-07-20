import Utils from '../Utils';
import DealTrackingService from '../../src/deal-tracking/DealTrackingService';
import Datastore from '../../src/common/Datastore';
import axios from 'axios';

describe('DealTrackingService', () => {
  let service: DealTrackingService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealTrackingService();
  });
  beforeEach(async () => {
    await Datastore.DealStateModel.deleteMany();
    await Datastore.DealTrackingStateModel.deleteMany();
  })
  describe('start', () => {
    it('should start the service', () => {
      const trackingSpy = spyOn<any>(service, 'startDealTracking').and.stub();
      service.start();
      expect(trackingSpy).toHaveBeenCalled()
    })
  })
  describe('dealTracking', () => {
    it('should start tracking on all clients', async () => {
      await Datastore.DealTrackingStateModel.create({
        stateType: 'client',
        stateKey: 'client1',
        stateValue: 'track'
      });
      await Datastore.DealTrackingStateModel.create({
        stateType: 'client',
        stateKey: 'client2',
        stateValue: 'track'
      });
      await Datastore.DealStateModel.create({
        client: 'client1',
        provider: 'provider',
        dealId: 1000000,
        state: 'published'
      });
      const insertSpy = spyOn<any>(service, 'insertDealFromFilscan').and.stub();
      const updateSpy = spyOn<any>(service, 'updateDealFromLotus').and.stub();
      await service['dealTracking']();
      expect(insertSpy).toHaveBeenCalledWith('client1', 1000000);
      expect(insertSpy).toHaveBeenCalledWith('client2', 0);
      expect(updateSpy).toHaveBeenCalledWith('client1');
      expect(updateSpy).toHaveBeenCalledWith('client2');
    })
  })
  describe('updateDealFromLotus', () => {
    it('should update state to slashed if the deal cannot be found anymore', async () => {
      const dealState = await Datastore.DealStateModel.create({
        client: 'client',
        provider: 'provider',
        dealId: 5083158,
        state: 'published'
      });
      const spy = spyOn(axios, 'post').and.resolveTo({data: {
          "jsonrpc": "2.0",
          "id": 1,
          "error": {
            "code": 1,
            "message": "deal 1000000 not found - deal may not have completed sealing before deal proposal start epoch, or deal may have been slashed"
          }
        }})
      await service['updateDealFromLotus']('client');
      expect(spy).toHaveBeenCalledWith('https://api.node.glif.io/rpc/v0', {
        "id":1,
        "jsonrpc":"2.0",
        "method":"Filecoin.StateMarketStorageDeal",
        "params":[5083158, null]
      }, { headers: {}});
      const found = await Datastore.DealStateModel.findById(dealState.id);
      expect(found).toEqual(jasmine.objectContaining({
        state: 'slashed'
      }))
    })
    it('should update to slashed if the slash epoch is more than zero', async () => {
      const dealState = await Datastore.DealStateModel.create({
        client: 'client',
        provider: 'provider',
        dealId: 5083158,
        state: 'published'
      });
      const spy = spyOn(axios, 'post').and.resolveTo({data: {
          "jsonrpc": "2.0",
          "result": {
            "Proposal": {
              "PieceCID": {
                "/": "baga6ea4seaqhrk3pg4thkoaukss2ehqer65t3gtj7e4hku6jmkdoygnc6wzoeca"
              },
              "PieceSize": 34359738368,
              "VerifiedDeal": true,
              "Client": "f0743060",
              "Provider": "f01240",
              "Label": "mAXASIIejKSFu+zszukahTB2/iEE21pLlxLRQAUypJiC6Vxar",
              "StartEpoch": 1683358,
              "EndEpoch": 3152158,
              "StoragePricePerEpoch": "0",
              "ProviderCollateral": "6259889973909030",
              "ClientCollateral": "0"
            },
            "State": {
              "SectorStartEpoch": 1681205,
              "LastUpdatedEpoch": 1696278,
              "SlashEpoch": 1696278
            }
          },
          "id": 1
        }})
      await service['updateDealFromLotus']('client');
      expect(spy).toHaveBeenCalledWith('https://api.node.glif.io/rpc/v0', {
        "id":1,
        "jsonrpc":"2.0",
        "method":"Filecoin.StateMarketStorageDeal",
        "params":[5083158, null]
      }, { headers: {}});
      const found = await Datastore.DealStateModel.findById(dealState.id);
      expect(found).toEqual(jasmine.objectContaining({
        pieceCid: 'baga6ea4seaqhrk3pg4thkoaukss2ehqer65t3gtj7e4hku6jmkdoygnc6wzoeca',
        expiration: 3152158,
        state: 'slashed'
      }))
    })
    it('should update published deal to active', async () => {
      const dealState = await Datastore.DealStateModel.create({
        client: 'client',
        provider: 'provider',
        dealId: 5083158,
        state: 'published'
      });
      const spy = spyOn(axios, 'post').and.resolveTo({data: {
          "jsonrpc": "2.0",
          "result": {
            "Proposal": {
              "PieceCID": {
                "/": "baga6ea4seaqhrk3pg4thkoaukss2ehqer65t3gtj7e4hku6jmkdoygnc6wzoeca"
              },
              "PieceSize": 34359738368,
              "VerifiedDeal": true,
              "Client": "f0743060",
              "Provider": "f01240",
              "Label": "mAXASIIejKSFu+zszukahTB2/iEE21pLlxLRQAUypJiC6Vxar",
              "StartEpoch": 1683358,
              "EndEpoch": 3152158,
              "StoragePricePerEpoch": "0",
              "ProviderCollateral": "6259889973909030",
              "ClientCollateral": "0"
            },
            "State": {
              "SectorStartEpoch": 1681205,
              "LastUpdatedEpoch": 1696278,
              "SlashEpoch": -1
            }
          },
          "id": 1
        }})
      await service['updateDealFromLotus']('client');
      expect(spy).toHaveBeenCalledWith('https://api.node.glif.io/rpc/v0', {
        "id":1,
        "jsonrpc":"2.0",
        "method":"Filecoin.StateMarketStorageDeal",
        "params":[5083158, null]
      }, { headers: {}});
      const found = await Datastore.DealStateModel.findById(dealState.id);
      expect(found).toEqual(jasmine.objectContaining({
        pieceCid: 'baga6ea4seaqhrk3pg4thkoaukss2ehqer65t3gtj7e4hku6jmkdoygnc6wzoeca',
        expiration: 3152158,
        state: 'active'
      }))
    })
  })

  xdescribe('insertDealFromFilscan', () => {
    xit('should download all deal ids for a client - real network call', async () => {
      await service['insertDealFromFilscan']('f3vfs6f7tagrcpnwv65wq3leznbajqyg77bmijrpvoyjv3zjyi3urq25vigfbs3ob6ug5xdihajumtgsxnz2pa', 0);
      const stored = await Datastore.DealStateModel.find({});
      expect(stored.length).toEqual(767);
      expect(stored[1].dealId).toEqual(766);
    }, 5 * 60 * 1000);

    it('should handle duplication deals', async () => {
      const spy = spyOn(axios, 'get').and.returnValues(Promise.resolve({
        data: {
          deals: [
            {
              id: 100,
              provider: 'provider1'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
            {
              id: 100,
              provider: 'provider2'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
          ]
        }
      }));
      await service['insertDealFromFilscan']('test_client', 0);
      expect(spy).toHaveBeenCalledTimes(3);
      const stored = await Datastore.DealStateModel.find({});
      expect(stored.length).toEqual(1);
      expect(stored[0].provider).toEqual('provider1');
    })

    it('should store deals from last deal', async () => {
      const spy = spyOn(axios, 'get').and.returnValues(Promise.resolve({
        data: {
          deals: [
            {
              id: 200,
              provider: 'provider1'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
            {
              id: 100,
              provider: 'provider2'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
          ]
        }
      }));
      await service['insertDealFromFilscan']('test_client', 100);
      expect(spy).toHaveBeenCalledTimes(2);
      const stored = await Datastore.DealStateModel.find({});
      expect(stored.length).toEqual(1);
    })

    it('should store all deal ids to database', async () => {
      const spy = spyOn(axios, 'get').and.returnValues(Promise.resolve({
        data: {
          deals: [
            {
              id: 200,
              provider: 'provider1'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
            {
              id: 100,
              provider: 'provider2'
            }
          ]
        }
      }),Promise.resolve({
        data: {
          deals: [
          ]
        }
      }));
      await service['insertDealFromFilscan']('test_client', 0);
      expect(spy).toHaveBeenCalledTimes(3);
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=0');
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=1');
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=2');
      const stored = await Datastore.DealStateModel.find({});
      expect(stored.length).toEqual(2);
    })
  })
})
