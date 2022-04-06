import Utils from '../Utils';
import DealTrackingService from '../../src/deal-tracking/DealTrackingService';
import Datastore from '../../src/common/Datastore';
import axios from 'axios';

fdescribe('DealTrackingService', () => {
  let service: DealTrackingService;
  beforeAll(async () => {
    await Utils.initDatabase();
    service = new DealTrackingService();
  });
  beforeEach(async () => {
    await Datastore.DealStateModel.remove();
    await Datastore.DealTrackingStateModel.remove();
  })
  describe('updateDealFromLotus', () => {
    fit('should update all deal status - real network call', async () => {
      Datastore.DealStateModel.create({
        dealId:
      });
    })
  })
  describe('insertDealFromFilfox', () => {
    xit('should download all deal ids for a client - real network call', async () => {
      await service['insertDealFromFilfox']('f3vfs6f7tagrcpnwv65wq3leznbajqyg77bmijrpvoyjv3zjyi3urq25vigfbs3ob6ug5xdihajumtgsxnz2pa', 0);
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
      await service['insertDealFromFilfox']('test_client', 0);
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
      await service['insertDealFromFilfox']('test_client', 100);
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
      await service['insertDealFromFilfox']('test_client', 0);
      expect(spy).toHaveBeenCalledTimes(3);
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=0');
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=1');
      expect(spy).toHaveBeenCalledWith('https://filfox.info/api/v1/deal/list?address=test_client&pageSize=100&page=2');
      const stored = await Datastore.DealStateModel.find({});
      expect(stored.length).toEqual(2);
    })
  })
})
