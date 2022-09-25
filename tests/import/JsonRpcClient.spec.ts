import JsonRpcClient from '../../src/import/JsonRpcClient';
import axios from 'axios';

describe('JsonRpcClient', () => {
  describe('call', () => {
    it('should throw error if call failed', async () => {
      const client = new JsonRpcClient('http://localhost:8080');
      await expectAsync(client.call('test', {})).toBeRejectedWith(jasmine.objectContaining({
        code: 'ECONNREFUSED'
      }));
    })
    it('should make the JSON RPC compliant call', async () => {
      const config = {
        headers: {}
      }
      const params = {
        foo: 'bar'
      }
      const client = new JsonRpcClient('http://localhost:3000/api', 'prefix', config);
      const postSpy = spyOn(axios, 'post').and.resolveTo({
        data: {
          id: 1,
          jsonrpc: '2.0',
          result: 'result',
        }
      });
      const result = await client.call('test', params);
      expect(result).toEqual({
        id: 1,
        jsonrpc: '2.0',
        result: 'result',
      });
      expect(postSpy).toHaveBeenCalledOnceWith('http://localhost:3000/api', {
          id: jasmine.anything(),
          jsonrpc: '2.0',
          method: 'prefixtest',
          params
        },
        config);
    });
  })
})
