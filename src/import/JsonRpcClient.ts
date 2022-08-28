import axios, { AxiosRequestConfig } from 'axios';
import { randomUUID } from 'crypto';

export class JsonRpcError implements Error {
  name = 'JsonRpcError';
  message: string;
  stack?: string;

  constructor (message: string, public code: string) {
    this.message = message || 'Server returned an error response';
    this.stack = new Error().stack;
  }
}

export type JsonRpcResult<T> = {
  id: string;
  jsonrpc: '2.0';
  result?: T;
  error?: Error;
};

export default class JsonRpcClient {
  constructor (private url: string, private prefix?: string, private config?: AxiosRequestConfig) {}

  public async call<P, R> (method: string, params: P): Promise<JsonRpcResult<R>> {
    try {
      const response = await axios.post<JsonRpcResult<R>>(
        this.url,
        {
          id: randomUUID(),
          jsonrpc: '2.0',
          method: (this.prefix ?? '') + method,
          params: params
        },
        this.config
      );

      return response.data;
    } catch (error: any) {
      throw new JsonRpcError(error.message, error.response?.statusText ?? error.code);
    }
  }
}
