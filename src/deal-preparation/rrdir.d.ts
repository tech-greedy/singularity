import * as fs from 'fs';
import { PicomatchOptions } from 'picomatch';

interface Options {
  stats?: boolean | undefined;
  followSymlinks?: boolean | undefined;
  exclude?: string[] | undefined;
  include?: string[] | undefined;
  strict?: boolean | undefined;
  match?: PicomatchOptions | undefined;
  startFrom?: string;
}

export interface Entry<T extends string | Buffer> {
  path: T;
  directory?: boolean | undefined;
  symlink?: boolean | undefined;
  stats?: fs.Stats | undefined;
  err?: Error | undefined;
  offset?: number;
}

export default interface rrdir {
  async(dir: string, options?: Options): Promise<Array<Entry<string>>>;
  async(dir: Buffer, options?: Options): Promise<Array<Entry<Buffer>>>;

  sync(dir: string, options?: Options): Array<Entry<string>>;
  sync(dir: Buffer, options?: Options): Array<Entry<Buffer>>;

  (dir: string, options?: Options): AsyncIterable<Entry<string>>;
  (dir: Buffer, options?: Options): AsyncIterable<Entry<Buffer>>;
}
