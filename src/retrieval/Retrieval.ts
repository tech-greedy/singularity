import { create } from 'ipfs-client';
import { DirNode, DynamicArray, DynamicMap, FileNode, LayeredArray, Source } from '../index/FsDag';
import { CID, IPFS } from 'ipfs-core';
import * as pth from 'path';
import { spawnSync } from 'child_process';
import fs, { ReadStream, WriteStream } from 'fs';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { rrdirSync } from '../deal-preparation/scanner/rrdir';

export interface FileStat {
  type: 'file' | 'dir',
  size?: number,
  name: string
}

export default class Retrieval {
  private ipfs: IPFS;

  public constructor (api: string) {
    this.ipfs = create({
      http: api
    });
  }

  private async resolveRootPath (path: string): Promise<[DirNode, string[]]> {
    if (!path.startsWith('singularity://')) {
      console.error('Unsupported protocol. The path needs to start with singularity://, i.e. singularity://ipns/index.dataset.io/path/to/folder');
      process.exit(1);
    }
    const splits = path.slice('singularity:/'.length).split('/');
    const ipns = splits.slice(0, 3).join('/');
    path = splits.slice(3).join('/');
    const resolved = await this.ipfs.dag.resolve(ipns);
    const segments = [];
    for (const segment of path.split('/')) {
      if (segment !== '') {
        segments.push(segment);
      }
    }
    const rootNode: DirNode = (await this.ipfs.dag.get(resolved.cid)).value;
    return [rootNode, segments];
  }

  private async resolveDynamicMap<T> (map: DynamicMap<T> | CID): Promise<Record<string, T>> {
    let resolvedMap: DynamicMap<T>;
    if (map instanceof CID) {
      resolvedMap = (await this.ipfs.dag.get(map)).value;
    } else {
      resolvedMap = map;
    }

    if (!Array.isArray(resolvedMap)) {
      return resolvedMap;
    }

    const result: Record<string, T> = {};
    for (const layer of resolvedMap) {
      const subMap = await this.resolveDynamicMap(layer.map);
      for (const key in subMap) {
        result[key] = subMap[key];
      }
    }
    return result;
  }

  private async resolveDynamicMapKey<T> (map: DynamicMap<T> | CID, key: string): Promise<T | undefined> {
    let resolvedMap: DynamicMap<T>;
    if (map instanceof CID) {
      resolvedMap = (await this.ipfs.dag.get(map)).value;
    } else {
      resolvedMap = map;
    }

    if (!Array.isArray(resolvedMap)) {
      return resolvedMap[key];
    }

    for (const layer of resolvedMap) {
      if (layer.from > key) {
        return undefined;
      }
      if (layer.from <= key && key <= layer.to) {
        return this.resolveDynamicMapKey(layer.map, key);
      }
    }

    return undefined;
  }

  private async resolveDynamicArray<T> (array: DynamicArray<T> | CID): Promise<T[]> {
    let resolvedArray: DynamicArray<T>;
    if (array instanceof CID) {
      resolvedArray = (await this.ipfs.dag.get(array)).value;
    } else {
      resolvedArray = array;
    }

    if (resolvedArray.length > 0 &&
      !Object.prototype.hasOwnProperty.call(resolvedArray[0], 'index')) {
      return <T[]>resolvedArray;
    }

    const layeredArray = <LayeredArray<T>[]>resolvedArray;
    const result = [];
    for (const layer of layeredArray) {
      const subArray = await this.resolveDynamicArray(layer.array);
      result.push(...subArray);
    }
    return result;
  }

  public async explain (path: string): Promise<[Source[] | { cid: string }[], FileStat]> {
    let [dir, segments] = await this.resolveRootPath(path);
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      let entry = await this.resolveDynamicMapKey(dir.realEntries!, segment);
      if (entry === undefined) {
        console.error(`Path ${path} cannot be resolved - ${segment} cannot be found`);
        process.exit(1);
      }
      if (entry instanceof CID) {
        entry = (await this.ipfs.dag.get(entry)).value;
      }
      entry = <FileNode | DirNode>entry;
      if (entry.type === 'file') {
        if (i !== segments.length - 1) {
          console.error(`Path ${path} cannot be resolved - ${segment} is a file`);
          process.exit(1);
        }
        return [await this.resolveDynamicArray(entry.realSources!), {
          type: 'file',
          size: entry.size,
          name: segment
        }];
      } else {
        dir = entry;
      }
    }

    const sources = await this.resolveDynamicArray(dir.realSources!);
    return [sources.map(source => ({ cid: source })), {
      type: 'dir',
      name: pth.basename(path)
    }];
  }

  public async list (path: string, verbose: boolean): Promise<string[] | FileStat[]> {
    let [dir, segments] = await this.resolveRootPath(path);
    for (let i = 0; i < segments.length; i++) {
      const segment = segments[i];
      let entry = await this.resolveDynamicMapKey(dir.realEntries!, segment);
      if (entry === undefined) {
        console.error(`Path ${path} cannot be resolved - ${segment} cannot be found`);
        process.exit(1);
      }
      if (entry instanceof CID) {
        entry = (await this.ipfs.dag.get(entry)).value;
      }
      entry = <FileNode | DirNode>entry;
      if (entry.type === 'file') {
        if (i !== segments.length - 1) {
          console.error(`Path ${path} cannot be resolved - ${segment} is a file`);
          process.exit(1);
        }
        if (!verbose) {
          return [segment];
        }
        return [{
          type: 'file',
          size: entry.size,
          name: segment
        }];
      } else {
        dir = entry;
      }
    }

    const dirEntries = await this.resolveDynamicMap(dir.realEntries!);
    if (!verbose) {
      return Array.from(Object.keys(dirEntries));
    }

    const result: FileStat[] = [];
    for (const [name, entry] of Object.entries(dirEntries)) {
      let resolved = entry;
      if (entry instanceof CID) {
        resolved = (await this.ipfs.dag.get(entry)).value;
      }
      resolved = <FileNode | DirNode>resolved;
      if (resolved.type === 'file') {
        result.push({
          type: 'file',
          size: resolved.size,
          name
        });
      } else {
        result.push({
          type: 'dir',
          name
        });
      }
    }
    return result;
  }

  public async cp (path: string, dest: string, providers: string[]): Promise<void> {
    const [sources, stat] = await this.explain(path);
    const cids = sources.map(source => source.cid);
    const name = stat.name;
    const tempDir = pth.resolve(dest, '.fetching');
    const tempPath = pth.resolve(dest, '.fetching', name);
    for (const cid of cids) {
      let success = false;
      for (const provider of providers) {
        console.log(`Checking whether ${cid} can be retrieved from ${provider}`);
        let command = ['lotus', 'client', 'ls', '--maxPrice', '0', '--miner', provider, cid].join(' ');
        console.log(command);
        const result = spawnSync('lotus', ['client', 'ls', '--maxPrice', '0', '--miner', provider, cid], { timeout: 10000 });
        if (result.signal || result.status !== 0) {
          console.error(result.stderr.toString());
          continue;
        }
        console.log(`${provider} has the piece ${cid}. Start retrieving...`);
        fs.rmSync(tempDir, {
          recursive: true,
          force: true
        });
        fs.mkdirSync(tempDir, { recursive: true });
        command = ['lotus', 'client', 'retrieve', '--maxPrice', '0', '--miner', provider, cid, tempPath].join(' ');
        console.log(command);
        const retrieveResult = spawnSync('lotus', ['client', 'retrieve', '--maxPrice', '0', '--miner', provider, cid, tempPath], { stdio: 'inherit' });
        if (retrieveResult.signal || (retrieveResult.status !== null && retrieveResult.status !== 0)) {
          console.error(retrieveResult.stderr.toString());
          continue;
        }
        console.log(`Retrieved ${cid} from ${provider} to ${tempPath}.`);
        success = true;
        break;
      }

      if (!success) {
        console.error(`Failed to retrieve ${cid} from any provider`);
        console.error('Cleaning up temporary files');
        fs.rmSync(tempDir, {
          recursive: true,
          force: true
        });
        process.exit(1);
      }

      for (const entry of rrdirSync(tempDir, { stats: true })) {
        const relative = pth.relative(tempDir, entry.path);
        const target = pth.join(dest, relative);
        if (entry.directory) {
          console.log(`mkdir -p ${target}`);
          fs.mkdirSync(target, { recursive: true });
        } else {
          if (!fs.existsSync(target)) {
            console.log(`mv ${entry.path} to ${target}`);
            fs.renameSync(entry.path, target);
          } else {
            console.log(`Appending ${entry.path} to ${target}`);
            const r = fs.createReadStream(entry.path);
            const w = fs.createWriteStream(target, { flags: 'a' });
            await Retrieval.pipe(r, w);
          }
        }
      }
      fs.rmSync(tempDir, {
        recursive: true,
        force: true
      });
    }
    console.log('Succeeded');
  }

  private static async pipe (r: ReadStream, w: WriteStream): Promise<void> {
    r.pipe(w);
    return new Promise(function (resolve, reject) {
      r.on('end', resolve);
      r.on('error', reject);
    });
  }
}
