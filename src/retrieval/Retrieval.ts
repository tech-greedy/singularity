import { create } from 'ipfs-client';
import { DirNode, FileNode, Source } from '../index/FsDag';
import { CID, IPFS } from 'ipfs-core';
import * as pth from 'path';
import { spawnSync } from 'child_process';
import fs, { ReadStream, WriteStream } from 'fs';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import { rrdirSync } from '../deal-preparation/rrdir';

export interface FileStat {
  type: 'file' | 'dir',
  size: number,
  name: string
}
export default class Retrieval {
  private static getClient (api: string): IPFS {
    return create({
      http: api
    });
  }

  private static async resolve (ipfs: IPFS, path: string): Promise<[CID, string]> {
    if (!path.startsWith('singularity://')) {
      console.error('Unsupported protocol. The path needs to start with singularity://, i.e. singularity://ipns/index.dataset.io/path/to/folder');
      process.exit(1);
    }
    const splits = path.slice('singularity:/'.length).split('/');
    const ipns = splits.slice(0, 3).join('/');
    path = splits.slice(3).join('/');
    const resolved = await ipfs.dag.resolve(ipns);
    const segments = [];
    for (const segment of path.split('/')) {
      if (segment !== '') {
        segments.push('entries', segment);
      }
    }
    const innerPath = segments.join('/');
    return [resolved.cid, innerPath];
  }

  public static async show (api: string, path: string): Promise<Source[] | { cid: string }[]> {
    const ipfs = Retrieval.getClient(api);
    const [resolved, innerPath] = await Retrieval.resolve(ipfs, path);
    let dagResult;
    try {
      dagResult = await ipfs.dag.get(resolved, {
        path: innerPath
      });
    } catch (error: any) {
      console.error(error.message);
      process.exit(1);
    }
    if (dagResult.remainderPath !== undefined && dagResult.remainderPath.length > 0) {
      console.error(`Remainder path cannot be resolved: ${dagResult.remainderPath}`);
      process.exit(1);
    }
    const node: DirNode | FileNode = dagResult.value;
    if (node.type === 'dir') {
      return node.sources.map(s => ({ cid: s }));
    } else {
      return node.sources;
    }
  }

  public static async list (api: string, path: string): Promise<FileStat[]> {
    const ipfs = Retrieval.getClient(api);
    const [resolved, innerPath] = await Retrieval.resolve(ipfs, path);
    const dagResult = await ipfs.dag.get(resolved, {
      path: innerPath
    });
    if (dagResult.remainderPath !== undefined && dagResult.remainderPath.length > 0) {
      console.error(`Remainder path cannot be resolved: ${dagResult.remainderPath}`);
      process.exit(1);
    }
    const index : FileNode | DirNode = dagResult.value;
    if (index.type === 'file') {
      return [{
        type: index.type,
        size: index.size,
        name: index.name
      }];
    } else {
      const result: FileStat[] = [];
      const entries: any = index.entries;
      for (const name in entries) {
        const entry: FileNode | CID = entries[name];
        if (entry instanceof CID) {
          result.push({
            name, type: 'dir', size: 0
          });
        } else {
          result.push({
            name, type: 'file', size: entry.size
          });
        }
      }
      return result;
    }
  }

  public static async cp (api: string, path: string, dest: string, providers: string[]): Promise<void> {
    const ipfs = Retrieval.getClient(api);
    const [resolved, innerPath] = await Retrieval.resolve(ipfs, path);
    const dagResult = await ipfs.dag.get(resolved, {
      path: innerPath
    });
    if (dagResult.remainderPath !== undefined && dagResult.remainderPath.length > 0) {
      console.error(`Remainder path cannot be resolved: ${dagResult.remainderPath}`);
      process.exit(1);
    }
    const node : FileNode | DirNode = dagResult.value;
    const tempDir = pth.resolve(dest, '.fetching');
    const tempPath = pth.resolve(dest, '.fetching', node.name);
    const result: FileNode | DirNode = dagResult.value;
    const sources: string[] = result.type === 'dir' ? result.sources : result.sources.map(s => s.cid);
    for (const source of sources) {
      let success = false;
      for (const provider of providers) {
        console.log(`Checking whether ${source} can be retrieved from ${provider}`);
        const result = spawnSync('lotus', ['client', 'ls', '--maxPrice', '0', '--miner', provider, source], { timeout: 10000 });
        if (result.signal || result.status !== 0) {
          console.error(result.stderr.toString());
          continue;
        }
        console.log(`${provider} has the piece ${source}. Start retrieving...`);
        fs.rmSync(tempDir, { recursive: true, force: true });
        fs.mkdirSync(tempDir, { recursive: true });
        const retrieveResult = spawnSync('lotus', ['client', 'retrieve', '--maxPrice', '0', '--miner', provider, source, tempPath], { stdio: 'inherit' });
        if (retrieveResult.signal || retrieveResult.status !== 0) {
          console.error(retrieveResult.stderr.toString());
          continue;
        }
        console.log(`Retrieved ${source} from ${provider} to ${tempPath}.`);
        success = true;
        break;
      }

      if (!success) {
        console.error('Cleaning up temporary files');
        fs.rmSync(tempDir, { recursive: true, force: true });
        process.exit(1);
      }

      for (const entry of rrdirSync(tempDir, { stats: true })) {
        const relative = pth.relative(tempDir, entry.path);
        const target = pth.join(dest, relative);
        if (entry.directory) {
          console.log(`mkdir ${target}`);
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
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
    console.log('Succeeded');
  }

  private static async pipe (r: ReadStream, w: WriteStream) : Promise<void> {
    r.pipe(w);
    return new Promise(function (resolve, reject) {
      r.on('end', resolve);
      r.on('error', reject);
    });
  }
}
