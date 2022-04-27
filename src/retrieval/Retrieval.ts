import * as IPFSCore from 'ipfs-core';
import { DirNode, FileNode, Source } from '../index/FsDag';
import { CID, IPFS } from 'ipfs-core';
import * as pth from 'path';
import { spawnSync } from 'child_process';
import fs from 'fs';
import rrdir from 'rrdir';

export interface FileStat {
  type: 'file' | 'dir',
  size: number,
  name: string
}
export default class Retrieval {
  private static async init (path: string): Promise<[CID, string, IPFS]> {
    if (!path.startsWith('singularity://')) {
      console.error('Unsupported protocol. The path needs to start with singularity://, i.e. singularity://ipns/index.dataset.io/path/to/folder');
      process.exit(1);
    }
    const splits = path.slice('singularity:/'.length).split('/');
    const ipns = splits.slice(0, 3).join('/');
    path = splits.slice(3).join('/');
    const ipfs = await IPFSCore.create(
      {
        silent: true
      }
    );
    const resolved = await ipfs.dag.resolve(ipns);
    const segments = [];
    for (const segment of path.split('/')) {
      if (segment !== '') {
        segments.push('entries', segment);
      }
    }
    const innerPath = segments.join('/');
    return [resolved.cid, innerPath, ipfs];
  }

  public static async show (path: string): Promise<Source[]> {
    const [resolved, innerPath, ipfs] = await Retrieval.init(path);
    const dagResult = await ipfs.dag.get(resolved, {
      path: innerPath + '/sources'
    });
    if (dagResult.remainderPath !== undefined && dagResult.remainderPath.length > 0) {
      console.error(`Remainder path cannot be resolved: ${dagResult.remainderPath}`);
      process.exit(1);
    }
    const index : {[key: string]: Source} = dagResult.value;
    return Object.values(index);
  }

  public static async list (path: string): Promise<FileStat[]> {
    const [resolved, innerPath, ipfs] = await Retrieval.init(path);
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

  public static async cp (path: string, dest: string, providers: string[]): Promise<void> {
    const [resolved, innerPath, ipfs] = await Retrieval.init(path);
    const dagResult = await ipfs.dag.get(resolved, {
      path: innerPath + '/sources'
    });
    if (dagResult.remainderPath !== undefined && dagResult.remainderPath.length > 0) {
      console.error(`Remainder path cannot be resolved: ${dagResult.remainderPath}`);
      process.exit(1);
    }
    const tempDir = pth.join(dest, '.fetching');
    const sources: Source[] = Object.values(dagResult.value);
    for (const source of sources) {
      let success = false;
      for (const provider of providers) {
        console.log(`Checking whether ${source.dataCid} can be retrieved from ${provider}`);
        const result = spawnSync('lotus', ['client', 'ls', '--maxPrice', '0', '--miner', provider, source.dataCid], { timeout: 10000 });
        if (result.signal || result.status !== 0) {
          console.error(result.stderr);
          continue;
        }
        console.log(`${provider} has the file ${source.dataCid}. Start retrieving...`);
        fs.rmSync(tempDir, { recursive: true, force: true });
        fs.mkdirSync(tempDir, { recursive: true });
        const selector = source.selector.map((n) => `Links/${n}/Hash`).join('/');
        const retrieveResult = spawnSync('lotus', ['client', 'retrieve', '--maxPrice', '0', '--miner', provider, '--data-selector', selector, source.dataCid, tempDir]);
        if (retrieveResult.signal || retrieveResult.status !== 0) {
          console.error(retrieveResult.stderr);
          continue;
        }
        console.log(`${provider} has the file ${source.dataCid}. Retrieval completed.`);
        success = true;
        break;
      }

      if (!success) {
        fs.rmSync(tempDir, { recursive: true, force: true });
        process.exit(1);
      }

      for (const entry of rrdir.sync(tempDir, { stats: true })) {
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
            console.log(`append ${entry.path} to ${target}`);
            const r = fs.createReadStream(entry.path);
            const w = fs.createWriteStream(target);
            r.pipe(w);
          }
        }
      }
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
    console.log('Succeeded');
  }
}
