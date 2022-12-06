import bodyParser from 'body-parser';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import ErrorCode from './ErrorCode';
import Datastore from '../common/Datastore';
import { ObjectId } from 'mongodb';
import { create } from 'ipfs-client';
import { CID, IPFS } from 'ipfs-core';
import { DirNode, LayeredArray, DynamizeArray, DynamicArray, FileNode, DynamicMap, DynamizeMap, Source } from './FsDag';
import path from 'path';
import config from '../common/Config';

export default class IndexService extends BaseService {
  private app: Express = express();
  public ipfsClient! : IPFS;

  public constructor () {
    super(Category.IndexService);
    this.createIndexRequest = this.createIndexRequest.bind(this);
    if (!this.enabled) {
      this.logger.warn('Service is not enabled. Exit now...');
      return;
    }
    this.app.use(Logger.getExpressLogger(Category.IndexService));
    this.app.use(bodyParser.urlencoded({ extended: false }));
    this.app.use(bodyParser.json());
    this.app.use(function (_req, res, next) {
      res.setHeader('Content-Type', 'application/json');
      next();
    });
    this.app.post('/create/:id', this.createIndexRequest);
    this.ipfsClient = create({
      http: config.get('index_service.ipfs_http')
    });
  }

  private dynamizeFileNode (file: FileNode, maxLinks: number): void {
    file.realSources = DynamizeArray(file.sources!, maxLinks);
    delete file.sources;
  }

  private dynamizeDirNode (dir: DirNode, maxLinks: number) : void {
    dir.realSources = DynamizeArray(dir.sources!, maxLinks);
    delete dir.sources;

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    for (const [_, value] of dir.entries!) {
      if ((<FileNode | DirNode> value).type === 'file') {
        this.dynamizeFileNode(<FileNode>value, maxLinks);
      } else {
        this.dynamizeDirNode(<DirNode>value, maxLinks);
      }
    }
    dir.realEntries = DynamizeMap(dir.entries!, maxLinks);
    delete dir.entries;
  }

  private async pinDynamicArray<T> (array: DynamicArray<T>, numOfProperties: number, maxNodes: number): Promise<[node: CID | DynamicArray<T>, count: number]> {
    if (array.length === 0 || !Object.prototype.hasOwnProperty.call(array[0], 'index')) {
      const count = array.length * numOfProperties + 1;
      if (count > maxNodes) {
        const result = await this.ipfsClient.dag.put(array, { pin: true });
        this.logger.info(`Pinned flat array with CID ${result}`, { size: array.length });
        return [result, 1];
      }
      return [array, array.length * numOfProperties + 1];
    }

    const layeredArray = <LayeredArray<T>[]>array;
    let total = 1;
    for (let i = 0; i < layeredArray.length; i++) {
      const [subNode, count] = await this.pinDynamicArray(<DynamicArray<T>>layeredArray[i].array, numOfProperties, maxNodes);
      total += count;
      layeredArray[i].array = subNode;
    }

    if (total > maxNodes) {
      const result = await this.ipfsClient.dag.put(layeredArray, { pin: true });
      this.logger.info(`Pinned layered array with CID ${result}`, { size: layeredArray.length });
      return [result, 1];
    }

    return [layeredArray, total];
  }

  private async pinEntries (entries: DynamicMap<FileNode | DirNode | CID>, maxNodes: number)
    : Promise<[node: DynamicMap<FileNode | DirNode | CID> | CID, count: number]> {
    if (!Array.isArray(entries)) {
      let total = 1;
      for (const key in entries) {
        const value = entries[key];
        if ((<FileNode | DirNode> value).type === 'file') {
          const [node, count] = await this.pinFileNode(<FileNode>value, maxNodes);
          total += count + 1;
          entries[key] = node;
        } else {
          const [node, count] = await this.pinDirNode(<DirNode>value, maxNodes);
          total += count + 1;
          entries[key] = node;
        }
      }

      if (total > maxNodes) {
        const result = await this.ipfsClient.dag.put(entries, { pin: true });
        this.logger.info(`Pinned flat entries map with CID ${result}`, { size: entries.size });
        return [result, 1];
      }

      return [entries, total];
    }

    const layeredMap = entries;
    let total = 5;
    for (let i = 0; i < layeredMap.length; i++) {
      const [node, count] = await this.pinEntries(<DynamicMap<FileNode | DirNode | CID>>layeredMap[i].map, maxNodes);
      total += count + 1;
      layeredMap[i].map = node;
    }

    if (total > maxNodes) {
      const result = await this.ipfsClient.dag.put(layeredMap, { pin: true });
      this.logger.info(`Pinned layered entries map with CID ${result}`, {
        size: layeredMap.length,
        from: layeredMap[0].from,
        to: layeredMap[layeredMap.length - 1].to
      });

      return [result, 1];
    }

    return [layeredMap, total];
  }

  private async pinDirNode (dir: DirNode, maxNodes: number, forcePin = false): Promise<[cid: DirNode | CID, count: number]> {
    const [node1, count1] = await this.pinDynamicArray(<DynamicArray<string>>dir.realSources, 1, maxNodes);
    dir.realSources = node1;
    const [node2, count2] = await this.pinEntries(<DynamicMap<FileNode | DirNode | CID>>dir.realEntries, maxNodes);
    dir.realEntries = node2;
    if (forcePin || count1 + count2 + 7 > maxNodes) {
      const result = await this.ipfsClient.dag.put(dir, { pin: true });
      this.logger.info(`Pinned dir node with CID ${result}`);
      return [result, 1];
    }

    return [dir, count1 + count2 + 7];
  }

  private async pinFileNode (file: FileNode, maxNodes: number): Promise<[node: FileNode | CID, count: number]> {
    const [node, count] = await this.pinDynamicArray(<DynamicArray<Source>>file.realSources!, 3, maxNodes);
    file.realSources = node;
    if (count + 8 > maxNodes) {
      const result = await this.ipfsClient.dag.put(file, { pin: true });
      this.logger.info(`Pinned file node with CID ${result}`);
      return [result, 1];
    }
    return [file, count + 8];
  }

  private async createIndexRequest (request: Request, response: Response): Promise<void> {
    const id = request.params['id'];
    const maxLinks = request.body.maxLinks ?? 1000;
    const maxNodes = request.body.maxNodes ?? 100;
    this.logger.info(`Creating index for dataset`, { id, maxLinks, maxNodes });
    const found = ObjectId.isValid(id) ? await Datastore.ScanningRequestModel.findById(id) : await Datastore.ScanningRequestModel.findOne({ name: id });
    if (!found) {
      this.sendError(response, ErrorCode.DATASET_NOT_FOUND);
      return;
    }
    if (found.status !== 'completed') {
      this.sendError(response, ErrorCode.SCANNING_INCOMPLETE);
      return;
    }
    const unfinishedGenerations = await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: { $ne: 'completed' } });
    const root : DirNode = {
      entries: new Map(),
      name: '',
      sources: [],
      type: 'dir'
    };
    for await (const generation of Datastore.GenerationRequestModel.find({ datasetId: found.id, status: 'completed' }).sort({ index: 1 })) {
      this.logger.info(`Reading generation metadata for index generation`, { dataset: generation.datasetName, index: generation.index });
      const generatedFileList = (await Datastore.OutputFileListModel.find({ generationId: generation.id }, null, { sort: { index: 1 } }))
        .map(r => r.generatedFileList).flat();
      for (const file of generatedFileList) {
        if (file.path === '') {
          root.sources!.push(generation.dataCid!);
          continue;
        }
        let node = root;
        const segments = file.path.split(path.sep);
        // Enter directories
        for (let i = 0; i < segments.length - 1; ++i) {
          if (!node.entries!.has(segments[i])) {
            this.logger.error(`Unexpected empty subnode ${segments[i]}. The file list may not have correct order or there may be missing file or folder.`,
              { path: file.path });
            node.entries!.set(segments[i], {
              sources: [],
              name: segments[i],
              entries: new Map(),
              type: 'dir'
            });
          }
          node = <DirNode>node.entries!.get(segments[i]);
        }
        const filename = segments[segments.length - 1];
        if (file.dir) {
          if (!node.entries!.has(filename)) {
            node.entries!.set(filename, {
              sources: [],
              name: filename,
              entries: new Map(),
              type: 'dir'
            });
          }
          (<DirNode>node.entries!.get(filename)).sources!.push(file.cid);
        } else {
          if (!node.entries!.has(filename)) {
            node.entries!.set(filename, {
              sources: [],
              name: filename,
              size: file.size!,
              type: 'file'
            });
          }
          (<FileNode>node.entries!.get(filename)).sources!.push({
            cid: file.cid,
            from: file.start ?? 0,
            to: file.end ?? file.size!
          });
        }
      }
    }

    // PIN to IPFS
    this.dynamizeDirNode(root, maxLinks);
    const rootCid = await this.pinDirNode(root, maxNodes, true);
    const result: any = {
      rootCid: rootCid[0].toString()
    };
    if (unfinishedGenerations > 0) {
      result.warning = `There are still ${unfinishedGenerations} incomplete generation requests`;
    }
    response.end(JSON.stringify(result));
  }

  public start (): void {
    const bind = config.get<string>('index_service.bind');
    const port = config.get<number>('index_service.port');
    this.app!.listen(port, bind, () => {
      this.logger.info(`Index Service started listening at http://${bind}:${port}`);
    });
  }

  private sendError (response: Response, error: ErrorCode) {
    this.logger.warn(`Error code`, { error });
    response.status(400);
    response.end(JSON.stringify({ error }));
  }
}
