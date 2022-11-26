import bodyParser from 'body-parser';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import ErrorCode from './ErrorCode';
import Datastore from '../common/Datastore';
import { ObjectId } from 'mongodb';
import { create } from 'ipfs-client';
import { CID, IPFS } from 'ipfs-core';
import { DirNode, LayeredArray, DynamizeArray, DynamicArray, FileNode, DynamicMap, DynamizeMap } from './FsDag';
import path from 'path';
import config from '../common/Config';

export default class IndexService extends BaseService {
  private app: Express = express();
  public ipfsClient! : IPFS;
  private static readonly maxLink = 1000;
  private static readonly pinThreshold = 10000;

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
    this.app.get('/create/:id', this.createIndexRequest);
    this.ipfsClient = create({
      http: config.get('index_service.ipfs_http')
    });
  }

  private dynamizeFileNode (file: FileNode): void {
    file.realSources = DynamizeArray(file.sources!, IndexService.maxLink);
    delete file.sources;
  }

  private dynamizeDirNode (dir: DirNode) : void {
    dir.realSources = DynamizeArray(dir.sources!, IndexService.maxLink);
    delete dir.sources;

    for (const [_, value] of dir.entries!) {
      if ((<FileNode | DirNode> value).type === 'file') {
        this.dynamizeFileNode(<FileNode>value);
      } else {
        this.dynamizeDirNode(<DirNode>value);
      }
    }
    dir.realEntries = DynamizeMap(dir.entries!, IndexService.maxLink);
    delete dir.entries;
  }

  private async pinDynamicArray<T> (array: DynamicArray<T>, numOfProperties: number): Promise<[node: CID | DynamicArray<T>, count: number]> {
    if (array.length === 0 || !Object.prototype.hasOwnProperty.call(array[0], 'index')) {
      const count = array.length * numOfProperties + 1;
      if (count > IndexService.pinThreshold) {
        const result = await this.ipfsClient.dag.put(array, { pin: true });
        this.logger.info(`Pinned flat array with CID ${result}`, { size: array.length });
        return [result, 1];
      }
      return [array, array.length * numOfProperties + 1];
    }

    const layeredArray = <LayeredArray<T>[]>array;
    let total = 1;
    for (let i = 0; i < layeredArray.length; i++) {
      const [subNode, count] = await this.pinDynamicArray(<DynamicArray<T>>layeredArray[i].array, numOfProperties);
      total += count;
      layeredArray[i].array = subNode;
    }

    if (total > IndexService.pinThreshold) {
      const result = await this.ipfsClient.dag.put(layeredArray, { pin: true });
      this.logger.info(`Pinned layered array with CID ${result}`, { size: layeredArray.length });
      return [result, 1];
    }

    return [layeredArray, total];
  }

  private async pinEntries (entries: DynamicMap<FileNode | DirNode | CID>)
    : Promise<[node: DynamicMap<FileNode | DirNode | CID> | CID, count: number]> {
    if (entries instanceof Map) {
      let total = 1;
      for (const [key, value] of entries) {
        if ((<FileNode | DirNode> value).type === 'file') {
          const [node, count] = await this.pinFileNode(<FileNode>value);
          total += count;
          entries.set(key, node);
        } else {
          const [node, count] = await this.pinDirNode(<DirNode>value);
          total += count;
          entries.set(key, node);
        }
      }

      if (total > IndexService.pinThreshold) {
        const result = await this.ipfsClient.dag.put(entries, { pin: true });
        this.logger.info(`Pinned flat entries map with CID ${result}`, { size: entries.size });
        return [result, 1];
      }

      return [entries, total];
    }

    const layeredMap = entries;
    let total = 1;
    for (let i = 0; i < layeredMap.length; i++) {
      const [node, count] = await this.pinEntries(<DynamicMap<FileNode | DirNode | CID>>layeredMap[i].map);
      total += count;
      layeredMap[i].map = node;
    }

    if (total > IndexService.pinThreshold) {
      const result = await this.ipfsClient.dag.put(layeredMap, { pin: true });
      this.logger.info(`Pinned layered entries map with CID ${result}`, {
        size: layeredMap.length,
        from: layeredMap[0].from,
        to: layeredMap[layeredMap.length - 1].to});

      return [result, 1];
    }

    return [layeredMap, total];
  }

  private async pinDirNode (dir: DirNode): Promise<[cid: CID, count: number]> {
    const realSources = DynamizeArray(dir.sources!, IndexService.maxLink);
    dir.realSources = await this.pinDynamicArray(realSources);
    const realEntries = DynamizeMap(dir.entries!, IndexService.maxLink);
    dir.realEntries = await this.pinEntries(realEntries);
    const result = await this.ipfsClient.dag.put(dir, {
      pin: true
    });
    this.logger.info(`Pinned dir ${dir.name} with CID ${result}`);
    return result;
  }

  private async pinFileNode (file: FileNode): Promise<[node: CID | FileNode, count: number]> {
    file.realSources = DynamizeArray(file.sources!, IndexService.maxLink);
    delete file.sources;
    const [node, count] = await this.pinDynamicArray(file.realSources);
    this.logger.info(`Pinned file sources array ${file.name} with CID ${file.realSources}`);
    const result = await this.ipfsClient.dag.put(file, {
      pin: true
    });
    this.logger.info(`Pinned file ${file.name} with CID ${result}`);
    return result;
  }

  private async createIndexRequest (request: Request, response: Response): Promise<void> {
    const id = request.params['id'];
    this.logger.info(`Creating index for dataset`, { id });
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
    this.dynamizeDirNode(root);
    const rootCid = await this.pinDirNode(root);
    const result: any = {
      rootCid: rootCid.toString()
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
