import bodyParser from 'body-parser';
import config from 'config';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import ErrorCode from './ErrorCode';
import Datastore from '../common/Datastore';
import { ObjectId } from 'mongodb';
import { create } from 'ipfs-client';
import { CID, IPFS } from 'ipfs-core';
import * as IpfsCore from 'ipfs-core';
import { DirNode, FileNode } from './FsDag';
import path from 'path';

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
    this.app.get('/create/:id', this.createIndexRequest);
    this.ipfsClient = create({
      http: config.get('index_service.ipfs_http')
    });
  }

  private async pinIndex (dir: DirNode): Promise<CID> {
    for (const [name, entry] of dir.entries.entries()) {
      const type = (<DirNode | FileNode>entry).type;
      if (type === 'dir') {
        dir.entries.set(name, await this.pinIndex(<DirNode>entry));
      } else if (type === 'file') {
        (<FileNode>entry).sourcesMap = null;
      }
    }
    dir.sourcesMap = null;
    return this.ipfsClient.dag.put(dir, {
      pin: true
    });
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
      sourcesMap: new Map(),
      sources: [],
      type: 'dir'
    };
    for await (const generation of Datastore.GenerationRequestModel.find({ datasetId: found.id, status: 'completed' }).sort({ index: 1 })) {
      const dataCid = generation.dataCid!;
      const pieceCid = generation.pieceCid!;
      for (const file of generation.fileList) {
        let node = root;
        const segments = path.relative(found.path, file.path).split(path.sep);
        // Enter directories
        for (let i = 0; i < segments.length - 1; ++i) {
          const source = {
            dataCid, pieceCid, selector: file.selector.slice(0, i)
          };
          if (!node.sourcesMap!.has(dataCid)) {
            node.sourcesMap!.set(dataCid, source);
            node.sources.push(source);
          }
          if (!node.entries.has(segments[i])) {
            const entry : DirNode = {
              entries: new Map(),
              sourcesMap: new Map(),
              sources: [],
              type: 'dir',
              name: segments[i]
            };
            node.entries.set(segments[i], entry);
          }
          node = <DirNode>node.entries.get(segments[i]);
        }
        // Handle file node
        const segment = segments[segments.length - 1];
        const source = {
          dataCid, pieceCid, selector: file.selector.slice(0, file.selector.length - 1)
        };
        if (!node.sourcesMap!.has(dataCid)) {
          node.sourcesMap!.set(dataCid, source);
          node.sources.push(source);
        }
        if (!node.entries.has(segment)) {
          node.entries.set(segment, {
            name: segment,
            size: file.size,
            sourcesMap: new Map(),
            sources: [],
            type: 'file'
          });
        }
        const fileSource = {
          dataCid, pieceCid, selector: file.selector, from: file.start, to: file.end
        };
        const sourceMap = (<FileNode>node.entries.get(segment)).sourcesMap!;
        if (!sourceMap.has(dataCid)) {
          sourceMap.set(dataCid, fileSource);
          (<FileNode>node.entries.get(segment)).sources.push(fileSource);
        }
      }
    }

    // PIN to IPFS
    const rootCid = await this.pinIndex(root);
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

  public async init (): Promise<void> {
    if (config.get('index_service.start_ipfs')) {
      const ipfs = await IpfsCore.create();
      this.ipfsClient = ipfs;
    }
  }

  private sendError (response: Response, error: ErrorCode) {
    this.logger.warn(`Error code`, { error });
    response.status(400);
    response.end(JSON.stringify({ error }));
  }
}
