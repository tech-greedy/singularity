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
      grpc: config.get('index_service.ipfs_grpc'),
      http: config.get('index_service.ipfs_http')
    });
  }

  private async pinIndex (dir: DirNode): Promise<CID> {
    for (const [name, entry] of dir.entries.entries()) {
      if ((<DirNode | FileNode>entry).type === 'dir') {
        dir.entries.set(name, await this.pinIndex(<DirNode>entry));
      }
    }
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
    const unfinishedGenerations = await Datastore.GenerationRequestModel.count({ datasetId: id, status: { $ne: 'completed' } });
    const root : DirNode = {
      entries: new Map(),
      name: '',
      sources: new Map(),
      type: 'dir'
    };
    for await (const generation of Datastore.GenerationRequestModel.find({ datasetId: id, status: 'completed' }).sort({ index: 1 })) {
      const dataCid = generation.dataCid!;
      const pieceCid = generation.pieceCid!;
      for (const file of generation.fileList) {
        let node = root;
        const segments = path.relative(found.path, file.path).split(path.sep);
        // Enter directories
        for (let i = 0; i < segments.length - 1; ++i) {
          node.sources.set(dataCid, {
            dataCid, pieceCid, selector: file.selector.slice(0, i)
          });
          if (!node.entries.has(segments[i])) {
            node.entries.set(segments[i], {
              entries: new Map(),
              name: segments[i],
              sources: new Map(),
              type: 'dir'
            });
          }
          node = <DirNode>node.entries.get(segments[i]);
        }
        // Handle file node
        const segment = segments[segments.length - 1];
        node.sources.set(dataCid, {
          dataCid, pieceCid, selector: file.selector.slice(0, file.selector.length - 1)
        });
        if (!node.entries.has(segment)) {
          node.entries.set(segment, {
            name: segment,
            size: file.size,
            sources: new Map(),
            type: 'file'
          });
        }
        (<FileNode>node.entries.get(segment)).sources.set(dataCid, {
          dataCid, pieceCid, selector: file.selector, from: file.start, to: file.end
        });
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
