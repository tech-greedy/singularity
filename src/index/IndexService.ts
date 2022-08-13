import bodyParser from 'body-parser';
import express, { Express, Request, Response } from 'express';
import BaseService from '../common/BaseService';
import Logger, { Category } from '../common/Logger';
import ErrorCode from './ErrorCode';
import Datastore from '../common/Datastore';
import { ObjectId } from 'mongodb';
import { create } from 'ipfs-client';
import { CID, IPFS } from 'ipfs-core';
import { DirNode, FileNode } from './FsDag';
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
    this.app.get('/create/:id', this.createIndexRequest);
    this.ipfsClient = create({
      http: config.index_service?.ipfs_http ?? 'http://localhost:5001'
    });
  }

  private async pinIndex (dir: DirNode): Promise<CID> {
    for (const [name, entry] of dir.entries.entries()) {
      const type = (<DirNode | FileNode>entry).type;
      if (type === 'dir') {
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
    const unfinishedGenerations = await Datastore.GenerationRequestModel.count({ datasetId: found.id, status: { $ne: 'completed' } });
    const root : DirNode = {
      entries: new Map(),
      name: '',
      sources: [],
      type: 'dir'
    };
    for await (const generation of Datastore.GenerationRequestModel.find({ datasetId: found.id, status: 'completed' }).sort({ index: 1 })) {
      const generatedFileList = (await Datastore.OutputFileListModel.find({ generationId: generation.id }, null, { sort: { index: 1 } }))
        .map(r => r.generatedFileList).flat();
      for (const file of generatedFileList) {
        if (file.path === '') {
          root.sources.push(generation.dataCid!);
          continue;
        }
        let node = root;
        const segments = file.path.split(path.sep);
        // Enter directories
        for (let i = 0; i < segments.length - 1; ++i) {
          if (!node.entries.has(segments[i])) {
            this.logger.error(`Unexpected empty subnode ${segments[i]}. The file list may not have correct order or there may be missing file or folder.`,
              { path: file.path });
            node.entries.set(segments[i], {
              sources: [],
              name: segments[i],
              entries: new Map(),
              type: 'dir'
            });
          }
          node = <DirNode>node.entries.get(segments[i]);
        }
        const filename = segments[segments.length - 1];
        if (file.dir) {
          if (!node.entries.has(filename)) {
            node.entries.set(filename, {
              sources: [],
              name: filename,
              entries: new Map(),
              type: 'dir'
            });
          }
          (<DirNode>node.entries.get(filename)).sources.push(file.cid);
        } else {
          if (!node.entries.has(filename)) {
            node.entries.set(filename, {
              sources: [],
              name: filename,
              size: file.size!,
              type: 'file'
            });
          }
          (<FileNode>node.entries.get(filename)).sources.push({
            cid: file.cid,
            from: file.start ?? 0,
            to: file.end ?? file.size!
          });
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
    const bind = config.index_service?.bind ?? '0.0.0.0';
    const port = config.index_service?.port ?? 7003;
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
