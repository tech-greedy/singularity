import fs from 'fs';
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose, { Schema } from 'mongoose';
import Logger, { Category } from './Logger';
import DealState from './model/DealState';
import DealTrackingState from './model/DealTrackingState';
import GenerationRequest from './model/GenerationRequest';
import ProviderMetric from './model/ProviderMetric';
import ReplicationRequest from './model/ReplicationRequest';
import ScanningRequest from './model/ScanningRequest';
import path from 'path';
import InputFileList, { FileInfo } from './model/InputFileList';
import OutputFileList, { GeneratedFileInfo } from './model/OutputFileList';
import config, { getConfigDir } from './Config';
import HealthCheck from './model/HealthCheck';

export default class Datastore {
  private static logger = Logger.getLogger(Category.Database);
  protected static mongoMemoryServer : MongoMemoryServer;

  // eslint-disable-next-line @typescript-eslint/ban-types
  public static HealthCheckModel: mongoose.Model<HealthCheck, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static ScanningRequestModel: mongoose.Model<ScanningRequest, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static GenerationRequestModel: mongoose.Model<GenerationRequest, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static DealStateModel: mongoose.Model<DealState, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static ReplicationRequestModel: mongoose.Model<ReplicationRequest, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static ProviderMetricModel: mongoose.Model<ProviderMetric, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static DealTrackingStateModel: mongoose.Model<DealTrackingState, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static InputFileListModel: mongoose.Model<InputFileList, {}, {}, {}>;
  // eslint-disable-next-line @typescript-eslint/ban-types
  public static OutputFileListModel: mongoose.Model<OutputFileList, {}, {}, {}>;

  private static DB_NAME = 'singularity';

  private static async setupLocalMongoDb (ip: string, port: number, path?: string): Promise<void> {
    if (path) {
      fs.mkdirSync(path, {
        recursive: true
      });
    }
    Datastore.mongoMemoryServer = await MongoMemoryServer.create({
      instance: {
        port,
        ip,
        dbPath: path,
        storageEngine: path ? 'wiredTiger' : 'ephemeralForTest',
        auth: false
      }
    });
  }

  private static async connectMongoDb (connectionString: string): Promise<void> {
    await mongoose.connect(connectionString, { dbName: Datastore.DB_NAME });
  }

  private static setupDataModels (): void {
    this.logger.info('Setting up database models.');
    this.setupHealthCheckSchema();
    this.setupScanningRequestSchema();
    this.setupGenerationRequestSchema();
    this.setupDealStateSchema();
    this.setupReplicationRequestSchema();
    this.setupProviderMetricSchema();
    this.setupDealTrackingStateSchema();
    this.setupInputFileListSchema();
    this.setupOutputFileListSchema();
  }

  private static setupOutputFileListSchema () {
    const generatedFileInfoSchema = new Schema<GeneratedFileInfo>({
      path: Schema.Types.String,
      size: Schema.Types.Number,
      start: Schema.Types.Number,
      end: Schema.Types.Number,
      dir: Schema.Types.Boolean,
      cid: Schema.Types.String
    });
    const schema = new Schema<OutputFileList>({
      generationId: {
        type: Schema.Types.String,
        index: 1
      },
      index: Schema.Types.Number,
      generatedFileList: [generatedFileInfoSchema]
    });
    Datastore.OutputFileListModel = mongoose.model<OutputFileList>('OutputFileList', schema);
  }

  private static setupInputFileListSchema () {
    const fileInfoSchema = new Schema<FileInfo>({
      path: Schema.Types.String,
      size: Schema.Types.Number,
      start: Schema.Types.Number,
      end: Schema.Types.Number
    });
    const schema = new Schema<InputFileList>({
      generationId: {
        type: Schema.Types.String,
        index: 1
      },
      index: Schema.Types.Number,
      fileList: [fileInfoSchema]
    });
    Datastore.InputFileListModel = mongoose.model<InputFileList>('InputFileList', schema);
  }

  private static setupDealTrackingStateSchema () {
    const dealTrackingStateSchema = new Schema<DealTrackingState>({
      stateType: Schema.Types.String,
      stateKey: Schema.Types.String,
      stateValue: Schema.Types.Mixed
    });
    Datastore.DealTrackingStateModel = mongoose.model<DealTrackingState>('DealTrackingState', dealTrackingStateSchema);
  }

  private static setupProviderMetricSchema () {
    const providerMetricSchema = new Schema<ProviderMetric>({
      provider: Schema.Types.String,
      key: Schema.Types.String,
      value: Schema.Types.Mixed
    });
    Datastore.ProviderMetricModel = mongoose.model<ProviderMetric>('ProviderMetric', providerMetricSchema);
  }

  private static setupReplicationRequestSchema () {
    const replicationRequestSchema = new Schema<ReplicationRequest>({
      datasetId: Schema.Types.String,
      workerId: {
        type: Schema.Types.String,
        index: 1
      },
      maxReplicas: Schema.Types.Number,
      storageProviders: Schema.Types.String,
      client: Schema.Types.String,
      urlPrefix: Schema.Types.String,
      maxPrice: Schema.Types.Number,
      maxNumberOfDeals: Schema.Types.Number,
      isVerfied: Schema.Types.Boolean,
      startDelay: Schema.Types.Number,
      duration: Schema.Types.Number,
      isOffline: Schema.Types.Boolean,
      status: Schema.Types.String,
      cronSchedule: Schema.Types.String,
      cronMaxDeals: Schema.Types.Number,
      cronMaxPendingDeals: Schema.Types.Number,
      errorMessage: Schema.Types.String
    }, {
      timestamps: true
    });
    Datastore.ReplicationRequestModel = mongoose.model<ReplicationRequest>('ReplicationRequest', replicationRequestSchema);
  }

  private static setupDealStateSchema () {
    const dealStateSchema = new Schema<DealState>({
      client: Schema.Types.String,
      provider: Schema.Types.String,
      dealCid: Schema.Types.String,
      dataCid: Schema.Types.String,
      pieceCid: Schema.Types.String,
      startEpoch: Schema.Types.Number,
      expiration: Schema.Types.Number,
      duration: Schema.Types.Number,
      price: Schema.Types.Number,
      verified: Schema.Types.Boolean,
      state: {
        type: Schema.Types.String,
        index: true
      },
      replicationRequestId: Schema.Types.String,
      datasetId: Schema.Types.String,
      dealId: {
        type: Schema.Types.Number,
        index: true
      },
      errorMessage: Schema.Types.String
    }, {
      timestamps: true
    });
    dealStateSchema.index({ pieceCid: 1, provider: 1, client: 1, state: 1 });
    dealStateSchema.index({ client: 1, state: 1 });
    dealStateSchema.index({ replicationRequestId: 1, state: 1 });
    dealStateSchema.index({ pieceCid: 1, state: 1 });
    Datastore.DealStateModel = mongoose.model<DealState>('DealState', dealStateSchema);
  }

  private static setupGenerationRequestSchema () {
    const generationRequestSchema = new Schema<GenerationRequest>({
      datasetId: Schema.Types.String,
      datasetName: Schema.Types.String,
      path: Schema.Types.String,
      index: Schema.Types.Number,
      outDir: Schema.Types.String,
      workerId: {
        type: Schema.Types.String,
        index: 1
      },
      status: Schema.Types.String,
      errorMessage: Schema.Types.String,
      dataCid: Schema.Types.String,
      carSize: Schema.Types.Number,
      pieceCid: Schema.Types.String,
      pieceSize: Schema.Types.Number,
      filenameOverride: Schema.Types.String,
      tmpDir: Schema.Types.String
    }, {
      timestamps: true
    });
    Datastore.GenerationRequestModel = mongoose.model<GenerationRequest>('GenerationRequest', generationRequestSchema);
  }

  private static setupScanningRequestSchema () {
    const scanningRequestSchema = new Schema<ScanningRequest>({
      name: {
        type: Schema.Types.String,
        unique: true
      },
      path: Schema.Types.String,
      minSize: Schema.Types.Number,
      maxSize: Schema.Types.Number,
      outDir: Schema.Types.String,
      workerId: {
        type: Schema.Types.String,
        index: 1
      },
      status: Schema.Types.String,
      errorMessage: Schema.Types.String,
      tmpDir: Schema.Types.String,
      scanned: Schema.Types.Number
    }, {
      timestamps: true
    });
    Datastore.ScanningRequestModel = mongoose.model<ScanningRequest>('ScanningRequest', scanningRequestSchema);
  }

  private static setupHealthCheckSchema () {
    const healthCheckSchema = new Schema<HealthCheck>({
      workerId: {
        type: Schema.Types.String,
        index: true,
        unique: true
      },
      downloadSpeed: Schema.Types.Number,
      updatedAt: {
        type: Schema.Types.Date,
        index: 1,
        expires: 60
      }
    }, {
      timestamps: true
    });
    Datastore.HealthCheckModel = mongoose.model<HealthCheck>('HealthCheck', healthCheckSchema);
  }

  public static async init (inMemory: boolean): Promise<void> {
    await Datastore.setupLocalMongoDb(
      config.get('database.local_bind'),
      config.get('database.local_port'),
      inMemory ? undefined : path.resolve(getConfigDir(), config.get<string>('database.local_path')));
  }

  public static async connect (): Promise<void> {
    await Datastore.connectMongoDb(config.connection.database);
    Datastore.setupDataModels();
  }
}
