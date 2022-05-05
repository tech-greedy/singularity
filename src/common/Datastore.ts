import config from 'config';
import fs from 'fs';
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose, { Schema } from 'mongoose';
import Logger, { Category } from './Logger';
import DealState from './model/DealState';
import DealTrackingState from './model/DealTrackingState';
import GenerationRequest, { FileInfo } from './model/GenerationRequest';
import HealthCheck from './model/HealthCheck';
import ProviderMetric from './model/ProviderMetric';
import ReplicationRequest from './model/ReplicationRequest';
import ScanningRequest from './model/ScanningRequest';
import path from 'path';

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
    this.setupDealTrackingState();
  }

  private static setupDealTrackingState () {
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
      minReplicas: Schema.Types.Number,
      client: Schema.Types.String,
      criteria: Schema.Types.String,
      status: Schema.Types.String
    });
    Datastore.ReplicationRequestModel = mongoose.model<ReplicationRequest>('ReplicationRequest', replicationRequestSchema);
  }

  private static setupDealStateSchema () {
    const dealStateSchema = new Schema<DealState>({
      client: Schema.Types.String,
      provider: Schema.Types.String,
      pieceCid: Schema.Types.String,
      dealId: Schema.Types.Number,
      expiration: Schema.Types.Number,
      state: Schema.Types.String
    });
    // TODO create index if needed
    Datastore.DealStateModel = mongoose.model<DealState>('DealState', dealStateSchema);
  }

  private static setupGenerationRequestSchema () {
    const fileInfoSchema = new Schema<FileInfo>({
      path: Schema.Types.String,
      name: Schema.Types.String,
      size: Schema.Types.Number,
      start: Schema.Types.Number,
      end: Schema.Types.Number,
      selector: [Schema.Types.Number]
    });
    const generationRequestSchema = new Schema<GenerationRequest>({
      datasetId: Schema.Types.String,
      datasetName: Schema.Types.String,
      path: Schema.Types.String,
      index: Schema.Types.Number,
      fileList: [fileInfoSchema],
      workerId: {
        type: Schema.Types.String,
        index: 1
      },
      status: Schema.Types.String,
      errorMessage: Schema.Types.String,
      dataCid: Schema.Types.String,
      pieceCid: Schema.Types.String,
      pieceSize: Schema.Types.Number
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
      workerId: {
        type: Schema.Types.String,
        index: 1
      },
      status: Schema.Types.String,
      errorMessage: Schema.Types.String
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

  public static async init (): Promise<void> {
    if (config.has('database.start_local') && config.get('database.start_local')) {
      await Datastore.setupLocalMongoDb(config.get('database.local_bind'), config.get('database.local_port'), path.resolve(process.env.NODE_CONFIG_DIR!, config.get<string>('database.local_path')));
    }

    if (config.has('connection.database')) {
      await Datastore.connectMongoDb(config.get('connection.database'));
      Datastore.setupDataModels();
    }
  }
}
