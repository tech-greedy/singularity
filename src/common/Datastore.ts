import config from 'config';
import fs from 'fs';
import { MongoMemoryServer } from 'mongodb-memory-server';
import mongoose, { Schema } from 'mongoose';
import Logger, { Category } from './Logger';
import HealthCheck from './model/HealthCheck';
import ScanningRequest from './model/ScanningRequest';

export default class Datastore {
  private static logger = Logger.getLogger(Category.Database);

  // eslint-disable-next-line @typescript-eslint/ban-types
  public static HealthCheckModel : mongoose.Model<HealthCheck, {}, {}, {}>;
  public static ScanningRequestModel : mongoose.Model<ScanningRequest, {}, {}, {}>;

  private static DB_NAME = 'singularity';

  private static async setupLocalMongoDb () : Promise<void> {
    this.logger.info('Starting local mongodb database.');
    const path = config.get<string>('database.local_path');
    fs.mkdirSync(path, {
      recursive: true
    });
    await MongoMemoryServer.create({
      instance: {
        port: config.get('database.local_port'),
        ip: config.get('database.local_bind'),
        dbPath: path,
        storageEngine: 'wiredTiger',
        auth: false
      }
    });
  }

  private static async connectMongoDb () : Promise<void> {
    await mongoose.connect(config.get('connection.database'), { dbName: Datastore.DB_NAME });
  }

  private static setupDataModels (): void {
    this.logger.info('Setting up database models.');
    const healthCheckSchema = new Schema<HealthCheck>({
      workerId: {
        type: Schema.Types.String,
        index: true,
        unique: true
      },
      createdAt: {
        type: Schema.Types.Date,
        index: 1,
        expires: 60
      }
    }, {
      timestamps: true
    });

    Datastore.HealthCheckModel = mongoose.model<HealthCheck>('HealthCheck', healthCheckSchema);

    const scanningRequestSchema = new Schema<ScanningRequest>({
      datasetName: {
        type: Schema.Types.String,
        index: true,
        unique: true
      },
      datasetPath: Schema.Types.String,
      minSize: Schema.Types.String,
      maxSize: Schema.Types.String,
      workerId: Schema.Types.String,
      completed: Schema.Types.Boolean
    });
    Datastore.ScanningRequestModel = mongoose.model<ScanningRequest>('ScanningRequest', scanningRequestSchema);
  }

  public static async init () : Promise<void> {
    if (config.has('database.start_local') && config.get('database.start_local')) {
      await Datastore.setupLocalMongoDb();
    }

    if (config.has('connection.database')) {
      await Datastore.connectMongoDb();
      Datastore.setupDataModels();
    }
  }
}
