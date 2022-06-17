import expressWinston from 'express-winston';
import winston from 'winston';
import config from 'config';
import path from 'path';

export enum Category {
  Cli = 'cli',
  DealPreparationService = 'deal_preparation_service',
  Database = 'database',
  DealPreparationWorker = 'deal_preparation_worker',
  IndexService = 'index_service',
  HttpHostingService = 'http_hosting_service',
  DealTrackingService = 'deal_tracking_service',
  DealReplicationService = 'deal_replication_service',
  DealReplicationWorker = 'deal_replication_worker',
}

const container = new winston.Container();
const loggerFormat = (category: string, colorize: boolean) => {
  const formats = [
    winston.format.timestamp(),
    winston.format.splat(),
    winston.format.label({
      label: category
    }),
    winston.format.printf(({ level, message, label, timestamp, ...others }) => {
      return `${timestamp} [${label}] ${level}: ${message} - ${JSON.stringify(others)}`;
    })];
  if (colorize) {
    formats.push(winston.format.colorize());
  }
  return winston.format.combine(...formats);
};
Object.values(Category).forEach(category => {
  const transports = [];
  transports.push(new winston.transports.Console({
    level: config.has('logging.console_level') ? config.get('logging.console_level') : 'info',
    format: loggerFormat(category, true)
  }));
  if (config.has('logging.file_level') && config.has('logging.file_path')) {
    transports.push(new winston.transports.File({
      level: config.get('logging.file_level'),
      dirname: path.resolve(process.env.NODE_CONFIG_DIR!, config.get('logging.file_path')),
      filename: `${category}.log`,
      format: loggerFormat(category, false)
    }));
  }
  container.add(category, {
    transports
  });
});

export default class Logger {
  public static getLogger (category: Category) {
    return container.get(category);
  }

  public static getExpressLogger (category: Category) {
    const transports = [];
    transports.push(new winston.transports.Console({
      level: config.has('logging.console_level') ? config.get('logging.console_level') : 'info',
      format: loggerFormat(category, true)
    }));
    if (config.has('logging.file_level') && config.has('logging.file_path')) {
      transports.push(new winston.transports.File({
        level: config.get('logging.file_level'),
        dirname: path.resolve(process.env.NODE_CONFIG_DIR!, config.get('logging.file_path')),
        filename: `${category}.log`,
        format: loggerFormat(category, false)
      }));
    }
    return expressWinston.logger({
      transports,
      meta: true,
      msg: 'HTTP {{req.method}} {{req.url}}',
      expressFormat: true,
      colorize: true,
      ignoreRoute: (_req, _res) => false
    });
  }
}
