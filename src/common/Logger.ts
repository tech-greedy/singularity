import expressWinston from 'express-winston';
import winston from 'winston';
import path from 'path';
import config, { getConfigDir } from './Config';

export enum Category {
  Default = 'default',
  DealPreparationService = 'deal_preparation_service',
  Database = 'database',
  DealPreparationWorker = 'deal_preparation_worker',
  IndexService = 'index_service',
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
    level: config.logging?.console_level ?? 'info',
    format: loggerFormat(category, true)
  }));
  if (config.logging?.file_path) {
    transports.push(new winston.transports.File({
      level: config.logging.file_level ?? 'info',
      dirname: path.resolve(getConfigDir(), config.get('logging.file_path')),
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
      level: config.logging?.console_level ?? 'info',
      format: loggerFormat(category, true)
    }));
    if (config.logging?.file_path) {
      transports.push(new winston.transports.File({
        level: config.logging?.file_level ?? 'info',
        dirname: path.resolve(getConfigDir(), config.get('logging.file_path')),
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
