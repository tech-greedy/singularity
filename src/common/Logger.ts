import expressWinston from 'express-winston';
import winston from 'winston';
import path from 'path';
import config, { getConfigDir } from './Config';
import * as Transport from 'winston-transport';

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

function getTransports (category : Category): Transport[] {
  const transports = [];
  transports.push(new winston.transports.Console({
    level: config.get('logging.console_level'),
    format: loggerFormat(category, true)
  }));
  if (config.get('logging.file_path')) {
    transports.push(new winston.transports.File({
      level: config.get('logging.file_level'),
      dirname: path.resolve(getConfigDir(), config.get('logging.file_path')),
      filename: `${category}.log`,
      format: loggerFormat(category, false)
    }));
  }
  return transports;
}

Object.values(Category).forEach(category => {
  container.add(category, {
    transports: getTransports(category)
  });
});

export default class Logger {
  public static getLogger (category: Category) {
    return container.get(category);
  }

  public static getExpressLogger (category: Category) {
    return expressWinston.logger({
      transports: getTransports(category),
      meta: true,
      msg: 'HTTP {{req.method}} {{req.url}}',
      expressFormat: true,
      colorize: true,
      ignoreRoute: (_req, _res) => false
    });
  }
}
