import expressWinston from 'express-winston';
import winston from 'winston';

export enum Category {
  Cli = 'cli',
  DealPreparationService = 'deal_preparation_service',
  Database = 'database',
  DealPreparationWorker = 'deal_preparation_worker',
  IndexService = 'index_service',
  HttpHostingService = 'http_hosting_service',
  DealTrackingService = 'deal_tracking_service',
}

const container = new winston.Container();
const loggerFormat = (category: string) => winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp(),
  winston.format.label({
    label: category
  }),
  winston.format.printf(({ level, message, label, timestamp }) => {
    return `${timestamp} [${label}] ${level}: ${message}`;
  }));
Object.values(Category).forEach(category => {
  container.add(category, {
    level: 'info',
    format: loggerFormat(category),
    transports: [new winston.transports.Console()]
  });
});

export default class Logger {
  public static getLogger (category: Category) {
    return container.get(category);
  }

  public static getExpressLogger (category: Category) {
    return expressWinston.logger({
      transports: [new winston.transports.Console()],
      format: loggerFormat(category),
      meta: true,
      msg: 'HTTP {{req.method}} {{req.url}}',
      expressFormat: true,
      colorize: true,
      ignoreRoute: (_req, _res) => false
    });
  }
}
