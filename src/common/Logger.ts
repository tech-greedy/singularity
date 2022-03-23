import expressWinston from 'express-winston';
import winston from 'winston';

export enum Category {
  Orchestrator = 'orchestrator',
  Database = 'database',
  DealPreparationWorker = 'deal_preparation_worker',
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
