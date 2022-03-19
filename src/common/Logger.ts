import expressWinston from 'express-winston';
import winston from 'winston';

export enum Category {
  Orchestrator = 'Orchestrator'
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
Object.keys(Category).forEach(category => {
  container.add(category, {
    level: 'info',
    format: loggerFormat(category),
    transports: [new winston.transports.Console()]
  });
});

export default class Logger {
  public static GetLogger (category: Category) {
    return container.get(category);
  }

  public static GetExpressLogger (category: Category) {
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
