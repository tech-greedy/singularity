import config from 'config';
import winston from 'winston';
import Logger, { Category } from './Logger';

export default abstract class BaseService {
  protected logger: winston.Logger;
  protected enabled: boolean;

  protected constructor (category: Category) {
    this.logger = Logger.getLogger(category);
    this.enabled = config.get(`${category}.enabled`);
  }

  public abstract start (): void;
}
