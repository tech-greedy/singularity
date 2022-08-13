import winston from 'winston';
import Logger, { Category } from './Logger';
import config from './Config';

export default abstract class BaseService {
  protected logger: winston.Logger;
  protected enabled: boolean;

  protected constructor (category: Category) {
    this.logger = Logger.getLogger(category);
    this.enabled = config[category]?.enabled ?? true;
  }

  public abstract start (): void;
}
