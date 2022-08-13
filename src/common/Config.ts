import toml from 'toml';
import path from 'path';
import { homedir } from 'os';
import fs from 'fs-extra';

interface Config {
  [key: string]: any;
  get<T> (key: string): T;
  getOrDefault<T> (key: string, defaultValue: T): T;
}

let config: Config = {
  get<T> (_key: string): T {
    throw new Error('Not implemented');
  },
  getOrDefault<T> (_key: string, _defaultValue: T): T {
    throw new Error('Not implemented');
  }
};
export default config;

export function getConfigDir (): string {
  return process.env.SINGULARITY_PATH || path.join(homedir(), '.singularity');
}

export class ConfigInitializer {
  public static async initialize (): Promise<void> {
    const configDir = getConfigDir();
    const configPath = path.join(configDir, 'config.toml');
    let fileString: string;
    if (await fs.pathExists(configPath)) {
      fileString = await fs.readFile(configPath, 'utf8');
      ConfigInitializer.updateValues(fileString);
      fs.watch(configPath, async (eventType: string, _filename: string) => {
        switch (eventType) {
          case 'change':
            console.log('Config file changed, reloading...');
            ConfigInitializer.updateValues(await fs.readFile(configPath, 'utf8'));
            break;
          default:
            console.error('Config file may have been renamed or deleted.');
        }
      });
    } else {
      fileString = require('../../config/default.toml');
      ConfigInitializer.updateValues(fileString);
    }
  }

  private static updateValues (fileString: string) {
    try {
      config = toml.parse(fileString);
      config.get = (key: string) => {
        let value: any = config;
        for (const k of key.split('.')) {
          value = value[k];
        }
        return value;
      };
      config.getOrDefault = (key: string, defaultValue: any) => {
        let value: any = config;
        for (const k of key.split('.')) {
          if (value[k] === undefined) {
            return defaultValue;
          }
          value = value[k];
        }
        return value;
      };
    } catch (e) {
      console.error('Error parsing config file: ', e);
    }
  }
}
