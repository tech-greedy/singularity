import toml from '@iarna/toml';
import path from 'path';
import { homedir } from 'os';
import fs from 'fs-extra';
import { FSWatcher } from 'fs';

interface Config {
  [key: string]: any;
  get<T> (key: string): T;
  getOrDefault<T> (key: string, defaultValue: T): T;
  has (key: string): boolean;
}

const config: Config = {
  get<T> (key: string): T {
    let value: any = config;
    for (const k of key.split('.')) {
      if (!(k in value)) {
        throw new Error(`Config key ${key} not found`);
      }
      value = value[k];
    }
    return value;
  },
  getOrDefault<T> (key: string, defaultValue: T): T {
    let value: any = config;
    for (const k of key.split('.')) {
      if (!(k in value)) {
        return defaultValue;
      }
      value = value[k];
    }
    return value;
  },
  has (key: string): boolean {
    let value: any = config;
    for (const k of key.split('.')) {
      if (!(k in value)) {
        return false;
      }
      value = value[k];
    }
    return true;
  }
};
export default config;

export function getConfigDir (): string {
  return process.env.SINGULARITY_PATH || path.join(homedir(), '.singularity');
}

export class ConfigInitializer {
  public static instanceId = 'unknown';
  private static initialized = false;
  private static fsWatcher: FSWatcher;

  public static unwatchFile (): void {
    ConfigInitializer.fsWatcher?.close();
  }

  public static watchFile (): void {
    if (ConfigInitializer.fsWatcher) {
      return;
    }
    const configDir = getConfigDir();
    const configPath = path.join(configDir, 'default.toml');
    if (fs.pathExistsSync(configPath)) {
      ConfigInitializer.fsWatcher = fs.watch(configPath, async (eventType: string, _filename: string) => {
        switch (eventType) {
          case 'change':
            if (await fs.pathExists(configPath)) {
              console.log('Config file changed, reloading...');
              ConfigInitializer.updateValues(await fs.readFile(configPath, 'utf8'));
            }
            break;
          default:
            console.error('Config file may have been renamed or deleted.');
        }
      });
    }
  }

  public static async initialize (useDefault = false): Promise<void> {
    if (ConfigInitializer.initialized) {
      return;
    }
    const configDir = getConfigDir();
    const configPath = path.join(configDir, 'default.toml');
    let fileString: string;
    if (!useDefault && await fs.pathExists(configPath)) {
      fileString = await fs.readFile(configPath, 'utf8');
      ConfigInitializer.updateValues(fileString);
    } else {
      fileString = await fs.readFile(path.join(__dirname, '..', '..', 'config', 'default.toml'), 'utf8');
      ConfigInitializer.updateValues(fileString);
    }
  }

  private static updateValues (fileString: string) {
    try {
      const tomlObject = toml.parse(fileString);
      for (const key in config) {
        if (['get', 'getOrDefault', 'has'].includes(key)) {
          continue;
        }
        delete config[key];
      }
      for (const key in tomlObject) {
        if (['get', 'getOrDefault', 'has'].includes(key)) {
          continue;
        }
        config[key] = tomlObject[key];
      }
    } catch (e) {
      console.error('Error parsing config file: ', e);
    }
  }
}
