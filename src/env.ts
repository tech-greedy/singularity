import { homedir } from 'os';
import path from 'path';
import fs from 'fs';

if (process.env.SINGULARITY_PATH) {
  process.env.NODE_CONFIG_DIR = process.env.SINGULARITY_PATH;
} else {
  const home = homedir();
  const defaultConfigPath = path.join(home, '.singularity');
  process.env.NODE_CONFIG_DIR = defaultConfigPath;
}

export const NODE_CONFIG_DIR = process.env.NODE_CONFIG_DIR;

fs.mkdirSync(NODE_CONFIG_DIR, { recursive: true });
if (!fs.existsSync(path.join(NODE_CONFIG_DIR, 'default.toml'))) {
  console.info(`Initializing at ${NODE_CONFIG_DIR} ...`);
  fs.copyFileSync(path.join(__dirname, '../config/default.toml'), path.join(NODE_CONFIG_DIR, 'default.toml'));
}
