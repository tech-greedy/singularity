import path from 'path';
import fs from 'fs-extra';
import Logger, { Category } from './Logger';
import { execSync } from 'child_process';

/* istanbul ignore next */
export default class GenerateCar {
  public static path?: string;
  public static initialize () {
    const logger = Logger.getLogger(Category.Default);
    if (!GenerateCar.path) {
      let dir = path.dirname(require.main!.filename);
      for (let i = 0; i < 10; ++i) {
        const p1 = path.join(dir, 'node_modules', '.bin', 'generate-car');
        const p2 = path.join(dir, 'node_modules', 'bin', 'generate-car');
        const p3 = path.join(dir, '.bin', 'generate-car');
        const p4 = path.join(dir, 'bin', 'generate-car');
        const p5 = path.join(dir, 'node_modules', '.bin', 'generate-car.exe');
        const p6 = path.join(dir, 'node_modules', 'bin', 'generate-car.exe');
        const p7 = path.join(dir, '.bin', 'generate-car.exe');
        const p8 = path.join(dir, 'bin', 'generate-car.exe');
        for (const p of [p1, p2, p3, p4, p5, p6, p7, p8]) {
          logger.debug(`Checking ${p} for generate-car binary`);
          if (fs.existsSync(p)) {
            GenerateCar.path = p;
            break;
          }
        }
        if (GenerateCar.path) {
          break;
        }
        dir = path.dirname(dir);
        if (dir === '/') {
          break;
        }
      }
      // Starting node v18.14.0, the generate-car will be in the PATH so let's just try invoking it directly
      if (!GenerateCar.path) {
        try {
          execSync('generate-car -h');
          GenerateCar.path = 'generate-car';
          return;
        } catch (error) {
          logger.error('Unable to find generate-car binary');
          throw error;
        }
      }
      logger.info(`Found generate-car at ${GenerateCar.path}`);
    }
  }
}
