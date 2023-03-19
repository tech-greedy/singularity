import path from 'path';
import fs from 'fs-extra';
import Logger, { Category } from './Logger';
import { execSync } from 'child_process';

/* istanbul ignore next */
export default class GenerateCar {
  public static pathMap: Map<string, string> = new Map();
  public static generateCarPath (): string {
    GenerateCar.initialize();
    return GenerateCar.pathMap.get('generate-car')!;
  }

  public static generateIpldCarPath (): string {
    GenerateCar.initialize();
    return GenerateCar.pathMap.get('generate-ipld-car')!;
  }

  public static initialize () {
    const logger = Logger.getLogger(Category.Default);
    for (const name of ['generate-car', 'generate-ipld-car']) {
      if (!GenerateCar.pathMap.has(name)) {
        let dir = path.dirname(require.main!.filename);
        for (let i = 0; i < 10; ++i) {
          const p1 = path.join(dir, 'node_modules', '.bin', name);
          const p2 = path.join(dir, 'node_modules', 'bin', name);
          const p3 = path.join(dir, '.bin', name);
          const p4 = path.join(dir, 'bin', name);
          const p5 = path.join(dir, 'node_modules', '.bin', name + '.exe');
          const p6 = path.join(dir, 'node_modules', 'bin', name + '.exe');
          const p7 = path.join(dir, '.bin', name + '.exe');
          const p8 = path.join(dir, 'bin', name + '.exe');
          for (const p of [p1, p2, p3, p4, p5, p6, p7, p8]) {
            logger.debug(`Checking ${p} for ${name} binary`);
            if (fs.existsSync(p)) {
              GenerateCar.pathMap.set(name, p);
              break;
            }
          }
          if (GenerateCar.pathMap.has(name)) {
            break;
          }
          dir = path.dirname(dir);
          if (dir === '/') {
            break;
          }
        }
        // Starting node v18.14.0, the generate-car will be in the PATH so let's just try invoking it directly
        if (!GenerateCar.pathMap.has(name)) {
          try {
            execSync(`${name} -h`);
            GenerateCar.pathMap.set(name, name);
            continue;
          } catch (error) {
            logger.error('Unable to find generate-car binary');
            throw error;
          }
        }
        logger.info(`Found ${name} binary at ${GenerateCar.pathMap.get(name)}`);
      }
    }
  }
}
