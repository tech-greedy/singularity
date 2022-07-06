import path from 'path';
import fs from 'fs-extra';

export default class GenerateCar {
  public static path?: string;
  public static initialize () {
    if (!GenerateCar.path) {
      let dir = path.dirname(require.main!.filename);
      for (let i = 0; i < 10; ++i) {
        const p1 = path.join(dir, 'node_modules', '.bin', 'generate-car');
        const p2 = path.join(dir, 'node_modules', 'bin', 'generate-car');
        if (fs.existsSync(p1)) {
          GenerateCar.path = p1;
          break;
        }
        if (fs.existsSync(p2)) {
          GenerateCar.path = p2;
          break;
        }
        dir = path.dirname(dir);
        if (dir === '/') {
          break;
        }
      }
      // Somehow, win32 has generate-car binary at same PATH as singularity executable
      if (!GenerateCar.path && process.platform !== 'win32') {
        throw new Error('Cannot find generate-car, please report this as a bug');
      }
    }
  }
}
