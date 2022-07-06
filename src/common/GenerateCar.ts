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
        const p3 = path.join(dir, '.bin', 'generate-car');
        const p4 = path.join(dir, 'bin', 'generate-car');
	for (const p of [p1, p2, p3, p4]) {
          if (fs.existsSync(p)) {
            GenerateCar.path = p;
            break;
          }
	}
	if (GenerateCar.path) {
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
