import ImportOptions from './ImportOptions';

export function throwError (message?: any, ...optionalParams: any[]) {
  console.error(message, ...optionalParams);
  process.exit(1);
}

export function validateImportOptions (options: ImportOptions) {
  if (!process.env.LOTUS_MINER_PATH ||
    !process.env.LOTUS_MARKETS_PATH ||
    !process.env.MINER_API_INFO ||
    !process.env.MARKETS_API_INFO) {
    throwError('Make sure you have one of the following environment variables set: LOTUS_MINER_PATH, LOTUS_MARKETS_PATH, MINER_API_INFO, MARKETS_API_INFO');
  }
  if (!options.path && !options.urlPrefix) {
    throwError('Either --path or --url-prefix is required');
  }
  if (options.urlPrefix && !options.downloadFolder) {
    throwError('--download-folder is required when --url-prefix is used');
  }
  if (options.importInterval < 0) {
    throwError('--import-interval must be greater than or equal to 0');
  }
  if (options.maxConcurrentImports < 1) {
    throwError('--max-concurrent-imports must be greater than or equal to 1');
  }
  if (options.importInterval < 120 && options.maxConcurrentImports > 1) {
    console.warn('The interval is less than 120s and the concurrency imports are greater than 1.' +
      ' This may lead to OOM. Make sure you understand what you are doing.');
  }
}
