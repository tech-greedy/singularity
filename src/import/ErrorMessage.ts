export enum ErrorMessage {
  LOTUS_MINER_PATH_MISSING = 'Make sure you have one of the following environment variables set: LOTUS_MINER_PATH, LOTUS_MARKETS_PATH, MINER_API_INFO, MARKETS_API_INFO',
  SINCE_LESS_THAN_0 = '--since must be greater than 0',
  PATH_OR_URL_TEMPLATE_REQUIRED = 'Either --path or --url-template is required',
  DOWNLOAD_FOLDER_REQUIRED = '--download-folder is required when --url-template is used',
  INTERVAL_LESS_THAN_0 = '--interval must be greater than or equal to 0',
  INTERVAL_CAP_LESS_THAN_1 = '--interval-cap must be greater than or equal to 1',
  DOWNLOAD_CONCURRENCY_LESS_THAN_1 = '--download-concurrency must be greater than or equal to 1',
  IMPORT_CONCURRENCY_LESS_THAN_1 = '--import-concurrency must be greater than or equal to 1',
  IMPORT_CONCURRENCY_GREATER_THAN_1 = 'The import concurrency is greater than 1.' +
    ' Make sure you have enough system resources to import multiple deals concurrently.',
  LOOP_AND_DRY_RUN = '--loop and --dry-run cannot be used together. Loop will be disabled.',
}
