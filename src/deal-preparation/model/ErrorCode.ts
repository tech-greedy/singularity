enum ErrorCode {
  DEAL_SIZE_NOT_ALLOWED = 'DEAL_SIZE_NOT_ALLOWED',
  PATH_NOT_ACCESSIBLE = 'PATH_NOT_ACCESSIBLE',
  TMPDIR_MISSING_FOR_S3 = 'TMPDIR_MISSING_FOR_S3',
  DATASET_NAME_CONFLICT = 'DATASET_NAME_CONFLICT',
  DATASET_NOT_FOUND = 'DATASET_NOT_FOUND',
  DATASET_GENERATION_REQUEST_NOT_FOUND = 'DATASET_GENERATION_REQUEST_NOT_FOUND',
  MIN_RATIO_INVALID = 'MIN_RATIO_INVALID',
  MAX_RATIO_INVALID = 'MAX_RATIO_INVALID',
  GENERATION_NOT_COMPLETED = 'GENERATION_NOT_COMPLETED',
}

export const ErrorMessage = {
  [ErrorCode.DEAL_SIZE_NOT_ALLOWED]: 'Deal size is not valid. It needs to be the power of 2 and between 256B to 64GiB',
  [ErrorCode.PATH_NOT_ACCESSIBLE]: 'Input path, tmp path or output path is not accessible',
  [ErrorCode.TMPDIR_MISSING_FOR_S3]: 'Tmpdir is required but not set for S3 dataset',
  [ErrorCode.DATASET_NAME_CONFLICT]: 'There is already a dataset with the same name, please try with a different name',
  [ErrorCode.DATASET_NOT_FOUND]: 'Dataset cannot be found by the specified name or id',
  [ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND]: 'The generation request cannot be found by the specified id and/or dataset',
  [ErrorCode.MIN_RATIO_INVALID]: 'The minimum target ratio is not valid. It needs to be between 0.5 and 0.95',
  [ErrorCode.MAX_RATIO_INVALID]: 'The maximum target ratio is not valid. It needs to be between 0.5 and 0.95 and more than minimum target ratio',
  [ErrorCode.GENERATION_NOT_COMPLETED]: 'The generation request is not complete yet'
};

export default ErrorCode;
