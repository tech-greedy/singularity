enum ErrorCode {
  DATASET_NAME_CONFLICT = 'DATASET_NAME_CONFLICT',
  DATASET_NOT_FOUND = 'DATASET_NOT_FOUND',
  GENERATION_NOT_FOUND = 'GENERATION_NOT_FOUND',
  REPLICATION_NOT_FOUND = 'REPLICATION_NOT_FOUND',
  CHANGE_STATE_INVALID = 'CHANGE_STATE_INVALID',
  INVALID_OBJECT_ID = 'INVALID_OBJECT_ID',
  INTERNAL_ERROR = 'INTERNAL_ERROR',
  NOT_CRON_JOB = 'NOT_CRON_JOB',
}

export const ErrorMessage = {
  [ErrorCode.DATASET_NAME_CONFLICT]: 'Replication request cannot be found by the specified id',
  [ErrorCode.DATASET_NOT_FOUND]: 'Dataset Preparation request cannot be found by the specified id or name',
  [ErrorCode.GENERATION_NOT_FOUND]: 'No generation requests are found for the specific dataset',
  [ErrorCode.REPLICATION_NOT_FOUND]: 'Replication request cannot be found by the specified id',
  [ErrorCode.CHANGE_STATE_INVALID]: 'Replication request already completed or has error and cannot be updated',
  [ErrorCode.INVALID_OBJECT_ID]: 'Replication request cannot be found by the specified id',
  [ErrorCode.INTERNAL_ERROR]: 'Replication request cannot be found by the specified id',
  [ErrorCode.NOT_CRON_JOB]: 'Replication request was not a cron job and cannot be changed',
};

export default ErrorCode;
