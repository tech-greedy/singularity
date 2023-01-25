enum ErrorCode {
  DATASET_NOT_FOUND = 'DATASET_NOT_FOUND',
  INVALID_MIN_START_DAYS = 'INVALID_MIN_START_DAYS',
  INVALID_MAX_START_DAYS = 'INVALID_MAX_START_DAYS',
  INVALID_MIN_MAX_START_DAYS = 'INVALID_MIN_MAX_START_DAYS',
  INVALID_PRICE = 'INVALID_PRICE',
  INVALID_MIN_DURATION_DAYS = 'INVALID_MIN_DURATION_DAYS',
  INVALID_MAX_DURATION_DAYS = 'INVALID_MAX_DURATION_DAYS',
  INVALID_MIN_MAX_DURATION_DAYS = 'INVALID_MIN_MAX_DURATION_DAYS',
  INVALID_PROVIDER = 'INVALID_PROVIDER',
  INVALID_DATASET = 'INVALID_DATASET',
  NO_MATCHING_POLICY = 'NO_MATCHING_POLICY',
  ALREADY_PROPOSED = 'ALREADY_PROPOSED',
  NO_PIECE_TO_PROPOSE = 'NO_PIECE_TO_PROPOSE',
  PIECE_NOT_FOUND = 'PIECE_NOT_FOUND',
  INVALID_MAX_DAYS = 'INVALID_MAX_DAYS',
  INVALID_REQUEST = 'INVALID_REQUEST'
}

export const ErrorMessage = {
  [ErrorCode.DATASET_NOT_FOUND]: 'Dataset cannot be found by the specified name or id',
  [ErrorCode.INVALID_MIN_START_DAYS]: 'Minimum start days need to be between 2 and 30',
  [ErrorCode.INVALID_MAX_START_DAYS]: 'Maximum start days need to be between 2 and 30',
  [ErrorCode.INVALID_MIN_MAX_START_DAYS]: 'Minimum start days cannot be greater than maximum start days',
  [ErrorCode.INVALID_PRICE]: 'Price needs to be greater or equal to 0',
  [ErrorCode.INVALID_MIN_DURATION_DAYS]: 'Minimum duration days need to be between 180 and 540',
  [ErrorCode.INVALID_MAX_DURATION_DAYS]: 'Maximum duration days need to be between 180 and 540',
  [ErrorCode.INVALID_MIN_MAX_DURATION_DAYS]: 'Minimum duration days cannot be greater than maximum duration days',
  [ErrorCode.INVALID_PROVIDER]: 'Provider is not provided',
  [ErrorCode.INVALID_DATASET]: 'Dataset is not provided',
  [ErrorCode.NO_MATCHING_POLICY]: 'No matching policy found',
  [ErrorCode.ALREADY_PROPOSED]: 'The deal has already been proposed for the same provider and pieceCid',
  [ErrorCode.NO_PIECE_TO_PROPOSE]: 'No more pieceCid to propose for the same provider and dataset',
  [ErrorCode.PIECE_NOT_FOUND]: 'The pieceCid is not found with the specified dataset',
  [ErrorCode.INVALID_MAX_DAYS]: 'maxDurationDays + maxStartDays >= 540, this will lead to invalid deal proposal',
  [ErrorCode.INVALID_REQUEST]: 'The request is invalid. Some parameters are missing.'
};

export default ErrorCode;
