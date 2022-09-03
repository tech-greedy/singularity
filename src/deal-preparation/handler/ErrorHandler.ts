import { Response } from 'express';
import ErrorCode, { ErrorMessage } from '../model/ErrorCode';
import winston from 'winston';

export default function sendError (logger: winston.Logger, response: Response, error: ErrorCode) {
  logger.warn(`Error code`, { error });
  response.status(400);
  response.end(JSON.stringify({ error, message: ErrorMessage[error] }));
}
