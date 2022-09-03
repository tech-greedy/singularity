import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import ErrorCode from '../model/ErrorCode';
import Datastore from '../../common/Datastore';
import sendError from './ErrorHandler';
import UpdatePreparationRequest from '../model/UpdatePreparationRequest';

export default async function handleUpdatePreparationRequest (this: DealPreparationService, request: Request, response: Response) {
  const id = request.params['id'];
  const { action } = <UpdatePreparationRequest>request.body;
  this.logger.info(`Received request to update dataset preparation request.`, {
    id,
    action
  });
  const found = await Datastore.findScanningRequest(id);
  if (!found) {
    sendError(this.logger, response, ErrorCode.DATASET_NOT_FOUND);
    return;
  }

  const actionMap = {
    resume: {
      condition: { status: 'paused' },
      update: {
        status: 'active',
        workerId: null
      }
    },
    pause: {
      condition: { status: 'active' },
      update: {
        status: 'paused',
        workerId: null
      }
    },
    retry: {
      condition: { status: 'error' },
      update: {
        status: 'active',
        $unset: {
          errorMessage: 1
        },
        workerId: null
      }
    }
  };
  const {
    condition,
    update
  } = actionMap[action];
  this.logger.info(`Updating dataset preparation request.`, { action });
  const success = (await Datastore.ScanningRequestModel.findOneAndUpdate({
    _id: found.id,
    ...condition
  }, update)) != null;
  this.logger.info(`Updated dataset preparation request.`, { success });
  response.end(JSON.stringify({
    success
  }));
}
