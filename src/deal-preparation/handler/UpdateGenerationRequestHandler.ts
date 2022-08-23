import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import ErrorCode from '../model/ErrorCode';
import sendError from './ErrorHandler';
import UpdateGenerationRequest from '../model/UpdateGenerationRequest';

export default async function handleUpdateGenerationRequest (this: DealPreparationService, request: Request, response: Response) {
  const dataset = request.params['dataset'];
  const id = request.params['id'];
  const { action, tmpDir, outDir } = <UpdateGenerationRequest>request.body;
  this.logger.info(`Received request to update dataset preparation request.`, {
    dataset,
    id,
    action
  });
  const found = await Datastore.findScanningRequest(dataset);
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
    },
    forceRetry: {
      condition: { status: { $in: ['completed', 'error'] } },
      update: {
        status: 'active',
        $unset: {
          errorMessage: 1
        },
        workerId: null
      }
    }
  };

  let success = false;
  if (!id) {
    if (tmpDir !== undefined || outDir !== undefined) {
      success ||= (await Datastore.GenerationRequestModel.updateMany({
        datasetId: found.id
      }, {
        $set: {
          tmpDir,
          outDir
        }
      })).modifiedCount > 0;
    }
    if (action) {
      success ||= (await Datastore.GenerationRequestModel.updateMany({
        datasetId: found.id,
        ...actionMap[action].condition
      }, actionMap[action].update)).modifiedCount > 0;
    }
  } else {
    const generation = await Datastore.findGenerationRequest(id, dataset);
    if (!generation) {
      sendError(this.logger, response, ErrorCode.DATASET_GENERATION_REQUEST_NOT_FOUND);
      return;
    }
    if (tmpDir !== undefined || outDir !== undefined) {
      success ||= (await Datastore.GenerationRequestModel.findByIdAndUpdate(
        generation.id,
        {
          $set: {
            tmpDir,
            outDir
          }
        })) != null;
    }
    if (action) {
      success ||= (await Datastore.GenerationRequestModel.findOneAndUpdate(
        { _id: generation.id, ...actionMap[action].condition },
        actionMap[action].update
      )) != null;
    }
  }

  response.end(JSON.stringify({
    success
  }));
}
