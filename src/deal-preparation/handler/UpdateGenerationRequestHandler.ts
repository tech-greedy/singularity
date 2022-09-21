import DealPreparationService from '../DealPreparationService';
import { Request, Response } from 'express';
import Datastore from '../../common/Datastore';
import ErrorCode from '../model/ErrorCode';
import sendError from './ErrorHandler';
import UpdateGenerationRequest from '../model/UpdateGenerationRequest';
import fs from 'fs-extra';
import { constants } from 'fs';

export default async function handleUpdateGenerationRequest (this: DealPreparationService, request: Request, response: Response) {
  const dataset = request.params['dataset'];
  const id = request.params['id'];
  const { action, tmpDir, outDir, skipInaccessibleFiles } = <UpdateGenerationRequest>request.body;
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
  try {
    if (tmpDir) {
      await fs.access(tmpDir, constants.F_OK);
    }
    if (outDir) {
      await fs.access(outDir, constants.F_OK);
    }
  } catch (_) {
    sendError(this.logger, response, ErrorCode.PATH_NOT_ACCESSIBLE);
    return;
  }

  const actionMap = {
    resume: {
      condition: { status: 'paused' },
      update: {
        $set: {
          status: 'active',
          workerId: null
        }
      }
    },
    pause: {
      condition: { status: 'active' },
      update: {
        $set: {
          status: 'paused',
          workerId: null
        }
      }
    },
    retry: {
      condition: { status: 'error' },
      update: {
        $set: {
          status: 'active',
          workerId: null
        },
        $unset: {
          errorMessage: 1
        }
      }
    },
    forceRetry: {
      condition: { status: { $in: ['completed', 'error'] } },
      update: {
        $set: {
          status: 'active',
          workerId: null
        },
        $unset: {
          errorMessage: 1
        }
      }
    }
  };

  let success = false;
  if (!id) {
    if (tmpDir !== undefined || outDir !== undefined || skipInaccessibleFiles !== undefined) {
      success ||= (await Datastore.GenerationRequestModel.updateMany({
        datasetId: found.id
      }, {
        $set: {
          tmpDir,
          outDir,
          skipInaccessibleFiles
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
    if (tmpDir !== undefined || outDir !== undefined || skipInaccessibleFiles !== undefined) {
      success ||= (await Datastore.GenerationRequestModel.findByIdAndUpdate(
        generation.id,
        {
          $set: {
            tmpDir,
            outDir,
            skipInaccessibleFiles
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
