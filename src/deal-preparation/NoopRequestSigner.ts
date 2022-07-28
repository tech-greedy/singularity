import { RequestSigner } from '@aws-sdk/types/dist-types/signature';
import { HttpRequest, RequestSigningArguments } from '@aws-sdk/types';

export default class NoopRequestSigner implements RequestSigner {
  public sign (requestToSign: HttpRequest, _options?: RequestSigningArguments): Promise<HttpRequest> {
    return Promise.resolve(requestToSign);
  }
}
