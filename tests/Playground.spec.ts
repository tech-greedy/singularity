import { GetObjectCommand, GetObjectCommandInput, S3Client } from '@aws-sdk/client-s3';
import fs from 'fs-extra';
import NoopRequestSigner from '../src/deal-preparation/NoopRequestSigner';
import { pipeline } from 'stream/promises';
import { performance } from 'perf_hooks';

xdescribe('Playground', () => {
  jasmine.DEFAULT_TIMEOUT_INTERVAL = 100000000;
  it('should expect same download speed as wget using nodejs streaming', async () => {
    const commandInput : GetObjectCommandInput = {
      Bucket: 'msd-for-monai',
      Key: 'Task09_Spleen.tar'
    };
    const command = new GetObjectCommand(commandInput);
    const client = new S3Client({ region: 'us-west-2', signer: new NoopRequestSigner()});
    const response = await client.send(command);
    const writeStream = fs.createWriteStream('./Task09_Spleen.tar');
    let timeSpentInMs = performance.now();
    await pipeline(response.Body, writeStream);
    timeSpentInMs = performance.now() - timeSpentInMs;
    console.log(`Spend ${timeSpentInMs} ms`);
  })
})
