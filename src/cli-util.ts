import { AxiosResponse } from 'axios';

export default class CliUtil {
  public static renderErrorAndExit (error: any) {
    if (error.response) {
      console.log(`Response: ${error.response.status}`);
      console.log(error.response.data);
    } else {
      console.error(error);
    }
    process.exit(1);
  }

  public static renderResponseOld (response: AxiosResponse) {
    console.table(response.data);
  }

  public static renderResponse<T> (data: T, json: boolean) {
    if (json) {
      console.log(JSON.stringify(data, null, 2));
    } else {
      console.table(data);
    }
  }
}
