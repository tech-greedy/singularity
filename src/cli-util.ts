import { AxiosResponse } from 'axios';

export default class CliUtil {
  public static renderErrorAndExit (error: any) {
    if (error.response) {
      console.error(`Response: ${error.response.status}`);
      if (error.response.data?.error) {
        console.error(error.response.data.error);
        console.error(error.response.data.message);
      } else {
        console.error(error.response.data);
      }
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
