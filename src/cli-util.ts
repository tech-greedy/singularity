import { AxiosResponse } from 'axios';

export default class CidUtil {
  public static renderErrorAndExit (error: any) {
    if (error instanceof Error) {
      console.error(error.message);
    } else {
      console.error(error);
    }
    process.exit(1);
  }

  public static renderResponse (response: AxiosResponse) {
    console.log(response.data);
  }
}
