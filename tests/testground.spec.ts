import axios from "axios";

xdescribe("axios", () => {
  jasmine.DEFAULT_TIMEOUT_INTERVAL = 600000;
  it('should not throw if the content length is too large', async () => {
    let buffer = Buffer.alloc(100000000, 'a');
    const response = await axios.post('https://api.web3.storage/upload', buffer, {
      headers: {
        Authorization: `Bearer ..`
      },
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
    });
    console.log(response.statusText);
  })
})
