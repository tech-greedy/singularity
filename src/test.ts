import rrdir from 'rrdir';

(async () => {
  for await (const entry of rrdir('.', {
    stats: true
  })) {
    console.log(entry);
  }
})();
