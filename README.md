# Feature extractor

## Installing Deps

```bash
$ pipenv install
```

## Running

An example way of running this

```bash
# Activate the environment
$ pipenv shell

# View the help
(extract-features) $ python extract-features.py --help
Usage: extract-features.py [OPTIONS]

Options:
  --input-file TEXT      input metadata.csv file  [required]
  --output-dir TEXT      output directory  [required]
  --num-workers INTEGER  Concurrent operations/requests
  --help                 Show this message and exit.

# Now run it
(extract-features) $ TIKA_CTAKES_API=http://tika-ctakes.covid19data.space.internal\
    TIKA_GEO_API=http://tika-geo.covid19data.space.internal\
    python extract-ctakes-features.new.py\
        --num-workers=4\
        --input-file=metadata.2020-04-03.csv\
        --output-dir=COVID_TEST-$(date -u +%Y%m%d%H%M)
## here is some sample output; these warnings are ok, there are tickets to fix the 'noop' one.
/usr/lib64/python3.7/asyncio/base_events.py:538: DtypeWarning: Columns (13) have mixed types.Specify dtype option on import or set low_memory=
False.
  self._run_once()
/home/ec2-user/.local/share/virtualenvs/extract-ctakes-features-BQXMqXgQ/lib64/python3.7/site-packages/aiohttp/client.py:977: RuntimeWarning: 
coroutine 'noop' was never awaited
  self._resp.release()
RuntimeWarning: Enable tracemalloc to get the object allocation traceback
Writing JSON to:  [COVID_TEST-202004092353/geo-json/10.1016_0002-9343(85)90361-4.json]
Writing JSON to:  [COVID_TEST-202004092353/ctakes-json/10.1016_0002-9343(73)90176-9.json]
Writing JSON to:  [COVID_TEST-202004092353/ctakes-json/10.1016_0002-9343(85)90361-4.json]
...
```

By default there is only 1 CTAKES server running, so if you plan on increasing to a bunch of workers they server may fall over before autoscaling has a chance to kick in. I typically increase the desired count before running with 32 workers.